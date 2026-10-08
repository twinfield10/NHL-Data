"""Precomputed ratings boards for the site: built after every pregame run and nightly.

The API used to build these on request (a lineup projection, league constants and the
rankings boards: ~4 s). Now the pipeline writes them under ``site/ratings/`` and the API reads
them, falling back to building live when the files are missing or out of date.

Layout (``manifest.json`` is written last, so a reader never sees a half-written build):

* ``site/ratings/{players,goalies,teams,goalie_weights,lineups,lines}.parquet``: the boards;
* ``site/ratings/units_{season}.parquet``: every observed 5v5 unit (current and last season);
* ``site/ratings/manifest.json``: ``day``, ``snapshot``, ``as_of``, ``season``, ``league``,
  ``unit_seasons`` and ``built_at``.

The snapshot's own ``ev/st/finishing/penalties`` tables aren't copied; readers load them from
``ratings/{snapshot}/``.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone

import polars as pl

from nhl.pregame import lineups
from nhl.ratings import rankings
from nhl.sim import constants as sim_constants
from nhl.sim.inputs import snapshot_dates
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

PREFIX = "site/ratings/"
MANIFEST = f"{PREFIX}manifest.json"
BOARDS = ("players", "goalies", "teams", "goalie_weights", "lineups", "lines")


def units_key(season: int) -> str:
    """Observed units of ``season``."""
    return f"{PREFIX}units_{season}.parquet"


def rating_snapshot(store: Store, day: date) -> date | None:
    """The latest rating snapshot dated on or before ``day``."""
    days = [d for d in snapshot_dates(store) if d <= day]
    return days[-1] if days else None


def build_rankings(store: Store, day: date, snap: date, games: pl.DataFrame) -> dict:
    """Player, goalie, team and line boards from snapshot ``snap``, with every team's lineup
    projected as of now (the computation behind ``/api/ratings/*``).

    Returns:
        ``snapshot, as_of, season, league, players, goalies, teams, goalie_weights, lineups,
        lines, tables`` (``tables``: the snapshot's ``ev/st/finishing/penalties``).
    """
    season = int(games.filter(pl.col("game_date") <= day)["season"].max())
    span = [season - 10001, season]
    tables = rankings.snapshot_tables(store, snap)
    players = store.read_parquet_required(keys.PLAYERS)
    teams = rankings.current_teams(store, span, games)
    team_ids = sorted(set(games.filter(pl.col("season") == season)["home_team_id"].cast(pl.Int64).to_list()))
    as_of = datetime.now(timezone.utc)
    dep = lineups.project(store, rankings.lineup_targets(team_ids, day), span, as_of=as_of)
    starts = pl.concat([s for y in span if (s := store.get_parquet(keys.goalie_starts(y))) is not None],
                       how="vertical_relaxed")
    goalies = rankings.goalie_weights(starts, games, teams)
    constants = sim_constants.estimate(store, season)
    return {
        "snapshot": snap, "as_of": as_of, "season": season,
        "league": {"xg60_5v5": constants["xg60_5v5"], "xg60_pp": constants["xg60_pp"]},
        "players": rankings.player_board(tables, players, teams, day),
        "goalies": rankings.goalie_board(tables, players, teams, day),
        "teams": rankings.team_board(tables, dep, goalies, constants), "goalie_weights": goalies, "lineups": dep,
        "lines": rankings.line_board(tables, dep), "tables": tables,
    }


def build_units(store: Store, season: int) -> pl.DataFrame | None:
    """Every forward line and D pair iced at 5v5 in ``season`` (None without stints)."""
    stints, rosters = store.get_parquet(keys.stints(season)), store.get_parquet(keys.rosters(season))
    if stints is None or rosters is None:
        return None
    return rankings.observed_units(stints, rosters)


def build(store: Store, day: date) -> dict | None:
    """Build and write every site ratings table for ``day``. Returns the manifest (None
    without a rating snapshot)."""
    start = time.monotonic()
    snap = rating_snapshot(store, day)
    if snap is None:
        logger.warning("site tables %s: no rating snapshot", day)
        return None
    r = build_rankings(store, day, snap, store.read_parquet_required(keys.GAMES))
    for name in BOARDS:
        store.put_parquet(f"{PREFIX}{name}.parquet", r[name])
    unit_seasons = []
    for season in (r["season"], r["season"] - 10001):
        units = build_units(store, season)
        if units is not None:
            store.put_parquet(units_key(season), units)
            unit_seasons.append(season)
    manifest = {
        "day": day.isoformat(), "snapshot": snap.isoformat(), "as_of": r["as_of"].isoformat(),
        "season": r["season"], "league": r["league"], "unit_seasons": unit_seasons,
        "built_at": datetime.now(timezone.utc).isoformat(),
    }
    store.put_bytes(MANIFEST, json.dumps(manifest).encode(), "application/json")
    logger.info("site tables %s built in %.1fs (snapshot %s)", day, time.monotonic() - start, snap)
    return manifest


def rebuild_quietly(store: Store, day: date) -> None:
    """:func:`build`, logging instead of raising: a failed site build must not fail the
    pregame run or the nightly job that triggered it."""
    try:
        build(store, day)
    except Exception:
        logger.exception("site tables %s: build failed (the API falls back to building live)", day)


def read_manifest(store: Store) -> dict | None:
    """The current manifest, or None before the first build."""
    raw = store.get_bytes(MANIFEST)
    return json.loads(raw) if raw else None


def load_rankings(store: Store, manifest: dict) -> dict:
    """The :func:`build_rankings` dict from the files a ``manifest`` points to."""
    snap = date.fromisoformat(manifest["snapshot"])
    out = {name: store.read_parquet_required(f"{PREFIX}{name}.parquet") for name in BOARDS}
    return {
        **out, "snapshot": snap, "as_of": datetime.fromisoformat(manifest["as_of"]), "season": manifest["season"],
        "league": manifest["league"], "tables": rankings.snapshot_tables(store, snap),
    }
