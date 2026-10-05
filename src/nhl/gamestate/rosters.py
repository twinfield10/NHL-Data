"""Per-game rosters and head coaches, read from raw per-game payloads.

* **Rosters** (dressed players and their position that game) come from the play-by-play
  ``rosterSpots``. They include dressed players who never took a shift, such as the backup
  goalie.
* **Coaches** and **scratches** come from the cached gamecenter right-rail payloads
  (``raw/external/right_rail/{season}/``, written by :mod:`nhl.sources.officials`).
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import polars as pl

from nhl.sources.officials import clean_name, name_slug, raw_right_rail_key, raw_right_rail_prefix
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.transform.events import roster_from_pbp

logger = logging.getLogger(__name__)

FORWARD_POSITIONS = ("C", "L", "R")

_COACH_SCHEMA = {
    "game_id": pl.Int64,
    "is_home": pl.Boolean,
    "head_coach": pl.Utf8,
    "scratches": pl.List(pl.Int64),
}


def _read_all(store: Store, keys_: list[str], workers: int) -> list[Any]:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return [p for p in pool.map(store.get_json_gz, keys_) if p is not None]


def season_rosters(store: Store, season: int, game_ids: list[int], workers: int = 16) -> pl.DataFrame:
    """Dressed players per game.

    Args:
        store: S3 store (raw play-by-play is usually in the local cache).
        season: 8-digit season id.
        game_ids: Games to read.
        workers: Parallel reads.

    Returns:
        ``game_id, player_id, team_id, position, player_name, is_forward, is_defense,
        is_goalie``.
    """
    payloads = _read_all(store, [keys.raw_pbp(season, g) for g in game_ids], workers)
    frames = [roster_from_pbp(p) for p in payloads]
    if not frames:
        return pl.DataFrame()
    pos = pl.col("position")
    return pl.concat(frames).with_columns(
        pos.is_in(FORWARD_POSITIONS).alias("is_forward"),
        (pos == "D").alias("is_defense"),
        (pos == "G").alias("is_goalie"),
    )


def parse_right_rail_coaches(payload: dict[str, Any], game_id: int) -> pl.DataFrame:
    """Head coach and scratches for both teams in one right-rail payload."""
    info = payload.get("gameInfo") or {}
    rows = []
    for is_home, side in ((True, "homeTeam"), (False, "awayTeam")):
        team = info.get(side) or {}
        coach = (team.get("headCoach") or {}).get("default")
        rows.append(
            {
                "game_id": game_id,
                "is_home": is_home,
                "head_coach": clean_name(coach),
                "scratches": [s["id"] for s in team.get("scratches") or [] if s.get("id") is not None],
            }
        )
    return pl.DataFrame(rows, schema=_COACH_SCHEMA)


def season_coaches(store: Store, season: int, games: pl.DataFrame, workers: int = 16) -> pl.DataFrame:
    """Head coach per team-game, with gaps filled from the team's nearest game.

    Args:
        store: S3 store.
        season: 8-digit season id.
        games: Games catalog rows for the season (``game_id, game_date, home_team_id,
            away_team_id``).
        workers: Parallel reads.

    Returns:
        ``game_id, season, game_date, team_id, is_home, head_coach, coach_id, scratches,
        coach_filled``. ``coach_filled`` marks rows whose coach came from a neighbouring game.
    """
    stored = {keys.game_id_from_key(k): k for k in store.list_keys(raw_right_rail_prefix(season))}
    wanted = [g for g in games["game_id"].to_list() if g in stored]

    def read(game_id: int) -> pl.DataFrame | None:
        payload = store.get_json_gz(raw_right_rail_key(season, game_id))
        return None if payload is None else parse_right_rail_coaches(payload, game_id)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        parsed = [f for f in pool.map(read, wanted) if f is not None]
    found = pl.concat(parsed) if parsed else pl.DataFrame(schema=_COACH_SCHEMA)
    missing = games.height - len(wanted)
    if missing:
        logger.warning("%s: %d games have no cached right-rail payload (run the officials backfill)", season, missing)

    team_games = pl.concat(
        [
            games.select("game_id", "game_date", pl.col("home_team_id").alias("team_id"), pl.lit(True).alias("is_home")),
            games.select("game_id", "game_date", pl.col("away_team_id").alias("team_id"), pl.lit(False).alias("is_home")),
        ]
    )
    coach = pl.col("head_coach")
    return (
        team_games.join(found, on=["game_id", "is_home"], how="left")
        .sort("team_id", "game_date", "game_id")
        .with_columns(coach.is_null().alias("coach_filled"))
        .with_columns(coach.forward_fill().backward_fill().over("team_id"))
        .with_columns(
            pl.col("head_coach").map_elements(name_slug, return_dtype=pl.Utf8).alias("coach_id"),
            pl.col("scratches").fill_null(pl.lit([], pl.List(pl.Int64))),
            pl.lit(season, pl.Int32).alias("season"),
        )
        .select("game_id", "season", "game_date", "team_id", "is_home", "head_coach", "coach_id", "scratches", "coach_filled")
        .sort("game_id", "is_home")
    )
