"""Pregame pricing (M5 phase D): today's games from projected lineups and the starter mixture.

``nhl pregame`` runs :func:`run`, which for every game on a date that hasn't started:

1. projects lineups as of now (:mod:`nhl.pregame.lineups`, ESPN return dates in force);
2. computes starter probabilities (:mod:`nhl.pregame.goalies`), overridden by DailyFaceoff
   ``Confirmed`` / ``Likely`` reports (:func:`apply_dfo_goalies`);
3. prices each likely starter pair with the M4 engine and averages the markets with weight
   P(home starter) × P(away starter);
4. writes snapshots (never overwritten), keyed by date and run ``stamp``: lineups, goalies,
   prices, the one-row-per-game slate and input freshness, plus a ``latest`` pointer. The
   layout is the site's data contract; see :mod:`nhl.pregame.slate`.

Forward-only inputs: each team's most recent head coach, and the referees assigned so far
(Scouting the Refs); with no assignment the crew factor is neutral.
"""

from __future__ import annotations

import fcntl
import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import polars as pl

from nhl.pregame import goalies, lineups, slate
from nhl.pregame.backtest import PRICE_COLS, mixture
from nhl.sim import constants as sim_constants
from nhl.sim import engine, inputs, markets
from nhl.sources.common import stamp
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

N_SIMS = 4000
EASTERN = ZoneInfo("America/New_York")
#: Starting probabilities given to DailyFaceoff statuses until our own captures calibrate them
#: (M5 phase E). The rest of the mass is shared among the other candidates by the model.
DFO_STATUS_P = {"Confirmed": 0.98, "Likely": 0.85}


@dataclass
class Pregame:
    """One pregame run's outputs."""

    as_of: datetime
    stamp: str
    games: pl.DataFrame
    lineups: pl.DataFrame
    goalies: pl.DataFrame
    prices: pl.DataFrame
    slate: pl.DataFrame  # one row per game: the date -> game_id summary (see nhl.pregame.slate)
    freshness: pl.DataFrame


def upcoming(store: Store, day: date, as_of: datetime) -> pl.DataFrame:
    """Games on ``day`` that haven't started at ``as_of``."""
    g = store.read_parquet_required(keys.GAMES).filter((pl.col("game_date") == day) & ~pl.col("is_final"))
    start = pl.col("start_time_et").str.to_datetime().dt.replace_time_zone("America/New_York").dt.convert_time_zone("UTC")
    return g.with_columns(start.alias("start_utc")).filter(pl.col("start_utc") > as_of)


def latest_coaches(store: Store, season: int, games: pl.DataFrame) -> pl.DataFrame:
    """``game_id, is_home, coach_id``: each team's head coach in its most recent game."""
    frames = [c for s in (season - 10001, season) if (c := store.get_parquet(keys.coaches(s))) is not None]
    if not frames:
        return pl.DataFrame(schema={"game_id": pl.Int64, "is_home": pl.Boolean, "coach_id": pl.String})
    last = pl.concat(frames, how="diagonal_relaxed").sort("game_date").group_by("team_id").last().select(
        pl.col("team_id").cast(pl.Int64), "coach_id"
    )
    return pl.concat([
        games.select("game_id", pl.lit(side == "home").alias("is_home"), pl.col(f"{side}_team_id").cast(pl.Int64).alias("team_id"))
        for side in ("home", "away")
    ]).join(last, on="team_id", how="left").select("game_id", "is_home", "coach_id")


def apply_dfo_goalies(probs: pl.DataFrame, store: Store, season: int, as_of: datetime) -> pl.DataFrame:
    """Override the model with DailyFaceoff's latest report per team-game known at ``as_of``.

    A ``Confirmed`` or ``Likely`` goalie gets :data:`DFO_STATUS_P`; the other candidates
    share the rest in proportion to the model. A named goalie who isn't a candidate is added.
    Adds ``p_model`` (the model alone), ``dfo_status`` and ``source``.
    """
    probs = probs.with_columns(pl.col("p_start").alias("p_model"), pl.lit(None, dtype=pl.String).alias("dfo_status"),
                               pl.lit("model").alias("source"))
    dfo = store.get_parquet(keys.dailyfaceoff_goalies(season))
    if dfo is None or dfo.is_empty():
        return probs
    latest = dfo.filter((pl.col("captured_at") <= as_of) & pl.col("game_id").is_not_null())
    teams = store.read_parquet_required(keys.TEAMS).select(pl.col("team_id").cast(pl.Int64), pl.col("team_abbr").alias("team"))
    latest = (
        latest.sort("captured_at").group_by("game_id", "team").last()
        .filter(pl.col("status").is_in(list(DFO_STATUS_P)) & pl.col("player_id").is_not_null())
        .join(teams, on="team").select("game_id", "team_id", pl.col("player_id").alias("dfo_goalie"), pl.col("status").alias("dfo"))
    )
    if latest.is_empty():
        return probs
    out = []
    for (gid, tid), part in probs.partition_by("game_id", "team_id", as_dict=True).items():
        hit = latest.filter((pl.col("game_id") == gid) & (pl.col("team_id") == tid))
        if hit.is_empty():
            out.append(part)
            continue
        goalie, status = hit["dfo_goalie"][0], hit["dfo"][0]
        p_named = DFO_STATUS_P[status]
        if goalie not in part["player_id"].to_list():
            part = pl.concat([part, pl.DataFrame({"game_id": [gid], "team_id": [tid], "player_id": [goalie], "p_start": [0.0],
                                                  "p_model": [0.0], "dfo_status": [None], "source": ["dfo"]})],
                             how="diagonal_relaxed")
        others = (part["player_id"] != goalie).fill_null(True)
        rest = part.filter(others)["p_model"].sum()
        part = part.with_columns(
            pl.when(~others).then(pl.lit(p_named))
            .otherwise(pl.col("p_model") / (rest if rest > 0 else 1.0) * (1 - p_named)).alias("p_start"),
            pl.when(~others).then(pl.lit(status)).otherwise(pl.col("dfo_status")).alias("dfo_status"),
            pl.lit("dfo").alias("source"),
        )
        out.append(part)
    return pl.concat(out, how="diagonal_relaxed")


def drop_injured_goalies(probs: pl.DataFrame, events: pl.DataFrame, day: date, as_of: datetime) -> pl.DataFrame:
    """Remove candidates ESPN rules out for ``day`` (latest report known at ``as_of`` is
    IR / Out / Suspension with no return date or one after ``day``), renormalising the rest.
    Only ESPN rows count: the report is current-state, so a stale entry can't linger."""
    espn = events.filter(pl.col("cause").str.starts_with("espn:") & (pl.col("known_at") <= as_of))
    if espn.is_empty():
        return probs
    latest = espn.sort("known_at").group_by("team_id", "player_id").last()
    out = latest.filter((pl.col("kind") == "out") & (pl.col("until").is_null() | (pl.col("until") > day))).select(
        "team_id", "player_id", pl.lit(True).alias("_out")
    )
    kept = probs.join(out, on=["team_id", "player_id"], how="left").filter(pl.col("_out").is_null()).drop("_out")
    dropped = probs.height - kept.height
    if dropped:
        logger.info("starter candidates ruled out by ESPN: %d", dropped)
    return kept.with_columns((pl.col("p_start") / pl.col("p_start").sum().over("game_id", "team_id")).alias("p_start"))


def starter_probs(store: Store, season: int, day: date, games: pl.DataFrame, as_of: datetime,
                  events: pl.DataFrame | None = None) -> pl.DataFrame:
    """``game_id, team_id, player_id, p_start, p_model, dfo_status, source`` for ``games``."""
    cand = goalies.build_candidates(store, [season], today=day).filter(pl.col("game_id").is_in(games["game_id"].implode()))
    probs = goalies.load(store, season).predict(cand).select("game_id", "team_id", "player_id", "p_start")
    if events is not None:
        probs = drop_injured_goalies(probs, events, day, as_of)
    return apply_dfo_goalies(probs, store, season, as_of)


def price(store: Store, season: int, games: pl.DataFrame, dep: pl.DataFrame, probs: pl.DataFrame,
          n_sims: int = N_SIMS) -> pl.DataFrame:
    """Market probabilities and score matrices for ``games``, mixed over starter pairs."""
    c = sim_constants.estimate(store, season)
    snapshots = inputs.snapshot_dates(store)
    coaches = latest_coaches(store, season, games)
    completed = store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final"))
    history = None
    if completed.height:
        dep_a, starts_a = inputs.actual_deployment(store, season)
        history = inputs.rate_table(store, season, completed, dep_a, starts_a, c, snapshots)
    team_res, acc, acc_m, base = None, None, None, None
    for starts, w in mixture(probs.select("game_id", "team_id", "player_id", "p_start"), games):
        inp = inputs.build_inputs(store, season, games, dep, starts, c, snapshots, coaches=coaches,
                                  history=history, team_res=team_res)
        team_res = inp.team_res
        sim = engine.simulate(inp, c, season, n_sims=n_sims)
        res = markets.prices(sim)
        weight = inp.games.select("game_id").join(games.select("game_id").with_columns(pl.Series("w", w)), on="game_id",
                                                  how="left", maintain_order="left")["w"].to_numpy()
        part = res.select(PRICE_COLS).to_numpy() * weight[:, None]
        part_m = markets.score_matrix(sim) * weight[:, None]
        acc = part if acc is None else acc + part
        acc_m = part_m if acc_m is None else acc_m + part_m
        base = inp.games
    if base is None:
        return pl.DataFrame()
    return base.select("game_id", "game_date", "home_team_id", "away_team_id").with_columns(
        *[pl.Series(col, acc[:, j]) for j, col in enumerate(PRICE_COLS)],
        pl.Series("score_matrix", acc_m.astype("float32"), dtype=pl.Array(pl.Float32, markets.MATRIX_SIZE)),
    )


#: One pregame run at a time on this machine. Two polls can trigger a reprice within seconds;
#: the second waits (it then sees the first one's inputs plus its own) instead of racing.
LOCK_PATH = "/tmp/nhl_data_pregame.lock"


@contextmanager
def _run_lock():
    with open(LOCK_PATH, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def run(store: Store, day: date | None = None, as_of: datetime | None = None, n_sims: int = N_SIMS,
        write: bool = True) -> Pregame | None:
    """Project, price and snapshot every game on ``day`` (default today) not yet started.

    Runs are serialised on this machine (:data:`LOCK_PATH`); ``as_of`` defaults to the
    moment the lock is taken, so a waiting run uses everything captured until then.
    """
    with _run_lock():
        return _run(store, day, as_of, n_sims, write)


def _run(store: Store, day: date | None, as_of: datetime | None, n_sims: int, write: bool) -> Pregame | None:
    as_of = as_of or datetime.now(timezone.utc)
    day = day or as_of.astimezone(EASTERN).date()
    games = upcoming(store, day, as_of)
    if games.is_empty():
        logger.info("pregame %s: no games left to price", day)
        return None
    season = int(games["season"][0])
    st = stamp(as_of)
    fresh = slate.freshness(store, season, as_of)
    targets = pl.concat([games.select("game_id", pl.col(f"{s}_team_id").cast(pl.Int64).alias("team_id"), "game_date")
                         for s in ("home", "away")])
    span = [season - 10001, season]
    rosters = pl.concat([r for s in span if (r := store.get_parquet(keys.rosters(s))) is not None])
    events = lineups.status_events(store, span, rosters)
    dep = lineups.project(store, targets, span, as_of=as_of, events=events)
    probs = starter_probs(store, season, day, games, as_of, events)
    prices = price(store, season, games, dep, probs, n_sims)
    tag = [pl.lit(as_of).alias("as_of"), pl.lit(st).alias("stamp")]
    rows = slate.build(store, season, games, dep, probs, prices, fresh, as_of, st)
    out = Pregame(as_of, st, games, dep.with_columns(*tag), probs.with_columns(*tag), prices.with_columns(*tag), rows, fresh)
    if write:
        store.put_parquet(keys.pregame_lineups(day, st), out.lineups)
        store.put_parquet(keys.pregame_goalies(day, st), out.goalies)
        store.put_parquet(keys.pregame_prices(day, st), out.prices)
        slate.write(store, day, st, rows, fresh)
    shapes = lineups.lineup_shape(dep)
    irregular = shapes.filter(~pl.col("regular"))
    if irregular.height:
        logger.warning("pregame %s: %d irregular projected lineups: %s", day, irregular.height, irregular.to_dicts())
    logger.info("pregame %s: priced %d games, %d fills, %d returns, %d game-time decisions", day, prices.height,
                dep.filter(pl.col("source") == "fill").height, dep.filter(pl.col("source") == "return").height,
                dep.filter(pl.col("source") == "gtd_backup").height)
    return out
