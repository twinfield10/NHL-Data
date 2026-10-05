"""Point-in-time rating snapshots (M3 phase F): ``ratings/{date}/{kind}.parquet``.

Each snapshot uses only games before ``date`` plus the stored season priors, so history
pages and backtests see exactly what was knowable then. Kinds:

* ``ev`` / ``st``: skater offence (``side`` O) and defence (``side`` D) in xG per 60
  relative to average (defence: lower is better);
* ``context_ev`` / ``context_st``: intercept and context terms (home, rest, score, zone,
  post-penalty, coaches);
* ``finishing``: shooter and goalie logit terms on top of xG, with the intercept and
  defenseman effect;
* ``penalties``: drawn and taken rates per 60.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import polars as pl

from nhl import config
from nhl.ratings import finishing, penalties, rapm
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

KINDS = ("ev", "st", "context_ev", "context_st", "finishing", "penalties")


def snapshot_key(day: date, kind: str) -> str:
    """Key for one snapshot table."""
    return f"ratings/{day.isoformat()}/{kind}.parquet"


def _context(fit: rapm.Fit, day: date) -> pl.DataFrame:
    terms = fit.coef.filter(~pl.col("term").str.contains(r"^[OD]:")).select("term", "mean")
    return pl.concat([pl.DataFrame({"term": ["intercept"], "mean": [fit.intercept]}), terms]).with_columns(
        pl.lit(day).alias("as_of")
    )


class _SeasonCache:
    """Designs, priors and per-season inputs reused across many dates of one season."""

    def __init__(self, store: Store, start_year: int) -> None:
        self.season = config.season_id(start_year)
        ages = rapm._ages(store, start_year)
        self.designs, self.priors = {}, {}
        for state in ("EV", "ST"):
            self.designs[state] = rapm.season_design(store, self.season, state)
            self.priors[state] = rapm.age_prior(
                store.get_parquet(rapm.prior_key(state, self.season)), ages, rapm.load_curve(store, state)
            )
        self.shots = finishing.load_shots(store, self.season)
        self.finishing_prior = store.get_parquet(finishing.season_prior_key(self.season))
        self.penalty_games = penalties.season_events(store, self.season)
        self.penalty_prior = store.get_parquet(penalties.prior_key(self.season))


def snapshot(store: Store, day: date, cache: _SeasonCache | None = None) -> dict[str, int]:
    """Write every rating table as of ``day``. Returns row counts per kind."""
    start = day.year if day.month >= 9 else day.year - 1
    cache = cache or _SeasonCache(store, start)
    tables: dict[str, pl.DataFrame] = {}
    for state in ("EV", "ST"):
        design = cache.designs[state]
        mask = (design.rows["game_date"] < day).to_numpy()
        hyper = rapm.Hyper() if state == "EV" else rapm.ST_HYPER
        f = rapm.fit(rapm.normal_equations(design, mask), cache.priors[state], hyper)
        tables[state.lower()] = f.players.with_columns(pl.lit(day).alias("as_of"), (1 / pl.col("precision").sqrt()).alias("sd_s"))
        tables[f"context_{state.lower()}"] = _context(f, day)

    shots = cache.shots.filter(pl.col("game_date") < day)
    if shots.is_empty():
        fin = (cache.finishing_prior if cache.finishing_prior is not None else pl.DataFrame()).with_columns(pl.lit(day).alias("as_of"))
    else:
        fit = finishing.fit(shots, cache.finishing_prior)
        fin = fit.terms.with_columns(
            pl.lit(day).alias("as_of"), (1 / pl.col("precision").sqrt()).alias("sd"),
            pl.lit(fit.intercept).alias("intercept"), pl.lit(fit.defense).alias("defense"),
        )
    tables["finishing"] = fin
    tables["penalties"] = penalties.fit(cache.penalty_games.filter(pl.col("game_date") < day), cache.penalty_prior).with_columns(
        pl.lit(day).alias("as_of")
    )
    for kind, frame in tables.items():
        store.put_parquet(snapshot_key(day, kind), frame)
    return {k: v.height for k, v in tables.items()}


def backfill(store: Store, start_years: list[int], every_days: int = 7) -> int:
    """Weekly (by default) snapshots across seasons, from first game + 1 week to season end."""
    n = 0
    games = store.read_parquet_required("processed/games.parquet")
    for year in start_years:
        season = config.season_id(year)
        g = games.filter((pl.col("season") == season) & pl.col("is_final"))
        if g.is_empty():
            continue
        cache = _SeasonCache(store, year)
        day, last = g["game_date"].min() + timedelta(days=every_days), g["game_date"].max() + timedelta(days=1)
        while day <= last:
            snapshot(store, day, cache)
            n += 1
            day += timedelta(days=every_days)
        logger.info("%s: snapshots through %s", season, last)
    return n
