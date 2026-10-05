"""Penalty drawing and taking rates per player (M3 phase E).

Only penalties that put a team short-handed count: 2, 4 and 5 minutes (misconducts and
game misconducts don't change the strength). Rates are per 60 minutes of ice time at
any strength.

Each rate has a gamma prior. The prior is last season's posterior with its evidence
(events and exposure) multiplied by ``decay``, plus ``position_hours`` of evidence at the
position average (F or D). The posterior mean is

    (prior_events + events) / (prior_hours + hours)

Tuned like the other ratings: fit through Dec 31, score the rest by Poisson log
likelihood (:func:`midseason_eval`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl

from nhl import config
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

PP_MINUTES = (2, 4, 5)
KINDS = ("taken", "drawn")


@dataclass
class Hyper:
    """Prior settings. Tuned 2026-10-05 with :func:`midseason_eval` (2012-2025): decay 0.7
    and 5 hours of position-average evidence (interior optimum); +7.4% Poisson log
    likelihood over position rates, so penalty tendencies are strongly individual."""

    decay: float = 0.7
    position_hours: float = 5.0


DEFAULT_HYPER = Hyper()


def season_events(store: Store, season: int) -> pl.DataFrame:
    """Per player-game: ``game_id, game_date, player_id, is_defense, toi_s, taken, drawn``."""
    ev = store.read_parquet_required(keys.events(season)).filter(
        (pl.col("event_type") == "PENALTY") & pl.col("penalty_minutes").is_in(PP_MINUTES)
    )
    taken = ev.group_by("game_id", pl.col("player_1_id").alias("player_id")).agg(pl.len().cast(pl.Int32).alias("taken"))
    drawn = ev.filter(pl.col("player_2_id").is_not_null()).group_by("game_id", pl.col("player_2_id").alias("player_id")).agg(
        pl.len().cast(pl.Int32).alias("drawn")
    )
    logs = store.read_parquet_required(keys.player_game_logs(season)).filter(
        (pl.col("strength") == "all") & (pl.col("position") != "G")
    ).select("game_id", "game_date", "player_id", (pl.col("position") == "D").alias("is_defense"), "toi_s")
    return (
        logs.join(taken, on=["game_id", "player_id"], how="left")
        .join(drawn, on=["game_id", "player_id"], how="left")
        .with_columns(pl.col("taken", "drawn").fill_null(0))
    )


def _totals(games: pl.DataFrame) -> pl.DataFrame:
    return games.group_by("player_id").agg(
        pl.col("is_defense").last(), (pl.col("toi_s").sum() / 3600).alias("hours"), pl.col("taken").sum(), pl.col("drawn").sum()
    )


def fit(games: pl.DataFrame, prior: pl.DataFrame | None, hyper: Hyper = DEFAULT_HYPER) -> pl.DataFrame:
    """Posterior evidence and rates per player.

    Args:
        games: :func:`season_events` rows (any span).
        prior: Carried evidence ``player_id, kind, events, hours`` (decayed already).

    Returns:
        ``player_id, is_defense, kind, events, hours, rate`` where ``events``/``hours`` include
        the prior and the position pseudo-evidence, and ``rate`` is per 60.
    """
    totals = _totals(games)
    position = totals.group_by("is_defense").agg(
        *[(pl.col(k).sum() / pl.col("hours").sum()).alias(k) for k in KINDS]
    )
    rows = []
    for kind in KINDS:
        part = totals.select("player_id", "is_defense", pl.lit(kind).alias("kind"), pl.col(kind).alias("obs"), pl.col("hours").alias("obs_hours"))
        if prior is not None:
            part = part.join(prior.filter(pl.col("kind") == kind).select("player_id", "events", "hours"), on="player_id", how="left")
        else:
            part = part.with_columns(pl.lit(None, pl.Float64).alias("events"), pl.lit(None, pl.Float64).alias("hours"))
        rows.append(part.join(position.select("is_defense", pl.col(kind).alias("pos_rate")), on="is_defense", how="left"))
    out = pl.concat(rows).with_columns(
        (pl.col("events").fill_null(0.0) + pl.col("pos_rate") * hyper.position_hours + pl.col("obs")).alias("events"),
        (pl.col("hours").fill_null(0.0) + hyper.position_hours + pl.col("obs_hours")).alias("hours"),
    )
    return out.with_columns((pl.col("events") / pl.col("hours")).alias("rate")).select(
        "player_id", "is_defense", "kind", "events", "hours", "rate", "pos_rate"
    )


def carry_forward(previous: pl.DataFrame | None, fitted: pl.DataFrame, hyper: Hyper = DEFAULT_HYPER) -> pl.DataFrame:
    """Next season's prior evidence: the player's own decayed evidence (the position
    pseudo-evidence is re-added fresh each season, so it doesn't pile up)."""
    current = fitted.select(
        "player_id", "kind",
        ((pl.col("events") - pl.col("pos_rate") * hyper.position_hours) * hyper.decay).alias("events"),
        ((pl.col("hours") - hyper.position_hours) * hyper.decay).alias("hours"),
    )
    if previous is None:
        return current
    absent = previous.join(current.select("player_id", "kind"), on=["player_id", "kind"], how="anti").with_columns(
        pl.col("events") * hyper.decay, pl.col("hours") * hyper.decay
    )
    return pl.concat([current, absent])


def _poisson_ll(rate_per_hour: np.ndarray, hours: np.ndarray, counts: np.ndarray) -> float:
    mu = np.clip(rate_per_hour * hours, 1e-12, None)
    return float(np.sum(counts * np.log(mu) - mu))


def midseason_eval(seasons: dict[int, pl.DataFrame], hyper: Hyper, eval_years: list[int]) -> dict[str, float]:
    """Fit through Dec 31 (from the chained prior), score the rest; baseline = position rates."""
    prior, priors = None, {}
    for year in sorted(seasons):
        priors[year] = prior
        prior = carry_forward(prior, fit(seasons[year], prior, hyper), hyper)
    ll_m = ll_b = 0.0
    for year in eval_years:
        games = seasons[year]
        cut = games["game_date"].median() if year in (2012, 2020) else date(year + 1, 1, 1)
        early, late = games.filter(pl.col("game_date") < cut), games.filter(pl.col("game_date") >= cut)
        post = fit(early, priors[year], hyper)
        base = fit(early, None, Hyper(decay=0.0, position_hours=1e9))
        lt = _totals(late)
        for kind in KINDS:
            obs = lt.select("player_id", "hours", pl.col(kind).alias("n"))
            for table, acc in ((post, "m"), (base, "b")):
                j = obs.join(table.filter(pl.col("kind") == kind).select("player_id", "rate"), on="player_id", how="left")
                fallback = float(table.filter(pl.col("kind") == kind)["rate"].mean())
                ll = _poisson_ll(j["rate"].fill_null(fallback).to_numpy(), j["hours"].to_numpy(), j["n"].to_numpy())
                if acc == "m":
                    ll_m += ll
                else:
                    ll_b += ll
    return {"model": ll_m, "baseline": ll_b, "improvement": (ll_m - ll_b) / abs(ll_b)}


def as_of(store: Store, day: date, hyper: Hyper = DEFAULT_HYPER) -> pl.DataFrame:
    """Penalty rates using games before ``day`` and the stored prior for its season."""
    start = day.year if day.month >= 9 else day.year - 1
    season = config.season_id(start)
    prior = store.get_parquet(prior_key(season))
    games = season_events(store, season).filter(pl.col("game_date") < day)
    return fit(games, prior, hyper).with_columns(pl.lit(day).alias("as_of"))


def prior_key(season: int) -> str:
    """Penalty-rate evidence carried *into* ``season``."""
    return f"ratings/penalties/prior/{season}.parquet"


def build_priors(store: Store, start_years: list[int], hyper: Hyper = DEFAULT_HYPER) -> None:
    """Chain full seasons and store the prior entering each following season."""
    prior = None
    for year in sorted(start_years):
        prior = carry_forward(prior, fit(season_events(store, config.season_id(year)), prior, hyper), hyper)
        store.put_parquet(prior_key(config.season_id(year + 1)), prior)
