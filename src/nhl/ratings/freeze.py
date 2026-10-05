"""Frozen-puck model: P(the goalie freezes the puck | a saved shot on goal).

A talent-neutral companion to xG (Magnus 8's "frozen vs in play" layer). Under neutral
xG, a goalie who gives up rebounds faces extra xG and his GSAx credits him for it; the
expected freeze rate per shot lets M3 rate rebound control separately (freezes above
expected).

* **Sample:** saved shots on goal (``SHOT`` with a goalie in net); penalty shots are already
  excluded from the shot table.
* **Label:** the next event in the game is a ``STOPPAGE`` with reason
  ``goalie-stopped-after-sog``.
* **Features:** the xG feature set (rink-adjusted), state flags, and the xG era flags plus
  one at 2019-20, when the recorded freeze rate stepped from ~0.22 to ~0.24.

Training mirrors xG: recency-weighted seasons, early stopping on a validation season, a
test season, a production refit, and out-of-fold predictions for history.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl
import xgboost as xgb

from nhl import config
from nhl.features.shots import FEATURE_SETS
from nhl.gamestate.goalies import FREEZE_REASON
from nhl.models.evaluate import evaluate
from nhl.models.xg import season_weights
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

FREEZE_ERA_START = 20192020
FEATURES: list[str] = [*FEATURE_SETS["v2e"], "is_pp", "is_sh", "era_freeze19"]
PARAMS: dict[str, Any] = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "tree_method": "hist",
    "eta": 0.05,
    "max_depth": 6,
    "min_child_weight": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "lambda": 5.0,
    "seed": 71,
}
HALF_LIFE = 3.0
MAX_ROUNDS, EARLY_STOP = 3000, 100


def freeze_labels(events: pl.DataFrame) -> pl.DataFrame:
    """``game_id, event_idx, frozen`` for every saved shot on goal in ``events``."""
    return (
        events.sort("game_id", "event_idx")
        .with_columns(
            pl.col("event_type").shift(-1).over("game_id").alias("_next"),
            pl.col("reason").shift(-1).over("game_id").alias("_next_reason"),
        )
        .filter(pl.col("event_type") == "SHOT")
        .select(
            "game_id", "event_idx",
            ((pl.col("_next") == "STOPPAGE") & (pl.col("_next_reason") == FREEZE_REASON)).fill_null(False).cast(pl.Int8).alias("frozen"),
        )
    )


def load_sample(store: Store, start_years: list[int]) -> pl.DataFrame:
    """Saved shots on goal with xG features and the freeze label."""
    frames = []
    for year in start_years:
        season = config.season_id(year)
        shots = store.read_parquet_required(keys.shots_rink_adjusted(season)).filter(
            (pl.col("event_type") == "SHOT") & pl.col("goalie_id").is_not_null() & (pl.col("strength_group") != "EN")
        )
        labels = freeze_labels(store.read_parquet_required(keys.events(season)).select("game_id", "event_idx", "event_type", "reason"))
        frames.append(shots.join(labels, on=["game_id", "event_idx"], how="inner"))
    g = pl.col("strength_group")
    return pl.concat(frames, how="vertical_relaxed").with_columns(
        (g == "PP").cast(pl.Float32).alias("is_pp"),
        (g == "SH").cast(pl.Float32).alias("is_sh"),
        (pl.col("season") >= FREEZE_ERA_START).cast(pl.Float32).alias("era_freeze19"),
    )


def _dmatrix(df: pl.DataFrame, weights: np.ndarray | None = None) -> xgb.DMatrix:
    d = xgb.DMatrix(df.select(FEATURES).to_numpy(), label=df["frozen"].to_numpy(), feature_names=FEATURES, missing=np.nan)
    if weights is not None:
        d.set_weight(weights)
    return d


def _seasons(df: pl.DataFrame, years: list[int]) -> pl.DataFrame:
    return df.filter(pl.col("season").is_in([config.season_id(y) for y in years]))


@dataclass
class FreezeResult:
    """A trained frozen-puck model and its evaluation."""

    booster: xgb.Booster
    best_iteration: int
    rounds_per_weight: float
    valid: dict[str, Any]
    test: dict[str, Any]
    test_by_era: dict[str, dict[str, Any]]


def train(sample: pl.DataFrame, train_years: list[int], valid_years: list[int], test_years: list[int]) -> FreezeResult:
    """Fit with early stopping on ``valid``, report on ``test``, refit on everything.

    The refit uses ``best_iteration`` scaled by total training weight (the xG convention),
    so a larger refit sample gets proportionally more rounds.
    """
    tr, va, te = (_seasons(sample, y) for y in (train_years, valid_years, test_years))
    anchor = config.season_id(max(train_years))
    w = season_weights(tr["season"].to_numpy(), anchor, HALF_LIFE)
    booster = xgb.train(
        PARAMS, _dmatrix(tr, w), num_boost_round=MAX_ROUNDS, evals=[(_dmatrix(va), "valid")],
        early_stopping_rounds=EARLY_STOP, verbose_eval=False,
    )
    best = booster.best_iteration + 1
    rounds_per_weight = best / w.sum()

    def score(df: pl.DataFrame) -> np.ndarray:
        return booster.predict(_dmatrix(df), iteration_range=(0, best))

    valid = evaluate(va["frozen"].to_numpy(), score(va))
    test = evaluate(te["frozen"].to_numpy(), score(te))
    by_era = {}
    for name, part in (("pre_2019", te.filter(pl.col("season") < FREEZE_ERA_START)), ("2019_on", te.filter(pl.col("season") >= FREEZE_ERA_START))):
        if part.height:
            by_era[name] = evaluate(part["frozen"].to_numpy(), score(part))

    everything = _seasons(sample, sorted(set(train_years + valid_years + test_years)))
    w_all = season_weights(everything["season"].to_numpy(), config.season_id(max(train_years + valid_years + test_years)), HALF_LIFE)
    production = xgb.train(PARAMS, _dmatrix(everything, w_all), num_boost_round=max(1, round(rounds_per_weight * w_all.sum())))
    logger.info("freeze: best iteration %d; test log loss %.5f (skill %.4f), freezes/expected %.3f",
                best, test["log_loss"], test["log_loss_skill"], test["goals_per_xg"])
    return FreezeResult(production, best, rounds_per_weight, valid, test, by_era)


def out_of_fold(sample: pl.DataFrame, years: list[int], rounds_per_weight: float) -> pl.DataFrame:
    """``p_freeze`` for every season from a model that never saw it (one fold per season)."""
    out = []
    for held in sorted(years):
        tr, te = _seasons(sample, [y for y in years if y != held]), _seasons(sample, [held])
        if te.is_empty():
            continue
        w = season_weights(tr["season"].to_numpy(), config.season_id(held), HALF_LIFE, symmetric=True)
        booster = xgb.train(PARAMS, _dmatrix(tr, w), num_boost_round=max(1, round(rounds_per_weight * w.sum())))
        out.append(te.select("season", "game_id", "event_idx", "goalie_id").with_columns(
            pl.Series("p_freeze", booster.predict(_dmatrix(te)), dtype=pl.Float32)
        ))
        logger.info("freeze: out-of-fold %s scored (%d shots)", config.season_id(held), te.height)
    return pl.concat(out)


def save(store: Store, result: FreezeResult, version: str, meta: dict[str, Any]) -> None:
    """Write the booster and metadata to ``models/freeze/{version}/`` and mark it LATEST."""
    prefix = f"models/freeze/{version}/"
    store.put_bytes(prefix + "model.ubj", bytes(result.booster.save_raw("ubj")))
    body = {
        **meta, "version": version, "features": FEATURES, "params": PARAMS, "half_life": HALF_LIFE,
        "best_iteration": result.best_iteration, "rounds_per_weight": result.rounds_per_weight,
        "valid": {k: v for k, v in result.valid.items() if k != "calibration"},
        "test": result.test, "test_by_era": result.test_by_era,
    }
    store.put_bytes(prefix + "meta.json", json.dumps(body, indent=2, default=str).encode())
    store.put_bytes("models/freeze/LATEST", version.encode())


def load(store: Store, version: str | None = None) -> xgb.Booster:
    """Load the production frozen-puck booster (LATEST by default)."""
    version = version or store.get_bytes("models/freeze/LATEST").decode().strip()
    booster = xgb.Booster()
    booster.load_model(bytearray(store.get_bytes(f"models/freeze/{version}/model.ubj")))
    return booster


def predict(booster: xgb.Booster, sample: pl.DataFrame) -> pl.DataFrame:
    """Score saved shots on goal with a production booster."""
    return sample.select("season", "game_id", "event_idx", "goalie_id").with_columns(
        pl.Series("p_freeze", booster.predict(_dmatrix(sample)), dtype=pl.Float32)
    )


def train_and_store(
    store: Store, train_years: list[int], valid_years: list[int], test_years: list[int], score_years: list[int], version: str
) -> FreezeResult:
    """Train, save as LATEST, write out-of-fold predictions for complete seasons and
    production predictions for any later season in ``score_years``.

    Args:
        store: S3 store.
        train_years, valid_years, test_years: Season start years per role.
        score_years: Seasons to write predictions for.
        version: Model version label.
    """
    complete = sorted(set(train_years + valid_years + test_years))
    sample = load_sample(store, sorted(set(complete) | set(score_years)))
    result = train(sample, train_years, valid_years, test_years)
    save(store, result, version, {"train": train_years, "valid": valid_years, "test": test_years})
    oof = out_of_fold(sample, [y for y in complete if y in score_years], result.rounds_per_weight)
    later = [y for y in score_years if y not in complete]
    frames = [oof] + ([predict(result.booster, _seasons(sample, later))] if later else [])
    for frame in frames:
        for (season,), part in frame.partition_by("season", as_dict=True).items():
            store.put_parquet(keys.freeze_predictions(season), part.with_columns(pl.lit(version).alias("model_version")))
    return result


def score_seasons(store: Store, years: list[int]) -> None:
    """Score seasons with the LATEST production model (nightly, for the current season)."""
    version = store.get_bytes("models/freeze/LATEST").decode().strip()
    booster = load(store, version)
    for year in years:
        sample = load_sample(store, [year])
        store.put_parquet(
            keys.freeze_predictions(config.season_id(year)),
            predict(booster, sample).with_columns(pl.lit(version).alias("model_version")),
        )
