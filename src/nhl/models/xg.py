"""Expected goals: one gradient-boosted model per game-state group (EV / PP / SH / EN).

Workflow (``nhl train-xg``):

1. **Tune** each group with Optuna on a season-based split: train on early seasons,
   early-stop and score trials on ``valid`` seasons by log loss.
2. **Test** the tuned model on later, untouched ``test`` seasons and report
   calibration (see :mod:`nhl.models.evaluate`).
3. **Refit** on every complete season for production scoring of the current season.
4. **Out-of-fold xG** for historical seasons: season-grouped folds, so no shot is scored
   by a model that saw it. These are the values downstream analysis should use.

Models are saved in XGBoost's native ``.ubj`` format with JSON metadata rather than
pickle, so they load across library versions.
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import optuna
import polars as pl
import xgboost as xgb

from nhl import config
from nhl.features.shots import FEATURES, STRENGTH_GROUPS
from nhl.models.evaluate import evaluate, format_report
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

BASE_PARAMS: dict[str, Any] = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "tree_method": "hist",
    "seed": 71,
}
MAX_ROUNDS = 5000
EARLY_STOP = 100


@dataclass
class SeasonSplit:
    """Season start years for each role."""

    train: list[int]
    valid: list[int]
    test: list[int]

    @property
    def all(self) -> list[int]:
        """Every complete season, for the production refit."""
        return sorted(set(self.train + self.valid + self.test))

    def to_dict(self) -> dict[str, list[int]]:
        """Seasons as 8-digit ids, for metadata."""
        return {k: [config.season_id(y) for y in getattr(self, k)] for k in ("train", "valid", "test")}


@dataclass
class GroupResult:
    """Everything learned for one strength group."""

    group: str
    params: dict[str, Any]
    best_iteration: int
    valid_metrics: dict[str, Any]
    test_metrics: dict[str, Any]
    production: xgb.Booster | None = None
    n_train: int = 0
    trials: list[dict[str, Any]] = field(default_factory=list)


def load_shots(store: Store, seasons: list[int]) -> pl.DataFrame:
    """Concatenate shot feature tables for the given season start years."""
    frames = [store.read_parquet_required(keys.shots(config.season_id(y))) for y in seasons]
    return pl.concat(frames, how="vertical_relaxed")


def _dmatrix(df: pl.DataFrame) -> xgb.DMatrix:
    return xgb.DMatrix(
        df.select(FEATURES).to_numpy(), label=df["is_goal"].to_numpy(), feature_names=FEATURES, missing=np.nan
    )


def _seasons(df: pl.DataFrame, years: list[int]) -> pl.DataFrame:
    return df.filter(pl.col("season").is_in([config.season_id(y) for y in years]))


def season_weights(seasons: np.ndarray, anchor: int, half_life: float, symmetric: bool = False) -> np.ndarray:
    """Exponential recency weights by season.

    The shot-to-goal relationship drifts (most visibly after the NHL's move to tracked
    shot coordinates), so recent seasons should dominate while older ones still teach
    the general shape. A season ``half_life`` seasons away from ``anchor`` gets weight
    0.5.

    Args:
        seasons: 8-digit season ids, one per row.
        anchor: 8-digit season id with weight 1 (the latest training season, or the
            season being scored out-of-fold).
        half_life: Seasons for the weight to halve.
        symmetric: Weight by absolute distance (out-of-fold scoring of past seasons
            uses neighbours on both sides).

    Returns:
        Weight per row.
    """
    gap = (anchor // 10000) - (seasons // 10000)
    gap = np.abs(gap) if symmetric else np.clip(gap, 0, None)
    return np.power(0.5, gap / half_life)


def _dmatrix_w(df: pl.DataFrame, weights: np.ndarray | None) -> xgb.DMatrix:
    d = _dmatrix(df)
    if weights is not None:
        d.set_weight(weights)
    return d


def _fit(params: dict[str, Any], dtrain: xgb.DMatrix, dvalid: xgb.DMatrix | None, rounds: int) -> xgb.Booster:
    booster_params = _booster_params(params)
    evals = [(dvalid, "valid")] if dvalid is not None else []
    return xgb.train(
        {**BASE_PARAMS, **booster_params},
        dtrain,
        num_boost_round=rounds,
        evals=evals,
        early_stopping_rounds=EARLY_STOP if dvalid is not None else None,
        verbose_eval=False,
    )


def _predict(booster: xgb.Booster, dmat: xgb.DMatrix) -> np.ndarray:
    """Probabilities from the best iteration (never raw margins)."""
    return booster.predict(dmat, iteration_range=(0, booster.best_iteration + 1)) if hasattr(
        booster, "best_iteration"
    ) else booster.predict(dmat)


def tune_group(
    group: str,
    shots: pl.DataFrame,
    split: SeasonSplit,
    n_trials: int,
    timeout: int,
) -> GroupResult:
    """Tune, test and refit one strength group.

    Optuna tunes the tree parameters *and* the recency half-life used to weight
    training seasons, early-stopping and scoring each trial on the ``valid`` season(s)
    by log loss. The winner is scored on the untouched ``test`` season(s), then refit
    on every complete season (weights anchored on the latest one) for production.

    Args:
        group: One of :data:`STRENGTH_GROUPS`.
        shots: Shot features for every season in ``split.all``.
        split: Season roles.
        n_trials: Maximum Optuna trials.
        timeout: Tuning time budget in seconds.

    Returns:
        A :class:`GroupResult` holding the production booster. ``params`` includes
        ``half_life``; ``rounds_per_weight`` (stored in ``best_iteration`` metadata) lets
        later fits scale boosting rounds to their weighted sample size.
    """
    data = shots.filter(pl.col("strength_group") == group)
    train, valid, test = _seasons(data, split.train), _seasons(data, split.valid), _seasons(data, split.test)
    dtrain, dvalid, dtest = _dmatrix(train), _dmatrix(valid), _dmatrix(test)
    train_seasons = train["season"].to_numpy()
    anchor = config.season_id(max(split.train))
    logger.info("%s: train %d / valid %d / test %d shots", group, train.height, valid.height, test.height)

    def objective(trial: optuna.Trial) -> float:
        params = {
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "max_depth": trial.suggest_int("max_depth", 3, 9),
            "min_child_weight": trial.suggest_float("min_child_weight", 1, 200, log=True),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-2, 50, log=True),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10, log=True),
            "gamma": trial.suggest_float("gamma", 1e-3, 5, log=True),
            "half_life": trial.suggest_float("half_life", 0.75, 8.0, log=True),
        }
        dtrain.set_weight(season_weights(train_seasons, anchor, params["half_life"]))
        booster = _fit(params, dtrain, dvalid, MAX_ROUNDS)
        trial.set_user_attr("best_iteration", booster.best_iteration)
        return float(booster.best_score)

    study = optuna.create_study(direction="minimize", sampler=optuna.samplers.TPESampler(seed=71))
    # Sensible defaults first, so even a tiny budget yields a reasonable model.
    study.enqueue_trial({"learning_rate": 0.05, "max_depth": 5, "min_child_weight": 20, "subsample": 0.8,
                         "colsample_bytree": 0.8, "reg_lambda": 1.0, "reg_alpha": 0.01, "gamma": 0.01, "half_life": 1.5})
    study.optimize(objective, n_trials=n_trials, timeout=timeout, show_progress_bar=False)
    best = study.best_trial
    params = dict(best.params)
    best_iter = int(best.user_attrs["best_iteration"])
    logger.info("%s: best valid logloss %.5f after %d trials (%d rounds, half-life %.2f seasons)",
                group, best.value, len(study.trials), best_iter, params["half_life"])

    # Re-fit the winning configuration to score valid and test.
    w_train = season_weights(train_seasons, anchor, params["half_life"])
    dtrain.set_weight(w_train)
    model = _fit(params, dtrain, dvalid, MAX_ROUNDS)
    valid_m = evaluate(valid["is_goal"].to_numpy(), _predict(model, dvalid))
    test_m = evaluate(test["is_goal"].to_numpy(), _predict(model, dtest))

    # Production: every complete season, weights anchored on the latest; boosting rounds
    # scaled by the weighted sample size so the rounds-to-data ratio matches tuning.
    rounds_per_weight = (best_iter + 1) / w_train.sum()
    params["rounds_per_weight"] = rounds_per_weight
    full = _seasons(data, split.all)
    w_full = season_weights(full["season"].to_numpy(), config.season_id(max(split.all)), params["half_life"])
    rounds = max(1, round(rounds_per_weight * w_full.sum()))
    production = _fit(_booster_params(params), _dmatrix_w(full, w_full), None, rounds)

    return GroupResult(
        group=group,
        params=params,
        best_iteration=best_iter,
        valid_metrics=valid_m,
        test_metrics=test_m,
        production=production,
        n_train=full.height,
        trials=[{"value": t.value, **t.params} for t in study.trials if t.value is not None],
    )


def _booster_params(params: dict[str, Any]) -> dict[str, Any]:
    """Strip our own keys (half_life, rounds_per_weight) before handing params to XGBoost."""
    return {k: v for k, v in params.items() if k not in ("half_life", "rounds_per_weight")}


def out_of_fold(group: str, shots: pl.DataFrame, seasons: list[int], params: dict[str, Any]) -> pl.DataFrame:
    """xG for historical shots from models that never saw that season.

    One fold per season: each season is scored by a model trained on every *other*
    season, weighted by distance in time to the scored season (so 2015-16 is scored
    mostly by 2013-2018 data and 2025-26 mostly by 2022-2025 data).

    Args:
        group: Strength group.
        shots: Shot features.
        seasons: Season start years to score.
        params: Tuned parameters including ``half_life`` and ``rounds_per_weight``.

    Returns:
        ``game_id, event_idx, xg`` for every shot in the group and seasons.
    """
    data = _seasons(shots.filter(pl.col("strength_group") == group), seasons)
    out = []
    for held in sorted(seasons):
        tr = _seasons(data, [y for y in seasons if y != held])
        te = _seasons(data, [held])
        if te.is_empty():
            continue
        w = season_weights(tr["season"].to_numpy(), config.season_id(held), params["half_life"], symmetric=True)
        rounds = max(1, round(params["rounds_per_weight"] * w.sum()))
        booster = _fit(_booster_params(params), _dmatrix_w(tr, w), None, rounds)
        out.append(te.select("game_id", "event_idx").with_columns(pl.Series("xg", booster.predict(_dmatrix(te)))))
        logger.info("%s: out-of-fold %s scored (%d shots, %d rounds)", group, config.season_id(held), te.height, rounds)
    return pl.concat(out)


def _git_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=config.REPO_ROOT, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def save_models(store: Store, version: str, results: list[GroupResult], split: SeasonSplit) -> str:
    """Write boosters, metadata and evaluation to ``models/xg/{version}/`` and mark it LATEST.

    Returns:
        The S3 prefix written.
    """
    prefix = keys.xg_model_prefix(version)
    with tempfile.TemporaryDirectory() as tmp:
        for r in results:
            path = Path(tmp) / f"{r.group}.ubj"
            r.production.save_model(path)
            store.put_bytes(prefix + f"{r.group}.ubj", path.read_bytes())
    metadata = {
        "version": version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "xgboost": xgb.__version__,
        "features": FEATURES,
        "split": split.to_dict(),
        "groups": {
            r.group: {
                "params": {**BASE_PARAMS, **r.params},  # includes half_life, rounds_per_weight
                "best_iteration_on_train": r.best_iteration,
                "production_rounds": r.production.num_boosted_rounds(),
                "production_rows": r.n_train,
                "valid": r.valid_metrics,
                "test": r.test_metrics,
            }
            for r in results
        },
    }
    store.put_bytes(prefix + "metadata.json", json.dumps(metadata, indent=2).encode(), "application/json")
    store.put_bytes(prefix + "tuning_trials.json", json.dumps({r.group: r.trials for r in results}).encode(), "application/json")
    store.put_bytes(keys.XG_LATEST, version.encode(), "text/plain")
    return prefix


def load_models(store: Store, version: str | None = None) -> tuple[dict[str, xgb.Booster], dict[str, Any]]:
    """Load production boosters and metadata (defaults to the LATEST version).

    Raises:
        FileNotFoundError: If no model has been trained yet.
    """
    if version is None:
        latest = store.get_bytes(keys.XG_LATEST)
        if latest is None:
            raise FileNotFoundError("no trained xG model; run `nhl train-xg`")
        version = latest.decode().strip()
    prefix = keys.xg_model_prefix(version)
    meta = json.loads(store.get_bytes(prefix + "metadata.json"))
    models = {}
    for group in meta["groups"]:
        booster = xgb.Booster()
        booster.load_model(bytearray(store.get_bytes(prefix + f"{group}.ubj")))
        models[group] = booster
    return models, meta


def predict_xg(shots: pl.DataFrame, models: dict[str, xgb.Booster]) -> pl.DataFrame:
    """Score shots with the production models.

    Returns:
        ``game_id, event_idx, xg``.
    """
    out = []
    for group, booster in models.items():
        part = shots.filter(pl.col("strength_group") == group)
        if part.height:
            out.append(part.select("game_id", "event_idx").with_columns(pl.Series("xg", booster.predict(_dmatrix(part)))))
    return pl.concat(out)


def report(results: list[GroupResult]) -> str:
    """Human-readable test-set summary for all groups."""
    return "\n\n".join(format_report(f"[{r.group} | test]", r.test_metrics) for r in results)


__all__ = [
    "STRENGTH_GROUPS", "SeasonSplit", "GroupResult", "load_shots", "tune_group", "out_of_fold",
    "save_models", "load_models", "predict_xg", "report",
]
