"""Evaluation metrics for probability models.

For expected goals, calibration (do 0.10-xG shots score 10% of the time?) matters as
much as ranking, so every report includes log loss, Brier, AUC, total goals vs total
xG and a binned reliability table. Predictions passed here must be probabilities;
the legacy ``score_model`` scored raw log-odds clipped to [0, 1], which made its
learning-rate comparisons meaningless.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

CALIBRATION_EDGES = [0.0, 0.02, 0.04, 0.06, 0.08, 0.10, 0.15, 0.20, 0.30, 0.50, 1.0]


def evaluate(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    """Score probability predictions against binary outcomes.

    Args:
        y: 0/1 outcomes.
        p: Predicted probabilities in (0, 1).

    Returns:
        Metrics dict including ``log_loss_skill`` (improvement over predicting the
        base rate for every shot) and a ``calibration`` table.

    Raises:
        ValueError: If ``p`` contains values outside [0, 1] (catches log-odds by mistake).
    """
    y = np.asarray(y, dtype=float)
    p = np.asarray(p, dtype=float)
    if p.size and (p.min() < 0 or p.max() > 1):
        raise ValueError("predictions must be probabilities in [0, 1]")
    base = np.full_like(p, y.mean())
    ll = log_loss(y, p, labels=[0, 1])
    ll_base = log_loss(y, base, labels=[0, 1])
    return {
        "n": int(y.size),
        "goals": int(y.sum()),
        "xg": float(p.sum()),
        "goals_per_xg": float(y.sum() / p.sum()) if p.sum() else None,
        "log_loss": float(ll),
        "log_loss_base_rate": float(ll_base),
        "log_loss_skill": float(1 - ll / ll_base),
        "brier": float(brier_score_loss(y, p)),
        "auc": float(roc_auc_score(y, p)) if 0 < y.sum() < y.size else None,
        "calibration": calibration_table(y, p).to_dicts(),
    }


def calibration_table(y: np.ndarray, p: np.ndarray, edges: list[float] | None = None) -> pl.DataFrame:
    """Binned reliability table: mean predicted vs observed goal rate.

    Args:
        y: 0/1 outcomes.
        p: Predicted probabilities.
        edges: Bin edges (defaults to :data:`CALIBRATION_EDGES`).

    Returns:
        ``bin, n, mean_xg, goal_rate``.
    """
    edges = edges or CALIBRATION_EDGES
    labels = [f"{lo:.2f}-{hi:.2f}" for lo, hi in zip(edges[:-1], edges[1:])]
    return (
        pl.DataFrame({"y": y, "p": p})
        .with_columns(pl.col("p").cut(edges[1:-1], labels=labels).alias("bin"))
        .group_by("bin")
        .agg(pl.len().alias("n"), pl.col("p").mean().alias("mean_xg"), pl.col("y").mean().alias("goal_rate"))
        .sort("bin")
        .with_columns(pl.col("bin").cast(pl.Utf8))
    )


def format_report(name: str, m: dict[str, Any]) -> str:
    """Render one metrics dict as a compact text block."""
    lines = [
        f"{name}: n={m['n']:,} goals={m['goals']:,} xG={m['xg']:,.1f} (goals/xG {m['goals_per_xg']:.3f}) "
        f"logloss={m['log_loss']:.4f} (skill {m['log_loss_skill']:+.3f}) brier={m['brier']:.4f} auc={m['auc']:.4f}",
    ]
    for row in m["calibration"]:
        if row["n"]:
            lines.append(f"    {row['bin']:>11}  n={row['n']:>7,}  xG={row['mean_xg']:.3f}  actual={row['goal_rate']:.3f}")
    return "\n".join(lines)
