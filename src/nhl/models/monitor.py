"""In-season xG calibration monitor.

Shot recording keeps drifting (more quick-succession attempts logged each season since
2022-23), so the production model is checked as the season accumulates: goals ÷ xG by
strength state, with a Poisson band on the goal count. A state is flagged when its band
excludes the healthy range, which is the cue to retrain early rather than at the
offseason.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import date
from typing import Any

import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: Healthy goals ÷ xG range. Past seasons sit within about ±5% overall and ±10% in the
#: smaller states, so anything confidently outside 0.90-1.10 is a real miscalibration.
HEALTHY = (0.90, 1.10)
#: Don't flag before a state has this many goals; earlier bands are too wide to act on.
MIN_GOALS = 150


def calibration_summary(preds: pl.DataFrame) -> list[dict[str, Any]]:
    """Goals ÷ xG by strength state (and overall) with an approximate 95% band.

    Args:
        preds: ``predictions/xg/{season}`` rows (``strength_group``, ``is_goal``, ``xg``).

    Returns:
        One dict per state: shots, goals, xg, ratio, lo, hi, flagged.
    """
    rows = []
    groups = [(s, preds.filter(pl.col("strength_group") == s)) for s in ("EV", "PP", "SH", "EN")]
    for state, part in [*groups, ("ALL", preds)]:
        goals, xg = int(part["is_goal"].sum()), float(part["xg"].sum())
        if xg <= 0:
            continue
        ratio = goals / xg
        half = 1.96 * math.sqrt(max(goals, 1)) / xg
        lo, hi = ratio - half, ratio + half
        flagged = goals >= MIN_GOALS and (hi < HEALTHY[0] or lo > HEALTHY[1])
        rows.append({"state": state, "shots": part.height, "goals": goals, "xg": round(xg, 1),
                     "ratio": round(ratio, 3), "lo": round(lo, 3), "hi": round(hi, 3), "flagged": flagged})
    return rows


def run_monitor(store: Store, season: int, write: bool = True) -> list[dict[str, Any]]:
    """Compute the season-to-date summary, log it, and (optionally) snapshot it to S3.

    Args:
        store: S3 store.
        season: 8-digit season id.
        write: Write ``monitoring/xg/{season}/{today}.json``.

    Returns:
        The summary rows.
    """
    preds = store.read_parquet_required(keys.xg_predictions(season))
    rows = calibration_summary(preds)
    version = preds["model_version"][0] if "model_version" in preds.columns and preds.height else None
    for r in rows:
        level = logging.WARNING if r["flagged"] else logging.INFO
        logger.log(level, "xG monitor %s %s: goals/xG %.3f [%.3f, %.3f] (%d goals, %d shots)%s",
                   season, r["state"], r["ratio"], r["lo"], r["hi"], r["goals"], r["shots"],
                   "  <-- outside healthy range, consider retraining" if r["flagged"] else "")
    if write:
        payload = {"season": season, "date": date.today().isoformat(), "model_version": version, "states": rows}
        store.put_bytes(f"monitoring/xg/{season}/{date.today().isoformat()}.json",
                        json.dumps(payload, indent=1).encode(), "application/json")
    return rows
