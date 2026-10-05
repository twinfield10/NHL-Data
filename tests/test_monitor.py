from __future__ import annotations

import numpy as np
import polars as pl

from nhl.models.monitor import calibration_summary


def _preds(n, rate, xg, state="EV", seed=0):
    rng = np.random.default_rng(seed)
    return pl.DataFrame({"strength_group": [state] * n, "is_goal": (rng.random(n) < rate).astype(int), "xg": [xg] * n})


def test_well_calibrated_is_not_flagged():
    rows = {r["state"]: r for r in calibration_summary(_preds(20000, 0.06, 0.06))}
    assert not rows["EV"]["flagged"] and 0.9 < rows["EV"]["ratio"] < 1.1


def test_confident_overprediction_is_flagged():
    rows = {r["state"]: r for r in calibration_summary(_preds(20000, 0.045, 0.06))}  # goals/xG ~0.75
    assert rows["EV"]["flagged"]


def test_small_samples_are_not_flagged():
    rows = {r["state"]: r for r in calibration_summary(_preds(500, 0.03, 0.06, "SH"))}
    assert not rows["SH"]["flagged"]  # too few goals to act on
