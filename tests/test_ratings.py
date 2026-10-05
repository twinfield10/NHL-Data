from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from nhl.ratings import finishing as F
from nhl.ratings.freeze import freeze_labels


def test_freeze_label_is_the_next_event():
    ev = pl.DataFrame({
        "game_id": [1] * 5, "event_idx": [1, 2, 3, 4, 5],
        "event_type": ["SHOT", "STOPPAGE", "SHOT", "HIT", "SHOT"],
        "reason": [None, "goalie-stopped-after-sog", None, None, None],
    })
    assert freeze_labels(ev).sort("event_idx")["frozen"].to_list() == [1, 0, 0]


def _shots(n: int = 6000, seed: int = 3) -> pl.DataFrame:
    """Shooter 1 converts at twice the xG odds; everyone else is neutral."""
    rng = np.random.default_rng(seed)
    shooter = rng.integers(1, 21, n)
    goalie = rng.integers(101, 105, n)
    xg = rng.uniform(0.02, 0.2, n)
    logit = np.log(xg / (1 - xg)) + np.where(shooter == 1, np.log(2.0), 0.0)
    goal = rng.random(n) < 1 / (1 + np.exp(-logit))
    return pl.DataFrame({
        "season": 20242025, "game_id": np.arange(n), "game_date": [date(2024, 11, 1)] * n, "event_idx": 1,
        "shooter_id": shooter, "goalie_id": goalie, "xg": xg.astype(np.float32), "is_goal": goal.astype(np.int8),
        "is_defense": shooter > 15,
    })


def test_finishing_recovers_a_good_shooter_and_stays_centred():
    f = F.fit(_shots(), hyper={"shooter": F.Hyper(1.0, 50, 0.0), "goalie": F.Hyper(1.0, 5000, 0.0)})
    sh = f.terms.filter(pl.col("role") == "shooter").sort("mean", descending=True)
    assert sh["player_id"][0] == 1 and sh["mean"][0] > 0.3
    weighted = (sh["mean"] * sh["shots"]).sum() / sh["shots"].sum()
    assert abs(weighted) < 1e-3


def test_no_talent_baseline_and_carry_forward():
    shots = _shots()
    base = F.fit(shots, hyper=F.NO_TALENT)
    assert np.abs(base.terms["mean"].to_numpy()).max() < 1e-3
    f = F.fit(shots)
    prior = F.carry_forward(None, f)
    # A player absent next season keeps a (further decayed) prior.
    nxt = F.fit(shots.filter(pl.col("shooter_id") != 1), prior)
    carried = F.carry_forward(prior, nxt).filter((pl.col("role") == "shooter") & (pl.col("player_id") == 1))
    assert carried.height == 1
    assert carried["precision"][0] < prior.filter((pl.col("role") == "shooter") & (pl.col("player_id") == 1))["precision"][0]
