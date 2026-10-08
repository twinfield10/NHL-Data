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


def _stint_rows(n: int = 4000, seed: int = 5) -> pl.DataFrame:
    """Attack rows where player 1 adds +1.0 xG/60 when attacking and 99 allows +0.5."""
    rng = np.random.default_rng(seed)
    rows = []
    for i in range(n):
        offence = sorted(rng.choice(np.arange(1, 21), 5, replace=False).tolist())
        defence = sorted(rng.choice(np.arange(81, 101), 5, replace=False).tolist())
        rate = 2.5 + (1.0 if 1 in offence else 0.0) + (0.5 if 99 in defence else 0.0)
        dur = 40
        rows.append({
            "game_id": i, "season": 20242025, "stint_id": 0, "period": 2, "start_s": 1500, "end_s": 1540,
            "duration_s": dur, "last_faceoff_s": 1400, "post_penalty_5v5": False, "att_home": bool(i % 2),
            "att_team": 1, "def_team": 2, "offence": offence, "defence": defence, "att_n": 5, "def_n": 5,
            "lead": 0, "xgf": rate * dur / 3600 + rng.normal(0, 0.01), "gf": 0, "att_changed": True,
            "fo_zone": "nz", "att_rest": "normal", "def_rest": "normal", "att_coach": None, "def_coach": None,
        })
    df = pl.DataFrame(rows, schema_overrides={"att_coach": pl.Utf8, "def_coach": pl.Utf8})
    return df.with_columns(pl.lit("otf").alias("zone_type"), (pl.col("xgf") * 3600 / pl.col("duration_s")).alias("y"))


def test_rapm_recovers_player_effects_and_centres():
    from nhl.ratings import design as D, rapm as R

    des = D.build(_stint_rows())
    f = R.fit(R.normal_equations(des), hyper=R.Hyper(newcomer_s=2000.0))
    p = f.players
    o1 = p.filter((pl.col("side") == "O") & (pl.col("player_id") == 1))["mean"][0]
    d99 = p.filter((pl.col("side") == "D") & (pl.col("player_id") == 99))["mean"][0]
    assert o1 > 0.6 and d99 > 0.3
    offence = p.filter(pl.col("side") == "O")
    assert abs((offence["mean"] * offence["toi_s"]).sum() / offence["toi_s"].sum()) < 1e-6


def test_defence_newcomers_get_position_prior_means():
    from nhl.ratings import design as D, rapm as R

    des = D.build(_stint_rows())
    normal = R.normal_equations(des)
    hyper = R.Hyper(newcomer_off_d=-0.25, newcomer_def_d=0.12)
    _, mean, _ = R._penalties(normal, None, hyper, frozenset({99}))
    by_col = dict(zip(normal.columns, mean))
    assert by_col["O:99"] == -0.25 and by_col["D:99"] == 0.12
    assert by_col["O:1"] == 0.0  # not a defenceman
    prior = pl.DataFrame({"player_id": [99], "side": ["O"], "mean": [0.3], "precision": [1e5]})
    _, mean, _ = R._penalties(normal, prior, hyper, frozenset({99}))
    assert dict(zip(normal.columns, mean))["O:99"] == 0.3  # an existing prior wins


def test_zone_shares_split_duration_by_second():
    from nhl.ratings.design import _zone_shares

    rows = pl.DataFrame({"start_s": [100, 140], "last_faceoff_s": [98, 100], "duration_s": [4, 10]})
    shares = _zone_shares(rows)
    assert np.allclose(shares[0, 2:6], 0.25) and shares[0].sum() == 1.0
    assert shares[1].sum() == 0.0  # 40-50 s after the faceoff: beyond the window


def test_age_prior_shifts_means():
    from nhl.ratings import rapm as R

    curve = R.AgeCurve(coef={"O": (0.0, -0.01, 0.0), "D": (0.0, 0.0, 0.0)})
    prior = pl.DataFrame({"player_id": [1, 2], "side": ["O", "O"], "mean": [0.0, 0.0], "precision": [1.0, 1.0]})
    ages = pl.DataFrame({"player_id": [1, 2], "age": [22.0, 32.0]})
    shifted = R.age_prior(prior, ages, curve).sort("player_id")["mean"].to_list()
    assert shifted[0] > 0 > shifted[1]
