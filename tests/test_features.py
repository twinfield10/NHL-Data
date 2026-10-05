from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from conftest import A_GOALIE, H_GOALIE, H_SKATER
from nhl import config
from nhl.features.shots import FEATURES, build_shot_features
from nhl.models.evaluate import evaluate
from nhl.transform.events import parse_events, roster_from_pbp
from nhl.transform.shifts import on_ice, parse_shifts


@pytest.fixture
def shots(raw_pbp, raw_shifts, players) -> pl.DataFrame:
    ev = parse_events(raw_pbp)
    ev = ev.join(on_ice(ev, parse_shifts(raw_shifts, roster_from_pbp(raw_pbp))), on="event_idx", how="left")
    return build_shot_features(ev, players)


def row(df: pl.DataFrame, idx: int) -> dict:
    return df.filter(pl.col("event_idx") == idx).row(0, named=True)


def test_one_row_per_unblocked_shot_with_all_features(shots):
    assert shots["event_idx"].to_list() == [2, 3, 6, 7, 8]
    assert set(FEATURES) <= set(shots.columns)


def test_goalie_is_the_defending_goalie(shots):
    assert row(shots, 2)["goalie_id"] == A_GOALIE          # home shot -> away goalie
    assert row(shots, 7)["goalie_id"] == H_GOALIE          # away shot -> home goalie
    assert row(shots, 7)["goalie_catches_r"] == 0          # H_GOALIE catches L


def test_score_diff_from_shooter_perspective(shots):
    assert row(shots, 6)["score_diff"] == -1               # away trails 0-1


def test_strength_groups(shots):
    assert row(shots, 2)["strength_group"] == "EV"
    assert row(shots, 7)["strength_group"] == "SH"        # situation 1451: away 4 vs home 5
    # Away goalie pulled: away is shooting with an extra attacker at a guarded net.
    assert row(shots, 8)["strength_group"] == "PP"
    assert row(shots, 8)["own_net_empty"] == 1


def test_previous_event_in_shooter_frame(shots):
    # Goal at idx 3 follows a same-team shot 2s earlier: a rebound.
    r = row(shots, 3)
    assert r["prior_shot_same"] == 1 and r["is_rebound"] == 1
    assert r["x_abs_last"] == 70 and r["seconds_since_last"] == 2
    # Missed shot at idx 6 follows the away team's own blocked attempt.
    assert row(shots, 6)["prior_block_same"] == 1


def test_shooter_attributes(shots):
    r = row(shots, 2)
    assert r["shooter_id"] == H_SKATER and r["shooter_shoots_r"] == 1 and r["shot_wrist"] == 1


def test_evaluate_rejects_log_odds():
    with pytest.raises(ValueError):
        evaluate(np.array([0, 1]), np.array([-2.0, 1.5]))


def test_evaluate_perfectly_calibrated_constant():
    y = np.array([0] * 90 + [1] * 10)
    m = evaluate(y, np.full(100, 0.1))
    assert m["goals_per_xg"] == pytest.approx(1.0)
    assert m["log_loss_skill"] == pytest.approx(0.0, abs=1e-9)


def test_parse_seasons():
    assert config.parse_seasons("2010-2012,2015") == [2010, 2011, 2012, 2015]
    assert config.season_id(2024) == 20242025


def test_season_weights_recency_and_symmetry():
    from nhl.models.xg import season_weights

    w = season_weights(np.array([20242025, 20232024, 20222023, 20252026]), 20242025, 1.0)
    assert w.tolist() == [1.0, 0.5, 0.25, 1.0]  # future seasons are not up-weighted beyond 1
    sym = season_weights(np.array([20132014, 20172018]), 20152016, 2.0, symmetric=True)
    assert sym[0] == pytest.approx(sym[1]) == pytest.approx(0.5)
