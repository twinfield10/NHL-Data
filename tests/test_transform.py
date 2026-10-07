from __future__ import annotations

import math

import polars as pl
import pytest

from conftest import A_GOALIE, A_SKATER, AWAY, H_D, H_GOALIE, H_SKATER
from nhl.transform.events import home_attack_direction, parse_events, roster_from_pbp
from nhl.transform.shifts import on_ice, parse_shifts


def by_idx(df: pl.DataFrame, idx: int) -> dict:
    return df.filter(pl.col("event_idx") == idx).row(0, named=True)


def test_running_score_counts_goals_before_event(raw_pbp):
    ev = parse_events(raw_pbp)
    assert (by_idx(ev, 2)["home_score"], by_idx(ev, 2)["away_score"]) == (0, 0)
    # The goal itself is scored at the pre-goal score.
    assert (by_idx(ev, 3)["home_score"], by_idx(ev, 3)["away_score"]) == (0, 0)
    assert (by_idx(ev, 4)["home_score"], by_idx(ev, 4)["away_score"]) == (1, 0)
    assert (by_idx(ev, 8)["home_score"], by_idx(ev, 8)["away_score"]) == (1, 0)


def test_direction_inferred_and_coordinates_normalized(raw_pbp):
    ev = parse_events(raw_pbp)
    assert by_idx(ev, 2)["direction_source"] == "shots"
    assert by_idx(ev, 2)["home_attacks_pos"] is True
    # Every shot attempt is in its own offensive zone after normalization.
    shots = ev.filter(pl.col("event_type").is_in(["SHOT", "MISSED_SHOT", "GOAL", "BLOCKED_SHOT"]))
    assert (shots["x_abs"] > 0).all()
    away_shot = by_idx(ev, 7)
    assert away_shot["x_abs"] == 65 and away_shot["y_abs"] == -20
    assert away_shot["event_distance"] == pytest.approx(math.hypot(89 - 65, 20))


def test_defending_side_field_wins_over_inference():
    ev = pl.DataFrame({
        "period": [1, 1], "home_defending_side": ["right", "right"],
        "event_type": ["SHOT", "SHOT"], "is_home_event": [True, True], "x": [70.0, 60.0],
    })
    out = home_attack_direction(ev).row(0, named=True)
    assert out["home_attacks_pos"] is False and out["direction_source"] == "api"


def test_direction_parity_fallback_for_period_without_shots():
    ev = pl.DataFrame({
        "period": [1, 1, 2], "home_defending_side": [None, None, None],
        "event_type": ["SHOT", "SHOT", "FACEOFF"], "is_home_event": [True, False, True], "x": [70.0, -70.0, 0.0],
    })
    out = {r["period"]: r for r in home_attack_direction(ev).iter_rows(named=True)}
    assert out[1]["home_attacks_pos"] is True
    assert out[2]["home_attacks_pos"] is False and out[2]["direction_source"] == "parity"


def test_blocked_shot_attributed_to_shooting_team(raw_pbp):
    blocked = by_idx(parse_events(raw_pbp), 5)
    assert blocked["event_team_id"] == AWAY
    assert blocked["player_1_id"] == A_SKATER and blocked["player_2_id"] == H_D


def test_situation_code_strength(raw_pbp):
    ev = parse_events(raw_pbp)
    assert by_idx(ev, 7)["strength_state"] == "5v4"
    assert by_idx(ev, 8)["away_net_empty"] is True and by_idx(ev, 8)["away_skaters"] == 6


def test_shift_fragments_merge(raw_pbp, raw_shifts):
    shifts = parse_shifts(raw_shifts, roster_from_pbp(raw_pbp))
    h = shifts.filter(pl.col("player_id") == H_SKATER)
    assert h.height == 1 and h.row(0, named=True)["end"] == 20


def test_on_ice_boundary_convention(raw_pbp, raw_shifts):
    ev = parse_events(raw_pbp)
    ice = on_ice(ev, parse_shifts(raw_shifts, roster_from_pbp(raw_pbp)))
    goal = by_idx(ice, 3)       # t=12, H_D's shift starts at 12: not on for the goal
    faceoff = by_idx(ice, 4)    # t=12 faceoff: H_D is coming on
    assert H_D not in goal["home_skater_ids"] and H_SKATER in goal["home_skater_ids"]
    assert H_D in faceoff["home_skater_ids"]
    assert goal["home_goalie_id"] == H_GOALIE and goal["away_goalie_id"] == A_GOALIE
    assert by_idx(ice, 8)["away_goalie_id"] is None


def test_on_ice_empty_without_shifts(raw_pbp):
    ice = on_ice(parse_events(raw_pbp), parse_shifts({"data": []}, roster_from_pbp(raw_pbp)))
    assert ice.is_empty() and "home_skater_ids" in ice.columns




def test_shift_rows_with_empty_times_are_dropped(raw_pbp, raw_shifts):
    raw_shifts["data"].append({"typeCode": 517, "duration": "00:30", "playerId": H_SKATER, "teamId": 1,
                               "period": 1, "startTime": "05:00", "endTime": ""})
    shifts = parse_shifts(raw_shifts, roster_from_pbp(raw_pbp))
    assert shifts.filter(pl.col("player_id") == H_SKATER).height == 1


def test_shots_override_contradicted_defending_side():
    ev = pl.DataFrame({
        "period": [1] * 5, "home_defending_side": ["right"] * 5,  # API says home attacks -x
        "event_type": ["SHOT"] * 5, "is_home_event": [True] * 5, "x": [70.0, 65.0, 80.0, 75.0, 60.0],
    })
    out = home_attack_direction(ev).row(0, named=True)
    assert out["home_attacks_pos"] is True and out["direction_source"] == "shots"
