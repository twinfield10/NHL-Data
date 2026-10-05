"""v2 feature groups 1-4 on a hand-built game (see docs/plans/m1-xg-features.md)."""

from __future__ import annotations

import math

import polars as pl
import pytest

from conftest import A_GOALIE, A_SKATER, AWAY, H_D, H_GOALIE, H_SKATER, HOME, play, spot
from nhl.features.shots import FEATURE_SETS, FEATURES, FEATURES_V2_EVENTS, add_geometry_features, build_shot_features
from nhl.transform.events import parse_events, roster_from_pbp
from nhl.transform.shifts import on_ice, parse_shifts

H_RELIEF = 198


@pytest.fixture
def v2_shots(players) -> pl.DataFrame:
    """Home attacks +x in period 1 (inferred from shots).

    idx1 t0  home wins OZ faceoff at (69, 22)
    idx2 t5  home SOG (70, 15)            -> first shot, goalie saw nothing yet
    idx3 t6  home miss (75, -10)          -> cross-ice from y=+15 one second earlier
    idx4 t6  home SOG (80, -8)            -> same second as idx3 (counts, lower idx); same side
    idx5 t20 away takeaway (-30, 5)
    idx6 t22 away SOG (-80, 10)           -> prev2/prev3 are home attempts (frame flipped)
    idx7 t600 away SOG (-70, -5) on a relief goalie entering cold
    """
    plays = [
        play(1, 1, "00:00", "faceoff", HOME, 69, 22, winningPlayerId=H_SKATER, losingPlayerId=A_SKATER),
        play(2, 1, "00:05", "shot-on-goal", HOME, 70, 15, shootingPlayerId=H_SKATER, goalieInNetId=A_GOALIE, shotType="wrist"),
        play(3, 1, "00:06", "missed-shot", HOME, 75, -10, shootingPlayerId=H_SKATER, goalieInNetId=A_GOALIE, shotType="wrist"),
        play(4, 1, "00:06", "shot-on-goal", HOME, 80, -8, shootingPlayerId=H_D, goalieInNetId=A_GOALIE, shotType="snap"),
        play(5, 1, "00:20", "takeaway", AWAY, -30, 5, playerId=A_SKATER),
        play(6, 1, "00:22", "shot-on-goal", AWAY, -80, 10, shootingPlayerId=A_SKATER, goalieInNetId=H_GOALIE, shotType="slap"),
        play(7, 1, "10:00", "shot-on-goal", AWAY, -70, -5, shootingPlayerId=A_SKATER, goalieInNetId=H_RELIEF, shotType="wrist"),
    ]
    pbp = {
        "id": 2024020002, "season": 20242025, "gameType": 2, "gameDate": "2024-10-09",
        "homeTeam": {"id": HOME, "abbrev": "HOM"}, "awayTeam": {"id": AWAY, "abbrev": "AWY"},
        "plays": plays,
        "rosterSpots": [spot(H_SKATER, HOME, "C"), spot(H_D, HOME, "D"), spot(H_GOALIE, HOME, "G"),
                        spot(H_RELIEF, HOME, "G"), spot(A_SKATER, AWAY, "L"), spot(A_GOALIE, AWAY, "G")],
    }
    ev = parse_events(pbp)
    ev = ev.join(on_ice(ev, parse_shifts({"data": []}, roster_from_pbp(pbp))), on="event_idx", how="left")
    return build_shot_features(ev, players)


def row(df: pl.DataFrame, idx: int) -> dict:
    return df.filter(pl.col("event_idx") == idx).row(0, named=True)


def test_output_columns_and_types(v2_shots):
    assert v2_shots["event_idx"].to_list() == [2, 3, 4, 6, 7]
    assert set(FEATURE_SETS["v2"]) <= set(v2_shots.columns)
    assert FEATURE_SETS["v1"] == FEATURES
    assert all(v2_shots.schema[c] == pl.Float32 for c in FEATURES_V2_EVENTS + ["same_team_last"])


def test_cross_ice(v2_shots):
    r = row(v2_shots, 3)
    assert r["crossed_slot"] == 1 and r["crossed_slot_same_team"] == 1
    assert r["lateral_ft"] == 25 and r["lateral_speed"] == 25  # 1 s apart
    assert row(v2_shots, 4)["crossed_slot"] == 0               # y -10 -> -8, same side


def test_rolling_counts_and_same_second_ordering(v2_shots):
    assert [row(v2_shots, i)["attempts_for_10s"] for i in (2, 3, 4)] == [0, 1, 2]
    assert [row(v2_shots, i)["goalie_sog_10s"] for i in (2, 3, 4)] == [0, 1, 1]  # idx3 was a miss
    assert [row(v2_shots, i)["goalie_att_10s"] for i in (2, 3, 4)] == [0, 1, 2]
    assert [row(v2_shots, i)["goalie_sog_game"] for i in (2, 3, 4)] == [0, 1, 1]
    assert row(v2_shots, 2)["goalie_secs_since_save"] is None
    assert row(v2_shots, 3)["goalie_secs_since_save"] == 1


def test_faceoff_features(v2_shots):
    r = row(v2_shots, 4)
    assert r["secs_since_faceoff"] == 6 and r["faceoff_zone_oz"] == 1
    assert r["attempts_since_faceoff_for"] == 2 and r["attempts_since_faceoff_against"] == 0
    away = row(v2_shots, 6)  # home won that faceoff in home's zone -> away's defensive zone
    assert away["faceoff_zone_oz"] == 0 and away["attempts_since_faceoff_against"] == 3


def test_sequence_frames(v2_shots):
    r = row(v2_shots, 4)
    assert r["prev2_own_attempt"] == 1 and r["prev2_secs"] == 1
    assert (r["prev2_x_abs"], r["prev2_y_abs"]) == (70, 15)
    assert r["prev3_faceoff_won"] == 1
    a = row(v2_shots, 6)  # previous home attempts seen from the away shooter's frame
    assert a["prev2_opp_attempt"] == 1 and (a["prev2_x_abs"], a["prev2_y_abs"]) == (-80, 8)
    assert a["prev3_opp_attempt"] == 1 and (a["prev3_x_abs"], a["prev3_y_abs"]) == (-75, 10)


def test_relief_goalie_starts_fresh(v2_shots):
    r = row(v2_shots, 7)
    assert r["goalie_id"] == H_RELIEF
    assert r["goalie_sog_game"] == 0 and r["goalie_mins_in_game"] == 0
    assert row(v2_shots, 6)["goalie_sog_game"] == 0  # first shot H_GOALIE faced


def test_net_geometry():
    pts = pl.DataFrame({
        "x_abs": [79.0, 85.0, 92.0, 60.0, 60.0, 85.0, 40.0],
        "y_abs": [0.0, 20.0, 5.0, 20.0, 25.0, 10.0, 0.0],
    }).with_columns(pl.lit(None, pl.Float32).alias(c) for c in ("x_abs_last", "y_abs_last", "seconds_since_last", "same_team_last"))
    g = add_geometry_features(pts)
    assert g["net_angle_deg"][0] == pytest.approx(2 * math.degrees(math.atan(3 / 10)), abs=1e-4)
    assert g["net_angle_deg"][1] == pytest.approx(math.degrees(math.atan2(-17, 4) - math.atan2(-23, 4)), abs=1e-4)
    assert g["net_angle_deg"][2] == 0 and g["behind_net"][2] == 1
    assert g["dist_near_post"][0] == pytest.approx(math.hypot(10, 3), abs=1e-4)
    assert g["in_slot"].to_list() == [1, 0, 0, 1, 0, 0, 0]
    assert g["crossed_slot"].to_list() == [0] * 7  # no previous event
