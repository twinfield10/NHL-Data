from __future__ import annotations

import polars as pl
import pytest

from nhl.features.fatigue import FATIGUE_FEATURES, add_fatigue_features

G = 2023020010  # game under test; earlier ids are history for shift norms
D1, F1, SHOOTER, D_NEW = 11, 12, 21, 13


def shift(game, player, start, end, period=1, team=1, goalie=False):
    return dict(game_id=game, season=20232024, player_id=player, team_id=team, period=period,
                start=start, end=end, is_goalie=goalie)


def event(idx, t, etype, home_ids, away_ids, reason=None, x=None):
    return dict(game_id=G, event_idx=idx, period=1, period_seconds=t, game_seconds=t, event_type=etype,
                reason=reason, x=x, home_attacks_pos=True, home_skater_ids=home_ids, away_skater_ids=away_ids,
                home_team_id=1, away_team_id=2)


@pytest.fixture
def players():
    return pl.DataFrame({"player_id": [D1, F1, SHOOTER, D_NEW], "position": ["D", "C", "L", "D"]})


@pytest.fixture
def shifts():
    history = [shift(g, D1, 0, 40) for g in (2023020001, 2023020002, 2023020003)]  # D1's norm: 40 s
    today = [
        shift(G, D1, 0, 40), shift(G, D1, 100, 200),   # 60 s rest, then on from 100
        shift(G, F1, 120, 180),                        # first shift of the period: no rest value
        shift(G, SHOOTER, 130, 170, team=2),
        shift(G, 99, 0, 1200, goalie=True),            # goalies are ignored
    ]
    return pl.DataFrame(history + today)


@pytest.fixture
def events():
    return pl.DataFrame([
        event(5, 100, "STOPPAGE", [D1], [SHOOTER], reason="icing"),
        event(6, 100, "FACEOFF", [D1], [SHOOTER], x=-69.0),          # home defensive zone (home attacks +x)
        event(10, 115, "SHOT", [D1], [SHOOTER]),                      # same defenders -> trap
        event(11, 125, "SHOT", [D1, D_NEW], [SHOOTER]),               # a change happened -> no trap
        event(12, 150, "SHOT", [D1, F1], [SHOOTER]),
    ], infer_schema_length=None)


def shots_for(idxs):
    return pl.DataFrame({"game_id": [G] * len(idxs), "event_idx": idxs, "season": [20232024] * len(idxs),
                         "shooter_id": [SHOOTER] * len(idxs), "is_home": [0.0] * len(idxs)})


def row(df, idx):
    return df.filter(pl.col("event_idx") == idx).row(0, named=True)


def test_current_shift_rest_norm_and_toi(events, shifts, players):
    out = add_fatigue_features(shots_for([12]), events, shifts, players)
    r = row(out, 12)
    assert r["def_max_shift_secs_d"] == 50       # D1 on since 100, shot at 150
    assert r["def_max_shift_secs_f"] == 30       # F1 on since 120
    assert r["def_shift_vs_norm_max"] == pytest.approx(50 / 40)  # F1 has no history -> only D1 counts
    assert r["def_rest_before_shift_min"] == 60  # D1 rested 40 -> 100; F1's first shift is null
    assert r["def_toi_last5"] == 90              # D1: 40 s + 50 s inside [-150, 150]
    assert r["shooter_shift_secs"] == 20
    assert set(FATIGUE_FEATURES) <= set(out.columns)
    assert all(out.schema[c] == pl.Float32 for c in FATIGUE_FEATURES)


def test_icing_trap_positive_and_line_change_negative(events, shifts, players):
    out = add_fatigue_features(shots_for([10, 11]), events, shifts, players)
    assert row(out, 10)["icing_trap"] == 1.0
    assert row(out, 11)["icing_trap"] == 0.0


def test_icing_trap_needs_icing_and_window(events, shifts, players):
    not_icing = events.with_columns(pl.when(pl.col("event_idx") == 5).then(pl.lit("offside")).otherwise(pl.col("reason")).alias("reason"))
    assert row(add_fatigue_features(shots_for([10]), not_icing, shifts, players), 10)["icing_trap"] == 0.0
    late = events.with_columns(pl.when(pl.col("event_idx") == 10).then(140).otherwise(pl.col("game_seconds")).alias("game_seconds"))
    assert row(add_fatigue_features(shots_for([10]), late, shifts, players), 10)["icing_trap"] == 0.0


def test_nulls_without_shift_data(events, players):
    no_lists = events.with_columns(pl.lit(None, pl.List(pl.Int64)).alias("home_skater_ids"),
                                   pl.lit(None, pl.List(pl.Int64)).alias("away_skater_ids"))
    empty = pl.DataFrame(schema={"game_id": pl.Int64, "season": pl.Int32, "player_id": pl.Int64, "team_id": pl.Int64,
                                 "period": pl.Int64, "start": pl.Int64, "end": pl.Int64, "is_goalie": pl.Boolean})
    out = add_fatigue_features(shots_for([12]), no_lists, empty, players)
    assert all(row(out, 12)[c] is None for c in FATIGUE_FEATURES)


@pytest.mark.parametrize("missing", [None, "empty"])
def test_missing_shifts_returns_null_columns(events, players, missing):
    shifts = None if missing is None else pl.DataFrame()
    out = add_fatigue_features(shots_for([10, 12]), events, shifts, players)
    assert out.height == 2
    assert all(out.schema[c] == pl.Float32 and out[c].null_count() == 2 for c in FATIGUE_FEATURES)


def test_games_without_shift_rows_are_null(events, shifts, players):
    other = pl.DataFrame({"game_id": [G + 1], "event_idx": [1], "season": [20232024], "shooter_id": [SHOOTER], "is_home": [0.0]})
    out = add_fatigue_features(pl.concat([shots_for([12]), other]), events, shifts, players)
    assert all(row(out, 1)[c] is None for c in FATIGUE_FEATURES)
    assert row(out, 12)["def_max_shift_secs_d"] == 50
