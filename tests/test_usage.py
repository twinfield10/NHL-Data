from __future__ import annotations

from datetime import date

import polars as pl

from nhl.usage.tiers import build_usage, summarize, validate, zone_starts

GAME, HOME, AWAY = 2025020001, 1, 2
FWD = list(range(101, 113))  # 12 home forwards, 101 most used
DEF = list(range(201, 207))  # 6 home defencemen


def _logs() -> pl.DataFrame:
    rows = []
    for i, p in enumerate(FWD + DEF):
        pos = "C" if p < 200 else "D"
        toi5 = 1000 - 30 * i if p < 200 else 1400 - 60 * (p - 201)
        for strength, toi in (("all", toi5 + 100), ("5v5", toi5), ("PP", 60 if p in (101, 102, 103, 201, 104) else 0),
                              ("SH", 0)):
            rows.append({"game_id": GAME, "season": 20252026, "game_date": date(2025, 10, 7), "player_id": p,
                         "team_id": HOME, "position": pos, "strength": strength, "toi_s": toi})
    return pl.DataFrame(rows)


def _lineups() -> pl.DataFrame:
    # Inference order isn't TOI order: 107-109 is reported first but plays less than 101-103.
    units = [("F", 1, [107, 108, 109], 0.9), ("F", 2, [101, 102, 103], 0.9), ("F", 3, [104, 105, 110], 0.4),
             ("D", 1, [201, 202], 0.9), ("D", 2, [203, 204], 0.9), ("D", 3, [205, 206], 0.9),
             ("PP", 1, [101, 102, 103, 104, 201], 1.0)]
    return pl.DataFrame([{"game_id": GAME, "team_id": HOME, "unit_type": t, "unit_rank": r, "members": m,
                          "confidence": c} for t, r, m, c in units])


def _stints() -> pl.DataFrame:
    base = {"game_id": GAME, "home_team_id": HOME, "away_team_id": AWAY, "home_goalie": 1, "away_goalie": 2,
            "valid_personnel": True, "home_n": 5, "away_n": 5}
    rows = [
        {**base, "strength_state": "5v5", "start_type": "faceoff_oz", "duration_s": 100,
         "home_skaters": [101, 102, 103, 201, 202], "away_skaters": [301, 302, 303, 401, 402]},
        {**base, "strength_state": "5v5", "start_type": "faceoff_dz", "duration_s": 100,
         "home_skaters": [101, 102, 103, 201, 202], "away_skaters": [301, 302, 303, 401, 402]},
        {**base, "strength_state": "5v5", "start_type": "faceoff_oz", "duration_s": 100,
         "home_skaters": [107, 108, 109, 203, 204], "away_skaters": [301, 302, 303, 401, 402]},
        {**base, "strength_state": "5v5", "start_type": "on_the_fly", "duration_s": 100,
         "home_skaters": [110, 111, 112, 205, 206], "away_skaters": [301, 302, 303, 401, 402]},
        {**base, "strength_state": "5v4", "start_type": "faceoff_oz", "duration_s": 60, "away_n": 4,
         "home_skaters": [101, 102, 103, 104, 201], "away_skaters": [301, 302, 401, 402]},
    ]
    return pl.DataFrame(rows)


def test_zone_starts_flip_for_the_away_team_and_skip_special_teams():
    z = zone_starts(_stints()).sort("player_id")
    home = z.filter(pl.col("player_id") == 101).row(0, named=True)
    assert (home["oz_starts"], home["dz_starts"]) == (1, 1)  # the 5v4 faceoff isn't counted
    away = z.filter(pl.col("player_id") == 301).row(0, named=True)
    assert (away["oz_starts"], away["dz_starts"]) == (1, 2)


def test_tiers_follow_ice_time_and_keep_confident_units_together():
    u = build_usage(_logs(), _lineups(), _stints())
    tier = dict(zip(u["player_id"], u["tier"]))
    # The most-used line is F1 even though inference reported it second.
    assert [tier[p] for p in (101, 102, 103)] == ["F1"] * 3
    # 107-109 rank 7th-9th individually; as a confident unit they stay together.
    assert [tier[p] for p in (107, 108, 109)] == ["F3"] * 3
    # The low-confidence unit (104, 105, 110) is ranked individually.
    assert u.filter(pl.col("player_id") == 110)["tier_source"].item() == "rank"
    assert [tier[p] for p in (201, 202, 203, 204, 205, 206)] == ["D1", "D1", "D2", "D2", "D3", "D3"]
    counts = u["tier"].value_counts()
    assert set(counts["count"].to_list()) == {3, 2}  # 3 per forward tier, 2 per pair
    assert u.filter(pl.col("player_id") == 104)["pp_unit"].item() == 1
    assert u.filter(pl.col("player_id") == 105)["pp_unit"].item() is None


def test_shares_and_summary():
    u = build_usage(_logs(), _lineups(), _stints())
    row = u.filter(pl.col("player_id") == 101).row(0, named=True)
    assert abs(row["share_5v5"] - 1000 / 400) < 1e-9  # team 5v5 time in the fixture is 400 s
    assert row["share_pp"] == 1.0 and row["oz_start_share"] == 0.5
    s = summarize(u)
    assert s.filter(pl.col("player_id") == 101)["tier_mode"].item() == "F1"
    assert validate(u)["tier_sizes_ok"] == 1.0
