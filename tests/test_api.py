from __future__ import annotations

import json
from datetime import date

import polars as pl

from nhl.api.data import SiteData
from nhl.api.serialize import rows, selection, with_selection
from nhl.storage import keys


class FakeStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store`."""

    def __init__(self, objects: dict):
        self.objects = objects

    def get_parquet(self, key):
        return self.objects.get(key)

    def read_parquet_required(self, key):
        return self.objects[key]

    def get_bytes(self, key):
        return self.objects.get(key)

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]


def test_selection_labels():
    assert selection("moneyline", 2, None, "CHI", "STL") == "STL"
    assert selection("puckline", 1, 1.5, "SEA", "VGK") == "SEA +1.5"
    assert selection("puckline", 2, 1.5, "SEA", "VGK") == "VGK -1.5"
    assert selection("total", 1, 5.5, "CHI", "STL") == "Over 5.5"
    assert selection("total", 2, 6.0, "CHI", "STL") == "Under 6"


def test_with_selection_adds_column():
    df = pl.DataFrame({"market": ["moneyline", "total"], "side": [1, 2], "line": [None, 5.5],
                       "home_abbr": ["CHI", "CHI"], "away_abbr": ["STL", "STL"]})
    assert with_selection(df)["selection"].to_list() == ["CHI", "Under 5.5"]


def test_rows_turns_nan_into_null_and_handles_empty():
    df = pl.DataFrame({"a": [1.0, float("nan")], "b": ["x", "y"], "stamp": ["s", "s"]})
    assert rows(df, drop=("stamp",)) == [{"a": 1.0, "b": "x"}, {"a": None, "b": "y"}]
    assert rows(None) == [] and rows(pl.DataFrame()) == []


def test_day_slate_keeps_each_games_last_pregame_price():
    day = date(2026, 10, 6)
    early = pl.DataFrame({"game_id": [1, 2], "p_home_win": [0.50, 0.40], "stamp": ["T1", "T1"]})
    late = pl.DataFrame({"game_id": [2], "p_home_win": [0.45], "stamp": ["T2"]})  # game 1 has started
    store = FakeStore({
        keys.pregame_slate(day, "T1"): early,
        keys.pregame_slate(day, "T2"): late,
        keys.pregame_latest(day): json.dumps({"stamp": "T2"}).encode(),
    })
    data = SiteData(store)

    merged = data.day_slate(day).sort("game_id")
    assert merged["p_home_win"].to_list() == [0.50, 0.45]
    assert merged["stamp"].to_list() == ["T1", "T2"]
    assert data.latest_stamp(day) == "T2"
    assert data.slate(day)["game_id"].to_list() == [2]


def test_day_slate_without_runs_is_none():
    assert SiteData(FakeStore({})).day_slate(date(2026, 10, 8)) is None


def test_team_context_records_l10_streak_and_previous_season():
    from nhl.api.teaminfo import team_context

    def g(gid, season, d, home, away, hs, as_, period=3):
        return {"game_id": gid, "season": season, "season_type": "R", "game_date": d, "home_abbr": home, "away_abbr": away,
                "home_score": hs, "away_score": as_, "last_period": period, "is_final": True}

    games = pl.DataFrame([
        g(1, 20252026, date(2026, 3, 1), "FLA", "LAK", 3, 2),
        g(2, 20262027, date(2026, 10, 1), "FLA", "LAK", 2, 3, period=4),  # FLA OT loss
        g(3, 20262027, date(2026, 10, 3), "LAK", "FLA", 1, 4),
        g(4, 20262027, date(2026, 10, 5), "FLA", "LAK", 5, 0),
        g(5, 20262027, date(2026, 10, 7), "FLA", "LAK", 0, 1),  # on/after the day: excluded
    ])
    ctx = team_context(games, ["FLA", "LAK", "SEA"], date(2026, 10, 7), 20262027)
    assert ctx["FLA"]["record"] | {"pts_pct": None} == {"gp": 3, "w": 2, "l": 0, "otl": 1, "pts": 5, "pts_pct": None}
    assert ctx["FLA"]["streak"] == "W2" and ctx["LAK"]["streak"] == "L2"
    assert team_context(games, ["FLA"], date(2026, 10, 2), 20262027)["FLA"]["streak"] == "OT1"
    assert ctx["LAK"]["record"]["w"] == 1 and ctx["LAK"]["l10"]["l"] == 2
    assert ctx["FLA"]["prev_record"] is None
    assert ctx["SEA"]["record"]["gp"] == 0 and ctx["SEA"]["prev_record"] is None and ctx["SEA"]["name"] == "Kraken"
