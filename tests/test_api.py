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


class _StubData:
    """Just enough of :class:`SiteData` for the context routes."""

    def __init__(self, tables: dict):
        self.tables = tables

    def rankings(self, day):
        return {"season": 20252026, "players": pl.DataFrame({"player_id": [2], "ev_net": [0.3]})}

    def games(self):
        return pl.DataFrame({"season": [20252026], "home_team_id": [1], "home_abbr": ["PIT"]})

    def player_names(self):
        return {1: "Skater One", 2: "Skater Two"}

    def processed(self, key, current):
        return self.tables.get(key)

    def view(self, key, day=None):
        return None


def _client(tables: dict, monkeypatch):
    from fastapi.testclient import TestClient

    from nhl.api.deps import get_data
    from nhl.api.main import app

    monkeypatch.setitem(app.dependency_overrides, get_data, lambda: _StubData(tables))
    return TestClient(app)


def test_player_context_route_returns_parts_usage_and_linemates(monkeypatch):
    season = 20252026
    summary = pl.DataFrame([{
        "season": season, "player_id": 1, "team_id": 1, "group": "F", "games": 10, "toi_s": 9000.0,
        **{f"{p}_{s}": v for p, v in (("own", 0.2), ("mates", 0.1), ("comp", -0.05), ("zone", 0.0), ("ctx", 0.0),
                                       ("resid", 0.05), ("actual", 2.9), ("league", 2.6)) for s in ("f",)},
        **{f"{p}_a": 0.0 for p in ("own", "mates", "comp", "zone", "ctx", "resid")}, "actual_a": 2.6, "league_a": 2.6,
        "qot_net": 0.01, "qoc_net": 0.02, "qot_net_pct": 0.7, "qoc_net_pct": 0.9, "qot_toi": 0.3, "qoc_toi": 0.31,
    }])
    usage = pl.DataFrame({"season": [season], "player_id": [1], "team_id": [1], "games_F1": [8], "tier_mode": ["F1"]})
    mates = pl.DataFrame({"team_id": [1], "player_id": [1], "mate_id": [2], "shared_s": [4500], "games": [9]})
    client = _client({keys.onice_context_summary(season): summary, keys.usage_summary(season): usage, keys.linemates(season): mates}, monkeypatch)
    body = client.get("/api/ratings/players/1/context?date=2026-01-01").json()
    row = body["rows"][0]
    assert row["team_abbr"] == "PIT" and row["usage"]["games_F1"] == 8
    assert abs(row["parts"]["own"]["d"] - 0.2) < 1e-12 and abs(row["parts"]["actual"]["d"] - 0.3) < 1e-9
    assert row["linemates"] == [{"player_id": 2, "player_name": "Skater Two", "shared_s": 4500, "share": 0.5, "games": 9, "ev_net": 0.3}]


def test_team_matchups_route_rejects_other_seasons_and_handles_no_data(monkeypatch):
    client = _client({}, monkeypatch)
    assert client.get("/api/ratings/teams/1/matchups?season=20102011&date=2026-01-01").status_code == 400
    body = client.get("/api/ratings/teams/1/matchups?date=2026-01-01").json()
    assert body["cells"] == [] and body["index"] is None


def _archetypes(season: int) -> pl.DataFrame:
    base = {"season": season, "group": "F", "toi_5v5_min": 900.0, "archetype": "skill winger", "confidence": 0.8,
            "p_skill_winger": 0.8, "p_two_way_centre": 0.2,
            **{a: 0.5 for a in ("perimeter", "shooter", "release", "physical", "size", "defensive", "centre")},
            **{f"{a}_pct": 70.0 for a in ("perimeter", "shooter", "release", "physical", "size", "defensive", "centre")},
            "comps": [{"player_id": 2, "season": 20182019, "distance": 0.4}]}
    return pl.DataFrame([{**base, "player_id": 1, "window": w} for w in ("season", "2yr")])


def test_player_style_route_returns_axes_probs_comps_and_history(monkeypatch):
    tables = {keys.archetypes(20252026): _archetypes(20252026), keys.archetypes(20242025): _archetypes(20242025)}
    body = _client(tables, monkeypatch).get("/api/ratings/players/1/style?date=2026-01-01").json()
    cur = body["views"][0]
    assert cur["window"] == "2yr" and cur["archetype"] == "skill winger" and cur["reliable"]
    assert [a["key"] for a in cur["axes"]][:2] == ["perimeter", "shooter"] and cur["axes"][0]["pct"] == 70.0
    assert cur["probs"][0] == {"name": "skill winger", "p": 0.8} and cur["probs"][1]["name"] == "two-way centre"
    assert cur["comps"] == [{"player_id": 2, "player_name": "Skater Two", "season": 20182019, "distance": 0.4}]
    assert body["views"][1]["window"] == "season" and [h["season"] for h in body["history"]] == [20242025, 20252026]


def test_archetype_lookup_falls_back_to_previous_season():
    from nhl.api.routers.style import archetype_lookup

    stub = _StubData({keys.archetypes(20242025): _archetypes(20242025)})
    assert archetype_lookup(stub, 20252026)[1] == {"archetype": "skill winger", "archetype_conf": 0.8, "style_group": "F"}
    assert archetype_lookup(_StubData({}), 20252026) == {}


def test_timed_get_serves_stale_and_refreshes_in_background():
    import time

    data = SiteData(FakeStore({}))
    calls = []

    def load():
        calls.append(1)
        return len(calls)

    assert data._timed_get("k", 0.0, load, stale_ok=True) == 1  # nothing cached: blocks
    assert data._timed_get("k", 0.0, load, stale_ok=True) == 1  # expired: stale value now
    for _ in range(100):
        if data._timed.get("k", (0, None))[1] == 2:
            break
        time.sleep(0.01)
    assert data._timed["k"][1] == 2  # refreshed in the background
    assert data._timed_get("k", 0.0, load) == 3  # without stale_ok an expired value blocks


def _site_store(manifest_day: str, snapshot: str) -> FakeStore:
    from nhl.site import tables as T

    objs = {f"ratings/{snapshot}/{k}.parquet": pl.DataFrame({"x": [1]}) for k in ("ev", "st", "finishing", "penalties")}
    objs.update({f"{T.PREFIX}{n}.parquet": pl.DataFrame({"board": [n]}) for n in T.BOARDS})
    objs[T.units_key(20262027)] = pl.DataFrame({"unit": ["precomputed"]})
    objs[T.MANIFEST] = json.dumps({
        "day": manifest_day, "snapshot": snapshot, "as_of": "2026-10-08T13:00:00+00:00", "season": 20262027,
        "league": {"xg60_5v5": 2.5, "xg60_pp": 6.5}, "unit_seasons": [20262027], "built_at": "2026-10-08T13:00:05+00:00",
    }).encode()
    return FakeStore(objs)


def test_rankings_and_units_come_from_site_tables_when_current(monkeypatch):
    from nhl.site import tables as T

    data = SiteData(_site_store("2026-10-08", "2026-10-08"))
    monkeypatch.setattr(data, "rating_snapshot", lambda day: date(2026, 10, 8))
    monkeypatch.setattr(T, "build_rankings", lambda *a: (_ for _ in ()).throw(AssertionError("built live")))
    r = data.rankings(date(2026, 10, 8))
    assert r["teams"]["board"][0] == "teams" and r["season"] == 20262027 and r["snapshot"] == date(2026, 10, 8)
    assert data.units(20262027, True)["unit"][0] == "precomputed"


def test_rankings_build_live_when_site_tables_are_stale(monkeypatch):
    from nhl.site import tables as T

    data = SiteData(_site_store("2026-10-07", "2026-10-07"))
    monkeypatch.setattr(data, "rating_snapshot", lambda day: date(2026, 10, 8))
    monkeypatch.setattr(data, "games", lambda: pl.DataFrame())
    monkeypatch.setattr(T, "build_rankings", lambda store, day, snap, games: {"live": True, "snapshot": snap})
    assert data.rankings(date(2026, 10, 8)) == {"live": True, "snapshot": date(2026, 10, 8)}
