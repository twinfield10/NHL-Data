"""Gold views: envelope and validity, the publisher's rebuild rules, the API serving stored bytes,
and the lineups lookups that replaced per-unit filters."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from nhl.api import lineupstats
from nhl.api.data import SiteData
from nhl.site import publish, views

UTC = timezone.utc
DAY = date(2026, 10, 9)
NOW = datetime(2026, 10, 9, 22, 0, tzinfo=UTC)  # 18:00 ET


class MemStore:
    """Bytes-only in-memory store."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.puts: list[str] = []

    def put_bytes(self, key, data, content_type=None):
        self.objects[key] = data
        self.puts.append(key)
        return "etag"

    def get_bytes(self, key):
        return self.objects.get(key)


def test_encode_meta_and_validity():
    until = NOW + timedelta(hours=1)
    raw = views.encode({"a": 1, "d": DAY}, until, DAY, now=NOW)
    body = json.loads(raw)
    assert body["a"] == 1 and body["d"] == "2026-10-09"
    m = views.meta(raw)
    assert m["v"] == views.VERSION and m["day"] == "2026-10-09"
    assert views.is_valid(m, NOW, DAY)
    assert not views.is_valid(m, until, DAY)  # puck dropped
    assert not views.is_valid(m, NOW, DAY + timedelta(days=1))  # built for another day
    assert not views.is_valid({**m, "v": views.VERSION - 1}, NOW)  # older payload shape
    assert views.meta(b"not json") == {}


def test_unit_index_matches_unit_record():
    units = pl.DataFrame({
        "kind": ["F", "F", "F", "D"], "player_ids": [[1, 2, 3], [3, 1, 2], [4, 5, 6], [7, 8]],
        "toi_s": [60.0, 40.0, 10.0, 30.0], "games": [1, 2, 1, 3], "xgf": [0.5, 0.1, 0.0, 0.2],
        "xga": [0.2, 0.1, 0.0, 0.1], "gf": [1, 0, 0, 0], "ga": [0, 1, 0, 0], "team_toi_s": [500.0, 500.0, 500.0, None],
    })
    idx = lineupstats.unit_index(units)
    for kind, ids in (("F", [2, 3, 1]), ("F", [6, 5, 4]), ("D", [8, 7])):
        assert idx[(kind, lineupstats.unit_key(ids))] == lineupstats.unit_record(units, kind, ids)
    assert ("D", lineupstats.unit_key([1, 2])) not in idx
    assert lineupstats.unit_index(None) == {}


def test_goalie_seasons_matches_goalie_season():
    starts = pl.DataFrame({"starter": [9, 9, 8, None], "shots_against": [30, 20, 25, 10], "goals_against": [2, 3, 1, 1],
                           "xga": [2.5, 2.0, 2.2, 1.0], "gsax": [0.5, -1.0, 1.2, 0.0]})
    out = lineupstats.goalie_seasons(starts)
    assert out[9] == lineupstats.goalie_season(starts, 9) and out[8] == lineupstats.goalie_season(starts, 8)
    assert None not in out


class _Data:
    """The bits of :class:`SiteData` the publisher touches."""

    def __init__(self, store):
        self.store = store

    def games(self):
        return pl.DataFrame({
            "game_id": [1, 2], "game_date": [DAY, DAY],
            "start_time_et": ["2026-10-09T17:00:00", "2026-10-09T19:00:00"],  # 21:00 and 23:00 UTC
        })


@pytest.fixture
def stub_builders(monkeypatch):
    calls: list[str] = []
    from nhl.api.routers import games, props, slate

    monkeypatch.setattr("nhl.api.data.SiteData", _Data)
    monkeypatch.setattr(slate, "build_slate", lambda data, day: calls.append("slate") or {"games": []})
    monkeypatch.setattr(props, "build_props", lambda data, day: calls.append("props") or {"props": []})
    monkeypatch.setattr(games, "build_game", lambda data, g: calls.append(f"game{g['game_id']}") or {})
    monkeypatch.setattr(games, "build_lineups", lambda data, g: calls.append(f"lineups{g['game_id']}") or {})
    monkeypatch.setattr(props, "build_game_props", lambda data, g: calls.append(f"gprops{g['game_id']}") or {})
    return calls


def test_publish_builds_everything_then_only_what_changed(stub_builders):
    store = MemStore()
    publish.publish_day(store, DAY, now=NOW)
    assert sorted(stub_builders) == sorted(["slate", "props", "game1", "game2", "lineups1", "lineups2", "gprops1", "gprops2"])
    # The slate goes stale when game 2 starts; game 1 already started, so its market tab is final.
    assert views.meta(store.objects[views.slate_key(DAY)])["valid_until"] == "2026-10-09T23:00:00+00:00"
    assert views.meta(store.objects[views.game_key(1, "game")])["valid_until"] is None
    assert views.meta(store.objects[views.game_key(2, "game")])["valid_until"] == "2026-10-09T23:00:00+00:00"

    stub_builders.clear()
    publish.publish_day(store, DAY, parts=[], now=NOW + timedelta(minutes=5))
    assert stub_builders == []  # nothing changed, nothing stale

    publish.publish_day(store, DAY, parts=["markets"], now=NOW + timedelta(minutes=10))
    assert stub_builders == ["slate", "game2"]  # started game 1 stays frozen

    stub_builders.clear()
    publish.publish_day(store, DAY, parts=[], now=NOW + timedelta(hours=1, minutes=5))
    assert stub_builders == ["slate", "game2", "gprops2"]  # game 2 started: rebuilt once at its close

    stub_builders.clear()
    publish.publish_day(store, DAY, parts=["lineups"], force=True, now=NOW + timedelta(hours=6))
    assert len(stub_builders) == 8


def test_publish_keeps_going_when_a_view_fails(stub_builders, monkeypatch):
    from nhl.api.routers import games

    def boom(data, g):
        raise RuntimeError("bad odds file")

    monkeypatch.setattr(games, "build_game", boom)
    store = MemStore()
    written = publish.publish_day(store, DAY, now=NOW)
    assert views.game_key(1, "game") not in written and views.game_key(1, "lineups") in written
    index = json.loads(store.objects[views.index_key(DAY)])
    assert views.game_key(1, "game") not in index  # retried next time


def test_site_data_serves_valid_views_only():
    store = MemStore()
    data = SiteData(store)
    key = views.ratings_key("players")
    store.objects[key] = views.encode({"skaters": []}, day=DAY)
    assert data.view(key, DAY) == store.objects[key]
    assert data.view(key, DAY + timedelta(days=1)) is None
    stale = views.game_key(1, "game")
    store.objects[stale] = views.encode({}, valid_until=datetime.now(UTC) - timedelta(minutes=1))
    assert data.view(stale) is None
    assert data.view(views.game_key(2, "game")) is None
