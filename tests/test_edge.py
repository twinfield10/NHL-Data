"""NHL EDGE normalization and snapshotting on real (trimmed) 2025-26 payloads."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import requests

from nhl.sources import edge
from nhl.storage import keys

FIXTURES = Path(__file__).parent / "fixtures" / "edge"
CAPTURED = datetime(2026, 10, 5, 18, 30, tzinfo=timezone.utc)


def bundle(kind: str) -> dict[str, Any]:
    """Raw bundle fixture (McDavid 8478402, Vasilevskiy 8476883, Tampa Bay 14; 2025-26)."""
    return json.loads((FIXTURES / f"bundle_{kind}_20252026.json").read_text())


def regular(frame: pl.DataFrame) -> dict[str, Any]:
    """The regular-season row as a dict."""
    return frame.filter(pl.col("game_type") == 2).row(0, named=True)


class MemoryStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store` (JSON + parquet only)."""

    def __init__(self) -> None:
        self.objects: dict[str, Any] = {}

    def put_json_gz(self, key: str, obj: Any) -> str:
        self.objects[key] = json.loads(json.dumps(obj))
        return "etag"

    def get_json_gz(self, key: str) -> Any | None:
        return self.objects.get(key)

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        buf = io.BytesIO()
        df.write_parquet(buf)
        self.objects[key] = buf.getvalue()
        return "etag"

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        data = self.objects.get(key)
        return None if data is None else pl.read_parquet(io.BytesIO(data))

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))


class FakeClient:
    """Serves the fixture payloads by URL; anything else is a 404 like the real API."""

    def __init__(self, bundles: dict[tuple[str, int], dict[str, Any]]) -> None:
        self.routes: dict[str, Any] = {}
        for (kind, entity_id), b in bundles.items():
            for gt, payloads in b["game_types"].items():
                for name, payload in payloads.items():
                    self.routes[f"{edge.EDGE_API}/{name}/{entity_id}/{b['season']}/{gt}"] = payload
        self.calls: list[str] = []

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        self.calls.append(url)
        if url not in self.routes:
            resp = requests.Response()
            resp.status_code = 404
            raise requests.HTTPError("404", response=resp)
        return self.routes[url]


# --------------------------------------------------------------------------- naming
@pytest.mark.parametrize("raw, expected", [
    ("burstsOver22", "bursts_over_22"),
    ("bursts20To22", "bursts_20_to_22"),
    ("distancePer60", "distance_per_60"),
    ("L Net Side", "l_net_side"),
    ("savePctg5v5Details", "save_pctg_5v5_details"),
    ("offensiveZoneStartsPctgPercentile", "offensive_zone_starts_pctg_percentile"),
])
def test_snake(raw, expected):
    assert edge.snake(raw) == expected


def test_available_game_types_reads_seasons_with_edge_stats():
    detail = bundle("skater")["game_types"]["2"]["skater-detail"]
    assert edge.available_game_types(detail, 20252026) == [2, 3]
    assert edge.available_game_types(detail, 20262027) == [2]
    assert edge.available_game_types(detail, 20202021) == []


# --------------------------------------------------------------------------- normalize
def test_skater_bundle_flattens_all_endpoints():
    frame = edge.normalize_bundle("skater", bundle("skater"))
    assert frame.height == 2 and sorted(frame["game_type"].to_list()) == [2, 3]
    r = regular(frame)
    assert (r["player_id"], r["season"], r["team"], r["position"]) == (8478402, 20252026, "EDM", "C")
    assert (r["games_played"], r["goals"], r["assists"], r["points"]) == (82, 48, 90, 138)
    assert r["speed_max_skating_speed"] == pytest.approx(24.6119)
    assert r["speed_max_skating_speed_percentile"] == pytest.approx(0.9967)
    assert r["speed_bursts_over_22"] == 151 and r["speed_bursts_20_to_22"] == 530
    assert r["shot_speed_avg_shot_speed"] == pytest.approx(48.3919)
    assert r["shot_speed_top_shot_speed"] == pytest.approx(82.05)
    assert r["distance_all_distance_total"] == pytest.approx(330.2671)
    assert r["distance_es_distance_per_60"] == pytest.approx(10.6454)
    assert r["zone_pp_offensive_zone_pctg"] == pytest.approx(0.63524141)
    assert r["zone_all_defensive_zone_pctg"] == pytest.approx(0.35392959)
    assert r["zone_starts_offensive_zone_starts_pctg"] == pytest.approx(0.4407)
    assert r["area_low_slot_sog"] == 97 and r["area_behind_the_net_sog"] == 11
    assert r["loc_high_goals"] == 26 and r["loc_all_sog"] == 306
    assert r["captured_at"] == CAPTURED
    # League averages, metric units and per-game lists are not stored.
    assert not [c for c in frame.columns if "league" in c or "metric" in c or "last_10" in c or "hardest" in c]
    assert all(frame.schema[c] == pl.Float64 for c in edge.metric_columns(frame, "skater"))


def test_goalie_bundle_keeps_detail_stats_and_shot_locations():
    r = regular(edge.normalize_bundle("goalie", bundle("goalie")))
    assert (r["player_id"], r["team"], r["games_played"], r["wins"], r["losses"], r["ot_losses"]) == (
        8476883, "TBL", 58, 39, 15, 4)
    assert r["goalie_goals_against_avg"] == pytest.approx(2.30853)
    assert r["goalie_goal_differential_per_60"] == pytest.approx(1.08431)
    assert r["svpct_games_above_900"] == 35
    assert r["sv5v5_save_pctg"] == pytest.approx(0.9126)
    assert r["sv5v5_shots_per_60"] == pytest.approx(24.722)
    assert r["loc_all_save_pctg"] == pytest.approx(0.91234)
    assert r["loc_high_goals_against"] == 65
    assert r["area_crease_save_pctg"] == pytest.approx(0.836364)
    assert r["area_r_corner_saves"] is None  # null in the API, kept null


def test_team_bundle_labels_strength_and_position():
    frame = edge.normalize_bundle("team", bundle("team"))
    r = regular(frame)
    assert (r["team_id"], r["team"], r["games_played"], r["points"]) == (14, "TBL", 82, 106)
    assert r["speed_f_bursts_over_22"] == 90 and r["speed_all_bursts_over_22"] == 95
    assert r["distance_es_d_distance_per_60"] == pytest.approx(8.5612)
    assert r["distance_pk_f_distance_per_60_rank"] == 6
    assert r["zone_all_offensive_zone_rank"] == 13
    assert r["shot_diff_shot_attempt_differential"] == pytest.approx(1.414634)
    playoffs = frame.filter(pl.col("game_type") == 3).row(0, named=True)
    assert (playoffs["games_played"], playoffs["wins"], playoffs["losses"]) == (7, 3, 4)


# --------------------------------------------------------------------------- fetch / snapshot
def test_fetch_bundle_uses_detail_for_availability_and_tolerates_404():
    client = FakeClient({("skater", 8478402): bundle("skater")})
    b = edge.fetch_bundle(client, "skater", 8478402, 20252026, CAPTURED)
    assert sorted(b["game_types"]) == ["2", "3"]
    assert len(client.calls) == 12  # 6 endpoints x 2 game types, nothing wasted
    missing = edge.fetch_bundle(client, "skater", 8400000, 20252026, CAPTURED)
    assert missing["game_types"] == {}
    assert len(client.calls) == 14  # regular + playoff detail tried, both 404


def test_snapshot_is_resumable_and_change_only(monkeypatch):
    store = MemoryStore()
    client = FakeClient({("goalie", 8476883): bundle("goalie")})
    monkeypatch.setattr(edge, "utcnow", lambda: CAPTURED)
    written = edge.snapshot_season(store, "goalie", 20252026, client, current=False, ids=[8476883, 8400000])
    assert written == 2
    assert "raw/external/edge/goalie/20252026/8476883.json.gz" in store.objects
    calls = len(client.calls)
    again = edge.snapshot_season(store, "goalie", 20252026, client, current=False, ids=[8476883, 8400000])
    assert again == 0 and len(client.calls) == calls  # both ids cached, nothing refetched
    table = store.get_parquet(keys.edge("goalie", 20252026))
    assert table.height == 2 and set(table["game_type"].to_list()) == {2, 3}

    # In-progress season: a later day refetches into a dated prefix; unchanged values add no rows.
    later = datetime(2026, 10, 6, 18, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(edge, "utcnow", lambda: later)
    store.objects.pop(keys.edge("goalie", 20252026))
    assert edge.snapshot_season(store, "goalie", 20252026, client, current=True, ids=[8476883]) == 2
    assert "raw/external/edge/goalie/20252026/2026-10-06/8476883.json.gz" in store.objects
    monkeypatch.setattr(edge, "utcnow", lambda: datetime(2026, 10, 7, 18, 0, tzinfo=timezone.utc))
    assert edge.snapshot_season(store, "goalie", 20252026, client, current=True, ids=[8476883]) == 0


def test_latest_edge_keeps_most_recent_snapshot():
    first = edge.normalize_bundle("skater", bundle("skater"))
    moved = first.with_columns(
        captured_at=pl.lit(datetime(2026, 10, 6, tzinfo=timezone.utc), dtype=first.schema["captured_at"]),
        speed_bursts_over_22=pl.col("speed_bursts_over_22") + 1,
    )
    latest = edge.latest_edge(pl.concat([moved, first]), "skater")
    assert latest.height == 2
    assert regular(latest)["speed_bursts_over_22"] == 152
