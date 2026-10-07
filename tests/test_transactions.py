"""ESPN roster transactions: clause splitting, classification, player extraction, storage."""

from __future__ import annotations

import io
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from nhl.sources import transactions as tx
from nhl.sources.dailyfaceoff import PlayerResolver
from nhl.storage import keys

FIXTURES = Path(__file__).parent / "fixtures"
CAPTURED = datetime(2026, 10, 5, 18, 0, tzinfo=timezone.utc)


def page() -> dict[str, Any]:
    """Real ESPN transactions entries (trimmed team blocks), 2022-10 .. 2026-10."""
    return json.loads((FIXTURES / "transactions" / "espn_transactions.json").read_text())


@pytest.fixture
def frame() -> pl.DataFrame:
    """Fixture page normalized with a resolver: real TBL roster + a small league table."""
    roster = json.loads((FIXTURES / "lineups" / "roster_TBL.json").read_text())
    players = pl.DataFrame({
        "player_id": [8478402, 8475754, 8471215],
        "player_name": ["Connor McDavid", "Evan Rodrigues", "Evgeni Malkin"],
        "last_season": [20252026, 20252026, 20252026],
    })
    resolver = PlayerResolver(players=players, roster_loader=lambda team: roster if team == "TBL" else {})
    return tx.normalize_transactions(page(), CAPTURED, resolver)


def one(frame: pl.DataFrame, name: str, **filters: Any) -> dict[str, Any]:
    """The single row for a player (optionally narrowed by column filters)."""
    rows = frame.filter(pl.col("player_name") == name, *(pl.col(k) == v for k, v in filters.items()))
    assert rows.height == 1, rows
    return rows.row(0, named=True)


# --------------------------------------------------------------------------- text
def test_sentences_survive_abbreviations():
    text = ("Acquired two 2026 first-round picks (No. 15 and 29) from St. Louis Blues for F Mason McTavish. "
            "Reinstated F A.J. Greer.")
    assert tx.split_sentences(text) == [
        "Acquired two 2026 first-round picks (No. 15 and 29) from St. Louis Blues for F Mason McTavish.",
        "Reinstated F A.J. Greer.",
    ]


def test_clauses_split_on_and_plus_verb():
    assert tx.split_clauses("Sent D Matt Kiersted to Iowa (AHL) and recalled G Cal Petersen from Iowa.") == [
        "Sent D Matt Kiersted to Iowa (AHL)", "recalled G Cal Petersen from Iowa",
    ]


@pytest.mark.parametrize("clause, kind", [
    ("Recalled LW Yegor Pinchuk from Milwaukee (AHL)", "recalled"),
    ("Called up D Alexander Petrovic from Charlotte (AHL)", "recalled"),
    ("Loaned D Hoyt Stanley to Belleville (AHL)", "assigned"),
    ("Placed C Patrick Giles on waivers", "waived"),
    ("Waived for the purpose of assignment to Cleveland Ds Corson Ceulemans and Colton White", "waived"),
    ("Claimed D William Trudeau off waivers from New York", "claimed"),
    ("Acquired F Cruz Lucius from Pittsburgh for future considerations", "traded"),
    ("Agreed to terms with RW Nathan Bastian on a 1-year, two-way contract", "signed"),
    ("Moved C Max Domi from the non-roster list to long-term injured reserve", "placed_ir"),
    ("Reinstated D Victor Hedman from long-term injured reserve", "activated_ir"),
    ("Reinstated G Semyon Varlamov from conditioning assignment with Bridgeport (AHL)", "recalled"),
    ("Reinstated C Michael Rasmussen to the active roster from his two-game suspension", "other"),
    ("Suspended D Logan Stanley for one game", "suspended"),
    ("Announced the retirement of G Jack Campbell", "retired"),
    ("Fired head coach Adam Foote", "other"),
])
def test_classify(clause, kind):
    assert tx.classify(clause) == kind


def test_extract_players_handles_groups_and_oxford_commas():
    clause = "Loaned G Nolan Lalonde, Fs Oiva Keskinen and Luke Tuch, and Ds Charlie Elick and Guillaume Richard to Cleveland (AHL)"
    assert tx.extract_players(clause) == [
        ("Nolan Lalonde", "G"), ("Oiva Keskinen", "F"), ("Luke Tuch", "F"),
        ("Charlie Elick", "D"), ("Guillaume Richard", "D"),
    ]
    assert tx.extract_players("Recalled D Jack St. Ivany from Wilkes-Barre/Scranton") == [("Jack St. Ivany", "D")]
    assert tx.extract_players("Activated c Nico Hischier from injured reserve") == [("Nico Hischier", "C")]
    assert tx.extract_players("Reinstated Tomas Nosek from non-roster injured reserve", "activated_ir") == [
        ("Tomas Nosek", None)]
    assert tx.extract_players("Hired Peter DeBoer as new head coach", "other") == []


# --------------------------------------------------------------------------- normalize
def test_normalize_rows_and_types(frame):
    assert frame.height == 46
    assert frame["transaction_id"].n_unique() == 46
    counts = dict(frame.group_by("type").len().iter_rows())
    assert counts == {"assigned": 10, "recalled": 6, "placed_ir": 7, "traded": 6, "waived": 4,
                      "activated_ir": 4, "signed": 3, "other": 3, "claimed": 1, "suspended": 1, "retired": 1}
    assert set(frame["type"].unique()) <= set(tx.TYPES)
    assert set(frame["source"].unique()) == {"espn"}


def test_dates_teams_and_seasons(frame):
    r = one(frame, "Yegor Pinchuk")
    assert (r["date"], r["season"], r["team"], r["espn_team_id"], r["position"], r["type"]) == (
        date(2026, 10, 4), 20262027, "NSH", 27, "LW", "recalled")
    assert one(frame, "Patrick Giles")["team"] == "SJS"           # ESPN "SJ"
    assert one(frame, "Nico Hischier")["season"] == 20222023
    # June 30 is still the old season; seasons roll over July 1.
    assert one(frame, "Arseny Gritsyuk")["season"] == 20252026


def test_trades_record_direction(frame):
    nj = frame.filter(pl.col("type") == "traded", pl.col("team") == "NJD")
    assert dict(nj.select("player_name", "direction").iter_rows()) == {
        "Evan Rodrigues": "in", "Jesper Boqvist": "in", "Ben Steeves": "in",
        "Jacob Markstrom": "out", "Angus Crookshank": "out",
    }
    mctavish = one(frame, "Mason McTavish")
    assert (mctavish["team"], mctavish["direction"]) == ("ANA", "out")


def test_pronoun_reuses_previous_player(frame):
    mclaughlin = frame.filter(pl.col("player_name") == "Marc McLaughlin").sort("type")
    assert mclaughlin["type"].to_list() == ["activated_ir", "waived"]


def test_staff_moves_have_no_player(frame):
    nyi = frame.filter(pl.col("team") == "NYI", pl.col("type") == "other")
    assert nyi.height == 2 and nyi["player_name"].null_count() == 2


def test_player_ids_from_league_fallback(frame):
    assert one(frame, "Evan Rodrigues")["player_id"] == 8475754          # league fallback
    assert one(frame, "Evgeni Malkin")["player_id"] == 8471215
    assert one(frame, "Yegor Pinchuk")["player_id"] is None


# --------------------------------------------------------------------------- storage
class MemoryStore:
    """In-memory stand-in for the S3 store (JSON + parquet)."""

    def __init__(self) -> None:
        self.objects: dict[str, Any] = {}

    def put_json_gz(self, key: str, obj: Any) -> str:
        self.objects[key] = obj
        return "etag"

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        buf = io.BytesIO()
        df.write_parquet(buf)
        self.objects[key] = buf.getvalue()
        return "etag"

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        data = self.objects.get(key)
        return None if data is None else pl.read_parquet(io.BytesIO(data))


class FakeESPN:
    """Serves the fixture page for every request."""

    def __init__(self) -> None:
        self.params: list[dict[str, Any]] = []

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        self.params.append(params or {})
        return page()


def test_poll_is_idempotent_and_partitions_by_season():
    store = MemoryStore()
    resolver = PlayerResolver(players=None, roster_loader=lambda team: {})
    first = tx.poll_transactions(store, client=FakeESPN(), resolver=resolver)
    assert first == 46
    assert tx.poll_transactions(store, client=FakeESPN(), resolver=resolver) == 0
    sizes = {s: store.get_parquet(keys.transactions(s)).height for s in (20222023, 20252026, 20262027)}
    assert sizes == {20222023: 2, 20252026: 23, 20262027: 21}
    assert any(k.startswith("raw/external/espn/") and k.endswith("-transactions.json.gz") for k in store.objects)


def test_backfill_requests_calendar_years_and_filters_seasons():
    store = MemoryStore()
    client = FakeESPN()
    resolver = PlayerResolver(players=None, roster_loader=lambda team: {})
    written = tx.backfill_transactions(store, [2022], client=client, resolver=resolver)
    assert [p["season"] for p in client.params] == [2022, 2023]
    assert written == 2  # only the 2022-23 rows of the fixture
    assert store.get_parquet(keys.transactions(20252026)) is None
