"""The shared props table: typing, player-id resolution and transition storage (no network)."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from nhl.odds import props as P
from nhl.sources.dailyfaceoff import PlayerResolver

FIXTURES = Path(__file__).parent / "fixtures" / "props"
CAPTURED = datetime(2026, 10, 5, 18, 0, tzinfo=timezone.utc)
START = datetime(2026, 10, 5, 23, 7, tzinfo=timezone.utc)


def rosters() -> dict[str, Any]:
    """Trimmed NHL rosters (TBL: Kucherov, Point, Moser, Vasilevskiy; PHI: Konecny, Zegras ...)."""
    return json.loads((FIXTURES / "nhl_rosters.json").read_text())


@pytest.fixture
def resolver() -> PlayerResolver:
    """Resolver on the fixture rosters plus a one-row league table."""
    players = pl.DataFrame({"player_id": [8470000], "player_name": ["Retired Guy"], "last_season": [20152016]})
    return PlayerResolver(players=players, roster_loader=lambda team: rosters().get(team, {}))


@pytest.fixture
def games() -> pl.DataFrame:
    """PHI @ TBL on 2026-10-05."""
    return pl.DataFrame({"game_id": [2026020040], "game_date": [date(2026, 10, 5)],
                         "home_abbr": ["TBL"], "away_abbr": ["PHI"]})


def row(**kw: Any) -> dict[str, Any]:
    """One prop row with PHI @ TBL defaults."""
    base = {"book": "4Casters", "captured_at": CAPTURED, "start_time": START, "away_team": "Philadelphia Flyers",
            "home_team": "Tampa Bay Lightning", "player_name": "Nikita Kucherov", "team": None,
            "prop_type": "goals", "line": 0.5, "side": "over", "price": 164.0, "source_event_id": "e1"}
    return base | kw


class FakeStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store`."""

    def __init__(self) -> None:
        self.parquet: dict[str, pl.DataFrame] = {}

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        return self.parquet.get(key)

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        self.parquet[key] = df
        return key

    def list_keys(self, prefix: str) -> list[str]:
        return [k for k in self.parquet if k.startswith(prefix)]


def test_props_key_is_per_source() -> None:
    assert P.props_key(20262027, "espn") == "external/odds/props/20262027/espn.parquet"
    assert P.props_key(20262027, "fourcasters") != P.props_key(20262027, "espn")


@pytest.mark.parametrize(("threshold", "line"), [("1+", 0.5), ("3+", 2.5), ("22+", 21.5), (3, 2.5), ("x", None)])
def test_milestone_line(threshold: Any, line: float | None) -> None:
    assert P.milestone_line(threshold) == line


def test_props_frame_types_and_vocabulary() -> None:
    df = P.props_frame([
        row(),
        row(side="under", price=-374.0),
        row(prop_type="hat_tricks"),             # not in the vocabulary: dropped
        row(side="maybe"),                       # not a side: dropped
        row(price=170.0),                        # same key + event + time: the first row wins
        row(source_event_id="e2", price=150.0),  # same player, another game: kept
    ])
    assert df.schema == pl.Schema(P.PROPS_SCHEMA)
    assert df.height == 3
    assert set(df["home_team"]) == {"TBL"} and set(df["away_team"]) == {"PHI"}
    assert set(df["price_point"]) == {"live"}
    assert df.filter(pl.col("source_event_id") == "e1").filter(pl.col("side") == "over")["price"].to_list() == [164.0]
    assert P.props_frame([]).is_empty()


def test_resolve_player_infers_team(resolver: PlayerResolver) -> None:
    # No team from the book: the matchup's rosters decide (Zegras is on PHI, the away side).
    assert P.resolve_player(resolver, "Trevor Zegras", None, "TBL", "PHI")[1] == "PHI"
    pid, team = P.resolve_player(resolver, "Nikita Kucherov", None, "TBL", "PHI")
    assert team == "TBL" and pid == next(p["id"] for p in rosters()["TBL"]["forwards"]
                                         if p["lastName"]["default"] == "Kucherov")
    # Punctuation-insensitive ("J.J." vs "JJ").
    assert P.resolve_player(resolver, "Jj Moser", None, "TBL", "PHI")[1] == "TBL"
    # League-wide fallback leaves the team unknown; nobody at all gives None.
    assert P.resolve_player(resolver, "Retired Guy", None, "TBL", "PHI") == (8470000, None)
    assert P.resolve_player(resolver, "Nobody Here", None, "TBL", "PHI") == (None, None)
    # A known team uses the resolver's own order.
    assert P.resolve_player(resolver, "Travis Konecny", "PHI", "TBL", "PHI")[1] == "PHI"


def test_store_props_writes_transitions(resolver: PlayerResolver, games: pl.DataFrame) -> None:
    store = FakeStore()
    rows = [row(), row(side="under", price=-374.0), row(player_name="Trevor Zegras", prop_type="assists", price=194.0),
            row(player_name="Nobody Here", price=900.0),
            row(home_team="New York Rangers", away_team="New York Islanders")]  # no such game: dropped
    assert P.store_props(store, P.props_frame(rows), games, "fourcasters", resolver) == 4
    table = store.parquet[P.props_key(20262027, "fourcasters")]
    assert set(table["game_id"]) == {2026020040}
    teams = dict(table.select("player_name", "team").unique().iter_rows())
    assert teams == {"Nikita Kucherov": "TBL", "Trevor Zegras": "PHI", "Nobody Here": None}
    assert table.filter(pl.col("player_name") == "Nobody Here")["player_id"].null_count() == 1
    # Nothing moved: nothing written. One price moves: one row.
    later = CAPTURED + timedelta(minutes=5)
    assert P.store_props(store, P.props_frame([dict(r, captured_at=later) for r in rows]), games, "fourcasters",
                         resolver) == 0
    moved = [dict(r, captured_at=later + timedelta(minutes=5)) for r in rows]
    moved[0]["price"] = 150.0
    assert P.store_props(store, P.props_frame(moved), games, "fourcasters", resolver) == 1
    assert P.load_props(store, 20262027).height == 5  # type: ignore[arg-type]


def test_store_props_empty_writes_nothing(games: pl.DataFrame) -> None:
    store = FakeStore()
    assert P.store_props(store, P.props_frame([]), games, "lowvig") == 0
    assert not store.parquet
