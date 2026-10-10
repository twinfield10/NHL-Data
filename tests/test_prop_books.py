"""FanDuel and LowVig (DST widget) prop pollers: normalization and storage (no network)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from nhl.ingest.http import SourceUnavailable
from nhl.sources import dst, fanduel
from nhl.sources.dailyfaceoff import PlayerResolver

FIXTURES = Path(__file__).parent / "fixtures" / "props"
CAPTURED = datetime(2026, 10, 8, 17, 0, tzinfo=timezone.utc)


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def keyed(df: pl.DataFrame) -> dict[tuple, float]:
    return {(r["player_name"], r["prop_type"], r["line"], r["side"]): r["price"] for r in df.iter_rows(named=True)}


class FakeStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store`."""

    def __init__(self) -> None:
        self.parquet: dict[str, pl.DataFrame] = {}
        self.json: dict[str, Any] = {}

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        return self.parquet.get(key)

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        self.parquet[key] = df
        return key

    def put_json_gz(self, key: str, payload: Any) -> str:
        self.json[key] = payload
        return key

    def list_keys(self, prefix: str) -> list[str]:
        return [k for k in self.parquet if k.startswith(prefix)]


@pytest.fixture
def resolver() -> PlayerResolver:
    players = pl.DataFrame({"player_id": [8480000], "player_name": ["Tim Stützle"], "last_season": [20262027]})
    return PlayerResolver(players=players, roster_loader=lambda team: {})


# --------------------------------------------------------------------------- FanDuel --
def test_fanduel_board_skips_non_games() -> None:
    events = fanduel.board_events(load("fanduel_props.json")["board"])
    assert events == [{"event_id": "36141732", "away": "Philadelphia Flyers", "home": "Ottawa Senators",
                       "start": "2026-10-08T23:11:00.000Z"}]


def test_fanduel_normalize() -> None:
    df = fanduel.normalize(load("fanduel_props.json"), CAPTURED)
    assert keyed(df) == {
        # The two-way market wins the shared key over the anytime-scorer price.
        ("Tim Stützle", "goals", 0.5, "over"): 180.0, ("Tim Stützle", "goals", 0.5, "under"): -250.0,
        ("Drake Batherson", "goals", 0.5, "over"): 180.0,  # "(OTT)" suffix stripped
        ("Trevor Zegras", "shots", 1.5, "over"): -118.0, ("Trevor Zegras", "shots", 1.5, "under"): -110.0,
        ("Dylan Cozens", "shots", 2.5, "over"): -102.0, ("Owen Tippett", "shots", 2.5, "over"): 125.0,
        ("Joseph Woll", "saves", 25.5, "over"): -118.0, ("Joseph Woll", "saves", 26.5, "over"): 108.0,
        # "No Goalscorer" is not a player.
        ("Tim Stützle", "first_goal", None, "yes"): 1100.0, ("Drake Batherson", "first_goal", None, "yes"): 1200.0,
    }
    teams = dict(df.select("player_name", "team").unique().iter_rows())
    assert teams["Dylan Cozens"] == "OTT" and teams["Owen Tippett"] == "PHI"
    assert teams["Joseph Woll"] is None  # his logo names a team not in the game: left to the roster match
    assert set(df["book"]) == {"FanDuel"} and set(df["home_team"]) == {"OTT"}


class FakeFanDuel:
    def __init__(self, raw: dict[str, Any], refuse_board: bool = False) -> None:
        self.raw, self.refuse_board, self.calls = raw, refuse_board, []

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        self.calls.append((url, params))
        if url.endswith("content-managed-page"):
            if self.refuse_board:
                raise SourceUnavailable("403")
            return self.raw["board"]
        tabs = self.raw["events"].get(str(params["eventId"]), {})
        if params["tab"] not in tabs:
            raise SourceUnavailable("403")
        return tabs[params["tab"]]


def test_fanduel_poll(monkeypatch: pytest.MonkeyPatch, resolver: PlayerResolver) -> None:
    monkeypatch.setattr(fanduel, "utcnow", lambda: CAPTURED)
    games = pl.DataFrame({"game_id": [2026020061], "game_date": [date(2026, 10, 8)],
                          "home_abbr": ["OTT"], "away_abbr": ["PHI"]})
    store, client = FakeStore(), FakeFanDuel(load("fanduel_props.json"))
    written = fanduel.poll(store, games, client=client, resolver=resolver)  # type: ignore[arg-type]
    assert written == 11
    assert [p["tab"] for _, p in client.calls[1:]] == list(fanduel.TABS)
    table = store.parquet["external/odds/props/20262027/fanduel.parquet"]
    assert set(table["game_id"]) == {2026020061}
    assert any(k.startswith("raw/external/fanduel/") for k in store.json)
    assert fanduel.poll(store, games, client=client, resolver=resolver) == 0  # nothing moved  # type: ignore[arg-type]


def test_fanduel_refused_is_skip() -> None:
    store = FakeStore()
    assert fanduel.poll(store, pl.DataFrame(), client=FakeFanDuel({}, refuse_board=True)) == 0  # type: ignore[arg-type]
    assert not store.parquet


# ------------------------------------------------------------------------ LowVig (DST) --
def test_dst_game_meta() -> None:
    meta = dst.game_meta(load("dst_props.json")["games"])
    assert meta == {"282744": {"home_team": "tampa bay lightning", "away_team": "minnesota wild",
                               "start_time": "2026-10-08T23:00:00.000Z"}}


def test_dst_normalize() -> None:
    df = dst.normalize(load("dst_props.json"), CAPTURED)
    assert keyed(df) == {
        ("Brayden Point", "points", 0.5, "over"): -167.0, ("Brayden Point", "points", 0.5, "under"): 128.0,
        ("Kirill Kaprizov", "points", 0.5, "over"): -213.0, ("Kirill Kaprizov", "points", 0.5, "under"): 162.0,
        ("Brayden Point", "goals", 0.5, "over"): 180.0, ("Brayden Point", "goals", 1.5, "over"): 900.0,
        ("Brayden Point", "goals", 2.5, "over"): 4900.0,
        ("Brayden Point", "shots", 1.5, "over"): -357.0, ("Brayden Point", "shots", 3.5, "over"): 290.0,
        ("Brayden Point", "shots", 4.5, "over"): 750.0,
        ("Kirill Kaprizov", "first_goal", None, "yes"): 1000.0, ("Matt Boldy", "first_goal", None, "yes"): 1000.0,
    }  # the market for game 999 has no game in the games list and is dropped
    assert set(df["book"]) == {"LowVig"} and set(df["home_team"]) == {"TBL"} and set(df["away_team"]) == {"MIN"}
    teams = dict(df.select("player_name", "team").unique().iter_rows())
    assert teams["Brayden Point"] == "TBL" and teams["Matt Boldy"] is None


def test_dst_namesakes_get_ids() -> None:
    raw = load("dst_props.json")
    raw["markets"][0]["payload"][0]["players"][0]["name"] = "Fredrik Elias Pettersson"
    df = dst.normalize(raw, CAPTURED)
    assert set(df.filter(pl.col("player_name") == "Fredrik Elias Pettersson")["player_id"]) == {8480012}


def test_resolver_name_variants() -> None:
    players = pl.DataFrame({"player_id": [8482116, 8483525], "player_name": ["Tim Stützle", "Danila Yurov"],
                            "last_season": [20262027, 20262027]})
    r = PlayerResolver(players=players, roster_loader=lambda team: {})
    assert r.resolve(None, "Tim Stuetzle") == 8482116
    assert r.resolve(None, "Yurov Danila") == 8483525
    assert r.resolve(None, "Nobody Here") is None


def test_resolver_tells_roster_namesakes_apart_only_by_jersey_or_middle_name() -> None:
    from nhl.odds.props import resolve_player

    van = {"forwards": [{"id": 8480012, "firstName": {"default": "Elias"}, "lastName": {"default": "Pettersson"}, "sweaterNumber": 40}],
           "defensemen": [{"id": 8483678, "firstName": {"default": "Elias"}, "lastName": {"default": "Pettersson"}, "sweaterNumber": 25}]}
    players = pl.DataFrame({"player_id": [8480012, 8483678], "player_name": ["Elias Pettersson"] * 2,
                            "last_season": [20262027, 20262027]})
    r = PlayerResolver(players=players, roster_loader=lambda team: van if team == "VAN" else {})
    assert r.resolve("VAN", "Elias Pettersson #25") == 8483678   # FanDuel's sweater suffix
    assert r.resolve("VAN", "Elias Pettersson #40") == 8480012
    assert r.resolve("VAN", "Elias N. Pettersson") == 8483678    # DraftKings' middle initial
    assert r.resolve("VAN", "Fredrik Elias Pettersson") == 8480012
    assert r.resolve("VAN", "Elias Pettersson") == 8480012        # plain name: the forward (checked by price)
    # Without a team (4Casters), both rosters are tried; the jersey still decides.
    assert resolve_player(r, "Elias Pettersson #25", None, "NYR", "VAN") == (8483678, "VAN")
    assert resolve_player(r, "Elias Pettersson", None, "NYR", "VAN") == (8480012, "VAN")


def test_resolver_never_guesses_between_unknown_namesakes() -> None:
    from nhl.odds.props import resolve_player

    def p(pid: int, num: int) -> dict:
        return {"id": pid, "firstName": {"default": "John"}, "lastName": {"default": "Smith"}, "sweaterNumber": num}

    roster = {"forwards": [p(1, 11)], "defensemen": [p(2, 22)]}
    players = pl.DataFrame({"player_id": [1, 2], "player_name": ["John Smith"] * 2, "last_season": [20262027] * 2})
    r = PlayerResolver(players=players, roster_loader=lambda team: roster if team == "AAA" else {})
    assert r.resolve("AAA", "John Smith") is None           # same name on one roster
    assert r.resolve(None, "John Smith") is None            # both active: no league guess
    assert r.resolve("AAA", "John Smith #22") == 2
    assert resolve_player(r, "John Smith", None, "AAA", "BBB") == (None, None)
