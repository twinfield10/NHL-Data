"""LowVig and 4Casters normalizers against trimmed real payloads (captured 2026-10-05).

``lowvig.json`` carries the board, two ``get-event`` payloads (PHI@TBL with its 1st period
and a trimmed ``ContestTypes`` block of alternates; NSH@TOR full game only) and the
``NHL 3 WAY`` board for PHI@TBL. ``fourcasters.json`` is the NHL order book; the NHL-PROPS
book is ``tests/fixtures/props/fourcasters_props.json``.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import polars as pl
import pytest

from nhl.ingest.http import SourceUnavailable
from nhl.odds.core import implied_probability
from nhl.sources import fourcasters, lowvig, novig
from nhl.sources.dailyfaceoff import PlayerResolver
from nhl.storage import keys

FIXTURES = Path(__file__).parent / "fixtures" / "books"
PROP_FIXTURES = Path(__file__).parent / "fixtures" / "props"
CAPTURED = datetime(2026, 10, 5, 18, 0, tzinfo=timezone.utc)


def load(name: str) -> dict:
    """Read one fixture payload."""
    return json.loads((FIXTURES / name).read_text())


def pick(df: pl.DataFrame, **eq: Any) -> pl.DataFrame:
    """Rows of ``df`` equal to every keyword."""
    return df.filter(*(pl.col(k) == v for k, v in eq.items()))


@pytest.fixture
def games() -> pl.DataFrame:
    """The slice of the games table the fixtures need (NYI@NYR deliberately absent)."""
    return pl.DataFrame({
        "game_id": [2026020040, 2026020044, 2026020051],
        "game_date": [date(2026, 10, 5), date(2026, 10, 6), date(2026, 10, 7)],
        "home_abbr": ["TBL", "TOR", "WSH"],
        "away_abbr": ["PHI", "NSH", "PIT"],
    })


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

    def put_json_gz(self, key: str, obj: Any) -> str:
        self.json[key] = obj
        return key


class FakeLowVig:
    """Serves the LowVig fixture by endpoint (the 3-way board by league)."""

    def __init__(self, raw: dict) -> None:
        self.raw = raw

    def post_json(self, url: str, payload: dict) -> Any:
        if url.endswith("/offering-by-league"):
            return self.raw["threeway"] if payload["League"] == lowvig.THREE_WAY_LEAGUE else self.raw["offering"]
        return self.raw["events"][str(payload["gameID"])]


class FakeFourCasters:
    """Serves the 4Casters order books by league; ``props=None`` refuses the props call."""

    def __init__(self, raw: dict, props: dict | None = None) -> None:
        self.raw, self.props = raw, props
        self.calls: list[dict] = []

    def post_json(self, url: str, payload: dict) -> Any:
        self.calls.append(payload)
        if payload == {"leagueRequested": "NHL"}:
            return self.raw
        assert payload == {"leagueRequested": "NHL-PROPS"}
        if self.props is None:
            raise SourceUnavailable("403")
        return self.props


def roster_loader(team: str) -> dict:
    """NHL rosters for the resolver, from the trimmed fixture."""
    return json.loads((PROP_FIXTURES / "nhl_rosters.json").read_text()).get(team, {})


class Refusing:
    """A client whose every call is refused at the edge."""

    def post_json(self, url: str, payload: dict) -> Any:
        raise SourceUnavailable("403")


# ------------------------------------------------------------------------- LowVig --
@pytest.fixture
def lv() -> pl.DataFrame:
    return lowvig.normalize(load("lowvig.json"), CAPTURED)


def test_lowvig_board_ids() -> None:
    assert lowvig.game_ids(load("lowvig.json")["offering"]) == [491144351, 491146506]


def test_lowvig_moneyline_and_meta(lv: pl.DataFrame) -> None:
    ml = pick(lv, home_team="TBL", market="moneyline", period="game")
    assert dict(ml.select("side", "price").iter_rows()) == {"home": -222.0, "away": 193.0}
    assert ml["line"].null_count() == 2
    row = ml.row(0, named=True)
    assert row["book"] == "LowVig" and row["away_team"] == "PHI"
    assert row["start_time"] == datetime(2026, 10, 5, 23, 7, tzinfo=timezone.utc)
    assert row["source_event_id"] == "491144351" and row["price_point"] == "live"
    assert row["market_uid"] == "game|moneyline|game|main"


def test_lowvig_first_period(lv: pl.DataFrame) -> None:
    # "1 1st Period": two-way moneyline, -0.5/+0.5 puck line, 1.5 total; game lines untouched.
    p1 = pick(lv, home_team="TBL", period="p1")
    got = {(r["market"], r["side"]): (r["line"], r["price"]) for r in p1.iter_rows(named=True)}
    assert got == {
        ("moneyline", "home"): (None, -162.0), ("moneyline", "away"): (None, 142.0),
        ("puckline", "home"): (-0.5, 142.0), ("puckline", "away"): (0.5, -162.0),
        ("total", "over"): (1.5, -120.0), ("total", "under"): (1.5, 100.0),
    }
    assert set(p1["market_uid"]) == {"p1|moneyline|game|main", "p1|puckline|game|main", "p1|total|game|main"}
    main = pick(lv, home_team="TBL", period="game", is_alternate=False)
    assert sorted(pick(main, market="puckline")["line"].to_list()) == [-1.5, 1.5]
    assert pick(main, market="total")["line"].unique().to_list() == [5.5]
    # The day-before game posts no period block.
    assert set(pick(lv, home_team="TOR")["period"]) == {"game"}


def test_lowvig_alternates(lv: pl.DataFrame) -> None:
    alt = pick(lv, home_team="TBL", is_alternate=True)
    assert set(alt["period"]) == {"game"}
    totals = {(r["side"], r["line"]): r["price"] for r in pick(alt, market="total").iter_rows(named=True)}
    assert totals == {("over", 4.5): -400.0, ("under", 4.5): 250.0, ("over", 7.5): 275.0, ("under", 7.5): -450.0}
    # "Alternate Puckline Philadelphia Flyers": PHI +2.5 -300 / TBL -2.5 +200, each line from
    # its own side; both sides share the home-perspective market_uid.
    puck = {(r["side"], r["line"]): (r["price"], r["market_uid"])
            for r in pick(alt, market="puckline").iter_rows(named=True)}
    assert puck == {
        ("away", 2.5): (-300.0, "game|puckline|game|-2.5"), ("home", -2.5): (200.0, "game|puckline|game|-2.5"),
        ("away", -1.5): (350.0, "game|puckline|game|1.5"), ("home", 1.5): (-600.0, "game|puckline|game|1.5"),
    }
    # "Alternate Team Total Tampa Bay Lightning" -> subject home; "Team to Score First" ignored.
    tt = pick(alt, market="team_total")
    assert set(tt["subject"]) == {"home"}
    assert sorted(set(tt["line"].to_list())) == [2.5, 4.5, 5.5]
    assert dict(pick(tt, line=4.5).select("side", "price").iter_rows()) == {"over": 200.0, "under": -300.0}
    assert len(alt) == 14


def test_lowvig_alternate_equal_to_main_is_skipped() -> None:
    raw = load("lowvig.json")
    groups = raw["events"]["491144351"]["EventOffering"]["ContestTypes"][0]["DescriptionGroup"]
    group = next(g for g in groups if g["Description"] == "Alternate Total 4.5")
    for contestant in group["Contests"][0]["Contestants"]:
        contestant["ThresholdLine"] = 5.5  # the main total
    df = lowvig.normalize(raw, CAPTURED)
    assert pick(df, home_team="TBL", market="total", line=5.5).height == 2
    assert pick(df, home_team="TBL", market="total", line=5.5, is_alternate=True).is_empty()


def test_lowvig_three_way_regulation(lv: pl.DataFrame) -> None:
    three = pick(lv, market="moneyline_3way")
    assert dict(three.select("side", "price").iter_rows()) == {"home": -138.0, "away": 280.0, "draw": 360.0}
    assert set(three["period"]) == {"reg"} and three["line"].null_count() == 3
    assert set(three["source_event_id"]) == {"491144357"}  # the 3-way board's own game id
    assert set(three["market_uid"]) == {"reg|moneyline_3way|game|main"}
    implied = sum(implied_probability(p) for p in three["price"])
    assert 1.0 < implied < 1.1  # all three outcomes: a complete book


def test_lowvig_puckline_sides_opposite(lv: pl.DataFrame) -> None:
    pl_ = pick(lv, home_team="TBL", market="puckline", period="game", is_alternate=False)
    lines = dict(pl_.select("side", "line").iter_rows())
    prices = dict(pl_.select("side", "price").iter_rows())
    assert lines == {"home": -1.5, "away": 1.5}
    assert prices == {"home": 115.0, "away": -135.0}


def test_lowvig_totals(lv: pl.DataFrame) -> None:
    tot = pick(lv, home_team="TBL", market="total", period="game", is_alternate=False)
    assert dict(tot.select("side", "price").iter_rows()) == {"over": -125.0, "under": 109.0}
    # A whole-number NHL total (6) on the Toronto game.
    tor = pick(lv, home_team="TOR", market="total")
    assert tor["line"].to_list() == [6.0, 6.0]
    assert dict(tor.select("side", "price").iter_rows()) == {"over": -101.0, "under": -115.0}


def test_lowvig_team_totals(lv: pl.DataFrame) -> None:
    tt = pick(lv, home_team="TBL", market="team_total", is_alternate=False)
    got = {(r["subject"], r["side"]): (r["line"], r["price"]) for r in tt.iter_rows(named=True)}
    assert got == {
        ("home", "over"): (3.5, -110.0), ("home", "under"): (3.5, -120.0),
        ("away", "over"): (2.5, 113.0), ("away", "under"): (2.5, -145.0),
    }
    assert set(tt["market_uid"]) == {"game|team_total|home|main", "game|team_total|away|main"}


def test_lowvig_unposted_zeros_absent(lv: pl.DataFrame) -> None:
    # Toronto's puck line and team totals are posted as zeros the day before.
    tor = pick(lv, home_team="TOR")
    assert set(tor["market"]) == {"moneyline", "total"}
    assert (lv["price"] != 0).all()


# ---------------------------------------------------------------------- 4Casters --
@pytest.fixture
def fc() -> pl.DataFrame:
    return fourcasters.normalize(load("fourcasters.json"), CAPTURED)


def test_vwap_underdog_walks_levels() -> None:
    price, filled = fourcasters.vwap([(130, 1000), (150, 300), (140, 300)], unit_size=600)
    assert filled == 600
    p = (300 * implied_probability(150) + 300 * implied_probability(140)) / 600 + fourcasters.EXCHANGE_VIG
    assert price == pytest.approx((1 - p) / p * 100)
    assert price == pytest.approx(140.48, abs=0.01)


def test_vwap_favourite_scales_target() -> None:
    # -200 best: target is 600 * 2 = 1200 risked.
    price, filled = fourcasters.vwap([(-210, 500), (-200, 1000)], unit_size=600)
    assert filled == 1200
    p = (1000 * implied_probability(-200) + 200 * implied_probability(-210)) / 1200 + fourcasters.EXCHANGE_VIG
    assert price == pytest.approx(-p / (1 - p) * 100)
    assert price == pytest.approx(-208.6, abs=0.1)


def test_vwap_thin_is_none() -> None:
    assert fourcasters.vwap([(150, 590)], unit_size=600) is None  # the full 600 must fill, as on Novig
    assert fourcasters.vwap([(150, 600)], unit_size=600) is not None
    assert fourcasters.vwap([]) is None


def test_vwap_default_size_matches_novig_floor() -> None:
    assert fourcasters.UNIT_SIZE == novig.GAME_MIN_STAKE == 100
    assert fourcasters.vwap([(150, 99)]) is None  # less than the full 100
    assert fourcasters.vwap([(150, 100)])[1] == 100
    assert fourcasters.vwap([(-200, 150), (-210, 100)])[1] == 200  # a favourite bets to win 100


def test_fourcasters_main_lines(fc: pl.DataFrame) -> None:
    tbl = pick(fc, home_team="TBL", is_alternate=False)
    assert set(tbl["book"]) == {"4Casters"}
    got = {(r["market"], r["side"]): (r["line"], r["price"]) for r in tbl.iter_rows(named=True)}
    assert got == {
        ("moneyline", "home"): (None, -213.0), ("moneyline", "away"): (None, 190.0),
        ("puckline", "home"): (-1.5, 120.0), ("puckline", "away"): (1.5, -132.0),
        ("total", "over"): (5.5, -123.0), ("total", "under"): (5.5, 110.0),
    }
    # Depth is the dollars resting on that side, far above the unit size here.
    assert (tbl["depth"] > fourcasters.UNIT_SIZE).all()
    assert tbl["source_event_id"].unique().to_list() == ["6ac0bf4ae16d06a69b2da690"]


def test_fourcasters_alternate_rungs(fc: pl.DataFrame) -> None:
    # TBL's ladders also rest at 0 (both sides) and a 5-goal total, beside the main lines.
    alt = pick(fc, home_team="TBL", is_alternate=True)
    got = {(r["market"], r["side"], r["line"]): r["price"] for r in alt.iter_rows(named=True)}
    assert got == {
        ("puckline", "home", 0.0): -213.0, ("puckline", "away", 0.0): 190.0,
        ("total", "over", 5.0): -244.0, ("total", "under", 5.0): 212.0,
    }
    assert set(alt["market_uid"]) == {"game|puckline|game|0.0", "game|total|game|5.0"}
    assert (alt["depth"] >= fourcasters.UNIT_SIZE * fourcasters.MIN_FILL_RATIO).all()


def test_fourcasters_thin_and_missing_omitted(fc: pl.DataFrame) -> None:
    # NYI@NYR: puck line and total rest on a few hundred dollars, enough for $100, and so
    # does its 6.5 alternate total.
    nyr = pick(fc, home_team="NYR")
    assert set(nyr["market"]) == {"moneyline", "puckline", "total"}
    assert set(nyr.filter(pl.col("is_alternate"))["line"]) == {6.5}
    # PIT@WSH: mainTotal 0 means no total yet; EDM@ANA has an empty book.
    assert set(pick(fc, home_team="WSH")["market"]) == {"moneyline", "puckline"}
    assert pick(fc, home_team="ANA").is_empty()


def test_fourcasters_period_child_game() -> None:
    raw = load("fourcasters.json")
    child = json.loads(json.dumps(raw["data"]["games"][0]))
    child.update(periodName="1st Period", id="child", parentGameID=raw["data"]["games"][0]["id"])
    second = dict(child, periodName="Overtime", id="ot")
    df = fourcasters.normalize([child, second], CAPTURED)
    assert set(df["period"]) == {"p1"}  # "Overtime" is not a stored period
    assert set(df["source_event_id"]) == {raw["data"]["games"][0]["id"]}


def test_fourcasters_props() -> None:
    props, team_totals = fourcasters.normalize_props(json.loads((PROP_FIXTURES / "fourcasters_props.json").read_text()),
                                                     CAPTURED)
    got = {(r["player_name"], r["prop_type"], r["line"], r["side"]): r["price"] for r in props.iter_rows(named=True)}
    assert got == {
        ("Nikita Kucherov", "goals", 0.5, "over"): 164.0, ("Nikita Kucherov", "goals", 0.5, "under"): -374.0,
        ("Brayden Point", "shots", 2.5, "over"): 124.0, ("Brayden Point", "shots", 2.5, "under"): -185.0,
        ("Trevor Zegras", "assists", 0.5, "over"): 194.0, ("Trevor Zegras", "assists", 0.5, "under"): -299.0,
        ("Andrei Vasilevskiy", "saves", 20.5, "over"): -130.0, ("Andrei Vasilevskiy", "saves", 20.5, "under"): -128.0,
    }
    # Noel Acciari's $40 over cannot fill $50; Cam York / J.J. Moser have empty books.
    assert not set(props["player_name"]) & {"Noel Acciari", "Cam York", "J.J. Moser"}
    assert set(props["source_event_id"]) == {"6ac0bf4ae16d06a69b2da690"}  # the matchup, not the prop
    assert set(props["home_team"]) == {"TBL"} and props["team"].null_count() == props.height
    # "TAMPA BAY LIGHTNING (TEAM TOTAL GOALS)" is a team total in the odds table.
    assert {(r["subject"], r["side"], r["line"]): r["price"] for r in team_totals.iter_rows(named=True)} == {
        ("home", "over", 3.5): 104.0, ("home", "under", 3.5): -135.0}
    assert set(team_totals["market_uid"]) == {"game|team_total|home|main"}


def test_fourcasters_live_skipped() -> None:
    raw = load("fourcasters.json")
    for game in raw["data"]["games"]:
        game["live"] = True
    assert fourcasters.normalize(raw, CAPTURED).is_empty()


# -------------------------------------------------------------------------- poll --
def test_lowvig_poll_writes_transitions_once(games: pl.DataFrame) -> None:
    store, client = FakeStore(), FakeLowVig(load("lowvig.json"))
    first = lowvig.poll(store, games, client=client)  # type: ignore[arg-type]
    # TBL: 10 game + 6 p1 + 14 alternates + 3 three-way; TOR: 4.
    assert first == 37
    table = store.parquet[keys.odds(20262027, "lowvig")]
    assert set(table["game_id"]) == {2026020040, 2026020044}
    assert pick(table, market="moneyline_3way")["game_id"].unique().to_list() == [2026020040]
    assert any(k.endswith("-threeway.json.gz") for k in store.json)
    assert lowvig.poll(store, games, client=client) == 0  # type: ignore[arg-type]
    assert not any(k.startswith("external/odds/props/") for k in store.parquet)  # LowVig has no props


def test_fourcasters_poll_drops_unmatched(games: pl.DataFrame) -> None:
    store = FakeStore()
    resolver = PlayerResolver(roster_loader=roster_loader)
    props_book = json.loads((PROP_FIXTURES / "fourcasters_props.json").read_text())
    client = FakeFourCasters(load("fourcasters.json"), props_book)
    written = fourcasters.poll(store, games, client=client, resolver=resolver)  # type: ignore[arg-type]
    # TBL 6 main + 4 alternates + 2 team totals + WSH 4; NYI@NYR is not in the games slice.
    assert written == 16
    assert set(store.parquet[keys.odds(20262027, "fourcasters")]["game_id"]) == {2026020040, 2026020051}
    props = store.parquet["external/odds/props/20262027/fourcasters.parquet"]
    assert props.height == 8 and set(props["game_id"]) == {2026020040}
    # Teams come from the roster match: the book lists only the matchup.
    teams = dict(props.select("player_name", "team").unique().iter_rows())
    assert teams == {"Nikita Kucherov": "TBL", "Brayden Point": "TBL", "Andrei Vasilevskiy": "TBL",
                     "Trevor Zegras": "PHI"}
    assert props["player_id"].null_count() == 0
    assert any(k.endswith("-props.json.gz") for k in store.json)


def test_fourcasters_props_refused_keeps_lines(games: pl.DataFrame) -> None:
    store = FakeStore()
    written = fourcasters.poll(store, games, client=FakeFourCasters(load("fourcasters.json")))  # type: ignore[arg-type]
    assert written == 14  # no team totals without the props board
    assert not any(k.startswith("external/odds/props/") for k in store.parquet)


@pytest.mark.parametrize("module", [lowvig, fourcasters])
def test_poll_refused_is_skip(module: Any, games: pl.DataFrame) -> None:
    store = FakeStore()
    assert module.poll(store, games, client=Refusing()) == 0
    assert not store.parquet


# ------------------------------------------------------------------- 4Casters sign-in --
class FakeExchangeWeb:
    """The exchange as of 2026-10-06: the order book needs a valid ``Authorization`` token."""

    def __init__(self, password: str = "pw", valid: str = "tok-2") -> None:
        self.session = SimpleNamespace(headers={})
        self.password, self.valid = password, valid
        self.logins = 0

    def post_json(self, url: str, payload: dict) -> Any:
        if url.endswith("/user/login"):
            assert "Authorization" not in self.session.headers  # a stale token isn't sent to login
            if payload["password"] != self.password:
                raise SourceUnavailable("401")
            self.logins += 1
            return {"data": {"user": {"auth": self.valid}}}
        if self.session.headers.get("Authorization") != self.valid:
            raise SourceUnavailable("403")
        return {"data": {"games": []}}


def test_fourcasters_signs_in_caches_token_and_reuses_it(tmp_path) -> None:
    token = tmp_path / "auth_token"
    web = FakeExchangeWeb()
    fourcasters.SignedInClient(web, "me", "pw", token).post_json("u/exchange/getOrderbook", {"leagueRequested": "NHL"})
    assert web.logins == 1 and token.read_text() == "tok-2" and oct(token.stat().st_mode)[-3:] == "600"
    again = FakeExchangeWeb()
    fourcasters.SignedInClient(again, "me", "pw", token).post_json("u/exchange/getOrderbook", {"leagueRequested": "NHL"})
    assert again.logins == 0  # the cached token is used without logging in


def test_fourcasters_expired_token_signs_in_again_once(tmp_path) -> None:
    token = tmp_path / "auth_token"
    token.write_text("tok-1")  # expired
    web = FakeExchangeWeb()
    assert fourcasters.SignedInClient(web, "me", "pw", token).post_json("u/x", {}) == {"data": {"games": []}}
    assert web.logins == 1 and token.read_text() == "tok-2"


def test_fourcasters_bad_login_is_a_skip_not_a_crash(games: pl.DataFrame, tmp_path) -> None:
    client = fourcasters.SignedInClient(FakeExchangeWeb(password="right"), "me", "wrong", tmp_path / "t")
    with pytest.raises(SourceUnavailable, match="login refused"):
        client.post_json("u/x", {})
    assert fourcasters.poll(FakeStore(), games, client=client) == 0  # type: ignore[arg-type]
