"""ESPN odds normalization on real, trimmed payloads (no network)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from nhl.odds.core import implied_probability, odds_frame
from nhl.odds.props import props_frame
from nhl.sources.dailyfaceoff import PlayerResolver
from nhl.sources import espn_odds
from nhl.sources.espn_odds import (
    event_meta,
    fetch_event_odds,
    normalize_event_odds,
    poll,
    split_suspect,
)

FIXTURES = Path(__file__).parent / "fixtures" / "espn"
PROP_FIXTURES = Path(__file__).parent / "fixtures" / "props"
CAPTURED = datetime(2026, 10, 5, 17, 0, tzinfo=timezone.utc)


def load(name: str) -> Any:
    """Read one fixture file."""
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture(scope="module")
def metas() -> dict[str, dict[str, Any]]:
    """Scoreboard metadata for every fixture event, by event id."""
    return {e["id"]: event_meta(e) for e in load("scoreboard_events.json")["events"]}


def quote(df: pl.DataFrame, book: str, point: str, market: str, side: str) -> tuple[float | None, float]:
    """The single (line, price) for one book/price point/market/side."""
    row = df.filter(
        (pl.col("book") == book) & (pl.col("price_point") == point)
        & (pl.col("market") == market) & (pl.col("side") == side)
    )
    assert row.height == 1, (book, point, market, side, row)
    return row.item(0, "line"), row.item(0, "price")


def load_prop(name: str) -> Any:
    """Read one props fixture file."""
    return json.loads((PROP_FIXTURES / name).read_text())


def athletes() -> dict[str, tuple[str, str | None]]:
    """ESPN athlete id -> (name, team) from the trimmed rosters (Cirelli deliberately absent)."""
    out: dict[str, tuple[str, str | None]] = {}
    for roster in load_prop("espn_rosters.json").values():
        out.update(espn_odds.roster_athletes(roster))
    return out


def test_event_meta(metas):
    m = metas["401349265"]
    assert (m["home_team"], m["away_team"]) == ("TB", "WSH")
    assert m["start_time"] == datetime(2021, 11, 1, 23, 0, tzinfo=timezone.utc)
    assert m["season_type"] == 2 and m["state"] == "post"
    assert metas["401892445"]["state"] == "pre"
    assert (metas["401892445"]["home_team_id"], metas["401892445"]["away_team_id"]) == ("20", "15")


def test_history_2023_open_close(metas):
    df = normalize_event_odds(load("odds_2023_401559375.json"), metas["401559375"], CAPTURED, "history")
    assert set(df["price_point"]) == {"open", "close"}
    assert set(df["home_team"]) == {"PHI"} and set(df["away_team"]) == {"BUF"}
    assert set(df["source_event_id"]) == {"401559375"}
    # Betfair moneyline moved from PHI +100 / BUF -120 at open to -110 / -110 at close.
    assert quote(df, "BetfairSportsbook", "open", "moneyline", "home") == (None, 100.0)
    assert quote(df, "BetfairSportsbook", "open", "moneyline", "away") == (None, -120.0)
    assert quote(df, "BetfairSportsbook", "close", "moneyline", "home") == (None, -110.0)
    # Puckline lines are from each side's perspective: home underdog +1.5, away -1.5.
    assert quote(df, "Bet365", "close", "puckline", "home") == (1.5, -260.0)
    assert quote(df, "Bet365", "close", "puckline", "away") == (-1.5, 215.0)
    assert quote(df, "Bet365", "open", "total", "over") == (6.5, -135.0)
    # ESPN BET flipped its puckline favorite between open and close.
    assert quote(df, "ESPN BET", "open", "puckline", "home") == (1.5, -240.0)
    assert quote(df, "ESPN BET", "close", "puckline", "home") == (-1.5, 210.0)
    assert quote(df, "ESPN BET", "close", "puckline", "away") == (1.5, -300.0)
    # DraftKings has no totals in this payload; PointsBet has no puckline.
    assert df.filter((pl.col("book") == "DraftKings") & (pl.col("market") == "total")).is_empty()
    assert df.filter((pl.col("book") == "PointsBet") & (pl.col("market") == "puckline")).is_empty()
    assert quote(df, "PointsBet", "close", "total", "under") == (6.5, 115.0)


def test_three_way_moneylines_are_reclassified(metas):
    """Bet365/DraftKings/PointsBet/Unibet quote 3-way regulation lines (PHI +135 / BUF +150)."""
    payload = load("odds_2023_401559375.json")
    raw = odds_frame(espn_odds.event_rows(payload, metas["401559375"], CAPTURED, "history"))
    bet365 = raw.filter((pl.col("book") == "Bet365") & (pl.col("market") == "moneyline") & (pl.col("price_point") == "close"))
    assert sorted(bet365["price"].to_list()) == [135.0, 150.0]
    df = normalize_event_odds(payload, metas["401559375"], CAPTURED, "history")
    ml_books = set(df.filter(pl.col("market") == "moneyline")["book"])
    assert ml_books == {"BetfairSportsbook", "ESPN BET"}
    three = df.filter(pl.col("market") == "moneyline_3way")
    assert set(three["book"]) == {"Bet365", "DraftKings", "PointsBet", "Unibet"}
    assert set(three["period"]) == {"reg"} and set(three["market_uid"]) == {"reg|moneyline_3way|game|main"}
    assert quote(df, "Bet365", "close", "moneyline_3way", "home") == (None, 135.0)
    assert quote(df, "Bet365", "close", "moneyline_3way", "away") == (None, 150.0)
    # The implied sum, not the signs, is what marks them (see the minus-money case below).
    sums = three.with_columns(pl.col("price").map_elements(implied_probability, return_dtype=pl.Float64).alias("p")) \
        .group_by("book", "price_point").agg(pl.col("p").sum()).get_column("p")
    assert sums.is_between(*espn_odds.THREE_WAY_RANGE).all()


def test_three_way_with_minus_money_favourite(metas):
    """Real case (COL @ SJS, 2023-10-14): Unibet's 3-way is SJS +390 / COL -175 (sum 0.84)."""
    df = normalize_event_odds(load("odds_2023_401559267.json"), metas["401559267"], CAPTURED, "history")
    assert quote(df, "Unibet", "close", "moneyline_3way", "away") == (None, -175.0)
    assert quote(df, "Unibet", "close", "moneyline_3way", "home") == (None, 390.0)
    assert df.filter((pl.col("book") == "Unibet") & (pl.col("market") == "moneyline")).is_empty()


def test_history_2021_last_only(metas):
    df = normalize_event_odds(load("odds_2021_401349265.json"), metas["401349265"], CAPTURED, "history")
    assert set(df["price_point"]) == {"last"}
    assert set(df["home_team"]) == {"TBL"}  # ESPN "TB" resolved to the NHL tricode
    assert quote(df, "Caesars Sportsbook (Colorado)", "last", "moneyline", "home") == (None, -145.0)
    assert quote(df, "Caesars Sportsbook (Colorado)", "last", "moneyline", "away") == (None, 125.0)
    assert quote(df, "MGM", "last", "puckline", "home") == (-1.5, 170.0)
    assert quote(df, "MGM", "last", "puckline", "away") == (1.5, -200.0)
    assert quote(df, "Bet365", "last", "total", "over") == (5.5, -125.0)
    # Bet365's TB +110 / WSH +220 is a 3-way line: reclassified, with its puckline and total.
    assert df.filter((pl.col("book") == "Bet365") & (pl.col("market") == "moneyline")).is_empty()
    assert quote(df, "Bet365", "last", "moneyline_3way", "home") == (None, 110.0)
    assert df.filter(pl.col("book") == "Bet365").height == 6


def test_live_providers_excluded(metas):
    payload = load("odds_2025_401688547.json")
    assert any("Live" in i["provider"]["name"] for i in payload["items"])
    df = normalize_event_odds(payload, metas["401688547"], CAPTURED, "history")
    assert set(df["book"]) == {"ESPN BET"}
    assert -15000.0 not in df["price"].to_list()
    assert quote(df, "ESPN BET", "close", "moneyline", "home") == (None, -145.0)
    assert quote(df, "ESPN BET", "open", "moneyline", "home") == (None, -135.0)
    live = normalize_event_odds(payload, metas["401688547"], CAPTURED, "live")
    assert set(live["book"]) == {"ESPN BET"}


def test_live_mode_uses_current(metas):
    df = normalize_event_odds(load("odds_2026_401892445.json"), metas["401892445"], CAPTURED, "live")
    assert df.height == 6
    assert set(df["price_point"]) == {"live"}
    assert df["captured_at"].unique().to_list() == [CAPTURED]
    assert quote(df, "DraftKings", "live", "moneyline", "home") == (None, -218.0)  # open was -175
    assert quote(df, "DraftKings", "live", "puckline", "away") == (1.5, -135.0)
    assert quote(df, "DraftKings", "live", "total", "under") == (5.5, 102.0)
    assert set(df["market_uid"]) == {"game|moneyline|game|main", "game|puckline|game|main", "game|total|game|main"}


def test_history_without_close_gives_open_and_last(metas):
    df = normalize_event_odds(load("odds_2026_401892445.json"), metas["401892445"], CAPTURED, "history")
    assert set(df["price_point"]) == {"open", "last"}
    assert quote(df, "DraftKings", "open", "moneyline", "home") == (None, -175.0)
    assert quote(df, "DraftKings", "last", "moneyline", "home") == (None, -218.0)


def test_stale_total_is_dropped(metas):
    """Real case (COL @ SJS, 2023-10-14): Unibet opened 3.5 at -1000 while every book hung 6.5."""
    df = normalize_event_odds(load("odds_2023_401559267.json"), metas["401559267"], CAPTURED, "history")
    unibet = df.filter((pl.col("book") == "Unibet") & (pl.col("market") == "total"))
    assert set(unibet["price_point"]) == {"close"}  # the 3.5 open is gone
    assert quote(df, "Unibet", "close", "total", "over") == (5.5, -182.0)  # 1.0 off the median: kept
    assert quote(df, "SugarHouse", "open", "total", "over") == (6.0, -119.0)
    # ESPN BET's close total has an under price but no over: incomplete pair, dropped.
    espn_close = df.filter((pl.col("book") == "ESPN BET") & (pl.col("price_point") == "close") & (pl.col("market") == "total"))
    assert espn_close.is_empty()
    assert quote(df, "ESPN BET", "open", "total", "under") == (6.5, -110.0)


def test_split_suspect_rules():
    base = {"book": "B", "captured_at": CAPTURED, "start_time": CAPTURED, "away_team": "BUF",
            "home_team": "PHI", "price_point": "close", "source_event_id": "1"}
    rows = [
        {**base, "market": "moneyline", "side": "home", "line": None, "price": -110.0},
        {**base, "market": "moneyline", "side": "away", "line": None, "price": -110.0},   # 1.048: kept
        {**base, "market": "puckline", "side": "home", "line": -1.5, "price": 200.0},
        {**base, "market": "puckline", "side": "away", "line": -1.5, "price": -250.0},  # lines not mirrored
        {**base, "book": "C", "market": "moneyline", "side": "home", "line": None, "price": -110.0},  # one side only
        {**base, "book": "D", "market": "moneyline", "side": "home", "line": None, "price": -400.0},
        {**base, "book": "D", "market": "moneyline", "side": "away", "line": None, "price": 300.0},  # 0.8 + 0.25 = 1.05: kept
        {**base, "book": "E", "market": "moneyline", "side": "home", "line": None, "price": -300.0},
        {**base, "book": "E", "market": "moneyline", "side": "away", "line": None, "price": -300.0},  # 1.5: dropped
        {**base, "book": "F", "market": "moneyline", "side": "home", "line": None, "price": 140.0},
        {**base, "book": "F", "market": "moneyline", "side": "away", "line": None, "price": 175.0},  # 0.78: 3-way
        {**base, "book": "G", "market": "moneyline", "side": "home", "line": None, "price": 105.0},
        {**base, "book": "G", "market": "moneyline", "side": "away", "line": None, "price": 105.0},  # 0.976: dropped
        {**base, "book": "H", "market": "moneyline", "side": "home", "line": None, "price": 600.0},
        {**base, "book": "H", "market": "moneyline", "side": "away", "line": None, "price": 900.0},  # 0.24: dropped
    ]
    kept, dropped = split_suspect(odds_frame(rows))
    assert sorted(kept["book"].to_list()) == ["B", "B", "D", "D", "F", "F"]
    assert set(kept.filter(pl.col("book") == "F")["market"]) == {"moneyline_3way"}
    assert set(kept.filter(pl.col("book") == "F")["period"]) == {"reg"}
    assert dropped.height == 9


def test_one_sided_alternate_rung_is_kept():
    """DraftKings sometimes quotes only the over of a team-total ladder rung."""
    base = {"book": "DraftKings", "captured_at": CAPTURED, "start_time": CAPTURED, "away_team": "BUF",
            "home_team": "PHI", "price_point": "live", "source_event_id": "1", "period": "reg",
            "market": "team_total", "subject": "home"}
    rows = [
        {**base, "side": "over", "line": 3.5, "price": -120.0},
        {**base, "side": "under", "line": 3.5, "price": -110.0},
        {**base, "side": "over", "line": 5.5, "price": 700.0, "is_alternate": True},     # one side: kept
        {**base, "side": "over", "line": 0.5, "price": -150.0, "is_alternate": True},
        {**base, "side": "under", "line": 0.5, "price": -150.0, "is_alternate": True},  # sum 1.2: dropped
    ]
    kept, dropped = split_suspect(odds_frame(rows))
    assert kept.height == 3 and set(dropped["line"]) == {0.5}


def test_period_totals_do_not_skew_the_median():
    """A 1.5 first-period total must not be read as a stale game total (or vice versa)."""
    base = {"captured_at": CAPTURED, "start_time": CAPTURED, "away_team": "BUF", "home_team": "PHI",
            "price_point": "live", "source_event_id": "1", "market": "total", "line": None}
    rows = []
    for book in ("A", "B", "C"):
        rows += [{**base, "book": book, "side": "over", "line": 6.5, "price": -110.0},
                 {**base, "book": book, "side": "under", "line": 6.5, "price": -110.0}]
    rows += [{**base, "book": "A", "period": "p1", "side": "over", "line": 1.5, "price": -125.0},
             {**base, "book": "A", "period": "p1", "side": "under", "line": 1.5, "price": -105.0}]
    kept, dropped = split_suspect(odds_frame(rows))
    assert dropped.is_empty() and kept.height == 8


# ------------------------------------------------------------------- prop bets
@pytest.fixture(scope="module")
def prop_bets(metas) -> tuple[pl.DataFrame, pl.DataFrame]:
    """DraftKings' propBets for PHI @ TB (2026-10-05) split into (odds, props) frames."""
    odds_rows, prop_rows = espn_odds.prop_bet_rows(
        load_prop("espn_propbets_401892445.json"), metas["401892445"], "DraftKings", CAPTURED, athletes())
    return odds_frame(odds_rows), props_frame(prop_rows)


def test_roster_athletes():
    got = athletes()
    assert got["2563060"] == ("Nikita Kucherov", "TB")
    assert got["3900169"] == ("Travis Konecny", "PHI")
    assert "3941973" not in got


def test_prop_bets_period_markets(prop_bets):
    odds, _ = prop_bets
    p1 = odds.filter(pl.col("period") == "p1")
    got = {(r["market"], r["side"]): (r["line"], r["price"]) for r in p1.iter_rows(named=True)}
    # 1st Period Moneyline is three-way: PHI (team 15) +255, TB (team 20) +135, team-less draw +175.
    assert got == {
        ("moneyline_3way", "away"): (None, 255.0), ("moneyline_3way", "home"): (None, 135.0),
        ("moneyline_3way", "draw"): (None, 175.0),
        ("puckline", "away"): (0.5, -166.0), ("puckline", "home"): (-0.5, 130.0),
        ("total", "over"): (1.5, -125.0), ("total", "under"): (1.5, -105.0),
    }
    p2 = odds.filter(pl.col("period") == "p2")
    assert dict(p2.select("side", "price").iter_rows()) == {"away": 255.0, "home": 115.0, "draw": 200.0}
    # The 3-way p1 line survives the sanity filter; the two-way pairs pass it.
    kept, dropped = split_suspect(odds)
    assert dropped.is_empty() and kept.height == odds.height
    assert set(kept.filter(pl.col("market") == "moneyline_3way")["period"]) == {"p1", "p2"}


def test_prop_bets_team_total_ladder(prop_bets):
    odds, _ = prop_bets
    tt = odds.filter(pl.col("market") == "team_total")
    assert set(tt["period"]) == {"reg"} and set(tt["subject"]) == {"home"}
    assert sorted(set(tt["line"].to_list())) == [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]
    # 3.5 (-120 / -120) is the balanced rung; the rest are alternates.
    main = tt.filter(~pl.col("is_alternate"))
    assert dict(main.select("side", "price").iter_rows()) == {"over": -120.0, "under": -120.0}
    assert set(main["line"]) == {3.5} and set(main["market_uid"]) == {"reg|team_total|home|main"}
    assert tt.filter(pl.col("line") == 1.5).select("side", "price").rows() == [("over", -825.0), ("under", 470.0)]


def test_prop_bets_player_props(prop_bets):
    _, props = prop_bets
    got = {(r["player_name"], r["prop_type"], r["line"], r["side"]): r["price"] for r in props.iter_rows(named=True)}
    # Two-way markets: first item over, second under.
    assert got[("Nikita Kucherov", "points", 1.5, "over")] == 105.0
    assert got[("Nikita Kucherov", "points", 1.5, "under")] == -145.0
    assert got[("Andrei Vasilevskiy", "saves", 20.5, "under")] == -110.0
    assert got[("Nikita Kucherov", "shots", 3.5, "over")] == 125.0
    # Ladders fold into overs: "1+ points" = points over 0.5, "4+ shots" = shots over 3.5
    # (same key and price as the two-way over, stored once), "2+ goals" = goals over 1.5.
    assert got[("Nikita Kucherov", "points", 0.5, "over")] == -425.0
    assert got[("Nikita Kucherov", "shots", 2.5, "over")] == -185.0
    assert got[("Nikita Kucherov", "goals", 0.5, "over")] == 145.0  # anytime goalscorer
    assert got[("Nikita Kucherov", "goals", 1.5, "over")] == 1000.0
    assert got[("Andrei Vasilevskiy", "saves", 21.5, "over")] == 110.0  # "22+ saves"
    assert got[("Travis Konecny", "first_goal", None, "yes")] == 1600.0
    assert props.filter(pl.col("prop_type") == "shots").filter(pl.col("line") == 3.5).height == 2
    assert set(props["team"]) == {"TBL", "PHI"}
    assert set(props["source_event_id"]) == {"401892445"}
    # Cirelli's anytime price has no name (not on the trimmed roster): skipped.
    assert "Anthony Cirelli" not in set(props["player_name"])


class FakeClient:
    """Serves canned JSON by URL substring and records calls."""

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, dict | None]] = []

    def get_json(self, url: str, params: dict | None = None) -> Any:
        """Return the first route whose key is in the URL (callables get the params)."""
        self.calls.append((url, params))
        for key, value in self.routes.items():
            if key in url:
                return value(params) if callable(value) else value
        raise AssertionError(f"unexpected url {url}")


class FakeStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store`."""

    def __init__(self) -> None:
        self.objects: dict[str, Any] = {}

    def put_json_gz(self, key: str, obj: Any) -> str:
        """Store a JSON object."""
        self.objects[key] = obj
        return "etag"

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        """Return a stored frame or None."""
        return self.objects.get(key)

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        """Store a frame."""
        self.objects[key] = df
        return "etag"


def test_fetch_event_odds_follows_pages():
    items = load("odds_2023_401559375.json")["items"]

    def page(params):
        n = params.get("page", 1)
        return {"count": len(items), "pageIndex": n, "pageCount": 2, "items": items[(n - 1) * 3: n * 3]}

    client = FakeClient({"/odds": page})
    payload = fetch_event_odds(client, "401559375")  # type: ignore[arg-type]
    assert [i["provider"]["name"] for i in payload["items"]] == [i["provider"]["name"] for i in items[:6]]
    assert len(client.calls) == 2


def test_poll_writes_transitions_once():
    board = {"events": [e for e in load("scoreboard_events.json")["events"] if e["id"] in ("401892445", "401559375")]}
    client = FakeClient({"scoreboard": board, "401892445/odds": load("odds_2026_401892445.json")})
    games = pl.DataFrame({
        "game_id": [2026020050], "season": [20262027], "season_type": ["R"],
        "game_date": [date(2026, 10, 5)], "home_abbr": ["TBL"], "away_abbr": ["PHI"], "is_final": [False],
    })
    store = FakeStore()
    written = poll(store, games, dates=[date(2026, 10, 5)], client=client)  # type: ignore[arg-type]
    assert written == 6
    table = store.objects["external/odds/live/20262027/espn.parquet"]
    assert set(table["game_id"]) == {2026020050}
    # The finished 2023 event is not fetched; the raw bundle is archived.
    assert not any("401559375" in url for url, _ in client.calls)
    assert any(k.startswith("raw/external/espn_odds/") for k in store.objects)
    # Nothing moved: a second poll writes nothing.
    assert poll(store, games, dates=[date(2026, 10, 5)], client=client) == 0  # type: ignore[arg-type]
    assert not any(k.startswith("external/odds/props/") for k in store.objects)  # no propBets link


def test_poll_with_prop_bets():
    board = {"events": [e for e in load("scoreboard_events.json")["events"] if e["id"] == "401892445"]}
    odds = load("odds_2026_401892445.json")
    odds["items"][0]["propBets"] = {"$ref": "http://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/"
                                            "401892445/competitions/401892445/odds/100/propBets?lang=en&region=us"}
    rosters = load_prop("espn_rosters.json")
    client = FakeClient({
        "scoreboard": board, "/propBets": load_prop("espn_propbets_401892445.json"),
        "/teams/20/roster": rosters["20"], "/teams/15/roster": rosters["15"], "401892445/odds": odds,
    })
    games = pl.DataFrame({
        "game_id": [2026020050], "season": [20262027], "season_type": ["R"],
        "game_date": [date(2026, 10, 5)], "home_abbr": ["TBL"], "away_abbr": ["PHI"], "is_final": [False],
    })
    store = FakeStore()
    nhl_rosters = load_prop("nhl_rosters.json")
    resolver = PlayerResolver(roster_loader=lambda team: nhl_rosters.get(team, {}))
    written = poll(store, games, dates=[date(2026, 10, 5)], client=client, resolver=resolver)  # type: ignore[arg-type]
    # 6 main + 7 p1 + 3 p2 + 12 team-total rungs.
    assert written == 28
    table = store.objects["external/odds/live/20262027/espn.parquet"]
    assert set(table["period"]) == {"game", "p1", "p2", "reg"}
    props = store.objects["external/odds/props/20262027/espn.parquet"]
    assert set(props["game_id"]) == {2026020050}
    assert props.height == 27
    # Kucherov, Konecny, Vasilevskiy are on the trimmed NHL rosters: all resolved.
    assert props["player_id"].null_count() == 0
    assert any("propBets" in url for url, _ in client.calls)
    bundle = next(v for k, v in store.objects.items() if k.startswith("raw/external/espn_odds/"))
    assert set(bundle["rosters"]) == {"20", "15"} and len(bundle["props"]) == 1
