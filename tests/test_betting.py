from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import polars as pl
import pytest

from nhl.betting import devig as D
from nhl.betting import lines as L


def test_implied_and_decimal():
    assert np.allclose(D.implied(np.array([-110.0, 100.0, 150.0])), [110 / 210, 0.5, 0.4])
    assert np.allclose(D.decimal(np.array([-200.0, 150.0])), [1.5, 2.5])


@pytest.mark.parametrize("method", D.METHODS)
def test_devig_sums_to_one_and_is_symmetric(method):
    a = np.array([-110.0, -250.0, 180.0])
    b = np.array([-110.0, 210.0, -220.0])
    pa, pb = D.fair(a, b, method), D.fair(b, a, method)
    assert np.allclose(pa + pb, 1.0, atol=1e-6)
    assert abs(pa[0] - 0.5) < 1e-9
    # The favourite stays the favourite and loses some margin.
    assert pa[1] > 0.65 and pa[1] < D.implied(np.array([-250.0]))[0]


def test_power_and_shin_load_the_longshot():
    a, b = np.array([-400.0]), np.array([300.0])
    mult, power = D.fair(a, b, "multiplicative")[0], D.fair(a, b, "power")[0]
    assert power > mult  # less margin removed from the favourite, more from the longshot


def _rows(book, game, market, point, line1, line2, p1, p2):
    s1, s2 = L.SIDES[market]
    t = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return [
        {"game_id": game, "book": book, "market": market, "point": point, "side": s1, "line": line1, "price": p1, "captured_at": t},
        {"game_id": game, "book": book, "market": market, "point": point, "side": s2, "line": line2, "price": p2, "captured_at": t},
    ]


def test_pair_clean_grade_and_consensus():
    rows = (
        _rows("DraftKings", 2023020001, "moneyline", "close", None, None, -150.0, 130.0)
        + _rows("BetMGM", 2023020001, "moneyline", "close", None, None, -140.0, 120.0)
        + _rows("SBR consensus", 2023020001, "moneyline", "close", None, None, -300.0, 250.0)  # ignored: single books exist
        + _rows("DraftKings", 2023020001, "total", "close", 6.0, 6.0, -110.0, -110.0)
        + _rows("BetMGM", 2023020001, "total", "close", 6.0, 6.0, -105.0, -115.0)
        + _rows("Caesars", 2023020001, "total", "close", 5.5, 5.5, -130.0, 110.0)  # minority line
        + _rows("DraftKings", 2023020001, "puckline", "close", -1.5, 1.5, 160.0, -190.0)
        + _rows("BetMGM", 2023020001, "puckline", "close", -1.5, -1.5, 160.0, -190.0)  # sides disagree: dropped
        + _rows("Caesars", 2023020001, "total", "last", -105.0, -105.0, 100.0, -120.0)  # line/price swapped: cleaned
    )
    paired = L.clean(L.pair_sides(pl.DataFrame(rows), "test"))
    assert paired.filter(pl.col("market") == "puckline").height == 1
    assert paired.filter((pl.col("market") == "total") & (pl.col("point") == "last")).is_empty()
    assert paired["season"].unique().to_list() == [20232024]

    c = D.consensus(paired, "multiplicative").sort("market")
    ml = c.filter(pl.col("market") == "moneyline").row(0, named=True)
    assert ml["books"] == 2 and 0.55 < ml["p_fair"] < 0.6
    tot = c.filter(pl.col("market") == "total").row(0, named=True)
    assert tot["line"] == 6.0 and tot["books"] == 2

    scores = pl.DataFrame({"game_id": [2023020001], "home_score": [4], "away_score": [2]})
    g = L.grade(paired, scores)
    y = dict(zip(g["market"] + ":" + g["line"].cast(pl.String).fill_null("-"), g["y"]))
    assert y["moneyline:-"] == 1.0
    assert y["puckline:-1.5"] == 1.0
    assert y["total:6.0"] is None  # 6 goals on a 6.0 total is a push
    assert y["total:5.5"] == 1.0


def test_canonical_book_names():
    names = pl.Series(["Caesars Sportsbook (Colorado)", "Caesars Sportsbook", "MGM", "DraftKings"])
    out = pl.select(L.canonical_book(pl.lit(names))).to_series().to_list()
    assert out == ["Caesars", "Caesars", "BetMGM", "DraftKings"]


def test_fit_logit_recovers_coefficients_and_model_probs_shape():
    from nhl.betting import evaluate as E

    rng = np.random.default_rng(0)
    x = rng.normal(size=(20000, 2))
    y = (rng.random(20000) < 1 / (1 + np.exp(-(0.1 + 0.8 * x[:, 0] + 0.3 * x[:, 1])))).astype(float)
    fit = E.fit_logit(x, y)
    assert np.allclose(fit.coef, [0.1, 0.8, 0.3], atol=0.06) and (fit.se < 0.05).all()

    prices = pl.DataFrame({
        "game_id": [1], "p_home_win": [0.6], "p_home_minus_1_5": [0.3], "p_away_minus_1_5": [0.2],
        **{f"p_over_{x}": [0.5] for x in E.TOTAL_LINES},
    })
    mp = E.model_probs(prices)
    assert mp.height == 3 + len(E.TOTAL_LINES)
    home_plus = mp.filter((pl.col("market") == "puckline") & (pl.col("line") == 1.5))["p_model"][0]
    assert abs(home_plus - 0.8) < 1e-12  # home +1.5 wins unless the away side wins by 2+


def test_attach_model_prices_whole_number_totals_from_score_matrix():
    from nhl.betting import evaluate as E
    from nhl.sim import markets as M

    k = M.MAX_GOALS + 1
    m = np.zeros((1, M.MATRIX_SIZE), dtype=np.float32)
    # 40% 4-2 (total 6, home by 2), 35% 3-2 (total 5), 25% 2-3 in OT (total 5).
    m[0, 0 * k * k + 4 * k + 2] = 0.40
    m[0, 0 * k * k + 3 * k + 2] = 0.35
    m[0, 1 * k * k + 2 * k + 3] = 0.25
    prices = pl.DataFrame({"game_id": [7], "score_matrix": [m[0]]}, schema={"game_id": pl.Int64, "score_matrix": pl.Array(pl.Float32, M.MATRIX_SIZE)})
    rows = pl.DataFrame({"game_id": [7, 7, 7, 7], "market": ["moneyline", "puckline", "total", "total"], "line": [None, -1.5, 6.0, 5.5]})
    out = E.attach_model(rows, prices).sort("market", "line")
    got = {(r["market"], r["line"]): (r["p_model"], r["p_push"]) for r in out.iter_rows(named=True)}
    assert abs(got[("moneyline", None)][0] - 0.75) < 1e-6
    assert abs(got[("puckline", -1.5)][0] - 0.40) < 1e-6
    assert abs(got[("total", 6.0)][1] - 0.40) < 1e-6 and got[("total", 6.0)][0] < 1e-6  # over 6.0 never wins; 40% push
    assert abs(got[("total", 5.5)][0] - 0.40) < 1e-6 and got[("total", 5.5)][1] == 0


class _Mem:
    def __init__(self, data=None):
        self.data = dict(data or {})
        self.bytes = {}

    def get_parquet(self, key):
        return self.data.get(key)

    def read_parquet_required(self, key):
        return self.data[key]

    def put_parquet(self, key, df):
        self.data[key] = df

    def list_keys(self, prefix):
        return sorted(k for k in self.data if k.startswith(prefix))


def _edges(rows):
    from datetime import date, datetime, timezone

    base = {"game_date": date(2026, 10, 6), "line": None, "book": "LowVig", "price": -110.0, "p_model_side": 0.6,
            "p_market_side": 0.5, "p": 0.55, "pregame_stamp": "S", "as_of": datetime(2026, 10, 6, 18, tzinfo=timezone.utc),
            "tier": "unvalidated"}
    return pl.DataFrame([{**base, **r} for r in rows])


def test_stakes_respect_bet_game_and_day_caps():
    from nhl.betting import edges as E

    e = _edges([
        {"game_id": 1, "market": "moneyline", "side": 1, "kelly": 0.05, "flagged": True},   # 5 u -> capped at 2
        {"game_id": 1, "market": "puckline", "side": 1, "kelly": 0.03, "flagged": True},    # 3 u -> 2; game 4 u -> 3
        {"game_id": 2, "market": "moneyline", "side": 2, "kelly": 0.01, "flagged": True},   # 1 u
        {"game_id": 3, "market": "total", "side": 1, "kelly": 0.04, "flagged": True},       # 4 u -> capped at 2
        {"game_id": 3, "market": "total", "side": 2, "kelly": 0.04, "flagged": False},      # not flagged: 0
    ])
    out = E._stakes(e, _Mem(), e["game_date"][0]).filter(pl.col("flagged"))
    st = dict(zip(zip(out["game_id"], out["market"]), out["stake_units"]))
    assert out.height == 4
    assert st[(1, "moneyline")] == 1.5 and st[(1, "puckline")] == 1.5 and st[(2, "moneyline")] == 1.0 and st[(3, "total")] == 2.0
    # Day cap: 8 u already in the ledger leaves 2 u for these 6 u.
    from nhl.betting import ledger
    from nhl.storage import keys
    row = {c: None for c in ledger.SCHEMA} | {"bet_id": "x", "kind": "paper", "game_id": 9, "game_date": e["game_date"][0],
                                              "market": "moneyline", "side": 1, "stake_units": 8.0, "tier": "unvalidated"}
    prior = pl.DataFrame([row], schema=ledger.SCHEMA)
    capped = E._stakes(e, _Mem({keys.BETS_LEDGER: prior}), e["game_date"][0])
    assert abs(capped["stake_units"].sum() - 2.0) < 0.02


def test_stakes_keep_placed_bets_and_count_them_against_the_caps():
    from nhl.betting import edges as E
    from nhl.betting import ledger
    from nhl.storage import keys

    e = _edges([
        {"game_id": 1, "market": "moneyline", "side": 1, "kelly": 0.05, "flagged": True},  # placed at 1.2 u
        {"game_id": 1, "market": "total", "side": 1, "kelly": 0.05, "flagged": True},      # new: game room 3 - 2.5
        {"game_id": 2, "market": "moneyline", "side": 2, "kelly": 0.05, "flagged": True},  # new
    ])
    day = e["game_date"][0]

    def bet(game_id, market, side, stake):
        return {c: None for c in ledger.SCHEMA} | {"bet_id": f"{game_id}{market}", "kind": "paper", "game_id": game_id,
                                                   "game_date": day, "market": market, "side": side, "stake_units": stake}

    prior = pl.DataFrame([bet(1, "moneyline", 1, 1.2), bet(1, "puckline", 2, 1.3), bet(7, "total", 1, 6.5)],
                         schema=ledger.SCHEMA)
    out = E._stakes(e, _Mem({keys.BETS_LEDGER: prior}), day)
    st = dict(zip(zip(out["game_id"], out["market"]), out["stake_units"]))
    assert st[(1, "moneyline")] == 1.2  # as placed, not resized
    # Game 1 has 2.5 u of its 3 u placed, so its new total gets 0.5 u; game 2's gets 2 u. The day has
    # 9 u placed, leaving 1 u for those 2.5 u, scaled together.
    assert st[(1, "total")] == 0.2 and st[(2, "moneyline")] == 0.8


def test_ledger_paper_once_then_grade_clv_and_result():
    from datetime import date, datetime, timezone

    from nhl.betting import ledger
    from nhl.storage import keys

    store = _Mem({keys.GAMES: pl.DataFrame({"game_id": [2026020044], "season": [20262027], "is_final": [True],
                                            "game_date": [date(2026, 10, 6)], "home_score": [3], "away_score": [2]})})
    e = _edges([{"game_id": 2026020044, "market": "moneyline", "side": 1, "price": 110.0, "stake_units": 1.0, "edge": 0.05}])
    assert ledger.add_paper(store, e) == 1 and ledger.add_paper(store, e) == 0  # same game/market/side: once
    # Closing consensus: home 55% fair, captured live.
    t = datetime(2026, 10, 6, 22, tzinfo=timezone.utc)
    live = pl.DataFrame({"game_id": [2026020044] * 2, "book": ["LowVig"] * 2, "market": ["moneyline"] * 2, "point": ["close"] * 2,
                         "line": [None, None], "price_1": [-125.0, -125.0], "price_2": [105.0, 105.0], "captured_at": [t, t],
                         "source": ["live"] * 2, "season": [20262027] * 2}, schema_overrides={"line": pl.Float64, "season": pl.Int32})
    import nhl.betting.lines as L
    orig = L.build
    L.build = lambda store_, season, alternates=False: live
    try:
        assert ledger.grade(store) == 1
    finally:
        L.build = orig
    row = ledger.load(store).row(0, named=True)
    assert row["result"] == "win" and abs(row["pnl_units"] - 1.1) < 1e-9
    assert row["p_close"] > 0.5 and abs(row["clv"] - (row["p_close"] * 2.1 - 1)) < 1e-9


def test_last_pregame_prices_keeps_each_games_final_run():
    from datetime import date

    from nhl.betting import edges as E
    from nhl.storage import keys

    day = date(2026, 10, 6)
    store = _Mem({
        keys.pregame_prices(day, "T1"): pl.DataFrame({"game_id": [1, 2], "p_home_win": [0.50, 0.40]}),
        keys.pregame_prices(day, "T2"): pl.DataFrame({"game_id": [2], "p_home_win": [0.45]}),  # game 1 started
    })
    got = E.last_pregame_prices(store, day).sort("game_id")
    assert got["p_home_win"].to_list() == [0.50, 0.45]
    assert got["stamp"].to_list() == ["T1", "T2"]
    assert E.last_pregame_prices(_Mem({}), day) is None


_START = datetime(2026, 10, 10, 23, tzinfo=timezone.utc)


def _poll(book, game, t, price_home, price_away=None, market="moneyline", alternate=False, start=_START):
    """Two odds-table rows (home/away) from one poll of a game starting at ``start``."""
    away = -price_home if price_away is None else price_away
    uid = f"game|{market}|game|main"
    base = {"book": book, "game_id": game, "captured_at": t, "start_time": start, "period": "game", "market": market,
            "subject": "game", "line": None, "is_alternate": alternate, "market_uid": uid, "price_point": "live"}
    return [{**base, "side": "home", "price": price_home}, {**base, "side": "away", "price": away}]


def _odds(rows):
    from nhl.odds.core import ODDS_SCHEMA

    return pl.DataFrame(rows, schema={k: v for k, v in ODDS_SCHEMA.items() if k in rows[0]})


def test_record_seen_keeps_latest_pregame_poll_per_market():
    from datetime import timedelta

    from nhl.odds.store import load_seen, record_seen
    from nhl.storage import keys

    start = _START
    store, key = _Mem(), keys.odds_seen(20262027, "test")
    assert record_seen(store, key, _odds(_poll("LowVig", 1, start - timedelta(hours=3), -120.0, 100.0))) == 2  # per side
    record_seen(store, key, _odds(_poll("LowVig", 1, start - timedelta(minutes=5), -120.0, 100.0)
                                  + _poll("LowVig", 2, start - timedelta(minutes=5), -110.0, -110.0, alternate=True)))
    # In-play polls never count as seen pregame.
    record_seen(store, key, _odds(_poll("LowVig", 1, start + timedelta(minutes=10), -300.0, 240.0)))
    seen = load_seen(store, keys.odds_seen_prefix(20262027)).sort("game_id")
    assert seen.height == 4  # each side, and the alternate rung's too
    assert seen["last_seen"].to_list() == [start - timedelta(minutes=5)] * 4


def test_live_close_drops_markets_pulled_before_puck_drop():
    from datetime import timedelta

    from nhl.storage import keys

    start = _START
    early, late = start - timedelta(hours=4), start - timedelta(minutes=5)
    # Transitions: each book quoted once, early, and never moved.
    odds = _odds(_poll("LowVig", 2026020050, early, -120.0, 100.0) + _poll("4Casters", 2026020050, early, -125.0, 108.0)
                 + _poll("DraftKings", 2026020050, early, -118.0, -102.0)
                 + _poll("4Casters", 2026020050, start + timedelta(minutes=20), -400.0, 320.0))  # in-play: not a close
    seen = pl.DataFrame({
        "book": ["LowVig", "4Casters"], "game_id": [2026020050] * 2,
        "market_uid": ["game|moneyline|game|main"] * 2, "last_seen": [late, early],
    }, schema={"book": pl.Utf8, "game_id": pl.Int64, "market_uid": pl.Utf8, "last_seen": pl.Datetime("us", "UTC")}).join(
        pl.DataFrame({"side": ["home", "away"]}), how="cross")
    store = _Mem({keys.odds(20262027, "x"): odds, keys.odds_seen(20262027, "x"): seen})
    close = L.live_lines(store, 20262027).filter(pl.col("point") == "close")
    got = {r["book"]: r["price_1"] for r in close.iter_rows(named=True)}
    # LowVig held its price to the last poll; 4Casters was pulled after its early quote;
    # DraftKings has no seen entry for the game (polled before the table) and is kept.
    assert got == {"LowVig": -120.0, "DraftKings": -118.0}


def test_exchanges_count_in_closing_consensus():
    rows = (_rows("4Casters", 2026020001, "moneyline", "close", None, None, -200.0, 180.0)
            + _rows("Novig", 2026020001, "moneyline", "close", None, None, -200.0, 200.0)
            + _rows("DraftKings", 2026020001, "moneyline", "close", None, None, -150.0, 130.0))
    c = D.consensus(L.pair_sides(pl.DataFrame(rows, schema_overrides={"line": pl.Float64}), "test"), "multiplicative").row(0, named=True)
    assert c["books"] == 3 and c["p_fair"] > 0.64  # the median is an exchange, not DraftKings


def _alt(rows):
    """Mark ``rows`` as an alternate rung (a book's second line comes from its ladder)."""
    return [{**r, "is_alternate": True} for r in rows]


def _pairs(rows):
    rows = [{"is_alternate": False, **r} for r in rows]
    return L.pair_sides(pl.DataFrame(rows, schema_overrides={"line": pl.Float64}), "test")


def test_primary_total_is_each_books_lowest_hold_line():
    g = 2026020080
    rows = (
        # A sportsbook with both lines: 6.5 at -140/+110 holds ~6.0%, 6 at -108/-107 ~3.6% -> 6.
        _rows("LowVig", g, "total", "close", 6.5, 6.5, -140.0, 110.0) + _alt(_rows("LowVig", g, "total", "close", 6.0, 6.0, -108.0, -107.0))
        + _rows("DraftKings", g, "total", "close", 6.0, 6.0, -110.0, -110.0)
        # An exchange holds less everywhere; it votes once, for its own lowest-hold line.
        + _rows("Novig", g, "total", "close", 6.5, 6.5, -125.0, 125.0) + _alt(_rows("Novig", g, "total", "close", 5.5, 5.5, -300.0, 290.0))
    )
    c = D.consensus(_pairs(rows), "multiplicative").row(0, named=True)
    assert c["line"] == 6.0 and c["books"] == 2
    by = {r["line"]: r for r in D.consensus_by_line(_pairs(rows), "multiplicative").iter_rows(named=True)}
    # Every line keeps its own over/under consensus, so a bet at 6.5 is judged at 6.5.
    assert by[6.5]["books"] == 2 and by[5.5]["books"] == 1 and 0.55 < by[6.5]["p_fair"] < 0.57


def test_primary_tie_breaks_on_books_then_hold_not_lower_line():
    g = 2026020081
    rows = (_rows("4Casters", g, "total", "close", 6.0, 6.0, -105.0, -105.0)
            + _rows("DraftKings", g, "total", "close", 6.5, 6.5, -115.0, -105.0)
            + _rows("LowVig", g, "total", "close", 6.5, 6.5, -112.0, -108.0) + _alt(_rows("LowVig", g, "total", "close", 6.0, 6.0, -150.0, 120.0)))
    # One vote each for 6 (4Casters) and 6.5 (DraftKings, LowVig): 6.5 has two votes.
    assert D.consensus(_pairs(rows), "multiplicative")["line"].to_list() == [6.5]
    tie = _rows("4Casters", g, "total", "close", 6.0, 6.0, -105.0, -105.0) + _rows("DraftKings", g, "total", "close", 6.5, 6.5, -115.0, -105.0)
    # One vote each, one book each: the lower median hold (6.0 here) wins, not the lower line by default.
    assert D.consensus(_pairs(tie), "multiplicative")["line"].to_list() == [6.0]


def test_pair_sides_pairs_each_rung_at_its_own_line():
    g = 2026020082
    rows = (_rows("4Casters", g, "puckline", "close", -1.5, 1.5, 150.0, -170.0)
            + _rows("4Casters", g, "puckline", "close", 1.5, -1.5, -300.0, 250.0)
            + _rows("4Casters", g, "total", "close", 5.5, 5.5, -150.0, 130.0) + _rows("4Casters", g, "total", "close", 6.5, 6.5, 120.0, -140.0))
    rows = [{**r, "is_alternate": r["line"] in (1.5, 6.5) if r["side"] in ("home", "over") else r["line"] in (-1.5, 6.5)} for r in rows]
    p = _pairs(rows)
    got = sorted(zip(p["market"], p["line"], p["price_1"], p["price_2"]))
    assert got == [("puckline", -1.5, 150.0, -170.0), ("puckline", 1.5, -300.0, 250.0),
                   ("total", 5.5, -150.0, 130.0), ("total", 6.5, 120.0, -140.0)]


def test_exchange_primary_ties_on_hold_go_to_the_line_nearest_even():
    g = 2026020083
    # Novig's 7.5 holds 0.42%, its 5.5 0.51%: a tie within tolerance, and 5.5 is nearer 50/50.
    rows = (_rows("Novig", g, "total", "close", 5.5, 5.5, -182.0, 178.0)
            + _alt(_rows("Novig", g, "total", "close", 7.5, 7.5, 208.0, -212.0))
            + _alt(_rows("Novig", g, "total", "close", 4.5, 4.5, -525.0, 466.0)))
    assert D.consensus(_pairs(rows), "multiplicative")["line"].to_list() == [5.5]


def test_live_close_drops_a_side_the_book_stopped_quoting():
    from datetime import timedelta

    from nhl.storage import keys

    start = _START
    early, late = start - timedelta(hours=4), start - timedelta(minutes=5)
    # 4Casters' away side went too thin to fill after its early quote; its home side stayed.
    odds = _odds(_poll("4Casters", 2026020051, early, -125.0, 108.0))
    seen = pl.DataFrame({"book": ["4Casters"] * 2, "game_id": [2026020051] * 2, "market_uid": ["game|moneyline|game|main"] * 2,
                         "side": ["home", "away"], "last_seen": [late, early]},
                        schema_overrides={"last_seen": pl.Datetime("us", "UTC")})
    store = _Mem({keys.odds(20262027, "x"): odds, keys.odds_seen(20262027, "x"): seen})
    # Without both sides at the close there is no closing pair.
    assert L.live_lines(store, 20262027).filter(pl.col("point") == "close").is_empty()


def test_history_fills_only_games_we_never_polled_and_close_check_compares():
    from datetime import timedelta

    from nhl.storage import keys

    start = _START
    polled, unpolled = 2026020060, 2026020061
    live = _odds(_poll("DraftKings", polled, start - timedelta(minutes=5), -150.0, 130.0))
    hist = pl.DataFrame([
        {"book": "DraftKings", "game_id": g, "captured_at": start, "period": "game", "market": "moneyline", "side": side,
         "line": None, "price": price, "is_alternate": False, "price_point": "close"}
        for g, (home, away) in ((polled, (-155.0, 135.0)), (unpolled, (-120.0, 100.0)))
        for side, price in (("home", home), ("away", away))
    ], schema_overrides={"line": pl.Float64, "captured_at": pl.Datetime("us", "UTC")})
    store = _Mem({keys.odds(20262027, "espn"): live, keys.odds_history(20262027): hist})
    close = L.build(store, 20262027).filter(pl.col("point") == "close")
    got = {(r["game_id"], r["source"]): r["price_1"] for r in close.iter_rows(named=True)}
    # The polled game keeps only our live close; ESPN's close fills the game we never polled.
    assert got == {(polled, "live"): -150.0, (unpolled, "espn_history"): -120.0}
    chk = L.close_check(store, 20262027).row(0, named=True)
    assert (chk["book"], chk["market"], chk["games"], chk["same_line"], chk["identical"]) == ("DraftKings", "moneyline", 1, 1.0, 0.0)
    assert 0 < chk["mean_abs_dp"] < 0.02


def test_book_rows_compare_each_book_with_the_consensus_at_its_own_line():
    from datetime import timedelta

    from nhl.betting import edges as E
    from nhl.storage import keys

    t = _START - timedelta(hours=2)
    g = 2026020095

    def total(book, line, over, under, alternate=False):
        uid = f"game|total|game|{line if alternate else 'main'}"
        base = {"book": book, "game_id": g, "captured_at": t, "start_time": _START, "period": "game", "market": "total",
                "subject": "game", "line": line, "is_alternate": alternate, "market_uid": uid, "price_point": "live"}
        return [{**base, "side": "over", "price": over}, {**base, "side": "under", "price": under}]

    # Two books hang 6, LowVig hangs 6.5 as its main line; Novig also quotes 6.5 on its ladder.
    odds = _odds(total("DraftKings", 6.0, -110.0, -110.0) + total("Novig", 6.0, -102.0, 102.0)
                 + total("Novig", 6.5, 140.0, -140.0, alternate=True) + total("LowVig", 6.5, 120.0, -150.0))
    store = _Mem({keys.odds(20262027, "x"): odds})
    lv = {f: E.book_rows(store, 20262027, [g], by_line=f).filter(pl.col("book") == "LowVig").row(0, named=True)
          for f in (False, True)}
    assert lv[False]["p_market"] == pytest.approx(lv[False]["p_book"])  # compared with itself only
    assert lv[True]["cons_books"] == 2 and lv[True]["p_market"] != pytest.approx(lv[True]["p_book"])
