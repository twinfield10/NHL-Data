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
            "tier": "unvalidated", "qualifies": True}
    return pl.DataFrame([{**base, **r} for r in rows])


def test_stakes_respect_bet_game_and_day_caps():
    from nhl.betting import edges as E

    e = _edges([
        {"game_id": 1, "market": "moneyline", "side": 1, "kelly": 0.05, "flagged": True},   # 5 u -> capped at 2
        {"game_id": 1, "market": "puckline", "side": 1, "kelly": 0.03, "flagged": True},    # 3 u -> 2; game 4 u -> 3
        {"game_id": 2, "market": "moneyline", "side": 2, "kelly": 0.01, "flagged": True},   # 1 u
        {"game_id": 3, "market": "total", "side": 1, "kelly": 0.04, "flagged": False},      # track only: 0
    ])
    out = E._stakes(e, _Mem(), e["game_date"][0]).sort("game_id", "market")
    st = dict(zip(zip(out["game_id"], out["market"]), out["stake_units"]))
    assert st[(1, "moneyline")] == 1.5 and st[(1, "puckline")] == 1.5 and st[(2, "moneyline")] == 1.0 and st[(3, "total")] == 0.0
    # Day cap: 8 u already in the ledger leaves 2 u for these 4 u.
    from nhl.betting import ledger
    from nhl.storage import keys
    row = {c: None for c in ledger.SCHEMA} | {"bet_id": "x", "kind": "paper", "game_id": 9, "game_date": e["game_date"][0],
                                              "market": "moneyline", "side": 1, "stake_units": 8.0, "tier": "unvalidated"}
    prior = pl.DataFrame([row], schema=ledger.SCHEMA)
    capped = E._stakes(e, _Mem({keys.BETS_LEDGER: prior}), e["game_date"][0])
    assert abs(capped["stake_units"].sum() - 2.0) < 0.02


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
    L.build = lambda store_, season: live
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
