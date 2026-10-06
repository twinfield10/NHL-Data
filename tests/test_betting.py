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
