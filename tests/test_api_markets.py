from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import polars as pl
import pytest

from nhl.api import lineupstats
from nhl.api import markets as mk
from nhl.betting import blend
from nhl.sim.markets import MAX_GOALS, overtime_split, split_three_way, three_way

UTC = timezone.utc


def _matrix(cells: dict[tuple[int, int, int], float]) -> np.ndarray:
    """Score matrix from ``{(ended, home, away): p}``."""
    k = MAX_GOALS + 1
    m = np.zeros(3 * k * k, dtype=np.float32)
    for (e, h, a), p in cells.items():
        m[e * k * k + h * k + a] = p
    return m


def test_three_way_counts_overtime_as_the_draw():
    m = _matrix({(0, 3, 1): 0.5, (0, 1, 2): 0.2, (1, 3, 2): 0.2, (2, 2, 1): 0.1})
    home, draw, away = (x[0] for x in three_way(m[None]))
    assert home == pytest.approx(0.5) and away == pytest.approx(0.2) and draw == pytest.approx(0.3)


def _quote(book, market, side, line, price, minute, period="game"):
    return {"book": book, "game_id": 1, "market": market, "side": side, "line": line, "price": price,
            "captured_at": datetime(2026, 10, 8, 18, minute, tzinfo=UTC), "period": period, "is_alternate": False}


def test_replay_builds_consensus_at_the_main_line_and_skips_post_start_quotes():
    odds = pl.DataFrame([
        _quote("A", "total", "over", 6.5, -110, 0), _quote("A", "total", "under", 6.5, -110, 0),
        _quote("B", "total", "over", 6.5, -105, 1), _quote("B", "total", "under", 6.5, -115, 1),
        _quote("C", "total", "over", 5.5, -150, 2), _quote("C", "total", "under", 5.5, 125, 2),
        _quote("A", "total", "over", 6.5, 300, 59),  # after puck drop: ignored
        _quote("A", "moneyline_3way", "home", None, 150, 3, "reg"), _quote("A", "moneyline_3way", "away", None, 200, 3, "reg"),
    ])
    quotes = mk.game_quotes(odds, 1, datetime(2026, 10, 8, 18, 30, tzinfo=UTC))
    history, state = mk.replay(quotes)
    now = mk.current(history)
    assert now["total"]["line"] == 6.5 and now["total"]["books"] == 2
    assert now["total"]["best"]["over"] == {"price": -105, "book": "B"}
    assert 0.48 < now["total"]["fair"]["over"] < 0.5  # A is a coin flip, B shades the under
    assert set(state["total"]) == {"A", "B", "C"}
    assert "moneyline_3way" not in now  # only two of three sides quoted


def test_replay_three_way_devigs_all_three_sides():
    odds = pl.DataFrame([_quote("A", "moneyline_3way", s, None, p, 0, "reg") for s, p in (("home", 120), ("draw", 300), ("away", 180))])
    cons = mk.current(mk.replay(mk.game_quotes(odds, 1, None))[0])["moneyline_3way"]
    assert sum(cons["fair"].values()) == pytest.approx(1.0)
    assert cons["fair"]["home"] > cons["fair"]["away"] > cons["fair"]["draw"]


def test_model_probs_drop_pushes_on_whole_totals():
    m = _matrix({(0, 3, 3): 0.0, (0, 4, 2): 0.4, (0, 3, 2): 0.4, (0, 2, 1): 0.2})
    p = mk.model_probs(m, "total", 6.0)
    assert p["over"] == pytest.approx(0.0) and p["under"] == pytest.approx(0.6 / 0.6 - 0.0)
    pl_ = mk.model_probs(m, "puckline", -1.5)
    assert pl_["home"] == pytest.approx(0.4)


def test_three_way_card_edges_use_the_best_price():
    m = _matrix({(0, 3, 1): 0.5, (0, 1, 2): 0.3, (1, 3, 2): 0.2})
    cons = {"books": 1, "fair": {"home": 0.45, "draw": 0.22, "away": 0.33},
            "best": {s: {"price": 100.0, "book": "A"} for s in ("home", "draw", "away")}}
    card = mk.three_way_card(m, cons)
    home = next(s for s in card["sides"] if s["side"] == "home")
    assert home["edge"] == pytest.approx(0.5 * 2 - 1)
    assert mk.three_way_card(None, None) is None


def test_split_three_way_with_the_simulators_inputs_is_its_own_three_way():
    m = _matrix({(0, 3, 1): 0.45, (0, 1, 2): 0.3, (1, 3, 2): 0.15, (2, 2, 3): 0.1})[None]
    p_ot, p_home_ot = overtime_split(m)
    assert p_ot[0] == pytest.approx(0.25) and p_home_ot[0] == pytest.approx(0.6)
    p_win = 0.45 + 0.15
    assert np.allclose(split_three_way(np.array([p_win]), p_ot, p_home_ot), three_way(m))


def test_three_way_blend_follows_the_moneyline_blend_and_calibrated_overtime():
    m = _matrix({(0, 3, 1): 0.45, (0, 1, 2): 0.3, (1, 3, 2): 0.15, (2, 2, 3): 0.1})
    # Market-only moneyline blend; overtime calibrated to a flat 30%.
    model = {"markets": {"moneyline": {"other": {"coef": [0.0, 1.0, 0.0]}}},
             "overtime": {"coef": [float(np.log(0.3 / 0.7)), 0.0], "feature": "abs_logit_p_home_win"}}
    tw = blend.three_way(model, "other", m, 0.5)
    assert tw["blend"]["draw"] == pytest.approx(0.3) and tw["model"]["draw"] == pytest.approx(0.3)
    assert tw["blend"]["home"] == pytest.approx(0.5 - 0.3 * 0.6)  # market moneyline less the OT games home wins
    assert sum(tw["blend"].values()) == pytest.approx(1.0)
    assert tw["model"]["home"] == pytest.approx(0.6 - 0.3 * 0.6)
    # No stored blend: the simulator's own split, no blend.
    raw = blend.three_way(None, "other", m, 0.5)
    assert raw["blend"] is None and raw["model"]["draw"] == pytest.approx(0.25)


def test_three_way_card_edges_use_the_blend_when_there_is_a_moneyline():
    m = _matrix({(0, 3, 1): 0.5, (0, 1, 2): 0.3, (1, 3, 2): 0.2})
    model = {"markets": {"moneyline": {"other": {"coef": [0.0, 1.0, 0.0]}}}}
    cons = {"books": 1, "fair": {"home": 0.45, "draw": 0.22, "away": 0.33},
            "best": {s: {"price": 100.0, "book": "A"} for s in ("home", "draw", "away")}}
    card = mk.three_way_card(m, cons, {"fair": {"home": 0.6, "away": 0.4}}, model)
    home = next(s for s in card["sides"] if s["side"] == "home")
    assert home["p_blend"] == pytest.approx(0.6 - 0.2) and home["edge"] == pytest.approx(0.4 * 2 - 1)


def test_fit_overtime_gives_close_games_more_overtimes():
    rng = np.random.default_rng(0)
    p_win = rng.uniform(0.25, 0.75, 20_000)
    true = 1 / (1 + np.exp(-(-1.1 - 0.5 * np.abs(np.log(p_win / (1 - p_win))))))
    prices = pl.DataFrame({"game_id": np.arange(p_win.size), "p_home_win": p_win})
    games = pl.DataFrame({"game_id": np.arange(p_win.size), "season_type": "R",
                          "last_period": np.where(rng.random(p_win.size) < true, 4, 3)})
    fit = blend.fit_overtime(prices, games)
    assert fit["n"] == p_win.size and fit["coef"][1] == pytest.approx(-0.5, abs=0.2)
    close, lopsided = blend.overtime({"overtime": fit}, np.array([0.5, 0.8]), np.array([0.2, 0.2]))
    assert close > 0.24 > lopsided
    # A blend stored before the calibration keeps the simulator's rate.
    assert blend.overtime({}, np.array([0.5]), np.array([0.21]))[0] == pytest.approx(0.21)


def test_unit_groups_and_records():
    lineup = [{"player_id": 1, "slot": "f1", "pp_unit": 1, "pk_unit": None},
              {"player_id": 2, "slot": "f1", "pp_unit": 1, "pk_unit": None},
              {"player_id": 3, "slot": "d1", "pp_unit": None, "pk_unit": 2},
              {"player_id": None, "slot": "f1", "pp_unit": None, "pk_unit": None}]
    groups = {g["slot"]: g for g in lineupstats.unit_groups(lineup)}
    assert groups["f1"]["player_ids"] == [1, 2] and groups["pp1"]["kind"] == "PP" and groups["pk2"]["player_ids"] == [3]
    units = pl.DataFrame({"kind": ["F", "F"], "player_ids": [[1, 2], [1, 2]], "toi_s": [60.0, 40.0], "games": [1, 2],
                          "xgf": [0.5, 0.1], "xga": [0.2, 0.1], "gf": [1, 0], "ga": [0, 1]})
    rec = lineupstats.unit_record(units, "F", [2, 1])
    assert rec["toi_s"] == 100.0 and rec["games"] == 3
    assert lineupstats.unit_record(units, "D", [1, 2]) is None
    # A game-time decision and his backup share a slot: the likelier one fills the line.
    gtd = [{"player_id": i, "slot": "f2", "pp_unit": None, "pk_unit": None, "p_dressed": p}
           for i, p in ((1, 1.0), (2, 1.0), (3, 0.3), (4, 0.7))]
    assert lineupstats.unit_groups(gtd)[0]["player_ids"] == [1, 2, 4]


def test_goalie_season_and_onice():
    starts = pl.DataFrame({"starter": [9, 9, 8], "shots_against": [30, 20, 25], "goals_against": [3, 1, 2],
                           "xga": [2.5, 2.0, 2.0], "gsax": [-0.5, 1.0, 0.0]})
    g = lineupstats.goalie_season(starts, 9)
    assert g["starts"] == 2 and g["sv_pct"] == pytest.approx(46 / 50) and g["gsax"] == pytest.approx(0.5)
    assert lineupstats.goalie_season(starts, 7) is None
    summary = pl.DataFrame({"player_id": [1, 1], "games": [10, 5], "toi_s": [3000, 1000],
                            "actual_f": [3.0, 2.0], "actual_a": [2.0, 2.0]})
    o = lineupstats.onice(summary, [1])[1]
    assert o["games"] == 15 and o["xgf60"] == pytest.approx(2.75)


def test_lines_source_takes_latest_version_before_puck_drop():
    df = pl.DataFrame({
        "team": ["VGK", "VGK", "VGK"], "source_name": ["a", "b", "c"], "source_url": ["u1", "u2", "u3"],
        "updated_at": [datetime(2026, 10, 8, h, tzinfo=UTC) for h in (10, 12, 23)],
        "captured_at": [datetime(2026, 10, 8, h, tzinfo=UTC) for h in (10, 12, 23)],
    })
    tweets = pl.DataFrame({"url": ["u2"], "available": [True], "text": ["lines"], "author_name": ["X"],
                           "author_handle": ["x"], "created_at": [datetime(2026, 10, 8, 12, tzinfo=UTC)]})
    src = lineupstats.lines_source(df, tweets, "VGK", datetime(2026, 10, 8, 20, tzinfo=UTC))
    assert src["source_name"] == "b" and src["tweet"]["text"] == "lines"
