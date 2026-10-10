"""Phase D: prop market probabilities, edges and the props ledger (no network)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import polars as pl
import pytest

from nhl.props import ledger, live, project

NOW = datetime(2026, 10, 8, 18, 0, tzinfo=timezone.utc)


def quote(book: str, side: str, price: float, line: float = 0.5, player: int = 1, prop: str = "points") -> dict[str, Any]:
    return {"book": book, "game_id": 10, "player_id": player, "prop_type": prop, "line": line, "side": side,
            "price": price}


def implied(p: float) -> float:
    return 100 / (p + 100) if p > 0 else -p / (-p + 100)


def test_market_probs_two_way_and_ladder() -> None:
    q = pl.DataFrame([
        quote("DK", "over", -120), quote("DK", "under", 100),  # two-way
        quote("FD", "over", 110),  # ladder rung, FD has no two-way: default margin
        quote("DK", "over", 300, line=1.5),  # DK ladder rung: DK's own margin from its two-way
    ])
    m = live.market_probs(q)
    dk = m.filter((pl.col("book") == "DK") & (pl.col("line") == 0.5))
    io, iu = implied(-120), implied(100)
    assert dk["p_book"][0] == pytest.approx(io / (io + iu))
    assert dk["two_way"][0]
    fd = m.filter(pl.col("book") == "FD")
    assert fd["p_book"][0] == pytest.approx(implied(110) / (1 + live.DEFAULT_MARGIN))
    rung = m.filter(pl.col("line") == 1.5)
    assert rung["p_book"][0] == pytest.approx(implied(300) / (io + iu))
    # Consensus at 0.5: median of DK and FD; one line, two books.
    both = m.filter(pl.col("line") == 0.5)
    assert both["books"].to_list() == [2, 2]
    assert both["p_market"][0] == pytest.approx((dk["p_book"][0] + fd["p_book"][0]) / 2)


def test_market_probs_prefers_two_way_books() -> None:
    # Gauthier assists O0.5, 2026-10-09: two two-way books and two over-only books.
    q = pl.DataFrame([quote("DK", "over", 170), quote("DK", "under", -235), quote("4C", "over", 150),
                      quote("4C", "under", -189), quote("FD", "over", 110), quote("LV", "over", 100)])
    m = live.market_probs(q)
    two = [implied(170) / (implied(170) + implied(-235)), implied(150) / (implied(150) + implied(-189))]
    assert m["p_market"][0] == pytest.approx(sum(two) / 2)  # over-only quotes left out
    assert set(m["books"]) == {4} and set(m["two_way_books"]) == {2}
    # One two-way book: not enough, so every book counts.
    m1 = live.market_probs(q.filter(pl.col("book") != "4C"))
    assert m1["two_way_books"][0] == 1 and m1["p_market"][0] == pytest.approx(m1["p_book"].median())


def test_market_probs_outlier() -> None:
    q = pl.DataFrame([quote("A", "over", -150), quote("A", "under", 130), quote("B", "over", -150),
                      quote("B", "under", 130), quote("C", "over", 250), quote("C", "under", -300)])
    m = live.market_probs(q)
    assert m.filter(pl.col("outlier"))["book"].to_list() == ["C"]


def projections(p1: float = 0.6) -> pl.DataFrame:
    row: dict[str, Any] = {"game_id": 10, "player_id": 1}
    for prop_type, stat in live.STATS.items():
        for k in live.THRESHOLDS[stat]:
            row[f"p_{stat}_{k}"] = p1 if k == 1 else p1 / (2 * k)
    return pl.DataFrame([row])


def test_price_quotes_best_book_and_blend() -> None:
    q = pl.DataFrame([quote("DK", "over", -120), quote("DK", "under", 100), quote("FD", "over", 110),
                      quote("FD", "under", -140)])
    best = live.price_quotes(live.market_probs(q), projections(0.6))
    over = best.filter(pl.col("side") == "over").row(0, named=True)
    assert over["book"] == "FD"  # +110 beats -120 for the same probability
    assert over["p_model_side"] == pytest.approx(0.6)
    lo, hi = sorted([over["p_model"], over["p_market"]])
    assert lo < over["p_blend"] < hi  # the blend sits between model and market
    assert over["edge"] == pytest.approx(over["p"] * (1 + 110 / 100) - 1)
    under = best.filter(pl.col("side") == "under").row(0, named=True)
    assert under["p"] == pytest.approx(1 - over["p"])


def test_lines_above_the_projection_are_not_priced() -> None:
    q = pl.DataFrame([quote("DK", "over", 5000, line=4.5)])
    assert live.price_quotes(live.market_probs(q), projections()).is_empty()


def test_calibration_keeps_ladder_order() -> None:
    df = pl.DataFrame({"p_points_1": [0.5, 0.05], "p_points_2": [0.49, 0.049], "p_points_3": [0.1, 0.01],
                       "p_points_4": [0.02, 0.001]})
    out = project.calibrate(df, {"points": (1, 2, 3, 4)})
    for row in out.iter_rows():
        assert list(row) == sorted(row, reverse=True)
    a, b = project.CALIBRATION[("points", 1)]
    import math
    assert out["p_points_1"][0] == pytest.approx(1 / (1 + math.exp(-a)))  # logit(0.5) = 0


# ---------------------------------------------------------------------------- ledger --
class FakeStore:
    def __init__(self, parquet: dict[str, pl.DataFrame] | None = None) -> None:
        self.parquet = dict(parquet or {})

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        return self.parquet.get(key)

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        self.parquet[key] = df
        return key

    def read_parquet_required(self, key: str) -> pl.DataFrame:
        return self.parquet[key]

    def list_keys(self, prefix: str) -> list[str]:
        return [k for k in self.parquet if k.startswith(prefix)]


def edge_row(**kw: Any) -> dict[str, Any]:
    base = {"game_id": 2026020061, "game_date": date(2026, 10, 8), "player_id": 8480000, "player_name": "Tim Stützle",
            "team": "OTT", "prop_type": "points", "line": 0.5, "side": "over", "book": "FanDuel", "price": 120.0,
            "stake_units": 0.25, "p_model_side": 0.55, "p_market_side": 0.47, "p": 0.51, "edge": 0.12, "books": 3,
            "pregame_stamp": "20261008T130000Z", "as_of": NOW}
    return base | kw


def test_add_paper_once_per_bet() -> None:
    store = FakeStore()
    e = pl.DataFrame([edge_row(), edge_row(line=1.5, price=300.0)])
    assert ledger.add_paper(store, e) == 2  # type: ignore[arg-type]
    assert ledger.add_paper(store, pl.DataFrame([edge_row(price=150.0)])) == 0  # type: ignore[arg-type]
    led = ledger.load(store)  # type: ignore[arg-type]
    assert led.height == 2 and led.filter(pl.col("line") == 0.5)["price"][0] == 120.0
    assert ledger.day_stakes(store, date(2026, 10, 8)) == pytest.approx(0.5)  # type: ignore[arg-type]


def test_grade(monkeypatch: pytest.MonkeyPatch) -> None:
    from nhl.storage import keys

    games = pl.DataFrame({"game_id": [2026020061], "season": [20262027], "is_final": [True],
                          "start_time_et": ["2026-10-08T19:00:00"], "game_date": [date(2026, 10, 8)],
                          "home_abbr": ["OTT"], "away_abbr": ["PHI"]})
    logs = pl.DataFrame({"game_id": [2026020061] * 2, "player_id": [8480000, 8480000], "strength": ["all", "PP"],
                         "position": ["C", "C"], "goals": [1, 1], "a1": [0, 0], "a2": [1, 0], "isf": [4, 2], "blocks": [1, 0],
                         "sa": [0, 0], "ga": [0, 0]})
    store = FakeStore({keys.GAMES: games, keys.player_game_logs(20262027): logs})
    ledger.add_paper(store, pl.DataFrame([  # type: ignore[arg-type]
        edge_row(), edge_row(line=2.5, price=600.0, side="over"), edge_row(player_id=8479999, player_name="Scratched")]))
    close = pl.DataFrame({"book": ["FanDuel", "FanDuel"], "game_id": [2026020061] * 2, "player_id": [8480000] * 2,
                          "prop_type": ["points"] * 2, "line": [0.5, 0.5], "side": ["over", "under"],
                          "price": [-150.0, 120.0]})
    monkeypatch.setattr("nhl.props.live.latest_quotes", lambda *a, **k: close)
    assert ledger.grade(store) == 3  # type: ignore[arg-type]
    led = ledger.load(store).sort("line")  # type: ignore[arg-type]
    res = {(r["player_name"], r["line"]): r for r in led.iter_rows(named=True)}
    win = res[("Tim Stützle", 0.5)]
    assert win["stat"] == 2.0 and win["result"] == "win" and win["pnl_units"] == pytest.approx(0.25 * 1.2)
    assert win["close_price"] == -150.0
    io, iu = implied(-150), implied(120)
    assert win["p_close"] == pytest.approx(io / (io + iu))
    assert win["clv"] == pytest.approx(win["p_close"] * 2.2 - 1)
    assert res[("Tim Stützle", 2.5)]["result"] == "loss"
    assert res[("Scratched", 0.5)]["result"] == "void" and res[("Scratched", 0.5)]["pnl_units"] == 0.0
    assert ledger.grade(store) == 0  # type: ignore[arg-type]


def test_stakes_count_placed_bets_against_the_caps() -> None:
    store = FakeStore()
    placed = [edge_row(player_id=p, line=0.5, stake_units=0.5) for p in range(1, 10)]  # 4.5 u on 9 players
    placed.append(edge_row(player_id=20, line=1.5, stake_units=0.5, prop_type="goals"))  # player 20 has 0.5 u in
    ledger.add_paper(store, pl.DataFrame(placed))  # type: ignore[arg-type]

    def row(player_id: int, prop: str = "points", line: float = 0.5) -> dict[str, Any]:
        return {"game_id": 2026020061, "player_id": player_id, "prop_type": prop, "line": line, "side": "over",
                "flagged": True, "kelly": 0.01}  # 1 u at full size, capped at 0.5 u per bet

    e = pl.DataFrame([row(1), row(20), row(20, "assists"), row(30)])
    out = {(r["player_id"], r["prop_type"]): r["stake_units"]
           for r in live._stakes(e, store, date(2026, 10, 8)).iter_rows(named=True)}  # type: ignore[arg-type]
    assert out[(1, "points")] == 0.5  # already placed: keeps its stake, adds nothing
    # 5 u day cap with 5.0 u placed: nothing left for new bets.
    assert out[(20, "points")] == out[(20, "assists")] == out[(30, "points")] == 0.0


def test_stakes_player_cap_counts_placed() -> None:
    store = FakeStore()
    ledger.add_paper(store, pl.DataFrame([edge_row(player_id=20, prop_type="goals", stake_units=0.8)]))  # type: ignore[arg-type]
    e = pl.DataFrame([{"game_id": 2026020061, "player_id": 20, "prop_type": "points", "line": 0.5, "side": "over",
                       "flagged": True, "kelly": 0.01}])
    out = live._stakes(e, store, date(2026, 10, 8))  # type: ignore[arg-type]
    assert out["stake_units"][0] == pytest.approx(0.2)  # 1 u per player-game, 0.8 already in


def test_broken_two_way_is_skipped() -> None:
    q = pl.DataFrame([quote("DK", "over", 135), quote("DK", "under", 140),  # sums to 0.84: not a market
                      quote("FD", "over", -188), quote("FD", "under", 140), quote("LV", "over", -167), quote("LV", "under", 128)])
    m = live.market_probs(q)
    dk = m.filter(pl.col("book") == "DK").row(0, named=True)
    assert dk["outlier"]
    assert dk["books"] == 2  # the consensus counts FD and LV only
    best = live.price_quotes(m, projections(0.6))
    assert "DK" not in best["book"].to_list()


def test_grade_shots_and_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    from nhl.storage import keys

    games = pl.DataFrame({"game_id": [2026020061], "season": [20262027], "is_final": [True],
                          "start_time_et": ["2026-10-08T19:00:00"], "game_date": [date(2026, 10, 8)],
                          "home_abbr": ["OTT"], "away_abbr": ["PHI"]})
    logs = pl.DataFrame({"game_id": [2026020061] * 2, "player_id": [8480000, 8475000], "strength": ["all", "all"],
                         "position": ["C", "G"], "goals": [0, 0], "a1": [0, 0], "a2": [0, 0], "isf": [4, 0], "blocks": [2, 0],
                         "sa": [0, 30], "ga": [0, 2]})
    store = FakeStore({keys.GAMES: games, keys.player_game_logs(20262027): logs})
    ledger.add_paper(store, pl.DataFrame([  # type: ignore[arg-type]
        edge_row(prop_type="shots", line=3.5), edge_row(prop_type="blocks", line=2.5),
        edge_row(player_id=8475000, player_name="Goalie", prop_type="saves", line=27.5, side="over")]))
    monkeypatch.setattr("nhl.props.live.latest_quotes", lambda *a, **k: pl.DataFrame(
        schema={"book": pl.String, "game_id": pl.Int64, "player_id": pl.Int64, "prop_type": pl.String, "line": pl.Float64,
                "side": pl.String, "price": pl.Float64}))
    assert ledger.grade(store) == 3  # type: ignore[arg-type]
    res = {r["prop_type"]: (r["stat"], r["result"]) for r in ledger.load(store).iter_rows(named=True)}  # type: ignore[arg-type]
    assert res == {"shots": (4.0, "win"), "blocks": (2.0, "loss"), "saves": (28.0, "win")}


# ------------------------------------------------------------------------ live view --
def test_live_view_status_clv_and_timing() -> None:
    utc = timezone.utc
    start = datetime(2026, 10, 9, 0, 0, tzinfo=utc)
    base = {"kind": "paper", "game_id": 1, "game_date": date(2026, 10, 8), "prop_type": "assists", "line": 0.5,
            "side": "over", "book": "FanDuel", "price": 172.0, "stake_units": 0.3, "clv": None, "result": None,
            "pnl_units": None, "graded_at": None}
    bets = pl.DataFrame([
        base | {"bet_id": "a", "player_id": 1, "placed_at": datetime(2026, 10, 8, 14, 0, tzinfo=utc)},  # still flagged
        base | {"bet_id": "b", "player_id": 2, "placed_at": datetime(2026, 10, 8, 22, 0, tzinfo=utc)},  # faded
        base | {"bet_id": "c", "player_id": 3, "placed_at": datetime(2026, 10, 8, 23, 30, tzinfo=utc)},  # line pulled
        base | {"bet_id": "d", "player_id": 4, "placed_at": datetime(2026, 10, 7, 20, 0, tzinfo=utc), "clv": 0.1,
                "result": "win", "pnl_units": 0.5, "graded_at": datetime(2026, 10, 9, 5, 0, tzinfo=utc)},
    ], schema_overrides={"clv": pl.Float64, "result": pl.String, "pnl_units": pl.Float64,
                         "graded_at": pl.Datetime("us", "UTC")})
    edges = pl.DataFrame([
        {"game_id": 1, "player_id": p, "prop_type": "assists", "line": 0.5, "side": "over", "book": "DraftKings",
         "price": 132.0, "edge": e, "flagged": f, "p_market_side": 0.42, "stamp": "S"}
        for p, e, f in ((1, 0.06, True), (2, 0.01, False))])
    starts = pl.DataFrame({"game_id": [1], "start_utc": [start]})
    v = {r["bet_id"]: r for r in ledger.live_view(bets, edges, starts, datetime(2026, 10, 8, 23, 45, tzinfo=utc))
         .iter_rows(named=True)}
    assert v["a"]["status"] == "value" and v["a"]["now_price"] == 132.0
    assert v["a"]["clv_now"] == pytest.approx(0.42 * 2.72 - 1)  # +172 taken vs a 42% market now
    assert v["a"]["lead_minutes"] == pytest.approx(600) and v["a"]["lead_bucket"] == "6-12h"
    assert v["b"]["status"] == "faded" and v["b"]["lead_bucket"] == "1-3h"
    assert v["c"]["status"] == "gone" and v["c"]["clv_now"] is None and v["c"]["lead_bucket"] == "<1h"
    assert v["d"]["status"] == "graded" and v["d"]["clv_now"] is None and v["d"]["lead_bucket"] == "12h+"
    after = ledger.live_view(bets, edges, starts, datetime(2026, 10, 9, 1, 0, tzinfo=utc))
    assert set(after.filter(pl.col("bet_id") != "d")["status"]) == {"closed"}
    t = ledger.timing(after)
    assert t.to_dicts() == [{"lead_bucket": "12h+", "bets": 1, "staked": 0.3, "pnl": 0.5, "mean_clv": 0.1,
                             "beat_close": 1.0, "mean_price_clv": None, "roi": pytest.approx(0.5 / 0.3)}]


def test_closing_quotes_drop_props_pulled_before_puck_drop() -> None:
    from datetime import timedelta

    from nhl.odds.props import props_key
    from nhl.props.live import closing_quotes
    from nhl.storage import keys

    utc = timezone.utc
    start = datetime(2026, 10, 10, 23, 0, tzinfo=utc)
    early = start - timedelta(hours=3)
    rows = [
        {"book": b, "game_id": 2026020070, "captured_at": early, "start_time": start, "player_name": n, "player_id": pid,
         "prop_type": "points", "line": 0.5, "side": side, "price": price}
        for b, n, pid in (("FanDuel", "A", 1), ("LowVig", "A", 1), ("DraftKings", "B", 2))
        for side, price in (("over", -120.0), ("under", 100.0))
    ]
    seen = pl.DataFrame({
        "book": ["FanDuel", "LowVig"], "game_id": [2026020070] * 2, "player_name": ["A", "A"], "prop_type": ["points"] * 2,
        "line": [0.5, 0.5], "last_seen": [start - timedelta(minutes=4), early],
    }, schema_overrides={"last_seen": pl.Datetime("us", "UTC")}).join(pl.DataFrame({"side": ["over", "under"]}), how="cross")
    store = FakeStore({props_key(20262027, "x"): pl.DataFrame(rows, schema_overrides={"captured_at": pl.Datetime("us", "UTC"),
                                                                                    "start_time": pl.Datetime("us", "UTC")}),
                       keys.props_seen(20262027, "x"): seen})
    close = closing_quotes(store, 20262027, {2026020070: start})  # type: ignore[arg-type]
    # FanDuel still listed it at the last poll; LowVig pulled it after its early quote; DraftKings
    # has no seen entry for the game (polled before the table existed) and is kept.
    assert sorted(close["book"].unique().to_list()) == ["DraftKings", "FanDuel"]
    assert close.height == 4


def test_current_quotes_leave_out_props_missing_from_the_books_latest_poll() -> None:
    from datetime import timedelta

    from nhl.odds.props import props_key
    from nhl.props.live import current_quotes
    from nhl.storage import keys

    utc = timezone.utc
    start = datetime(2026, 10, 11, 23, 0, tzinfo=utc)
    t1, t2 = start - timedelta(hours=5), start - timedelta(hours=4)  # two LowVig polls, 1 h apart
    rows = [
        {"book": "LowVig", "game_id": 2026020090, "captured_at": t1, "start_time": start, "player_name": n, "player_id": pid,
         "prop_type": "points", "line": 0.5, "side": side, "price": price}
        for n, pid in (("Kept", 1), ("Pulled", 2)) for side, price in (("over", -120.0), ("under", 100.0))
    ] + [{"book": "FanDuel", "game_id": 2026020090, "captured_at": t1, "start_time": start, "player_name": "Old",
          "player_id": 3, "prop_type": "points", "line": 0.5, "side": "over", "price": 150.0}]
    dt = pl.Datetime("us", "UTC")
    seen = pl.DataFrame({  # "Kept" was listed at the 2nd poll, "Pulled" only at the 1st; FanDuel isn't tracked yet
        "book": ["LowVig", "LowVig"], "game_id": [2026020090] * 2, "player_name": ["Kept", "Pulled"],
        "prop_type": ["points"] * 2, "line": [0.5, 0.5], "last_seen": [t2, t1],
    }, schema_overrides={"last_seen": dt}).join(pl.DataFrame({"side": ["over", "under"]}), how="cross")
    store = FakeStore({props_key(20262027, "x"): pl.DataFrame(rows, schema_overrides={"captured_at": dt, "start_time": dt}),
                       keys.props_seen(20262027, "x"): seen})
    q = current_quotes(store, 20262027, [2026020090], t2 + timedelta(minutes=5))  # type: ignore[arg-type]
    assert sorted(q["player_name"].unique().to_list()) == ["Kept", "Old"]
