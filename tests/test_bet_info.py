"""What the model knew when a paper bet was placed (nhl.betting.info)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl

from nhl.betting import info, ledger
from nhl.props import ledger as props_ledger
from nhl.storage import keys

DAY = date(2026, 10, 9)
START = datetime(2026, 10, 9, 23, 0, tzinfo=timezone.utc)
FIRST = "20261009T120000Z"


class _Mem:
    def __init__(self, data=None):
        self.data = dict(data or {})

    def get_parquet(self, key):
        return self.data.get(key)

    def read_parquet_required(self, key):
        return self.data[key]

    def put_parquet(self, key, df):
        self.data[key] = df

    def list_keys(self, prefix):
        return sorted(k for k in self.data if k.startswith(prefix))


def _slate(**kw) -> pl.DataFrame:
    row = {"game_id": 1, "start_time": START, "home_starter_dfo": "Confirmed", "away_starter_dfo": "Confirmed",
           "home_starter_p": 0.99, "away_starter_p": 0.98, "home_dfo_share": 1.0, "away_dfo_share": 1.0,
           "home_lineup_issues": "", "away_lineup_issues": None, "home_game_time_decisions": 0,
           "away_game_time_decisions": 0, "stale_inputs": None}
    return pl.DataFrame([row | kw], schema_overrides={"stale_inputs": pl.String, "away_lineup_issues": pl.String})


def _store(slates: dict[str, pl.DataFrame]) -> _Mem:
    data = {keys.pregame_slate(DAY, st): s for st, s in slates.items()}
    data[keys.betting_edges(DAY, FIRST)] = pl.DataFrame({"x": [1]})
    data[keys.betting_edges(DAY, "20261009T150000Z")] = pl.DataFrame({"x": [1]})
    return _Mem(data)


def _bets(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame([{"bet_id": str(i), "game_id": 1, "game_date": DAY} | r for i, r in enumerate(rows)])


def test_grades_and_windows() -> None:
    store = _store({
        "A": _slate(),
        "B": _slate(home_starter_dfo="Likely", away_lineup_issues="8479324 out | 8480001 gtd"),
        "C": _slate(away_starter_dfo=None, away_dfo_share=0.2),
        "S": _slate(stale_inputs="odds"),
    })
    out = info.attach(store, _bets(
        {"pregame_stamp": "A", "placed_at": datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)},   # first run -> open
        {"pregame_stamp": "B", "placed_at": datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)},   # later -> pre
        {"pregame_stamp": "C", "placed_at": START - timedelta(minutes=10)},                       # last 30 min -> post
        {"pregame_stamp": "S", "placed_at": datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)},
        {"pregame_stamp": "missing", "placed_at": datetime(2026, 10, 9, 15, 0, tzinfo=timezone.utc)},
    ), info.game_prefix)
    r = {x["pregame_stamp"]: x for x in out.iter_rows(named=True)}
    assert (r["A"]["info_grade"], r["A"]["window"]) == ("A", "open")
    assert (r["B"]["info_grade"], r["B"]["window"], r["B"]["lineup_flags"]) == ("B", "pre", 2)
    assert (r["C"]["info_grade"], r["C"]["window"], r["C"]["away_goalie"]) == ("C", "post", "Model")
    assert r["S"]["info_grade"] == "C"
    assert r["missing"]["info_grade"] is None and r["missing"]["window"] == "pre"


def test_add_paper_records_snapshot_and_old_ledgers_load() -> None:
    store = _store({"A": _slate()})
    e = pl.DataFrame([{"game_id": 1, "game_date": DAY, "market": "moneyline", "side": 1, "line": None, "book": "X",
                       "price": 110.0, "stake_units": 1.0, "tier": "t", "p_model_side": 0.6, "p_market_side": 0.5,
                       "p": 0.55, "edge": 0.05, "pregame_stamp": "A",
                       "as_of": datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)}], schema_overrides={"line": pl.Float64})
    assert ledger.add_paper(store, e) == 1  # type: ignore[arg-type]
    led = ledger.load(store)  # type: ignore[arg-type]
    assert led["info_grade"][0] == "A" and led["window"][0] == "open"

    # A ledger written before the snapshot columns still loads, then backfills.
    store.data[keys.BETS_LEDGER] = led.drop(list(info.SCHEMA))
    assert ledger.load(store)["info_grade"][0] is None  # type: ignore[arg-type]
    assert info.backfill(store) == {"game": 1, "props": 0}  # type: ignore[arg-type]
    assert ledger.load(store)["info_grade"][0] == "A"  # type: ignore[arg-type]


def test_breakdown_and_game_live_view() -> None:
    now = datetime(2026, 10, 9, 16, tzinfo=timezone.utc)
    bets = pl.DataFrame({
        "game_id": [1, 1, 1], "market": ["moneyline", "total", "total"], "side": [1, 1, 2], "line": [None, 6.5, 6.5],
        "price": [110.0, -110.0, -110.0], "stake_units": [1.0, 1.0, 1.0], "placed_at": [now] * 3,
        "graded_at": [now, None, None], "result": ["win", None, None], "pnl_units": [1.1, None, None],
        "clv": [0.02, None, None], "window": ["open", "pre", "pre"], "info_grade": ["A", "C", "C"],
    }, schema_overrides={"line": pl.Float64, "side": pl.Int64, "graded_at": pl.Datetime("us", "UTC"),
                         "result": pl.String, "pnl_units": pl.Float64, "clv": pl.Float64})
    b = info.breakdown(bets)
    assert b.height == 1 and b["window"][0] == "open" and b["roi"][0] == 1.1

    edges = pl.DataFrame({"game_id": [1, 1], "market": ["moneyline", "total"], "side": [1, 1], "line": [None, 6.5],
                          "price": [100.0, -120.0], "book": ["X", "Y"], "edge": [0.04, 0.0], "flagged": [True, False],
                          "p_market_side": [0.5, 0.55], "stamp": ["T", "T"]}, schema_overrides={"line": pl.Float64})
    starts = pl.DataFrame({"game_id": [1], "start_utc": [START]})
    v = props_ledger.live_view(bets, edges, starts, now, key=["game_id", "market", "side", "line"])
    st = dict(zip(zip(v["market"], v["side"]), v["status"]))
    assert st == {("moneyline", 1): "graded", ("total", 1): "faded", ("total", 2): "gone"}
    assert v.filter(pl.col("market") == "total", pl.col("side") == 1)["clv_now"][0] > 0  # -110 vs 55% fair
