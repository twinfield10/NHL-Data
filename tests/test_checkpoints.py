from datetime import date, datetime, timedelta, timezone

import polars as pl

from nhl.betting import checkpoints as C

START = datetime(2026, 10, 10, 23, 0, tzinfo=timezone.utc)


def snap(hours_before: float, side: int, edge: float, flagged: bool, kelly: float = 0.01, market: str = "moneyline",
         game_id: int = 1, book: str = "LowVig") -> dict:
    as_of = START - timedelta(hours=hours_before)
    return {"game_id": game_id, "game_date": date(2026, 10, 10), "market": market, "side": side, "line": None, "book": book,
            "price": -110.0, "edge": edge, "kelly": kelly, "flagged": flagged, "tier": "unvalidated", "p_model_side": 0.55,
            "p_market_side": 0.5, "p": 0.53, "pregame_stamp": f"P{hours_before}", "start_utc": START, "as_of": as_of,
            "stamp": as_of.strftime("%Y%m%dT%H%M%SZ")}


def test_checkpoints_take_the_latest_snapshot_at_or_before_each_moment() -> None:
    rows = [snap(30, 1, 0.01, False), snap(30, 2, -0.02, False),       # open (more than 24 h out)
            snap(20, 1, 0.03, True), snap(20, 2, -0.04, False),         # first flag; the T-24h snapshot is the 30 h one
            snap(5, 1, 0.025, True), snap(5, 2, -0.03, False),          # T-6h and T-3h; goalies confirmed here
            snap(0.2, 1, 0.005, False), snap(0.2, 2, -0.01, False),     # T-60m and close
            snap(20, 1, 0.01, False, book="Novig")]                     # a worse book at 20 h: not the side's best
    best = C.best_quotes(pl.DataFrame(rows), C.GAME)
    confirmed = pl.DataFrame({"game_id": [1], "stamp": [best.filter(pl.col("as_of") == START - timedelta(hours=5))["stamp"][0]]})
    got = C.pick(best, C.GAME, confirmed)
    at = {(r["checkpoint"], r["side"]): (START - r["as_of"]) / timedelta(hours=1) for r in got.iter_rows(named=True)}
    assert at[("open", 1)] == 30 and at[("T-24h", 1)] == 30 and at[("T-6h", 1)] == 20 and at[("T-3h", 1)] == 5
    assert at[("goalies_confirmed", 2)] == 5 and at[("T-60m", 1)] == 5 and at[("close", 1)] == 0.2
    assert at[("first_flag", 1)] == 20 and ("first_flag", 2) not in at
    assert got.filter((pl.col("checkpoint") == "T-6h") & (pl.col("side") == 1))["book"].to_list() == ["LowVig"]


def test_all_in_stakes_cap_bets_and_games_and_pick_the_bigger_side() -> None:
    rows = pl.DataFrame([snap(5, 1, 0.05, True, kelly=0.05), snap(5, 2, 0.03, True, kelly=0.05),          # both sides flag
                         snap(5, 1, 0.04, True, kelly=0.03, market="total"), snap(5, 1, 0.04, True, kelly=0.03, market="puckline"),
                         snap(5, 2, 0.01, False, kelly=0.0, market="total")]).with_columns(pl.lit("T-6h").alias("checkpoint"))
    st = C.stake_game(rows)
    got = {(r["market"], r["side"]): r["stake_units"] for r in st.iter_rows(named=True)}
    assert got[("moneyline", 2)] == 0.0 and got[("total", 2)] == 0.0
    # 2 + 2 + 2 = 6 u flagged on the game, scaled to its 3 u.
    assert got[("moneyline", 1)] == got[("total", 1)] == got[("puckline", 1)] == 1.0
