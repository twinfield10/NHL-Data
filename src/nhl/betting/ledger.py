"""The bet ledger (M6 phase E): paper bets from the model and real bets you place, graded the
same way on closing-line value (CLV) and result.

``bets/ledger.parquet``, one row per bet:

* identity: ``bet_id``, ``kind`` (``paper`` | ``real``), ``placed_at``, ``game_id``,
  ``game_date``, ``market``, ``side`` (1 = home / over, 2 = away / under), ``line``,
  ``book``, ``price`` (American), ``stake_units``, ``tier``;
* the model's view when placed: ``p_model``, ``p_market``, ``p_blend``, ``edge``,
  ``pregame_stamp``;
* grading (filled by :func:`grade` once the game is final): ``close_line``, ``p_close``
  (devigged closing consensus for the side), ``clv`` (p_close × decimal − 1, only when the
  close is at the bet's line), ``result`` (``win`` | ``loss`` | ``push``), ``pnl_units``,
  ``graded_at``.

A paper bet is recorded the first time a (game, market, side) is flagged and never re-added,
so the ledger reflects the price available when the model first liked it.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timezone

import polars as pl

from nhl.betting import devig, evaluate, info
from nhl.betting import lines as lines_mod
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

SCHEMA = {
    "bet_id": pl.String, "kind": pl.String, "placed_at": pl.Datetime("us", "UTC"), "game_id": pl.Int64, "game_date": pl.Date,
    "market": pl.String, "side": pl.Int8, "line": pl.Float64, "book": pl.String, "price": pl.Float64, "stake_units": pl.Float64,
    "tier": pl.String, "p_model": pl.Float64, "p_market": pl.Float64, "p_blend": pl.Float64, "edge": pl.Float64,
    "pregame_stamp": pl.String, "note": pl.String,
    "close_line": pl.Float64, "p_close": pl.Float64, "clv": pl.Float64, "result": pl.String, "pnl_units": pl.Float64,
    "graded_at": pl.Datetime("us", "UTC"),
    **info.SCHEMA,
}


def load(store: Store) -> pl.DataFrame:
    led = store.get_parquet(keys.BETS_LEDGER)
    if led is None:
        return pl.DataFrame(schema=SCHEMA)
    missing = [pl.lit(None, dtype=t).alias(c) for c, t in SCHEMA.items() if c not in led.columns]
    return led.with_columns(missing).cast(SCHEMA).select(list(SCHEMA))


def _bet_id(*parts) -> str:
    return hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:16]


def day_stakes(store: Store, day: date, exclude: pl.DataFrame | None = None) -> float:
    """Units of paper bets already flagged for ``day`` (not counting ``exclude`` keys)."""
    led = load(store).filter((pl.col("kind") == "paper") & (pl.col("game_date") == day))
    if exclude is not None and exclude.height:
        led = led.join(exclude.cast({"side": pl.Int8}), on=["game_id", "market", "side"], how="anti")
    return float(led["stake_units"].sum())


def add_paper(store: Store, edges: pl.DataFrame) -> int:
    """Append paper bets for edges whose (game, market, side) isn't in the ledger yet."""
    if edges.is_empty():
        return 0
    led = load(store)
    new = edges.select(
        "game_id", "game_date", "market", pl.col("side").cast(pl.Int8), "line", "book", "price", "stake_units", "tier",
        pl.col("p_model_side").alias("p_model"), pl.col("p_market_side").alias("p_market"), pl.col("p").alias("p_blend"),
        "edge", "pregame_stamp", pl.col("as_of").alias("placed_at"),
    ).join(led.filter(pl.col("kind") == "paper").select("game_id", "market", "side"), on=["game_id", "market", "side"], how="anti")
    if new.is_empty():
        return 0
    new = info.attach(store, new, info.game_prefix).with_columns(
        pl.lit("paper").alias("kind"), pl.lit(None, dtype=pl.String).alias("note"),
        pl.concat_str([pl.lit("paper"), "game_id", "market", "side"], separator="|").map_elements(
            lambda s: hashlib.sha1(s.encode()).hexdigest()[:16], return_dtype=pl.String).alias("bet_id"),
    )
    store.put_parquet(keys.BETS_LEDGER, pl.concat([led, new], how="diagonal_relaxed").cast(SCHEMA).select(list(SCHEMA)))
    return new.height


def record_real(store: Store, game_id: int, market: str, side: int, price: float, stake_units: float, book: str,
                line: float | None = None, note: str | None = None, placed_at: datetime | None = None) -> str:
    """Record a bet you actually placed. Model numbers come from the latest edges snapshot when
    the same (game, market, side, line) is in it. Returns the bet id."""
    placed_at = placed_at or datetime.now(timezone.utc)
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("game_id") == game_id)
    if games.is_empty():
        raise ValueError(f"unknown game {game_id}")
    game_date = games["game_date"][0]
    view = _latest_view(store, game_date, game_id, market, side, line)
    row = {
        "bet_id": _bet_id("real", game_id, market, side, line, price, placed_at.isoformat()), "kind": "real",
        "placed_at": placed_at, "game_id": game_id, "game_date": game_date, "market": market, "side": side, "line": line,
        "book": book, "price": price, "stake_units": stake_units, "tier": view.get("tier"), "p_model": view.get("p_model_side"),
        "p_market": view.get("p_market_side"), "p_blend": view.get("p"), "edge": view.get("edge"),
        "pregame_stamp": view.get("pregame_stamp"), "note": note,
    }
    led = load(store)
    store.put_parquet(keys.BETS_LEDGER, pl.concat([led, pl.DataFrame([row])], how="diagonal_relaxed").cast(SCHEMA).select(list(SCHEMA)))
    return row["bet_id"]


def _latest_view(store: Store, day: date, game_id: int, market: str, side: int, line: float | None) -> dict:
    keys_ = sorted(store.list_keys(f"pregame/edges/{day.isoformat()}/"))
    if not keys_:
        return {}
    e = store.read_parquet_required(keys_[-1]).filter(
        (pl.col("game_id") == game_id) & (pl.col("market") == market) & (pl.col("side") == side)
    )
    if line is not None:
        e = e.filter(pl.col("line") == line)
    return e.row(0, named=True) if e.height else {}


def grade(store: Store) -> int:
    """Grade every ungraded bet whose game is final. Returns bets graded."""
    led = load(store)
    todo = led.filter(pl.col("graded_at").is_null())
    if todo.is_empty():
        return 0
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("is_final") & pl.col("game_id").is_in(todo["game_id"].implode()))
    todo = todo.filter(pl.col("game_id").is_in(games["game_id"].implode()))
    if todo.is_empty():
        return 0
    seasons = sorted({int(g) // 1_000_000 for g in todo["game_id"].to_list()})
    lines = pl.concat([lines_mod.build(store, int(f"{y}{y + 1}")) for y in seasons])
    close = pl.concat([
        devig.consensus(lines.filter(pl.col("market") == m), method, ("close",)) for m, method in evaluate.METHOD.items()
    ]).select("game_id", "market", pl.col("line").alias("close_line"), pl.col("p_fair").alias("close_p1"))
    scores = games.select("game_id", "home_score", "away_score")
    g = lines_mod.grade(todo.drop("close_line", "p_close", "clv", "result", "pnl_units", "graded_at"), scores)
    g = g.join(close, on=["game_id", "market"], how="left")
    dec = pl.when(pl.col("price") < 0).then(1 + 100 / -pl.col("price")).otherwise(1 + pl.col("price") / 100)
    won = pl.when(pl.col("side") == 1).then(pl.col("y")).otherwise(1 - pl.col("y"))
    same = (pl.col("close_line") == pl.col("line")) | (pl.col("close_line").is_null() & pl.col("line").is_null())
    p_close = pl.when(pl.col("side") == 1).then(pl.col("close_p1")).otherwise(1 - pl.col("close_p1"))
    g = g.with_columns(
        pl.when(same).then(p_close).alias("p_close"),
        pl.when(same).then(p_close * dec - 1).alias("clv"),
        pl.when(pl.col("y").is_null()).then(pl.lit("push")).when(won == 1).then(pl.lit("win")).otherwise(pl.lit("loss")).alias("result"),
        pl.when(pl.col("y").is_null()).then(0.0).when(won == 1).then(pl.col("stake_units") * (dec - 1))
        .otherwise(-pl.col("stake_units")).alias("pnl_units"),
        pl.lit(datetime.now(timezone.utc)).alias("graded_at"),
    ).drop("y", "close_p1").cast(SCHEMA).select(list(SCHEMA))
    out = pl.concat([led.join(g.select("bet_id"), on="bet_id", how="anti"), g]).sort("placed_at")
    store.put_parquet(keys.BETS_LEDGER, out)
    return g.height


def summary(store: Store) -> pl.DataFrame:
    """Graded bets by kind, tier and market: count, mean CLV, share beating the close, units won, ROI."""
    led = load(store).filter(pl.col("graded_at").is_not_null())
    if led.is_empty():
        return led
    return led.group_by("kind", "tier", "market").agg(
        pl.len().alias("bets"), pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
        pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        (pl.col("pnl_units").sum() / pl.col("stake_units").sum()).alias("roi"),
    ).sort("kind", "tier", "market")
