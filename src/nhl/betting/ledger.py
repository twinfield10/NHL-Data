"""The bet ledger (M6 phase E): paper bets from the model and real bets you place, graded the
same way on closing-line value (CLV) and result.

``bets/ledger.parquet``, one row per bet:

* identity: ``bet_id``, ``kind`` (``paper`` | ``real``), ``placed_at``, ``game_id``,
  ``game_date``, ``market``, ``side`` (1 = home / over, 2 = away / under), ``line``,
  ``book``, ``price`` (American), ``stake_units``, ``tier``;
* the model's view when placed: ``p_model``, ``p_market``, ``p_blend``, ``edge``,
  ``pregame_stamp``;
* grading (filled by :func:`grade` once the game is final): ``close_line`` (the market's
  closing primary line, the lowest-hold line most books agree on, for reference),
  ``close_price`` (the bet's book's last price before puck drop at the bet's line),
  ``p_close`` (devigged closing consensus for the side **at the bet's own line**, over the
  books quoting that line, alternate rungs included), ``clv`` (p_close × decimal − 1; null
  only when no book quoted the bet's line at the close), ``result`` (``win`` | ``loss`` |
  ``push``), ``pnl_units``, ``graded_at``.

Paper bets are **laddered** (:mod:`nhl.betting.edges`): a (game, market, side) position takes
up to three fills, one per tier of time to puck drop. Each fill is a row with ``fill`` (1, 2,
3) and ``ladder`` (``early`` | ``day`` | ``late``), graded on its own at its own price and line.
:func:`positions` rolls fills up into one row per position for the site.
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
    "pregame_stamp": pl.String, "note": pl.String, "fill": pl.Int8, "ladder": pl.String,
    "close_line": pl.Float64, "close_price": pl.Float64, "p_close": pl.Float64, "clv": pl.Float64, "result": pl.String, "pnl_units": pl.Float64,
    "graded_at": pl.Datetime("us", "UTC"),
    **info.SCHEMA,
}


def load(store: Store) -> pl.DataFrame:
    led = store.get_parquet(keys.BETS_LEDGER)
    if led is None:
        return pl.DataFrame(schema=SCHEMA)
    missing = [pl.lit(None, dtype=t).alias(c) for c, t in SCHEMA.items() if c not in led.columns]
    return led.with_columns(missing).with_columns(pl.col("fill").fill_null(1)).cast(SCHEMA).select(list(SCHEMA))


def _bet_id(*parts) -> str:
    return hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:16]


def day_stakes(store: Store, day: date, exclude: pl.DataFrame | None = None) -> float:
    """Units of paper bets already flagged for ``day`` (not counting ``exclude`` keys)."""
    led = load(store).filter((pl.col("kind") == "paper") & (pl.col("game_date") == day))
    if exclude is not None and exclude.height:
        led = led.join(exclude.cast({"side": pl.Int8}), on=["game_id", "market", "side"], how="anti")
    return float(led["stake_units"].sum())


def add_paper(store: Store, edges: pl.DataFrame) -> int:
    """Append a fill for each edge row whose (game, market, side) has no fill in its ``ladder``
    tier yet (rows from :func:`nhl.betting.edges._stakes` with ``fill_new``; the row's stake is
    ``fill_units``). Returns fills added."""
    if edges.is_empty():
        return 0
    key = ["game_id", "market", "side"]
    led = load(store)
    paper = led.filter(pl.col("kind") == "paper")
    new = edges.select(
        "game_id", "game_date", "market", pl.col("side").cast(pl.Int8), "line", "book", "price",
        pl.col("fill_units").alias("stake_units"), "ladder", "tier",
        pl.col("p_model_side").alias("p_model"), pl.col("p_market_side").alias("p_market"), pl.col("p").alias("p_blend"),
        "edge", "pregame_stamp", pl.col("as_of").alias("placed_at"),
    ).join(paper.select(*key, "ladder"), on=[*key, "ladder"], how="anti", nulls_equal=True).unique(key, keep="first")
    if new.is_empty():
        return 0
    prior = paper.group_by(key).agg(pl.col("fill").max().alias("_prior"))
    new = new.join(prior, on=key, how="left").with_columns((pl.col("_prior").fill_null(0) + 1).cast(pl.Int8).alias("fill")).drop("_prior")
    # Fill 1 keeps the id paper bets had before the ladder; later fills add their number.
    ident = pl.when(pl.col("fill") == 1).then(pl.concat_str([pl.lit("paper"), "game_id", "market", "side"], separator="|")).otherwise(
        pl.concat_str([pl.lit("paper"), "game_id", "market", "side", "fill"], separator="|"))
    new = info.attach(store, new, info.game_prefix).with_columns(
        pl.lit("paper").alias("kind"), pl.lit(None, dtype=pl.String).alias("note"),
        ident.map_elements(lambda s: hashlib.sha1(s.encode()).hexdigest()[:16], return_dtype=pl.String).alias("bet_id"),
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


def price_clv(price: pl.Expr, ref: pl.Expr) -> pl.Expr:
    """Price CLV: implied(ref) / implied(price) − 1, the same as decimal(price) / decimal(ref) − 1.

    ``ref`` is the same book's price later (now or at the close) on the same side and line, so
    the book's margin mostly cancels; positive when the price shortened after the bet."""
    return pl.when(price < 0).then(1 + 100 / -price).otherwise(1 + price / 100) / pl.when(ref < 0).then(
        1 + 100 / -ref).otherwise(1 + ref / 100) - 1


def _close_prices(bets: pl.DataFrame, lines: pl.DataFrame) -> pl.Series:
    """Each bet's book's closing price on its side, when that book closed at the bet's line."""
    close = lines.filter((pl.col("point") == "close") & (pl.col("source") == "live")).select(
        "game_id", "market", "book", "line", "price_1", "price_2").unique(["game_id", "market", "book", "line"], keep="last")
    j = bets.select("bet_id", "game_id", "market", "book", "line", "side").join(
        close, on=["game_id", "market", "book", "line"], how="left", nulls_equal=True)
    j = j.with_columns(pl.when(pl.col("side") == 1).then("price_1").otherwise("price_2").alias("close_price"))
    return bets.select("bet_id").join(j.select("bet_id", "close_price"), on="bet_id", how="left")["close_price"]


def fill_close_prices(store: Store) -> int:
    """Fill ``close_price`` on graded bets graded before it was recorded. Returns bets filled."""
    led = load(store)
    todo = led.filter(pl.col("graded_at").is_not_null() & pl.col("close_price").is_null())
    if todo.is_empty():
        return 0
    seasons = sorted({int(g) // 1_000_000 for g in todo["game_id"].to_list()})
    lines = pl.concat([lines_mod.build(store, int(f"{y}{y + 1}"), alternates=True) for y in seasons])
    todo = todo.with_columns(_close_prices(todo, lines).alias("close_price"))
    filled = todo.filter(pl.col("close_price").is_not_null())
    if filled.height:
        out = pl.concat([led.join(filled.select("bet_id"), on="bet_id", how="anti"), filled]).sort("placed_at")
        store.put_parquet(keys.BETS_LEDGER, out.cast(SCHEMA).select(list(SCHEMA)))
    return filled.height


def grade(store: Store, regrade: bool = False) -> int:
    """Grade every ungraded bet whose game is final (every final bet with ``regrade``, e.g.
    after the close definition changes). Returns bets graded."""
    led = load(store)
    todo = led if regrade else led.filter(pl.col("graded_at").is_null())
    if todo.is_empty():
        return 0
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("is_final") & pl.col("game_id").is_in(todo["game_id"].implode()))
    todo = todo.filter(pl.col("game_id").is_in(games["game_id"].implode()))
    if todo.is_empty():
        return 0
    seasons = sorted({int(g) // 1_000_000 for g in todo["game_id"].to_list()})
    lines = pl.concat([lines_mod.build(store, int(f"{y}{y + 1}"), alternates=True) for y in seasons])
    per_market = [(lines.filter(pl.col("market") == m), method) for m, method in evaluate.METHOD.items()]
    primary = pl.concat([devig.consensus(ls, method, ("close",)) for ls, method in per_market]).select(
        "game_id", "market", pl.col("line").alias("close_line"))
    at_line = pl.concat([devig.consensus_by_line(ls, method, ("close",)) for ls, method in per_market]).select(
        "game_id", "market", "line", pl.col("p_fair").alias("close_p1"))
    scores = games.select("game_id", "home_score", "away_score")
    todo = todo.with_columns(_close_prices(todo, lines).alias("close_price"))
    g = lines_mod.grade(todo.drop("close_line", "p_close", "clv", "result", "pnl_units", "graded_at"), scores)
    g = (g.join(primary, on=["game_id", "market"], how="left")
         .join(at_line, on=["game_id", "market", "line"], how="left", nulls_equal=True))
    dec = pl.when(pl.col("price") < 0).then(1 + 100 / -pl.col("price")).otherwise(1 + pl.col("price") / 100)
    won = pl.when(pl.col("side") == 1).then(pl.col("y")).otherwise(1 - pl.col("y"))
    p_close = pl.when(pl.col("side") == 1).then(pl.col("close_p1")).otherwise(1 - pl.col("close_p1"))
    g = g.with_columns(
        p_close.alias("p_close"),
        (p_close * dec - 1).alias("clv"),
        pl.when(pl.col("y").is_null()).then(pl.lit("push")).when(won == 1).then(pl.lit("win")).otherwise(pl.lit("loss")).alias("result"),
        pl.when(pl.col("y").is_null()).then(0.0).when(won == 1).then(pl.col("stake_units") * (dec - 1))
        .otherwise(-pl.col("stake_units")).alias("pnl_units"),
        pl.lit(datetime.now(timezone.utc)).alias("graded_at"),
    ).drop("y", "close_p1").cast(SCHEMA).select(list(SCHEMA))
    out = pl.concat([led.join(g.select("bet_id"), on="bet_id", how="anti"), g]).sort("placed_at")
    store.put_parquet(keys.BETS_LEDGER, out)
    return g.height


def _decimal(price: pl.Expr) -> pl.Expr:
    return pl.when(price < 0).then(1 + 100 / -price).otherwise(1 + price / 100)


def _american(dec: pl.Expr) -> pl.Expr:
    return pl.when(dec >= 2).then((dec - 1) * 100).otherwise(-100 / (dec - 1)).round(0)


def positions(bets: pl.DataFrame) -> pl.DataFrame:
    """One row per position: a paper (game, market, side) with its fills rolled up, and each real
    bet as its own position.

    Sums ``stake_units`` and ``pnl_units``; ``price`` is the unit-weighted average (in decimal
    odds, back to American); ``clv`` / ``p_close`` and any of ``clv_now``, ``price_clv`` are
    unit-weighted means (plain means for a position of 0 u fills). The first fill gives the
    identity, line, book, timing and model view; the latest fill gives the live columns
    (``status``, ``now_price``, ``now_book``, ``book_now``). ``result`` follows the units won
    (win / loss / push). ``n_fills`` and ``fills`` (time, tier, line, book, price, units, CLV,
    units won) describe the fills.
    """
    if bets.is_empty():
        return bets.with_columns(pl.lit(None, dtype=pl.Int64).alias("n_fills"))
    pos = pl.when(pl.col("kind") == "paper").then(
        pl.concat_str([pl.lit("paper"), "game_id", "market", "side"], separator="|")).otherwise(pl.col("bet_id"))
    b = bets.with_columns(pos.alias("_pos"), pl.col("stake_units").fill_null(0.0).alias("_w")).sort("placed_at")
    staked = pl.col("_w").sum()

    def weighted(c: str) -> pl.Expr:
        w = pl.when(pl.col(c).is_not_null()).then(pl.col("_w")).otherwise(0.0)
        return pl.when(w.sum() > 0).then((pl.col(c).fill_null(0.0) * pl.col("_w")).sum() / w.sum()).otherwise(pl.col(c).mean())
    mean_cols = [c for c in ("clv", "p_close", "clv_now", "price_clv") if c in b.columns]
    latest = [c for c in ("status", "now_price", "now_book", "book_now", "now_stamp") if c in b.columns]
    fill_fields = [c for c in ("placed_at", "ladder", "line", "book", "price", "stake_units", "clv", "pnl_units", "result")
                   if c in b.columns]
    firsts = [c for c in b.columns if c not in {"_pos", "_w", "stake_units", "pnl_units", "price", "result", "graded_at",
                                               *mean_cols, *latest}]
    out = b.group_by("_pos", maintain_order=True).agg(
        *[pl.col(c).first() for c in firsts],
        *[pl.col(c).last() for c in latest],
        pl.col("stake_units").sum(),
        pl.when(pl.col("pnl_units").is_null().any()).then(None).otherwise(pl.col("pnl_units").sum()).alias("pnl_units"),
        _american(pl.when(staked > 0).then((_decimal(pl.col("price")) * pl.col("_w")).sum() / staked)
                  .otherwise(_decimal(pl.col("price")).mean())).alias("price"),
        *[weighted(c).alias(c) for c in mean_cols],
        pl.when(pl.col("graded_at").is_null().any()).then(None).otherwise(pl.col("graded_at").max()).alias("graded_at"),
        pl.when(pl.len() == 1).then(pl.col("result").first()).alias("_one_result"),
        pl.len().alias("n_fills"),
        pl.struct(fill_fields).alias("fills"),
    )
    won = pl.col("pnl_units")
    result = (pl.when(pl.col("n_fills") == 1).then(pl.col("_one_result"))
              .when(won.is_null()).then(None).when(won > 0).then(pl.lit("win")).when(won < 0).then(pl.lit("loss"))
              .otherwise(pl.lit("push")))
    return out.with_columns(result.alias("result")).drop("_pos", "_one_result")


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
