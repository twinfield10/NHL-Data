"""The lines table (M6 phase A): one row per game, book, main market and price point.

Both sides of a market sit on one row, ready for devig:

    game_id, season, book, market, point, line, price_1, price_2, captured_at, source

* ``market``: ``moneyline`` | ``puckline`` | ``total`` (full game, main line only).
* ``point``: ``open`` | ``close`` | ``last``.
* ``price_1`` / ``price_2``: American prices of **home / away** (moneyline, puck line) or
  **over / under** (totals).
* ``line``: the home handicap for puck lines (−1.5 or +1.5), the total for totals, null for
  moneylines.
* ``source``: ``live`` (our polls), ``espn_history``, ``sbr``.

**Live close** = the last transition before puck drop (4Casters keeps quoting in-play, so
nothing after ``start_time`` counts); ``open`` = the first capture; ``last`` = the latest.
Historical rows carry their own ``open`` / ``close`` / ``last``.

**Cleaning** (counted and logged, never repaired): totals outside 4-9 and history puck lines
other than ±1.5 (line and price swapped in some ESPN rows), pairs whose two sides disagree
on the line, and pairs whose implied probabilities sum outside 0.98-1.15.
"""

from __future__ import annotations

import logging

import polars as pl

from nhl.betting.devig import implied
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

MAIN_MARKETS = ("moneyline", "puckline", "total")
SIDES = {"moneyline": ("home", "away"), "puckline": ("home", "away"), "total": ("over", "under")}
OVERROUND = (0.98, 1.15)
TOTAL_RANGE = (4.0, 9.0)

#: Canonical book names. Caesars' state skins are one book.
BOOK_NAMES = {
    "MGM": "BetMGM",
    "BetfairSportsbook": "Betfair",
    "Caesars Sportsbook": "Caesars",
}
#: Books excluded from the closing consensus (4Casters quotes in-play after puck drop;
#: SBR consensus is already a consensus, used only when no single book exists).
NOT_IN_CONSENSUS = ("4Casters", "SBR consensus")

LINES_SCHEMA = {
    "game_id": pl.Int64, "season": pl.Int32, "book": pl.String, "market": pl.String, "point": pl.String,
    "line": pl.Float64, "price_1": pl.Float64, "price_2": pl.Float64,
    "captured_at": pl.Datetime("us", "UTC"), "source": pl.String,
}


def canonical_book(book: pl.Expr) -> pl.Expr:
    """Map book names to one name per book (``Caesars Sportsbook (Colorado)`` → ``Caesars``)."""
    base = book.str.replace(r"\s*\(.*\)$", "")
    return base.replace(BOOK_NAMES)


def _season(game_id: pl.Expr) -> pl.Expr:
    y = (game_id // 1_000_000).cast(pl.Int32)
    return y * 10_000 + y + 1


def _main(odds: pl.DataFrame) -> pl.DataFrame:
    return odds.filter(
        (pl.col("period") == "game") & pl.col("market").is_in(MAIN_MARKETS) & ~pl.col("is_alternate").fill_null(False)
        & pl.col("game_id").is_not_null()
    ).with_columns(canonical_book(pl.col("book")).alias("book"))


def pair_sides(rows: pl.DataFrame, source: str) -> pl.DataFrame:
    """One row per (game, book, market, point) from per-side rows with a ``point`` column.

    Puck-line and total sides must agree on the line (home −1.5 ↔ away +1.5; over 6 ↔
    under 6); disagreeing pairs are dropped. Book skins collapsed onto one name keep the
    first pair.
    """
    out = []
    for market, (s1, s2) in SIDES.items():
        m = rows.filter(pl.col("market") == market)
        key = ["game_id", "book", "point"]
        a = m.filter(pl.col("side") == s1).unique(subset=key, keep="first").select(*key, pl.col("line").alias("l1"), pl.col("price").alias("price_1"), "captured_at")
        b = m.filter(pl.col("side") == s2).unique(subset=key, keep="first").select(*key, pl.col("line").alias("l2"), pl.col("price").alias("price_2"))
        j = a.join(b, on=key, how="inner")
        agree = (
            pl.lit(True) if market == "moneyline"
            else (pl.col("l1") == -pl.col("l2")) if market == "puckline"
            else (pl.col("l1") == pl.col("l2"))
        )
        bad = j.filter(~agree).height
        if bad:
            logger.info("%s %s: %d pairs with mismatched lines dropped", source, market, bad)
        out.append(j.filter(agree).select(
            "game_id", _season(pl.col("game_id")).alias("season"), "book", pl.lit(market).alias("market"), "point",
            (pl.lit(None, dtype=pl.Float64) if market == "moneyline" else pl.col("l1")).alias("line"),
            "price_1", "price_2", "captured_at", pl.lit(source).alias("source"),
        ))
    return pl.concat(out).cast(LINES_SCHEMA)


def clean(lines: pl.DataFrame) -> pl.DataFrame:
    """Drop impossible rows (see the module docstring), logging counts by reason."""
    over = pl.Series(implied(lines["price_1"].to_numpy()) + implied(lines["price_2"].to_numpy()))
    lines = lines.with_columns(over.alias("_over"))
    reasons = {
        "total out of range": (pl.col("market") == "total") & ~pl.col("line").is_between(*TOTAL_RANGE),
        "puck line not ±1.5": (pl.col("market") == "puckline") & (pl.col("line").abs() != 1.5),
        "overround out of range": ~pl.col("_over").is_between(*OVERROUND),
    }
    for reason, expr in reasons.items():
        n = lines.filter(expr).height
        if n:
            logger.info("lines: %d rows dropped (%s)", n, reason)
        lines = lines.filter(~expr)
    return lines.drop("_over")


def history_lines(store: Store, season: int) -> pl.DataFrame:
    """Open / close / last from the SBR archive and ESPN history for one season."""
    frames = []
    for key, source in ((keys.odds_history_sbr(season), "sbr"), (keys.odds_history(season), "espn_history")):
        odds = store.get_parquet(key)
        if odds is None or odds.is_empty():
            continue
        rows = _main(odds).filter(pl.col("price_point").is_in(["open", "close", "last"])).rename({"price_point": "point"})
        frames.append(pair_sides(rows, source))
    return pl.concat(frames) if frames else pl.DataFrame(schema=LINES_SCHEMA)


def live_lines(store: Store, season: int) -> pl.DataFrame:
    """Open / close / last from our own polls (transitions) for one season."""
    files = [k for k in store.list_keys(keys.odds_live_prefix(season)) if k.endswith(".parquet")]
    if not files:
        return pl.DataFrame(schema=LINES_SCHEMA)
    odds = _main(pl.concat([store.read_parquet_required(k) for k in files], how="diagonal_relaxed")).sort("captured_at")
    sides = ["book", "game_id", "market", "side"]
    before = odds.filter(pl.col("captured_at") < pl.col("start_time"))
    points = pl.concat([
        before.group_by(sides).first().with_columns(pl.lit("open").alias("point")),
        before.group_by(sides).last().with_columns(pl.lit("close").alias("point")),
        odds.group_by(sides).last().with_columns(pl.lit("last").alias("point")),
    ], how="diagonal_relaxed")
    return pair_sides(points, "live")


def build(store: Store, season: int) -> pl.DataFrame:
    """The season's cleaned lines table from every source."""
    lines = pl.concat([history_lines(store, season), live_lines(store, season)])
    return clean(lines).sort("game_id", "market", "book", "point")


def outcomes(store: Store, season: int) -> pl.DataFrame:
    """``game_id, home_score, away_score`` for final games (scores include a shootout goal,
    as books grade puck lines and totals)."""
    return store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final")).select(
        "game_id", "home_score", "away_score"
    )


def grade(lines: pl.DataFrame, scores: pl.DataFrame) -> pl.DataFrame:
    """Add ``y``: 1 if side 1 (home / over) won, 0 if it lost, null for a push."""
    j = lines.join(scores, on="game_id", how="inner")
    margin = pl.col("home_score") - pl.col("away_score")
    total = pl.col("home_score") + pl.col("away_score")
    y = (
        pl.when(pl.col("market") == "moneyline").then((margin > 0).cast(pl.Float64))
        .when(pl.col("market") == "puckline").then(
            pl.when(margin + pl.col("line") > 0).then(1.0).when(margin + pl.col("line") < 0).then(0.0)
        )
        .when(pl.col("market") == "total").then(
            pl.when(total > pl.col("line")).then(1.0).when(total < pl.col("line")).then(0.0)
        )
    )
    return j.with_columns(y.alias("y")).drop("home_score", "away_score")


def coverage(lines: pl.DataFrame) -> pl.DataFrame:
    """Games with a closing line per season and market, and the books behind them."""
    close = lines.filter(pl.col("point") == "close")
    return close.group_by("season", "market").agg(
        pl.col("game_id").n_unique().alias("games"), pl.col("book").n_unique().alias("books"),
        pl.col("source").unique().sort().str.join(",").alias("sources"),
    ).sort("season", "market")
