"""The lines table (M6 phase A): one row per game, book, main market and price point.

Both sides of a market sit on one row, ready for devig:

    game_id, season, book, market, point, line, price_1, price_2, captured_at, source

* ``market``: ``moneyline`` | ``puckline`` | ``total`` (full game, main line only).
* ``point``: ``open`` | ``close`` | ``last``.
* ``price_1`` / ``price_2``: American prices of **home / away** (moneyline, puck line) or
  **over / under** (totals).
* ``line``: the home handicap for puck lines (−1.5 or +1.5), the total for totals, null for
  moneylines. Sides always pair **at the same line** (over 6.5 with under 6.5, home −1.5 with
  away +1.5).
* ``source``: ``live`` (our polls), ``espn_history``, ``sbr``.

By default each book contributes its main line only. With ``alternates=True`` the live rows
also carry every alternate total rung and the ±1.5 puck-line rungs a book quotes (LowVig's
alternates, the exchanges' ladders), so the closing consensus can find each total's primary
line and price a bet at its own line (:mod:`nhl.betting.devig`).

**Live close** = each book's price at our last poll before the scheduled start: its last
transition before ``start_time`` (nothing after counts, so an exchange still quoting in-play
can't leak in), kept only if the book **still listed the market** within
:data:`CLOSE_MAX_GAP` of the start. Transitions store changes only, so the seen table
(:func:`nhl.odds.store.load_seen`) is what separates a price that held from a market taken
down hours earlier; a pulled market has no close. Games polled before the seen table existed
(no entry for that book and game) keep their last transition unchecked.
``open`` = the first capture; ``last`` = the latest. Historical rows carry their own
``open`` / ``close`` / ``last``.

**Cleaning** (counted and logged, never repaired): totals outside 4-9 and history puck lines
other than ±1.5 (line and price swapped in some ESPN rows), sides with no partner at their
line, and pairs whose implied probabilities sum outside 0.98-1.15.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import polars as pl

from nhl.betting.devig import implied
from nhl.odds.core import MAIN_LINE
from nhl.odds.store import SEEN_KEY, load_seen, still_listed
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
#: A book's live close counts only if it listed the market this close to the start. Every
#: book is polled every 5 minutes in the last 90, so 15 minutes allows two missed polls while
#: still catching markets pulled earlier in the day.
CLOSE_MAX_GAP = timedelta(minutes=15)

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


def _ladder(odds: pl.DataFrame) -> pl.DataFrame:
    """Alternate full-game total rungs and the ±1.5 puck-line rungs."""
    return odds.filter(
        (pl.col("period") == "game") & pl.col("is_alternate").fill_null(False) & pl.col("game_id").is_not_null()
        & ((pl.col("market") == "total") | ((pl.col("market") == "puckline") & (pl.col("line").abs() == 1.5)))
    ).with_columns(canonical_book(pl.col("book")).alias("book"))


def pair_sides(rows: pl.DataFrame, source: str) -> pl.DataFrame:
    """One row per (game, book, market, point, line) from per-side rows with a ``point`` column.

    Sides pair only at the same line (home −1.5 ↔ away +1.5; over 6 ↔ under 6); a side with no
    partner at its line is dropped. Book skins collapsed onto one name keep the first pair, a
    book's main line wins over an alternate rung at the same line, and a book's main lines keep
    one line per point (the first) so main-only readers see one line per book.
    """
    alt = pl.col("is_alternate").fill_null(False) if "is_alternate" in rows.columns else pl.lit(False)
    rows = rows.with_columns(alt.alias("_alt")).sort("_alt", maintain_order=True)
    out = []
    for market, (s1, s2) in SIDES.items():
        m = rows.filter(pl.col("market") == market)
        key = ["game_id", "book", "point", "_pl"]
        partner = (-pl.col("line")) if market == "puckline" else pl.col("line")
        a = m.filter(pl.col("side") == s1).with_columns(pl.col("line").alias("_pl")).unique(
            subset=key, keep="first", maintain_order=True).select(*key, pl.col("price").alias("price_1"), "captured_at", "_alt")
        b = m.filter(pl.col("side") == s2).with_columns(partner.alias("_pl")).unique(
            subset=key, keep="first", maintain_order=True).select(*key, pl.col("price").alias("price_2"))
        j = a.join(b, on=key, how="inner", nulls_equal=True)
        bad = a.join(b.select("game_id", "book", "point").unique(), on=["game_id", "book", "point"], how="semi").join(
            j.select(key), on=key, how="anti", nulls_equal=True).height
        if bad:
            logger.info("%s %s: %d sides with no partner at their line dropped", source, market, bad)
        main = j.filter(~pl.col("_alt")).unique(subset=["game_id", "book", "point"], keep="first", maintain_order=True)
        out.append(pl.concat([main, j.filter(pl.col("_alt"))]).select(
            "game_id", _season(pl.col("game_id")).alias("season"), "book", pl.lit(market).alias("market"), "point",
            (pl.lit(None, dtype=pl.Float64) if market == "moneyline" else pl.col("_pl")).alias("line"),
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


def live_lines(store: Store, season: int, alternates: bool = False) -> pl.DataFrame:
    """Open / close / last from our own polls (transitions) for one season; with
    ``alternates``, the alternate total and ±1.5 puck-line rungs too."""
    files = [k for k in store.list_keys(keys.odds_live_prefix(season)) if k.endswith(".parquet")]
    if not files:
        return pl.DataFrame(schema=LINES_SCHEMA)
    raw = pl.concat([store.read_parquet_required(k) for k in files], how="diagonal_relaxed")
    odds = (pl.concat([_main(raw), _ladder(raw)], how="diagonal_relaxed") if alternates else _main(raw)).sort("captured_at")
    # A main market keeps one market_uid as its line moves; each alternate rung has its own.
    sides = ["book", "game_id", "market", "market_uid", "side"]
    before = odds.filter(pl.col("captured_at") < pl.col("start_time"))
    rung = (~pl.col("market_uid").str.ends_with(f"|{MAIN_LINE}")).alias("_rung")
    seen = load_seen(store, keys.odds_seen_prefix(season))
    if seen is not None:  # Caesars' state skins are one book here too
        seen = seen.with_columns(canonical_book(pl.col("book"))).group_by(SEEN_KEY).agg(pl.col("last_seen").max()).with_columns(rung)
    # Rungs were first tracked after main lines, so each is checked only once its kind is tracked.
    close = still_listed(before.group_by(sides).last().with_columns(rung), seen, SEEN_KEY, CLOSE_MAX_GAP,
                         cover_by=("book", "game_id", "_rung")).drop("_rung")
    points = pl.concat([
        before.group_by(sides).first().with_columns(pl.lit("open").alias("point")),
        close.with_columns(pl.lit("close").alias("point")),
        odds.group_by(sides).last().with_columns(pl.lit("last").alias("point")),
    ], how="diagonal_relaxed")
    return pair_sides(points, "live")


def build(store: Store, season: int, alternates: bool = False) -> pl.DataFrame:
    """The season's cleaned lines table from every source (``alternates``: see :func:`live_lines`).

    A game we polled uses our live captures only: backfilled history (ESPN's published
    open/close) fills in just the games we never polled, so one book's close never enters a
    consensus twice from two sources. :func:`close_check` compares the two where both exist.
    """
    live = live_lines(store, season, alternates)
    history = history_lines(store, season).join(live.select("game_id").unique(), on="game_id", how="anti")
    lines = pl.concat([history, live])
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


def close_check(store: Store, season: int) -> pl.DataFrame:
    """Our captured closes against the history source's published close for the same book.

    Per (book, market), over games with both: ``games``, ``same_line`` (share at the same
    line), ``identical`` (share with both prices equal) and ``mean_abs_dp`` / ``max_abs_dp``
    (gap in devigged side-1 probability at the same line). ESPN publishes one close per book
    (DraftKings this season), so this says whether our polls catch that book's real close.
    """
    from nhl.betting.devig import fair

    hist = history_lines(store, season).filter(pl.col("point") == "close").select(
        "game_id", "book", "market", pl.col("line").alias("h_line"), pl.col("price_1").alias("h1"), pl.col("price_2").alias("h2"))
    live = clean(live_lines(store, season)).filter(pl.col("point") == "close")
    j = live.join(hist, on=["game_id", "book", "market"], how="inner")
    if j.is_empty():
        return pl.DataFrame(schema={"book": pl.String, "market": pl.String, "games": pl.UInt32, "same_line": pl.Float64,
                                    "identical": pl.Float64, "mean_abs_dp": pl.Float64, "max_abs_dp": pl.Float64})
    gap = pl.Series(fair(j["price_1"].to_numpy(), j["price_2"].to_numpy()) - fair(j["h1"].to_numpy(), j["h2"].to_numpy())).abs()
    same = (pl.col("line") == pl.col("h_line")) | (pl.col("line").is_null() & pl.col("h_line").is_null())
    return j.with_columns(gap.alias("_gap"), same.alias("_same")).group_by("book", "market").agg(
        pl.len().alias("games"), pl.col("_same").mean().alias("same_line"),
        ((pl.col("price_1") == pl.col("h1")) & (pl.col("price_2") == pl.col("h2"))).mean().alias("identical"),
        pl.col("_gap").filter(pl.col("_same")).mean().alias("mean_abs_dp"),
        pl.col("_gap").filter(pl.col("_same")).max().alias("max_abs_dp"),
    ).sort("book", "market")
