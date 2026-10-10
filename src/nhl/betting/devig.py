"""Removing the bookmaker's margin (devig) from two-way prices.

Each method maps the two raw implied probabilities ``q_a, q_b`` (summing to ``1 + margin``)
to fair probabilities summing to 1:

* **multiplicative**: ``p = q / Σq`` (margin spread in proportion to price);
* **additive**: ``p = q − margin / 2`` (margin split equally);
* **power**: ``p = q^k`` with ``k`` such that ``Σ q^k = 1`` (more margin on longshots);
* **shin**: Shin's insider-trading model, ``z`` such that the probabilities sum to 1
  (also loads longshots; the standard choice for favourite-longshot bias).

All functions are vectorised over numpy arrays of American prices.
"""

from __future__ import annotations

import numpy as np
import polars as pl

METHODS = ("multiplicative", "additive", "power", "shin")


def implied(american: np.ndarray) -> np.ndarray:
    """Raw implied probability (vig included) of American prices."""
    a = np.asarray(american, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(a < 0, -a / (-a + 100.0), 100.0 / (a + 100.0))


def decimal(american: np.ndarray) -> np.ndarray:
    """Decimal odds (stake included) of American prices."""
    a = np.asarray(american, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(a < 0, 1.0 + 100.0 / -a, 1.0 + a / 100.0)


def _bisect(f, lo: np.ndarray, hi: np.ndarray, iters: int = 60) -> np.ndarray:
    """Root of the increasing-or-decreasing ``f`` in [lo, hi], elementwise (sign of f(lo) kept)."""
    flo = f(lo)
    for _ in range(iters):
        mid = (lo + hi) / 2
        fm = f(mid)
        same = np.sign(fm) == np.sign(flo)
        lo = np.where(same, mid, lo)
        flo = np.where(same, fm, flo)
        hi = np.where(same, hi, mid)
    return (lo + hi) / 2


def fair(price_a: np.ndarray, price_b: np.ndarray, method: str = "multiplicative") -> np.ndarray:
    """Fair probability of side ``a`` from both sides' American prices.

    Returns NaN where either price is missing.
    """
    qa, qb = implied(price_a), implied(price_b)
    total = qa + qb
    if method == "multiplicative":
        p = qa / total
    elif method == "additive":
        p = qa - (total - 1.0) / 2.0
    elif method == "power":
        # Σ q^k = 1; k > 1 when the book has a margin. q^k is decreasing in k for q < 1.
        k = _bisect(lambda k: qa**k + qb**k - 1.0, np.full_like(qa, 0.5), np.full_like(qa, 5.0))
        p = qa**k
    elif method == "shin":
        def probs(z: np.ndarray, q: np.ndarray) -> np.ndarray:
            return (np.sqrt(z**2 + 4.0 * (1.0 - z) * q**2 / total) - z) / (2.0 * (1.0 - z))

        z = _bisect(lambda z: probs(z, qa) + probs(z, qb) - 1.0, np.zeros_like(qa), np.full_like(qa, 0.5))
        p = probs(z, qa)
    else:
        raise ValueError(f"unknown devig method {method!r}; use one of {METHODS}")
    return np.clip(p, 1e-6, 1 - 1e-6)


#: Books left out of the consensus: the SBR consensus is itself a consensus (used only when
#: no single book is available). The exchanges (4Casters, Novig) are in: their live close
#: stops at puck drop like every book's, and they are priced at a fillable size.
NOT_IN_CONSENSUS = ("SBR consensus",)


def _book_lines(lines: pl.DataFrame, method: str, points: tuple[str, ...]) -> pl.DataFrame:
    """One row per (game, market, book, line) at the first available point in ``points``, with
    ``p`` (side 1 fair) and ``hold`` (overround − 1); the SBR consensus only where no single
    book quotes."""
    rank = {p: i for i, p in enumerate(points)}
    rows = (
        lines.filter(pl.col("point").is_in(points))
        .with_columns(pl.col("point").replace_strict(rank, return_dtype=pl.Int8).alias("_rank"))
        .sort("_rank").unique(subset=["game_id", "market", "book", "line"], keep="first", maintain_order=True)
    )
    a, b = rows["price_1"].to_numpy(), rows["price_2"].to_numpy()
    rows = rows.with_columns(pl.Series("p", fair(a, b, method)), pl.Series("hold", implied(a) + implied(b) - 1))
    single = ~pl.col("book").is_in(NOT_IN_CONSENSUS)
    return rows.filter(single | ~single.any().over("game_id", "market"))


#: Lines within this much hold of a book's lowest count as tied for its primary; the tie goes
#: to the line closest to even. Exchange ladders hold ~0.4-1.5% at every rung, so their raw
#: minimum is noise (a 7.5 at 0.42% beside a 5.5 at 0.51%); a sportsbook's main line usually
#: holds 2+ points less than its alternates, so its primary is unaffected.
HOLD_TOLERANCE = 0.005

CONSENSUS_SCHEMA = {"game_id": pl.Int64, "season": pl.Int32, "market": pl.String, "line": pl.Float64,
                    "p_fair": pl.Float64, "books": pl.UInt32}


def consensus_by_line(lines: pl.DataFrame, method: str = "multiplicative", points: tuple[str, ...] = ("close", "last")) -> pl.DataFrame:
    """Market fair probability of side 1 at **every** line quoted, per (game, market, line).

    Only sides at the same line are devigged together (over 6.5 with under 6.5, home −1.5 with
    away +1.5); ``p_fair`` is the median over the books quoting that line, ``books`` their count.
    A bet at any line is judged against this table at its own line.

    Returns:
        ``game_id, season, market, line, p_fair, books``.
    """
    if lines.is_empty():
        return pl.DataFrame(schema=CONSENSUS_SCHEMA)
    return _book_lines(lines, method, points).group_by("game_id", "season", "market", "line").agg(
        pl.col("p").median().alias("p_fair"), pl.len().alias("books")
    ).cast(CONSENSUS_SCHEMA)


def consensus(lines: pl.DataFrame, method: str = "multiplicative", points: tuple[str, ...] = ("close", "last")) -> pl.DataFrame:
    """Market fair probability of side 1 per (game, market) at the market's **primary** line.

    Each book contributes its first available price point in ``points`` (close, else last) at
    every line it quotes. A book's primary line is the one where it holds least (overround − 1
    of the over/under, or home/away, at that one line): that's the line the book thinks closest
    to even. Lines within :data:`HOLD_TOLERANCE` of the book's lowest hold tie, and the tie goes
    to the fair probability nearest 50%. Hold is compared only within a book, since books'
    margins differ (an exchange's ~1% vs a sportsbook's ~4-8%). The market's primary is the line most books call primary;
    ties go to the line more books quote, then the lower median hold, then the lower line.
    ``p_fair`` is the median over every book quoting the primary line. Single books are
    preferred; the SBR consensus fills in only where no other book quotes.

    Returns:
        ``game_id, season, market, line, p_fair, books``.
    """
    if lines.is_empty():
        return pl.DataFrame(schema=CONSENSUS_SCHEMA)
    rows = _book_lines(lines, method, points)
    book = ["game_id", "market", "book"]
    near_min = pl.col("hold") <= pl.col("hold").min().over(book) + HOLD_TOLERANCE
    votes = (rows.filter(near_min).with_columns((pl.col("p") - 0.5).abs().alias("_off_even"))
             .sort("_off_even", "hold", "line").unique(subset=book, keep="first", maintain_order=True)
             .group_by("game_id", "market", "line").agg(pl.len().alias("_votes")))
    by_line = rows.group_by("game_id", "season", "market", "line").agg(
        pl.col("p").median().alias("p_fair"), pl.len().alias("books"), pl.col("hold").median().alias("_hold"))
    return (by_line.join(votes, on=["game_id", "market", "line"], how="inner", nulls_equal=True)
            .sort(["_votes", "books", "_hold", "line"], descending=[True, True, False, False])
            .unique(subset=["game_id", "market"], keep="first", maintain_order=True)
            .select(list(CONSENSUS_SCHEMA)).cast(CONSENSUS_SCHEMA))