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


#: Books left out of the consensus: 4Casters quotes in-play after puck drop, and the SBR
#: consensus is itself a consensus (used only when no single book is available).
NOT_IN_CONSENSUS = ("4Casters", "SBR consensus")


def consensus(lines: pl.DataFrame, method: str = "multiplicative", points: tuple[str, ...] = ("close", "last")) -> pl.DataFrame:
    """Market fair probability of side 1 per (game, market) from the lines table.

    Each book contributes its first available price point in ``points`` (close, else last).
    Puck lines and totals use the line most books hang, and the median fair probability of
    the books at that line. Single books are preferred; the SBR consensus or 4Casters fill
    in only where no other book quotes.

    Returns:
        ``game_id, season, market, line, p_fair, books``.
    """
    rank = {p: i for i, p in enumerate(points)}
    rows = (
        lines.filter(pl.col("point").is_in(points))
        .with_columns(pl.col("point").replace_strict(rank, return_dtype=pl.Int8).alias("_rank"))
        .sort("_rank").unique(subset=["game_id", "market", "book"], keep="first")
    )
    rows = rows.with_columns(pl.Series("p", fair(rows["price_1"].to_numpy(), rows["price_2"].to_numpy(), method)))
    single = ~pl.col("book").is_in(NOT_IN_CONSENSUS)
    rows = rows.filter(single | ~single.any().over("game_id", "market"))
    mode_line = (
        rows.group_by("game_id", "market", "line").agg(pl.len().alias("n"))
        .sort("n", "line", descending=[True, False]).unique(subset=["game_id", "market"], keep="first")
        .select("game_id", "market", "line")
    )
    return rows.join(mode_line, on=["game_id", "market", "line"], nulls_equal=True).group_by("game_id", "season", "market", "line").agg(
        pl.col("p").median().alias("p_fair"), pl.len().alias("books")
    )
