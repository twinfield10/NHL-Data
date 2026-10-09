"""Market probabilities from simulated final scores.

Totals follow the usual book rule: overtime goals count and a shootout win adds one goal
to the winner (already in :class:`nhl.sim.engine.SimResult`). The puck line is ±1.5 on
that final score.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from nhl.sim.engine import SimResult

TOTAL_LINES = (4.5, 5.5, 6.5, 7.5)


def prices(result: SimResult) -> pl.DataFrame:
    """Per game: P(home win), puck lines, totals, mean goals and how games end."""
    h, a = result.home.astype(np.int32), result.away.astype(np.int32)
    margin, total = h - a, h + a
    cols = {
        "p_home_win": (margin > 0).mean(axis=1),
        "p_home_minus_1_5": (margin >= 2).mean(axis=1),
        "p_away_minus_1_5": (margin <= -2).mean(axis=1),
        "mean_home_goals": h.mean(axis=1),
        "mean_away_goals": a.mean(axis=1),
        "p_overtime": (result.ended >= 1).mean(axis=1),
        "p_shootout": (result.ended == 2).mean(axis=1),
    }
    for line in TOTAL_LINES:
        cols[f"p_over_{line}"] = (total > line).mean(axis=1)
    return pl.DataFrame(cols)


#: Score matrix: goals per team 0..MAX_GOALS (higher folded into the top cell) × how the game
#: ended (0 regulation, 1 overtime, 2 shootout). Scores are book-graded finals (a shootout
#: win adds one goal), so every main-market line can be priced from it.
MAX_GOALS = 12
MATRIX_SIZE = 3 * (MAX_GOALS + 1) ** 2


def score_matrix(result: SimResult) -> np.ndarray:
    """``(G, 507)`` float32: P(home = i, away = j, ended = e), flattened as e·169 + i·13 + j."""
    g, n = result.home.shape
    k = MAX_GOALS + 1
    h = np.minimum(result.home, MAX_GOALS).astype(np.int64)
    a = np.minimum(result.away, MAX_GOALS).astype(np.int64)
    flat = np.arange(g)[:, None] * MATRIX_SIZE + result.ended.astype(np.int64) * k * k + h * k + a
    counts = np.bincount(flat.ravel(), minlength=g * MATRIX_SIZE).reshape(g, MATRIX_SIZE)
    return (counts / n).astype(np.float32)


def _grid(matrix: np.ndarray) -> np.ndarray:
    """``(G, 13, 13)``: P(home = i, away = j), any ending."""
    k = MAX_GOALS + 1
    return np.asarray(matrix, dtype=np.float64).reshape(-1, 3, k, k).sum(axis=1)


def line_probs(matrix: np.ndarray, market: str, line: float | np.ndarray | None = None) -> tuple[np.ndarray, np.ndarray]:
    """``(p_side1_wins, p_push)`` per game from score matrices.

    * ``moneyline``: side 1 = home (incl. OT/SO); never a push.
    * ``puckline``: ``line`` is the home handicap (−1.5 → home must win by 2+).
    * ``total``: side 1 = over ``line``; a whole-number line can push.
    ``line`` may be a scalar or one value per game.
    """
    grid = _grid(matrix)
    k = MAX_GOALS + 1
    i, j = np.meshgrid(np.arange(k), np.arange(k), indexing="ij")
    if market == "moneyline":
        return (grid * (i > j)).sum(axis=(1, 2)), np.zeros(grid.shape[0])
    lines = np.broadcast_to(np.asarray(line, dtype=np.float64), (grid.shape[0],))[:, None, None]
    if market == "puckline":
        value = (i - j)[None] + lines
    elif market == "total":
        value = (i + j)[None] - lines
    else:
        raise ValueError(f"unknown market {market!r}")
    return (grid * (value > 0)).sum(axis=(1, 2)), (grid * (value == 0)).sum(axis=(1, 2))


def three_way(matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """``(p_home_reg, p_draw, p_away_reg)`` per game: the regulation three-way moneyline, where
    a game that reaches overtime (or a shootout) is the draw."""
    k = MAX_GOALS + 1
    reg = np.asarray(matrix, dtype=np.float64).reshape(-1, 3, k, k)[:, 0]
    i, j = np.meshgrid(np.arange(k), np.arange(k), indexing="ij")
    home, away = (reg * (i > j)).sum(axis=(1, 2)), (reg * (i < j)).sum(axis=(1, 2))
    return home, 1.0 - home - away, away


def overtime_split(matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """``(p_overtime, p_home_given_ot)`` per game: how often the game passes regulation and how
    often the home team wins once it does (0.5 where it never does)."""
    k = MAX_GOALS + 1
    ot = np.asarray(matrix, dtype=np.float64).reshape(-1, 3, k, k)[:, 1:].sum(axis=1)
    i, j = np.meshgrid(np.arange(k), np.arange(k), indexing="ij")
    p_ot = ot.sum(axis=(1, 2))
    home = (ot * (i > j)).sum(axis=(1, 2))
    return p_ot, np.where(p_ot > 0, home / np.maximum(p_ot, 1e-12), 0.5)


def split_three_way(p_home_win: np.ndarray, p_ot: np.ndarray, p_home_ot: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """``(p_home_reg, p_draw, p_away_reg)`` from a full-game home win probability, the chance of
    overtime and the home share of overtime wins: each side's regulation win is its full-game
    win less the overtime games it wins. With the simulator's own three inputs this is exactly
    :func:`three_way`; with a blended moneyline or a calibrated overtime rate it keeps the
    three-way consistent with them. A side that would go negative is floored and the rest
    renormalised."""
    p_home_win, p_ot, p_home_ot = (np.asarray(x, dtype=np.float64) for x in (p_home_win, p_ot, p_home_ot))
    home = np.maximum(p_home_win - p_ot * p_home_ot, 1e-4)
    away = np.maximum(1 - p_home_win - p_ot * (1 - p_home_ot), 1e-4)
    total = home + away + p_ot
    return home / total, p_ot / total, away / total
