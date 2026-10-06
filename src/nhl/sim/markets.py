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
