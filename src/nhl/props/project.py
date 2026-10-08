"""Goals, assists and points projections from the simulator's team goals (M9 phase B).

Each team goal is handed to one scorer and up to two assisters. For a player:

* **Goal share** at strength bucket *b*: his projected deployment share there (``s5`` for even
  strength, ``spp``, ``spk``) × his goal rate ``g60_b``, over the same sum for his dressed
  teammates.
* **Assist share**: the same with ``a60_b``, scaled so the team's shares add to the league's
  assists per goal in that bucket.
* A team goal is at strength *b* with the league probability ``f_b``; so per team goal the player
  scores with ``p_g = Σ f_b · goal share_b`` and assists with ``p_a = Σ f_b · assist share_b``
  (exclusive events: nobody assists his own goal), and gets a point with ``p_g + p_a``.

Given the team's goal count *G* the player's count is Binomial(*G*, *p*). *G* comes from the
pregame score matrix (:mod:`nhl.sim.markets`), with the shootout winner's extra goal taken
back off (no player is credited with it). So P(player ≥ k) = Σ_G P(G) · P(Binomial(G, p) ≥ k),
and a team the simulator expects to score more lifts every player's line with it.
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import betabinom, binom

from nhl.props.rates import BUCKET_NAMES
from nhl.sim.markets import MAX_GOALS

#: Deployment share column per bucket.
SHARE_COL = {"ev": "s5", "pp": "spp", "sh": "spk"}
#: A player is never more than this likely to be on a goal as an assister.
MAX_ASSIST_P = 0.9
#: Exponent on player rates in the shares (see :func:`shares`).
RATE_POWER = 1.0
#: Beta-binomial concentration per stat (None = binomial); see :func:`tail_probs`.
CONCENTRATION: dict[str, float | None] = {"goals": None, "ast": None, "points": None}
#: Thresholds scored and stored: P(stat >= k).
THRESHOLDS = {"goals": (1, 2), "ast": (1, 2), "points": (1, 2, 3)}


def team_goal_dists(history: pl.DataFrame) -> pl.DataFrame:
    """Per team-game: P(team scores G goals), G = 0..MAX_GOALS, from the pregame score matrix.

    A shootout win is booked as one extra goal; that goal is removed from the winner.

    Args:
        history: ``predictions/pregame_history/{season}`` (``game_id``, ``home_team_id``,
            ``away_team_id``, ``score_matrix``).

    Returns:
        ``game_id, team_id, dist`` (list of MAX_GOALS + 1 floats) and ``mean_goals``.
    """
    k = MAX_GOALS + 1
    m = np.stack(history["score_matrix"].to_numpy()).astype(np.float64).reshape(-1, 3, k, k)
    m = m / m.sum(axis=(1, 2, 3), keepdims=True)
    home = m[:, :2].sum(axis=1).sum(axis=2)  # regulation + overtime: P(home = i)
    away = m[:, :2].sum(axis=1).sum(axis=1)
    so = m[:, 2]
    i = np.arange(k)
    win_h = np.tril(np.ones((k, k)), -1).astype(bool)  # i > j: home won the shootout
    for g in range(m.shape[0]):
        grid = so[g]
        hw, aw = np.where(win_h, grid, 0.0), np.where(win_h.T, grid, 0.0)
        # Winner's booked score is one more than his goals: shift that mass down a row.
        home[g] += np.roll(hw.sum(axis=1), -1) + aw.sum(axis=1)
        away[g] += np.roll(aw.sum(axis=0), -1) + hw.sum(axis=0)
    out = []
    for side, dist, team in (("home", home, "home_team_id"), ("away", away, "away_team_id")):
        out.append(pl.DataFrame({
            "game_id": history["game_id"], "team_id": history[team].cast(pl.Int64),
            "dist": [list(map(float, d)) for d in dist], "mean_goals": dist @ i,
        }))
    return pl.concat(out)


def shares(dep: pl.DataFrame, rates: pl.DataFrame, mix: dict[str, dict[str, float]],
           power: float = 1.0) -> pl.DataFrame:
    """Per projected skater: ``p_goal`` and ``p_assist`` per team goal.

    Args:
        dep: Projected deployment (``game_id, team_id, player_id, s5, spp, spk``; shares as
            :func:`nhl.pregame.lineups.project` returns them). Placeholder rows (null
            ``player_id``) take league-average rates for their group.
        rates: :func:`nhl.props.rates.season_rates` (point-in-time rates per game).
        mix: League ``f`` (goal share by bucket) and ``apg`` (assists per goal by bucket).
        power: Rates enter the shares as ``rate ** power``; above 1 spreads players apart.
    """
    rate_cols = [f"{k}_{b}" for k in ("g60", "a60") for b in BUCKET_NAMES]
    weight = dep.select("p_dressed").to_series() if "p_dressed" in dep.columns else None
    df = dep.select("game_id", "team_id", "player_id", "position", *SHARE_COL.values(),
                    *(["p_dressed"] if weight is not None else [])).join(
        rates.select("game_id", "player_id", *rate_cols), on=["game_id", "player_id"], how="left")
    # Players with no rate row (no game log yet, placeholders): their group's mean rate that game.
    grp = pl.when(pl.col("position") == "D").then(pl.lit("D")).otherwise(pl.lit("F"))
    df = df.with_columns(grp.alias("_grp")).with_columns(
        [pl.col(c).fill_null(pl.col(c).mean().over("game_id", "_grp")).fill_null(pl.col(c).mean()) for c in rate_cols])
    dressed = pl.col("p_dressed").fill_null(1.0) if weight is not None else pl.lit(1.0)
    exprs_g, exprs_a = [], []
    for b, share in SHARE_COL.items():
        # Teammates count by how likely they dress; the player's own share is conditional on
        # his dressing (a prop on a player who sits is void).
        wg = pl.col(share) * pl.col(f"g60_{b}") ** power
        wa = pl.col(share) * pl.col(f"a60_{b}") ** power
        exprs_g.append(mix["f"][b] * wg / (wg * dressed).sum().over("game_id", "team_id"))
        exprs_a.append(mix["f"][b] * mix["apg"][b] * wa / (wa * dressed).sum().over("game_id", "team_id"))
    out = df.with_columns(
        pl.sum_horizontal([e.fill_nan(0.0) for e in exprs_g]).alias("p_goal"),
        pl.sum_horizontal([e.fill_nan(0.0) for e in exprs_a]).clip(0.0, MAX_ASSIST_P).alias("p_assist"),
    ).with_columns((pl.col("p_goal") + pl.col("p_assist")).clip(0.0, 1.0).alias("p_point"))
    return out.select("game_id", "team_id", "player_id", "position", "p_goal", "p_assist", "p_point",
                      *(["p_dressed"] if weight is not None else []))


def tail_probs(p: np.ndarray, dist: np.ndarray, ks: tuple[int, ...],
               concentration: float | None = None) -> dict[int, np.ndarray]:
    """P(player count >= k) averaged over G ~ ``dist``, per row.

    Given G team goals the count is Binomial(G, p); with ``concentration`` κ the per-goal
    probability itself varies from game to game as Beta(κp, κ(1−p)) (a beta-binomial), for
    nights when the player's line drives the scoring.

    Args:
        p: ``(n,)`` per-goal probability.
        dist: ``(n, MAX_GOALS + 1)`` team goal distribution per row.
        ks: Thresholds.
        concentration: κ; None = plain binomial.
    """
    g = np.arange(dist.shape[1])
    p = np.clip(p, 1e-9, 1 - 1e-9)
    out = {}
    for k in ks:
        if concentration is None:
            tail = binom.sf(k - 1, g[None, :], p[:, None])  # P(X >= k | G)
        else:
            a, b = concentration * p[:, None], concentration * (1 - p[:, None])
            tail = betabinom.sf(k - 1, g[None, :], a, b)
        out[k] = (tail * dist).sum(axis=1)
    return out


def project_players(player_shares: pl.DataFrame, goal_dists: pl.DataFrame,
                    concentration: dict[str, float | None] | None = None,
                    thresholds: dict[str, tuple[int, ...]] | None = None) -> pl.DataFrame:
    """Per player-game: P(goals/assists/points >= k) for :data:`THRESHOLDS`, and expected counts.

    Args:
        player_shares: :func:`shares`.
        goal_dists: :func:`team_goal_dists` (or any frame with ``game_id, team_id, dist, mean_goals``).
        concentration: Beta-binomial κ per stat (default :data:`CONCENTRATION`).
        thresholds: k per stat (default :data:`THRESHOLDS`).
    """
    df = player_shares.join(goal_dists, on=["game_id", "team_id"], how="inner")
    if df.is_empty():
        return df
    dist = np.array(df["dist"].to_list())
    cols = {}
    for stat, pcol in (("goals", "p_goal"), ("ast", "p_assist"), ("points", "p_point")):
        p = df[pcol].to_numpy()
        kappa = (CONCENTRATION if concentration is None else concentration).get(stat)
        for k, v in tail_probs(p, dist, (thresholds or THRESHOLDS)[stat], kappa).items():
            cols[f"p_{stat}_{k}"] = v
        cols[f"exp_{stat}"] = p * df["mean_goals"].to_numpy()
    return df.drop("dist").with_columns(**{k: pl.Series(v) for k, v in cols.items()})


#: Logit recalibration ``logit p' = a + b · logit p`` per (stat, k), fitted on the 2016-2026
#: backtest (444,883 skater-games; out of sample on 2019-26 when fitted on 2016-19 it gained
#: 0.0001-0.0005 log loss). b > 1: the raw projection is slightly too compressed (stars low,
#: depth players high). A k above the fitted ones uses the stat's highest fitted k.
CALIBRATION: dict[tuple[str, int], tuple[float, float]] = {
    ("goals", 1): (0.105, 1.058), ("goals", 2): (0.059, 1.010),
    ("ast", 1): (0.114, 1.095), ("ast", 2): (0.278, 1.074),
    ("points", 1): (0.073, 1.114), ("points", 2): (0.255, 1.095), ("points", 3): (0.244, 1.038),
}


def calibrate(df: pl.DataFrame, thresholds: dict[str, tuple[int, ...]]) -> pl.DataFrame:
    """Apply :data:`CALIBRATION` to every ``p_{stat}_{k}`` column, keeping P(>= k) non-increasing in k."""
    out = df
    for stat, ks in thresholds.items():
        fitted = sorted(k for s, k in CALIBRATION if s == stat)
        prev = None
        for k in ks:
            col = f"p_{stat}_{k}"
            if col not in out.columns or not fitted:
                continue
            a, b = CALIBRATION[(stat, k if k in fitted else fitted[-1])]
            p = pl.col(col).clip(1e-6, 1 - 1e-6)
            new = 1 / (1 + (-(a + b * (p / (1 - p)).log())).exp())
            if prev is not None:
                new = pl.min_horizontal(new, pl.col(prev))
            out = out.with_columns(new.alias(col))
            prev = col
    return out


def poisson_goal_dists(team_games: pl.DataFrame, mean_goals: float) -> pl.DataFrame:
    """Every team-game at one league-average Poisson goal distribution (the no-opponent baseline)."""
    from scipy.stats import poisson

    g = np.arange(MAX_GOALS + 1)
    d = poisson.pmf(g, mean_goals)
    d[-1] += 1 - d.sum()
    return team_games.select("game_id", "team_id").unique().with_columns(
        pl.lit(list(map(float, d))).alias("dist"), pl.lit(mean_goals).alias("mean_goals"))


__all__ = ["CALIBRATION", "MAX_ASSIST_P", "calibrate", "SHARE_COL", "THRESHOLDS", "poisson_goal_dists", "project_players", "shares",
           "tail_probs", "team_goal_dists"]
