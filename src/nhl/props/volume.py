"""Team shot volume, and player shots on goal / blocks and goalie saves (M9 phase C).

The simulator draws goals only, so the shot-based props get their own team layer.

**Team model.** For each team-game, the expected number of the team's shots on goal (and,
separately, its blocked shots) is a Poisson regression on point-in-time team rates:

    log E[team SOG] = log(league SOG per game) + b0 + b1·log(team SF/game ÷ league)
                      + b2·log(opponent SA/game ÷ league) + b3·home + b4·logit P(win)

Blocks use the team's own blocks per game and the opponent's shot attempts per game. The rates
are this season's earlier games plus last season's at :data:`LAST_SEASON_WEIGHT`, shrunk
toward the league with :data:`TEAM_PRIOR_GAMES` games. P(win) is the pregame model's, so game
script enters through it (a heavy favourite shoots less). The count is negative binomial: team
SOG within a team-season varies 1.4x the Poisson variance, blocks 1.6x. Coefficients and
dispersion (:data:`COEF`) are fitted on 2016-17 to 2018-19 with :func:`fit`.

**Players.** Given team SOG *T*, a skater's shots are Binomial(*T*, *s*), where *s* is his share
of the team's shots: per strength bucket, deployment share × his shots-on-goal rate, over the same
sum for his dressed teammates, mixed by the league share of shots by strength. That's the same
structure as goals (:mod:`nhl.props.project`); blocks likewise with blocked-shot rates.

**Goalie saves.** The starter faces the opponent's shots on goal *T* (negative binomial as above);
each is a goal against with q = the opponent's expected goals on him (the simulator's mean,
less empty-net goals, :data:`EN_GOAL_SHARE`) ÷ E[T]. He finishes the game with
:data:`P_FINISH`; a pulled starter plays about half (:data:`PULLED_SHARE`). Saves | T is then
Binomial(T, (1 − q) · exposure).
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import binom, nbinom

from nhl.props.project import DEP_COLS, SHARE_COL
from nhl.props.rates import BUCKET_NAMES, SEASON_WEIGHTS
from nhl.storage import keys
from nhl.storage.s3 import Store

LAST_SEASON_WEIGHT = SEASON_WEIGHTS[1]
TEAM_PRIOR_GAMES = 10.0
LEAGUE_PRIOR_GAMES = 200.0
#: Share of goals scored into an empty net (not on the goalie): 4,418 of 78,918, 2016-2026.
EN_GOAL_SHARE = 0.056
P_FINISH = 0.94
PULLED_SHARE = 0.5
MAX_COUNT = {"sog": 90, "blk": 60}
#: Poisson-regression coefficients (intercept, own rate, opponent rate, home, logit P(win)) and
#: negative-binomial size r, per target; fitted on 2016-17 to 2018-19 (:func:`fit`).
COEF: dict[str, dict[str, float]] = {
    "sog": {"b0": -0.0044, "own": 0.6285, "opp": 0.7441, "home": 0.0158, "win": 0.0642, "r": 92.5},
    "blk": {"b0": 0.0177, "own": 0.8266, "opp": 1.2789, "home": -0.0264, "win": -0.0325, "r": 25.5},
}
#: Exponent on projected deployment shares per stat (see :func:`player_shares`); tuned on
#: 2016-17 to 2018-19. Shots like a little of the goals' sharpening (1.3); blocks none (depth
#: defencemen block as much as top-pair ones).
DEP_POWER: dict[str, float] = {"sog": 1.15, "blk": 1.0}
#: Team-log columns: what each target's own rate and the opponent's rate are built from.
TARGETS = {"sog": ("sf", "sa"), "blk": ("blocks", "cf")}


# ----------------------------------------------------------------------------- team --
def team_logs(store: Store, season: int) -> pl.DataFrame:
    """All-strength team game logs with the game date (``game_id, game_date, team_id,
    opp_team_id, is_home, sf, sa, cf, blocks``)."""
    t = store.get_parquet(keys.team_game_logs(season))
    if t is None:
        return pl.DataFrame()
    games = store.read_parquet_required(keys.GAMES).select("game_id", "game_date")
    return t.filter(pl.col("strength") == "all").join(games, on="game_id").select(
        "game_id", "game_date", pl.col("team_id").cast(pl.Int64), pl.col("opp_team_id").cast(pl.Int64), "is_home",
        *[pl.col(c).cast(pl.Float64) for c in ("sf", "sa", "cf", "blocks")])


def team_rates(cur: pl.DataFrame, last: pl.DataFrame) -> pl.DataFrame:
    """Point-in-time per-game team rates (``sf_pg``, ``sa_pg``, ``cf_pg``, ``blocks_pg``) before
    each game of ``cur``, and the league level (``lg_*``) at the same moment.

    The league level is this season's team-games so far, shrunk toward last season's mean with
    :data:`LEAGUE_PRIOR_GAMES` team-games. Shot totals drift from year to year (30.1 shots on goal
    per team-game in 2023-24, 28.2 in 2024-25), so last season alone would bias every game.
    """
    cols = ["sf", "sa", "cf", "blocks"]
    prior = {c: float(last[c].mean()) if not last.is_empty() else float(cur[c].mean()) for c in cols}
    cur = cur.sort("game_date", "game_id")
    # League level from games on earlier dates only (a day's games are priced together).
    daily = cur.group_by("game_date").agg(pl.len().cast(pl.Float64).alias("_dn"), *[pl.col(c).sum().alias(f"_d_{c}") for c in cols]) \
        .sort("game_date").with_columns(pl.col("_dn").cum_sum().shift(1).fill_null(0.0).alias("_ln"),
                                        *[pl.col(f"_d_{c}").cum_sum().shift(1).fill_null(0.0).alias(f"_l_{c}") for c in cols])
    cur = cur.join(daily.select("game_date", "_ln", *[f"_l_{c}" for c in cols]), on="game_date", how="left").with_columns(
        *[(pl.col(c).cum_sum().over("team_id") - pl.col(c)).alias(f"_s_{c}") for c in cols],
        (pl.int_range(pl.len()).over("team_id")).cast(pl.Float64).alias("_n"),
        *[((pl.col(f"_l_{c}") + LEAGUE_PRIOR_GAMES * prior[c]) / (pl.col("_ln") + LEAGUE_PRIOR_GAMES)).alias(f"lg_{c}") for c in cols])
    if not last.is_empty():
        old = last.group_by("team_id").agg(pl.len().cast(pl.Float64).alias("_on"), *[pl.col(c).sum().alias(f"_o_{c}") for c in cols])
        cur = cur.join(old, on="team_id", how="left")
    else:
        cur = cur.with_columns(pl.lit(None, dtype=pl.Float64).alias("_on"), *[pl.lit(None, dtype=pl.Float64).alias(f"_o_{c}") for c in cols])
    w = LAST_SEASON_WEIGHT
    games = pl.col("_n") + w * pl.col("_on").fill_null(0.0)
    # A team's own history is measured against its own season's level: rescale last season's totals.
    scale = {c: pl.col(f"lg_{c}") / prior[c] for c in cols}
    return cur.with_columns(
        *[((pl.col(f"_s_{c}") + w * pl.col(f"_o_{c}").fill_null(0.0) * scale[c] + TEAM_PRIOR_GAMES * pl.col(f"lg_{c}"))
           / (games + TEAM_PRIOR_GAMES)).alias(f"{c}_pg") for c in cols],
    ).drop([c for c in cur.columns if c.startswith("_")])


def features(rates: pl.DataFrame, history: pl.DataFrame) -> pl.DataFrame:
    """Per team-game regression inputs for every target, plus the outcomes.

    Args:
        rates: :func:`team_rates`.
        history: Per game ``game_id, home_team_id, p_home_win`` (the pregame history or live prices).
    """
    opp = rates.select("game_id", pl.col("team_id").alias("opp_team_id"),
                       *[pl.col(f"{c}_pg").alias(f"opp_{c}_pg") for c in ("sf", "sa", "cf", "blocks")])
    p = history.select("game_id", pl.col("home_team_id").cast(pl.Int64), "p_home_win")
    df = rates.join(opp, on=["game_id", "opp_team_id"], how="inner").join(p, on="game_id", how="inner").with_columns(
        pl.when(pl.col("team_id") == pl.col("home_team_id")).then(pl.col("p_home_win")).otherwise(1 - pl.col("p_home_win"))
        .clip(0.02, 0.98).alias("p_win"))
    out = df.with_columns(
        pl.col("is_home").cast(pl.Float64).alias("home"),
        (pl.col("p_win") / (1 - pl.col("p_win"))).log().alias("z"),
    )
    for t, (own, opp_col) in TARGETS.items():
        out = out.with_columns(
            (pl.col(f"{own}_pg") / pl.col(f"lg_{own}")).log().alias(f"{t}_own"),
            (pl.col(f"opp_{opp_col}_pg") / pl.col(f"lg_{opp_col}")).log().alias(f"{t}_opp"),
            pl.col(f"lg_{own}").alias(f"{t}_lg"),
        )
    return out


def expected(feat: pl.DataFrame, target: str, coef: dict[str, dict[str, float]] | None = None) -> pl.Series:
    """E[team count] for ``target`` (``sog`` | ``blk``) per row of :func:`features`."""
    c = (coef or COEF)[target]
    eta = (c["b0"] + c["own"] * pl.col(f"{target}_own") + c["opp"] * pl.col(f"{target}_opp")
           + c["home"] * pl.col("home") + c["win"] * pl.col("z"))
    return feat.select((pl.col(f"{target}_lg") * eta.exp()).alias("mu"))["mu"]


def fit(feat: pl.DataFrame) -> dict[str, dict[str, float]]:
    """Fit :data:`COEF` (Poisson regression on the rate, weighted by the league level; then the
    negative-binomial size by moments) on outcome rows of :func:`features`."""
    from sklearn.linear_model import PoissonRegressor

    out = {}
    for t, (own, _) in TARGETS.items():
        x = feat.select(f"{t}_own", f"{t}_opp", "home", "z").to_numpy()
        lg = feat[f"{t}_lg"].to_numpy()
        y = feat[own].to_numpy()
        m = PoissonRegressor(alpha=0.0, max_iter=1000).fit(x, y / lg, sample_weight=lg)
        mu = lg * np.exp(m.intercept_ + x @ m.coef_)
        inv_r = max(float(np.mean((y - mu) ** 2 - mu) / np.mean(mu ** 2)), 1e-4)
        out[t] = {"b0": float(m.intercept_), "own": float(m.coef_[0]), "opp": float(m.coef_[1]),
                  "home": float(m.coef_[2]), "win": float(m.coef_[3]), "r": 1 / inv_r}
    return out


# ------------------------------------------------------------------------- players --
def stat_mix(logs: pl.DataFrame, stat: str) -> dict[str, float]:
    """League share of a player stat (``sog`` | ``blk``) by strength bucket, from bucket logs."""
    tot = {b: float(logs[f"{stat}_{b}"].sum()) for b in BUCKET_NAMES}
    s = sum(tot.values()) or 1.0
    return {b: v / s for b, v in tot.items()}


def player_shares(dep: pl.DataFrame, rates: pl.DataFrame, mix: dict[str, float], stat: str,
                  dep_power: float = 1.0) -> pl.DataFrame:
    """Each projected skater's share of his team's ``stat`` (``sog`` | ``blk``): per bucket, ice
    time × his rate over his dressed teammates', mixed by the league share by bucket."""
    rate = {"sog": "sog60", "blk": "blk60"}[stat]
    cols = [f"{rate}_{b}" for b in BUCKET_NAMES] + ["en_pg"]
    df = dep.select("game_id", "team_id", "player_id", "position", *DEP_COLS,
                    *(["p_dressed"] if "p_dressed" in dep.columns else [])).join(
        rates.select("game_id", "player_id", *cols), on=["game_id", "player_id"], how="left")
    grp = pl.when(pl.col("position") == "D").then(pl.lit("D")).otherwise(pl.lit("F"))
    df = df.with_columns(grp.alias("_grp")).with_columns(
        [pl.col(c).fill_null(pl.col(c).mean().over("game_id", "_grp")).fill_null(pl.col(c).mean()) for c in cols])
    dressed = pl.col("p_dressed").fill_null(1.0) if "p_dressed" in df.columns else pl.lit(1.0)
    parts = []
    for b, share in SHARE_COL.items():
        if share in DEP_COLS:
            ice = pl.col(share) ** dep_power
        else:
            ice = pl.when(pl.col("s5") > 0).then(pl.col(share)).otherwise(0.0)
        w = ice * pl.col(f"{rate}_{b}")
        parts.append((mix[b] * w / (w * dressed).sum().over("game_id", "team_id")).fill_nan(0.0))
    return df.with_columns(pl.sum_horizontal(parts).clip(0.0, 1.0).alias("share")).select(
        "game_id", "team_id", "player_id", "position", "share")


def nb_pmf(mu: np.ndarray, r: float, n: int) -> np.ndarray:
    """``(len(mu), n + 1)`` negative-binomial pmf with mean ``mu`` and size ``r`` (tail folded in)."""
    k = np.arange(n + 1)
    p = r / (r + mu[:, None])
    pmf = nbinom.pmf(k[None, :], r, p)
    pmf[:, -1] += 1 - pmf.sum(axis=1)
    return pmf


def thinned_tails(share: np.ndarray, mu: np.ndarray, r: float, n: int, ks: tuple[int, ...]) -> dict[int, np.ndarray]:
    """P(Binomial(T, share) >= k) with T ~ NB(mu, r), per row."""
    pmf = nb_pmf(mu, r, n)
    t = np.arange(n + 1)
    return {k: (binom.sf(k - 1, t[None, :], share[:, None]) * pmf).sum(axis=1) for k in ks}


def player_props(shares: pl.DataFrame, team_mu: pl.DataFrame, stat: str, ks: tuple[int, ...],
                 r: float | None = None) -> pl.DataFrame:
    """P(player ``stat`` >= k) and the expected count per projected skater.

    Args:
        shares: :func:`player_shares`.
        team_mu: ``game_id, team_id, mu`` (expected team count).
        stat: ``sog`` | ``blk``.
        ks: Thresholds.
        r: Negative-binomial size (default :data:`COEF`).
    """
    df = shares.join(team_mu, on=["game_id", "team_id"], how="inner")
    if df.is_empty():
        return df
    share, mu = df["share"].to_numpy(), df["mu"].to_numpy()
    tails = thinned_tails(share, mu, r or COEF[stat]["r"], MAX_COUNT[stat], ks)
    return df.with_columns(pl.Series(f"exp_{stat}", share * mu),
                           *[pl.Series(f"p_{stat}_{k}", v) for k, v in tails.items()])


def saves_props(starters: pl.DataFrame, ks: tuple[int, ...], r: float | None = None) -> pl.DataFrame:
    """P(saves >= k) per starting goalie.

    Args:
        starters: ``mu_against`` (opponent's expected SOG) and ``goals_against`` (opponent's
            expected goals, shootout removed) per row.
        ks: Thresholds.
        r: Negative-binomial size of team SOG (default :data:`COEF`).
    """
    mu = starters["mu_against"].to_numpy()
    q = np.clip(starters["goals_against"].to_numpy() * (1 - EN_GOAL_SHARE) / mu, 0.0, 0.5)
    n = MAX_COUNT["sog"]
    pmf = nb_pmf(mu, r or COEF["sog"]["r"], n)
    t = np.arange(n + 1)
    tails = {}
    for k in ks:
        full = binom.sf(k - 1, t[None, :], (1 - q)[:, None])
        pulled = binom.sf(k - 1, t[None, :], ((1 - q) * PULLED_SHARE)[:, None])
        tails[k] = ((P_FINISH * full + (1 - P_FINISH) * pulled) * pmf).sum(axis=1)
    exp = mu * (1 - q) * (P_FINISH + (1 - P_FINISH) * PULLED_SHARE)
    return starters.with_columns(pl.Series("exp_saves", exp), *[pl.Series(f"p_saves_{k}", v) for k, v in tails.items()])


__all__ = ["COEF", "EN_GOAL_SHARE", "TARGETS", "expected", "features", "fit", "nb_pmf", "player_props", "player_shares",
           "saves_props", "stat_mix", "team_logs", "team_rates", "thinned_tails"]

