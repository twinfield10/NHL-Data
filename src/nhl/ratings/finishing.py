"""Finishing and goaltending: shooter and goalie terms on top of neutral xG (M3 phase B).

One logistic GLM over unblocked, non-empty-net shots (Magnus 8's shooter/goalie terms,
placed on our talent-neutral xG instead of inside it):

    logit P(goal) = logit(xG) + c + d · is_defenseman + shooter_s + goalie_g

``c`` is an unpenalized season intercept that absorbs any xG calibration drift, so the
talent terms stay centred. ``d`` is an unpenalized position effect: xG is talent-neutral,
and defensemen convert about 7% fewer of their xG than forwards (2010-2026), so without it
every defenseman's term would carry that shared gap. Positive ``shooter_s`` = scores more than xG expects; positive
``goalie_g`` = **allows** more than expected (a worse goalie).

**Bayesian season-to-season updating.** Every term has a Gaussian prior: last season's
posterior mean, with precision = ``decay`` × last season's posterior precision (players
change, so old evidence fades). Players without a previous estimate start at
``newcomer_mean`` with the precision of ``newcomer_shots`` imaginary shots. Within a
season the MAP estimate is found by Newton's method (IRLS). Fitting on shots before a date
gives the point-in-time rating as of that date. Priors are aged into each new season with
:data:`AGE_CURVES` (:func:`age_prior`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import polars as pl
from scipy import sparse

from nhl import config
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: Typical p(1-p) of a shot, to turn "imaginary shots" into prior precision.
SHOT_INFORMATION = 0.06
#: Strength of the zero-mean constraint on each group of talent terms.
CENTERING = 1e6
EPS = 1e-6


@dataclass
class Hyper:
    """Prior settings for one role (shooter or goalie)."""

    decay: float
    newcomer_shots: float
    newcomer_mean: float
    max_precision_shots: float = 20000.0


#: Tuned 2026-10-05 by :func:`midseason_eval` (fit to Dec 31, score the rest, 2012-2025):
#: +0.19% log loss vs neutral xG with only the intercept and position effect. Shooters carry
#: almost all of it (goalies alone +0.04%); results were flat for goalie decay 0.8-0.95
#: and 10k-30k newcomer shots.
DEFAULT_HYPER = {
    "shooter": Hyper(decay=0.8, newcomer_shots=1000.0, newcomer_mean=-0.05),
    "goalie": Hyper(decay=0.8, newcomer_shots=10000.0, newcomer_mean=0.025),
}
#: Expected season-to-season change in each role's term (logit) by age: c0 + c1·a + c2·a²,
#: a = age − 27 (clipped to 19-38). Fitted 2026-10-08 by the delta method on chained
#: full-season estimates and scaled ×2 (they are shrunk). Shooters improve about +0.015 a
#: season at 21 and decline −0.014 at 33; goalies get worse (allow more) after about 30.
#: Re-chained fit-to-Dec-31 test, 13 seasons: +1.68 bp log loss, 13 of 13 (shooters alone
#: +1.27 bp, 13/13; goalies alone +0.41 bp, 10/13).
#:
#: Tested and rejected with it: a defenceman-specific newcomer shooter mean (the ``d`` term
#: already carries the gap, ±0.5 bp); goalie back-to-back, heavy workload (4+ starts in 7 days)
#: and long rest as goal-rate effects (pooled 2010-2026: +0.015 ± 0.016, +0.034 ± 0.027,
#: −0.009 ± 0.011 logit; refit each season they cost 1-4 bp, held fixed they add +0.04 bp).
AGE_CURVES: dict[str, tuple[float, float, float]] = {
    "shooter": (-0.00313, -0.00234, 0.000101),
    "goalie": (-0.00240, 0.000611, 0.0000443),
}

#: Talent terms switched off (pinned at 0): the intercept + position baseline.
NO_TALENT = {"shooter": Hyper(1.0, 1e9, 0.0), "goalie": Hyper(1.0, 1e9, 0.0)}


@dataclass
class Fit:
    """Posterior after fitting one span of shots."""

    intercept: float
    terms: pl.DataFrame  # role, player_id, mean, precision, shots, goals, xg
    n_shots: int = 0
    defense: float = 0.0
    hyper: dict[str, Hyper] = field(default_factory=lambda: dict(DEFAULT_HYPER))

    def prior_for_next(self) -> pl.DataFrame:
        """``role, player_id, mean, precision`` carried into the next season (decayed)."""
        decay = pl.col("role").replace_strict({r: h.decay for r, h in self.hyper.items()}, return_dtype=pl.Float64)
        cap = pl.col("role").replace_strict(
            {r: h.max_precision_shots * SHOT_INFORMATION for r, h in self.hyper.items()}, return_dtype=pl.Float64
        )
        return self.terms.select(
            "role", "player_id", "mean", pl.min_horizontal(pl.col("precision") * decay, cap).alias("precision")
        )


def load_shots(store: Store, season: int) -> pl.DataFrame:
    """Unblocked shots with a goalie in net, xG, outcome and shooter position, for one season."""
    positions = store.read_parquet_required(keys.rosters(season)).select(
        "game_id", pl.col("player_id").alias("shooter_id"), "is_defense"
    )
    return (
        store.read_parquet_required(keys.xg_predictions(season))
        .filter(pl.col("goalie_id").is_not_null() & pl.col("shooter_id").is_not_null() & (pl.col("strength_group") != "EN"))
        .join(positions, on=["game_id", "shooter_id"], how="left")
        .select("season", "game_id", "game_date", "event_idx", "shooter_id", "goalie_id", "xg", "is_goal",
                pl.col("is_defense").fill_null(False))
    )


def age_prior(prior: pl.DataFrame | None, ages: pl.DataFrame | None,
              curves: dict[str, tuple[float, float, float]] | None = None) -> pl.DataFrame | None:
    """Shift prior means by each player's expected change into the new season.

    Args:
        prior: ``role, player_id, mean, precision``.
        ages: ``player_id, age`` at the start of the new season (missing ages count as 27).
        curves: Role -> quadratic coefficients (default :data:`AGE_CURVES`).
    """
    if prior is None or ages is None:
        return prior
    curves = AGE_CURVES if curves is None else curves
    j = prior.join(ages, on="player_id", how="left")
    a = np.clip(j["age"].fill_null(27.0).to_numpy().astype(float), 19, 38) - 27
    role = j["role"].to_numpy()
    shift = np.zeros(len(a))
    for r, (c0, c1, c2) in curves.items():
        shift = np.where(role == r, c0 + c1 * a + c2 * a * a, shift)
    return j.with_columns((pl.col("mean") + pl.Series(shift)).alias("mean")).drop("age")


def _priors(shots: pl.DataFrame, prior: pl.DataFrame | None, hyper: dict[str, Hyper]) -> pl.DataFrame:
    """Prior mean and precision for every shooter and goalie in ``shots``."""
    players = pl.concat([
        shots.select(pl.lit("shooter").alias("role"), pl.col("shooter_id").alias("player_id")).unique(),
        shots.select(pl.lit("goalie").alias("role"), pl.col("goalie_id").alias("player_id")).unique(),
    ])
    if prior is not None:
        players = players.join(prior, on=["role", "player_id"], how="left")
    else:
        players = players.with_columns(pl.lit(None, pl.Float64).alias("mean"), pl.lit(None, pl.Float64).alias("precision"))
    new_mean = pl.col("role").replace_strict({r: h.newcomer_mean for r, h in hyper.items()}, return_dtype=pl.Float64)
    new_prec = pl.col("role").replace_strict(
        {r: h.newcomer_shots * SHOT_INFORMATION for r, h in hyper.items()}, return_dtype=pl.Float64
    )
    return players.with_columns(
        pl.col("mean").fill_null(new_mean),
        pl.col("precision").fill_null(new_prec),
    ).sort("role", "player_id")


def fit(shots: pl.DataFrame, prior: pl.DataFrame | None = None, hyper: dict[str, Hyper] | None = None,
        max_iter: int = 25, tol: float = 1e-7) -> Fit:
    """MAP fit of the intercept and every shooter/goalie term on ``shots``.

    Args:
        shots: Output of :func:`load_shots` (any span).
        prior: ``role, player_id, mean, precision`` (``Fit.prior_for_next`` of the previous
            season); None starts everyone as a newcomer.
        hyper: Prior settings per role.

    Returns:
        The posterior.
    """
    hyper = hyper or DEFAULT_HYPER
    terms = _priors(shots, prior, hyper)
    # Columns: 0 intercept, 1 defenseman, then one per player term.
    fixed = 2
    index = {(r, p): i + fixed for i, (r, p) in enumerate(terms.select("role", "player_id").iter_rows())}
    n, k = shots.height, terms.height + fixed
    rows = np.repeat(np.arange(n), 4)
    s_idx = np.array([index[("shooter", p)] for p in shots["shooter_id"].to_list()])
    g_idx = np.array([index[("goalie", p)] for p in shots["goalie_id"].to_list()])
    cols = np.column_stack([np.zeros(n, dtype=int), np.ones(n, dtype=int), s_idx, g_idx]).ravel()
    vals = np.column_stack([np.ones(n), shots["is_defense"].to_numpy().astype(float), np.ones(n), np.ones(n)]).ravel()
    x = sparse.csr_matrix((vals, (rows, cols)), shape=(n, k))

    # Centering: each group's shot-weighted mean term is held at 0 by a large rank-one
    # penalty (Magnus 8's centring term). Without it the free intercept and the talent
    # terms are confounded (every shot has exactly one shooter and one goalie).
    counts = np.asarray(x.sum(axis=0)).ravel()
    role = np.array(["", ""] + terms["role"].to_list())
    centering = np.zeros((k, k))
    for r in ("shooter", "goalie"):
        v = np.where(role == r, counts, 0.0)
        v /= v.sum() or 1.0
        centering += CENTERING * np.outer(v, v)

    xg = np.clip(shots["xg"].to_numpy().astype(float), EPS, 1 - EPS)
    offset = np.log(xg / (1 - xg))
    y = shots["is_goal"].to_numpy().astype(float)
    m = np.concatenate([[0.0, 0.0], terms["mean"].to_numpy()])
    prec = np.concatenate([[1e-8, 1e-8], terms["precision"].to_numpy()])
    def objective(b: np.ndarray) -> float:
        eta = np.clip(offset + x @ b, -30, 30)
        return float(
            np.sum(y * eta - np.logaddexp(0, eta)) - 0.5 * np.sum(prec * (b - m) ** 2) - 0.5 * b @ centering @ b
        )

    # Damped Newton: halve the step until the penalized log-likelihood improves.
    beta = m.copy()
    current = objective(beta)
    for _ in range(max_iter):
        p = 1 / (1 + np.exp(-np.clip(offset + x @ beta, -30, 30)))
        grad = x.T @ (y - p) - prec * (beta - m) - centering @ beta
        hess = (x.T @ sparse.diags(p * (1 - p)) @ x).toarray() + np.diag(prec) + centering
        step = np.linalg.solve(hess, grad)
        scale = 1.0
        while scale > 1e-4:
            candidate = objective(beta + scale * step)
            if candidate >= current - 1e-9:
                break
            scale /= 2
        beta, current = beta + scale * step, candidate
        if np.max(np.abs(scale * step)) < tol:
            break
    post_prec = np.diag(hess)[fixed:]

    goals = shots.group_by("shooter_id").agg(pl.len().alias("shots"), pl.col("is_goal").sum().alias("goals"), pl.col("xg").sum().alias("xg"))
    faced = shots.group_by("goalie_id").agg(pl.len().alias("shots"), pl.col("is_goal").sum().alias("goals"), pl.col("xg").sum().alias("xg"))
    stats = pl.concat([
        goals.rename({"shooter_id": "player_id"}).with_columns(pl.lit("shooter").alias("role")),
        faced.rename({"goalie_id": "player_id"}).with_columns(pl.lit("goalie").alias("role")),
    ]).with_columns(pl.col("goals").cast(pl.Int32), pl.col("shots").cast(pl.Int32))
    out = terms.select("role", "player_id", pl.col("mean").alias("prior_mean")).with_columns(
        pl.Series("mean", beta[fixed:]), pl.Series("precision", post_prec)
    ).join(stats, on=["role", "player_id"], how="left")
    return Fit(intercept=float(beta[0]), terms=out, n_shots=n, hyper=hyper, defense=float(beta[1]))


def predict(fitted: Fit, shots: pl.DataFrame) -> np.ndarray:
    """Talent-adjusted goal probability for ``shots`` (unknown players use newcomer means)."""
    prior = _priors(shots, fitted.terms.select("role", "player_id", "mean", "precision"), fitted.hyper)
    lookup = {(r, p): m for r, p, m in prior.select("role", "player_id", "mean").iter_rows()}
    s = np.array([lookup[("shooter", p)] for p in shots["shooter_id"].to_list()])
    g = np.array([lookup[("goalie", p)] for p in shots["goalie_id"].to_list()])
    xg = np.clip(shots["xg"].to_numpy().astype(float), EPS, 1 - EPS)
    d = shots["is_defense"].to_numpy().astype(float) * fitted.defense
    eta = np.clip(np.log(xg / (1 - xg)) + fitted.intercept + d + s + g, -30, 30)
    return 1 / (1 + np.exp(-eta))


def carry_forward(previous: pl.DataFrame | None, fitted: Fit) -> pl.DataFrame:
    """Next season's prior: this season's decayed posterior, plus decayed priors of players
    who didn't play this season (injured, in the minors), so they aren't forgotten."""
    current = fitted.prior_for_next()
    if previous is None:
        return current
    decay = pl.col("role").replace_strict({r: h.decay for r, h in fitted.hyper.items()}, return_dtype=pl.Float64)
    absent = previous.join(current.select("role", "player_id"), on=["role", "player_id"], how="anti").with_columns(
        pl.col("precision") * decay
    )
    return pl.concat([current, absent])


def chain(seasons: dict[int, pl.DataFrame], hyper: dict[str, Hyper] | None = None) -> dict[int, tuple[pl.DataFrame | None, Fit]]:
    """Fit seasons in order, each starting from the carried-forward posterior.

    Args:
        seasons: Season start year -> shots (:func:`load_shots`).
        hyper: Prior settings.

    Returns:
        Year -> (prior used for that season, full-season fit).
    """
    out: dict[int, tuple[pl.DataFrame | None, Fit]] = {}
    prior = None
    for year in sorted(seasons):
        f = fit(seasons[year], prior, hyper)
        out[year] = (prior, f)
        prior = carry_forward(prior, f)
        logger.info("finishing %s: %d shots, intercept %.4f", config.season_id(year), f.n_shots, f.intercept)
    return out


def midseason_eval(seasons: dict[int, pl.DataFrame], hyper: dict[str, Hyper], eval_years: list[int]) -> dict[str, float]:
    """Point-in-time test: fit through Dec 31 (from the chained prior), score the rest.

    Returns:
        Total log loss on post-Jan-1 shots for the model and for neutral xG with only the
        intercept refit (``baseline``), and the relative improvement.
    """
    from sklearn.metrics import log_loss

    chained = chain({y: s for y, s in seasons.items() if y < max(eval_years)}, hyper)
    model_ll, base_ll, n = 0.0, 0.0, 0
    for year in eval_years:
        prior = carry_forward(chained[year - 1][0], chained[year - 1][1]) if year - 1 in chained else None
        cutoff = pl.date(year + 1, 1, 1)
        shots = seasons[year]
        early, late = shots.filter(pl.col("game_date") < cutoff), shots.filter(pl.col("game_date") >= cutoff)
        if early.is_empty() or late.is_empty():
            continue
        f = fit(early, prior, hyper)
        y = late["is_goal"].to_numpy()
        # Baseline: neutral xG with only the intercept and position effect fitted on the same shots.
        base = predict(fit(early, None, NO_TALENT), late)
        model_ll += log_loss(y, predict(f, late), labels=[0, 1]) * len(y)
        base_ll += log_loss(y, base, labels=[0, 1]) * len(y)
        n += len(y)
    return {"model": model_ll / n, "baseline": base_ll / n, "improvement": 1 - model_ll / base_ll, "n": n}


def season_prior_key(season: int) -> str:
    """Prior carried *into* ``season`` (the decayed posterior through the previous season)."""
    return f"ratings/finishing/prior/{season}.parquet"


def build_priors(store: Store, start_years: list[int], hyper: dict[str, Hyper] | None = None) -> None:
    """Chain full seasons and store the prior entering each following season.

    ``ratings/finishing/prior/{s}`` holds what is known before season *s* starts, so a
    point-in-time fit for any date in *s* needs only that file and *s*'s own shots.
    """
    from nhl.ratings.rapm import _ages

    hyper = hyper or DEFAULT_HYPER
    prior = None
    for year in sorted(start_years):
        prior = age_prior(prior, _ages(store, year))
        f = fit(load_shots(store, config.season_id(year)), prior, hyper)
        prior = carry_forward(prior, f)
        store.put_parquet(season_prior_key(config.season_id(year + 1)), prior)
        logger.info("finishing prior for %s written (%d players)", config.season_id(year + 1), prior.height)


def as_of(store: Store, day: date, hyper: dict[str, Hyper] | None = None) -> pl.DataFrame:
    """Shooter and goalie ratings using only shots before ``day``.

    Returns:
        ``as_of, role, player_id, mean, sd, prior_mean, shots, goals, xg`` plus the fit's
        ``intercept`` and ``defense`` as columns.
    """
    from nhl.ratings.rapm import _ages

    start = day.year if day.month >= 9 else day.year - 1
    season = config.season_id(start)
    prior = age_prior(store.get_parquet(season_prior_key(season)), _ages(store, start))
    shots = load_shots(store, season).filter(pl.col("game_date") < day)
    if shots.is_empty():
        terms = (prior if prior is not None else pl.DataFrame(schema={"role": pl.Utf8, "player_id": pl.Int64, "mean": pl.Float64, "precision": pl.Float64}))
        return terms.with_columns(pl.lit(day).alias("as_of"), (1 / pl.col("precision").sqrt()).alias("sd"))
    f = fit(shots, prior, hyper)
    return f.terms.with_columns(
        pl.lit(day).alias("as_of"),
        (1 / pl.col("precision").sqrt()).alias("sd"),
        pl.lit(f.intercept).alias("intercept"),
        pl.lit(f.defense).alias("defense"),
    ).drop("precision")
