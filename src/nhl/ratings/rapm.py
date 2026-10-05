"""Stint ridge regression with season-to-season priors (M3 phases C/D, Magnus 9).

Solves, in closed form,

    β = (XᵀWX + Λ + K)⁻¹ (XᵀWy + Λβ₀)

* ``W``: stint duration (seconds); ``y``: attacking xG per 60.
* ``Λ``: diagonal prior precision, in **equivalent seconds of ice time**. Players: the
  decayed posterior precision from last season, or ``newcomer_s`` for players without
  one. Context terms get small ridges; coaches a large one.
* ``β₀``: prior means. Last season's estimate for players (``newcomer_off`` /
  ``newcomer_def`` for new ones); 0 for context.
* ``K``: structure. TOI-weighted zero-sum on skater offence and on skater defence, and
  second-difference smoothing on each zone-start type's per-second terms.

Ratings are in xG per 60 relative to league average: positive offence creates more,
positive defence **allows** more (so lower is better).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING

import numpy as np
import polars as pl
from scipy import sparse

from nhl import config
from nhl.ratings.design import ZONE_SECONDS, ZONE_TYPES, Design
from nhl.storage import keys

if TYPE_CHECKING:
    from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

CENTERING = 1e12


@dataclass
class Hyper:
    """Penalty settings (precisions in equivalent seconds of ice time).

    EV defaults tuned 2026-10-05 with :func:`chain_eval` (fit to Dec 31, score the rest,
    2012-2025): an interior optimum at decay 0.7-0.75 and a newcomer prior worth ~100k
    seconds, centred on league average (Magnus 9's below-average newcomer prior scored
    worse here). Results were flat in ``max_precision_s``; ``context_s`` 2k beat 50k.
    """

    decay: float = 0.7
    newcomer_s: float = 1.0e5
    newcomer_off: float = 0.0
    newcomer_def: float = 0.0
    max_precision_s: float = 3.0e6
    context_s: float = 2000.0
    coach_s: float = 2.0e5
    zone_smooth: float = 5.0e5


#: Special teams (PP offence / PK defence), tuned the same way: decay 0.9 and a 20k-second
#: newcomer prior (+0.65% vs no player terms; EV gets +0.17%, PP skill is concentrated).
ST_HYPER = Hyper(decay=0.9, newcomer_s=2.0e4, coach_s=2.0e4)


@dataclass
class Fit:
    """Posterior for one span of stints."""

    intercept: float
    coef: pl.DataFrame  # term, mean, precision
    players: pl.DataFrame  # player_id, side, mean, precision, prior_mean, toi_s
    hyper: Hyper = field(default_factory=Hyper)

    def prior_for_next(self) -> pl.DataFrame:
        """``player_id, side, mean, precision`` carried into the next season (decayed)."""
        return self.players.select(
            "player_id", "side", "mean",
            pl.min_horizontal(pl.col("precision") * self.hyper.decay, pl.lit(self.hyper.max_precision_s)).alias("precision"),
        )


def carry_forward(previous: pl.DataFrame | None, fitted: Fit) -> pl.DataFrame:
    """Next season's prior, keeping (further decayed) priors of players who sat out."""
    current = fitted.prior_for_next()
    if previous is None:
        return current
    absent = previous.join(current.select("player_id", "side"), on=["player_id", "side"], how="anti").with_columns(
        pl.col("precision") * fitted.hyper.decay
    )
    return pl.concat([current, absent])


@dataclass
class Normal:
    """Sufficient statistics of a weighted least-squares problem (intercept appended last)."""

    gram: np.ndarray  # (k+1, k+1): XᵀWX
    rhs: np.ndarray  # (k+1,): XᵀWy
    yy: float  # yᵀWy
    weight: float  # Σw
    toi: np.ndarray  # (k,): Σw per column (seconds on ice for player columns)
    columns: list[str]

    def __add__(self, other: "Normal") -> "Normal":
        assert self.columns == other.columns
        return Normal(self.gram + other.gram, self.rhs + other.rhs, self.yy + other.yy,
                      self.weight + other.weight, self.toi + other.toi, self.columns)

    def group(self, prefix: str) -> np.ndarray:
        """Indices of columns whose name starts with ``prefix``."""
        return np.array([i for i, c in enumerate(self.columns) if c.startswith(prefix)], dtype=int)


def normal_equations(design: Design, mask: np.ndarray | None = None) -> Normal:
    """Gram matrix and right-hand side for the rows in ``mask``."""
    x, y, w = design.x, design.y, design.w
    if mask is not None:
        x, y, w = x[mask], y[mask], w[mask]
    xi = sparse.hstack([x, sparse.csr_matrix(np.ones((x.shape[0], 1)))]).tocsr()
    xtw = xi.T.multiply(w).tocsr()
    k = len(design.columns)
    return Normal(
        gram=(xtw @ xi).toarray(), rhs=np.asarray(xtw @ y).ravel(), yy=float(np.sum(w * y * y)), weight=float(w.sum()),
        toi=np.asarray(x.multiply(w[:, None]).sum(axis=0)).ravel()[:k], columns=list(design.columns),
    )


def _penalties(normal: Normal, prior: pl.DataFrame | None, hyper: Hyper) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Prior precision and mean per column, and the dense structural matrix K."""
    k = len(normal.columns)
    prec = np.full(k, hyper.context_s)
    mean = np.zeros(k)
    lookup: dict[tuple[int, str], tuple[float, float]] = {}
    if prior is not None:
        lookup = {(p, s): (m, pr) for p, s, m, pr in prior.select("player_id", "side", "mean", "precision").iter_rows()}
    for i, c in enumerate(normal.columns):
        if c.startswith(("O:", "D:")):
            side, pid = c[0], int(c[2:])
            m, pr = lookup.get((pid, side), (hyper.newcomer_off if side == "O" else hyper.newcomer_def, hyper.newcomer_s))
            mean[i], prec[i] = m, pr
        elif c.startswith("coach:"):
            prec[i] = hyper.coach_s

    big = np.zeros((k, k))
    for side in ("O", "D"):
        idx = normal.group(f"{side}:")
        v = np.zeros(k)
        v[idx] = normal.toi[idx] / max(normal.toi[idx].sum(), 1.0)
        big += CENTERING * np.outer(v, v)
    for t in ZONE_TYPES:
        idx = normal.group(f"zone:{t}:")
        if len(idx) == ZONE_SECONDS:
            d2 = np.zeros((ZONE_SECONDS - 2, k))
            for j in range(ZONE_SECONDS - 2):
                d2[j, idx[j]], d2[j, idx[j + 1]], d2[j, idx[j + 2]] = 1.0, -2.0, 1.0
            big += hyper.zone_smooth * d2.T @ d2
    return prec, mean, big


def fit(normal: Normal, prior: pl.DataFrame | None = None, hyper: Hyper | None = None) -> Fit:
    """Solve the penalized least squares.

    Args:
        normal: From :func:`normal_equations` (any span of rows).
        prior: ``player_id, side, mean, precision`` (``carry_forward`` output).
        hyper: Penalty settings.
    """
    hyper = hyper or Hyper()
    k = len(normal.columns)
    prec, mean, big = _penalties(normal, prior, hyper)
    h = normal.gram.copy()
    h[:k, :k] += np.diag(prec) + big
    h[k, k] += 1e-6  # keeps an empty span (no rows) solvable
    rhs = normal.rhs.copy()
    rhs[:k] += prec * mean
    beta = np.linalg.solve(h, rhs)
    # Posterior precision for carrying forward: data + prior only (the centering and
    # smoothing constraints are structural, not evidence about a player).
    post = np.diag(normal.gram)[:k] + prec

    coef = pl.DataFrame({"term": normal.columns, "mean": beta[:k], "precision": post, "prior_mean": mean, "toi_s": normal.toi})
    players = (
        coef.filter(pl.col("term").str.contains(r"^[OD]:"))
        .with_columns(
            pl.col("term").str.slice(0, 1).alias("side"),
            pl.col("term").str.slice(2).cast(pl.Int64).alias("player_id"),
        )
        .select("player_id", "side", "mean", "precision", "prior_mean", "toi_s")
    )
    return Fit(intercept=float(beta[k]), coef=coef, players=players, hyper=hyper)


def weighted_sse(fitted: Fit, normal: Normal) -> float:
    """Σ w (y − ŷ)² over the rows summarised by ``normal`` (same columns as the fit)."""
    b = np.append(fitted.coef["mean"].to_numpy(), fitted.intercept)
    return float(normal.yy - 2 * b @ normal.rhs + b @ normal.gram @ b)


def predict(fitted: Fit, design: Design, mask: np.ndarray | None = None) -> np.ndarray:
    """Predicted xG per 60 for the design's rows (columns must match the fit)."""
    x = design.x if mask is None else design.x[mask]
    return x @ fitted.coef["mean"].to_numpy() + fitted.intercept


@dataclass
class AgeCurve:
    """Expected season-to-season change in a skater's rating by age (per side).

    ``shift(age) = scale × (c0 + c1·(age−27) + c2·(age−27)²)``, fitted to the TOI-weighted
    change of chained full-season estimates (the delta method). Those changes are of
    *shrunk* estimates and so understate real aging; ``scale`` is tuned.
    """

    coef: dict[str, tuple[float, float, float]]
    scale: float = 1.0

    def shift(self, side: str, age: np.ndarray) -> np.ndarray:
        """Prior-mean adjustment for players of ``age`` (years) on ``side``."""
        c0, c1, c2 = self.coef[side]
        a = np.clip(age, 19, 38) - 27
        return self.scale * (c0 + c1 * a + c2 * a * a)

    @staticmethod
    def estimate(deltas: pl.DataFrame) -> "AgeCurve":
        """Fit from ``side, age, delta, weight`` rows."""
        coef = {}
        for side in ("O", "D"):
            d = deltas.filter(pl.col("side") == side)
            a = d["age"].to_numpy().astype(float) - 27
            coef[side] = tuple(np.polyfit(a, d["delta"].to_numpy(), 2, w=np.sqrt(d["weight"].to_numpy()))[::-1])
        return AgeCurve(coef=coef)


def age_prior(prior: pl.DataFrame | None, ages: pl.DataFrame | None, curve: AgeCurve | None) -> pl.DataFrame | None:
    """Shift prior means by the expected aging of each player into the new season.

    Args:
        prior: ``player_id, side, mean, precision``.
        ages: ``player_id, age`` at the start of the new season.
        curve: The aging curve; None leaves the prior unchanged.
    """
    if prior is None or ages is None or curve is None:
        return prior
    j = prior.join(ages, on="player_id", how="left")
    age = j["age"].fill_null(27.0).to_numpy().astype(float)
    side = j["side"].to_numpy()
    shift = np.where(side == "O", curve.shift("O", age), curve.shift("D", age))
    return j.with_columns((pl.col("mean") + pl.Series(shift)).alias("mean")).drop("age")


def chain_eval(
    halves: dict[int, tuple[Normal, Normal]], hyper: Hyper, eval_years: list[int], baseline: Hyper | None = None,
    ages: dict[int, pl.DataFrame] | None = None, curve: AgeCurve | None = None,
) -> dict[str, float]:
    """Point-in-time test: chain full seasons, then for each eval season fit through Dec 31
    and score the rest by weighted squared error of xG per 60.

    Args:
        halves: Year -> (stints before Jan 1, stints from Jan 1), same columns per year.
        hyper: Settings under test.
        eval_years: Seasons to score.
        baseline: Settings for the comparison model (default: players pinned at 0).

    Returns:
        ``model`` and ``baseline`` weighted MSE and the relative ``improvement``.
        With ``ages`` (year -> ``player_id, age``) and ``curve``, priors are aged at
        the start of every season.
    """
    baseline = baseline or Hyper(decay=1.0, newcomer_s=1e12, newcomer_off=0.0, newcomer_def=0.0)
    prior, priors = None, {}
    for year in sorted(halves):
        prior = age_prior(prior, (ages or {}).get(year), curve)
        priors[year] = prior
        early, late = halves[year]
        prior = carry_forward(prior, fit(early + late, prior, hyper))
    sse_m = sse_b = weight = 0.0
    for year in eval_years:
        early, late = halves[year]
        sse_m += weighted_sse(fit(early, priors[year], hyper), late)
        sse_b += weighted_sse(fit(early, None, baseline), late)
        weight += late.weight
    return {"model": sse_m / weight, "baseline": sse_b / weight, "improvement": 1 - sse_m / sse_b}


# --- production: season priors and point-in-time fits -----------------------------------


def _ages(store: "Store", start_year: int) -> pl.DataFrame:
    players = store.read_parquet_required(keys.PLAYERS)
    return players.select(
        "player_id",
        ((pl.date(start_year, 10, 1) - pl.col("birth_date").str.to_date(strict=False)).dt.total_days() / 365.25).alias("age"),
    ).drop_nulls()


def season_design(store: "Store", season: int, state: str = "EV") -> Design:
    """Design for one season's stints (``EV`` or ``ST``)."""
    from nhl.ratings.design import attack_rows, build
    from nhl.reference.venues import schedule_context_key

    rows = attack_rows(
        store.read_parquet_required(keys.stints(season)),
        store.read_parquet_required(schedule_context_key(season)),
        store.read_parquet_required(keys.coaches(season)),
        state=state,
    )
    games = store.read_parquet_required(keys.GAMES).select("game_id", "game_date")
    design = build(rows)
    design.rows = design.rows.join(games, on="game_id", how="left")
    return design


def prior_key(state: str, season: int) -> str:
    """Prior carried *into* ``season`` for ``state`` (decayed and aged)."""
    return f"ratings/{state.lower()}/prior/{season}.parquet"


def curve_key(state: str) -> str:
    """Stored aging curve for ``state``."""
    return f"ratings/{state.lower()}/age_curve.json"


def build_priors(store: "Store", start_years: list[int], state: str = "EV", hyper: Hyper | None = None,
                 curve: AgeCurve | None = None) -> None:
    """Chain full seasons and store the (aged) prior entering each following season."""
    import json

    hyper = hyper or (Hyper() if state == "EV" else ST_HYPER)
    if curve is not None:
        store.put_bytes(curve_key(state), json.dumps({"coef": curve.coef, "scale": curve.scale}).encode())
    prior = None
    for year in sorted(start_years):
        season = config.season_id(year)
        prior = age_prior(prior, _ages(store, year), curve)
        f = fit(normal_equations(season_design(store, season, state)), prior, hyper)
        prior = carry_forward(prior, f)
        store.put_parquet(prior_key(state, config.season_id(year + 1)), prior)
        logger.info("%s prior for %s written (%d player-sides)", state, config.season_id(year + 1), prior.height)


def load_curve(store: "Store", state: str) -> AgeCurve | None:
    """The stored aging curve, if any."""
    import json

    raw = store.get_bytes(curve_key(state))
    if raw is None:
        return None
    body = json.loads(raw)
    return AgeCurve(coef={k: tuple(v) for k, v in body["coef"].items()}, scale=body["scale"])


def as_of(store: "Store", day: date, state: str = "EV", hyper: Hyper | None = None) -> tuple[Fit, pl.DataFrame]:
    """Ratings from stints before ``day`` plus the stored prior for its season.

    Returns:
        The fit and a tidy player table ``as_of, player_id, side, mean, sd_s, prior_mean, toi_s``
        (``sd_s`` = 1/√precision, in xG/60 per √second of evidence — comparable within a run).
    """
    hyper = hyper or (Hyper() if state == "EV" else ST_HYPER)
    start = day.year if day.month >= 9 else day.year - 1
    season = config.season_id(start)
    prior = age_prior(store.get_parquet(prior_key(state, season)), _ages(store, start), load_curve(store, state))
    design = season_design(store, season, state)
    mask = (design.rows["game_date"] < day).to_numpy()
    f = fit(normal_equations(design, mask), prior, hyper)
    table = f.players.with_columns(pl.lit(day).alias("as_of"), (1 / pl.col("precision").sqrt()).alias("sd_s"))
    return f, table
