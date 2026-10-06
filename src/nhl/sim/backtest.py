"""Backtest the simulator (the M4 bar) against actual results and a Poisson baseline.

For each season: point-in-time constants and inputs (actual lineups, latest rating
snapshot before each game), N simulations per game, market prices, and the outcome.

**Baseline** (from the roadmap): independent Poisson goals with
λ = team xGF/60 to date × opponent xGA/60 to date ÷ league xG/60 × league goals/xG × home
edge (each side's rates shrunk toward the league with 10 games of league evidence; league
levels from the previous season), and ties split 50/50.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import poisson

from nhl.sim import constants as sim_constants
from nhl.sim import engine, inputs, markets
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

SHRINK_GAMES = 10
MAX_GOALS = 15


def poisson_baseline(store: Store, season: int, games: pl.DataFrame) -> pl.DataFrame:
    """``game_id, p_home_win_poisson, p_over_5.5_poisson, p_over_6.5_poisson``."""
    logs = store.read_parquet_required(keys.team_game_logs(season)).filter(pl.col("strength") == "all")
    dates = store.read_parquet_required(keys.GAMES).select("game_id", "game_date")
    t = (
        logs.join(dates, on="game_id").sort("game_date", "game_id")
        .with_columns(
            *[(pl.col(c).cum_sum().over("team_id") - pl.col(c)).alias(f"{c}_td") for c in ("xgf", "xga", "toi_s", "gf")],
            (pl.int_range(pl.len()).over("team_id")).alias("gp"),
        )
    )
    # League levels from the previous season only (point-in-time, like the simulator).
    prev = store.read_parquet_required(keys.team_game_logs(season - 10001)).filter(pl.col("strength") == "all")
    league_xg60 = float(prev["xgf"].sum() / (prev["toi_s"].sum() / 3600))
    league_g_per_xg = float(prev["gf"].sum() / prev["xgf"].sum())
    home_goals = prev.filter(pl.col("is_home"))["gf"].sum()
    away_goals = prev.filter(~pl.col("is_home"))["gf"].sum()
    home_edge = float(np.sqrt(home_goals / away_goals))
    prior_s = SHRINK_GAMES * 3600
    t = t.with_columns(
        ((pl.col("xgf_td") + league_xg60 * SHRINK_GAMES) / ((pl.col("toi_s_td") + prior_s) / 3600)).alias("xgf60"),
        ((pl.col("xga_td") + league_xg60 * SHRINK_GAMES) / ((pl.col("toi_s_td") + prior_s) / 3600)).alias("xga60"),
    ).select("game_id", "team_id", "is_home", "xgf60", "xga60")
    h = t.filter(pl.col("is_home")).rename({"xgf60": "h_xgf", "xga60": "h_xga"}).drop("is_home", "team_id")
    a = t.filter(~pl.col("is_home")).rename({"xgf60": "a_xgf", "xga60": "a_xga"}).drop("is_home", "team_id")
    j = games.select("game_id").join(h, on="game_id", how="left").join(a, on="game_id", how="left")
    lam_h = j["h_xgf"].to_numpy() * j["a_xga"].to_numpy() / league_xg60 * league_g_per_xg * home_edge
    lam_a = j["a_xgf"].to_numpy() * j["h_xga"].to_numpy() / league_xg60 * league_g_per_xg / home_edge
    k = np.arange(MAX_GOALS + 1)
    ph = poisson.pmf(k[None, :], lam_h[:, None])
    pa = poisson.pmf(k[None, :], lam_a[:, None])
    joint = ph[:, :, None] * pa[:, None, :]
    diff = k[:, None] - k[None, :]
    tot = k[:, None] + k[None, :]
    win = (joint * (diff > 0)).sum(axis=(1, 2)) + 0.5 * (joint * (diff == 0)).sum(axis=(1, 2))
    return j.select("game_id").with_columns(
        pl.Series("p_home_win_poisson", win),
        pl.Series("p_over_5.5_poisson", (joint * (tot > 5.5)).sum(axis=(1, 2))),
        pl.Series("p_over_6.5_poisson", (joint * (tot > 6.5)).sum(axis=(1, 2))),
    )


def run_season(store: Store, season: int, n_sims: int = 1000, scale: float | None = None,
               sigma: float | None = None, pace: float | None = None, snapshots: list[date] | None = None,
               prepared: tuple | None = None, team_prior_h: float | None = None,
               team_fin_prior_g: float | None = None) -> pl.DataFrame:
    """Per-game prices, baseline and outcome for one season (``prepared`` = (inputs, constants))."""
    if prepared is None:
        c = sim_constants.estimate(store, season)
        inp = inputs.build_season(store, season, c, snapshots)
    else:
        inp, c = prepared
    res = engine.simulate(inp, c, season, n_sims=n_sims, scale=scale, sigma=sigma, pace=pace, team_prior_h=team_prior_h,
                          team_fin_prior_g=team_fin_prior_g)
    priced = pl.concat([inp.games, markets.prices(res)], how="horizontal")
    base = poisson_baseline(store, season, inp.games)
    shootout = (pl.col("season_type") == "R") & (pl.col("last_period") == 5)
    return priced.join(base, on="game_id", how="left").with_columns(
        pl.lit(season).alias("season"),
        (pl.col("home_score") > pl.col("away_score")).cast(pl.Int8).alias("home_won"),
        (pl.col("home_score") - pl.col("away_score")).alias("margin"),
        (pl.col("home_score") + pl.col("away_score")).alias("total"),
        (pl.col("last_period") > 3).alias("went_ot"),
        shootout.alias("shootout"),
    )


def _logloss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p, 1e-4, 1 - 1e-4)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def summarize(results: pl.DataFrame) -> dict[str, float]:
    """The M4 bar metrics."""
    r = results.filter(pl.col("p_home_win_poisson").is_not_null())
    y = r["home_won"].to_numpy()
    out = {
        "games": float(r.height),
        "logloss_sim": _logloss(y, r["p_home_win"].to_numpy()),
        "logloss_poisson": _logloss(y, r["p_home_win_poisson"].to_numpy()),
        "brier_sim": float(np.mean((r["p_home_win"].to_numpy() - y) ** 2)),
        "margin2_sim": float((r["p_home_minus_1_5"] + r["p_away_minus_1_5"]).mean()),
        "margin2_actual": float((r["margin"].abs() >= 2).mean()),
        "ot_sim": float(r["p_overtime"].mean()),
        "ot_actual": float(r["went_ot"].mean()),
        "goals_sim": float((r["mean_home_goals"] + r["mean_away_goals"]).mean()),
        "goals_actual": float(r["total"].mean()),
    }
    for line in (5.5, 6.5):
        over = (r["total"] > line).cast(pl.Int8).to_numpy()
        out[f"over_{line}_sim"] = float(r[f"p_over_{line}"].mean())
        out[f"over_{line}_actual"] = float(over.mean())
        out[f"logloss_over_{line}_sim"] = _logloss(over, r[f"p_over_{line}"].to_numpy())
        out[f"logloss_over_{line}_poisson"] = _logloss(over, r[f"p_over_{line}_poisson"].to_numpy())
    pl_home = (r["margin"] >= 2).cast(pl.Int8).to_numpy()
    out["logloss_home_puckline_sim"] = _logloss(pl_home, r["p_home_minus_1_5"].to_numpy())
    cal = calibration(r)
    big = cal.filter(pl.col("n") >= 100)
    out["max_calibration_error"] = float((big["predicted"] - big["actual"]).abs().max()) if big.height else float("nan")
    return out


def calibration(results: pl.DataFrame, edges: tuple[float, ...] = (0.3, 0.4, 0.45, 0.5, 0.55, 0.6, 0.7)) -> pl.DataFrame:
    """Binned predicted vs actual home win rate."""
    return (
        results.with_columns(pl.col("p_home_win").cut(list(edges)).alias("bin"))
        .group_by("bin")
        .agg(pl.len().alias("n"), pl.col("p_home_win").mean().alias("predicted"), pl.col("home_won").mean().alias("actual"))
        .sort("bin")
    )


def write_report(results: pl.DataFrame, path: Path, notes: str = "") -> str:
    """Markdown report: overall bar, per season, calibration."""
    overall = summarize(results)
    per_season = {s: summarize(results.filter(pl.col("season") == s)) for s in sorted(results["season"].unique().to_list())}
    keys_ = ["logloss_sim", "logloss_poisson", "margin2_sim", "margin2_actual", "ot_sim", "ot_actual", "goals_sim", "goals_actual"]
    by_type = {t: summarize(results.filter(pl.col("season_type") == t)) for t in ("R", "P")
               if results.filter(pl.col("season_type") == t).height}
    lines = [
        "# M4 backtest: game simulator",
        "",
        f"Generated {date.today().isoformat()} by `nhl backtest-sim`. Actual lineups, point-in-time ratings "
        "(latest snapshot before each game) and constants (three prior seasons). " + notes,
        "",
        "## Overall",
        "",
        "| metric | value |",
        "|---|---|",
        *[f"| {k} | {v:.4f} |" for k, v in overall.items()],
        "",
        "## By season",
        "",
        "| season | games | " + " | ".join(keys_) + " |",
        "|---|---|" + "---|" * len(keys_),
        *[f"| {s} | {int(m['games'])} | " + " | ".join(f"{m[k]:.4f}" for k in keys_) + " |" for s, m in per_season.items()],
        "",
        "## Regular season vs playoffs",
        "",
        "| type | games | " + " | ".join(keys_) + " |",
        "|---|---|" + "---|" * len(keys_),
        *[f"| {t} | {int(m['games'])} | " + " | ".join(f"{m[k]:.4f}" for k in keys_) + " |" for t, m in by_type.items()],
        "",
        "## Calibration (P(home win), all seasons)",
        "",
        "| bin | games | predicted | actual |",
        "|---|---|---|---|",
        *[f"| {b} | {n} | {p:.3f} | {a:.3f} |" for b, n, p, a in calibration(results).iter_rows()],
        "",
        "**Bar** ([M4 plan](../plans/m4-simulator.md)): calibration within 2 points in bins with ≥100 games; "
        "2+ goal margins, OT rate and total goals within sampling error; moneyline log loss below the Poisson baseline.",
        "",
    ]
    text = "\n".join(lines)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return text


def run(store: Store, seasons: list[int], n_sims: int = 1000) -> pl.DataFrame:
    """Backtest seasons with the engine's calibrated defaults; store per-game results."""
    snapshots = inputs.snapshot_dates(store)
    frames = []
    for season in seasons:
        r = run_season(store, season, n_sims=n_sims, snapshots=snapshots)
        store.put_parquet(keys.sim_backtest(season), r)
        frames.append(r)
        logger.info("%s: backtest %s", season, {k: round(v, 4) for k, v in summarize(r).items() if k.startswith("logloss")})
    return pl.concat(frames)
