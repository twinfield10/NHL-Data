"""Phase B backtest: player goals / assists / points projections vs simple baselines.

For every skater who dressed in a 2016-17 to 2025-26 game and was in that morning's projected
lineup, score P(stat >= k) on what happened. Everything is point-in-time: the team goal
distribution comes from the honest pregame score matrices (``predictions/pregame_history``),
deployment from the morning lineup projection, rates from earlier games only.

Methods (all scored on the same rows):

* ``model``: :mod:`nhl.props.project` with the simulator's goal distribution for the team.
* ``no_team``: the same player shares, but every team at one league-average Poisson goal
  count (no opponent, no team strength, no game script).
* ``season_avg``: the player's per-game average this season, shrunk toward his own last
  season (or his position's league average) with :data:`SEASON_AVG_PRIOR_GAMES` games; Poisson.
* ``last10``: his per-game average over his last 10 games (any season), shrunk toward his
  position's league average with :data:`LAST10_PRIOR_GAMES` games; Poisson.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import poisson

from nhl.pregame import lineups
from nhl.props import project, rates
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

METHODS = ("model", "no_team", "season_avg", "last10")
STATS = ("goals", "ast", "points")
SEASON_AVG_PRIOR_GAMES = 10.0
LAST10_PRIOR_GAMES = 3.0
EPS = 1e-6


def deployment(store: Store, season: int, refresh: bool = False) -> pl.DataFrame:
    """Morning-of projected deployment for every team-game of ``season`` (cached in S3).

    The same projection the pregame history prices with (:func:`nhl.pregame.backtest.run_season`).
    """
    key = keys.pregame_deployment(season)
    if not refresh and (cached := store.get_parquet(key)) is not None:
        return cached
    games = store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final"))
    targets = pl.concat([games.select("game_id", pl.col(f"{s}_team_id").alias("team_id"), "game_date")
                         for s in ("home", "away")])
    dep = lineups.project(store, targets, [season - 10001, season])
    store.put_parquet(key, dep)
    return dep


def _poisson_tails(mean: np.ndarray, stat: str) -> dict[str, np.ndarray]:
    return {f"p_{stat}_{k}": poisson.sf(k - 1, mean) for k in project.THRESHOLDS[stat]}


def baselines(logs: dict[int, pl.DataFrame], season: int) -> pl.DataFrame:
    """``season_avg`` and ``last10`` probabilities per skater-game of ``season``."""
    def per_game(frame: pl.DataFrame) -> pl.DataFrame:
        goals = pl.sum_horizontal(*[f"goals_{b}" for b in rates.BUCKET_NAMES])
        ast = pl.sum_horizontal(*[f"ast_{b}" for b in rates.BUCKET_NAMES])
        return frame.select("game_id", "game_date", "season", "player_id", "grp", goals.alias("goals"),
                            ast.alias("ast"), (goals + ast).alias("points"))

    cur, last = per_game(logs[season]), per_game(logs[season - 10001])
    lg = last.group_by("grp").agg([pl.col(s).mean().alias(f"lg_{s}") for s in STATS])
    own_last = last.group_by("player_id").agg([pl.col(s).mean().alias(f"own_{s}") for s in STATS])

    cur = cur.sort("game_date", "game_id").join(lg, on="grp", how="left").join(own_last, on="player_id", how="left")
    gp = pl.int_range(pl.len()).over("player_id")
    season_avg = cur.with_columns(
        [((pl.col(s).cum_sum().over("player_id") - pl.col(s)
           + SEASON_AVG_PRIOR_GAMES * pl.coalesce(f"own_{s}", f"lg_{s}")) / (gp + SEASON_AVG_PRIOR_GAMES)).alias(f"m_{s}")
         for s in STATS])

    both = pl.concat([last, cur.select(last.columns)]).sort("game_date", "game_id").join(lg, on="grp", how="left")
    roll = both.with_columns(
        [(pl.col(s).shift(1).rolling_sum(10, min_samples=1).over("player_id").fill_null(0.0)).alias(f"sum_{s}") for s in STATS]
        + [pl.col(STATS[0]).shift(1).is_not_null().cast(pl.Int32).rolling_sum(10, min_samples=1).over("player_id")
           .fill_null(0).alias("n10")]
    ).with_columns([((pl.col(f"sum_{s}") + LAST10_PRIOR_GAMES * pl.col(f"lg_{s}")) / (pl.col("n10") + LAST10_PRIOR_GAMES))
                    .alias(f"m_{s}") for s in STATS]).filter(pl.col("season") == season)

    out = []
    for name, frame in (("season_avg", season_avg), ("last10", roll)):
        cols = {}
        for s in STATS:
            cols.update(_poisson_tails(frame[f"m_{s}"].to_numpy(), s))
            cols[f"exp_{s}"] = frame[f"m_{s}"].to_numpy()
        out.append(frame.select("game_id", "player_id").with_columns(pl.lit(name).alias("method"),
                                                                     **{k: pl.Series(v) for k, v in cols.items()}))
    return pl.concat(out)


def run_season(store: Store, season: int, shrink: rates.Shrink = rates.Shrink(),
               logs: dict[int, pl.DataFrame] | None = None, dep: pl.DataFrame | None = None,
               power: float = project.RATE_POWER) -> pl.DataFrame:
    """All methods' probabilities and the outcome for every scored skater-game of ``season``.

    Returns:
        Long frame: ``game_id, player_id, team_id, position, method`` and ``p_{stat}_{k}``,
        ``exp_{stat}``, plus the realized ``goals``, ``ast``, ``points``.
    """
    logs = dict(logs or {})
    for s in (season, season - 10001, season - 20002):
        if s not in logs:
            logs[s] = rates.bucket_logs(store, s)
    history = store.read_parquet_required(keys.pregame_history(season))
    dep = dep if dep is not None else deployment(store, season)
    pit, mix = rates.season_rates(store, season, shrink, logs)
    sh = project.shares(dep, pit, mix, power)

    dists = project.team_goal_dists(history)
    league_mean = float(dists["mean_goals"].mean())
    model = project.project_players(sh, dists).with_columns(pl.lit("model").alias("method"))
    flat = project.project_players(sh, project.poisson_goal_dists(dists, league_mean)).with_columns(
        pl.lit("no_team").alias("method"))
    prob_cols = [f"p_{s}_{k}" for s in STATS for k in project.THRESHOLDS[s]] + [f"exp_{s}" for s in STATS]

    # Score only skaters who dressed and were projected (a prop on a player who sits is void).
    outcome = pit.select("game_id", "player_id", "goals", "ast", "points")
    keep = model.filter(pl.col("player_id").is_not_null()).select("game_id", "player_id", "team_id", "position").unique(
        ["game_id", "player_id"]).join(outcome, on=["game_id", "player_id"], how="inner")
    frames = [m.filter(pl.col("player_id").is_not_null()).unique(["game_id", "player_id"]).select(
        "game_id", "player_id", "method", *prob_cols) for m in (model, flat)]
    frames.append(baselines(logs, season).select("game_id", "player_id", "method", *prob_cols))
    return keep.join(pl.concat(frames), on=["game_id", "player_id"], how="inner").with_columns(
        pl.lit(season).alias("season"))


def scores(results: pl.DataFrame, by: tuple[str, ...] = ("method",)) -> pl.DataFrame:
    """Log loss and Brier per method (and ``by``), for every stat threshold."""
    rows = []
    for s in STATS:
        for k in project.THRESHOLDS[s]:
            y = (pl.col(s) >= k).cast(pl.Float64)
            p = pl.col(f"p_{s}_{k}").clip(EPS, 1 - EPS)
            rows.append(results.group_by(*by).agg(
                pl.lit(f"{s}>={k}").alias("target"), pl.len().alias("n"), y.mean().alias("rate"),
                p.mean().alias("mean_p"),
                (-(y * p.log() + (1 - y) * (1 - p).log())).mean().alias("logloss"),
                ((p - y) ** 2).mean().alias("brier")))
    return pl.concat(rows).sort("target", *by)


def calibration(results: pl.DataFrame, method: str = "model", bins: int = 10) -> pl.DataFrame:
    """Mean predicted vs realized rate by predicted-probability decile, per target."""
    rows = []
    r = results.filter(pl.col("method") == method)
    for s in STATS:
        for k in project.THRESHOLDS[s]:
            col = f"p_{s}_{k}"
            rows.append(r.with_columns(pl.col(col).qcut(bins, labels=[str(i) for i in range(bins)],
                                                         allow_duplicates=True).alias("bin"))
                        .group_by("bin").agg(pl.lit(f"{s}>={k}").alias("target"), pl.len().alias("n"),
                                             pl.col(col).mean().alias("predicted"),
                                             (pl.col(s) >= k).mean().alias("realized")))
    return pl.concat(rows).sort("target", "bin")


def run(store: Store, seasons: list[int], shrink: rates.Shrink = rates.Shrink(), write: bool = True) -> pl.DataFrame:
    """Backtest every season (stored per season under :func:`keys.props_backtest` when ``write``)."""
    logs: dict[int, pl.DataFrame] = {}
    frames = []
    for season in seasons:
        for s in (season, season - 10001, season - 20002):
            if s not in logs:
                logs[s] = rates.bucket_logs(store, s)
        r = run_season(store, season, shrink, logs)
        if write:
            store.put_parquet(keys.props_backtest(season), r)
        sc = scores(r).filter(pl.col("target") == "points>=1")
        logger.info("%s: %d skater-games; points>=1 log loss %s", season, r.filter(pl.col("method") == "model").height,
                    ", ".join(f"{m} {v:.4f}" for m, v in sc.select("method", "logloss").iter_rows()))
        frames.append(r)
    return pl.concat(frames)


def _md(df: pl.DataFrame, digits: int = 4) -> str:
    cols = df.columns
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.iter_rows():
        lines.append("| " + " | ".join(f"{v:.{digits}f}" if isinstance(v, float) else str(v) for v in row) + " |")
    return "\n".join(lines)


def write_report(results: pl.DataFrame, path: Path) -> str:
    """Markdown report: pooled and per-season scores, wins per season, calibration."""
    pooled = scores(results)
    per_season = scores(results, by=("season", "method"))
    best = per_season.with_columns(
        (pl.col("logloss") == pl.col("logloss").min().over("season", "target")).alias("best"),
        pl.col("logloss").filter(pl.col("method") == "model").first().over("season", "target").alias("_m"),
    )
    gate = best.filter(pl.col("method") != "model").group_by("target").agg(
        (pl.col("_m") < pl.col("logloss")).all().alias("model_beats_all_every_season"),
        pl.col("season").n_unique().alias("seasons"))
    wide = per_season.pivot(on="method", index=["target", "season"], values="logloss").sort("target", "season")
    text = "\n\n".join([
        "# Player props: goals / assists / points backtest (M9 phase B)",
        f"Seasons {results['season'].min()}-{results['season'].max()}, "
        f"{results.filter(pl.col('method') == 'model').height:,} skater-games (dressed and in the morning projection). "
        "Methods: see `src/nhl/props/backtest.py`. Lower log loss is better.",
        "## Gate: model beats every baseline in every season", _md(gate.sort("target")),
        "## Pooled", _md(pooled.select("target", "method", "n", "rate", "mean_p", "logloss", "brier")),
        "## Log loss by season", _md(wide),
        "## Calibration (model, by predicted decile)", _md(calibration(results).drop("bin")),
    ]) + "\n"
    path.write_text(text)
    return text


__all__ = ["METHODS", "baselines", "calibration", "deployment", "run", "run_season", "scores", "write_report"]
