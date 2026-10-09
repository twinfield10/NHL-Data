"""Phase C backtest: player shots on goal and blocks, and goalie saves, vs simple baselines.

Same rules as :mod:`nhl.props.backtest`: every input is point-in-time, skaters are scored
when they dressed and were in the morning projection, and every method is scored on the same
rows. Goalies are scored on the games they started.

Methods:

* ``model``: :mod:`nhl.props.volume` (team model × player share; saves from the opponent's
  shots and the simulator's goals).
* ``no_team``: the same shares, with every team at the league-average shot (or block) count, and
  for saves the league-average shots against and goals.
* ``season_avg`` / ``last10``: the player's (goalie's) per-game average this season (shrunk
  toward his last season or the league) / over his last 10 games (starts), Poisson.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import poisson

from nhl.props import backtest as B
from nhl.props import project, rates
from nhl.props import volume as V
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

THRESHOLDS = {"shots": (1, 2, 3, 4, 5), "blocks": (1, 2, 3), "saves": (20, 23, 25, 27, 30)}
STAT_OF = {"shots": "sog", "blocks": "blk"}
SEASON_PRIOR = 10.0
LAST10_PRIOR = 3.0
GOALIE_PRIOR_STARTS = 5.0


def _poisson_cols(mean: np.ndarray, stat: str) -> dict[str, np.ndarray]:
    return {f"p_{stat}_{k}": poisson.sf(k - 1, mean) for k in THRESHOLDS[stat]} | {f"exp_{stat}": mean}


def _baselines(frame: pl.DataFrame, last: pl.DataFrame, stat: str, who: str, season: int, prior_n: float) -> pl.DataFrame:
    """``season_avg`` and ``last10`` Poisson probabilities for ``stat`` per row of ``frame`` (this season).

    Args:
        frame: This season's rows ``game_id, game_date, {who}, stat``.
        last: Last season's rows, same columns.
        who: The id column (``player_id`` or ``goalie_id``).
        prior_n: Games of prior in the season average.
    """
    lg = float(last[stat].mean()) if not last.is_empty() else float(frame[stat].mean())
    own = last.group_by(who).agg(pl.col(stat).mean().alias("_own"))
    cur = frame.sort("game_date", "game_id").join(own, on=who, how="left")
    n = pl.int_range(pl.len()).over(who)
    sa = cur.with_columns(((pl.col(stat).cum_sum().over(who) - pl.col(stat) + prior_n * pl.col("_own").fill_null(lg))
                           / (n + prior_n)).alias("_m"))
    both = pl.concat([last.select("game_id", "game_date", who, stat).with_columns(pl.lit(False).alias("_cur")),
                      frame.select("game_id", "game_date", who, stat).with_columns(pl.lit(True).alias("_cur"))]).sort("game_date", "game_id")
    roll = both.with_columns(
        pl.col(stat).shift(1).rolling_sum(10, min_samples=1).over(who).fill_null(0.0).alias("_s"),
        pl.col(stat).shift(1).is_not_null().cast(pl.Int32).rolling_sum(10, min_samples=1).over(who).fill_null(0).alias("_n"),
    ).with_columns(((pl.col("_s") + LAST10_PRIOR * lg) / (pl.col("_n") + LAST10_PRIOR)).alias("_m")).filter(pl.col("_cur"))
    out = []
    for name, f in (("season_avg", sa), ("last10", roll)):
        cols = _poisson_cols(f["_m"].to_numpy(), stat)
        out.append(f.select("game_id", who).with_columns(pl.lit(name).alias("method"), *[pl.Series(k, v) for k, v in cols.items()]))
    return pl.concat(out)


def _skater_frames(logs: dict[int, pl.DataFrame], season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    def per_game(f: pl.DataFrame) -> pl.DataFrame:
        return f.select("game_id", "game_date", "player_id",
                        pl.sum_horizontal(*[f"sog_{b}" for b in rates.BUCKET_NAMES]).alias("shots"),
                        pl.sum_horizontal(*[f"blk_{b}" for b in rates.BUCKET_NAMES]).alias("blocks"))
    return per_game(logs[season]), per_game(logs[season - 10001])


def run_season(store: Store, season: int, logs: dict[int, pl.DataFrame] | None = None, dep: pl.DataFrame | None = None,
               dep_power: float | None = None, coef: dict | None = None,
               shrink: rates.Shrink = rates.Shrink()) -> pl.DataFrame:
    """Every method's probabilities and outcomes for ``season`` (long: one row per subject × method).

    Returns:
        ``stat`` (shots | blocks | saves), ``game_id``, ``subject_id`` (player or goalie),
        ``method``, the outcome ``y`` and ``p_{stat}_{k}`` / ``exp_{stat}`` columns.
    """
    logs = dict(logs or {})
    for s in (season, season - 10001, season - 20002):
        if s not in logs:
            logs[s] = rates.bucket_logs(store, s)
    history = store.read_parquet_required(keys.pregame_history(season))
    dep = dep if dep is not None else B.deployment(store, season)
    pit, mix = rates.season_rates(store, season, shrink, logs)
    feat = V.features(V.team_rates(V.team_logs(store, season), V.team_logs(store, season - 10001)), history)
    frames = []

    # Skaters: shots and blocks.
    cur, last = _skater_frames(logs, season)
    for stat, key in STAT_OF.items():
        mus = {"model": feat.select("game_id", "team_id", V.expected(feat, key, coef).alias("mu")),
               "no_team": feat.select("game_id", "team_id", pl.col(f"{key}_lg").alias("mu"))}
        power = V.DEP_POWER[key] if dep_power is None else dep_power
        sh = V.player_shares(dep, pit, V.stat_mix(logs[season - 10001], key), key, power)
        sh = sh.filter(pl.col("player_id").is_not_null()).unique(["game_id", "player_id"])
        outcome = cur.select("game_id", "player_id", pl.col(stat).alias("y"))
        keep = sh.join(outcome, on=["game_id", "player_id"], how="inner")
        for method, mu in mus.items():
            p = V.player_props(keep, mu, key, THRESHOLDS[stat], r=(coef or V.COEF)[key]["r"])
            p = p.rename({f"p_{key}_{k}": f"p_{stat}_{k}" for k in THRESHOLDS[stat]} | {f"exp_{key}": f"exp_{stat}"})
            frames.append(p.select("game_id", pl.col("player_id").alias("subject_id"), "y", pl.lit(method).alias("method"),
                                   *[c for c in p.columns if c.startswith(("p_", "exp_"))]).with_columns(pl.lit(stat).alias("stat")))
        base = _baselines(cur.select("game_id", "game_date", "player_id", stat), last.select("game_id", "game_date", "player_id", stat),
                          stat, "player_id", season, SEASON_PRIOR)
        frames.append(base.join(keep.select("game_id", "player_id", "y"), on=["game_id", "player_id"], how="inner")
                      .rename({"player_id": "subject_id"}).with_columns(pl.lit(stat).alias("stat")))

    # Goalies: saves by the starter.
    starts = {s: store.get_parquet(keys.goalie_starts(s)) for s in (season, season - 10001)}
    def starter_rows(gs: pl.DataFrame | None) -> pl.DataFrame:
        if gs is None:
            return pl.DataFrame(schema={"game_id": pl.Int64, "game_date": pl.Date, "team_id": pl.Int64, "goalie_id": pl.Int64,
                                        "saves": pl.Float64})
        return gs.select("game_id", "game_date", pl.col("team_id").cast(pl.Int64), pl.col("starter").cast(pl.Int64).alias("goalie_id"),
                         pl.col("saves").cast(pl.Float64))
    g_cur, g_last = starter_rows(starts[season]), starter_rows(starts[season - 10001])
    dists = project.team_goal_dists(history).select("game_id", pl.col("team_id").alias("opp_team_id"), pl.col("mean_goals").alias("goals_against"))
    opp_mu = feat.select("game_id", pl.col("team_id").alias("opp_team_id"), V.expected(feat, "sog", coef).alias("mu_against"),
                         pl.col("sog_lg").alias("lg_against"))
    g = (g_cur.join(feat.select("game_id", "team_id", "opp_team_id"), on=["game_id", "team_id"], how="inner")
         .join(opp_mu, on=["game_id", "opp_team_id"], how="inner").join(dists, on=["game_id", "opp_team_id"], how="inner"))
    league_goals = float(dists["goals_against"].mean())
    for method, frame in (("model", g), ("no_team", g.with_columns(pl.col("lg_against").alias("mu_against"),
                                                                  pl.lit(league_goals).alias("goals_against")))):
        p = V.saves_props(frame, THRESHOLDS["saves"], r=(coef or V.COEF)["sog"]["r"])
        frames.append(p.select("game_id", pl.col("goalie_id").alias("subject_id"), pl.col("saves").alias("y"),
                               pl.lit(method).alias("method"), *[c for c in p.columns if c.startswith(("p_saves", "exp_saves"))])
                      .with_columns(pl.lit("saves").alias("stat")))
    base = _baselines(g_cur.rename({"saves": "saves"}), g_last, "saves", "goalie_id", season, GOALIE_PRIOR_STARTS)
    frames.append(base.join(g.select("game_id", "goalie_id", pl.col("saves").alias("y")), on=["game_id", "goalie_id"], how="inner")
                  .rename({"goalie_id": "subject_id"}).with_columns(pl.lit("saves").alias("stat")))
    return pl.concat(frames, how="diagonal_relaxed").with_columns(pl.lit(season).alias("season"))


def scores(results: pl.DataFrame, by: tuple[str, ...] = ("method",)) -> pl.DataFrame:
    """Log loss and Brier per method (and ``by``) for every stat threshold."""
    rows = []
    for stat, ks in THRESHOLDS.items():
        part = results.filter(pl.col("stat") == stat)
        for k in ks:
            y = (pl.col("y") >= k).cast(pl.Float64)
            p = pl.col(f"p_{stat}_{k}").clip(B.EPS, 1 - B.EPS)
            rows.append(part.group_by(*by).agg(
                pl.lit(f"{stat}>={k}").alias("target"), pl.len().alias("n"), y.mean().alias("rate"), p.mean().alias("mean_p"),
                (-(y * p.log() + (1 - y) * (1 - p).log())).mean().alias("logloss"), ((p - y) ** 2).mean().alias("brier")))
    return pl.concat(rows).sort("target", *by)


def calibration(results: pl.DataFrame, stat: str, method: str = "model", bins: int = 10) -> pl.DataFrame:
    """Mean expected vs realized count by expected-count decile."""
    r = results.filter((pl.col("stat") == stat) & (pl.col("method") == method))
    return r.with_columns(pl.col(f"exp_{stat}").qcut(bins, labels=[str(i) for i in range(bins)], allow_duplicates=True).alias("bin")) \
        .group_by("bin").agg(pl.len().alias("n"), pl.col(f"exp_{stat}").mean().alias("expected"), pl.col("y").mean().alias("realized")).sort("bin")


def run(store: Store, seasons: list[int], write: bool = True) -> pl.DataFrame:
    """Backtest every season (stored under :func:`keys.props_volume_backtest` when ``write``)."""
    logs: dict[int, pl.DataFrame] = {}
    frames = []
    for season in seasons:
        for s in (season, season - 10001, season - 20002):
            if s not in logs:
                logs[s] = rates.bucket_logs(store, s)
        r = run_season(store, season, logs)
        if write:
            store.put_parquet(keys.props_volume_backtest(season), r)
        sc = scores(r).filter(pl.col("target").is_in(["shots>=3", "blocks>=2", "saves>=25"]))
        logger.info("%s: %s", season, "; ".join(f"{t} {m} {v:.4f}" for t, m, v in sc.select("target", "method", "logloss").iter_rows()))
        frames.append(r)
    return pl.concat(frames, how="diagonal_relaxed")


def write_report(results: pl.DataFrame, path: Path) -> str:
    """Markdown report: gate, pooled and per-season scores, calibration by decile."""
    per = scores(results, by=("season", "method"))
    gate = per.with_columns(pl.col("logloss").filter(pl.col("method") == "model").first().over("season", "target").alias("_m")) \
        .filter(pl.col("method") != "model").group_by("target").agg(
            (pl.col("_m") < pl.col("logloss")).all().alias("model_beats_all_every_season"), pl.col("season").n_unique().alias("seasons"))
    wide = per.pivot(on="method", index=["target", "season"], values="logloss").sort("target", "season")
    cal = "\n\n".join(f"**{s}**\n\n" + B._md(calibration(results, s).drop("bin")) for s in THRESHOLDS)
    text = "\n\n".join([
        "# Player props: shots on goal, blocks, saves backtest (M9 phase C)",
        f"Seasons {results['season'].min()}-{results['season'].max()}. Skaters who dressed and were in the morning projection; "
        "goalies on the games they started. Methods: `src/nhl/props/backtest_volume.py`. Lower log loss is better. "
        "The team model's coefficients were fitted on 2016-17 to 2018-19.",
        "## Gate: model beats every baseline in every season", B._md(gate.sort("target")),
        "## Pooled", B._md(scores(results).select("target", "method", "n", "rate", "mean_p", "logloss", "brier")),
        "## Log loss by season", B._md(wide),
        "## Calibration (model, expected vs realized count by decile)", cal,
    ]) + "\n"
    path.write_text(text)
    return text


__all__ = ["THRESHOLDS", "calibration", "run", "run_season", "scores", "write_report"]
