"""Pregame backtest (M5 phase C): what lineup and starting-goalie uncertainty cost.

Four variants per season, all with the M4 engine and point-in-time ratings:

* ``actual``: the dressed lineup and starter as played (the M4 backtest);
* ``lineup``: projected lineups (:mod:`nhl.pregame.lineups`), actual starter;
* ``goalie``: actual lineups, the starter mixture (:mod:`nhl.pregame.goalies`);
* ``pregame``: projected lineups and the starter mixture, i.e. what we'd know that morning.

**Mixture.** Each team's two most likely starters (renormalised) give up to four starter
pairs; each pair is simulated and the market probabilities are averaged with weight
P(home starter) × P(away starter). Team residuals always compare against the actual-lineup
rates of earlier games, as they would live.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

from nhl.pregame import goalies, lineups
from nhl.sim import backtest as sim_backtest
from nhl.sim import constants as sim_constants
from nhl.sim import inputs
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

PRICE_COLS = (
    "p_home_win", "p_home_minus_1_5", "p_away_minus_1_5", "mean_home_goals", "mean_away_goals",
    "p_overtime", "p_shootout", "p_over_4.5", "p_over_5.5", "p_over_6.5", "p_over_7.5",
)
VARIANTS = ("actual", "lineup", "goalie", "pregame")


def starter_probs(store: Store, season: int) -> pl.DataFrame:
    """``game_id, team_id, player_id, p_start`` for every team-game, from the season's model."""
    cand = goalies.build_candidates(store, [season])
    return goalies.load(store, season).predict(cand).select("game_id", "team_id", "player_id", "p_start")


def mixture(probs: pl.DataFrame, games: pl.DataFrame, top: int = 2) -> list[tuple[pl.DataFrame, np.ndarray]]:
    """Starter pairs to simulate: ``[(starts, weight per game)]`` aligned with ``games``.

    ``starts`` is ``game_id, team_id, starter`` (null = an unknown goalie, rated average).
    """
    ranked = (
        probs.sort("p_start", descending=True)
        .with_columns(pl.int_range(pl.len()).over("game_id", "team_id").alias("k"))
        .filter(pl.col("k") < top)
        .with_columns((pl.col("p_start") / pl.col("p_start").sum().over("game_id", "team_id")).alias("p"))
    )
    out = []
    for kh in range(top):
        for ka in range(top):
            sides = []
            w = np.ones(games.height)
            for side, k in (("home", kh), ("away", ka)):
                pick = games.select("game_id", pl.col(f"{side}_team_id").alias("team_id")).join(
                    ranked.filter(pl.col("k") == k).select("game_id", "team_id", pl.col("player_id").alias("starter"), "p"),
                    on=["game_id", "team_id"], how="left",
                )
                w = w * pick["p"].fill_null(0.0).to_numpy()
                sides.append(pick.select("game_id", "team_id", "starter"))
            if w.sum() > 0:
                out.append((pl.concat(sides), w))
    return out


def _priced(store: Store, season: int, games: pl.DataFrame, dep: pl.DataFrame, starts: pl.DataFrame,
            c: dict, snapshots: list[date], history: pl.DataFrame, n_sims: int,
            team_res: dict | None = None) -> pl.DataFrame:
    inp = inputs.build_inputs(store, season, games, dep, starts, c, snapshots, history=history, team_res=team_res)
    return sim_backtest.run_season(store, season, n_sims=n_sims, prepared=(inp, c))


def _mixed(store: Store, season: int, games: pl.DataFrame, dep: pl.DataFrame, pairs: list[tuple[pl.DataFrame, np.ndarray]],
           c: dict, snapshots: list[date], history: pl.DataFrame, n_sims: int, team_res: dict) -> pl.DataFrame:
    base, acc = None, None
    weights = games.select("game_id")
    for i, (starts, w) in enumerate(pairs):
        r = _priced(store, season, games, dep, starts, c, snapshots, history, n_sims, team_res)
        wcol = r.select("game_id").join(weights.with_columns(pl.Series("w", w)), on="game_id", how="left", maintain_order="left")["w"].to_numpy()
        part = r.select(PRICE_COLS).to_numpy() * wcol[:, None]
        if base is None:
            base, acc = r, part
        else:
            acc = acc + part
    return base.with_columns(*[pl.Series(col, acc[:, j]) for j, col in enumerate(PRICE_COLS)])


def run_season(store: Store, season: int, n_sims: int = 1000, snapshots: list[date] | None = None) -> pl.DataFrame:
    """All four variants for one season, stacked with a ``variant`` column."""
    snapshots = snapshots if snapshots is not None else inputs.snapshot_dates(store)
    c = sim_constants.estimate(store, season)
    games = store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final"))
    dep_actual, starts_actual = inputs.actual_deployment(store, season)
    history = inputs.rate_table(store, season, games, dep_actual, starts_actual, c, snapshots)
    team_res = inputs.build_inputs(store, season, games, dep_actual, starts_actual, c, snapshots, history=history).team_res

    targets = pl.concat([
        games.select("game_id", pl.col(f"{s}_team_id").alias("team_id"), "game_date") for s in ("home", "away")
    ])
    span = [season - 10001, season]
    dep_proj = lineups.project(store, targets, span)
    pairs = mixture(starter_probs(store, season), games)

    out = {
        "actual": _priced(store, season, games, dep_actual, starts_actual, c, snapshots, history, n_sims, team_res),
        "lineup": _priced(store, season, games, dep_proj, starts_actual, c, snapshots, history, n_sims, team_res),
        "goalie": _mixed(store, season, games, dep_actual, pairs, c, snapshots, history, n_sims, team_res),
        "pregame": _mixed(store, season, games, dep_proj, pairs, c, snapshots, history, n_sims, team_res),
    }
    return pl.concat([r.with_columns(pl.lit(v).alias("variant")) for v, r in out.items()], how="diagonal_relaxed")


def run(store: Store, seasons: list[int], n_sims: int = 1000) -> pl.DataFrame:
    snapshots = inputs.snapshot_dates(store)
    frames = []
    for season in seasons:
        r = run_season(store, season, n_sims, snapshots)
        frames.append(r)
        logger.info("%s: %s", season, {
            v: round(sim_backtest.summarize(r.filter(pl.col("variant") == v))["logloss_sim"], 4) for v in VARIANTS
        })
    return pl.concat(frames)


def summary(results: pl.DataFrame) -> pl.DataFrame:
    """Per variant (and per season): games, log losses, Brier, calibration, totals."""
    rows = []
    for season in [None, *sorted(results["season"].unique().to_list())]:
        part = results if season is None else results.filter(pl.col("season") == season)
        for v in VARIANTS:
            m = sim_backtest.summarize(part.filter(pl.col("variant") == v))
            rows.append({"season": "all" if season is None else str(season), "variant": v, **m})
    return pl.DataFrame(rows)


def write_report(results: pl.DataFrame, accuracy: pl.DataFrame, path: Path) -> str:
    """Markdown report: the cost of lineup and starter uncertainty, and lineup accuracy."""
    s = summary(results)
    allv = s.filter(pl.col("season") == "all")
    lines = [
        "# M5 pregame backtest",
        "",
        "M4 engine with point-in-time ratings. `actual`: lineups and starters as played (the M4 bar). "
        "`lineup`: projected lineups (last game + transactions; ESPN injury history doesn't exist before "
        "2026-10-05, so this is the pessimistic case). `goalie`: the starter mixture. `pregame`: both.",
        "",
        "## All seasons",
        "",
        "| variant | games | moneyline LL | Poisson LL | Brier | max cal. err | O/U 5.5 LL | O/U 6.5 LL | puck line LL |",
        "|---|---|---|---|---|---|---|---|---|",
        *[f"| {r['variant']} | {int(r['games'])} | {r['logloss_sim']:.4f} | {r['logloss_poisson']:.4f} | {r['brier_sim']:.4f} | "
          f"{r['max_calibration_error']:.3f} | {r['logloss_over_5.5_sim']:.4f} | {r['logloss_over_6.5_sim']:.4f} | "
          f"{r['logloss_home_puckline_sim']:.4f} |" for r in allv.iter_rows(named=True)],
        "",
        "## Moneyline log loss by season",
        "",
        "| season | " + " | ".join(VARIANTS) + " | Poisson |",
        "|---|" + "---|" * (len(VARIANTS) + 1),
    ]
    for season in sorted(x for x in s["season"].unique().to_list() if x != "all"):
        part = s.filter(pl.col("season") == season)
        vals = {r["variant"]: r["logloss_sim"] for r in part.iter_rows(named=True)}
        lines.append(f"| {season} | " + " | ".join(f"{vals[v]:.4f}" for v in VARIANTS) + f" | {part['logloss_poisson'][0]:.4f} |")
    lines += [
        "",
        "## Lineup projection accuracy",
        "",
        "Share of actually dressed skaters that were projected (`weighted`: weighted by their 5v5 share).",
        "",
        "| season | method | found | weighted | all 18 right |",
        "|---|---|---|---|---|",
        *[f"| {r['season']} | {r['method']} | {r['found']:.3f} | {r['weighted']:.3f} | {r['perfect']:.3f} |" for r in accuracy.iter_rows(named=True)],
        "",
    ]
    text = "\n".join(lines)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return text


def lineup_accuracy(store: Store, seasons: list[int]) -> pl.DataFrame:
    """Per season: projection accuracy with status events vs the last game alone."""
    rows = []
    for season in seasons:
        span = [season - 10001, season]
        hist = lineups.deployment_history(store, span)
        rosters = pl.concat([r for s in span if (r := store.get_parquet(keys.rosters(s))) is not None])
        ev = lineups.status_events(store, span, rosters)
        this = hist.filter(pl.col("game_id") // 1_000_000 == season // 10000)
        targets = this.select("game_id", "team_id", "game_date").unique()
        for method, events in (("last game + events", ev), ("last game only", ev.head(0))):
            a = lineups.accuracy(lineups.project(store, targets, span, hist=hist, events=events), this)
            rows.append({
                "season": season, "method": method, "found": a["found"].sum() / a["dressed"].sum(),
                "weighted": a["weighted"].mean(), "perfect": (a["found"] == a["dressed"]).mean(),
            })
    return pl.DataFrame(rows)
