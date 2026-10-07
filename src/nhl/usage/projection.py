"""Does a skater's context improve the forecast of his future on-ice results? (usage plan phase D)

At cut dates within each regular season (after :data:`CUT_SHARES` of its games, so the
short 2012-13 and 2020-21 schedules are cut at the same stage), forecast each skater's **rest-of-season** 5v5
on-ice xGF/60 and xGA/60 using only what was known at the cut, then score against what
happened. Methods (all xG per 60):

* ``raw``: his on-ice rate to date (what public sites show).
* ``raw_shrunk``: the raw rate shrunk to the league mean with :data:`SHRINK_S` seconds.
* ``own``: league intercept + his own rating at the cut (no context).
* ``todate``: ``own`` + his season-to-date teammates, competition, zone and context parts
  (:mod:`nhl.usage.onice`, each game at its own snapshot).
* ``recent``: ``own`` + his teammates over his team's last :data:`RECENT_GAMES` games,
  re-rated at the cut (Σ shared 5v5 time ÷ his time × teammate rating), + to-date
  competition, zone and context.
* ``blend``: (1 − :data:`BLEND_RAW`) × ``todate`` + :data:`BLEND_RAW` × ``raw_shrunk``. The
  ratings are shrunk hard (M3 priors), so the raw rate still adds information; the weight
  was fitted leaving one season out (0.30-0.40 in every fold, 2026-10-07).

**Points** (5v5, goals + assists per 60) are scored the same way: ``raw`` / ``raw_shrunk``
points per 60 against ``ipp`` = his shrunk share of on-ice goals × the ``recent`` xGF/60 ×
the league's goals per xG to date. ``ipp`` uses the ``blend`` xGF/60.

Scored by future-TOI-weighted MSE over skaters with ≥ :data:`MIN_PAST_S` before and
≥ :data:`MIN_FUTURE_S` after the cut. Skill = 1 − MSE ÷ MSE(``raw``).
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from nhl.sim.inputs import _latest_before, snapshot_dates
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

CUT_SHARES = (0.25, 0.40, 0.55)
RECENT_GAMES = 10
MIN_PAST_S = 200 * 60
MIN_FUTURE_S = 200 * 60
#: Tuned 2026-10-07 on 2015-26 (best 200-300 min for xGF, xGA and points; within 1%).
SHRINK_S = 300 * 60
BLEND_RAW = 0.33
#: Prior weight (on-ice goals) for the share of on-ice goals a skater gets a point on.
IPP_PRIOR_GOALS = 20.0
METHODS = ("raw", "raw_shrunk", "own", "todate", "recent", "blend")
_KEYS = ["player_id", "team_id"]


def cut_dates(games: pl.DataFrame, season: int) -> list[date]:
    """Dates by which :data:`CUT_SHARES` of ``season``'s regular-season games had been played
    (``games``: ``season, game_date`` of regular-season games)."""
    days = games.filter(pl.col("season") == season).sort("game_date")["game_date"]
    return sorted({days[min(int(q * len(days)), len(days) - 1)] for q in CUT_SHARES}) if len(days) else []


def shared_toi(stints: pl.DataFrame) -> pl.DataFrame:
    """5v5 seconds each ordered teammate pair shared per game: ``game_id, team_id, player_id, mate_id, shared_s``."""
    ev = stints.filter(
        pl.col("valid_personnel") & (pl.col("strength_state") == "5v5") & (pl.col("period") <= 3)
        & pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()
    )
    sides = []
    for side in ("home", "away"):
        part = ev.select("game_id", pl.col(f"{side}_team_id").alias("team_id"), pl.col(f"{side}_skaters").alias("player_id"),
                         pl.col(f"{side}_skaters").alias("mate_id"), "duration_s")
        sides.append(part.explode("player_id", empty_as_null=True).explode("mate_id", empty_as_null=True)
                     .filter(pl.col("player_id") != pl.col("mate_id"))
                     .group_by("game_id", "team_id", "player_id", "mate_id").agg(pl.col("duration_s").sum().alias("shared_s")))
    return pl.concat(sides)


def _wmean(cols: list[str]) -> list[pl.Expr]:
    w = pl.col("toi_s")
    return [((pl.col(c) * w).sum() / w.sum()).alias(c) for c in cols]


def cut_frame(store: Store, season: int, cut: date, onice: pl.DataFrame, pairs: pl.DataFrame,
              logs: pl.DataFrame, days: list[date]) -> pl.DataFrame:
    """Forecasts and outcomes for every qualifying skater at one cut date (see module docstring)."""
    snap = _latest_before(days, cut)
    if snap is None:
        return pl.DataFrame()
    ev = store.read_parquet_required(f"ratings/{snap.isoformat()}/ev.parquet")
    ctx = store.read_parquet_required(f"ratings/{snap.isoformat()}/context_ev.parquet")
    intercept = float(ctx.filter(pl.col("term") == "intercept")["mean"][0])
    rating = ev.pivot(on="side", index="player_id", values="mean").rename({"O": "o", "D": "d"})

    past, future = onice.filter(pl.col("game_date") < cut), onice.filter(pl.col("game_date") >= cut)
    parts = [f"{p}_{s}" for p in ("actual", "mates", "comp", "zone", "ctx") for s in ("f", "a")]
    p = past.group_by(_KEYS).agg(pl.col("toi_s").sum().alias("past_s"), *_wmean(parts)).filter(pl.col("past_s") >= MIN_PAST_S)
    # Current team = team of his last game before the cut.
    last_team = past.sort("game_date").group_by("player_id").agg(pl.col("team_id").last())
    p = p.join(last_team, on=_KEYS, how="inner")
    f = (future.group_by(_KEYS).agg(pl.col("toi_s").sum().alias("future_s"), *_wmean(["actual_f", "actual_a"]))
         .rename({"actual_f": "future_f", "actual_a": "future_a"}).filter(pl.col("future_s") >= MIN_FUTURE_S))
    df = p.join(f, on=_KEYS, how="inner").join(rating, on="player_id", how="left").with_columns(
        pl.col("o").fill_null(0.0), pl.col("d").fill_null(0.0)
    )

    # Recent teammates, re-rated at the cut.
    team_games = (past.select("game_id", "team_id", "game_date").unique().sort("game_date", descending=True)
                  .with_columns(pl.int_range(pl.len()).over("team_id").alias("_k")).filter(pl.col("_k") < RECENT_GAMES))
    rp = pairs.join(team_games.select("game_id", "team_id"), on=["game_id", "team_id"], how="inner")
    own_recent = past.join(team_games.select("game_id", "team_id"), on=["game_id", "team_id"], how="inner").group_by(_KEYS).agg(
        pl.col("toi_s").sum().alias("recent_s"))
    mates = (rp.join(rating.rename({"player_id": "mate_id"}), on="mate_id", how="left")
             .with_columns(pl.col("o").fill_null(0.0), pl.col("d").fill_null(0.0))
             .group_by(_KEYS).agg((pl.col("shared_s") * pl.col("o")).sum().alias("_mo"), (pl.col("shared_s") * pl.col("d")).sum().alias("_md"))
             .join(own_recent, on=_KEYS, how="inner")
             .select(*_KEYS, (pl.col("_mo") / pl.col("recent_s")).alias("mates_recent_f"),
                     (pl.col("_md") / pl.col("recent_s")).alias("mates_recent_a")))
    df = df.join(mates, on=_KEYS, how="left").with_columns(
        pl.col("mates_recent_f").fill_null(pl.col("mates_f")), pl.col("mates_recent_a").fill_null(pl.col("mates_a")))

    w = pl.col("past_s")
    league_f = float(df.select((pl.col("actual_f") * w).sum() / w.sum()).item())
    league_a = float(df.select((pl.col("actual_a") * w).sum() / w.sum()).item())
    out = df.with_columns(pl.lit(season).alias("season"), pl.lit(cut).alias("cut"))
    for s, own, league in (("f", "o", league_f), ("a", "d", league_a)):
        out = out.with_columns(
            pl.col(f"actual_{s}").alias(f"raw_{s}"),
            ((pl.col(f"actual_{s}") * w + league * SHRINK_S) / (w + SHRINK_S)).alias(f"raw_shrunk_{s}"),
            (intercept + pl.col(own)).alias(f"own_{s}"),
            (intercept + pl.col(own) + pl.col(f"mates_{s}") + pl.col(f"comp_{s}") + pl.col(f"zone_{s}") + pl.col(f"ctx_{s}")).alias(f"todate_{s}"),
            (intercept + pl.col(own) + pl.col(f"mates_recent_{s}") + pl.col(f"comp_{s}") + pl.col(f"zone_{s}") + pl.col(f"ctx_{s}")).alias(f"recent_{s}"),
        ).with_columns(((1 - BLEND_RAW) * pl.col(f"todate_{s}") + BLEND_RAW * pl.col(f"raw_shrunk_{s}")).alias(f"blend_{s}"))
    return out.join(_points(logs, cut), on=_KEYS, how="left")


def _points(logs: pl.DataFrame, cut: date) -> pl.DataFrame:
    """5v5 points per 60 before / after ``cut`` and the inputs of the ``ipp`` forecast."""
    five = logs.filter(pl.col("strength") == "5v5").with_columns(
        (pl.col("goals") + pl.col("a1") + pl.col("a2")).alias("pts"))
    agg = lambda d, tag: d.group_by(_KEYS).agg(  # noqa: E731
        pl.col("toi_s").sum().alias(f"{tag}_toi"), pl.col("pts").sum().alias(f"{tag}_pts"),
        pl.col("gf").sum().alias(f"{tag}_gf"), pl.col("xgf").sum().alias(f"{tag}_xgf"))
    return agg(five.filter(pl.col("game_date") < cut), "p").join(agg(five.filter(pl.col("game_date") >= cut), "n"), on=_KEYS, how="inner")


def score(frames: pl.DataFrame) -> pl.DataFrame:
    """Future-TOI-weighted MSE per method, side and season, plus skill vs ``raw``."""
    pts = frames.drop_nulls(["p_toi", "n_toi"]).filter(pl.col("n_toi") > 0)
    league_ppg = pts.select(pl.col("p_pts").sum() / pl.col("p_toi").sum()).item()
    g_per_xg = pts.select(pl.col("p_gf").sum() / pl.col("p_xgf").sum()).item()
    league_ipp = pts.select(pl.col("p_pts").sum() / pl.col("p_gf").sum()).item()
    pts = pts.with_columns(
        (pl.col("n_pts") * 3600 / pl.col("n_toi")).alias("future_p"),
        (pl.col("p_pts") * 3600 / pl.col("p_toi")).alias("raw_p"),
        ((pl.col("p_pts") + league_ppg * SHRINK_S) * 3600 / (pl.col("p_toi") + SHRINK_S)).alias("raw_shrunk_p"),
        ((pl.col("p_pts") + league_ipp * IPP_PRIOR_GOALS) / (pl.col("p_gf") + IPP_PRIOR_GOALS) * pl.col("blend_f") * g_per_xg).alias("ipp_p"),
    )
    rows = []
    for (season,), g in frames.partition_by("season", as_dict=True).items():
        for side in ("f", "a"):
            w = g["future_s"].to_numpy()
            y = g[f"future_{side}"].to_numpy()
            for m in METHODS:
                e = g[f"{m}_{side}"].to_numpy() - y
                rows.append({"season": season, "target": f"xg{side}", "method": m, "mse": float((w * e * e).sum() / w.sum()), "n": g.height})
        gp = pts.filter(pl.col("season") == season)
        w, y = gp["n_toi"].to_numpy(), gp["future_p"].to_numpy()
        for m in ("raw", "raw_shrunk", "ipp"):
            e = gp[f"{m}_p"].to_numpy() - y
            rows.append({"season": season, "target": "pts", "method": m, "mse": float((w * e * e).sum() / w.sum()), "n": gp.height})
    out = pl.DataFrame(rows)
    return out.with_columns(
        (1 - pl.col("mse") / pl.col("mse").filter(pl.col("method") == "raw").first().over("season", "target")).alias("skill")
    ).sort("target", "season", "method")


def run(store: Store, seasons: list[int]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """All cut frames and their scores for ``seasons``."""
    days = snapshot_dates(store)
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("season_type") == "R").select("game_id", "season", "game_date")
    frames = []
    for season in seasons:
        onice = store.get_parquet(keys.onice_context(season))
        if onice is None:
            continue
        onice = onice.join(games.select("game_id", "game_date"), on="game_id", how="inner")
        pairs = shared_toi(store.read_parquet_required(keys.stints(season)))
        logs = store.read_parquet_required(keys.player_game_logs(season)).join(games.select("game_id"), on="game_id", how="inner")
        for cut in cut_dates(games, season):
            fr = cut_frame(store, season, cut, onice, pairs, logs, days)
            if fr.height:
                frames.append(fr)
        logger.info("projection %s done", season)
    frames = pl.concat(frames, how="diagonal_relaxed")
    return frames, score(frames)


def pooled(scores: pl.DataFrame) -> pl.DataFrame:
    """Per target and method: pooled MSE (weighted by players), skill vs ``raw`` and
    ``raw_shrunk``, and the number of seasons the method beats ``raw_shrunk``."""
    base = scores.filter(pl.col("method") == "raw_shrunk").select("season", "target", pl.col("mse").alias("_rs"))
    j = scores.join(base, on=["season", "target"])
    return j.group_by("target", "method").agg(
        ((pl.col("mse") * pl.col("n")).sum() / pl.col("n").sum()).alias("mse"),
        pl.col("skill").mean().alias("skill_vs_raw"),
        (1 - (pl.col("mse") * pl.col("n")).sum() / (pl.col("_rs") * pl.col("n")).sum()).alias("skill_vs_shrunk"),
        (pl.col("mse") < pl.col("_rs")).sum().alias("seasons_beat_shrunk"), pl.len().alias("seasons"),
    ).sort("target", "mse")


def write_report(scores: pl.DataFrame, path) -> str:
    """Numbers for ``docs/reports/usage-projection-numbers.md``."""
    from pathlib import Path

    from nhl.usage.matchups import _md

    text = "\n".join([
        "## Numbers (generated by `nhl usage-projection`)", "",
        "### Pooled, 2015-16 to 2025-26 (cuts after 25%, 40% and 55% of each season's games)", "",
        _md(pooled(scores), 4), "",
        "### By season (skill vs raw)", "",
        _md(scores.pivot(on="method", index=["target", "season"], values="skill").sort("target", "season"), 3), "",
    ])
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(text)
    return text
