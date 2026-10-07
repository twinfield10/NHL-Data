"""How well projected deployment matches actual ice time (M5 diagnostic).

The simulator weights player ratings by each skater's share of his team's 5v5, PP and PK
skater time. This measures, per player-game, the projected share (as of the morning, no
DailyFaceoff history) against the actual one, in **minutes**: share × the team's actual
skater time in that state, so errors in how long a game spends on the power play don't
count here (the simulator generates those).

* **End to end:** the full pregame projection (:func:`nhl.pregame.lineups.project`),
  including roster misses (a projected player who didn't dress counts with actual 0, and a
  dressed player who wasn't projected with projection 0).
* **Share predictors, on players who dressed:** last game, mean of the last 10, the
  recency-weighted average in use (half-life :data:`lineups.HALF_LIFE`), season to date.
* **By role in the player's previous game** (M2 inferred lines: F1-F4, D1-D3, PP1/PP2/none,
  PK1/PK2/none), what is known that morning, with **bias** (projected − actual). Grouping by
  the role a player had *that night* is biased: whoever ended up on the top line partly got
  there by playing more, so top roles look under-projected when they aren't.
* **Team composite:** the share-weighted sum of EV offence ratings, projected vs actual. This
  is what moves a price, and is compared with the spread of composites across teams.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl

from nhl.pregame import lineups
from nhl.sim import inputs
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

STATE_COLS = {"s5": "5v5", "spp": "PP", "spk": "SH"}


def actual_minutes(store: Store, seasons: list[int]) -> pl.DataFrame:
    """Per skater-game actual minutes by state and the team's skater-minutes in each state."""
    frames = []
    for s in seasons:
        logs = store.read_parquet_required(keys.player_game_logs(s)).filter(pl.col("position") != "G")
        wide = (
            logs.filter(pl.col("strength").is_in(list(STATE_COLS.values())))
            .pivot(on="strength", index=["game_id", "team_id", "player_id"], values="toi_s", aggregate_function="sum")
        )
        frames.append(wide)
    wide = pl.concat(frames, how="diagonal_relaxed").with_columns(
        *[(pl.col(v).fill_null(0) / 60).alias(f"min_{k}") for k, v in STATE_COLS.items()]
    ).select("game_id", pl.col("team_id").cast(pl.Int64), "player_id", *[f"min_{k}" for k in STATE_COLS])
    return wide.with_columns(*[pl.col(f"min_{k}").sum().over("game_id", "team_id").alias(f"team_{k}") for k in STATE_COLS])


def roles(store: Store, seasons: list[int]) -> pl.DataFrame:
    """``game_id, team_id, player_id, role_ev, role_pp, role_pk`` from M2 inferred lines."""
    lu = pl.concat([store.read_parquet_required(keys.lineups(s)) for s in seasons]).explode("members").select(
        "game_id", pl.col("team_id").cast(pl.Int64), pl.col("members").alias("player_id"), "unit_type", "unit_rank"
    )
    ev = lu.filter(pl.col("unit_type").is_in(["F", "D"])).group_by("game_id", "team_id", "player_id").agg(
        (pl.col("unit_type") + pl.col("unit_rank").cast(pl.String)).sort().first().alias("role_ev"))
    pp = lu.filter(pl.col("unit_type") == "PP").group_by("game_id", "team_id", "player_id").agg(
        ("PP" + pl.col("unit_rank").min().cast(pl.String)).alias("role_pp"))
    pk = lu.filter(pl.col("unit_type") == "PK").group_by("game_id", "team_id", "player_id").agg(
        ("PK" + pl.col("unit_rank").min().cast(pl.String)).alias("role_pk"))
    return ev.join(pp, on=["game_id", "team_id", "player_id"], how="full", coalesce=True).join(
        pk, on=["game_id", "team_id", "player_id"], how="full", coalesce=True
    )


def end_to_end(store: Store, season: int, hist: pl.DataFrame, events: pl.DataFrame, mins: pl.DataFrame) -> pl.DataFrame:
    """Projected vs actual minutes per player-game (union of projected and dressed)."""
    this = hist.filter(pl.col("game_id") // 1_000_000 == season // 10000)
    targets = this.select("game_id", "team_id", "game_date").unique()
    proj = lineups.project(store, targets, [season - 10001, season], hist=hist, events=events, dfo=pl.DataFrame())
    proj = proj.filter(pl.col("player_id").is_not_null()).select(
        "game_id", "team_id", "player_id", *[(pl.col(k) / lineups.SCALE[k]).alias(f"proj_{k}") for k in STATE_COLS]
    )
    teams = mins.select("game_id", "team_id", *[f"team_{k}" for k in STATE_COLS]).unique()
    j = (
        proj.join(mins.drop([f"team_{k}" for k in STATE_COLS]), on=["game_id", "team_id", "player_id"], how="full", coalesce=True)
        .filter(pl.col("game_id").is_in(targets["game_id"].implode()))
        .join(teams, on=["game_id", "team_id"], how="inner")
    )
    return j.with_columns(
        *[pl.col(f"proj_{k}").fill_null(0.0) * pl.col(f"team_{k}") for k in STATE_COLS],
        *[pl.col(f"min_{k}").fill_null(0.0) for k in STATE_COLS],
    ).rename({f"proj_{k}": f"pmin_{k}" for k in STATE_COLS})


def predictors(hist: pl.DataFrame, season: int) -> pl.DataFrame:
    """For dressed players: actual share and four predictors from his earlier games."""
    h = hist.sort("player_id", "game_date", "game_id").with_columns(pl.int_range(pl.len()).over("player_id").alias("_n"))
    out = h
    for k in STATE_COLS:
        prev = pl.col(k).shift(1).over("player_id")
        out = out.with_columns(
            prev.alias(f"{k}_last"),
            pl.col(k).shift(1).rolling_mean(window_size=10, min_samples=1).over("player_id").alias(f"{k}_last10"),
            pl.col(k).ewm_mean(half_life=lineups.HALF_LIFE_SHOT if k == "sshot" else lineups.HALF_LIFE, ignore_nulls=True)
            .shift(1).over("player_id").alias(f"{k}_ewma"),
            # Season to date: same team-season of the game.
            (pl.col(k).fill_null(0).cum_sum() - pl.col(k).fill_null(0)).over("player_id", (pl.col("game_id") // 1_000_000)).alias(f"{k}_sum"),
        ).with_columns(
            (pl.col(f"{k}_sum") / pl.int_range(pl.len()).over("player_id", (pl.col("game_id") // 1_000_000)).replace(0, None)).alias(f"{k}_season")
        ).drop(f"{k}_sum")
    return out.filter(pl.col("game_id") // 1_000_000 == season // 10000)


def run(store: Store, seasons: list[int]) -> dict[str, pl.DataFrame]:
    """All tables for the report."""
    span = sorted({s - 10001 for s in seasons} | set(seasons))
    hist = lineups.deployment_history(store, span)
    rosters = pl.concat([r for s in span if (r := store.get_parquet(keys.rosters(s))) is not None])
    events = lineups.status_events(store, span, rosters)
    mins = actual_minutes(store, seasons)
    dates = store.read_parquet_required(keys.GAMES).select("game_id", "game_date")
    role = roles(store, span).join(dates, on="game_id").sort("game_date", "game_id").with_columns(
        *[pl.col(c).shift(1).over("player_id", "team_id") for c in ("role_ev", "role_pp", "role_pk")]
    ).drop("game_date")  # each player's role in his previous game for the team
    e2e = pl.concat([end_to_end(store, s, hist, events, mins) for s in seasons]).join(
        role, on=["game_id", "team_id", "player_id"], how="left"
    )
    pred = pl.concat([predictors(hist, s) for s in seasons]).join(
        mins.select("game_id", "team_id", "player_id", *[f"team_{k}" for k in STATE_COLS]), on=["game_id", "team_id", "player_id"]
    ).join(role, on=["game_id", "team_id", "player_id"], how="left")
    return {"e2e": e2e, "pred": pred, "composite": composite(store, e2e, hist)}


def composite(store: Store, e2e: pl.DataFrame, hist: pl.DataFrame) -> pl.DataFrame:
    """Team 5v5 offence composite (Σ share × EV offence rating), projected vs actual, with the
    latest rating snapshot before each game."""
    snaps = inputs.snapshot_dates(store)
    dates = hist.select("game_id", "game_date").unique()
    day_snap = pl.DataFrame({"game_date": dates["game_date"].unique().to_list()}).with_columns(
        pl.col("game_date").map_elements(lambda x: inputs._latest_before(snaps, x), return_dtype=pl.Date).alias("snap"))
    d = e2e.join(dates, on="game_id").join(day_snap, on="game_date").filter(pl.col("snap").is_not_null())
    parts = []
    for (snap,), part in d.partition_by("snap", as_dict=True).items():
        ev = store.read_parquet_required(f"ratings/{snap.isoformat()}/ev.parquet").filter(pl.col("side") == "O").select(
            "player_id", pl.col("mean").alias("o"))
        parts.append(part.join(ev, on="player_id", how="left").with_columns(pl.col("o").fill_null(0.0)))
    d = pl.concat(parts)
    return d.group_by("game_id", "team_id").agg(
        (pl.col("pmin_s5") * pl.col("o")).sum().alias("proj"), (pl.col("min_s5") * pl.col("o")).sum().alias("actual"),
        pl.col("min_s5").sum().alias("team_min"),
    ).with_columns(
        (pl.col("proj") / pl.col("team_min") * 5).alias("proj"), (pl.col("actual") / pl.col("team_min") * 5).alias("actual")
    )


def _mae(err: pl.Expr) -> pl.Expr:
    return err.abs().mean()


def summarize(tables: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    e2e, pred, comp = tables["e2e"], tables["pred"], tables["composite"]
    overall = pl.DataFrame([{
        "state": k, "player_games": e2e.filter((pl.col(f"min_{k}") > 0) | (pl.col(f"pmin_{k}") > 0)).height,
        "mean_actual_min": e2e.filter(pl.col(f"min_{k}") > 0)[f"min_{k}"].mean(),
        "mae_min": e2e.select(_mae(pl.col(f"pmin_{k}") - pl.col(f"min_{k}")))[0, 0],
        "mae_min_dressed_and_projected": e2e.filter((pl.col(f"min_{k}") > 0) & (pl.col(f"pmin_{k}") > 0))
        .select(_mae(pl.col(f"pmin_{k}") - pl.col(f"min_{k}")))[0, 0],
    } for k in STATE_COLS])
    by_role = {}
    for k, role in (("s5", "role_ev"), ("spp", "role_pp"), ("spk", "role_pk")):
        by_role[k] = e2e.filter(pl.col(f"pmin_{k}") > 0).with_columns(pl.col(role).fill_null("none")).group_by(role).agg(
            pl.len().alias("n"), pl.col(f"min_{k}").mean().alias("actual_min"), pl.col(f"pmin_{k}").mean().alias("proj_min"),
            (pl.col(f"pmin_{k}") - pl.col(f"min_{k}")).mean().alias("bias_min"),
            _mae(pl.col(f"pmin_{k}") - pl.col(f"min_{k}")).alias("mae_min"),
        ).sort(role)
    rows = []
    for k in STATE_COLS:
        d = pred.filter(pl.col(k).is_not_null())
        for p in ("last", "last10", "ewma", "season"):
            dd = d.filter(pl.col(f"{k}_{p}").is_not_null())
            rows.append({"state": k, "predictor": p, "n": dd.height,
                         "mae_min": dd.select(_mae((pl.col(f"{k}_{p}") - pl.col(k)) * pl.col(f"team_{k}")))[0, 0]})
    compare = pl.DataFrame(rows).pivot(on="predictor", index="state", values="mae_min")
    c = comp.with_columns((pl.col("proj") - pl.col("actual")).alias("err"))
    comp_summary = pl.DataFrame([{
        "team_games": c.height, "sd_actual_composite": c["actual"].std(), "sd_error": c["err"].std(),
        "mean_error": c["err"].mean(), "corr": float(np.corrcoef(c["proj"].to_numpy(), c["actual"].to_numpy())[0, 1]),
    }])
    return {"overall": overall, "role_5v5": by_role["s5"], "role_pp": by_role["spp"], "role_pk": by_role["spk"],
            "predictors": compare, "composite": comp_summary}


def write_report(summary: dict[str, pl.DataFrame], seasons: list[int], path: Path) -> str:
    def table(df: pl.DataFrame) -> list[str]:
        fmt = lambda v: f"{v:.3f}" if isinstance(v, float) else str(v)  # noqa: E731
        return ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns),
                *["| " + " | ".join(fmt(v) for v in r) + " |" for r in df.iter_rows()]]
    lines = [
        "# M5 deployment accuracy (projected vs actual ice time)",
        "",
        f"Seasons {', '.join(map(str, seasons))}. Projections as of the morning, last game + transactions "
        "(no DailyFaceoff history). Minutes = share × the team's actual skater time in the state.",
        "", "## End to end, per player-game (minutes)", "", *table(summary["overall"]),
        "", "## 5v5 by role in the previous game (projected players; `none` = no role last game, "
        "mostly projected players who didn't dress)", "", *table(summary["role_5v5"]),
        "", "## Power play by previous unit", "", *table(summary["role_pp"]),
        "", "## Penalty kill by previous unit", "", *table(summary["role_pk"]),
        "", "## Share predictors on dressed players (MAE, minutes)", "", *table(summary["predictors"]),
        "", "## Team 5v5 offence composite (what moves a price)", "",
        "Σ share × EV offence rating per team-game, ×5 (xG/60 units). `sd_error` vs `sd_actual_composite` "
        "says how much deployment error blurs team strength.", "", *table(summary["composite"]), "",
    ]
    text = "\n".join(lines)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return text
