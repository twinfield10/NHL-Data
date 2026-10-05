"""The M3 bar: do point-in-time ratings predict the rest of a season?

For each season and cutoff date *D*: fit EV ratings on stints before *D* (from the stored
prior), then predict every team's 5v5 xG for and against per 60 over the rest of the
season **on the stints actually played** (actual lineups and deployment, so only the
ratings are being tested). Competitors:

* ``ratings``: the M3 model;
* ``team_to_date``: each team's 5v5 xGF/60 and xGA/60 before *D* (baseline a);
* ``last_season``: player terms frozen at the aged prior, intercept and context refit
  (baseline b);
* ``league``: every team average (floor).

Reported: RMSE and correlation with the actual rest-of-season xGF/60, xGA/60 and
xG differential per 60, and the correlation with the actual goal differential per 60.
"""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import polars as pl

from nhl import config
from nhl.ratings import rapm
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

CUTOFFS = ((11, 15), (1, 1), (2, 15))


def _cutoff_dates(start_year: int, first_game: date, last_game: date) -> list[date]:
    out = []
    for month, day in CUTOFFS:
        d = date(start_year if month >= 9 else start_year + 1, month, day)
        if first_game + (last_game - first_game) * 0.15 < d < last_game - (last_game - first_game) * 0.25:
            out.append(d)
    return out


def _team_rates(rows: pl.DataFrame, pred: np.ndarray | None) -> pl.DataFrame:
    """Per team: hours, xGF/60, xGA/60 (actual or predicted) and GF/GA per 60."""
    r = rows.with_columns(
        (pl.Series("p", pred) * pl.col("duration_s") / 3600 if pred is not None else pl.col("xgf")).alias("val")
    )
    f = r.group_by(pl.col("att_team").alias("team")).agg(
        (pl.col("duration_s").sum() / 3600).alias("hours"), pl.col("val").sum().alias("xgf"), pl.col("gf").sum().alias("gf")
    )
    a = r.group_by(pl.col("def_team").alias("team")).agg(pl.col("val").sum().alias("xga"), pl.col("gf").sum().alias("ga"))
    return f.join(a, on="team").with_columns(
        (pl.col("xgf") / pl.col("hours")).alias("xgf60"),
        (pl.col("xga") / pl.col("hours")).alias("xga60"),
        ((pl.col("gf") - pl.col("ga")) / pl.col("hours")).alias("gd60"),
    )


def season_cutoffs(store: Store, start_year: int) -> pl.DataFrame:
    """Predicted vs actual rest-of-season team rates for every cutoff in one season."""
    season = config.season_id(start_year)
    design = rapm.season_design(store, season, "EV")
    prior = rapm.age_prior(
        store.get_parquet(rapm.prior_key("EV", season)), rapm._ages(store, start_year), rapm.load_curve(store, "EV")
    )
    frozen = None if prior is None else prior.with_columns(pl.lit(1e12).alias("precision"))
    frozen_hyper = rapm.Hyper(newcomer_s=1e12)
    dates = design.rows["game_date"]
    out = []
    for cut in _cutoff_dates(start_year, dates.min(), dates.max()):
        early = (dates < cut).to_numpy()
        late_rows = design.rows.filter(~pl.Series(early))
        normal = rapm.normal_equations(design, early)
        actual = _team_rates(late_rows, None).select("team", "hours", "xgf60", "xga60", "gd60")
        preds = {
            "ratings": rapm.predict(rapm.fit(normal, prior), design, ~early),
            "last_season": rapm.predict(rapm.fit(normal, frozen, frozen_hyper), design, ~early),
        }
        frames = [
            _team_rates(late_rows, p).select("team", pl.lit(name).alias("model"), "xgf60", "xga60")
            for name, p in preds.items()
        ]
        to_date = _team_rates(design.rows.filter(pl.Series(early)), None)
        frames.append(to_date.select("team", pl.lit("team_to_date").alias("model"), "xgf60", "xga60"))
        league = float(to_date["xgf"].sum() / to_date["hours"].sum())
        frames.append(to_date.select("team", pl.lit("league").alias("model"), pl.lit(league).alias("xgf60"), pl.lit(league).alias("xga60")))
        out.append(
            pl.concat(frames)
            .join(actual, on="team", suffix="_actual")
            .with_columns(pl.lit(season).alias("season"), pl.lit(cut).alias("cutoff"))
        )
        logger.info("%s cutoff %s scored", season, cut)
    return pl.concat(out) if out else pl.DataFrame()


def summarize(results: pl.DataFrame) -> pl.DataFrame:
    """RMSE / correlation per model across all team-season-cutoffs (weighted equally)."""
    r = results.with_columns(
        (pl.col("xgf60") - pl.col("xga60")).alias("diff"),
        (pl.col("xgf60_actual") - pl.col("xga60_actual")).alias("diff_actual"),
    )
    return (
        r.group_by("model")
        .agg(
            pl.len().alias("n"),
            ((pl.col("xgf60") - pl.col("xgf60_actual")) ** 2).mean().sqrt().alias("rmse_xgf60"),
            ((pl.col("xga60") - pl.col("xga60_actual")) ** 2).mean().sqrt().alias("rmse_xga60"),
            ((pl.col("diff") - pl.col("diff_actual")) ** 2).mean().sqrt().alias("rmse_diff"),
            pl.corr("diff", "diff_actual").alias("corr_diff"),
            pl.corr("diff", "gd60").alias("corr_goal_diff"),
        )
        .sort("rmse_diff")
    )


def evaluate(store: Store, start_years: list[int]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run every season; returns (per team-cutoff results, summary)."""
    results = pl.concat([r for y in start_years if not (r := season_cutoffs(store, y)).is_empty()])
    return results, summarize(results)


def write_report(results: pl.DataFrame, summary: pl.DataFrame, path: "Path") -> str:
    """Markdown report of the bar (all cutoffs, and by cutoff month)."""
    from pathlib import Path

    def table(df: pl.DataFrame) -> list[str]:
        cols = ["model", "n", "rmse_xgf60", "rmse_xga60", "rmse_diff", "corr_diff", "corr_goal_diff"]
        out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
        for r in df.select(cols).iter_rows():
            out.append("| " + " | ".join(f"{v:.3f}" if isinstance(v, float) else str(v) for v in r) + " |")
        return out

    seasons = sorted(results["season"].unique().to_list())
    lines = [
        "# M3 evaluation: rest-of-season prediction",
        "",
        f"Generated {date.today().isoformat()} by `nhl evaluate-ratings`; seasons {seasons[0]}-{seasons[-1]}, "
        "cutoffs Nov 15, Jan 1 and Feb 15 (when inside the season). Each team's 5v5 xG for/against per 60 "
        "after the cutoff, predicted on the stints actually played. See `nhl.ratings.evaluate`.",
        "",
        "## All cutoffs",
        "",
        *table(summary),
    ]
    for month, label in ((11, "Nov 15"), (1, "Jan 1"), (2, "Feb 15")):
        part = results.filter(pl.col("cutoff").dt.month() == month)
        if part.height:
            lines += ["", f"## {label}", "", *table(summarize(part))]
    lines += [
        "",
        "**Bar** ([M3 plan](../plans/m3-ratings.md)): `ratings` beats `team_to_date` and `last_season` on "
        "rest-of-season xG differential.",
        "",
    ]
    text = "\n".join(lines)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(text)
    return text
