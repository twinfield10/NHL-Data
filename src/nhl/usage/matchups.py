"""Line matchups (usage plan phase C): how much coaches match, who gets the hard minutes, and
whether matchups interact beyond the additive ratings.

* **Matching matrix** (:func:`matching_matrix`): seconds of each own tier against each opponent
  tier at 5v5, from the EV design rows (each stint once per team). For a skater of tier T, each
  opponent in the group splits the stint's time equally, so a row's opponent mix is a
  distribution. ``ratio`` = P(opp tier | own tier) ÷ P(opp tier): 1.0 is no matching, > 1 means
  that tier is seen more than its ice time alone explains. Stored per team, coach and venue
  in ``processed/usage_matchups/{season}`` for the site.
* **Matching intensity** (:func:`intensity`): mutual information (bits) between own and
  opponent forward tier per team-season and venue; 0 = no matching at all.
* **Interaction test** (:func:`interaction_test`): regress each stint's point-in-time residual
  (actual − additive prediction, :func:`nhl.usage.onice.stint_predictions`) on tier main
  effects plus (a) attacker × defender forward-tier cells or (b) the product of the attackers'
  O sum and the defenders' D sum, leave one season out, and compare held-out weighted MSE.
  If neither beats main effects, matchups can't move game prices: the simulator's team totals
  are ice-time-weighted sums, which are the same whoever faces whom.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

F_TIERS = ("F1", "F2", "F3", "F4")
D_TIERS = ("D1", "D2", "D3")


def _tier_lists(rows: pl.DataFrame, usage: pl.DataFrame) -> pl.DataFrame:
    """Adds per row ``att_ft, def_ft`` (modal forward tier of each side; ties go to the higher
    tier) and keeps ``_i``. Rows where a side has no tiered forward get nulls."""
    tiers = usage.select("game_id", "player_id", "tier")
    out = rows
    for side, name in (("offence", "att_ft"), ("defence", "def_ft")):
        modal = (
            rows.select("_i", "game_id", pl.col(side).alias("player_id")).explode("player_id", empty_as_null=True)
            .join(tiers, on=["game_id", "player_id"], how="inner")
            .filter(pl.col("tier").str.starts_with("F"))
            .group_by("_i", "tier").len()
            .sort("_i", "len", "tier", descending=[False, True, False])
            .group_by("_i", maintain_order=True).first()
            .select("_i", pl.col("tier").alias(name))
        )
        out = out.join(modal, on="_i", how="left")
    return out


def exploded_pairs(rows: pl.DataFrame, usage: pl.DataFrame, coaches: pl.DataFrame) -> pl.DataFrame:
    """Own skater × opponent skater pairs per attack row, with the opponent's share of the row.

    Every attack row is one stint seen from the attacking team; the team's own skaters are
    ``offence`` and the opponents ``defence``. Using both rows of a stint covers each team once.

    Returns:
        ``season, game_id, team_id, coach_id, venue, own_tier, opp_tier, seconds`` where
        ``seconds`` = stint duration × (1 / opponents of that group on the ice).
    """
    tiers = usage.select("game_id", "player_id", "tier")
    coach = coaches.select("game_id", "team_id", "coach_id")
    base = rows.select("_i", "season", "game_id", pl.col("att_team").alias("team_id"),
                       pl.when(pl.col("att_home")).then(pl.lit("home")).otherwise(pl.lit("away")).alias("venue"),
                       "duration_s", "offence", "defence")
    own = (base.select("_i", "season", "game_id", "team_id", "venue", "duration_s", pl.col("offence").alias("player_id"))
           .explode("player_id", empty_as_null=True).join(tiers, on=["game_id", "player_id"], how="inner")
           .select("_i", "season", "game_id", "team_id", "venue", "duration_s", pl.col("tier").alias("own_tier")))
    opp = (base.select("_i", "game_id", pl.col("defence").alias("player_id")).explode("player_id", empty_as_null=True)
           .join(tiers, on=["game_id", "player_id"], how="inner")
           .with_columns(pl.col("tier").str.head(1).alias("opp_group"))
           .with_columns((1.0 / pl.len().over("_i", "opp_group")).alias("frac"))
           .select("_i", pl.col("tier").alias("opp_tier"), "frac"))
    return (own.join(opp, on="_i", how="inner")
            .with_columns((pl.col("duration_s") * pl.col("frac")).alias("seconds"))
            .join(coach, on=["game_id", "team_id"], how="left")
            .select("season", "game_id", "team_id", "coach_id", "venue", "own_tier", "opp_tier", "seconds"))


def matching_matrix(pairs: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """Seconds, shares and matching ratio per ``by`` × own tier × opponent tier.

    ``share`` = P(opp tier | own tier) within the opponent group (F or D); ``base`` = P(opp tier)
    over all own tiers of the same group (the no-matching expectation); ``ratio`` = share ÷ base.
    """
    p = pairs.with_columns(pl.col("own_tier").str.head(1).alias("own_group"), pl.col("opp_tier").str.head(1).alias("opp_group"))
    m = p.group_by(*by, "own_group", "own_tier", "opp_group", "opp_tier").agg(pl.col("seconds").sum())
    return m.with_columns(
        (pl.col("seconds") / pl.col("seconds").sum().over(*by, "own_tier", "opp_group")).alias("share"),
        (pl.col("seconds").sum().over(*by, "own_group", "opp_group", "opp_tier")
         / pl.col("seconds").sum().over(*by, "own_group", "opp_group")).alias("base"),
    ).with_columns((pl.col("share") / pl.col("base")).alias("ratio")).sort(*by, "own_tier", "opp_tier")


def intensity(matrix: pl.DataFrame, by: list[str], own_group: str = "F", opp_group: str = "F") -> pl.DataFrame:
    """Mutual information (bits) between own and opponent tier per ``by``, from :func:`matching_matrix`."""
    m = matrix.filter((pl.col("own_group") == own_group) & (pl.col("opp_group") == opp_group))
    pj = pl.col("seconds") / pl.col("seconds").sum().over(*by)
    po = pl.col("seconds").sum().over(*by, "own_tier") / pl.col("seconds").sum().over(*by)
    return m.with_columns((pj * (pj / (po * pl.col("base"))).log(2)).alias("_mi")).group_by(*by).agg(
        pl.col("_mi").sum().alias("mi_bits"), pl.col("seconds").sum().alias("seconds"),
        pl.col("ratio").filter((pl.col("own_tier") == "F1") & (pl.col("opp_tier") == "F1")).first().alias("f1_vs_f1"),
    )


# --- interaction test -----------------------------------------------------------------------


@dataclass
class Normal:
    """X'WX, X'Wy, y'Wy and Σw for one season and one feature set."""

    xtx: np.ndarray
    xty: np.ndarray
    yy: float
    w: float

    def __add__(self, other: "Normal") -> "Normal":
        return Normal(self.xtx + other.xtx, self.xty + other.xty, self.yy + other.yy, self.w + other.w)

    def solve(self, ridge: float = 1e-6) -> np.ndarray:
        return np.linalg.solve(self.xtx + ridge * np.eye(len(self.xty)), self.xty)

    def sse(self, b: np.ndarray) -> float:
        return float(self.yy - 2 * b @ self.xty + b @ self.xtx @ b)


#: Feature sets: ``main`` = intercept, attacker / defender forward-tier dummies (F1 is the
#: reference) and the additive sums; ``cells`` adds attacker × defender tier cells;
#: ``product`` adds (o_sum − mean) × (d_sum − mean).
FEATURE_SETS = ("main", "cells", "product")


def features(rows: pl.DataFrame, which: str, o_mean: float = 0.0, d_mean: float = 0.0) -> np.ndarray:
    """Feature matrix for attack rows with ``att_ft, def_ft, o_sum, d_sum``."""
    cols = [np.ones(rows.height)]
    for side in ("att_ft", "def_ft"):
        cols += [(rows[side] == t).to_numpy().astype(float) for t in F_TIERS[1:]]
    cols += [rows["o_sum"].to_numpy(), rows["d_sum"].to_numpy()]
    if which == "cells":
        cols += [((rows["att_ft"] == a) & (rows["def_ft"] == d)).to_numpy().astype(float)
                 for a in F_TIERS[1:] for d in F_TIERS[1:]]
    elif which == "product":
        cols.append((rows["o_sum"].to_numpy() - o_mean) * (rows["d_sum"].to_numpy() - d_mean))
    return np.column_stack(cols)


def season_normals(rows: pl.DataFrame, o_mean: float, d_mean: float) -> dict[str, Normal]:
    """Normal equations per feature set for one season's tiered attack rows."""
    w = rows["duration_s"].to_numpy().astype(float)
    y = rows["resid"].to_numpy()
    out = {}
    for which in FEATURE_SETS:
        x = features(rows, which, o_mean, d_mean)
        xw = x * w[:, None]
        out[which] = Normal(xw.T @ x, xw.T @ y, float(np.sum(w * y * y)), float(w.sum()))
    out["zero"] = Normal(np.zeros((1, 1)), np.zeros(1), float(np.sum(w * y * y)), float(w.sum()))
    return out


def interaction_test(normals: dict[int, dict[str, Normal]]) -> pl.DataFrame:
    """Leave-one-season-out held-out weighted MSE per feature set (and zero)."""
    out = []
    for season in sorted(normals):
        row = {"season": season, "weight_s": normals[season]["zero"].w}
        for which in (*FEATURE_SETS, "zero"):
            test = normals[season][which]
            if which == "zero":
                row["mse_zero"] = test.yy / test.w
                continue
            train = sum((normals[s][which] for s in normals if s != season), start=Normal(
                np.zeros_like(test.xtx), np.zeros_like(test.xty), 0.0, 0.0))
            row[f"mse_{which}"] = test.sse(train.solve()) / test.w
        out.append(row)
    return pl.DataFrame(out).with_columns(
        (1 - pl.col("mse_main") / pl.col("mse_zero")).alias("gain_main"),
        (1 - pl.col("mse_cells") / pl.col("mse_main")).alias("gain_cells"),
        (1 - pl.col("mse_product") / pl.col("mse_main")).alias("gain_product"),
    )


def cell_effects(normals: dict[int, dict[str, Normal]]) -> pl.DataFrame:
    """All-season ``cells`` fit: the interaction deviation (xG/60) for each attacker × defender
    forward-tier pair (F1 rows and columns are the reference, 0)."""
    total = sum((n["cells"] for n in normals.values()), start=Normal(
        np.zeros_like(next(iter(normals.values()))["cells"].xtx), np.zeros_like(next(iter(normals.values()))["cells"].xty), 0.0, 0.0))
    b = total.solve()
    k = 1 + 2 * (len(F_TIERS) - 1) + 2
    cells = b[k:]
    return pl.DataFrame([
        {"att_ft": a, "def_ft": d, "effect": float(cells[i * (len(F_TIERS) - 1) + j])}
        for i, a in enumerate(F_TIERS[1:]) for j, d in enumerate(F_TIERS[1:])
    ])


# --- who gets the hard minutes --------------------------------------------------------------


def hard_minutes(store: Store, seasons: list[int], min_toi_s: int = 200 * 60) -> pl.DataFrame:
    """Standardized OLS of competition faced (``qoc_net``) on a skater's own offence rating,
    own defence rating (sign-flipped: higher = better), DZ-start share and average tier, by
    position group, pooled over ``seasons`` (skater-team-seasons with ≥ ``min_toi_s``)."""
    frames = []
    for season in seasons:
        oc = store.get_parquet(keys.onice_context_summary(season))
        us = store.get_parquet(keys.usage_summary(season))
        if oc is None or us is None:
            continue
        frames.append(oc.join(us.select("season", "player_id", "team_id", "tier_avg", "oz_starts", "dz_starts"),
                              on=["season", "player_id", "team_id"], how="inner"))
    df = pl.concat(frames).filter(pl.col("toi_s") >= min_toi_s).with_columns(
        (-pl.col("own_a")).alias("def_rating"), pl.col("own_f").alias("off_rating"),
        (pl.col("dz_starts") / (pl.col("oz_starts") + pl.col("dz_starts"))).alias("dz_share"),
    ).drop_nulls(["qoc_net", "off_rating", "def_rating", "dz_share", "tier_avg"])
    out = []
    names = ["off_rating", "def_rating", "dz_share", "tier_avg"]
    for (group,), g in df.partition_by("group", as_dict=True).items():
        z = lambda c: (g[c].to_numpy() - g[c].mean()) / g[c].std()  # noqa: E731
        x = np.column_stack([np.ones(g.height)] + [z(c) for c in names])
        y = z("qoc_net")
        b, *_ = np.linalg.lstsq(x, y, rcond=None)
        r2 = 1 - np.sum((y - x @ b) ** 2) / np.sum((y - y.mean()) ** 2)
        out.append({"group": group, "n": g.height, "r2": float(r2), **{n: float(v) for n, v in zip(names, b[1:])},
                    **{f"corr_{n}": float(np.corrcoef(z(n), y)[0, 1]) for n in names}})
    return pl.DataFrame(out).sort("group")


# --- driver ----------------------------------------------------------------------------------


def run_season(store: Store, season: int) -> tuple[pl.DataFrame, dict[str, Normal] | None]:
    """Matching matrix (stored) and interaction normal equations for one season."""
    from nhl.usage.onice import stint_predictions

    usage = store.read_parquet_required(keys.usage(season))
    coaches = store.read_parquet_required(keys.coaches(season))
    preds = stint_predictions(store, season)
    if preds.is_empty():
        return pl.DataFrame(), None
    pairs = exploded_pairs(preds, usage, coaches)
    by = ["season", "team_id", "coach_id", "venue"]
    team = matching_matrix(pairs, by)
    store.put_parquet(keys.usage_matchups(season), team)
    league = matching_matrix(pairs, ["season", "venue"])
    rows = _tier_lists(preds, usage).drop_nulls(["att_ft", "def_ft"])
    o_mean, d_mean = (float(rows[c].mean()) for c in ("o_sum", "d_sum"))
    return league.with_columns(pl.lit(None, pl.Int32).alias("team_id")), season_normals(rows, o_mean, d_mean)


def run(store: Store, seasons: list[int]) -> dict[str, pl.DataFrame]:
    """Every season's matchup tables, then the pooled study (see :func:`write_report`)."""
    leagues, normals = [], {}
    for season in seasons:
        league, n = run_season(store, season)
        if n is None:
            logger.info("matchups %s: no snapshots", season)
            continue
        leagues.append(league)
        normals[season] = n
        logger.info("matchups %s done", season)
    league = pl.concat(leagues)
    pooled = (league.group_by("venue", "own_group", "own_tier", "opp_group", "opp_tier").agg(pl.col("seconds").sum())
              .pipe(lambda m: matching_matrix(m.rename({"seconds": "s"}).with_columns(pl.col("s").alias("seconds")).drop("s")
                                              .with_columns(pl.lit(0).alias("season")), ["season", "venue"])))
    teams = pl.concat([store.read_parquet_required(keys.usage_matchups(s)) for s in normals])
    by = ["season", "team_id", "coach_id"]
    team_all = matching_matrix(teams.group_by(*by, "own_tier", "opp_tier").agg(pl.col("seconds").sum()), by)
    home_away = matching_matrix(teams.group_by(*by, "venue", "own_tier", "opp_tier").agg(pl.col("seconds").sum()), [*by, "venue"])
    d_vs_f1 = (home_away.filter((pl.col("own_tier") == "D1") & (pl.col("opp_tier") == "F1"))
               .pivot(on="venue", index=by, values="ratio").rename({"home": "d1_f1_home", "away": "d1_f1_away"}))
    return {
        "league": pooled,
        "intensity": intensity(team_all, by).join(d_vs_f1, on=by, how="left"),
        "interaction": interaction_test(normals),
        "cells": cell_effects(normals),
        "hard_minutes": hard_minutes(store, list(normals)),
    }


def _md(df: pl.DataFrame, digits: int = 3) -> str:
    """A small frame as a Markdown table."""
    df = df.with_columns(pl.selectors.float().round(digits))
    head = "| " + " | ".join(df.columns) + " |"
    sep = "|" + "---|" * len(df.columns)
    body = ["| " + " | ".join("" if v is None else str(v) for v in row) + " |" for row in df.iter_rows()]
    return "\n".join([head, sep, *body])


def write_report(res: dict[str, pl.DataFrame], path) -> str:
    """Numbers section of ``docs/reports/usage-matchups.md`` (the narrative is written by hand)."""
    lg = res["league"]
    ff = lg.filter((pl.col("own_group") == "F") & (pl.col("opp_group") == "F") & (pl.col("venue") == "home"))
    df = lg.filter((pl.col("own_group") == "D") & (pl.col("opp_group") == "F"))
    it = res["interaction"]
    gains = it.select("gain_cells", "gain_product")
    n = it.height
    summary = pl.DataFrame({
        "test": ["cells", "product"],
        "mean_gain": [float(gains["gain_cells"].mean()), float(gains["gain_product"].mean())],
        "se": [float(gains["gain_cells"].std() / np.sqrt(n)), float(gains["gain_product"].std() / np.sqrt(n))],
        "seasons_positive": [int((gains["gain_cells"] > 0).sum()), int((gains["gain_product"] > 0).sum())],
    })
    parts = [
        "## Numbers (generated by `nhl usage-matchups`)", "",
        "### Forward tier vs forward tier (ratio to no matching; symmetric, pooled)", "",
        _md(ff.pivot(on="opp_tier", index="own_tier", values="ratio").sort("own_tier")), "",
        "### Defence pair vs forward tier, by venue (home has the last change)", "",
        _md(df.pivot(on="opp_tier", index=["venue", "own_tier"], values="ratio").sort("venue", "own_tier")), "",
        "### Matching intensity per team-season (F vs F mutual information, bits) and D1 vs F1 by venue", "",
        _md(res["intensity"].select(
            pl.col("mi_bits").mean().alias("mi_mean"), pl.col("mi_bits").quantile(0.1).alias("mi_p10"),
            pl.col("mi_bits").quantile(0.9).alias("mi_p90"), pl.col("d1_f1_home").mean(), pl.col("d1_f1_away").mean(),
            (pl.col("d1_f1_home") > pl.col("d1_f1_away")).mean().alias("share_home_higher"), pl.len().alias("team_seasons"),
        ), 4), "",
        "### Who gets the hard minutes (standardized OLS of QoC net)", "",
        _md(res["hard_minutes"]), "",
        "### Interaction test (leave one season out; gain = 1 − held-out MSE ÷ main-effects MSE)", "",
        _md(it.select("season", "gain_main", "gain_cells", "gain_product"), 6), "",
        _md(summary, 6), "",
        "### Cell effects (all seasons; xG/60 for the attacking team, F1 row/column = 0)", "",
        _md(res["cells"].pivot(on="def_ft", index="att_ft", values="effect").sort("att_ft"), 4), "",
    ]
    text = "\n".join(parts)
    from pathlib import Path

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(text)
    return text
