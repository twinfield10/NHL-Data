"""Arena scorer-bias adjustment for shot locations.

Off-ice scorers in each arena record where shots come from, and some arenas are
systematically off: before the NHL's tracking era, Madison Square Garden recorded
shots about 4 ft closer than other rinks, inflating xG there, while Tampa erred the
other way. See https://hockeyviz.com/txt/scorerBias. In our data the arena-to-arena
spread in mean shot distance fell from 1.5 ft (2010-20) to 0.6 ft (2021+).

Method: a team-controlled quantile mapping in the spirit of Schuckers & Curro (2013).

1. **Estimate from visitors only.** For arena *a* and season *s*, take visiting teams'
   unblocked shots recorded at *a* (``F``) and the *same visiting teams'* shots in
   their other road games (``G``). ``G`` is reweighted so each visiting team counts as
   much as it does at *a*, so team style can't masquerade as scorer bias. Pooled over
   a +/-1 season window that never crosses the 2021-22 tracking boundary.
2. **Map** every shot at *a* (home and visitor) through ``G^-1(F(x))`` for distance
   and angle separately, shrunk toward no change by ``n / (n + SHRINK_K)``.
3. **Rebuild** coordinates and every location-derived feature from the adjusted
   distance and angle. Raw values are kept with a ``_raw`` suffix.

Home-team shots are never used for estimation, so how much the adjustment also
aligns *their* distribution is an out-of-sample check (see :func:`validation_report`).
"""

from __future__ import annotations

import logging

import numpy as np
import polars as pl

from nhl.transform.events import GOAL_X

logger = logging.getLogger(__name__)

#: Seasons from here on use tracked shot coordinates; windows never cross this line.
TRACKING_ERA_START = 20212022
#: Quantile grid for the maps.
QUANTILES = np.linspace(0.01, 0.99, 50)
#: Shrinkage: a map estimated from n visitor shots moves shots by n / (n + K) of its shift.
SHRINK_K = 1500
#: Arena-seasons with fewer visitor shots than this get no adjustment.
MIN_SHOTS = 300
#: Window half-width in seasons.
WINDOW = 1

ADJUSTED = ["x_abs", "y_abs", "event_distance", "event_angle", "distance_from_last",
            "event_angle_change", "event_angle_change_speed", "puck_speed_since_last", "off_wing"]


def _era(season: int) -> int:
    return int(season >= TRACKING_ERA_START)


def _wquantiles(values: np.ndarray, weights: np.ndarray, qs: np.ndarray) -> np.ndarray:
    order = np.argsort(values)
    v, w = values[order], weights[order]
    cw = np.cumsum(w)
    cw = (cw - 0.5 * w) / cw[-1]
    return np.interp(qs, cw, v)


def estimate_maps(shots: pl.DataFrame, game_venues: pl.DataFrame) -> pl.DataFrame:
    """Estimate per arena-season quantile maps for distance and angle.

    Args:
        shots: Shot feature tables (``processed/shots``) for all seasons to adjust.
        game_venues: ``processed/game_venues.parquet`` (``arena_id`` per game).

    Returns:
        One row per (arena_id, season) with list columns ``dist_from``/``dist_to``,
        ``angle_from``/``angle_to`` (quantiles of the arena vs. the reference), the
        visitor sample size ``n`` and ``shrink`` weight.
    """
    base = (
        shots.filter(pl.col("strength_group") != "EN")
        .join(game_venues.select("game_id", "arena_id"), on="game_id", how="inner")
        .filter(pl.col("is_home") == 0)
        .select("season", "arena_id", pl.col("event_team_abbr").alias("team"), "event_distance", "event_angle")
        .drop_nulls()
        .filter(pl.col("event_distance").is_not_nan() & pl.col("event_angle").is_not_nan())
    )
    seasons = sorted(base["season"].unique().to_list())
    rows = []
    for season in seasons:
        window = [s for s in seasons if abs((s // 10000) - (season // 10000)) <= WINDOW and _era(s) == _era(season)]
        pool = base.filter(pl.col("season").is_in(window))
        team_counts = pool.group_by("team", "arena_id").len()
        team_totals = pool.group_by("team").len().rename({"len": "total"})
        for arena in pool.filter(pl.col("season") == season)["arena_id"].unique().to_list():
            here = pool.filter(pl.col("arena_id") == arena)
            n = here.height
            if n < MIN_SHOTS:
                continue
            # Reference: the same visitors' shots elsewhere, weighted to the team mix at this arena.
            mix = (
                team_counts.filter(pl.col("arena_id") == arena).select("team", pl.col("len").alias("n_here"))
                .join(team_totals, on="team")
                .join(team_counts.filter(pl.col("arena_id") != arena).group_by("team").agg(pl.col("len").sum().alias("n_else")), on="team")
                .with_columns((pl.col("n_here") / pl.col("n_else")).alias("w"))
            )
            ref = pool.filter(pl.col("arena_id") != arena).join(mix.select("team", "w"), on="team", how="inner")
            if ref.height < MIN_SHOTS:
                continue
            w_ref = ref["w"].to_numpy()
            ones = np.ones(n)
            rows.append({
                "arena_id": arena,
                "season": season,
                "n": n,
                "shrink": n / (n + SHRINK_K),
                "dist_from": _wquantiles(here["event_distance"].to_numpy(), ones, QUANTILES).tolist(),
                "dist_to": _wquantiles(ref["event_distance"].to_numpy(), w_ref, QUANTILES).tolist(),
                "angle_from": _wquantiles(here["event_angle"].to_numpy(), ones, QUANTILES).tolist(),
                "angle_to": _wquantiles(ref["event_angle"].to_numpy(), w_ref, QUANTILES).tolist(),
            })
    maps = pl.DataFrame(rows)
    logger.info("estimated %d arena-season maps across %d seasons", maps.height, len(seasons))
    return maps


def _map_values(values: np.ndarray, src: np.ndarray, dst: np.ndarray, shrink: float) -> np.ndarray:
    """Quantile-map values from ``src`` quantiles onto ``dst``; linear shift beyond the grid."""
    mapped = np.interp(values, src, dst)
    lo, hi = values < src[0], values > src[-1]
    mapped[lo] = values[lo] + (dst[0] - src[0])
    mapped[hi] = values[hi] + (dst[-1] - src[-1])
    return values + shrink * (mapped - values)


def apply_maps(
    shots: pl.DataFrame, game_venues: pl.DataFrame, maps: pl.DataFrame, include_tracking_era: bool = False
) -> pl.DataFrame:
    """Adjust shot locations with the arena maps and rebuild location-derived features.

    Only scorer-era seasons (before :data:`TRACKING_ERA_START`) are adjusted by default:
    on held-out home-team shots the adjustment cut the arena spread from 1.89 to 1.15 ft
    in 2010-21 but did nothing in the tracking era (1.82 -> 1.84 ft), where coordinates
    are no longer entered by scorers. Shots at arena-seasons without a map are unchanged. Original values are kept
    as ``<col>_raw`` and ``rink_shift_ft`` records the distance change.

    Args:
        shots: Shot feature rows.
        game_venues: ``processed/game_venues.parquet``.
        maps: Output of :func:`estimate_maps`.

    Returns:
        ``shots`` with :data:`ADJUSTED` columns replaced.
    """
    df = shots.join(game_venues.select("game_id", "arena_id"), on="game_id", how="left").with_row_index("_r")
    dist = df["event_distance"].to_numpy().astype(float).copy()
    ang = df["event_angle"].to_numpy().astype(float).copy()
    keyed = {
        (r["arena_id"], r["season"]): r
        for r in maps.iter_rows(named=True)
        if include_tracking_era or r["season"] < TRACKING_ERA_START
    }
    for (arena, season), part in df.group_by("arena_id", "season"):
        m = keyed.get((arena, season))
        if m is None:
            continue
        idx = part["_r"].to_numpy()
        ok = ~np.isnan(dist[idx])
        sel = idx[ok]
        new_d = np.clip(_map_values(dist[sel], np.array(m["dist_from"]), np.array(m["dist_to"]), m["shrink"]), 0.5, None)
        new_a = np.clip(_map_values(ang[sel], np.array(m["angle_from"]), np.array(m["angle_to"]), m["shrink"]), 0.0, 180.0)
        # Never let the mapping invent a missing value: keep the raw location if it would.
        ok_map = np.isfinite(new_d) & np.isfinite(new_a)
        dist[sel] = np.where(ok_map, new_d, dist[sel])
        ang[sel] = np.where(ok_map, new_a, ang[sel])

    raw = {c: pl.col(c).alias(f"{c}_raw") for c in ADJUSTED}
    theta = pl.col("event_angle").radians()
    out = (
        df.with_columns(*raw.values())
        .with_columns(
            pl.Series("event_distance", dist, dtype=pl.Float32),
            pl.Series("event_angle", ang, dtype=pl.Float32),
        )
        .with_columns(
            (GOAL_X - pl.col("event_distance") * theta.cos()).cast(pl.Float32).alias("x_abs"),
            (pl.col("y_abs_raw").sign().replace(0, 1) * pl.col("event_distance") * theta.sin()).cast(pl.Float32).alias("y_abs"),
        )
        .with_columns(
            ((pl.col("x_abs") - pl.col("x_abs_last")).pow(2) + (pl.col("y_abs") - pl.col("y_abs_last")).pow(2)).sqrt().cast(pl.Float32).alias("distance_from_last"),
            (pl.col("event_angle") - pl.arctan2(pl.col("y_abs_last").abs(), GOAL_X - pl.col("x_abs_last")).degrees()).abs().cast(pl.Float32).alias("event_angle_change"),
        )
        .with_columns(
            (pl.col("distance_from_last") / pl.col("seconds_since_last")).cast(pl.Float32).alias("puck_speed_since_last"),
            (pl.col("event_angle_change") / pl.col("seconds_since_last")).cast(pl.Float32).alias("event_angle_change_speed"),
            pl.when(pl.col("shooter_shoots_r").is_null()).then(None).otherwise(
                (
                    (pl.col("event_angle") > 10)
                    & (((pl.col("shooter_shoots_r") == 1) & (pl.col("y_abs") > 0)) | ((pl.col("shooter_shoots_r") == 0) & (pl.col("y_abs") < 0)))
                ).cast(pl.Float32)
            ).alias("off_wing"),
            (pl.col("event_distance") - pl.col("event_distance_raw")).alias("rink_shift_ft"),
        )
        .drop("_r")
    )
    # v2 location features (net angle, slot, cross-ice movement) derive from x_abs/y_abs,
    # so recompute them from the adjusted coordinates when the shots table carries them.
    from nhl.features import shots as shot_features

    add_geometry = getattr(shot_features, "add_geometry_features", None)
    if add_geometry is not None and "same_team_last" in out.columns:
        out = add_geometry(out)
    return out


def validation_report(shots: pl.DataFrame, game_venues: pl.DataFrame, adjusted: pl.DataFrame) -> pl.DataFrame:
    """Out-of-sample check on *home-team* shots (never used to estimate the maps).

    For each era, the spread across arenas of each home team's mean shot distance at
    home minus its mean distance on the road, before and after adjustment. Scorer bias
    inflates that spread; a good adjustment shrinks it.

    Returns:
        ``era, sd_before, sd_after`` in feet.
    """
    def home_minus_road(df: pl.DataFrame, col: str) -> pl.DataFrame:
        d = df.filter(pl.col("strength_group") != "EN").with_columns(pl.col("season").map_elements(_era, return_dtype=pl.Int8).alias("era"))
        home = d.filter(pl.col("is_home") == 1).group_by("era", "season", "event_team_abbr").agg(pl.col(col).mean().alias("h"))
        road = d.filter(pl.col("is_home") == 0).group_by("era", "season", "event_team_abbr").agg(pl.col(col).mean().alias("r"))
        return home.join(road, on=["era", "season", "event_team_abbr"]).with_columns((pl.col("h") - pl.col("r")).alias("gap"))

    shots = shots.filter(pl.col("event_distance").is_not_nan())
    adjusted = adjusted.filter(pl.col("event_distance").is_not_nan())
    before = home_minus_road(shots, "event_distance").group_by("era").agg(pl.col("gap").std().alias("sd_before"))
    # After: home-team shots use adjusted distances; road games are adjusted at *other* arenas.
    after = home_minus_road(adjusted, "event_distance").group_by("era").agg(pl.col("gap").std().alias("sd_after"))
    return before.join(after, on="era").sort("era")
