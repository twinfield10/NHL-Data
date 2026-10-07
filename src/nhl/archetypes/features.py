"""Style features per skater (archetypes plan phase A): ``processed/style/{season}``.

Archetypes describe *how* a skater plays, never how well (owner, 2026-10-07): quality is the
M3 ratings' job. Every feature is therefore a mix, a rate of an action, or a deployment share.
Nothing here is an outcome measure such as xGF, goals or points per 60.

* **Counts** (``processed/style_counts/{season}``): one row per regular-season skater-game,
  5v5 unless noted. Shots are the skater's own unblocked 5v5 attempts from the
  rink-adjusted shot table, so location shares are net of arena scorer bias.
* **Scorer bias:** hits, hits taken, giveaways, takeaways and blocks are recorded by the
  home scorer and vary a lot by arena (M4 found about half of team "style" was arena bias).
  Their features use **road games only**.
* **Windows:** ``season`` (that season alone) and ``2yr`` (that season plus half-weighted
  counts from the previous one), so a skater with 20 games this year is still placed.
* **Shrinkage:** each feature is shrunk to its position-group mean by empirical Bayes,
  with the prior weight ``k`` fitted by method of moments on skaters with ≥
  :data:`PRIOR_MIN_MINUTES` 5v5 minutes:

  - ``rate`` (Poisson): x = count / hours, noise m / hours, shrunk (count + k m) / (hours + k);
  - ``share`` (binomial): p = s / n, noise m(1 − m) / n, shrunk (s + k m) / (n + k);
  - ``mean`` (normal): x̄ = Σx / n, noise σ² / n with σ² the pooled within-player variance;
  - ``fixed``: height and weight, not shrunk.

  The implied reliability of a feature after t units of exposure is t / (t + k); it is
  reported at 500 5v5 minutes as ``rel_500``, next to the empirical split-half reliability.
* **EDGE add-on** (2021-22 on): skating and shot-speed traits joined unshrunk, null before.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import polars as pl

from nhl import config
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

PRIOR_MIN_MINUTES = 200
MIN_PRIOR_SKATERS = 100
#: Minutes threshold for the split-half check (each half then has roughly half of them).
SPLIT_HALF_MIN_MINUTES = 500
PREV_SEASON_WEIGHT = 0.5
WINDOWS = ("season", "2yr")
FORWARDS = ("C", "L", "R")

_SHOT_EVENTS = ("SHOT", "MISSED_SHOT", "GOAL")
_ROAD = ("toi_5v5_s", "hits", "hits_taken", "giveaways", "takeaways", "blocks", "ca")


@dataclass(frozen=True)
class Feature:
    """One style feature: ``kind`` in {rate, share, mean, fixed}; ``num`` / ``den`` are count columns."""

    name: str
    kind: str
    num: str
    den: str = ""
    sq: str = ""
    label: str = ""


FEATURES: tuple[Feature, ...] = (
    # Shooting volume and where shots come from
    Feature("shots60", "rate", "iff", "toi_5v5_s", label="unblocked attempts / 60"),
    Feature("slot_share", "share", "n_slot", "n_shots", label="share of shots from the slot"),
    Feature("net_front_share", "share", "n_net_front", "n_shots", label="share within 20 ft"),
    Feature("point_share", "share", "n_point", "n_shots", label="share beyond 45 ft"),
    Feature("off_wing_share", "share", "n_off_wing", "n_shots", label="share from the off wing"),
    Feature("shot_dist", "mean", "dist_sum", "n_dist", sq="dist_sq", label="mean shot distance (ft)"),
    # Shot types
    Feature("wristsnap_share", "share", "n_wristsnap", "n_shots", label="wrist and snap shots"),
    Feature("slap_share", "share", "n_slap", "n_shots", label="slap shots"),
    Feature("backhand_share", "share", "n_backhand", "n_shots", label="backhands"),
    Feature("tip_share", "share", "n_tip", "n_shots", label="tips and deflections"),
    # How chances arise
    Feature("rebound_share", "share", "n_rebound", "n_shots", label="rebound shots"),
    Feature("rush_share", "share", "n_rush", "n_shots", label="rush shots"),
    Feature("turnover_share", "share", "n_turnover", "n_shots", label="shots off a turnover"),
    Feature("transition_share", "share", "n_transition", "n_shots", label="rush or turnover shots"),
    Feature("a1_ratio", "share", "a1", "a1_plus_g", label="primary assists / (A1 + goals)"),
    # Puck play and physical (road only)
    Feature("take60", "rate", "takeaways_road", "toi_5v5_road_s", label="takeaways / 60 (road)"),
    Feature("give60", "rate", "giveaways_road", "toi_5v5_road_s", label="giveaways / 60 (road)"),
    Feature("hits60", "rate", "hits_road", "toi_5v5_road_s", label="hits / 60 (road)"),
    Feature("hits_taken60", "rate", "hits_taken_road", "toi_5v5_road_s", label="hits taken / 60 (road)"),
    Feature("blocks60", "rate", "blocks_road", "toi_5v5_road_s", label="blocks / 60 (road)"),
    Feature("block_share", "share", "blocks_road", "ca_road", label="share of on-ice attempts against blocked (road)"),
    Feature("pen_taken60", "rate", "pen_taken", "toi_all_s", label="penalties taken / 60"),
    Feature("pen_drawn60", "rate", "pen_drawn", "toi_all_s", label="penalties drawn / 60"),
    # Deployment
    Feature("faceoffs60", "rate", "faceoffs", "toi_all_s", label="faceoffs taken / 60"),
    Feature("dz_start_share", "share", "dz_starts", "oz_dz_starts", label="DZ share of O/D zone starts"),
    Feature("pp_share", "mean", "share_pp", "games", sq="share_pp_sq", label="share of team PP time"),
    Feature("pk_share", "mean", "share_pk", "games", sq="share_pk_sq", label="share of team PK time"),
    Feature("pp1_rate", "share", "games_pp1", "games", label="share of games on PP1"),
    # Body
    Feature("height_in", "fixed", "height_in", label="height (in)"),
    Feature("weight_lb", "fixed", "weight_lb", label="weight (lb)"),
)

#: EDGE traits (2021-22 on), unshrunk; ``None`` means computed below.
EDGE_FEATURES = {
    "edge_max_speed": "speed_max_skating_speed",
    "edge_bursts20_pg": None,
    "edge_dist60": "distance_all_distance_per_60",
    "edge_shot_speed": "shot_speed_avg_shot_speed",
    "edge_oz_time": "zone_es_offensive_zone_pctg",
}

_KEYS = ["game_id", "player_id"]


# --- counts per skater-game ------------------------------------------------------------------


def _log_counts(logs: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """Regular-season skater-games with 5v5 and all-strength counts and ``is_home``."""
    reg = games.filter(pl.col("season_type") == "R").select("game_id", "home_team_id")
    logs = logs.filter(pl.col("position") != "G").join(reg, on="game_id", how="inner")
    allst = logs.filter(pl.col("strength") == "all").select(
        "game_id", "season", "game_date", "team_id", "player_id", "position",
        (pl.col("team_id") == pl.col("home_team_id")).alias("is_home"),
        pl.col("toi_s").alias("toi_all_s"), "pen_taken", "pen_drawn",
        (pl.col("fo_won") + pl.col("fo_lost")).alias("faceoffs"),
    )
    ev = logs.filter(pl.col("strength") == "5v5").select(
        *_KEYS, pl.col("toi_s").alias("toi_5v5_s"), "iff", "goals", "a1", "ca",
        "hits", "hits_taken", "giveaways", "takeaways", "blocks",
    )
    return allst.join(ev, on=_KEYS, how="left")


def _shot_counts(shots: pl.DataFrame) -> pl.DataFrame:
    """Per (game, shooter): the skater's 5v5 unblocked attempts by location, type and origin."""
    s = shots.filter((pl.col("strength_state") == "5v5") & pl.col("event_type").is_in(_SHOT_EVENTS))
    flag = lambda e: e.cast(pl.Int32).sum()  # noqa: E731
    turnover = ((pl.col("prior_give_opp") == 1) | (pl.col("prior_take_same") == 1)) & (pl.col("seconds_since_last") <= 5)
    return s.group_by("game_id", pl.col("shooter_id").alias("player_id")).agg(
        pl.len().cast(pl.Int32).alias("n_shots"),
        flag(pl.col("in_slot") == 1).alias("n_slot"),
        flag(pl.col("event_distance") <= 20).alias("n_net_front"),
        flag(pl.col("event_distance") > 45).alias("n_point"),
        flag(pl.col("off_wing") == 1).alias("n_off_wing"),
        # A few shots have no location (NaN distance); they count everywhere except distance.
        pl.col("event_distance").fill_nan(None).count().cast(pl.Int32).alias("n_dist"),
        pl.col("event_distance").fill_nan(None).sum().alias("dist_sum"),
        (pl.col("event_distance").fill_nan(None) ** 2).sum().alias("dist_sq"),
        flag((pl.col("shot_wrist") == 1) | (pl.col("shot_snap") == 1)).alias("n_wristsnap"),
        flag(pl.col("shot_slap") == 1).alias("n_slap"),
        flag(pl.col("shot_backhand") == 1).alias("n_backhand"),
        flag((pl.col("shot_tip_in") == 1) | (pl.col("shot_deflected") == 1)).alias("n_tip"),
        flag(pl.col("is_rebound") == 1).alias("n_rebound"),
        flag(pl.col("is_rush_play") == 1).alias("n_rush"),
        flag(turnover & (pl.col("is_rush_play") != 1)).alias("n_turnover"),
    )


def build_counts(logs: pl.DataFrame, games: pl.DataFrame, shots: pl.DataFrame, usage: pl.DataFrame) -> pl.DataFrame:
    """One row per regular-season skater-game with every count the features need.

    Args:
        logs: ``processed/game_logs/player/{season}``.
        games: ``processed/games``.
        shots: ``processed/shots_rink/{season}`` (rink-adjusted locations).
        usage: ``processed/usage/{season}``.
    """
    use = usage.select(
        *_KEYS, pl.col("share_pp").fill_null(0), pl.col("share_pk").fill_null(0),
        (pl.col("pp_unit") == 1).fill_null(False).cast(pl.Int8).alias("games_pp1"),
        "oz_starts", "dz_starts",
    )
    out = (
        _log_counts(logs, games)
        .join(_shot_counts(shots), on=_KEYS, how="left")
        .join(use, on=_KEYS, how="left")
    )
    count_cols = [c for c in out.columns if c not in ("game_id", "season", "game_date", "team_id", "player_id",
                                                       "position", "is_home")]
    return out.with_columns(pl.col(count_cols).fill_null(0)).sort("game_id", "player_id")


# --- player-season aggregates ------------------------------------------------------------------


def _sum_exprs(weight: pl.Expr) -> list[pl.Expr]:
    road = ~pl.col("is_home")
    num = ["toi_5v5_s", "toi_all_s", "iff", "goals", "a1", "pen_taken", "pen_drawn", "faceoffs",
           "n_shots", "n_slot", "n_net_front", "n_point", "n_off_wing", "n_dist", "dist_sum", "dist_sq",
           "n_wristsnap", "n_slap", "n_backhand", "n_tip", "n_rebound", "n_rush", "n_turnover",
           "share_pp", "share_pk", "games_pp1", "oz_starts", "dz_starts"]
    return [
        pl.len().alias("games_raw"),
        (pl.col("toi_all_s").is_not_null() * weight).sum().alias("games"),
        *[(pl.col(c) * weight).sum().alias(c) for c in num],
        (pl.col("share_pp") ** 2 * weight).sum().alias("share_pp_sq"),
        (pl.col("share_pk") ** 2 * weight).sum().alias("share_pk_sq"),
        *[(pl.col(c) * weight).filter(road).sum().alias(f"{c.removesuffix('_s')}_road_s" if c.endswith("_s") else f"{c}_road")
          for c in _ROAD],
    ]


def aggregate(counts: pl.DataFrame, weight: pl.Expr | None = None) -> pl.DataFrame:
    """Sum (optionally weighted) counts to one row per player: ``player_id, position, group, ...``.

    ``position`` is the skater's most common position, ``group`` F or D. Derived denominators
    ``a1_plus_g`` and ``oz_dz_starts`` are added.
    """
    weight = pl.lit(1.0) if weight is None else weight
    return (
        counts.group_by("player_id")
        .agg(pl.col("position").mode().sort().first(), *_sum_exprs(weight))
        .with_columns(
            pl.when(pl.col("position") == "D").then(pl.lit("D")).otherwise(pl.lit("F")).alias("group"),
            (pl.col("a1") + pl.col("goals")).alias("a1_plus_g"),
            (pl.col("oz_starts") + pl.col("dz_starts")).alias("oz_dz_starts"),
            (pl.col("n_rush") + pl.col("n_turnover")).alias("n_transition"),
            (pl.col("toi_5v5_s") / 60).alias("toi_5v5_min"),
        )
    )


# --- empirical-Bayes shrinkage -------------------------------------------------------------------


def _den(df: pl.DataFrame, f: Feature) -> np.ndarray:
    d = df[f.den].to_numpy().astype(float)
    return d / 3600.0 if f.kind == "rate" else d


def fit_prior(pop: pl.DataFrame, f: Feature) -> tuple[float, float]:
    """``(mean, k)`` for one feature from a population of player aggregates (method of moments).

    ``k`` is the prior weight in the feature's own exposure units (hours for rates, events for
    shares and means); ``inf`` when the between-player variance is not above the noise.
    """
    num = pop[f.num].to_numpy().astype(float)
    if f.kind == "fixed":
        return float(np.nanmean(num)), 0.0
    den = _den(pop, f)
    ok = den > 0
    num, den = num[ok], den[ok]
    m = num.sum() / den.sum()
    x = num / den
    if f.kind == "rate":
        noise, scale = m / den, m
    elif f.kind == "share":
        noise, scale = m * (1 - m) / den, m * (1 - m)
    else:  # mean
        sq = pop[f.sq].to_numpy().astype(float)[ok]
        within = (sq - num ** 2 / den).sum() / max((den - 1).clip(0).sum(), 1.0)
        noise, scale = within / den, within
    tau2 = float(np.mean((x - m) ** 2 - noise))
    if tau2 <= 0:
        return float(m), float("inf")
    k = scale / tau2 - (1.0 if f.kind == "share" else 0.0)
    return float(m), max(float(k), 0.0)


def shrink(df: pl.DataFrame, f: Feature, m: float, k: float) -> tuple[np.ndarray, np.ndarray]:
    """``(raw, shrunk)`` values of one feature for every row of ``df``."""
    num = df[f.num].to_numpy().astype(float)
    if f.kind == "fixed":
        return num, num
    den = _den(df, f)
    with np.errstate(divide="ignore", invalid="ignore"):
        raw = np.where(den > 0, num / den, np.nan)
        shrunk = m if not np.isfinite(k) else (num + k * m) / (den + k)
    shrunk = np.where(np.isfinite(shrunk), shrunk, m)
    return raw, np.broadcast_to(shrunk, raw.shape).astype(float)


def _exposure_500(pop: pl.DataFrame, f: Feature) -> float:
    """The feature's typical exposure for a skater with 500 5v5 minutes."""
    if f.kind == "fixed":
        return float("nan")
    per_min = _den(pop, f).sum() / pop["toi_5v5_min"].sum()
    return float(per_min * 500)


def shrink_all(agg: pl.DataFrame, fallback: pl.DataFrame | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fit priors by position group and shrink every feature.

    Early in a season, when fewer than :data:`MIN_PRIOR_SKATERS` skaters in a group reach
    :data:`PRIOR_MIN_MINUTES`, the group's priors come from ``fallback`` (the previous
    season's ``style_priors`` for the same window) instead.

    Returns:
        ``(features, priors)``: features has ``{name}`` (shrunk) and ``{name}_raw`` per feature;
        priors has ``group, feature, kind, mean, k, rel_500`` (``rel_500`` = t / (t + k) at the
        exposure of a 500-minute skater).
    """
    parts, priors = [], []
    for group in ("F", "D"):
        df = agg.filter(pl.col("group") == group)
        pop = df.filter(pl.col("toi_5v5_min") >= PRIOR_MIN_MINUTES)
        borrowed = fallback is not None and pop.height < MIN_PRIOR_SKATERS
        if borrowed:
            prior = {r["feature"]: (r["mean"], r["k"]) for r in fallback.filter(pl.col("group") == group).iter_rows(named=True)}
            pop = df  # only for the exposure used in rel_500
        cols = {}
        for f in FEATURES:
            m, k = prior[f.name] if borrowed and f.name in prior else fit_prior(pop, f)
            raw, shrunk = shrink(df, f, m, k)
            cols[f.name], cols[f"{f.name}_raw"] = shrunk, raw
            t = _exposure_500(pop, f)
            rel = float("nan") if f.kind == "fixed" else (0.0 if not np.isfinite(k) else t / (t + k))
            priors.append({"group": group, "feature": f.name, "kind": f.kind, "mean": m, "k": k, "rel_500": rel,
                           "borrowed": borrowed})
        parts.append(df.select("player_id").with_columns(pl.Series(n, v) for n, v in cols.items()))
    return pl.concat(parts), pl.DataFrame(priors)


# --- split-half reliability ------------------------------------------------------------------


def split_half(counts: pl.DataFrame, min_minutes: float = SPLIT_HALF_MIN_MINUTES) -> pl.DataFrame:
    """Odd/even-game reliability of each raw feature among skaters with ≥ ``min_minutes`` at 5v5.

    Returns ``group, feature, n, r_half, split_half`` where ``split_half`` is the Spearman-Brown
    full-sample reliability 2r / (1 + r).
    """
    numbered = counts.sort("game_date", "game_id").with_columns(
        (pl.int_range(pl.len()).over("player_id") % 2).alias("_half")
    )
    halves = [aggregate(numbered.filter(pl.col("_half") == h)) for h in (0, 1)]
    full = aggregate(counts).filter(pl.col("toi_5v5_min") >= min_minutes).select("player_id", "group")
    rows = []
    for group in ("F", "D"):
        ids = full.filter(pl.col("group") == group).select("player_id")
        a, b = (h.join(ids, on="player_id", how="inner").sort("player_id") for h in halves)
        a, b = a.join(b.select("player_id"), on="player_id"), b.join(a.select("player_id"), on="player_id")
        for f in FEATURES:
            if f.kind == "fixed":
                continue
            xa, xb = (shrink(h, f, 0.0, 0.0)[0] for h in (a, b))
            ok = np.isfinite(xa) & np.isfinite(xb)
            r = float(np.corrcoef(xa[ok], xb[ok])[0, 1]) if ok.sum() > 10 else float("nan")
            rows.append({"group": group, "feature": f.name, "n": int(ok.sum()), "r_half": r,
                         "split_half": 2 * r / (1 + r) if np.isfinite(r) and r > -1 else float("nan")})
    return pl.DataFrame(rows)


# --- EDGE add-on --------------------------------------------------------------------------------


def edge_traits(edge: pl.DataFrame | None) -> pl.DataFrame | None:
    """Regular-season EDGE traits per player (``edge_*`` columns), or None when not available."""
    if edge is None or edge.is_empty():
        return None
    reg = edge.filter(pl.col("game_type") == 2)
    return reg.group_by("player_id").agg(
        pl.col("speed_max_skating_speed").max().alias("edge_max_speed"),
        ((pl.col("speed_bursts_over_22") + pl.col("speed_bursts_20_to_22")).sum()
         / pl.col("games_played").sum()).alias("edge_bursts20_pg"),
        *[((pl.col(src) * pl.col("games_played")).sum() / pl.col("games_played").sum()).alias(name)
          for name, src in EDGE_FEATURES.items() if src and name != "edge_max_speed"],
    )


# --- build -------------------------------------------------------------------------------------


def _window_counts(counts: pl.DataFrame, prev: pl.DataFrame | None, window: str) -> pl.DataFrame:
    """Aggregates for one window; ``2yr`` adds the previous season's counts at half weight."""
    if window == "season" or prev is None:
        return aggregate(counts)
    both = pl.concat([counts.with_columns(pl.lit(1.0).alias("_w")),
                      prev.with_columns(pl.lit(PREV_SEASON_WEIGHT).alias("_w"))], how="diagonal_relaxed")
    # The current season decides the position.
    pos = aggregate(counts).select("player_id", "position", "group")
    agg = aggregate(both, pl.col("_w"))
    return agg.drop("position", "group").join(pos, on="player_id", how="inner")


def build_style(counts: pl.DataFrame, prev: pl.DataFrame | None, players: pl.DataFrame,
                edge: pl.DataFrame | None, season: int,
                prev_priors: pl.DataFrame | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Style features and priors for both windows of one season (see module docstring).

    ``prev_priors`` (the previous season's ``style_priors``) backs up thin early-season groups.
    """
    bio = players.select("player_id", pl.col("height_in").cast(pl.Float64), pl.col("weight_lb").cast(pl.Float64))
    traits = edge_traits(edge)
    halves = split_half(counts)
    out, priors = [], []
    for window in WINDOWS:
        agg = _window_counts(counts, prev, window).join(bio, on="player_id", how="left")
        fallback = None if prev_priors is None else prev_priors.filter(pl.col("window") == window)
        feats, pri = shrink_all(agg, fallback)
        base = agg.select("player_id", "position", "group", "games_raw", "games", "toi_5v5_min")
        df = base.join(feats, on="player_id").with_columns(pl.lit(season).alias("season"), pl.lit(window).alias("window"))
        if traits is not None:
            df = df.join(traits, on="player_id", how="left")
        out.append(df)
        pri = pri.with_columns(pl.lit(season).alias("season"), pl.lit(window).alias("window"))
        if window == "season":
            pri = pri.join(halves, on=["group", "feature"], how="left")
        priors.append(pri)
    style = pl.concat(out, how="diagonal_relaxed").sort("window", "group", "player_id")
    first = ["season", "window", "player_id", "position", "group", "games_raw", "games", "toi_5v5_min"]
    return style.select(*first, pl.exclude(first)), pl.concat(priors, how="diagonal_relaxed")


def season_counts(store: Store, season: int, games: pl.DataFrame | None = None) -> pl.DataFrame:
    """Build and store ``processed/style_counts/{season}``."""
    games = store.read_parquet_required(keys.GAMES) if games is None else games
    counts = build_counts(
        store.read_parquet_required(keys.player_game_logs(season)),
        games,
        store.read_parquet_required(keys.shots_rink_adjusted(season)),
        store.read_parquet_required(keys.usage(season)),
    )
    store.put_parquet(keys.style_counts(season), counts)
    return counts


def build_season(store: Store, season: int) -> dict[str, float]:
    """Build ``style_counts``, ``style`` and ``style_priors`` for one season; return checks."""
    games = store.read_parquet_required(keys.GAMES)
    counts = season_counts(store, season, games)
    prev_id = config.season_id(config.season_start_year(season) - 1)
    prev = store.get_parquet(keys.style_counts(prev_id))
    if prev is None and store.get_parquet(keys.usage(prev_id)) is not None:
        prev = season_counts(store, prev_id, games)
    edge = store.get_parquet(keys.edge("skater", season))
    style, priors = build_style(counts, prev, store.read_parquet_required(keys.PLAYERS), edge, season,
                                store.get_parquet(keys.style_priors(prev_id)))
    store.put_parquet(keys.style(season), style)
    store.put_parquet(keys.style_priors(season), priors)
    sp = priors.filter(pl.col("window") == "season")
    checks = {
        "skaters": float(style.filter(pl.col("window") == "season").height),
        "fitted_500": float(style.filter((pl.col("window") == "season") & (pl.col("toi_5v5_min") >= 500)).height),
        "median_split_half": float(sp["split_half"].drop_nulls().median() or float("nan")),
        "median_rel_500": float(sp["rel_500"].drop_nans().drop_nulls().median() or float("nan")),
    }
    logger.info("style %s: %s", season, ", ".join(f"{k} {v:.3f}" for k, v in checks.items()))
    return checks
