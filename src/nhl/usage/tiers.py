"""Deployment tiers and usage per skater-game (usage plan phase A): ``processed/usage/{season}``.

Tiers come from **ice time only** (owner, 2026-10-07); quality comes from the ratings and
is kept separate, so "top line" never means "top-line numbers".

* **Rank:** ``toi_rank`` is each skater's 5v5 TOI rank among his team's dressed forwards
  (defencemen) who played 5v5 in the game. Tier = ``ceil(slot / 3)`` for forwards (F1-F4)
  and ``ceil(slot / 2)`` for defence (D1-D3), capped at F4 / D3 so a 13th forward or 7th
  defenceman lands in the bottom tier.
* **Units:** a skater in an inferred line or pair (:mod:`nhl.gamestate.lineups`) with
  ``confidence`` ≥ :data:`UNIT_CONFIDENCE` is ranked as a block with his unit, at the
  members' mean 5v5 TOI, so linemates share a tier whenever the units are complete.
  Others are ranked at their own TOI. Tiers then fill in order of that ranking
  (``tier_source`` says which applied).
* **Special teams:** ``pp_unit`` / ``pk_unit`` are 1 or 2 when the skater is in that inferred
  unit (unit 1 wins when he's in both), else null.
* **Zone starts:** 5v5 stints that begin with a faceoff, from the skater's point of view.
  ``oz_start_share`` = OZ ÷ (OZ + DZ); neutral-zone starts are counted but left out of it.

Team shares use the team's own state time from the stints (5v5, PP = more skaters, PK =
fewer, both goalies in), so ``share_5v5`` is the share of the team's 5v5 time the skater
was on for (a top-pair defenceman is ≈ 0.45, a fourth-line forward ≈ 0.20).
"""

from __future__ import annotations

import logging

import polars as pl

from nhl.gamestate.lineups import team_stints
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

UNIT_CONFIDENCE = 0.6
FORWARDS = ("C", "L", "R")
#: (group, unit_type in the lineups table, players per tier, tiers)
GROUPS = (("F", "F", 3, 4), ("D", "D", 2, 3))

_KEYS = ["game_id", "team_id", "player_id"]


def _strength_toi(logs: pl.DataFrame) -> pl.DataFrame:
    """``game_id, season, game_date, team_id, player_id, position, toi_5v5_s, toi_pp_s, toi_pk_s``."""
    skaters = logs.filter(pl.col("position") != "G")
    base = skaters.filter(pl.col("strength") == "all").select("game_id", "season", "game_date", *_KEYS[1:], "position")
    for strength, name in (("5v5", "toi_5v5_s"), ("PP", "toi_pp_s"), ("SH", "toi_pk_s")):
        part = skaters.filter(pl.col("strength") == strength).select(*_KEYS, pl.col("toi_s").alias(name))
        base = base.join(part, on=_KEYS, how="left")
    return base.with_columns(pl.col("toi_5v5_s", "toi_pp_s", "toi_pk_s").fill_null(0).cast(pl.Int32))


def _team_state_seconds(stints: pl.DataFrame) -> pl.DataFrame:
    """``game_id, team_id, team_5v5_s, team_pp_s, team_pk_s``."""
    ts = team_stints(stints).filter(pl.col("state") != "other")
    return ts.group_by("game_id", "team_id").agg(
        *[pl.col("duration_s").filter(pl.col("state") == s).sum().alias(f"team_{n}_s")
          for s, n in (("5v5", "5v5"), ("PP", "pp"), ("PK", "pk"))]
    )


def _tiers(toi: pl.DataFrame, lineups: pl.DataFrame) -> pl.DataFrame:
    """``game_id, team_id, player_id, toi_rank, tier, tier_source``."""
    out = []
    for group, unit_type, per, n_tiers in GROUPS:
        in_group = pl.col("position").is_in(FORWARDS) if group == "F" else (pl.col("position") == "D")
        played = toi.filter(in_group & (pl.col("toi_5v5_s") > 0))
        units = (
            lineups.filter((pl.col("unit_type") == unit_type) & (pl.col("confidence") >= UNIT_CONFIDENCE))
            .select("game_id", "team_id", "unit_rank", pl.col("members").alias("player_id"))
            .explode("player_id", empty_as_null=True)
            .join(played.select(*_KEYS, "toi_5v5_s"), on=_KEYS, how="inner")
            .with_columns(pl.col("toi_5v5_s").mean().over("game_id", "team_id", "unit_rank").alias("unit_toi"))
            .select(*_KEYS, "unit_rank", "unit_toi")
        )
        rank = lambda *by: pl.struct(*by).rank("ordinal").over("game_id", "team_id").cast(pl.Int16)  # noqa: E731
        ranked = played.join(units, on=_KEYS, how="left").with_columns(
            rank(-pl.col("toi_5v5_s"), "player_id").alias("toi_rank"),
            # A unit sorts as one block at its members' mean TOI; everyone else at his own.
            rank(-pl.coalesce("unit_toi", pl.col("toi_5v5_s").cast(pl.Float64)), "unit_rank",
                 -pl.col("toi_5v5_s"), "player_id").alias("_slot"),
        )
        out.append(ranked.select(
            *_KEYS, "toi_rank",
            pl.concat_str(pl.lit(group), (pl.col("_slot") / per).ceil().clip(1, n_tiers).cast(pl.Int8)).alias("tier"),
            pl.when(pl.col("unit_toi").is_not_null()).then(pl.lit("unit")).otherwise(pl.lit("rank")).alias("tier_source"),
        ))
    return pl.concat(out)


def _special_units(lineups: pl.DataFrame) -> pl.DataFrame:
    """``game_id, team_id, player_id, pp_unit, pk_unit`` (null when in neither unit)."""
    parts = []
    for unit_type, name in (("PP", "pp_unit"), ("PK", "pk_unit")):
        parts.append(
            lineups.filter(pl.col("unit_type") == unit_type)
            .select("game_id", "team_id", "unit_rank", pl.col("members").alias("player_id"))
            .explode("player_id", empty_as_null=True)
            .group_by(_KEYS).agg(pl.col("unit_rank").min().cast(pl.Int8).alias(name))
        )
    return parts[0].join(parts[1], on=_KEYS, how="full", coalesce=True)


def zone_starts(stints: pl.DataFrame) -> pl.DataFrame:
    """5v5 faceoff starts per skater-game: ``game_id, team_id, player_id, oz_starts, nz_starts, dz_starts``.

    ``start_type`` is from the home team's point of view, so away skaters' OZ and DZ swap.
    """
    fo = stints.filter(
        (pl.col("strength_state") == "5v5") & pl.col("valid_personnel")
        & pl.col("start_type").is_in(["faceoff_oz", "faceoff_nz", "faceoff_dz"])
        & pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()
    )
    flip = {"faceoff_oz": "dz", "faceoff_dz": "oz", "faceoff_nz": "nz"}
    sides = []
    for own, zone in (("home", pl.col("start_type").str.strip_prefix("faceoff_")),
                      ("away", pl.col("start_type").replace_strict(flip))):
        sides.append(fo.select("game_id", pl.col(f"{own}_team_id").alias("team_id"),
                               pl.col(f"{own}_skaters").alias("player_id"), zone.alias("zone")).explode("player_id", empty_as_null=True))
    starts = pl.concat(sides)
    return starts.group_by(_KEYS).agg(
        *[(pl.col("zone") == z).sum().cast(pl.Int16).alias(f"{z}_starts") for z in ("oz", "nz", "dz")]
    )


def build_usage(logs: pl.DataFrame, lineups: pl.DataFrame, stints: pl.DataFrame) -> pl.DataFrame:
    """One row per skater-game that dressed (see module docstring).

    Args:
        logs: ``processed/game_logs/players/{season}`` (by strength).
        lineups: ``processed/lineups/{season}``.
        stints: ``processed/stints/{season}``.

    Returns:
        ``game_id, season, game_date, team_id, player_id, position, toi_5v5_s, toi_pp_s,
        toi_pk_s, share_5v5, share_pp, share_pk, toi_rank, tier, tier_source, pp_unit,
        pk_unit, oz_starts, nz_starts, dz_starts, oz_start_share``. ``tier`` is null for a
        skater with no 5v5 time.
    """
    toi = _strength_toi(logs)
    team = _team_state_seconds(stints)
    share = lambda a, b: pl.when(pl.col(b) > 0).then(pl.col(a) / pl.col(b))  # noqa: E731
    return (
        toi.join(team, on=["game_id", "team_id"], how="left")
        .join(_tiers(toi, lineups), on=_KEYS, how="left")
        .join(_special_units(lineups), on=_KEYS, how="left")
        .join(zone_starts(stints), on=_KEYS, how="left")
        .with_columns(pl.col("oz_starts", "nz_starts", "dz_starts").fill_null(0))
        .with_columns(
            share("toi_5v5_s", "team_5v5_s").alias("share_5v5"),
            share("toi_pp_s", "team_pp_s").alias("share_pp"),
            share("toi_pk_s", "team_pk_s").alias("share_pk"),
            pl.when((pl.col("oz_starts") + pl.col("dz_starts")) > 0)
            .then(pl.col("oz_starts") / (pl.col("oz_starts") + pl.col("dz_starts"))).alias("oz_start_share"),
        )
        .drop("team_5v5_s", "team_pp_s", "team_pk_s")
        .sort("game_id", "team_id", "tier", "toi_rank", nulls_last=True)
    )


def summarize(usage: pl.DataFrame) -> pl.DataFrame:
    """Per (season, player, team): games, games by tier, TOI per game and special-teams roles.

    A traded player gets one row per team. ``tier_mode`` is his most common tier (ties go to
    the higher tier); ``tier_avg`` is the mean tier number (1 = top).
    """
    played = usage.filter(pl.col("tier").is_not_null())
    tier_cols = [f"F{i}" for i in range(1, 5)] + [f"D{i}" for i in range(1, 4)]
    return played.group_by("season", "player_id", "team_id").agg(
        pl.col("position").last(),
        pl.len().alias("games"),
        *[(pl.col("tier") == t).sum().cast(pl.Int16).alias(f"games_{t}") for t in tier_cols],
        pl.col("tier").mode().sort().first().alias("tier_mode"),
        pl.col("tier").str.slice(1).cast(pl.Int8).mean().alias("tier_avg"),
        (pl.col("toi_5v5_s").mean() / 60).alias("toi_5v5_pg"),
        (pl.col("toi_pp_s").mean() / 60).alias("toi_pp_pg"),
        (pl.col("toi_pk_s").mean() / 60).alias("toi_pk_pg"),
        pl.col("share_5v5").mean(), pl.col("share_pp").mean(), pl.col("share_pk").mean(),
        (pl.col("pp_unit") == 1).sum().cast(pl.Int16).alias("games_pp1"),
        (pl.col("pp_unit") == 2).sum().cast(pl.Int16).alias("games_pp2"),
        (pl.col("pk_unit") == 1).sum().cast(pl.Int16).alias("games_pk1"),
        (pl.col("pk_unit") == 2).sum().cast(pl.Int16).alias("games_pk2"),
        pl.col("oz_starts").sum(), pl.col("nz_starts").sum(), pl.col("dz_starts").sum(),
    ).with_columns(
        pl.when((pl.col("oz_starts") + pl.col("dz_starts")) > 0)
        .then(pl.col("oz_starts") / (pl.col("oz_starts") + pl.col("dz_starts"))).alias("oz_start_share")
    ).sort("season", "team_id", "tier_avg")


def validate(usage: pl.DataFrame) -> dict[str, float]:
    """Sanity numbers for one season's usage table.

    * ``tier_stability``: share of a skater's consecutive games (same team) in the same tier;
    * ``unit_sourced``: share of tiers taken from a confident unit;
    * ``share_5v5_sum_f`` / ``_d``: mean per team-game of Σ share_5v5 (≈ 3 forwards, ≈ 2 D on
      the ice at 5v5);
    * ``tier_sizes_ok``: share of regular team-games whose tier counts are 3/3/3/3 and 2/2/2.
    """
    played = usage.filter(pl.col("tier").is_not_null()).sort("player_id", "team_id", "game_date")
    prev = pl.col("tier").shift(1).over("player_id", "team_id")
    stab = played.select((pl.col("tier") == prev).drop_nulls().mean()).item()
    is_f = pl.col("position").is_in(FORWARDS)
    sums = played.group_by("game_id", "team_id").agg(
        pl.col("share_5v5").filter(is_f).sum().alias("f"), pl.col("share_5v5").filter(~is_f).sum().alias("d"),
        pl.col("tier").filter(is_f).value_counts().alias("fc"), pl.col("tier").filter(~is_f).value_counts().alias("dc"),
        is_f.sum().alias("nf"), (~is_f).sum().alias("nd"),
    )
    regular = sums.filter((pl.col("nf") == 12) & (pl.col("nd") == 6))
    ok = regular.select(
        ((pl.col("fc").list.len() == 4) & pl.col("fc").list.eval(pl.element().struct.field("count") == 3).list.all()
         & (pl.col("dc").list.len() == 3) & pl.col("dc").list.eval(pl.element().struct.field("count") == 2).list.all()).mean()
    ).item()
    return {
        "player_games": float(played.height),
        "tier_stability": float("nan") if stab is None else float(stab),
        "unit_sourced": float((played["tier_source"] == "unit").mean()),
        "share_5v5_sum_f": float(sums["f"].mean()),
        "share_5v5_sum_d": float(sums["d"].mean()),
        "tier_sizes_ok": float(ok),
    }


def build_season(store: Store, season: int) -> dict[str, float]:
    """Build and store ``processed/usage/{season}`` and ``processed/usage_summary/{season}``."""
    usage = build_usage(
        store.read_parquet_required(keys.player_game_logs(season)),
        store.read_parquet_required(keys.lineups(season)),
        store.read_parquet_required(keys.stints(season)),
    )
    store.put_parquet(keys.usage(season), usage)
    store.put_parquet(keys.usage_summary(season), summarize(usage))
    checks = validate(usage)
    logger.info("usage %s: %s", season, ", ".join(f"{k} {v:.3f}" for k, v in checks.items()))
    return checks
