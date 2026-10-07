"""Shift-fatigue features for the xG model (v2 feature group 5).

v1 carries each team's *average* elapsed shift time. These features target the cases an
average blurs: one exhausted defender, a player deep into an unusually long shift for
him, a double shift, heavy recent ice time, and the icing trap (a team that iced the
puck may not change lines for the ensuing defensive-zone faceoff).

All features describe the *defending* skaters except ``shooter_shift_secs``, and use
only information available at the moment of the shot. Shots in games without a shift
chart get nulls. Inputs:

* ``shots``: ``processed/shots`` rows (``game_id, event_idx, shooter_id, is_home``).
* ``events``: ``processed/events`` for the same games (period, clock, on-ice lists,
  attack direction, stoppage reasons).
* ``shifts``: ``processed/shifts`` (merged shifts). Pass the previous season too, so
  early-season shift norms have history.
* ``players``: ``processed/players`` (``player_id, position``).

The current-shift boundary rule matches :mod:`nhl.transform.shifts`: a non-faceoff
event at time ``t`` belongs to shifts with ``start < t <= end``.
"""

from __future__ import annotations

import polars as pl

FATIGUE_FEATURES: list[str] = [
    "def_max_shift_secs_d",
    "def_max_shift_secs_f",
    "def_shift_vs_norm_max",
    "def_rest_before_shift_min",
    "def_toi_last5",
    "icing_trap",
    "shooter_shift_secs",
]

#: Games of history used for a player's typical shift length, and the minimum required.
NORM_GAMES = 20
NORM_MIN_GAMES = 3
#: Trailing window (game seconds) for recent ice time.
TOI_WINDOW = 300
#: Seconds after an icing faceoff during which a shot counts as an icing trap.
ICING_WINDOW = 30
#: Faceoff dots are at |x| = 69; anything beyond the blue line (|x| > 25) is in a zone.
ZONE_X = 25


def _shift_table(shifts: pl.DataFrame) -> pl.DataFrame:
    """Skater shifts with game-clock bounds, rest before the shift and a per-game norm."""
    s = (
        shifts.filter(~pl.col("is_goalie"))
        .with_columns(
            ((pl.col("period").cast(pl.Int32) - 1) * 1200 + pl.col("start")).alias("gs_start"),
            ((pl.col("period").cast(pl.Int32) - 1) * 1200 + pl.col("end")).alias("gs_end"),
            (pl.col("end") - pl.col("start")).alias("duration"),
        )
        .sort("game_id", "player_id", "period", "start")
        .with_columns(
            # Rest since the same player's previous shift in the same period (no intermissions).
            (pl.col("start") - pl.col("end").shift(1).over("game_id", "player_id", "period")).alias("rest_before")
        )
    )
    # Typical shift length: median of the player's per-game median shift over his previous
    # NORM_GAMES games (strictly earlier games; game ids increase chronologically).
    per_game = (
        s.group_by("player_id", "game_id")
        .agg(pl.col("duration").median().alias("game_median"))
        .sort("player_id", "game_id")
        .with_columns(
            pl.col("game_median")
            .shift(1)
            .rolling_median(window_size=NORM_GAMES, min_samples=NORM_MIN_GAMES)
            .over("player_id")
            .alias("shift_norm")
        )
        .select("player_id", "game_id", "shift_norm")
    )
    return s.join(per_game, on=["player_id", "game_id"], how="left")


def _shot_context(shots: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Per-shot clock, defending skaters and team ids from the event table."""
    ev = events.select(
        "game_id", "event_idx", "period", "period_seconds", "game_seconds",
        "home_skater_ids", "away_skater_ids", "home_team_id", "away_team_id",
    )
    return (
        shots.select("game_id", "event_idx", "shooter_id", "is_home")
        .join(ev, on=["game_id", "event_idx"], how="left")
        .with_columns(
            (pl.col("is_home") == 1).alias("_shooter_home"),
        )
        .with_columns(
            pl.when(pl.col("_shooter_home")).then(pl.col("away_skater_ids")).otherwise(pl.col("home_skater_ids")).alias("def_ids"),
        )
        .select("game_id", "event_idx", "shooter_id", "_shooter_home", "period", "period_seconds", "game_seconds", "def_ids")
    )


def _icing_faceoffs(events: pl.DataFrame) -> pl.DataFrame:
    """Faceoffs that follow an icing, with the iced (defending) side and on-ice sets.

    The faceoff after an icing is in the icing team's defensive zone, so a shot against
    that team soon after, with the same skaters still on, is an icing trap.
    """
    e = (
        events.sort("game_id", "event_idx")
        .with_columns(
            pl.when(pl.col("event_type") == "STOPPAGE").then(pl.col("event_idx")).forward_fill().over("game_id").alias("_last_stop_idx"),
            pl.when(pl.col("event_type") == "STOPPAGE").then(pl.col("reason")).forward_fill().over("game_id").alias("_last_stop_reason"),
            pl.when(pl.col("event_type") == "FACEOFF").then(pl.col("event_idx")).forward_fill().over("game_id").alias("_fo_ffill"),
        )
        # Previous faceoff strictly before this row.
        .with_columns(pl.col("_fo_ffill").shift(1).over("game_id").alias("_prev_fo_idx"))
    )
    fo = e.filter(pl.col("event_type") == "FACEOFF").with_columns(
        (
            (pl.col("_last_stop_reason") == "icing")
            & (pl.col("_last_stop_idx") > pl.col("_prev_fo_idx").fill_null(-1))
        ).alias("after_icing"),
        # Home team's attack direction this period -> which team's defensive zone the dot is in.
        (pl.col("x") * pl.when(pl.col("home_attacks_pos")).then(1.0).otherwise(-1.0)).alias("_x_home"),
    )
    return fo.select(
        "game_id",
        pl.col("event_idx").alias("fo_idx"),
        pl.col("period").alias("fo_period"),
        pl.col("game_seconds").alias("fo_gs"),
        "after_icing",
        (pl.col("_x_home") < -ZONE_X).alias("in_home_dzone"),
        (pl.col("_x_home") > ZONE_X).alias("in_away_dzone"),
        pl.col("home_skater_ids").alias("fo_home_ids"),
        pl.col("away_skater_ids").alias("fo_away_ids"),
    ).sort("game_id", "fo_idx")


def add_fatigue_features(
    shots: pl.DataFrame, events: pl.DataFrame, shifts: pl.DataFrame | None, players: pl.DataFrame
) -> pl.DataFrame:
    """Add :data:`FATIGUE_FEATURES` to ``shots``.

    Args:
        shots: Shot feature rows (``game_id, event_idx, shooter_id, is_home`` needed).
        events: Event rows covering the shots' games.
        shifts: Merged shifts covering the shots' games, plus earlier games for norms.
            None or empty leaves every fatigue column null.
        players: Player table with ``player_id, position``.

    Returns:
        ``shots`` with the fatigue columns appended (Float32; null without shift data).
    """
    if shifts is None or shifts.is_empty():
        return shots.with_columns(pl.lit(None, pl.Float32).alias(c) for c in FATIGUE_FEATURES)
    st = _shift_table(shifts)
    ctx = _shot_context(shots, events)
    key = ["game_id", "event_idx"]
    pos = players.select("player_id", (pl.col("position") == "D").alias("is_d"))

    # Defending skaters' current shifts.
    defenders = ctx.select(*key, "period", "period_seconds", "game_seconds", "def_ids").explode("def_ids", empty_as_null=True).rename(
        {"def_ids": "player_id"}
    ).drop_nulls("player_id")  # explode keeps the default empty-list behaviour; nulls dropped here
    cur = (
        defenders.join(st, on=["game_id", "player_id", "period"], how="inner")
        .filter((pl.col("start") < pl.col("period_seconds")) & (pl.col("period_seconds") <= pl.col("end")))
        .with_columns((pl.col("period_seconds") - pl.col("start")).alias("elapsed"))
        .join(pos, on="player_id", how="left")
        .group_by(*key, "player_id")
        .agg(pl.col("elapsed").max(), pl.col("rest_before").first(), pl.col("shift_norm").first(), pl.col("is_d").first())
    )
    agg_cur = cur.group_by(key).agg(
        pl.col("elapsed").filter(pl.col("is_d")).max().alias("def_max_shift_secs_d"),
        pl.col("elapsed").filter(~pl.col("is_d").fill_null(False)).max().alias("def_max_shift_secs_f"),
        (pl.col("elapsed") / pl.col("shift_norm")).max().alias("def_shift_vs_norm_max"),
        pl.col("rest_before").min().alias("def_rest_before_shift_min"),
    )

    # Defending skaters' ice time in the trailing window (game seconds), across shifts.
    toi = (
        defenders.join(st.select("game_id", "player_id", "gs_start", "gs_end"), on=["game_id", "player_id"], how="inner")
        .filter((pl.col("gs_end") > pl.col("game_seconds") - TOI_WINDOW) & (pl.col("gs_start") < pl.col("game_seconds")))
        .with_columns(
            (
                pl.min_horizontal("gs_end", "game_seconds")
                - pl.max_horizontal("gs_start", pl.col("game_seconds") - TOI_WINDOW)
            ).clip(lower_bound=0).alias("overlap")
        )
        .group_by(*key, "player_id")
        # Shift charts occasionally contain overlapping rows for one player; a player can't
        # be on the ice more than the whole window.
        .agg(pl.col("overlap").sum().clip(upper_bound=TOI_WINDOW))
        .group_by(key)
        .agg(pl.col("overlap").max().alias("def_toi_last5"))
    )

    # The shooter's own current shift.
    shooter = (
        ctx.select(*key, pl.col("shooter_id").alias("player_id"), "period", "period_seconds")
        .join(st.select("game_id", "player_id", "period", "start", "end"), on=["game_id", "player_id", "period"], how="inner")
        .filter((pl.col("start") < pl.col("period_seconds")) & (pl.col("period_seconds") <= pl.col("end")))
        .group_by(key)
        .agg((pl.col("period_seconds") - pl.col("start")).max().alias("shooter_shift_secs"))
    )

    # Icing trap: the most recent faceoff before the shot followed an icing, sits in the
    # defending team's zone, was <= ICING_WINDOW s ago, and the defenders haven't changed.
    fo = _icing_faceoffs(events)
    trap = (
        ctx.select(*key, "period", "game_seconds", "_shooter_home", "def_ids")
        .sort("game_id", "event_idx")
        .join_asof(fo, left_on="event_idx", right_on="fo_idx", by="game_id", strategy="backward", check_sortedness=False)
        .with_columns(
            pl.when(pl.col("_shooter_home")).then(pl.col("in_away_dzone")).otherwise(pl.col("in_home_dzone")).alias("_in_def_zone"),
            pl.when(pl.col("_shooter_home")).then(pl.col("fo_away_ids")).otherwise(pl.col("fo_home_ids")).alias("_fo_def_ids"),
        )
        .with_columns(
            pl.when(pl.col("def_ids").is_null() | pl.col("_fo_def_ids").is_null())
            .then(None)
            .otherwise(
                (
                    pl.col("after_icing").fill_null(False)
                    & pl.col("_in_def_zone").fill_null(False)
                    & (pl.col("fo_period") == pl.col("period"))
                    & ((pl.col("game_seconds") - pl.col("fo_gs")) <= ICING_WINDOW)
                    & (pl.col("def_ids").list.sort() == pl.col("_fo_def_ids").list.sort())
                ).fill_null(False).cast(pl.Float32)
            )
            .alias("icing_trap")
        )
        .select(*key, "icing_trap")
    )

    out = (
        shots.join(agg_cur, on=key, how="left")
        .join(toi, on=key, how="left")
        .join(shooter, on=key, how="left")
        .join(trap, on=key, how="left")
    )
    return out.with_columns(pl.col(FATIGUE_FEATURES).cast(pl.Float32))
