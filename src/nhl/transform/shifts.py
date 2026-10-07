"""Attach on-ice players and shift fatigue to events using the shift charts.

Replaces the legacy ``append_shift_data`` (a per-event ``map_elements`` loop) with a
vectorized interval join. Boundary convention when an event happens on the same
second as a line change:

* faceoffs belong to the players coming **on** (``start <= t < end``);
* every other event belongs to the players going **off** (``start < t <= end``).
"""

from __future__ import annotations

from typing import Any

import polars as pl

_ON_ICE_SCHEMA = {
    "event_idx": pl.Int32,
    "home_skater_ids": pl.List(pl.Int64),
    "away_skater_ids": pl.List(pl.Int64),
    "home_goalie_id": pl.Int64,
    "away_goalie_id": pl.Int64,
    "home_skaters_on": pl.UInt32,
    "away_skaters_on": pl.UInt32,
    "home_avg_shift_secs": pl.Float64,
    "away_avg_shift_secs": pl.Float64,
}


def parse_shifts(raw: dict[str, Any], roster: pl.DataFrame) -> pl.DataFrame:
    """Normalize a raw shift chart and merge back-to-back shift fragments.

    Args:
        raw: ``/shiftcharts`` payload.
        roster: Game roster from :func:`nhl.transform.events.roster_from_pbp`.

    Returns:
        ``player_id, team_id, period, start, end, is_goalie`` with ``start``/``end`` in
        seconds into the period. Empty if the chart is missing.
    """
    # A handful of 2019-20 charts carry shift rows with an empty start/end time; drop those
    # rows rather than the game.
    rows = [
        r for r in raw.get("data") or []
        if r.get("typeCode") == 517 and r.get("duration")
        and ":" in (r.get("startTime") or "") and ":" in (r.get("endTime") or "")
    ]
    if not rows:
        return pl.DataFrame(
            schema={"player_id": pl.Int64, "team_id": pl.Int32, "period": pl.Int8,
                    "start": pl.Int32, "end": pl.Int32, "is_goalie": pl.Boolean}
        )

    def secs(v: str) -> int:
        m, s = v.split(":")
        return int(m) * 60 + int(s)

    shifts = pl.DataFrame(
        {
            "player_id": [r["playerId"] for r in rows],
            "team_id": [r["teamId"] for r in rows],
            "period": [r["period"] for r in rows],
            "start": [secs(r["startTime"]) for r in rows],
            "end": [secs(r["endTime"]) for r in rows],
        },
        schema={"player_id": pl.Int64, "team_id": pl.Int32, "period": pl.Int8, "start": pl.Int32, "end": pl.Int32},
    ).filter(pl.col("end") > pl.col("start"))

    # Keep only players dressed for this game, on the team they dressed for: at least one
    # API chart (2021020513) also carries another game's shifts under this game id.
    shifts = shifts.join(roster.select("player_id", "team_id"), on=["player_id", "team_id"], how="semi")
    goalies = roster.filter(pl.col("position") == "G").select("player_id", pl.lit(True).alias("is_goalie"))
    shifts = shifts.join(goalies, on="player_id", how="left").with_columns(pl.col("is_goalie").fill_null(False))

    # Merge fragments where a player's next shift starts the second the last one ended,
    # so elapsed shift time reflects the real shift.
    return (
        shifts.unique()
        .sort("player_id", "period", "start")
        .with_columns(
            (pl.col("start") > pl.col("end").shift(1).over("player_id", "period"))
            .fill_null(True)
            .cum_sum()
            .over("player_id", "period")
            .alias("_block")
        )
        .group_by("player_id", "team_id", "period", "is_goalie", "_block")
        .agg(pl.col("start").min(), pl.col("end").max())
        .drop("_block")
    )


def on_ice(events: pl.DataFrame, shifts: pl.DataFrame) -> pl.DataFrame:
    """Compute who was on the ice for every event.

    Args:
        events: One game's events with ``event_idx, period, period_seconds, event_type,
            home_team_id``.
        shifts: Output of :func:`parse_shifts`.

    Returns:
        One row per ``event_idx`` with skater id lists, goalie ids, skater counts and
        the average elapsed shift time of each team's skaters. Empty (correct schema)
        when the game has no shift chart.
    """
    if shifts.is_empty() or events.is_empty():
        return pl.DataFrame(schema=_ON_ICE_SCHEMA)

    home_id = events["home_team_id"][0]
    ev = events.select("event_idx", "period", "period_seconds", (pl.col("event_type") == "FACEOFF").alias("is_fo"))
    t, s, e = pl.col("period_seconds"), pl.col("start"), pl.col("end")
    joined = (
        ev.join(shifts, on="period", how="inner")
        .filter(pl.when(pl.col("is_fo")).then((s <= t) & (t < e)).otherwise((s < t) & (t <= e)))
        .with_columns(
            (pl.col("team_id") == home_id).alias("is_home"),
            (t - s).alias("elapsed"),
        )
    )

    def side(is_home: bool, prefix: str) -> pl.DataFrame:
        part = joined.filter(pl.col("is_home") == is_home)
        skaters = (
            part.filter(~pl.col("is_goalie"))
            .group_by("event_idx")
            .agg(
                pl.col("player_id").sort().alias(f"{prefix}_skater_ids"),
                pl.len().alias(f"{prefix}_skaters_on"),
                pl.col("elapsed").mean().alias(f"{prefix}_avg_shift_secs"),
            )
        )
        goalie = (
            part.filter(pl.col("is_goalie"))
            .group_by("event_idx")
            .agg(pl.col("player_id").sort_by("elapsed", descending=True).first().alias(f"{prefix}_goalie_id"))
        )
        return skaters.join(goalie, on="event_idx", how="full", coalesce=True)

    out = (
        events.select("event_idx")
        .join(side(True, "home"), on="event_idx", how="left")
        .join(side(False, "away"), on="event_idx", how="left")
    )
    return out.select(list(_ON_ICE_SCHEMA)).cast(_ON_ICE_SCHEMA)
