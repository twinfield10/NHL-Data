"""Parse one game's raw play-by-play into a tidy event table.

Ported from the legacy ``ping_nhl_api`` / ``align_and_cast_columns`` with these changes:

* Score is a running count of goals *before* each event. The legacy code read the
  ``homeScore``/``awayScore`` detail fields, which the API only sets on goals, so the
  score state fed to the model was 0-0 for nearly every shot.
* Attack direction comes from ``homeTeamDefendingSide`` when present and is otherwise
  inferred from where each team's shots land (the field is missing before ~2019-20).
* Blocked shots are attributed to the shooting team via the roster; the API's
  ``eventOwnerTeamId`` for them has not been consistent across seasons.
"""

from __future__ import annotations

from typing import Any

import polars as pl

#: Goal line x-coordinate (feet from centre ice).
GOAL_X = 89.0

EVENT_TYPES: dict[str, str] = {
    "faceoff": "FACEOFF",
    "shot-on-goal": "SHOT",
    "missed-shot": "MISSED_SHOT",
    "blocked-shot": "BLOCKED_SHOT",
    "goal": "GOAL",
    "hit": "HIT",
    "giveaway": "GIVEAWAY",
    "takeaway": "TAKEAWAY",
    "penalty": "PENALTY",
    "delayed-penalty": "DELAYED_PENALTY",
    "stoppage": "STOPPAGE",
    "period-start": "PERIOD_START",
    "period-end": "PERIOD_END",
    "game-end": "GAME_END",
    "shootout-complete": "SHOOTOUT_COMPLETE",
    "failed-shot-attempt": "FAILED_SHOT",
}

UNBLOCKED_SHOTS = ("SHOT", "MISSED_SHOT", "GOAL")
SHOT_ATTEMPTS = ("SHOT", "MISSED_SHOT", "GOAL", "BLOCKED_SHOT")

# Which detail field holds player 1 / player 2 for each event type.
_PLAYER_1 = {
    "FACEOFF": "winningPlayerId",
    "HIT": "hittingPlayerId",
    "GOAL": "scoringPlayerId",
    "SHOT": "shootingPlayerId",
    "MISSED_SHOT": "shootingPlayerId",
    "BLOCKED_SHOT": "shootingPlayerId",
    "PENALTY": "committedByPlayerId",
    "GIVEAWAY": "playerId",
    "TAKEAWAY": "playerId",
}
_PLAYER_2 = {
    "FACEOFF": "losingPlayerId",
    "HIT": "hitteePlayerId",
    "GOAL": "assist1PlayerId",
    "BLOCKED_SHOT": "blockingPlayerId",
    "PENALTY": "drawnByPlayerId",
}
_PLAYER_3 = {"GOAL": "assist2PlayerId", "PENALTY": "servedByPlayerId"}


def _mmss(value: str | None) -> int | None:
    if not value:
        return None
    minutes, seconds = value.split(":")
    return int(minutes) * 60 + int(seconds)


def roster_from_pbp(pbp: dict[str, Any]) -> pl.DataFrame:
    """Players dressed for a game, from the play-by-play ``rosterSpots``.

    Returns:
        ``game_id, player_id, team_id, position, player_name``.
    """
    spots = pbp.get("rosterSpots") or []
    return pl.DataFrame(
        {
            "game_id": [pbp["id"]] * len(spots),
            "player_id": [s["playerId"] for s in spots],
            "team_id": [s["teamId"] for s in spots],
            "position": [s.get("positionCode") for s in spots],
            "player_name": [
                f"{s.get('firstName', {}).get('default', '')} {s.get('lastName', {}).get('default', '')}".strip()
                for s in spots
            ],
        },
        schema={
            "game_id": pl.Int64,
            "player_id": pl.Int64,
            "team_id": pl.Int32,
            "position": pl.Utf8,
            "player_name": pl.Utf8,
        },
    )


def _play_rows(pbp: dict[str, Any], player_team: dict[int, int]) -> list[dict[str, Any]]:
    """Flatten plays into row dicts with player roles resolved."""
    home_id = pbp["homeTeam"]["id"]
    rows = []
    for p in pbp.get("plays", []):
        d = p.get("details") or {}
        etype = EVENT_TYPES.get(p.get("typeDescKey"), (p.get("typeDescKey") or "").upper())
        pd = p.get("periodDescriptor") or {}
        owner = d.get("eventOwnerTeamId")
        p1 = d.get(_PLAYER_1.get(etype, ""), None)
        if etype == "BLOCKED_SHOT" and p1 in player_team:
            owner = player_team[p1]
        rows.append(
            {
                "event_id": p.get("eventId"),
                "event_idx": p.get("sortOrder"),
                "period": pd.get("number"),
                "period_type": pd.get("periodType"),
                "period_seconds": _mmss(p.get("timeInPeriod")),
                "situation_code": p.get("situationCode"),
                "home_defending_side": p.get("homeTeamDefendingSide"),
                "event_type": etype,
                "event_team_id": owner,
                "x": d.get("xCoord"),
                "y": d.get("yCoord"),
                "zone_code": d.get("zoneCode"),
                "shot_type": d.get("shotType"),
                "reason": d.get("reason"),
                "secondary_reason": d.get("secondaryReason"),
                "penalty_type": d.get("descKey") if etype == "PENALTY" else None,
                "penalty_minutes": d.get("duration"),
                "player_1_id": p1,
                "player_2_id": d.get(_PLAYER_2.get(etype, ""), None),
                "player_3_id": d.get(_PLAYER_3.get(etype, ""), None),
                "goalie_in_net_id": d.get("goalieInNetId"),
                "is_home_event": None if owner is None else owner == home_id,
            }
        )
    return rows


_ROW_SCHEMA = {
    "event_id": pl.Int32,
    "event_idx": pl.Int32,
    "period": pl.Int8,
    "period_type": pl.Utf8,
    "period_seconds": pl.Int32,
    "situation_code": pl.Utf8,
    "home_defending_side": pl.Utf8,
    "event_type": pl.Utf8,
    "event_team_id": pl.Int32,
    "x": pl.Float32,
    "y": pl.Float32,
    "zone_code": pl.Utf8,
    "shot_type": pl.Utf8,
    "reason": pl.Utf8,
    "secondary_reason": pl.Utf8,
    "penalty_type": pl.Utf8,
    "penalty_minutes": pl.Int16,
    "player_1_id": pl.Int64,
    "player_2_id": pl.Int64,
    "player_3_id": pl.Int64,
    "goalie_in_net_id": pl.Int64,
    "is_home_event": pl.Boolean,
}


def home_attack_direction(events: pl.DataFrame) -> pl.DataFrame:
    """Decide, per period, whether the home team attacks toward +x.

    Uses ``homeTeamDefendingSide`` when the API provides it ("left" means the home
    goalie is at -x, so home attacks +x). Otherwise each team's unblocked shots vote
    with the sign of their x coordinate (shots overwhelmingly come from the offensive
    zone); periods with no usable shots fall back to the game-level vote, flipped on
    even periods because teams change ends every period.

    Args:
        events: Output of the row parse with ``period, home_defending_side, event_type,
            is_home_event, x``.

    Returns:
        ``period, home_attacks_pos (bool), direction_source``.
    """
    periods = events.select("period").unique()
    side = (
        events.filter(pl.col("home_defending_side").is_in(["left", "right"]))
        .group_by("period")
        .agg((pl.col("home_defending_side").mode().first() == "left").alias("side_vote"))
    )
    parity = pl.when(pl.col("period") % 2 == 1).then(1).otherwise(-1)
    votes = (
        events.filter(
            pl.col("event_type").is_in(UNBLOCKED_SHOTS) & pl.col("x").is_not_null() & (pl.col("x").abs() > 25)
        )
        .with_columns(
            (pl.col("x").sign() * pl.when(pl.col("is_home_event")).then(1).otherwise(-1)).alias("vote")
        )
        .group_by("period")
        .agg(pl.col("vote").sum().alias("vote"))
    )
    _strong_conflict = (
        pl.col("vote").is_not_null() & (pl.col("vote").abs() >= 4) & ((pl.col("vote") > 0) != pl.col("side_vote"))
    ).fill_null(False)
    game_vote = votes.with_columns((pl.col("vote") * parity).alias("v")).get_column("v").sum()
    return (
        periods.join(side, on="period", how="left")
        .join(votes, on="period", how="left")
        .with_columns(
            # The API field wins unless the shots contradict it decisively (|vote| >= 4),
            # which happens in a couple of 2019-20 games where the field is simply wrong.
            pl.when(pl.col("side_vote").is_not_null() & (_strong_conflict.not_()))
            .then(pl.col("side_vote"))
            .when(pl.col("vote").is_not_null() & (pl.col("vote") != 0))
            .then(pl.col("vote") > 0)
            .otherwise(pl.lit(game_vote) * parity > 0)
            .alias("home_attacks_pos"),
            pl.when(pl.col("side_vote").is_not_null() & (_strong_conflict.not_()))
            .then(pl.lit("api"))
            .when(pl.col("vote").is_not_null() & (pl.col("vote") != 0))
            .then(pl.lit("shots"))
            .otherwise(pl.lit("parity"))
            .alias("direction_source"),
        )
        .select("period", "home_attacks_pos", "direction_source")
    )


def parse_events(pbp: dict[str, Any]) -> pl.DataFrame:
    """Turn one game's raw play-by-play into the event table (without on-ice players).

    Args:
        pbp: Raw ``/gamecenter/{id}/play-by-play`` payload.

    Returns:
        One row per event, sorted by ``event_idx``, shootout excluded, with running
        score, strength, normalized coordinates, distance and angle.
    """
    roster = roster_from_pbp(pbp)
    player_team = dict(zip(roster["player_id"].to_list(), roster["team_id"].to_list()))
    rows = _play_rows(pbp, player_team)
    home, away = pbp["homeTeam"], pbp["awayTeam"]

    df = (
        pl.DataFrame(rows, schema=_ROW_SCHEMA)
        .filter(pl.col("period_type") != "SO")
        .sort("event_idx")
        .with_columns(
            pl.lit(pbp["id"], pl.Int64).alias("game_id"),
            pl.lit(pbp["season"], pl.Int32).alias("season"),
            pl.lit("P" if pbp["gameType"] == 3 else "R").alias("season_type"),
            pl.lit(pbp["gameDate"]).str.to_date().alias("game_date"),
            pl.lit(home["id"], pl.Int32).alias("home_team_id"),
            pl.lit(away["id"], pl.Int32).alias("away_team_id"),
            pl.lit(home.get("abbrev")).alias("home_abbr"),
            pl.lit(away.get("abbrev")).alias("away_abbr"),
            pl.col("situation_code").forward_fill(),
        )
    )
    if df.is_empty():
        return df

    # Clock. Regular-season OT is 5 minutes, but OT always starts at 3600s.
    df = df.with_columns(
        ((pl.col("period").cast(pl.Int32) - 1) * 1200 + pl.col("period_seconds")).alias("game_seconds")
    )

    # Running score before each event (goals in periods 1..OT only; shootout already dropped).
    df = df.with_columns(
        ((pl.col("event_type") == "GOAL") & pl.col("is_home_event")).fill_null(False).cast(pl.Int16).alias("_hg"),
        ((pl.col("event_type") == "GOAL") & ~pl.col("is_home_event")).fill_null(False).cast(pl.Int16).alias("_ag"),
    ).with_columns(
        (pl.col("_hg").cum_sum() - pl.col("_hg")).alias("home_score"),
        (pl.col("_ag").cum_sum() - pl.col("_ag")).alias("away_score"),
    ).drop("_hg", "_ag")

    # Strength from situationCode: [away goalie][away skaters][home skaters][home goalie].
    sc = pl.col("situation_code")
    df = df.with_columns(
        (sc.str.slice(0, 1) == "0").alias("away_net_empty"),
        sc.str.slice(1, 1).cast(pl.Int8, strict=False).alias("away_skaters"),
        sc.str.slice(2, 1).cast(pl.Int8, strict=False).alias("home_skaters"),
        (sc.str.slice(3, 1) == "0").alias("home_net_empty"),
        sc.is_in(["0101", "1010"]).alias("is_penalty_shot"),
    ).with_columns(
        pl.concat_str(pl.col("home_skaters"), pl.lit("v"), pl.col("away_skaters")).alias("strength_state")
    )

    # Coordinates relative to the event team attacking toward +x.
    direction = home_attack_direction(df)
    df = (
        df.join(direction, on="period", how="left")
        .with_columns(
            pl.when(pl.col("is_home_event") == pl.col("home_attacks_pos")).then(1.0).otherwise(-1.0).alias("_flip")
        )
        .with_columns(
            (pl.col("x") * pl.col("_flip")).alias("x_abs"),
            (pl.col("y") * pl.col("_flip")).alias("y_abs"),
        )
        .drop("_flip")
        .with_columns(
            ((GOAL_X - pl.col("x_abs")).pow(2) + pl.col("y_abs").pow(2)).sqrt().alias("event_distance"),
            pl.arctan2(pl.col("y_abs").abs(), GOAL_X - pl.col("x_abs")).degrees().alias("event_angle"),
        )
        .with_columns(
            pl.when(pl.col("is_home_event")).then(pl.col("home_abbr")).otherwise(pl.col("away_abbr")).alias("event_team_abbr"),
        )
    )

    shooter_positions = roster.select(
        pl.col("player_id").alias("player_1_id"), pl.col("position").alias("player_1_position")
    )
    return df.join(shooter_positions, on="player_1_id", how="left").sort("event_idx")
