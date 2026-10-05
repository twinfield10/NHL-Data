"""Shot-level features for the expected goals model.

One code path for every game state; the legacy repo had four near-identical copies
(``split_by_strength`` / ``model_prep``). Fixes relative to the legacy features:

* the goalie attached to a shot is the *defending* goalie (``goalieInNetId``, falling back
  to the defending team's on-ice goalie). The legacy code used the shooting team's own
  goalie, so goalie-hand features described the wrong player;
* score state uses the running score (see :mod:`nhl.transform.events`);
* shot type is used as reported, with unusual types bucketed as ``other``. There is no
  imputation model, which removes the legacy leak of ``is_goal`` into imputed shot types;
* fatigue is the average elapsed shift time of each team's skaters, not time since the
  set of players last changed.
"""

from __future__ import annotations

import polars as pl

from nhl.transform.events import GOAL_X, UNBLOCKED_SHOTS

#: Events that form the "previous event" context for a shot (legacy ``xG_Events``).
CONTEXT_EVENTS = ("GOAL", "SHOT", "MISSED_SHOT", "BLOCKED_SHOT", "FACEOFF", "TAKEAWAY", "GIVEAWAY", "HIT")

SHOT_TYPES = ("wrist", "snap", "slap", "backhand", "tip-in", "deflected", "wrap-around")

STRENGTH_GROUPS = ("EV", "PP", "SH", "EN")

#: Model inputs, in a fixed order. ``strength_group`` selects the model and is not a feature.
FEATURES: list[str] = [
    # location
    "x_abs", "y_abs", "event_distance", "event_angle",
    # previous event
    "seconds_since_last", "distance_from_last", "x_abs_last", "y_abs_last",
    "event_angle_change", "event_angle_change_speed", "puck_speed_since_last",
    "prior_shot_same", "prior_miss_same", "prior_block_same",
    "prior_shot_opp", "prior_miss_opp", "prior_block_opp",
    "prior_give_same", "prior_give_opp", "prior_take_same", "prior_take_opp",
    "prior_hit_same", "prior_hit_opp", "prior_face_win", "prior_face_lose",
    "is_rebound", "is_post_miss_shot", "is_set_play", "is_rush_play", "is_fast_rush_play",
    # shot type
    *[f"shot_{t.replace('-', '_')}" for t in SHOT_TYPES], "shot_other",
    # shooter / goalie
    "shooter_is_d", "shooter_shoots_r", "off_wing", "goalie_catches_r",
    # game state
    "shooting_skaters", "defending_skaters", "own_net_empty", "score_diff",
    "strength_state_secs", "period", "game_seconds", "is_overtime", "is_home", "is_playoff",
    # fatigue
    "shooting_avg_shift_secs", "defending_avg_shift_secs", "shift_secs_diff",
]

ID_COLUMNS = [
    "season", "game_id", "game_date", "event_idx", "period_seconds", "event_type", "event_team_abbr",
    "shooter_id", "goalie_id", "strength_state", "strength_group", "is_goal",
]


def _flag(cond: pl.Expr) -> pl.Expr:
    return cond.fill_null(False).cast(pl.Int8)


def build_shot_features(events: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Turn a processed event table into one row per unblocked shot with model features.

    Args:
        events: ``processed/events/{season}.parquet`` (any number of games/seasons).
        players: ``processed/players.parquet`` (position and shoots/catches).

    Returns:
        ``ID_COLUMNS + FEATURES``. Shootouts and penalty shots are excluded; shots with
        no coordinates are dropped because location is the core of the model.
    """
    hands = players.select("player_id", "shoots_catches", pl.col("position").alias("bio_position"))
    game = ["game_id", "period"]

    ctx = (
        events.filter(
            pl.col("event_type").is_in(CONTEXT_EVENTS)
            & pl.col("x_abs").is_not_null()
            & ~pl.col("is_penalty_shot").fill_null(False)
        )
        .sort("game_id", "event_idx")
        .with_columns(
            pl.col("event_type").shift(1).over(game).alias("event_type_last"),
            pl.col("event_team_id").shift(1).over(game).alias("event_team_last"),
            pl.col("game_seconds").shift(1).over(game).alias("game_seconds_last"),
            pl.col("x_abs").shift(1).over(game).alias("_x_last_raw"),
            pl.col("y_abs").shift(1).over(game).alias("_y_last_raw"),
            pl.col("event_angle").shift(1).over(game).alias("event_angle_last"),
        )
    )

    # Seconds the current strength state has lasted (time into a power play, etc.).
    state_runs = (
        events.sort("game_id", "event_idx")
        .with_columns(
            (pl.col("situation_code") != pl.col("situation_code").shift(1).over("game_id"))
            .fill_null(True)
            .cum_sum()
            .over("game_id")
            .alias("_state_run")
        )
        .with_columns(
            (pl.col("game_seconds") - pl.col("game_seconds").min().over("game_id", "_state_run")).alias(
                "strength_state_secs"
            )
        )
        .select("game_id", "event_idx", "strength_state_secs")
    )

    shots = (
        ctx.filter(pl.col("event_type").is_in(UNBLOCKED_SHOTS) & pl.col("event_type_last").is_not_null())
        .join(state_runs, on=["game_id", "event_idx"], how="left")
        .with_columns(
            pl.col("is_home_event").fill_null(False).alias("is_home_b"),
            (pl.col("event_team_last") == pl.col("event_team_id")).fill_null(False).alias("same_team"),
        )
        .with_columns(
            # Previous event location in the current shooter's frame.
            pl.when(pl.col("same_team")).then(pl.col("_x_last_raw")).otherwise(-pl.col("_x_last_raw")).alias("x_abs_last"),
            pl.when(pl.col("same_team")).then(pl.col("_y_last_raw")).otherwise(-pl.col("_y_last_raw")).alias("y_abs_last"),
            # Same-second events get 0.5s, as in the legacy model, so speeds stay finite.
            pl.max_horizontal(pl.col("game_seconds") - pl.col("game_seconds_last"), pl.lit(0.5)).alias("seconds_since_last"),
            pl.when(pl.col("is_home_b")).then(pl.col("home_skaters")).otherwise(pl.col("away_skaters")).alias("shooting_skaters"),
            pl.when(pl.col("is_home_b")).then(pl.col("away_skaters")).otherwise(pl.col("home_skaters")).alias("defending_skaters"),
            pl.when(pl.col("is_home_b")).then(pl.col("away_net_empty")).otherwise(pl.col("home_net_empty")).alias("defending_net_empty"),
            pl.when(pl.col("is_home_b")).then(pl.col("home_net_empty")).otherwise(pl.col("away_net_empty")).alias("_own_net_empty"),
            pl.when(pl.col("is_home_b"))
            .then(pl.col("home_score") - pl.col("away_score"))
            .otherwise(pl.col("away_score") - pl.col("home_score"))
            .clip(-4, 4)
            .alias("score_diff"),
            pl.coalesce(
                "goalie_in_net_id",
                pl.when(pl.col("is_home_b")).then(pl.col("away_goalie_id")).otherwise(pl.col("home_goalie_id")),
            ).alias("goalie_id"),
            pl.when(pl.col("is_home_b")).then(pl.col("home_avg_shift_secs")).otherwise(pl.col("away_avg_shift_secs")).alias("shooting_avg_shift_secs"),
            pl.when(pl.col("is_home_b")).then(pl.col("away_avg_shift_secs")).otherwise(pl.col("home_avg_shift_secs")).alias("defending_avg_shift_secs"),
        )
        .with_columns(
            ((pl.col("x_abs") - pl.col("x_abs_last")).pow(2) + (pl.col("y_abs") - pl.col("y_abs_last")).pow(2))
            .sqrt()
            .alias("distance_from_last"),
            pl.arctan2(pl.col("y_abs_last").abs(), GOAL_X - pl.col("x_abs_last")).degrees().alias("event_angle_last"),
            (pl.col("defending_avg_shift_secs") - pl.col("shooting_avg_shift_secs")).alias("shift_secs_diff"),
        )
        .with_columns(
            (pl.col("event_angle") - pl.col("event_angle_last")).abs().alias("event_angle_change"),
            (pl.col("distance_from_last") / pl.col("seconds_since_last")).alias("puck_speed_since_last"),
        )
        .with_columns(
            (pl.col("event_angle_change") / pl.col("seconds_since_last")).alias("event_angle_change_speed"),
        )
    )

    last, same = pl.col("event_type_last"), pl.col("same_team")
    shots = shots.with_columns(
        _flag((last == "SHOT") & same).alias("prior_shot_same"),
        _flag((last == "MISSED_SHOT") & same).alias("prior_miss_same"),
        _flag((last == "BLOCKED_SHOT") & same).alias("prior_block_same"),
        _flag((last.is_in(["SHOT", "GOAL"])) & ~same).alias("prior_shot_opp"),
        _flag((last == "MISSED_SHOT") & ~same).alias("prior_miss_opp"),
        _flag((last == "BLOCKED_SHOT") & ~same).alias("prior_block_opp"),
        _flag((last == "GIVEAWAY") & same).alias("prior_give_same"),
        _flag((last == "GIVEAWAY") & ~same).alias("prior_give_opp"),
        _flag((last == "TAKEAWAY") & same).alias("prior_take_same"),
        _flag((last == "TAKEAWAY") & ~same).alias("prior_take_opp"),
        _flag((last == "HIT") & same).alias("prior_hit_same"),
        _flag((last == "HIT") & ~same).alias("prior_hit_opp"),
        _flag((last == "FACEOFF") & same).alias("prior_face_win"),
        _flag((last == "FACEOFF") & ~same).alias("prior_face_lose"),
    )

    secs = pl.col("seconds_since_last")
    turnover = (
        (pl.col("prior_give_opp") == 1) | (pl.col("prior_take_same") == 1) | (pl.col("prior_shot_opp") == 1)
        | (pl.col("prior_miss_opp") == 1) | (pl.col("prior_block_opp") == 1)
    )
    shot_type = pl.col("shot_type").fill_null("other")
    shots = shots.with_columns(
        _flag((pl.col("prior_shot_same") == 1) & (secs <= 3)).alias("is_rebound"),
        _flag(((pl.col("prior_miss_same") == 1) | (pl.col("prior_block_same") == 1)) & (secs <= 3)).alias("is_post_miss_shot"),
        _flag((pl.col("prior_face_win") == 1) & (pl.col("x_abs_last") > 25) & (secs <= 3)).alias("is_set_play"),
        _flag(turnover & (pl.col("x_abs_last") < 0) & (secs <= 5)).alias("is_rush_play"),
        _flag(turnover & (pl.col("x_abs_last") < 25) & (secs <= 3)).alias("is_fast_rush_play"),
        *[_flag(shot_type == t).alias(f"shot_{t.replace('-', '_')}") for t in SHOT_TYPES],
        _flag(~shot_type.is_in(SHOT_TYPES)).alias("shot_other"),
    )

    # Shooter and goalie attributes.
    shots = (
        shots.rename({"player_1_id": "shooter_id"})
        .join(hands.rename({"player_id": "shooter_id"}), on="shooter_id", how="left")
        .join(
            hands.select(pl.col("player_id").alias("goalie_id"), pl.col("shoots_catches").alias("goalie_catches")),
            on="goalie_id",
            how="left",
        )
        .with_columns(
            _flag(pl.coalesce("player_1_position", "bio_position") == "D").alias("shooter_is_d"),
            pl.when(pl.col("shoots_catches").is_null()).then(None).otherwise(_flag(pl.col("shoots_catches") == "R")).alias("shooter_shoots_r"),
            pl.when(pl.col("goalie_catches").is_null()).then(None).otherwise(_flag(pl.col("goalie_catches") == "R")).alias("goalie_catches_r"),
        )
        .with_columns(
            # Shooting from the off wing (right-handed shooter on the left side, facing +x).
            pl.when(pl.col("shoots_catches").is_null())
            .then(None)
            .otherwise(
                _flag(
                    (pl.col("event_angle") > 10)
                    & (((pl.col("shoots_catches") == "R") & (pl.col("y_abs") > 0)) | ((pl.col("shoots_catches") == "L") & (pl.col("y_abs") < 0)))
                )
            )
            .alias("off_wing"),
        )
    )

    # Game state and strength group from the shooting team's perspective.
    shots = shots.with_columns(
        _flag(pl.col("_own_net_empty")).alias("own_net_empty"),
        _flag(pl.col("period") >= 4).alias("is_overtime"),
        _flag(pl.col("is_home_b")).alias("is_home"),
        _flag(pl.col("season_type") == "P").alias("is_playoff"),
        _flag(pl.col("event_type") == "GOAL").alias("is_goal"),
        pl.when(pl.col("defending_net_empty").fill_null(False))
        .then(pl.lit("EN"))
        .when(pl.col("shooting_skaters") == pl.col("defending_skaters"))
        .then(pl.lit("EV"))
        .when(pl.col("shooting_skaters") > pl.col("defending_skaters"))
        .then(pl.lit("PP"))
        .otherwise(pl.lit("SH"))
        .alias("strength_group"),
    )

    return shots.select(ID_COLUMNS + FEATURES).with_columns(pl.col(FEATURES).cast(pl.Float32))
