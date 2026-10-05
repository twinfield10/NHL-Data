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


#: Event-type flags for the 2nd and 3rd previous context events, relative to the shooter.
SEQ_FLAGS = ("own_attempt", "opp_attempt", "faceoff_won", "faceoff_lost", "turnover_for", "turnover_against", "hit")

#: v2 feature groups 1-4 (docs/plans/m1-xg-features.md). Fatigue (group 5) lives in
#: :mod:`nhl.features.fatigue`.
FEATURES_V2_EVENTS: list[str] = [
    # 1. cross-ice movement
    "lateral_ft", "lateral_speed", "crossed_slot", "crossed_slot_same_team",
    # 3. net geometry
    "net_angle_deg", "dist_near_post", "behind_net", "in_slot",
    # 2. sequences
    *[f"prev{k}_{f}" for k in (2, 3) for f in (*SEQ_FLAGS, "secs", "x_abs", "y_abs")],
    "secs_since_faceoff", "faceoff_zone_oz", "attempts_since_faceoff_for", "attempts_since_faceoff_against",
    "attempts_for_10s", "attempts_for_30s",
    # 4. goalie workload
    "goalie_sog_10s", "goalie_sog_60s", "goalie_sog_120s", "goalie_att_10s", "goalie_att_60s",
    "goalie_sog_game", "goalie_secs_since_save", "goalie_mins_in_game",
]

#: Net posts sit at y = +/-3 ft on the goal line.
POST_Y = 3.0
#: Composite-key scale: ``game_id * KEY_SCALE + event_idx`` (or game seconds) sorts globally.
KEY_SCALE = 1_000_000


def _flag(cond: pl.Expr) -> pl.Expr:
    return cond.fill_null(False).cast(pl.Int8)


def build_shot_features(
    events: pl.DataFrame, players: pl.DataFrame, shifts: pl.DataFrame | None = None
) -> pl.DataFrame:
    """Turn a processed event table into one row per unblocked shot with model features.

    Args:
        events: ``processed/events/{season}.parquet`` (any number of games/seasons).
        players: ``processed/players.parquet`` (position and shoots/catches).
        shifts: Merged shifts (``processed/shifts/{season}``) for the fatigue group;
            None leaves the fatigue columns null.

    Returns:
        ``ID_COLUMNS + FEATURE_SETS["v2"]`` plus ``same_team_last``, every feature
        Float32. Shootouts and penalty shots are excluded; shots with no coordinates are
        dropped because location is the core of the model.
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
            *[
                pl.col(c).shift(k).over(game).alias(f"_p{k}_{c}")
                for k in (2, 3)
                for c in ("event_type", "event_team_id", "game_seconds", "x_abs", "y_abs")
            ],
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

    shots = shots.with_columns(_flag(pl.col("same_team")).alias("same_team_last"))
    shots = add_geometry_features(shots)
    shots = _add_sequence_features(shots)
    shots = _add_faceoff_features(shots, events)
    shots = _add_rolling_features(shots, events)
    shots = _apply_fatigue(shots, events, shifts, players)

    features = feature_set("v2")
    return shots.select(ID_COLUMNS + features + ["same_team_last"]).with_columns(
        pl.col(features + ["same_team_last"]).cast(pl.Float32)
    )


# --------------------------------------------------------------------------- v2 helpers
def add_geometry_features(df: pl.DataFrame) -> pl.DataFrame:
    """(Re)compute the location-derived v2 features from a shots table.

    Needs only ``x_abs, y_abs, x_abs_last, y_abs_last, seconds_since_last,
    same_team_last``, so it can be re-run after shot locations are adjusted (e.g. the
    arena scorer-bias correction in :mod:`nhl.features.rink`).

    Returns:
        ``df`` with ``net_angle_deg, dist_near_post, behind_net, in_slot, lateral_ft,
        lateral_speed, crossed_slot, crossed_slot_same_team`` (Float32).
    """
    x, y = pl.col("x_abs"), pl.col("y_abs")
    xl, yl = pl.col("x_abs_last"), pl.col("y_abs_last")
    depth = GOAL_X - x
    lateral = (y - yl).abs()
    crossed = (
        (pl.col("seconds_since_last") <= 3)
        & (y.sign() != yl.sign())
        & (y.abs() >= 3)
        & (yl.abs() >= 3)
        & (xl > 25)
    ).fill_null(False)
    slot = (
        (x >= 54) & (x <= GOAL_X) & (y.abs() <= 22)
        & ((x < 69) | (y.abs() <= POST_Y + (GOAL_X - x) * 19 / 20))
    ).fill_null(False)
    return df.with_columns(
        pl.when(x >= GOAL_X)
        .then(0.0)
        .otherwise((pl.arctan2(POST_Y - y, depth) - pl.arctan2(-POST_Y - y, depth)).abs().degrees())
        .cast(pl.Float32)
        .alias("net_angle_deg"),
        (depth.pow(2) + (y.abs() - POST_Y).pow(2)).sqrt().cast(pl.Float32).alias("dist_near_post"),
        (x > GOAL_X).fill_null(False).cast(pl.Float32).alias("behind_net"),
        slot.cast(pl.Float32).alias("in_slot"),
        lateral.cast(pl.Float32).alias("lateral_ft"),
        (lateral / pl.col("seconds_since_last")).cast(pl.Float32).alias("lateral_speed"),
        crossed.cast(pl.Float32).alias("crossed_slot"),
        (crossed & (pl.col("same_team_last") == 1)).fill_null(False).cast(pl.Float32).alias("crossed_slot_same_team"),
    )


def _add_sequence_features(shots: pl.DataFrame) -> pl.DataFrame:
    """Type flags, timing and shooter-frame location of the 2nd and 3rd previous events."""
    shooter = pl.col("event_team_id")
    attempts = ["SHOT", "MISSED_SHOT", "GOAL", "BLOCKED_SHOT"]
    out = []
    for k in (2, 3):
        etype, team = pl.col(f"_p{k}_event_type"), pl.col(f"_p{k}_event_team_id")
        same = team == shooter
        exists = etype.is_not_null()
        flip = pl.when(same).then(1.0).otherwise(-1.0)
        out += [
            _flag(etype.is_in(attempts) & same).alias(f"prev{k}_own_attempt"),
            _flag(etype.is_in(attempts) & ~same).alias(f"prev{k}_opp_attempt"),
            _flag((etype == "FACEOFF") & same).alias(f"prev{k}_faceoff_won"),
            _flag((etype == "FACEOFF") & ~same).alias(f"prev{k}_faceoff_lost"),
            _flag(((etype == "TAKEAWAY") & same) | ((etype == "GIVEAWAY") & ~same)).alias(f"prev{k}_turnover_for"),
            _flag(((etype == "GIVEAWAY") & same) | ((etype == "TAKEAWAY") & ~same)).alias(f"prev{k}_turnover_against"),
            _flag(etype == "HIT").alias(f"prev{k}_hit"),
            pl.when(exists).then(pl.col("game_seconds") - pl.col(f"_p{k}_game_seconds")).alias(f"prev{k}_secs"),
            pl.when(exists).then(pl.col(f"_p{k}_x_abs") * flip).alias(f"prev{k}_x_abs"),
            pl.when(exists).then(pl.col(f"_p{k}_y_abs") * flip).alias(f"prev{k}_y_abs"),
        ]
    return shots.with_columns(out)


def _ord_key(idx_col: str = "event_idx") -> pl.Expr:
    """Globally sortable key: game, then event order (or game seconds)."""
    return pl.col("game_id").cast(pl.Int64) * KEY_SCALE + pl.col(idx_col).cast(pl.Int64)


def _count_before(
    left: pl.DataFrame, stream: pl.DataFrame, by: list[str], left_key: str, stream_key: str, name: str
) -> pl.DataFrame:
    """Add ``name`` = number of ``stream`` rows in the same ``by`` group with key < left key.

    Both keys must be built with :func:`_ord_key` (they embed the game), so groups never
    leak across games. ``left`` must carry a ``_row`` index; the result is re-sorted by it.
    """
    s = (
        stream.select(*by, pl.col(stream_key).alias("_k"))
        .sort("_k")
        .with_columns(pl.int_range(1, pl.len() + 1).over(by).alias(name))
    )
    joined = (
        left.sort(left_key)
        .join_asof(s, left_on=left_key, right_on="_k", by=by, strategy="backward", allow_exact_matches=False, check_sortedness=False)
        .drop("_k")
        .with_columns(pl.col(name).fill_null(0))
        .sort("_row")
    )
    return joined


def _add_faceoff_features(shots: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Time since the period's last faceoff, its zone, and attempts by each side since."""
    ev = events.filter(~pl.col("is_penalty_shot").fill_null(False)).with_columns(_ord_key().alias("_ord"))
    fo = (
        ev.filter(pl.col("event_type") == "FACEOFF")
        .select("game_id", "period", "_ord",
                pl.col("game_seconds").alias("_fo_t"), pl.col("event_team_id").alias("_fo_team"),
                pl.col("x_abs").alias("_fo_x"))
        .sort("_ord")
    )
    left = shots.with_row_index("_row").with_columns(_ord_key().alias("_ord"))
    left = (
        left.sort("_ord")
        .join_asof(fo.rename({"_ord": "_fo_ord"}), left_on="_ord", right_on="_fo_ord", by=["game_id", "period"],
                   strategy="backward", allow_exact_matches=False, check_sortedness=False)
        .sort("_row")
    )
    attempts = ev.filter(pl.col("event_type").is_in(["SHOT", "MISSED_SHOT", "GOAL", "BLOCKED_SHOT"])).select(
        "game_id", "period", pl.col("event_team_id").alias("_team"), "_ord"
    )
    opp = pl.when(pl.col("event_team_id") == pl.col("home_team_id")).then(pl.col("away_team_id")).otherwise(pl.col("home_team_id"))
    left = left.with_columns(pl.col("event_team_id").alias("_team")).with_columns(pl.col("_fo_ord").fill_null(0).alias("_fo_ord_k"))
    left = _count_before(left, attempts, ["game_id", "period", "_team"], "_ord", "_ord", "_for_now")
    left = _count_before(left, attempts, ["game_id", "period", "_team"], "_fo_ord_k", "_ord", "_for_fo")
    left = left.with_columns(opp.alias("_team"))
    left = _count_before(left, attempts, ["game_id", "period", "_team"], "_ord", "_ord", "_ag_now")
    left = _count_before(left, attempts, ["game_id", "period", "_team"], "_fo_ord_k", "_ord", "_ag_fo")
    has_fo = pl.col("_fo_ord").is_not_null()
    fo_frame = pl.when(pl.col("_fo_team") == pl.col("event_team_id")).then(pl.col("_fo_x")).otherwise(-pl.col("_fo_x"))
    return left.with_columns(
        pl.when(has_fo).then(pl.col("game_seconds") - pl.col("_fo_t")).alias("secs_since_faceoff"),
        pl.when(has_fo).then((fo_frame > 25).fill_null(False).cast(pl.Float32)).alias("faceoff_zone_oz"),
        pl.when(has_fo).then(pl.col("_for_now") - pl.col("_for_fo")).alias("attempts_since_faceoff_for"),
        pl.when(has_fo).then(pl.col("_ag_now") - pl.col("_ag_fo")).alias("attempts_since_faceoff_against"),
    ).drop("_row", "_ord", "_fo_ord", "_fo_ord_k", "_fo_t", "_fo_team", "_fo_x", "_team",
           "_for_now", "_for_fo", "_ag_now", "_ag_fo")


def _window_count(left: pl.DataFrame, stream: pl.DataFrame, by: list[str], window: int, name: str) -> pl.DataFrame:
    """Rows of ``stream`` in the ``by`` group strictly before each shot (by event order)
    and no more than ``window`` seconds earlier (same-second events count if earlier)."""
    left = _count_before(left, stream, by, "_ord", "_ord", "_c_idx")
    left = left.with_columns((pl.col("_tkey") - window).alias("_tkey_w"))
    left = _count_before(left, stream, by, "_tkey_w", "_tkey", "_c_old")
    return left.with_columns((pl.col("_c_idx") - pl.col("_c_old")).clip(0).alias(name)).drop("_c_idx", "_c_old", "_tkey_w")


def _add_rolling_features(shots: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Shooting-team attempt rates and the defending goalie's rolling workload."""
    ev = (
        events.filter(~pl.col("is_penalty_shot").fill_null(False))
        .with_columns(_ord_key().alias("_ord"), _ord_key("game_seconds").alias("_tkey"))
    )
    attempts = ev.filter(pl.col("event_type").is_in(["SHOT", "MISSED_SHOT", "GOAL", "BLOCKED_SHOT"])).select(
        "game_id", "period", pl.col("event_team_id").alias("_team"), "_ord", "_tkey"
    )
    home = pl.col("is_home_event").fill_null(False)
    faced = (
        ev.filter(pl.col("event_type").is_in(list(UNBLOCKED_SHOTS)))
        .with_columns(
            pl.coalesce(
                "goalie_in_net_id",
                pl.when(home).then(pl.col("away_goalie_id")).otherwise(pl.col("home_goalie_id")),
            ).alias("_goalie")
        )
        .filter(pl.col("_goalie").is_not_null())
        .select("game_id", "period", "_goalie", "_ord", "_tkey", "event_type", "game_seconds")
    )
    sog = faced.filter(pl.col("event_type").is_in(["SHOT", "GOAL"]))
    saves = faced.filter(pl.col("event_type") == "SHOT")

    left = shots.with_row_index("_row").with_columns(
        _ord_key().alias("_ord"), _ord_key("game_seconds").alias("_tkey"),
        pl.col("event_team_id").alias("_team"), pl.col("goalie_id").alias("_goalie"),
    )
    team_by = ["game_id", "period", "_team"]
    goalie_by = ["game_id", "period", "_goalie"]
    left = _window_count(left, attempts, team_by, 10, "attempts_for_10s")
    left = _window_count(left, attempts, team_by, 30, "attempts_for_30s")
    for w in (10, 60, 120):
        left = _window_count(left, sog, goalie_by, w, f"goalie_sog_{w}s")
    for w in (10, 60):
        left = _window_count(left, faced, goalie_by, w, f"goalie_att_{w}s")
    left = _count_before(left, sog, ["game_id", "_goalie"], "_ord", "_ord", "goalie_sog_game")

    last_save = saves.select("game_id", "_goalie", pl.col("_ord").alias("_sv_ord"), pl.col("game_seconds").alias("_sv_t")).sort("_sv_ord")
    left = (
        left.sort("_ord")
        .join_asof(last_save, left_on="_ord", right_on="_sv_ord", by=["game_id", "_goalie"],
                   strategy="backward", allow_exact_matches=False, check_sortedness=False)
        .sort("_row")
    )
    # First time this goalie appears in this game (on-ice goalie columns, else in-net ids).
    appearances = pl.concat([
        events.select("game_id", pl.col(c).alias("_goalie"), "game_seconds")
        for c in ("home_goalie_id", "away_goalie_id", "goalie_in_net_id") if c in events.columns
    ]).drop_nulls().group_by("game_id", "_goalie").agg(pl.col("game_seconds").min().alias("_g_first"))
    no_goalie = pl.col("_goalie").is_null()
    left = left.join(appearances, on=["game_id", "_goalie"], how="left").with_columns(
        (pl.col("game_seconds") - pl.col("_sv_t")).alias("goalie_secs_since_save"),
        ((pl.col("game_seconds") - pl.col("_g_first")).clip(0) / 60).alias("goalie_mins_in_game"),
        *[
            pl.when(no_goalie).then(None).otherwise(pl.col(c)).alias(c)
            for c in ("goalie_sog_10s", "goalie_sog_60s", "goalie_sog_120s", "goalie_att_10s", "goalie_att_60s", "goalie_sog_game")
        ],
    )
    return left.sort("_row").drop("_row", "_ord", "_tkey", "_team", "_goalie", "_sv_ord", "_sv_t", "_g_first")


def _apply_fatigue(
    shots: pl.DataFrame, events: pl.DataFrame, shifts: pl.DataFrame | None, players: pl.DataFrame
) -> pl.DataFrame:
    """Add the fatigue group (v2 group 5) if :mod:`nhl.features.fatigue` is installed."""
    import importlib

    try:
        fatigue = importlib.import_module("nhl.features.fatigue")
    except ModuleNotFoundError:
        return shots
    return fatigue.add_fatigue_features(shots, events, shifts, players)


try:  # Group 5 lives in its own module; imported last so it may import from this one.
    from nhl.features.fatigue import FATIGUE_FEATURES
except ModuleNotFoundError:  # pragma: no cover - only while fatigue.py doesn't exist yet
    FATIGUE_FEATURES: list[str] = []

#: Feature sets by version. Models record the list they were trained on in metadata.
FEATURE_SETS: dict[str, list[str]] = {
    "v1": FEATURES,
    "v2": FEATURES + FEATURES_V2_EVENTS + list(FATIGUE_FEATURES),
}


def feature_set(name: str) -> list[str]:
    """Columns of a feature set (``"v1"`` or ``"v2"``)."""
    return FEATURE_SETS[name]
