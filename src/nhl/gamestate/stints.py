"""Stints: maximal intervals of constant on-ice personnel, the rows M3 ratings are fit on.

A stint is an interval within a period where both teams' skater sets and goalies are
unchanged. Stints are additionally cut at every **faceoff** and **goal**, so each stint
has one score state and one "most recent faceoff". M3 splits a stint's duration into
seconds 0-34 after that faceoff for its decaying zone-start terms. Strength comes from
the shift charts (skaters on ice), not ``situationCode``, which only updates on events.

Event attribution follows the on-ice convention of :mod:`nhl.transform.shifts`:

* faceoffs belong to the stint starting at that second (``start <= t < end``);
* every other event belongs to the stint ending at it (``start < t <= end``).

Since every shift start/end is a stint boundary, a stint's personnel always equals the
on-ice lists of the events attributed to it.

Everything is vectorized over a whole season.
"""

from __future__ import annotations

import polars as pl

from nhl.transform.events import SHOT_ATTEMPTS, UNBLOCKED_SHOTS

PERIOD_SECONDS = 1200
#: Seconds after a faceoff that M3's zone-start terms cover (Magnus 9 uses 35).
ZONE_WINDOW_S = 35

_GAME_KEYS = ["game_id", "period"]
_FLIP_ZONE = {"O": "D", "D": "O", "N": "N"}
_ZONE_NAMES = {"O": "oz", "N": "nz", "D": "dz"}

#: Per-side event counts carried on each stint (``home_<name>`` / ``away_<name>``).
COUNT_COLUMNS = ("cf", "ff", "sf", "gf", "xgf", "pen_taken", "pim", "fo_won")


def period_bounds(events: pl.DataFrame, shifts: pl.DataFrame) -> pl.DataFrame:
    """End second of every played period.

    The latest of the ``PERIOD_END`` event, the last event and the last shift end, capped at
    20:00. ``PERIOD_END`` alone can be a second early (a 2012-13 shot is logged at 20:00 of
    a period "ending" at 19:59). Shootouts are never included (they're not in ``events``).

    Returns:
        ``game_id, period, period_end`` (seconds into the period).
    """
    ev = events.filter(pl.col("period_type") != "SO")
    marked = (
        ev.filter(pl.col("event_type") == "PERIOD_END")
        .group_by(_GAME_KEYS)
        .agg(pl.col("period_seconds").max().alias("_marked"))
    )
    last_event = ev.group_by(_GAME_KEYS).agg(pl.col("period_seconds").max().alias("_last_event"))
    last_shift = shifts.group_by(_GAME_KEYS).agg(pl.col("end").max().alias("_last_shift"))
    return (
        last_event.join(marked, on=_GAME_KEYS, how="left")
        .join(last_shift, on=_GAME_KEYS, how="left")
        .with_columns(
            pl.min_horizontal(pl.max_horizontal("_marked", "_last_event", "_last_shift"), pl.lit(PERIOD_SECONDS))
            .cast(pl.Int32)
            .alias("period_end")
        )
        .select(*_GAME_KEYS, "period_end")
        .filter(pl.col("period_end") > 0)
    )


def _intervals(events: pl.DataFrame, shifts: pl.DataFrame, bounds: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Elementary intervals between consecutive boundaries, and the shifts clipped to periods.

    Boundaries are every shift start/end, period start/end, faceoff and goal second.
    ``hard`` marks a boundary where a new stint must start even if personnel is unchanged
    (faceoffs, goals, period start).
    """
    t = pl.col("t")
    clipped = (
        shifts.join(bounds, on=_GAME_KEYS, how="inner")
        .with_columns(pl.col("end").clip(upper_bound=pl.col("period_end")), pl.col("start").clip(lower_bound=0))
        .filter(pl.col("end") > pl.col("start"))
    )
    marks = events.filter(pl.col("event_type").is_in(["FACEOFF", "GOAL"])).select(
        *_GAME_KEYS, pl.col("period_seconds").alias("t"), pl.lit(True).alias("hard")
    )
    soft = pl.lit(False).alias("hard")
    points = (
        pl.concat(
            [
                clipped.select(*_GAME_KEYS, pl.col("start").alias("t"), soft),
                clipped.select(*_GAME_KEYS, pl.col("end").alias("t"), soft),
                bounds.select(*_GAME_KEYS, pl.lit(0, pl.Int32).alias("t"), pl.lit(True).alias("hard")),
                bounds.select(*_GAME_KEYS, pl.col("period_end").alias("t"), soft),
                marks,
            ],
            how="vertical_relaxed",
        )
        .join(bounds, on=_GAME_KEYS, how="inner")
        .filter((t >= 0) & (t <= pl.col("period_end")))
        .group_by(*_GAME_KEYS, "t")
        .agg(pl.col("hard").any())
        .sort(*_GAME_KEYS, "t")
        .with_columns(
            pl.int_range(pl.len()).over(_GAME_KEYS).alias("iv"),
            t.shift(-1).over(_GAME_KEYS).alias("t_end"),
        )
    )
    return points, clipped


def _on_ice_by_interval(points: pl.DataFrame, clipped: pl.DataFrame, home_ids: pl.DataFrame) -> pl.DataFrame:
    """Skater lists and goalies for every elementary interval."""
    idx = points.select(*_GAME_KEYS, "t", "iv")
    covered = (
        clipped.join(idx.rename({"t": "start", "iv": "iv_start"}), on=[*_GAME_KEYS, "start"], how="inner")
        .join(idx.rename({"t": "end", "iv": "iv_end"}), on=[*_GAME_KEYS, "end"], how="inner")
        .join(home_ids, on="game_id", how="inner")
        .with_columns(
            pl.int_ranges("iv_start", "iv_end").alias("iv"),
            (pl.col("team_id") == pl.col("home_team_id")).alias("is_home"),
        )
        .explode("iv", empty_as_null=True)
        .select(*_GAME_KEYS, "iv", "player_id", "is_home", "is_goalie")
    )

    def side(is_home: bool, prefix: str) -> pl.DataFrame:
        part = covered.filter(pl.col("is_home") == is_home)
        skaters = part.filter(~pl.col("is_goalie")).group_by(*_GAME_KEYS, "iv").agg(
            pl.col("player_id").sort().alias(f"{prefix}_skaters")
        )
        # Two goalies on at once is a shift-chart error at a goalie change; keep one.
        goalie = part.filter(pl.col("is_goalie")).group_by(*_GAME_KEYS, "iv").agg(
            pl.col("player_id").max().alias(f"{prefix}_goalie")
        )
        return skaters.join(goalie, on=[*_GAME_KEYS, "iv"], how="full", coalesce=True)

    empty = pl.lit([], pl.List(pl.Int64))
    return (
        points.filter(pl.col("t_end").is_not_null())
        .join(side(True, "home"), on=[*_GAME_KEYS, "iv"], how="left")
        .join(side(False, "away"), on=[*_GAME_KEYS, "iv"], how="left")
        .with_columns(
            pl.col("home_skaters").fill_null(empty),
            pl.col("away_skaters").fill_null(empty),
        )
    )


def _merge_intervals(intervals: pl.DataFrame) -> pl.DataFrame:
    """Collapse consecutive intervals with identical personnel into stints."""
    state = ["home_skaters", "away_skaters", "home_goalie", "away_goalie"]
    changed = pl.any_horizontal(
        *[pl.col(c).ne_missing(pl.col(c).shift(1).over(_GAME_KEYS)) for c in state]
    )
    return (
        intervals.sort(*_GAME_KEYS, "t")
        .with_columns((pl.col("hard") | changed).fill_null(True).alias("_new"))
        .with_columns(pl.col("_new").cum_sum().over(_GAME_KEYS).alias("_seg"))
        .group_by(*_GAME_KEYS, "_seg")
        .agg(
            pl.col("t").min().alias("start"),
            pl.col("t_end").max().alias("end"),
            *[pl.col(c).first() for c in state],
        )
        .drop("_seg")
    )


def attribute_events(stints: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Attach ``stint_id`` to events of a built stint table (faceoff / everything-else rule).

    Args:
        stints: Output of :func:`build_stints`.
        events: Processed events for the same games.

    Returns:
        The events that fall in a stint, with ``stint_id``.
    """
    offset = (pl.col("period").cast(pl.Int32) - 1) * PERIOD_SECONDS
    periodic = stints.select(
        "game_id", "period", "stint_id",
        (pl.col("start_s") - offset).alias("start"),
        (pl.col("end_s") - offset).alias("end"),
    )
    return _attribute(periodic, events).drop("t", "start", "end")


def _attribute(stints: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """``stint_id`` for each event, using the faceoff / everything-else boundary rule."""
    keyed = stints.select("game_id", "period", "stint_id", "start", "end")
    ev = events.with_columns(pl.col("period_seconds").cast(pl.Int32).alias("t")).sort("t")
    is_fo = pl.col("event_type") == "FACEOFF"
    by_start = ev.filter(is_fo).join_asof(
        keyed.sort("start"), left_on="t", right_on="start", by=_GAME_KEYS, strategy="backward", check_sortedness=False
    )
    by_end = ev.filter(~is_fo).join_asof(
        keyed.sort("end"), left_on="t", right_on="end", by=_GAME_KEYS, strategy="forward", check_sortedness=False
    )
    return pl.concat([by_start, by_end], how="diagonal_relaxed").filter(
        pl.col("stint_id").is_not_null() & (pl.col("t") >= pl.col("start")) & (pl.col("t") <= pl.col("end"))
    )


def _event_counts(attributed: pl.DataFrame) -> pl.DataFrame:
    """Per-stint counts for and against, from the home team's perspective."""
    etype = pl.col("event_type")
    not_ps = ~pl.col("is_penalty_shot").fill_null(False)
    measures = {
        "cf": (etype.is_in(SHOT_ATTEMPTS) & not_ps).cast(pl.Int32),
        "ff": (etype.is_in(UNBLOCKED_SHOTS) & not_ps).cast(pl.Int32),
        "sf": (etype.is_in(["SHOT", "GOAL"]) & not_ps).cast(pl.Int32),
        "gf": (etype == "GOAL").cast(pl.Int32),
        "xgf": pl.col("xg").fill_null(0.0),
        "pen_taken": (etype == "PENALTY").cast(pl.Int32),
        "pim": pl.when(etype == "PENALTY").then(pl.col("penalty_minutes").fill_null(0)).otherwise(0).cast(pl.Int32),
        "fo_won": (etype == "FACEOFF").cast(pl.Int32),
    }
    home = pl.col("is_home_event").fill_null(False)
    away = (~pl.col("is_home_event")).fill_null(False)
    return attributed.group_by("game_id", "stint_id").agg(
        *[expr.filter(home).sum().alias(f"home_{name}") for name, expr in measures.items()],
        *[expr.filter(away).sum().alias(f"away_{name}") for name, expr in measures.items()],
    )


def _faceoff_context(stints: pl.DataFrame, events: pl.DataFrame) -> pl.DataFrame:
    """Most recent faceoff at or before each stint's start, with its zone (home perspective)."""
    fo = (
        events.filter(pl.col("event_type") == "FACEOFF")
        .with_columns(
            pl.when(pl.col("is_home_event"))
            .then(pl.col("zone_code"))
            .otherwise(pl.col("zone_code").replace_strict(_FLIP_ZONE, default=None))
            .alias("last_faceoff_zone"),
            pl.col("period_seconds").cast(pl.Int32).alias("_fo_t"),
        )
        .sort("event_idx")
        .unique(subset=[*_GAME_KEYS, "_fo_t"], keep="last")
        .select(*_GAME_KEYS, "_fo_t", "last_faceoff_zone")
        .sort("_fo_t")
    )
    return stints.sort("start").join_asof(fo, left_on="start", right_on="_fo_t", by=_GAME_KEYS, strategy="backward", check_sortedness=False
    )


def build_stints(events: pl.DataFrame, shifts: pl.DataFrame, xg: pl.DataFrame | None = None) -> pl.DataFrame:
    """Build the stint table for any set of games.

    Args:
        events: Processed events (``processed/events``) for the games.
        shifts: Merged shifts (``processed/shifts``) for the same games. Games without
            shifts produce no stints.
        xg: Per-shot predictions (``game_id, event_idx, xg``); xG columns are 0 without it.

    Returns:
        One row per stint; see ``docs/plans/m2-game-state.md`` section 1 for the columns.
        Times ``start_s``/``end_s``/``last_faceoff_s`` are game seconds.
    """
    if shifts.is_empty() or events.is_empty():
        return pl.DataFrame()
    events = events.filter(pl.col("game_id").is_in(shifts["game_id"].unique().implode()))
    if xg is not None:
        events = events.join(xg.select("game_id", "event_idx", "xg"), on=["game_id", "event_idx"], how="left")
    else:
        events = events.with_columns(pl.lit(None, pl.Float32).alias("xg"))

    games = events.group_by("game_id").agg(
        pl.col("season").first(), pl.col("home_team_id").first(), pl.col("away_team_id").first()
    )
    bounds = period_bounds(events, shifts)
    points, clipped = _intervals(events, shifts, bounds)
    stints = _merge_intervals(_on_ice_by_interval(points, clipped, games.select("game_id", "home_team_id")))
    stints = stints.sort("game_id", "period", "start").with_columns(
        pl.int_range(pl.len()).over("game_id").cast(pl.Int32).alias("stint_id")
    )

    counts = _event_counts(_attribute(stints, events))
    stints = (
        stints.join(counts, on=["game_id", "stint_id"], how="left")
        .with_columns(pl.col(f"{s}_{c}").fill_null(0) for s in ("home", "away") for c in COUNT_COLUMNS)
    )
    stints = _faceoff_context(stints, events).sort("game_id", "stint_id")

    n_home, n_away = pl.col("home_skaters").list.len(), pl.col("away_skaters").list.len()
    goalies_in = pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()
    segment = [*_GAME_KEYS, "_fo_t"]
    # A real power play always starts at a faceoff (the call stops play), so the shadow
    # needs the faceoff stint itself short-handed; a transient 4v5 during a sloppy line
    # change mid-play doesn't count. In 2024-25 the shadow is ~7% of 5v5 time (median
    # 3.4 min per game): penalties mostly expire in play and the next whistle can be far off.
    shorthanded = goalies_in & ((n_home < 5) | (n_away < 5)) & (pl.col("_fo_t") == pl.col("start"))
    offset = (pl.col("period").cast(pl.Int32) - 1) * PERIOD_SECONDS

    stints = stints.with_columns(
        n_home.cast(pl.Int8).alias("home_n"),
        n_away.cast(pl.Int8).alias("away_n"),
        pl.concat_str(n_home, pl.lit("v"), n_away).alias("strength_state"),
        # Plausible on-ice counts: 3-6 skaters a side, 6 only with that goalie pulled.
        # Shift-chart errors (late exits) briefly put 6-8 skaters on; M3 should drop these.
        (
            n_home.is_between(3, 6) & n_away.is_between(3, 6)
            & ((n_home < 6) | pl.col("home_goalie").is_null())
            & ((n_away < 6) | pl.col("away_goalie").is_null())
        ).alias("valid_personnel"),
        (pl.col("home_gf").cum_sum() - pl.col("home_gf")).over("game_id").cast(pl.Int16).alias("home_score"),
        (pl.col("away_gf").cum_sum() - pl.col("away_gf")).over("game_id").cast(pl.Int16).alias("away_score"),
        pl.when(pl.col("_fo_t") == pl.col("start"))
        .then(pl.concat_str(
            pl.lit("faceoff_"),
            pl.col("last_faceoff_zone").replace_strict(_ZONE_NAMES, default="unknown"),
        ))
        .otherwise(pl.lit("on_the_fly"))
        .alias("start_type"),
        *[
            pl.col(c).ne(pl.col(c).shift(1)).fill_null(False).cum_max().over(segment).alias(f"{s}_changed_since_faceoff")
            for s, c in (("home", "home_skaters"), ("away", "away_skaters"))
        ],
        (
            goalies_in & (n_home == 5) & (n_away == 5)
            & shorthanded.first().over(segment).fill_null(False)
        ).alias("post_penalty_5v5"),
    )

    return (
        stints.join(games, on="game_id", how="left")
        .with_columns(
            (offset + pl.col("start")).alias("start_s"),
            (offset + pl.col("end")).alias("end_s"),
            (pl.col("end") - pl.col("start")).alias("duration_s"),
            (offset + pl.col("_fo_t")).alias("last_faceoff_s"),
        )
        .select(
            "game_id", "season", "stint_id", "period", "start_s", "end_s", "duration_s",
            "home_team_id", "away_team_id",
            "home_skaters", "away_skaters", "home_goalie", "away_goalie", "home_n", "away_n", "strength_state",
            "valid_personnel", "home_score", "away_score",
            "start_type", "last_faceoff_s", "last_faceoff_zone",
            "home_changed_since_faceoff", "away_changed_since_faceoff", "post_penalty_5v5",
            *[f"{s}_{c}" for c in COUNT_COLUMNS for s in ("home", "away")],
        )
        .sort("game_id", "stint_id")
    )
