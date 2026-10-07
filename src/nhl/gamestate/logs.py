"""Team-game and player-game logs by strength, derived from stints and events.

Both tables are **long**: one row per (team or player, game, ``strength``) with
``strength`` in:

* ``all``;
* ``5v5`` (both goalies in);
* ``EV`` (equal skaters, both goalies in; includes 4v4 and 3v3);
* ``PP`` / ``SH`` (both goalies in, more / fewer skaters);
* ``EN_own`` (own net empty) / ``EN_opp`` (opponent's net empty).

The strength rows other than ``all`` partition the game, so they sum to ``all``.

Every event is classified by the strength of the stint it belongs to (same attribution
rule as the stints), so on-ice, individual and box-score counts always agree.
"""

from __future__ import annotations

import polars as pl

from nhl.gamestate.stints import attribute_events
from nhl.transform.events import SHOT_ATTEMPTS, UNBLOCKED_SHOTS

STRENGTHS = ("5v5", "EV", "PP", "SH", "EN_own", "EN_opp")

_ON_ICE = ("cf", "ff", "sf", "gf", "xgf")
_AGAINST = {"cf": "ca", "ff": "fa", "sf": "sa", "gf": "ga", "xgf": "xga"}


def _team_view(stints: pl.DataFrame) -> pl.DataFrame:
    """Each stint from each team's side, with that team's strength label."""
    sides = []
    for own, opp in (("home", "away"), ("away", "home")):
        sides.append(
            stints.select(
                "game_id", "season", "stint_id", "duration_s",
                pl.col(f"{own}_team_id").alias("team_id"),
                pl.col(f"{opp}_team_id").alias("opp_team_id"),
                pl.lit(own == "home").alias("is_home"),
                pl.col(f"{own}_skaters").alias("skaters"),
                pl.col(f"{own}_n").alias("own_n"),
                pl.col(f"{opp}_n").alias("opp_n"),
                pl.col(f"{own}_goalie").is_null().alias("own_empty"),
                pl.col(f"{opp}_goalie").is_null().alias("opp_empty"),
                *[pl.col(f"{own}_{c}").alias(c) for c in _ON_ICE],
                *[pl.col(f"{opp}_{c}").alias(_AGAINST[c]) for c in _ON_ICE],
            )
        )
    own, opp = pl.col("own_n"), pl.col("opp_n")
    return pl.concat(sides).with_columns(
        pl.when(pl.col("own_empty")).then(pl.lit("EN_own"))
        .when(pl.col("opp_empty")).then(pl.lit("EN_opp"))
        .when(own > opp).then(pl.lit("PP"))
        .when(own < opp).then(pl.lit("SH"))
        .otherwise(pl.lit("EV"))
        .alias("strength"),
        ((own == 5) & (opp == 5) & ~pl.col("own_empty") & ~pl.col("opp_empty")).alias("is_5v5"),
    )


def _expand_strengths(df: pl.DataFrame) -> pl.DataFrame:
    """Duplicate rows into ``all`` and ``5v5`` so one group-by covers every strength."""
    return pl.concat(
        [
            df,
            df.with_columns(pl.lit("all").alias("strength")),
            df.filter(pl.col("is_5v5")).with_columns(pl.lit("5v5").alias("strength")),
        ]
    )


def _event_strengths(stints: pl.DataFrame, events: pl.DataFrame, xg: pl.DataFrame | None) -> pl.DataFrame:
    """Events with the event team's strength label and xG."""
    view = _team_view(stints).select("game_id", "stint_id", "team_id", "strength", "is_5v5")
    if xg is not None:
        events = events.join(xg.select("game_id", "event_idx", "xg"), on=["game_id", "event_idx"], how="left")
    else:
        events = events.with_columns(pl.lit(None, pl.Float32).alias("xg"))
    return attribute_events(stints, events).join(
        view, left_on=["game_id", "stint_id", "event_team_id"], right_on=["game_id", "stint_id", "team_id"], how="left"
    )


def team_game_logs(stints: pl.DataFrame, events: pl.DataFrame, xg: pl.DataFrame | None = None) -> pl.DataFrame:
    """Team-game rows by strength.

    Returns:
        ``game_id, season, team_id, opp_team_id, is_home, strength, toi_s``, on-ice
        ``cf/ca, ff/fa, sf/sa, gf/ga, xgf/xga``, ``pp_opportunities`` (on the ``all`` row
        only), and box counts by the event team's strength: ``pen_taken``, ``pim``,
        ``pen_drawn``, ``fo_won``, ``fo_lost``, ``hits``, ``blocks``, ``giveaways``,
        ``takeaways``.
    """
    view = _team_view(stints)
    on_ice = (
        _expand_strengths(view)
        .group_by("game_id", "season", "team_id", "opp_team_id", "is_home", "strength")
        .agg(
            pl.col("duration_s").sum().alias("toi_s"),
            *[pl.col(c).sum() for c in ("cf", "ca", "ff", "fa", "sf", "sa", "gf", "ga")],
            pl.col("xgf").sum(), pl.col("xga").sum(),
        )
    )
    # A power-play opportunity starts at a faceoff with the team up a skater after not being so.
    pp = (
        view.sort("game_id", "team_id", "stint_id")
        .join(stints.select("game_id", "stint_id", "start_type"), on=["game_id", "stint_id"], how="left")
        .with_columns((pl.col("strength") == "PP").alias("_pp"))
        .with_columns(
            (pl.col("_pp") & ~pl.col("_pp").shift(1).over("game_id", "team_id").fill_null(False)
             & (pl.col("start_type") != "on_the_fly")).alias("_new_pp")
        )
        .group_by("game_id", "team_id")
        .agg(pl.col("_new_pp").sum().cast(pl.Int16).alias("pp_opportunities"))
        .with_columns(pl.lit("all").alias("strength"))
    )

    ev = _event_strengths(stints, events, xg)
    etype = pl.col("event_type")
    own = (
        _expand_strengths(ev.filter(pl.col("strength").is_not_null()))
        .group_by("game_id", pl.col("event_team_id").alias("team_id"), "strength")
        .agg(
            (etype == "PENALTY").sum().cast(pl.Int16).alias("pen_taken"),
            pl.col("penalty_minutes").filter(etype == "PENALTY").sum().cast(pl.Int16).alias("pim"),
            (etype == "FACEOFF").sum().cast(pl.Int16).alias("fo_won"),
            (etype == "HIT").sum().cast(pl.Int16).alias("hits"),
            (etype == "GIVEAWAY").sum().cast(pl.Int16).alias("giveaways"),
            (etype == "TAKEAWAY").sum().cast(pl.Int16).alias("takeaways"),
        )
    )
    # Events owned by the opponent, credited to this team with the strength flipped.
    flip = {"PP": "SH", "SH": "PP", "EN_own": "EN_opp", "EN_opp": "EN_own"}
    against = (
        _expand_strengths(ev.filter(pl.col("strength").is_not_null()))
        .with_columns(pl.col("strength").replace(flip))
        .join(view.select("game_id", "team_id", "opp_team_id").unique(), left_on=["game_id", "event_team_id"],
              right_on=["game_id", "opp_team_id"], how="inner")
        .group_by("game_id", "team_id", "strength")
        .agg(
            (etype == "PENALTY").sum().cast(pl.Int16).alias("pen_drawn"),
            (etype == "FACEOFF").sum().cast(pl.Int16).alias("fo_lost"),
            (etype == "BLOCKED_SHOT").sum().cast(pl.Int16).alias("blocks"),
        )
    )
    box = ["pen_taken", "pim", "pen_drawn", "fo_won", "fo_lost", "hits", "blocks", "giveaways", "takeaways"]
    return (
        on_ice.join(own, on=["game_id", "team_id", "strength"], how="left")
        .join(against, on=["game_id", "team_id", "strength"], how="left")
        .join(pp, on=["game_id", "team_id", "strength"], how="left")
        .with_columns(pl.col(box).fill_null(0))
        .sort("game_id", "team_id", "strength")
    )


def player_game_logs(
    stints: pl.DataFrame,
    events: pl.DataFrame,
    shifts: pl.DataFrame,
    rosters: pl.DataFrame,
    players: pl.DataFrame | None = None,
    xg: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Player-game rows by strength (skaters and goalies who took a shift).

    Returns:
        ``game_id, season, game_date, player_id, team_id, position, age, strength, toi_s``;
        on-ice ``cf/ca ... xgf/xga``; individual ``goals, a1, a2, icf, iff, isf, ixg,
        pen_taken, pen_drawn, fo_won, fo_lost, hits, hits_taken, blocks, giveaways,
        takeaways``; and on the ``all`` row ``shifts`` and ``avg_shift_s``.
    """
    view = _team_view(stints)
    on_ice = (
        _expand_strengths(view)
        .explode("skaters")
        .rename({"skaters": "player_id"})
        .filter(pl.col("player_id").is_not_null())
        .group_by("game_id", "season", "player_id", "team_id", "strength")
        .agg(
            pl.col("duration_s").sum().alias("toi_s"),
            *[pl.col(c).sum() for c in ("cf", "ca", "ff", "fa", "sf", "sa", "gf", "ga")],
            pl.col("xgf").sum(), pl.col("xga").sum(),
        )
    )

    ev = _expand_strengths(_event_strengths(stints, events, xg).filter(pl.col("strength").is_not_null()))
    etype, ps = pl.col("event_type"), ~pl.col("is_penalty_shot").fill_null(False)
    flip = {"PP": "SH", "SH": "PP", "EN_own": "EN_opp", "EN_opp": "EN_own"}
    # (role column, flip strength to the player's team?, {stat: condition})
    roles = [
        ("player_1_id", False, {
            "goals": etype == "GOAL",
            "icf": etype.is_in(SHOT_ATTEMPTS) & ps,
            "iff": etype.is_in(UNBLOCKED_SHOTS) & ps,
            "isf": etype.is_in(["SHOT", "GOAL"]) & ps,
            "pen_taken": etype == "PENALTY",
            "fo_won": etype == "FACEOFF",
            "hits": etype == "HIT",
            "giveaways": etype == "GIVEAWAY",
            "takeaways": etype == "TAKEAWAY",
        }),
        ("player_2_id", False, {"a1": etype == "GOAL"}),
        ("player_3_id", False, {"a2": etype == "GOAL"}),
        ("player_2_id", True, {
            "pen_drawn": etype == "PENALTY",
            "fo_lost": etype == "FACEOFF",
            "hits_taken": etype == "HIT",
            "blocks": etype == "BLOCKED_SHOT",
        }),
    ]
    parts = []
    for col, flipped, stats in roles:
        frame = ev.filter(pl.col(col).is_not_null())
        if flipped:
            frame = frame.with_columns(pl.col("strength").replace(flip))
        parts.append(
            frame.group_by("game_id", pl.col(col).alias("player_id"), "strength").agg(
                *[cond.sum().cast(pl.Int16).alias(name) for name, cond in stats.items()]
            )
        )
    ixg = (
        ev.filter(etype.is_in(UNBLOCKED_SHOTS) & ps)
        .group_by("game_id", pl.col("player_1_id").alias("player_id"), "strength")
        .agg(pl.col("xg").sum().alias("ixg"))
    )
    individual = parts[0]
    for part in [*parts[1:], ixg]:
        individual = individual.join(part, on=["game_id", "player_id", "strength"], how="full", coalesce=True)

    shift_stats = (
        shifts.filter(~pl.col("is_goalie"))
        .group_by("game_id", "player_id")
        .agg(pl.len().cast(pl.Int16).alias("shifts"), (pl.col("end") - pl.col("start")).mean().alias("avg_shift_s"))
        .with_columns(pl.lit("all").alias("strength"))
    )
    goalie_toi = _goalie_rows(stints)

    out = (
        pl.concat([on_ice, goalie_toi], how="diagonal_relaxed")
        .join(individual, on=["game_id", "player_id", "strength"], how="left")
        .join(shift_stats, on=["game_id", "player_id", "strength"], how="left")
    )
    stat_cols = ["goals", "a1", "a2", "icf", "iff", "isf", "pen_taken", "pen_drawn", "fo_won", "fo_lost",
                 "hits", "hits_taken", "blocks", "giveaways", "takeaways"]
    out = out.with_columns(pl.col(stat_cols).fill_null(0), pl.col("ixg").fill_null(0.0))

    dates = events.group_by("game_id").agg(pl.col("game_date").first())
    out = out.join(dates, on="game_id", how="left").join(
        rosters.select("game_id", "player_id", "position"), on=["game_id", "player_id"], how="left"
    )
    if players is not None:
        out = out.join(players.select("player_id", "birth_date"), on="player_id", how="left").with_columns(
            ((pl.col("game_date") - pl.col("birth_date").str.to_date(strict=False)).dt.total_days() / 365.25)
            .cast(pl.Float32)
            .alias("age")
        ).drop("birth_date")
    else:
        out = out.with_columns(pl.lit(None, pl.Float32).alias("age"))
    lead = ["game_id", "season", "game_date", "player_id", "team_id", "position", "age", "strength", "toi_s"]
    return out.select(*lead, *[c for c in out.columns if c not in lead]).sort("game_id", "team_id", "player_id", "strength")


def _goalie_rows(stints: pl.DataFrame) -> pl.DataFrame:
    """Goalie TOI and on-ice counts by strength (from the goalie's team's perspective)."""
    view = _team_view(stints).join(
        pl.concat(
            [
                stints.select("game_id", "stint_id", pl.col(f"{s}_team_id").alias("team_id"), pl.col(f"{s}_goalie").alias("player_id"))
                for s in ("home", "away")
            ]
        ).filter(pl.col("player_id").is_not_null()),
        on=["game_id", "stint_id", "team_id"],
        how="inner",
    )
    return (
        _expand_strengths(view)
        .group_by("game_id", "season", "player_id", "team_id", "strength")
        .agg(
            pl.col("duration_s").sum().alias("toi_s"),
            *[pl.col(c).sum() for c in ("cf", "ca", "ff", "fa", "sf", "sa", "gf", "ga")],
            pl.col("xgf").sum(), pl.col("xga").sum(),
        )
    )
