"""Goalie starts: who started each team-game, how long he lasted, and his workload.

* ``starter``: the team's first goalie in net (normally on the ice at the opening faceoff).
* ``finished``: the starter was the last goalie in net. ``relief_goalie`` and
  ``relief_entry_s`` describe the first other goalie to take a shift. Pulls for an extra
  attacker are not reliefs.
* Shots against and xG against are counted while the starter is in net (``goalie_in_net_id``
  on the event). ``gsax`` = xGA − GA on unblocked shots, excluding penalty shots.
* ``sog_frozen``: shots on goal he faced that were followed directly (next event) by a
  ``goalie-stopped-after-sog`` stoppage. The frozen-puck model supplies the expected rate.
* Rest and workload count every appearance (start or relief) strictly before the game,
  including the previous season's when ``prior`` is given.
"""

from __future__ import annotations

import polars as pl

FREEZE_REASON = "goalie-stopped-after-sog"


def goalie_appearances(stints: pl.DataFrame) -> pl.DataFrame:
    """Every goalie who played in each team-game, with first and last seconds in net.

    Returns:
        ``game_id, team_id, goalie_id, first_s, last_s, toi_s``.
    """
    sides = [
        stints.filter(pl.col(f"{side}_goalie").is_not_null()).select(
            "game_id",
            pl.col(f"{side}_team_id").alias("team_id"),
            pl.col(f"{side}_goalie").alias("goalie_id"),
            "start_s", "end_s", "duration_s",
        )
        for side in ("home", "away")
    ]
    return (
        pl.concat(sides)
        .group_by("game_id", "team_id", "goalie_id")
        .agg(
            pl.col("start_s").min().alias("first_s"),
            pl.col("end_s").max().alias("last_s"),
            pl.col("duration_s").sum().alias("toi_s"),
        )
    )


def _shots_against(events: pl.DataFrame, xg: pl.DataFrame | None) -> pl.DataFrame:
    """Per goalie-game: shots on goal, goals, unblocked xG and freezes faced."""
    ev = events.sort("game_id", "event_idx").with_columns(
        pl.col("event_type").shift(-1).over("game_id").alias("_next_type"),
        pl.col("reason").shift(-1).over("game_id").alias("_next_reason"),
    )
    ev = ev.filter(
        pl.col("event_type").is_in(["SHOT", "MISSED_SHOT", "GOAL"])
        & pl.col("goalie_in_net_id").is_not_null()
        & ~pl.col("is_penalty_shot").fill_null(False)
    )
    if xg is not None:
        ev = ev.join(xg.select("game_id", "event_idx", "xg"), on=["game_id", "event_idx"], how="left")
    else:
        ev = ev.with_columns(pl.lit(None, pl.Float32).alias("xg"))
    etype = pl.col("event_type")
    return ev.group_by("game_id", pl.col("goalie_in_net_id").alias("goalie_id")).agg(
        etype.is_in(["SHOT", "GOAL"]).sum().cast(pl.Int32).alias("shots_against"),
        (etype == "GOAL").sum().cast(pl.Int32).alias("goals_against"),
        pl.col("xg").sum().cast(pl.Float64).alias("xga"),
        ((etype == "SHOT") & (pl.col("_next_type") == "STOPPAGE") & (pl.col("_next_reason") == FREEZE_REASON))
        .sum()
        .cast(pl.Int32)
        .alias("sog_frozen"),
    )


def build_goalie_starts(
    stints: pl.DataFrame,
    events: pl.DataFrame,
    xg: pl.DataFrame | None = None,
    prior: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """One row per team-game describing the starting goalie.

    Args:
        stints: Season stints.
        events: Season events (for shots against and freezes).
        xg: Per-shot predictions.
        prior: The previous season's goalie appearances (``goalie_appearances`` output
            with ``game_date``), so rest days carry over the season boundary.

    Returns:
        See module docstring.
    """
    games = events.group_by("game_id").agg(
        pl.col("season").first(), pl.col("game_date").first(), pl.col("home_team_id").first()
    )
    apps = goalie_appearances(stints).join(games, on="game_id", how="left")
    # The first goalie in net. Not simply stint 0: a few shift charts miss the starter's
    # opening shift, leaving the first seconds without a goalie.
    starters = (
        apps.sort("first_s", "goalie_id")
        .unique(["game_id", "team_id"], keep="first")
        .select("game_id", "team_id", pl.col("goalie_id").alias("starter"))
    )

    # The goalie with the latest exit finished the game.
    last_goalie = (
        apps.sort("last_s", descending=True)
        .unique(["game_id", "team_id"], keep="first")
        .select("game_id", "team_id", pl.col("goalie_id").alias("_last_goalie"))
    )
    out = (
        starters.join(last_goalie, on=["game_id", "team_id"], how="left")
        .join(
            apps.select("game_id", "goalie_id", "toi_s", pl.col("last_s").alias("_starter_last_s")),
            left_on=["game_id", "starter"],
            right_on=["game_id", "goalie_id"],
            how="left",
        )
        .with_columns((pl.col("_last_goalie") == pl.col("starter")).alias("finished"))
    )
    relief = (
        apps.join(out.select("game_id", "team_id", "starter"), on=["game_id", "team_id"], how="inner")
        .filter(pl.col("goalie_id") != pl.col("starter"))
        .sort("first_s")
        .unique(["game_id", "team_id"], keep="first")
        .select("game_id", "team_id", pl.col("goalie_id").alias("relief_goalie"), pl.col("first_s").alias("relief_entry_s"))
    )
    out = (
        out.join(relief, on=["game_id", "team_id"], how="left")
        .join(games, on="game_id", how="left")
        .with_columns((pl.col("team_id") == pl.col("home_team_id")).alias("home"))
        .join(_shots_against(events, xg), left_on=["game_id", "starter"], right_on=["game_id", "goalie_id"], how="left")
        .with_columns(
            pl.col("shots_against", "goals_against", "sog_frozen").fill_null(0),
            pl.col("xga").fill_null(0.0),
        )
        .with_columns(
            (pl.col("shots_against") - pl.col("goals_against")).alias("saves"),
            (pl.col("xga") - pl.col("goals_against")).alias("gsax"),
            pl.when(pl.col("finished")).then(None).otherwise(pl.col("_starter_last_s")).alias("pulled_at_s"),
        )
    )
    return _with_workload(out, apps, prior).select(
        "game_id", "season", "game_date", "team_id", "home", "starter", "finished", "pulled_at_s",
        "relief_goalie", "relief_entry_s", "toi_s", "shots_against", "goals_against", "saves", "xga", "gsax",
        "sog_frozen", "rest_days", "is_back_to_back", "starts_last_7d", "consecutive_starts",
    ).sort("game_id", "home")


def _with_workload(starts: pl.DataFrame, apps: pl.DataFrame, prior: pl.DataFrame | None) -> pl.DataFrame:
    """Rest days, back-to-backs, recent starts and the starter's current streak."""
    history = apps.select("goalie_id", "game_id", "game_date")
    if prior is not None and not prior.is_empty():
        history = pl.concat([prior.select("goalie_id", "game_id", "game_date"), history], how="vertical_relaxed")
    history = history.unique().sort("goalie_id", "game_date", "game_id")
    rest = history.with_columns(
        (pl.col("game_date") - pl.col("game_date").shift(1).over("goalie_id")).dt.total_days().cast(pl.Int32).alias("rest_days")
    ).select("goalie_id", "game_id", "rest_days")

    starts = starts.join(rest, left_on=["starter", "game_id"], right_on=["goalie_id", "game_id"], how="left")

    # Starts by the same goalie in the 7 days before this game (rolling over the team's starts).
    own = starts.select("starter", "game_id", "game_date").sort("starter", "game_date", "game_id")
    window = (
        own.rolling(index_column="game_date", period="7d", closed="left", group_by="starter")
        .agg(pl.len().cast(pl.Int8).alias("starts_last_7d"))
        .unique(["starter", "game_date"])
    )
    team_seq = starts.sort("team_id", "game_date", "game_id").with_columns(
        (pl.col("starter") != pl.col("starter").shift(1).over("team_id")).fill_null(True).cum_sum().over("team_id").alias("_run")
    ).with_columns(pl.int_range(1, pl.len() + 1).over("team_id", "_run").cast(pl.Int16).alias("consecutive_starts"))
    return (
        team_seq.join(window, on=["starter", "game_date"], how="left")
        .with_columns(
            pl.col("starts_last_7d").fill_null(0),
            (pl.col("rest_days") == 1).fill_null(False).alias("is_back_to_back"),
        )
    )
