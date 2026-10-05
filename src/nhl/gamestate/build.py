"""Build every M2 game-state table for a season and write it to S3 (``nhl game-state``)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import polars as pl

from nhl import config
from nhl.gamestate.goalies import build_goalie_starts
from nhl.gamestate.lineups import build_lineups
from nhl.gamestate.logs import player_game_logs, team_game_logs
from nhl.gamestate.rosters import season_coaches, season_rosters
from nhl.gamestate.stints import build_stints
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)


@dataclass
class SeasonBuild:
    """Row counts written for one season, plus coverage notes."""

    season: int
    rows: dict[str, int] = field(default_factory=dict)
    games: int = 0
    games_without_shifts: list[int] = field(default_factory=list)

    def summary(self) -> str:
        """One-line description for logs."""
        counts = ", ".join(f"{k} {v:,}" for k, v in self.rows.items())
        return f"{self.season}: {self.games} games ({len(self.games_without_shifts)} without shifts) | {counts}"


def _prior_appearances(store: Store, season: int) -> pl.DataFrame | None:
    """Previous season's goalie appearances (starters and first relievers) for rest days."""
    prev = store.get_parquet(keys.goalie_starts(season - 10001))
    if prev is None:
        return None
    return pl.concat(
        [
            prev.select(pl.col("starter").alias("goalie_id"), "game_id", "game_date"),
            prev.filter(pl.col("relief_goalie").is_not_null()).select(
                pl.col("relief_goalie").alias("goalie_id"), "game_id", "game_date"
            ),
        ]
    )


def build_season(store: Store, start_year: int, workers: int = 16) -> SeasonBuild:
    """Build stints, rosters, coaches, lineups, goalie starts and game logs for a season.

    Args:
        store: S3 store.
        start_year: Season start year.
        workers: Parallel reads of per-game raw payloads.

    Returns:
        What was written.
    """
    season = config.season_id(start_year)
    events = store.read_parquet_required(keys.events(season))
    shifts = store.read_parquet_required(keys.shifts(season))
    xg = store.get_parquet(keys.xg_predictions(season))
    if xg is None:
        logger.warning("%s: no xG predictions; xG columns will be zero", season)
    players = store.get_parquet(keys.PLAYERS)
    games = store.read_parquet_required(keys.GAMES).filter(
        (pl.col("season") == season) & pl.col("is_final") & pl.col("game_id").is_in(events["game_id"].unique().implode())
    )

    report = SeasonBuild(season=season, games=games.height)
    report.games_without_shifts = sorted(set(games["game_id"].to_list()) - set(shifts["game_id"].unique().to_list()))

    stints = build_stints(events, shifts, xg)
    rosters = season_rosters(store, season, games["game_id"].to_list(), workers=workers)
    coaches = season_coaches(store, season, games, workers=workers)
    tables = {
        keys.stints(season): stints,
        keys.rosters(season): rosters,
        keys.coaches(season): coaches,
        keys.lineups(season): build_lineups(stints, rosters),
        keys.goalie_starts(season): build_goalie_starts(
            stints, events, xg, _prior_appearances(store, season), store.get_parquet(keys.freeze_predictions(season))
        ),
        keys.team_game_logs(season): team_game_logs(stints, events, xg),
        keys.player_game_logs(season): player_game_logs(stints, events, shifts, rosters, players, xg),
    }
    for key, frame in tables.items():
        store.put_parquet(key, frame)
        report.rows[key.split("/")[1] if "game_logs" not in key else "logs_" + key.split("/")[2]] = frame.height
    logger.info(report.summary())
    return report
