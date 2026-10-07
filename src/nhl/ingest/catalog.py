"""Game schedule/results, team and player reference tables.

These replace the legacy per-day schedule crawl and the hardcoded roster URL lists:
the stats API exposes every game, every team and per-season player bios in bulk.
"""

from __future__ import annotations

import logging

import polars as pl

from nhl import config
from nhl.ingest.http import NHLClient
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)


def refresh_games(store: Store, client: NHLClient) -> pl.DataFrame:
    """Download the full game catalog and write ``processed/games.parquet``.

    Args:
        store: S3 store.
        client: NHL API client.

    Returns:
        Regular season and playoff games from ``FIRST_SEASON`` on, one row per game.
    """
    raw = client.all_games()
    store.put_json_gz(keys.CATALOG_GAMES, raw)
    teams = refresh_teams(store, client)
    games = games_from_catalog(raw, teams)
    store.put_parquet(keys.GAMES, games)
    return games


def games_from_catalog(raw: list[dict], teams: pl.DataFrame) -> pl.DataFrame:
    """Normalize the raw stats-API game list.

    Args:
        raw: Records from ``/stats/rest/en/game``.
        teams: Team table from :func:`teams_from_catalog`.

    Returns:
        Games DataFrame with ``season_type`` in {"R", "P"} and ``is_final``.
    """
    abbr = teams.select("team_id", "team_abbr")
    games = (
        pl.DataFrame(raw, infer_schema_length=None)
        .filter(
            pl.col("gameType").is_in(list(config.GAME_TYPES))
            & (pl.col("season") >= config.season_id(config.FIRST_SEASON))
        )
        .select(
            pl.col("id").cast(pl.Int64).alias("game_id"),
            pl.col("season").cast(pl.Int32),
            pl.col("gameType").replace_strict(config.GAME_TYPES, return_dtype=pl.Utf8).alias("season_type"),
            pl.col("gameDate").str.to_date().alias("game_date"),
            pl.col("easternStartTime").alias("start_time_et"),
            pl.col("homeTeamId").cast(pl.Int32).alias("home_team_id"),
            pl.col("visitingTeamId").cast(pl.Int32).alias("away_team_id"),
            pl.col("homeScore").cast(pl.Int16).alias("home_score"),
            pl.col("visitingScore").cast(pl.Int16).alias("away_score"),
            pl.col("period").cast(pl.Int8).alias("last_period"),
            (pl.col("gameStateId") == config.FINAL_GAME_STATE).alias("is_final"),
        )
        .join(abbr.rename({"team_id": "home_team_id", "team_abbr": "home_abbr"}), on="home_team_id", how="left")
        .join(abbr.rename({"team_id": "away_team_id", "team_abbr": "away_abbr"}), on="away_team_id", how="left")
        .sort("game_id")
    )
    return games


def refresh_teams(store: Store, client: NHLClient) -> pl.DataFrame:
    """Download teams and write ``processed/teams.parquet``."""
    raw = client.teams()
    store.put_json_gz(keys.CATALOG_TEAMS, raw)
    teams = teams_from_catalog(raw)
    store.put_parquet(keys.TEAMS, teams)
    return teams


def teams_from_catalog(raw: list[dict]) -> pl.DataFrame:
    """Normalize the raw team list to ``team_id, team_abbr, team_name``."""
    return pl.DataFrame(
        {
            "team_id": [t["id"] for t in raw],
            "team_abbr": [t["triCode"] for t in raw],
            "team_name": [t["fullName"] for t in raw],
        },
        schema={"team_id": pl.Int32, "team_abbr": pl.Utf8, "team_name": pl.Utf8},
    )


def refresh_players(store: Store, client: NHLClient, seasons: list[int], force: bool = False) -> pl.DataFrame:
    """Fetch skater/goalie bios per season and rebuild ``processed/players.parquet``.

    Past seasons are only downloaded once; pass ``force`` to re-pull them.

    Args:
        store: S3 store.
        client: NHL API client.
        seasons: Season start years to include.
        force: Re-download seasons already in S3.

    Returns:
        One row per player with position and shoots/catches hand.
    """
    frames: list[pl.DataFrame] = []
    current = max(seasons)
    for year in seasons:
        sid = config.season_id(year)
        for kind in ("skater", "goalie"):
            key = keys.raw_player_bios(kind, sid)
            raw = None if (force or year == current) else store.get_json_gz(key)
            if raw is None:
                raw = client.player_bios(kind, sid)
                store.put_json_gz(key, raw)
            if raw:
                frames.append(players_from_bios(raw, kind, sid))
    players = (
        pl.concat(frames, how="vertical_relaxed")
        .sort("last_season")
        .unique("player_id", keep="last")
        .sort("player_id")
    )
    store.put_parquet(keys.PLAYERS, players)
    return players


def players_from_bios(raw: list[dict], kind: str, season: int) -> pl.DataFrame:
    """Normalize one bios payload.

    Args:
        raw: Records from ``/stats/rest/en/{kind}/bios``.
        kind: ``"skater"`` or ``"goalie"``.
        season: Season id the payload is for.
    """
    name_field = "skaterFullName" if kind == "skater" else "goalieFullName"
    return pl.DataFrame(
        {
            "player_id": [r["playerId"] for r in raw],
            "player_name": [r.get(name_field) for r in raw],
            "position": [r.get("positionCode", "G") for r in raw],
            "shoots_catches": [r.get("shootsCatches") for r in raw],
            "birth_date": [r.get("birthDate") for r in raw],
            "height_in": [r.get("height") for r in raw],
            "weight_lb": [r.get("weight") for r in raw],
            "last_season": [season] * len(raw),
        },
        schema={
            "player_id": pl.Int64,
            "player_name": pl.Utf8,
            "position": pl.Utf8,
            "shoots_catches": pl.Utf8,
            "birth_date": pl.Utf8,
            "height_in": pl.Int16,
            "weight_lb": pl.Int16,
            "last_season": pl.Int32,
        },
    )
