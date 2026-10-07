"""Write and read the live odds tables.

Each poller writes its own file (``external/odds/live/{season}/{source}.parquet``), so
pollers running at the same time never read-modify-write the same object. Readers
combine them with :func:`load_live_odds`.
"""

from __future__ import annotations

import logging

import polars as pl

from nhl import config
from nhl.odds.core import MARKET_KEY, ODDS_SCHEMA, TRANSITION_VALUES, attach_game_ids
from nhl.sources.common import append_transitions
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)


def season_of_game(game_id: pl.Expr) -> pl.Expr:
    """8-digit season id from an NHL game id (``2026020040 -> 20262027``)."""
    start = game_id // 1_000_000
    return start * 10000 + start + 1


def store_odds(store: Store, odds: pl.DataFrame, games: pl.DataFrame, source: str) -> int:
    """Attach game ids, drop and log unmatched rows, and append transitions by season.

    Args:
        store: S3 store.
        odds: Normalized odds rows from :func:`nhl.odds.core.odds_frame`.
        games: ``processed/games.parquet``.
        source: Poller name (``lowvig``, ``fourcasters``, ``espn``); selects the file.

    Returns:
        Rows written across all seasons.
    """
    if odds.is_empty():
        logger.info("[%s] no rows to store", source)
        return 0
    odds = attach_game_ids(odds, games)
    unmatched = odds.filter(pl.col("game_id").is_null())
    if unmatched.height:
        pairs = sorted(unmatched.select("away_team", "home_team").unique().rows(), key=str)
        logger.warning("[%s] %d row(s) across %d game(s) matched no scheduled game and were dropped: %s",
                       source, unmatched.height, len(pairs), pairs)
    odds = odds.filter(pl.col("game_id").is_not_null())
    written = 0
    for (season,), part in odds.with_columns(season_of_game(pl.col("game_id")).alias("_season")).partition_by(
        "_season", as_dict=True
    ).items():
        written += append_transitions(
            store, keys.odds(int(season), source), part.drop("_season"), MARKET_KEY, TRANSITION_VALUES
        )
    logger.info("[%s] %d row(s) written", source, written)
    return written


def load_live_odds(store: Store, season: int) -> pl.DataFrame:
    """Every live-polled price for a season, all sources combined.

    Args:
        store: S3 store.
        season: 8-digit season id.
    """
    frames = [store.get_parquet(k) for k in store.list_keys(keys.odds_live_prefix(season)) if k.endswith(".parquet")]
    frames = [f for f in frames if f is not None]
    if not frames:
        return pl.DataFrame(schema=ODDS_SCHEMA | {"game_id": pl.Int64})
    return pl.concat(frames, how="diagonal_relaxed").sort("game_id", "captured_at")


__all__ = ["store_odds", "load_live_odds", "season_of_game", "config"]
