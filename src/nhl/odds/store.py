"""Write and read the live odds tables.

Each poller writes its own file (``external/odds/live/{season}/{source}.parquet``), so
pollers running at the same time never read-modify-write the same object. Readers
combine them with :func:`load_live_odds`.

Each poll also refreshes the poller's *seen* table (:func:`record_seen`): when each book
last listed each main market before puck drop. Transitions alone can't tell a price that
held until the close from a market the book took down hours earlier.
"""

from __future__ import annotations

import logging
from datetime import timedelta

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
        record_seen(store, keys.odds_seen(int(season), source), part)
    logger.info("[%s] %d row(s) written", source, written)
    return written


#: Grain of the seen table: one market or alternate rung (both sides) of one game at one book.
SEEN_KEY = ("book", "game_id", "market_uid")


def record_seen(store: Store, key: str, rows: pl.DataFrame, by: tuple[str, ...] = SEEN_KEY) -> int:
    """Move ``last_seen`` forward for every market in this poll quoted before puck drop.

    Args:
        store: S3 store.
        key: The poller's seen table (:func:`keys.odds_seen`, :func:`keys.props_seen`).
        rows: One poll's odds or prop rows (``captured_at``, ``start_time`` and ``by``).
        by: The table's grain; both sides of a market share one row.

    Alternate rungs count too (each has its own ``market_uid``), so a pulled rung has no
    close either. A poll that returned nothing never reaches here, so a failed poll can't
    mark anything seen.

    Returns:
        Markets in the poll that were recorded.
    """
    keep = pl.col("game_id").is_not_null() & (pl.col("captured_at") < pl.col("start_time"))
    schema = {**rows.select(by).schema, "last_seen": pl.Datetime("us", "UTC")}
    seen = rows.filter(keep).group_by(by).agg(pl.col("captured_at").max().alias("last_seen")).cast(schema)
    if seen.is_empty():
        return 0
    existing = store.get_parquet(key)
    if existing is not None:
        seen_all = pl.concat([existing.cast(schema), seen]).group_by(by).agg(pl.col("last_seen").max())
    else:
        seen_all = seen
    store.put_parquet(key, seen_all.sort(*by, nulls_last=True))
    return seen.height


def load_seen(store: Store, prefix: str, by: tuple[str, ...] = SEEN_KEY) -> pl.DataFrame | None:
    """Every poller's seen table under ``prefix``, combined (the latest ``last_seen`` per market);
    None when there is none."""
    frames = [store.get_parquet(k) for k in store.list_keys(prefix) if k.endswith(".parquet")]
    frames = [f for f in frames if f is not None]
    if not frames:
        return None
    return pl.concat(frames, how="diagonal_relaxed").group_by(by).agg(pl.col("last_seen").max())


def still_listed(close: pl.DataFrame, seen: pl.DataFrame | None, by: tuple[str, ...], max_gap: timedelta,
                 cover_by: tuple[str, ...] = ("book", "game_id")) -> pl.DataFrame:
    """Closing rows whose book still listed the market within ``max_gap`` of the start.

    Args:
        close: Each series' last price before ``start_time`` (columns ``by`` and ``start_time``).
        seen: From :func:`load_seen`, with the same ``by`` (book names as in ``close``).
        by: Market grain, starting with ``book`` and ``game_id``.
        max_gap: How long before the start the market must still have been listed.
        cover_by: Columns (in both frames) saying what ``seen`` tracked: a row whose
            ``cover_by`` values never appear in ``seen`` predates the tracking.

    A (book, game) absent from ``seen`` was polled before the table existed and is kept as is;
    one present whose market was last seen too early (or never) was pulled, and has no close.
    """
    if seen is None or seen.is_empty():
        return close
    by = list(by)
    cover_by = list(cover_by)
    covered = seen.select(cover_by).unique().cast({c: close.schema[c] for c in cover_by}).with_columns(
        pl.lit(True).alias("_covered"))
    seen = seen.select(*by, "last_seen").cast({c: close.schema[c] for c in by})
    j = close.join(seen, on=by, how="left", nulls_equal=True).join(covered, on=cover_by, how="left", nulls_equal=True)
    keep = pl.col("_covered").is_null() | (pl.col("last_seen") >= pl.col("start_time") - max_gap).fill_null(False)
    pulled = j.filter(~keep)
    if pulled.height:
        logger.info("close: %d side(s) dropped, market no longer listed near puck drop (%s)", pulled.height,
                    ", ".join(f"{b} {n}" for b, n in pulled.group_by("book").len().sort("book").iter_rows()))
    return j.filter(keep).drop("last_seen", "_covered")


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


__all__ = ["store_odds", "record_seen", "load_seen", "still_listed", "load_live_odds", "season_of_game", "config"]
