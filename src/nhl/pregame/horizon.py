"""Which game dates to price: the pricing horizon (docs/plans/bet-timing.md, phase A).

Books hang a game's lines a median of ~32 h before puck drop (up to ~53 h). The horizon is
**today** (Eastern), whenever it has a game not yet started, plus **every later date with a
game not yet started that a book has quoted**. Reprices, edges and prop edges loop over it,
so a game is priced from the first odds poll that sees it rather than the next morning.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

EASTERN = ZoneInfo("America/New_York")


def _unstarted(store: Store, now: datetime) -> pl.DataFrame:
    """``game_id, game_date, season``: games not final and not yet started at ``now``."""
    start = pl.col("start_time_et").str.to_datetime(strict=False).dt.replace_time_zone("America/New_York")
    return (store.read_parquet_required(keys.GAMES).filter(~pl.col("is_final"))
            .with_columns(start.dt.convert_time_zone("UTC").alias("_start"))
            .filter(pl.col("_start") > now).select("game_id", "game_date", "season"))


def _quoted(store: Store, games: pl.DataFrame) -> set[int]:
    """Game ids among ``games`` with at least one captured quote from any book."""
    from nhl.odds.store import load_live_odds

    out: set[int] = set()
    for season in games["season"].unique().to_list():
        odds = load_live_odds(store, int(season))
        if not odds.is_empty():
            out |= set(odds.filter(pl.col("game_id").is_in(games["game_id"].implode()))["game_id"].unique().to_list())
    return out


def dates(store: Store, now: datetime | None = None) -> list[date]:
    """The horizon: today if it has a game left, plus every later date with a quoted game."""
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(EASTERN).date()
    games = _unstarted(store, now).filter(pl.col("game_date") >= today)
    if games.is_empty():
        return []
    later = games.filter(pl.col("game_date") > today)
    quoted = _quoted(store, later) if not later.is_empty() else set()
    days = set(later.filter(pl.col("game_id").is_in(list(quoted)))["game_date"].to_list())
    if (games["game_date"] == today).any():
        days.add(today)
    return sorted(days)


def unpriced(store: Store, days: list[date]) -> list[date]:
    """The dates in ``days`` without a pregame snapshot yet."""
    return [d for d in days if store.get_bytes(keys.pregame_latest(d)) is None]
