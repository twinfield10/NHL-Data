"""Fetch raw per-game play-by-play and shift charts into ``raw/``.

Idempotent: a game whose raw files already exist in S3 is skipped, so the same
command serves the historical backfill and the nightly update.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import polars as pl

from nhl import config
from nhl.ingest.http import NHLClient
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)


@dataclass
class IngestReport:
    """Outcome of ingesting one season."""

    season: int
    candidates: int = 0
    fetched: int = 0
    skipped: int = 0
    empty_shifts: list[int] = field(default_factory=list)
    failed: dict[int, str] = field(default_factory=dict)

    def summary(self) -> str:
        """One-line human-readable summary."""
        return (
            f"{self.season}: {self.candidates} final games | fetched {self.fetched} | "
            f"already stored {self.skipped} | empty shift charts {len(self.empty_shifts)} | "
            f"failed {len(self.failed)}"
        )


def _existing_ids(store: Store, season: int) -> set[int]:
    pbp = {keys.game_id_from_key(k) for k in store.list_keys(keys.raw_pbp_prefix(season))}
    shifts = {keys.game_id_from_key(k) for k in store.list_keys(keys.raw_shifts_prefix(season))}
    return pbp & shifts


def _fetch_one(store: Store, client: NHLClient, season: int, game_id: int) -> bool:
    """Fetch and store both raw files for one game. Returns True if shifts were empty."""
    pbp = client.play_by_play(game_id)
    if not pbp.get("plays"):
        raise ValueError("play-by-play has no plays")
    shifts = client.shift_chart(game_id)
    store.put_json_gz(keys.raw_pbp(season, game_id), pbp)
    store.put_json_gz(keys.raw_shifts(season, game_id), shifts)
    return not shifts.get("data")


def ingest_season(
    store: Store,
    client: NHLClient,
    games: pl.DataFrame,
    start_year: int,
    workers: int = 6,
    refetch: bool = False,
) -> IngestReport:
    """Download raw files for every final game in a season that is not yet stored.

    Args:
        store: S3 store.
        client: NHL API client (its rate limiter is shared by all workers).
        games: Games table from :func:`nhl.ingest.catalog.refresh_games`.
        start_year: Season start year.
        workers: Concurrent download threads.
        refetch: Re-download games that already exist.

    Returns:
        An :class:`IngestReport`.
    """
    season = config.season_id(start_year)
    ids = (
        games.filter((pl.col("season") == season) & pl.col("is_final"))
        .get_column("game_id")
        .to_list()
    )
    report = IngestReport(season=season, candidates=len(ids))
    existing = set() if refetch else _existing_ids(store, season)
    todo = [g for g in ids if g not in existing]
    report.skipped = len(ids) - len(todo)
    logger.info("%s: %d final games, %d to fetch", season, len(ids), len(todo))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_one, store, client, season, g): g for g in todo}
        for n, fut in enumerate(as_completed(futures), 1):
            game_id = futures[fut]
            try:
                if fut.result():
                    report.empty_shifts.append(game_id)
                report.fetched += 1
            except Exception as exc:  # noqa: BLE001 - record and keep going; reported at the end
                report.failed[game_id] = repr(exc)
                logger.warning("game %s failed: %r", game_id, exc)
            if n % 200 == 0:
                logger.info("%s: %d/%d fetched", season, n, len(todo))

    if todo:
        store.put_json_gz(
            f"raw/_reports/{season}.json.gz",
            {
                "season": season,
                "candidates": report.candidates,
                "empty_shifts": sorted(report.empty_shifts),
                "failed": report.failed,
            },
        )
    logger.info(report.summary())
    return report
