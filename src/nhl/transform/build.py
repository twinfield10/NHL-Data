"""Build ``processed/events/{season}.parquet`` from the raw per-game files."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import polars as pl

from nhl import config
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.transform.events import parse_events, roster_from_pbp
from nhl.transform.shifts import on_ice, parse_shifts

logger = logging.getLogger(__name__)


def build_game_parts(store: Store, season: int, game_id: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Parse one game's raw files into (events with on-ice players, merged shifts).

    Args:
        store: S3 store.
        season: 8-digit season id.
        game_id: NHL game id.

    Returns:
        ``(events, shifts)``; both empty if the raw play-by-play is missing. Shifts carry
        ``game_id`` and ``season`` and are empty when the game has no shift chart.
    """
    pbp = store.get_json_gz(keys.raw_pbp(season, game_id))
    if pbp is None:
        return pl.DataFrame(), pl.DataFrame()
    events = parse_events(pbp)
    raw_shifts = store.get_json_gz(keys.raw_shifts(season, game_id)) or {}
    shifts = parse_shifts(raw_shifts, roster_from_pbp(pbp))
    events = events.join(on_ice(events, shifts), on="event_idx", how="left")
    shifts = shifts.with_columns(pl.lit(game_id, pl.Int64).alias("game_id"), pl.lit(season, pl.Int32).alias("season"))
    return events, shifts.select("game_id", "season", "player_id", "team_id", "period", "start", "end", "is_goalie")


def build_game(store: Store, season: int, game_id: int) -> pl.DataFrame:
    """Parse one game's raw files into events with on-ice players attached.

    Args:
        store: S3 store.
        season: 8-digit season id.
        game_id: NHL game id.

    Returns:
        The game's event rows (empty if the raw play-by-play is missing).
    """
    return build_game_parts(store, season, game_id)[0]


def build_season(store: Store, start_year: int, workers: int = 8) -> pl.DataFrame:
    """Rebuild one season's event table from raw and write it to S3.

    Args:
        store: S3 store.
        start_year: Season start year.
        workers: Parallel parse threads.

    Returns:
        The season's event table.
    """
    season = config.season_id(start_year)
    game_ids = sorted(keys.game_id_from_key(k) for k in store.list_keys(keys.raw_pbp_prefix(season)))
    if not game_ids:
        raise FileNotFoundError(f"no raw play-by-play for {season}; run `nhl ingest` first")

    failures: dict[int, str] = {}

    def safe(gid: int) -> tuple[pl.DataFrame, pl.DataFrame] | None:
        try:
            return build_game_parts(store, season, gid)
        except Exception as exc:  # noqa: BLE001 - one malformed game must not sink the season
            failures[gid] = repr(exc)
            logger.warning("game %s failed to parse: %r", gid, exc)
            return None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        parts = [p for p in pool.map(safe, game_ids) if p is not None and not p[0].is_empty()]

    events = pl.concat([e for e, _ in parts], how="diagonal_relaxed").sort("game_id", "event_idx")
    shift_frames = [s for _, s in parts if not s.is_empty()]
    store.put_parquet(keys.events(season), events)
    if shift_frames:
        shifts = pl.concat(shift_frames, how="vertical_relaxed").sort("game_id", "player_id", "period", "start")
        store.put_parquet(keys.shifts(season), shifts)
    _log_quality(season, events, len(game_ids), failures)
    return events


def _log_quality(season: int, events: pl.DataFrame, n_games: int, failures: dict[int, str]) -> None:
    """Log coverage stats that would reveal silent data problems."""
    games = events.select("game_id").n_unique()
    no_shift = (
        events.group_by("game_id").agg(pl.col("home_skaters_on").is_null().all().alias("x")).get_column("x").sum()
    )
    sources = events.group_by("direction_source").len().sort("len", descending=True).rows()
    shots = events.filter(pl.col("event_type").is_in(["SHOT", "MISSED_SHOT", "GOAL"]))
    # In the offensive zone the vast majority of unblocked shots should land at x_abs > 25.
    oz_share = shots.select((pl.col("x_abs") > 25).mean()).item()
    logger.info(
        "%s: %d/%d games parsed (%d failed) | %s events | games w/o shifts: %d | "
        "direction sources: %s | unblocked shots with x_abs>25: %.1f%%",
        season, games, n_games, len(failures), f"{events.height:,}", no_shift, sources, 100 * (oz_share or 0),
    )
