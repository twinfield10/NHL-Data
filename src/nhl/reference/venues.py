"""Venues, per-game venue assignments and schedule/travel context.

Three tables:

* ``keys.VENUES`` - the curated reference table, written from the checked-in
  ``venues.csv`` next to this module: one row per venue *name variant* seen in NHL
  regular-season/playoff games since 2010-11 (arenas get renamed, so several names share
  an ``arena_id``), with coordinates, IANA timezone, elevation, indoor/outdoor and the home
  team that used it with a season range (``home_first_season``/``home_last_season``, null =
  ongoing). Tenancy is per arena, so a game is a *home* game when any row of its arena
  names the game's home team for that season.
* ``keys.GAME_VENUES`` - one row per game with a stored raw play-by-play: the top-level
  ``venue`` / ``venueLocation`` / ``venueUTCOffset`` / ``specialEvent`` fields, plus
  ``is_neutral_site`` (the venue is not the nominal home team's arena that season: Global
  Series, outdoor games, the 2020 playoff hubs, Lake Tahoe) and ``is_outdoor``.
* ``processed/schedule_context/{season}.parquet`` - :func:`travel_features`: per
  team-game rest, density, travel distance, timezone shift and home-stand/road-trip
  position, computed only from that team's *earlier* games plus the scheduled venue of the
  current game (both known pregame).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

VENUES_CSV = Path(__file__).with_name("venues.csv")
EARTH_RADIUS_KM = 6371.0088

VENUE_SCHEMA: dict[str, pl.DataType] = {
    "venue_name": pl.String(),
    "arena_id": pl.String(),
    "city": pl.String(),
    "latitude": pl.Float64(),
    "longitude": pl.Float64(),
    "timezone": pl.String(),
    "elevation_m": pl.Float64(),
    "is_outdoor": pl.Boolean(),
    "home_team": pl.String(),
    "home_first_season": pl.Int32(),
    "home_last_season": pl.Int32(),
    "coord_confidence": pl.String(),
    "notes": pl.String(),
}

GAME_VENUE_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "season": pl.Int32(),
    "game_type": pl.Int8(),
    "game_date": pl.Date(),
    "start_time_utc": pl.Datetime("us", "UTC"),
    "home_abbr": pl.String(),
    "away_abbr": pl.String(),
    "venue_name": pl.String(),
    "venue_location": pl.String(),
    "venue_utc_offset": pl.String(),
    "special_event": pl.String(),
}


def schedule_context_key(season: int) -> str:
    """Key for a season's per team-game schedule/travel context."""
    return f"processed/schedule_context/{season}.parquet"


# --------------------------------------------------------------------------- venue table
def load_venue_csv(path: Path = VENUES_CSV) -> pl.DataFrame:
    """Read and validate the curated venue CSV.

    Args:
        path: CSV location (defaults to the checked-in file).

    Raises:
        ValueError: On duplicate venue names, unknown timezones, out-of-range coordinates or
            name variants of one arena that disagree on location.
    """
    frame = pl.read_csv(path, schema=VENUE_SCHEMA)
    dupes = frame.filter(pl.col("venue_name").is_duplicated())["venue_name"].unique().to_list()
    if dupes:
        raise ValueError(f"duplicate venue names: {dupes}")
    for tz in frame["timezone"].unique().to_list():
        ZoneInfo(tz)  # raises on an unknown zone
    bad = frame.filter(~pl.col("latitude").is_between(-90, 90) | ~pl.col("longitude").is_between(-180, 180))
    if not bad.is_empty():
        raise ValueError(f"coordinates out of range: {bad['venue_name'].to_list()}")
    spread = frame.group_by("arena_id").agg(
        (pl.col("latitude").max() - pl.col("latitude").min()).alias("dlat"),
        (pl.col("longitude").max() - pl.col("longitude").min()).alias("dlon"),
        pl.col("timezone").n_unique().alias("ntz"),
    )
    bad = spread.filter((pl.col("dlat") > 0.02) | (pl.col("dlon") > 0.02) | (pl.col("ntz") > 1))
    if not bad.is_empty():
        raise ValueError(f"name variants of one arena disagree: {bad['arena_id'].to_list()}")
    return frame


def write_venues(store: Store, path: Path = VENUES_CSV) -> pl.DataFrame:
    """Write the curated venue table to ``keys.VENUES`` and return it."""
    frame = load_venue_csv(path)
    store.put_parquet(keys.VENUES, frame)
    return frame


def arenas(venues: pl.DataFrame) -> pl.DataFrame:
    """One row per ``arena_id`` with location attributes (name variants share them)."""
    return venues.group_by("arena_id").agg(
        pl.col("latitude").first(),
        pl.col("longitude").first(),
        pl.col("timezone").first(),
        pl.col("elevation_m").first(),
        pl.col("is_outdoor").first(),
    )


def tenancy(venues: pl.DataFrame) -> pl.DataFrame:
    """(arena_id, home_team, home_first_season, home_last_season) for arenas with a home team."""
    return (
        venues.filter(pl.col("home_team").is_not_null())
        .select("arena_id", "home_team", "home_first_season", "home_last_season")
        .unique()
    )


def _in_tenancy(season: pl.Expr) -> pl.Expr:
    return (season >= pl.col("home_first_season")) & (
        pl.col("home_last_season").is_null() | (season <= pl.col("home_last_season"))
    )


def default_home_arenas(venues: pl.DataFrame, team_seasons: pl.DataFrame) -> pl.DataFrame:
    """Each team's home arena per season according to the tenancy ranges.

    When two arenas cover the same season (e.g. NYI split Barclays/Nassau 2018-20) the
    tenancy that started most recently wins.

    Args:
        venues: Venue table.
        team_seasons: Frame with ``team`` and ``season``.

    Returns:
        (team, season, arena_id); teams without a covering tenancy are absent.
    """
    return (
        team_seasons.select("team", "season").unique()
        .join(tenancy(venues), left_on="team", right_on="home_team")
        .filter(_in_tenancy(pl.col("season")))
        .sort("home_first_season", descending=True)
        .group_by("team", "season", maintain_order=True)
        .agg(pl.col("arena_id").first())
    )


# --------------------------------------------------------------------------- game venues
def extract_game_venue(raw: dict[str, Any]) -> dict[str, Any]:
    """Venue fields of one raw gamecenter play-by-play payload.

    Args:
        raw: Raw play-by-play JSON.
    """
    def text(field_name: str) -> str | None:
        value = raw.get(field_name)
        return value.get("default") if isinstance(value, dict) else value

    start = raw.get("startTimeUTC")
    special = (raw.get("specialEvent") or {}).get("name") or {}
    return {
        "game_id": raw.get("id"),
        "season": raw.get("season"),
        "game_type": raw.get("gameType"),
        "game_date": date.fromisoformat(raw["gameDate"]) if raw.get("gameDate") else None,
        "start_time_utc": datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None,
        "home_abbr": (raw.get("homeTeam") or {}).get("abbrev"),
        "away_abbr": (raw.get("awayTeam") or {}).get("abbrev"),
        "venue_name": text("venue"),
        "venue_location": text("venueLocation"),
        "venue_utc_offset": raw.get("venueUTCOffset"),
        "special_event": special.get("default"),
    }


def classify_game_venues(game_venues: pl.DataFrame, venues: pl.DataFrame) -> pl.DataFrame:
    """Attach ``arena_id``, ``is_neutral_site`` and ``is_outdoor`` to raw game venues.

    Args:
        game_venues: Rows with :data:`GAME_VENUE_SCHEMA`.
        venues: Venue table.

    Returns:
        ``game_venues`` plus arena_id / is_neutral_site / is_outdoor (null when the venue
        name is not in the venue table; see :func:`unmapped_venues`).
    """
    base = game_venues.drop("arena_id", "is_neutral_site", "is_outdoor", strict=False)
    mapped = base.join(venues.select("venue_name", "arena_id", "is_outdoor"), on="venue_name", how="left")
    home = (
        mapped.select("game_id", "season", "home_abbr", "arena_id")
        .join(tenancy(venues), left_on=["arena_id", "home_abbr"], right_on=["arena_id", "home_team"])
        .filter(_in_tenancy(pl.col("season")))
        .select("game_id")
        .unique()
        .with_columns(pl.lit(True).alias("_home"))
    )
    return (
        mapped.join(home, on="game_id", how="left")
        .with_columns(
            pl.when(pl.col("arena_id").is_null())
            .then(None)
            .otherwise(pl.col("_home").is_null())
            .alias("is_neutral_site")
        )
        .drop("_home")
        .sort("game_id")
    )


def unmapped_venues(game_venues: pl.DataFrame, venues: pl.DataFrame) -> pl.DataFrame:
    """Venue names in ``game_venues`` that have no row in the venue table, with game counts."""
    return (
        game_venues.join(venues.select("venue_name"), on="venue_name", how="anti")
        .group_by("venue_name", "venue_location")
        .agg(pl.len().alias("games"), pl.col("game_id").min().alias("first_game_id"))
        .sort("games", descending=True)
    )


def timezone_mismatches(game_venues: pl.DataFrame, venues: pl.DataFrame) -> pl.DataFrame:
    """Games whose ``venueUTCOffset`` disagrees with the curated timezone at puck drop.

    A non-empty result points at a wrong ``timezone`` in ``venues.csv``.
    """
    rows = (
        game_venues.filter(pl.col("venue_utc_offset").is_not_null() & pl.col("start_time_utc").is_not_null())
        .join(venues.select("venue_name", "timezone"), on="venue_name")
        .select("game_id", "venue_name", "timezone", "start_time_utc", "venue_utc_offset")
    )
    expected = [
        _format_offset(start.astimezone(ZoneInfo(tz)).utcoffset())
        for tz, start in rows.select("timezone", "start_time_utc").iter_rows()
    ]
    return rows.with_columns(pl.Series("expected_offset", expected, dtype=pl.String)).filter(
        pl.col("expected_offset") != pl.col("venue_utc_offset")
    )


def _format_offset(delta: timedelta | None) -> str | None:
    if delta is None:
        return None
    minutes = int(delta.total_seconds() // 60)
    sign = "+" if minutes >= 0 else "-"
    return f"{sign}{abs(minutes) // 60:02d}:{abs(minutes) % 60:02d}"


def pbp_seasons(store: Store) -> list[int]:
    """Seasons that have a ``raw/pbp/{season}/`` prefix in the bucket."""
    paginator = store.client.get_paginator("list_objects_v2")
    seasons: list[int] = []
    for page in paginator.paginate(Bucket=store.bucket, Prefix="raw/pbp/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            part = prefix["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if part.isdigit():
                seasons.append(int(part))
    return sorted(seasons)


def read_game_venues(store: Store, season: int, skip: set[int] | None = None, workers: int = 8) -> pl.DataFrame:
    """Extract venue fields from every stored raw play-by-play file of a season.

    Args:
        store: S3 store (raw pbp is served from the local cache when present).
        season: 8-digit season id.
        skip: Game ids already extracted.
        workers: Reader threads.
    """
    skip = skip or set()
    todo = [k for k in store.list_keys(keys.raw_pbp_prefix(season)) if keys.game_id_from_key(k) not in skip]

    def one(key: str) -> dict[str, Any] | None:
        raw = store.get_json_gz(key)
        return extract_game_venue(raw) if raw else None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        rows = [r for r in pool.map(one, todo) if r is not None]
    return pl.DataFrame(rows, schema=GAME_VENUE_SCHEMA)


def update_game_venues(store: Store, venues: pl.DataFrame, seasons: Iterable[int] | None = None) -> pl.DataFrame:
    """Incrementally (re)build ``keys.GAME_VENUES`` from stored raw play-by-play.

    Games already in the table are not re-read; classification is recomputed for every
    row so edits to ``venues.csv`` take effect.

    Args:
        store: S3 store.
        venues: Venue table.
        seasons: Seasons to scan (default: every season with raw pbp).

    Returns:
        The full table written.
    """
    existing = store.get_parquet(keys.GAME_VENUES)
    base = pl.DataFrame(schema=GAME_VENUE_SCHEMA) if existing is None else existing.select(list(GAME_VENUE_SCHEMA))
    seen = set(base["game_id"].to_list())
    frames = [base]
    for season in seasons if seasons is not None else pbp_seasons(store):
        fresh = read_game_venues(store, season, skip=seen)
        logger.info("%s: %d new game venue rows", season, fresh.height)
        frames.append(fresh)
    table = classify_game_venues(pl.concat(frames).unique("game_id", keep="last"), venues)
    store.put_parquet(keys.GAME_VENUES, table)
    return table


# --------------------------------------------------------------------------- travel
def haversine_km(lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr) -> pl.Expr:
    """Great-circle distance in km between two coordinate expressions (degrees)."""
    p1, p2 = lat1.radians(), lat2.radians()
    dlat = p2 - p1
    dlon = (lon2 - lon1).radians()
    a = (dlat / 2).sin().pow(2) + p1.cos() * p2.cos() * (dlon / 2).sin().pow(2)
    return 2 * EARTH_RADIUS_KM * a.sqrt().arcsin()


def _utc_offsets(pairs: pl.DataFrame) -> pl.DataFrame:
    """UTC offset in hours of each (timezone, game_date) pair at local noon."""
    rows = [
        (tz, day, datetime(day.year, day.month, day.day, 12, tzinfo=ZoneInfo(tz)).utcoffset().total_seconds() / 3600)
        for tz, day in pairs.drop_nulls().unique().iter_rows()
    ]
    return pl.DataFrame(rows, schema={"timezone": pl.String(), "game_date": pl.Date(), "utc_offset": pl.Float64()}, orient="row")


def played_or_upcoming(games: pl.DataFrame, today: date | None = None) -> pl.DataFrame:
    """Drop scheduled games that were never played (e.g. unneeded "if necessary" playoff games).

    Keeps final games plus non-final games dated on/after ``today`` (default: the latest
    final game's date in ``games``).
    """
    if "is_final" not in games.columns:
        return games
    cutoff = today or games.filter(pl.col("is_final"))["game_date"].max()
    if cutoff is None:
        return games
    return games.filter(pl.col("is_final") | (pl.col("game_date") >= cutoff))


def travel_features(
    games: pl.DataFrame, game_venues: pl.DataFrame, venues: pl.DataFrame, today: date | None = None
) -> pl.DataFrame:
    """Per team-game schedule and travel context, using only the team's earlier games.

    The venue of a game comes from ``game_venues`` (``venue_source = "pbp"``) or, when the
    game has no stored play-by-play yet (upcoming games, partially downloaded seasons),
    from the home team's tenancy arena (``"home_default"``). "Previous location" for a
    team's first game of a season is its home arena.

    Args:
        games: Games table (game_id, season, game_date, home_abbr, away_abbr; optionally
            start_time_et and is_final - never-played games are dropped).
        game_venues: Output of :func:`classify_game_venues` (game_id, arena_id,
            is_neutral_site).
        venues: Venue table.
        today: Non-final games before this date are treated as never played (see
            :func:`played_or_upcoming`).

    Returns:
        One row per (game_id, team) with: season, game_date, team, opponent, is_home,
        is_neutral_site, at_home (home and not neutral), arena_id, venue_source,
        days_rest (null for a team's first game of the season), is_back_to_back,
        games_in_last_7_days (team games in the 7 days before game_date), travel_km
        (great-circle from the previous game's arena, or from home for the first game),
        tz_shift_hours (current minus previous venue UTC offset on game day),
        tz_shift_from_home, elevation_m, homestand_game (1-based position within a run of
        consecutive at-home games, else null) and road_trip_game (same for away/neutral
        runs).
    """
    sched = played_or_upcoming(games, today)
    order_col = "start_time_et" if "start_time_et" in sched.columns else "game_id"
    sched = sched.select("game_id", "season", "game_date", "home_abbr", "away_abbr", pl.col(order_col).alias("_order"))

    gv = game_venues.select("game_id", "arena_id", "is_neutral_site").filter(pl.col("arena_id").is_not_null())
    homes = default_home_arenas(venues, sched.select(pl.col("home_abbr").alias("team"), "season"))
    sched = (
        sched.join(gv, on="game_id", how="left")
        .join(homes.rename({"team": "home_abbr", "arena_id": "_default_arena"}), on=["home_abbr", "season"], how="left")
        .with_columns(
            pl.when(pl.col("arena_id").is_not_null()).then(pl.lit("pbp")).otherwise(pl.lit("home_default")).alias("venue_source"),
            pl.coalesce("arena_id", "_default_arena").alias("arena_id"),
            pl.col("is_neutral_site").fill_null(False),
        )
        .drop("_default_arena")
    )

    side = ["game_id", "season", "game_date", "_order", "arena_id", "venue_source", "is_neutral_site"]
    long = pl.concat([
        sched.select(*side, pl.col("home_abbr").alias("team"), pl.col("away_abbr").alias("opponent"), pl.lit(True).alias("is_home")),
        sched.select(*side, pl.col("away_abbr").alias("team"), pl.col("home_abbr").alias("opponent"), pl.lit(False).alias("is_home")),
    ])

    # Team home arena (for the season's first trip and tz_shift_from_home): the arena of
    # most of the team's non-neutral home games that season, else the tenancy default.
    observed = (
        long.filter(pl.col("is_home") & ~pl.col("is_neutral_site") & (pl.col("venue_source") == "pbp"))
        .group_by("team", "season", "arena_id").len()
        .sort("len", "arena_id", descending=[True, False])
        .group_by("team", "season", maintain_order=True)
        .agg(pl.col("arena_id").first().alias("home_arena_id"))
    )
    fallback = default_home_arenas(venues, long.select("team", "season")).rename({"arena_id": "_tenancy_arena"})
    long = (
        long.join(observed, on=["team", "season"], how="left")
        .join(fallback, on=["team", "season"], how="left")
        .with_columns(pl.coalesce("home_arena_id", "_tenancy_arena").alias("home_arena_id"))
        .drop("_tenancy_arena")
    )

    geo = arenas(venues)
    long = (
        long.join(geo.select("arena_id", "latitude", "longitude", "timezone", "elevation_m"), on="arena_id", how="left")
        .join(
            geo.select(pl.col("arena_id").alias("home_arena_id"), pl.col("latitude").alias("home_lat"),
                       pl.col("longitude").alias("home_lon"), pl.col("timezone").alias("home_tz")),
            on="home_arena_id", how="left",
        )
        .sort("team", "game_date", "_order", "game_id")
    )

    by = ["team", "season"]
    prev = lambda c: pl.col(c).shift(1).over(by)  # noqa: E731 - local shorthand
    first = pl.int_range(pl.len()).over(by) == 0
    week_ago = pl.col("game_date") - pl.duration(days=7)
    long = long.with_columns(
        pl.when(first).then(None).otherwise((pl.col("game_date") - prev("game_date")).dt.total_days()).cast(pl.Int32).alias("days_rest"),
        pl.sum_horizontal(
            *((pl.col("game_date").shift(k).over(by) >= week_ago).fill_null(False).cast(pl.Int8) for k in range(1, 8))
        ).cast(pl.Int8).alias("games_in_last_7_days"),
        pl.when(first).then(pl.col("home_lat")).otherwise(prev("latitude")).alias("_prev_lat"),
        pl.when(first).then(pl.col("home_lon")).otherwise(prev("longitude")).alias("_prev_lon"),
        pl.when(first).then(pl.col("home_tz")).otherwise(prev("timezone")).alias("_prev_tz"),
        (pl.col("is_home") & ~pl.col("is_neutral_site")).alias("at_home"),
    )

    offsets = _utc_offsets(
        pl.concat([
            long.select("timezone", "game_date"),
            long.select(pl.col("_prev_tz").alias("timezone"), "game_date"),
            long.select(pl.col("home_tz").alias("timezone"), "game_date"),
        ])
    )
    long = (
        long.join(offsets.rename({"utc_offset": "_off"}), on=["timezone", "game_date"], how="left")
        .join(offsets.rename({"timezone": "_prev_tz", "utc_offset": "_prev_off"}), on=["_prev_tz", "game_date"], how="left")
        .join(offsets.rename({"timezone": "home_tz", "utc_offset": "_home_off"}), on=["home_tz", "game_date"], how="left")
        .sort("team", "game_date", "_order", "game_id")
    )

    run = (pl.col("at_home") != pl.col("at_home").shift(1).over(by)).fill_null(True).cum_sum().over(by)
    long = long.with_columns(run.alias("_run")).with_columns(
        (pl.int_range(pl.len()).over([*by, "_run"]) + 1).cast(pl.Int16).alias("_run_pos")
    )
    return (
        long.with_columns(
            (pl.col("days_rest") == 1).alias("is_back_to_back"),
            haversine_km(pl.col("_prev_lat"), pl.col("_prev_lon"), pl.col("latitude"), pl.col("longitude")).alias("travel_km"),
            (pl.col("_off") - pl.col("_prev_off")).alias("tz_shift_hours"),
            (pl.col("_off") - pl.col("_home_off")).alias("tz_shift_from_home"),
            pl.when(pl.col("at_home")).then(pl.col("_run_pos")).alias("homestand_game"),
            pl.when(~pl.col("at_home")).then(pl.col("_run_pos")).alias("road_trip_game"),
        )
        .select(
            "game_id", "season", "game_date", "team", "opponent", "is_home", "is_neutral_site", "at_home",
            "arena_id", "venue_source", "days_rest", "is_back_to_back", "games_in_last_7_days", "travel_km",
            "tz_shift_hours", "tz_shift_from_home", "elevation_m", "homestand_game", "road_trip_game",
        )
        .sort("game_date", "game_id", "is_home")
    )


def build_schedule_context(store: Store, seasons: Iterable[int], venues: pl.DataFrame | None = None) -> dict[int, int]:
    """Write :func:`travel_features` for each season to :func:`schedule_context_key`.

    Args:
        store: S3 store.
        seasons: 8-digit season ids to write.
        venues: Venue table (default: ``keys.VENUES``, else the checked-in CSV).

    Returns:
        Rows written per season.
    """
    if venues is None:
        venues = store.get_parquet(keys.VENUES)
    if venues is None:
        venues = load_venue_csv()
    today = datetime.now(ZoneInfo("America/New_York")).date()
    games = store.read_parquet_required(keys.GAMES)
    game_venues = store.get_parquet(keys.GAME_VENUES)
    if game_venues is None:
        game_venues = classify_game_venues(pl.DataFrame(schema=GAME_VENUE_SCHEMA), venues)
    written: dict[int, int] = {}
    for season in seasons:
        frame = travel_features(games.filter(pl.col("season") == season), game_venues, venues, today)
        if frame.is_empty():
            continue
        store.put_parquet(schedule_context_key(season), frame)
        written[season] = frame.height
    return written
