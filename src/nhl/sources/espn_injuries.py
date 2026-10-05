"""ESPN's league-wide NHL injury report, stored as status transitions.

``site.api.espn.com/.../hockey/nhl/injuries`` lists every currently injured, suspended or
day-to-day player grouped by team. Each poll archives the raw payload, normalizes one row
per listed athlete and appends transitions keyed (team, espn_athlete_id). A player who was
listed before and is missing from a fresh poll has come off the report (returned,
activated, or traded): that is recorded as a row with ``status = "Removed"``.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Any

import polars as pl

from nhl.ingest.http import SourceUnavailable, WebClient
from nhl.sources.common import append_transitions, archive_raw, utcnow
from nhl.sources.dailyfaceoff import EASTERN, PlayerResolver, parse_time, season_for_date
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/injuries"
REMOVED = "Removed"
KEYS = ("team", "espn_athlete_id")
VALUES = ("status", "short_comment", "reported_at")
_ATHLETE_ID_RE = re.compile(r"/id/(\d+)")

SCHEMA: dict[str, pl.DataType] = {
    "team": pl.String(),
    "espn_athlete_id": pl.Int64(),
    "player_name": pl.String(),
    "player_id": pl.Int64(),
    "position": pl.String(),
    "status": pl.String(),
    "injury_type": pl.String(),
    "return_date": pl.Date(),
    "short_comment": pl.String(),
    "long_comment": pl.String(),
    "reported_at": pl.Datetime("us", "UTC"),
    "captured_at": pl.Datetime("us", "UTC"),
}


def athlete_id(athlete: dict[str, Any]) -> int | None:
    """ESPN athlete id from the athlete's player-card link or headshot URL.

    The entry's own ``id`` is the injury (news item) id, not the athlete id.
    """
    hrefs = [link.get("href", "") for link in athlete.get("links") or []]
    hrefs.append((athlete.get("headshot") or {}).get("href", ""))
    for href in hrefs:
        match = _ATHLETE_ID_RE.search(href) or re.search(r"/players/full/(\d+)\.png", href)
        if match:
            return int(match.group(1))
    return None


def _return_date(value: str | None) -> date | None:
    """Parse ``details.returnDate`` (YYYY-MM-DD); None if absent or malformed."""
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


def normalize_injuries(
    payload: dict[str, Any], captured_at: datetime, resolver: PlayerResolver | None = None
) -> pl.DataFrame:
    """One row per listed athlete.

    Args:
        payload: Raw ESPN injuries JSON.
        captured_at: Poll time (UTC).
        resolver: Optional player-id resolver; ``player_id`` is null without one.

    Returns:
        Frame with :data:`SCHEMA` columns.
    """
    rows: list[dict[str, Any]] = []
    for team_block in payload.get("injuries") or []:
        for item in team_block.get("injuries") or []:
            athlete = item.get("athlete") or {}
            espn_team = athlete.get("team") or {}
            team = resolve_team(espn_team.get("abbreviation")) or resolve_team(team_block.get("displayName"))
            if team is None:
                logger.warning("espn injuries: unknown team %r", team_block.get("displayName"))
            name = athlete.get("displayName")
            details = item.get("details") or {}
            rows.append({
                "team": team,
                "espn_athlete_id": athlete_id(athlete),
                "player_name": name,
                "player_id": resolver.resolve(team, name) if resolver and name else None,
                "position": (athlete.get("position") or {}).get("abbreviation"),
                "status": item.get("status"),
                "injury_type": details.get("type"),
                "return_date": _return_date(details.get("returnDate")),
                "short_comment": item.get("shortComment"),
                "long_comment": item.get("longComment"),
                "reported_at": parse_time(item.get("date")),
                "captured_at": captured_at,
            })
    return pl.DataFrame(rows, schema=SCHEMA)


def removed_rows(existing: pl.DataFrame | None, incoming: pl.DataFrame, captured_at: datetime) -> pl.DataFrame:
    """Rows marking previously listed athletes that are absent from a fresh poll.

    For each (team, espn_athlete_id) whose latest stored status is not already
    ``"Removed"`` and which does not appear in ``incoming``, emit a row with
    ``status = "Removed"``, null comments/report time and the identity columns carried over.

    Args:
        existing: Stored injury transitions (or None).
        incoming: This poll's normalized rows.
        captured_at: Poll time (UTC).
    """
    if existing is None or existing.is_empty():
        return incoming.clear()
    latest = (
        existing.sort("captured_at")
        .group_by(list(KEYS), maintain_order=True)
        .last()
        .filter(pl.col("status") != REMOVED)
    )
    gone = latest.join(incoming.select(KEYS), on=list(KEYS), how="anti", nulls_equal=True)
    if gone.is_empty():
        return incoming.clear()
    cleared = [c for c in incoming.columns if c not in ("team", "espn_athlete_id", "player_name", "player_id", "position")]
    out = gone.select(incoming.columns).with_columns(
        *(pl.lit(None, dtype=incoming.schema[c]).alias(c) for c in cleared)
    ).with_columns(
        status=pl.lit(REMOVED),
        captured_at=pl.lit(captured_at, dtype=incoming.schema["captured_at"]),
    )
    return out.cast(incoming.schema)  # type: ignore[arg-type]


def poll_injuries(
    store: Store, client: WebClient | None = None, resolver: PlayerResolver | None = None
) -> int:
    """Poll ESPN injuries once and append transitions (including removals).

    An empty report is treated as a bad payload rather than "everyone recovered": nothing
    is marked removed in that case.

    Args:
        store: S3 store.
        client: Optional ESPN client.
        resolver: Optional shared player-id resolver.

    Returns:
        Number of rows written to :func:`keys.injuries`.
    """
    client = client or WebClient("espn", rps=1.0, headers={"Accept": "application/json"})
    captured_at = utcnow()
    try:
        payload = client.get_json(INJURIES_URL)
    except SourceUnavailable as exc:
        logger.warning("espn injuries skipped: %r", exc)
        return 0
    archive_raw(store, "espn", captured_at, "injuries", payload)
    resolver = resolver or PlayerResolver.from_store(store)
    incoming = normalize_injuries(payload, captured_at, resolver)
    resolver.log_unresolved("espn injuries")
    if incoming.is_empty():
        logger.warning("espn injuries: empty report; not marking anyone removed")
        return 0
    key = keys.injuries(season_for_date(captured_at.astimezone(EASTERN).date()))
    rows = pl.concat([incoming, removed_rows(store.get_parquet(key), incoming, captured_at)])
    return append_transitions(store, key, rows, KEYS, VALUES)
