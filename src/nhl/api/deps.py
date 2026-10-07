"""FastAPI dependencies: one shared :class:`SiteData` per process."""

from __future__ import annotations

from datetime import date
from functools import lru_cache

from fastapi import HTTPException, Query

from nhl.api.data import SiteData
from nhl.api.serialize import today_et


@lru_cache(maxsize=1)
def get_data() -> SiteData:
    """The process-wide data access object (created on first request)."""
    return SiteData()


def game_day(day: str | None = Query(None, alias="date", description="YYYY-MM-DD (Eastern); default today")) -> date:
    """Parse the ``date`` query parameter, defaulting to today in Eastern time."""
    if day is None:
        return today_et()
    try:
        return date.fromisoformat(day)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"bad date {day!r}; use YYYY-MM-DD") from exc
