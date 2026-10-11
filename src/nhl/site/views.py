"""Gold views: the site's API responses, prebuilt by the pipeline and served as stored bytes.

The API's heavier endpoints (slate, a game's market / lineups / props tabs, the props board,
the ratings boards and team matchups) used to join, replay and aggregate on request. The
pipeline now builds those payloads with the same code (:mod:`nhl.site.publish`) and writes the
finished JSON here; the API returns the bytes untouched and only builds live when a view is
missing or no longer valid.

Layout (all ``application/json``):

* ``site/views/days/{date}/slate.json`` and ``.../props.json``: ``/api/slate`` and ``/api/props``;
* ``site/views/days/{date}/index.json``: what the publisher built for that day and until when;
* ``site/views/games/{game_id}/{game,lineups,props}.json``: the game page's three tabs;
* ``site/views/ratings/{players,teams,lines_{season}}.json``: the ratings boards;
* ``site/views/matchups/{season}/{team_id}.json``: a team's line-matchup matrices.

Every view carries ``_view``: ``v`` (:data:`VERSION`), ``built_at``, ``valid_until`` (when a
time-dependent view goes stale, e.g. a game's market tab switches to closing prices at puck
drop; null when it doesn't) and ``day`` (for views that answer one date, e.g. ratings).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

from fastapi.encoders import jsonable_encoder

PREFIX = "site/views/"
#: Bump when a view's payload shape changes, so the API ignores views built by older code.
VERSION = 2


def slate_key(day: date) -> str:
    """``/api/slate?date=day``."""
    return f"{PREFIX}days/{day.isoformat()}/slate.json"


def props_key(day: date) -> str:
    """``/api/props?date=day`` (default ``min_edge``)."""
    return f"{PREFIX}days/{day.isoformat()}/props.json"


def index_key(day: date) -> str:
    """The publisher's record of ``day``'s views: ``{key: {built_at, valid_until}}``."""
    return f"{PREFIX}days/{day.isoformat()}/index.json"


def game_key(game_id: int, tab: str) -> str:
    """``/api/games/{game_id}`` (``tab="game"``), ``.../lineups`` or ``.../props``."""
    return f"{PREFIX}games/{game_id}/{tab}.json"


def ratings_key(board: str) -> str:
    """``/api/ratings/{players,teams}`` or ``lines_{season}``."""
    return f"{PREFIX}ratings/{board}.json"


def matchups_key(season: int, team_id: int) -> str:
    """``/api/ratings/teams/{team_id}/matchups?season=season``."""
    return f"{PREFIX}matchups/{season}/{team_id}.json"


def encode(body: dict, valid_until: datetime | None = None, day: date | None = None,
           now: datetime | None = None) -> bytes:
    """A response body as the API would send it (FastAPI's JSON encoding), plus ``_view``."""
    meta = {"v": VERSION, "built_at": (now or datetime.now(timezone.utc)).isoformat(),
            "valid_until": valid_until.isoformat() if valid_until else None,
            "day": day.isoformat() if day else None}
    return json.dumps(jsonable_encoder({**body, "_view": meta}), ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode()


def meta(raw: bytes) -> dict:
    """The ``_view`` block of a stored view (empty for anything unreadable)."""
    try:
        return json.loads(raw).get("_view") or {}
    except (ValueError, AttributeError):
        return {}


def is_valid(m: dict, now: datetime, day: date | None = None) -> bool:
    """Whether a view with metadata ``m`` can still be served at ``now`` (for ``day``)."""
    if m.get("v") != VERSION:
        return False
    if m.get("valid_until") and now >= datetime.fromisoformat(m["valid_until"]):
        return False
    return day is None or m.get("day") == day.isoformat()
