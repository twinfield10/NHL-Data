"""Build the site's gold views (:mod:`nhl.site.views`) with the API's own code and write them.

Who publishes what:

* every ``nhl poll`` that passes its window: the parts its changes touch (``markets`` after odds
  or a reprice, ``props`` after prop quotes or a reprice, ``lineups`` after a reprice), plus any
  view that is missing or has gone stale (a game that just started switches to closing prices);
* ``nhl pregame`` / ``edges`` / ``props-edges`` / ``record-bet``: the parts they change;
* every site ratings build (:func:`nhl.site.tables.build`): the ratings boards;
* nightly (``nhl site-views --force``, ``nhl site-tables``): yesterday and today in full (final
  scores, graded bets), team matchups and the archetype history.

Publishers on this machine are serialised (:data:`LOCK_PATH`), so the per-day index is never
written by two at once. A view that fails to build is logged and left as it was (the API
falls back to building live).
"""

from __future__ import annotations

import fcntl
import json
import logging
import time
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from datetime import date, datetime, timezone

import polars as pl

from nhl.site import views
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

LOCK_PATH = "/tmp/nhl_data_views.lock"
#: View groups a pipeline step can mark as changed.
PARTS = ("markets", "lineups", "props")


@contextmanager
def _lock():
    with open(LOCK_PATH, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _put(store: Store, key: str, body: dict, valid_until: datetime | None = None, day: date | None = None,
         now: datetime | None = None) -> None:
    store.put_bytes(key, views.encode(body, valid_until, day, now), "application/json")


def _read_index(store: Store, day: date) -> dict:
    raw = store.get_bytes(views.index_key(day))
    try:
        return json.loads(raw) if raw else {}
    except ValueError:
        return {}


def publish_day(store: Store, day: date, parts: Iterable[str] = PARTS, force: bool = False,
                now: datetime | None = None) -> dict[str, float]:
    """Rebuild ``day``'s views: the slate and props board, and each game's market, lineups and
    props tabs.

    A view is rebuilt when it is missing, has passed its ``valid_until``, ``force`` is set, or
    its part is in ``parts`` and (for a game's tabs) the game hasn't started; a started game's
    tabs are frozen once rebuilt after puck drop.

    Returns:
        ``key -> seconds`` for each view written.
    """
    from nhl.api import markets as mk
    from nhl.api.data import SiteData
    from nhl.api.routers import games as games_router
    from nhl.api.routers import props as props_router
    from nhl.api.routers import slate as slate_router

    parts = set(parts)
    with _lock():
        now = now or datetime.now(timezone.utc)
        data = SiteData(store)
        todays = data.games().filter(pl.col("game_date") == day).sort("start_time_et", "game_id").to_dicts()
        starts = {g["game_id"]: mk.start_utc(g) for g in todays}
        upcoming = [s for s in starts.values() if s is not None and s > now]
        index = _read_index(store, day)
        written: dict[str, float] = {}

        def due(key: str, part: str, started: bool = False) -> bool:
            entry = index.get(key)
            if force or entry is None or entry.get("v") != views.VERSION:
                return True
            if entry.get("valid_until") and now >= datetime.fromisoformat(entry["valid_until"]):
                return True
            return part in parts and not started

        def build(key: str, make: Callable[[], dict], valid_until: datetime | None) -> None:
            t0 = time.monotonic()
            try:
                body = make()
                _put(store, key, body, valid_until, now=now)
            except Exception:  # one view failing must not stop the rest
                logger.exception("view %s failed", key)
                return
            index[key] = {"v": views.VERSION, "built_at": now.isoformat(),
                          "valid_until": valid_until.isoformat() if valid_until else None}
            written[key] = round(time.monotonic() - t0, 2)

        if todays and due(views.slate_key(day), "markets"):
            build(views.slate_key(day), lambda: slate_router.build_slate(data, day), min(upcoming, default=None))
        if todays and due(views.props_key(day), "props"):
            build(views.props_key(day), lambda: props_router.build_props(data, day), None)
        for g in todays:
            start = starts[g["game_id"]]
            started = start is not None and start <= now
            until = None if started or start is None else start
            for tab, part, make in (("game", "markets", games_router.build_game),
                                    ("lineups", "lineups", games_router.build_lineups),
                                    ("props", "props", props_router.build_game_props)):
                key = views.game_key(g["game_id"], tab)
                if due(key, part, started):
                    # Lineups don't change at puck drop (their sources are cut at the start time).
                    build(key, lambda make=make, g=g: make(data, g), None if tab == "lineups" else until)
        if written:
            store.put_bytes(views.index_key(day), json.dumps(index).encode(), "application/json")
    logger.info("views %s: %d written (%s)", day, len(written), ", ".join(sorted(parts)) or "stale only")
    return written


def publish_quietly(store: Store, day: date | None = None, parts: Iterable[str] = PARTS, force: bool = False) -> None:
    """:func:`publish_day` (default today, Eastern), logging instead of raising: a failed publish
    must not fail the pipeline step that triggered it."""
    from nhl.api.serialize import today_et

    day = day or today_et()
    try:
        publish_day(store, day, parts, force)
    except Exception:
        logger.exception("views %s: publish failed (the API builds live)", day)


def publish_ratings(store: Store, day: date) -> dict[str, float]:
    """The ratings boards for ``day`` (players, teams, lines for this season and last), from the
    site tables just built. Called at the end of :func:`nhl.site.tables.build`."""
    from nhl.api.data import SiteData
    from nhl.api.routers import ratings

    data = SiteData(store)
    r = data.rankings(day)
    if r is None:
        return {}
    written = {}
    jobs = [("players", lambda: ratings.build_players(data, day)), ("teams", lambda: ratings.build_teams(data, day))]
    jobs += [(f"lines_{s}", lambda s=s: ratings.build_lines(data, day, s)) for s in (r["season"], r["season"] - 10001)]
    with _lock():
        for board, make in jobs:
            t0 = time.monotonic()
            try:
                _put(store, views.ratings_key(board), make(), day=day)
            except Exception:
                logger.exception("ratings view %s failed", board)
                continue
            written[board] = round(time.monotonic() - t0, 2)
    return written


def publish_seasons(store: Store, day: date) -> dict[str, float]:
    """Nightly season-level gold: every team's matchup view (this season and last) and the
    archetype history table (:data:`nhl.site.tables.ARCHETYPE_HISTORY`)."""
    from nhl.api.data import SiteData
    from nhl.api.routers import context, style
    from nhl.site import tables as site_tables

    data = SiteData(store)
    r = data.rankings(day)
    if r is None:
        return {}
    current, previous = r["season"], r["season"] - 10001
    written = {}
    with _lock():
        for season in (current, previous):
            t0 = time.monotonic()
            try:
                for team_id, body in context.build_matchups(data, season, current, previous).items():
                    _put(store, views.matchups_key(season, team_id), body)
            except Exception:
                logger.exception("matchup views %s failed", season)
                continue
            written[f"matchups_{season}"] = round(time.monotonic() - t0, 2)
        t0 = time.monotonic()
        try:
            store.put_parquet(site_tables.ARCHETYPE_HISTORY, style.archetype_history(data, current))
            written["archetype_history"] = round(time.monotonic() - t0, 2)
        except Exception:
            logger.exception("archetype history failed")
    return written
