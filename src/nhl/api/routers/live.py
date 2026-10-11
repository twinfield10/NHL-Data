"""``/api/live``: in-game scores and box scores, for following pending bets while games are on.

**Not a second source of truth.** Grading still runs off the nightly catalog; this only shows
how a started bet is doing before that. It reads the NHL's public endpoints on request:

* ``/api/live?date=YYYY-MM-DD``: every game on the date from ``api-web.nhle.com/v1/score``:
  state, period, clock, score and shots, plus links to the NHL GameCenter and ESPN's game page
  (``espn_url``, found on ESPN's scoreboard by date and teams; ESPN+ games stream from there).
  The NHL's game id *is* our ``game_id``, so the join is exact.
* ``/api/live/{game_id}/boxscore``: per-player goals, assists, points, shots, blocks and saves
  (the prop stats we bet), keyed by the NHL ``player_id`` our props use.

**Why a server route.** api-web sends no CORS headers, and ESPN refuses browser requests, so
the browser cannot call either directly. Responses are cached in memory for
:data:`SCORE_TTL_SECONDS` / :data:`BOX_TTL_SECONDS`, so any number of open pages costs one
upstream call per window. The module keeps no state beyond that cache and needs no bucket
access, so when the site is deployed (e.g. a Cloudflare Worker in front of static pages) the
same two routes port as-is.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import date

import requests
from fastapi import APIRouter, HTTPException, Query

from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/live", tags=["live"])

NHL_SCORE = "https://api-web.nhle.com/v1/score/{day}"
NHL_BOX = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
ESPN_SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates={day}"
NHL_SITE = "https://www.nhl.com"
#: The NHL caches its score feed for ~20 s; asking more often returns the same bytes.
SCORE_TTL_SECONDS = 20.0
BOX_TTL_SECONDS = 30.0
#: ESPN ids don't change; refetched only to pick up a newly listed game.
ESPN_TTL_SECONDS = 1800.0
TIMEOUT = 8.0
HEADERS = {"User-Agent": "Mozilla/5.0 (NHL-Data site)", "Accept": "application/json"}

_cache: dict[str, tuple[float, object]] = {}
_lock = threading.Lock()


def _cached(key: str, ttl: float, load):
    """``load()`` cached for ``ttl`` seconds; on an upstream failure the last value (if any) is
    served stale rather than failing the page."""
    now = time.monotonic()
    with _lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    try:
        value = load()
    except (requests.RequestException, ValueError):
        logger.exception("live: %s failed", key)
        if hit:
            return hit[1]
        raise HTTPException(status_code=502, detail="live scores unavailable")
    with _lock:
        _cache[key] = (now, value)
    return value


def _get(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


#: api-web ``gameState`` -> ``pre`` / ``live`` / ``final``. CRIT is the last minutes of a close game.
STATE = {"FUT": "pre", "PRE": "pre", "LIVE": "live", "CRIT": "live", "OFF": "final", "FINAL": "final"}


def _period_label(number: int | None, kind: str | None) -> str | None:
    if not number:
        return None
    if kind == "SO":
        return "SO"
    if kind == "OT" or number > 3:
        return "OT" if number == 4 else f"{number - 3}OT"
    return ("1st", "2nd", "3rd")[number - 1]


def parse_score(payload: dict) -> list[dict]:
    """The score feed as one flat row per game."""
    out = []
    for g in payload.get("games", []):
        pd = g.get("periodDescriptor") or {}
        clock = g.get("clock") or {}
        state = STATE.get(g.get("gameState"), "pre")
        period = _period_label(pd.get("number"), pd.get("periodType"))
        if state == "live":
            detail = f"{period} INT" if clock.get("inIntermission") else f"{clock.get('timeRemaining', '')} {period}".strip()
        elif state == "final":
            last = (g.get("gameOutcome") or {}).get("lastPeriodType")
            detail = "Final" + (f"/{last}" if last in ("OT", "SO") else "")
        else:
            detail = None
        home, away = g.get("homeTeam") or {}, g.get("awayTeam") or {}
        link = g.get("gameCenterLink")
        out.append({
            "game_id": g["id"], "state": state, "game_state": g.get("gameState"), "detail": detail,
            "period": pd.get("number"), "period_type": pd.get("periodType"),
            "clock": clock.get("timeRemaining"), "intermission": bool(clock.get("inIntermission")),
            "home_abbr": home.get("abbrev"), "away_abbr": away.get("abbrev"),
            "home_score": home.get("score"), "away_score": away.get("score"),
            "home_sog": home.get("sog"), "away_sog": away.get("sog"),
            "start_utc": g.get("startTimeUTC"),
            "gamecenter_url": f"{NHL_SITE}{link}" if link else None,
        })
    return out


def parse_espn(payload: dict, day: date) -> dict[tuple[str, str], str]:
    """``(away, home) tricode -> ESPN game page`` from ESPN's scoreboard."""
    out = {}
    for e in payload.get("events", []):
        comp = (e.get("competitions") or [{}])[0]
        sides = {c.get("homeAway"): resolve_team((c.get("team") or {}).get("abbreviation"), day) for c in comp.get("competitors", [])}
        if sides.get("home") and sides.get("away") and e.get("id"):
            out[(sides["away"], sides["home"])] = f"https://www.espn.com/nhl/game/_/gameId/{e['id']}"
    return out


def _espn_links(day: date) -> dict[tuple[str, str], str]:
    try:
        return _cached(f"espn/{day}", ESPN_TTL_SECONDS,
                       lambda: parse_espn(_get(ESPN_SCOREBOARD.format(day=day.strftime("%Y%m%d"))), day))
    except HTTPException:  # the links are a convenience; scores still answer
        return {}


@router.get("")
def live_scores(day: date = Query(..., alias="date")) -> dict:
    """Every game on ``date``: state, score, clock and links (see the module docstring)."""
    games = _cached(f"score/{day}", SCORE_TTL_SECONDS, lambda: parse_score(_get(NHL_SCORE.format(day=day.isoformat()))))
    espn = _espn_links(day)
    return {"date": day, "games": [{**g, "espn_url": espn.get((g["away_abbr"], g["home_abbr"]))} for g in games]}


#: Box-score field per prop stat (goalies: saves).
SKATER_STATS = {"goals": "goals", "assists": "assists", "points": "points", "shots": "sog", "blocks": "blockedShots"}


def parse_boxscore(payload: dict) -> dict:
    """``{state, players: [{player_id, team, goals, assists, points, shots, blocks, saves}]}``."""
    players = []
    for side in ("awayTeam", "homeTeam"):
        team = (payload.get(side) or {}).get("abbrev")
        stats = (payload.get("playerByGameStats") or {}).get(side) or {}
        for group in ("forwards", "defense", "goalies"):
            for p in stats.get(group, []):
                row = {"player_id": p.get("playerId"), "team": team, "position": p.get("position")}
                row.update({k: p.get(v) for k, v in SKATER_STATS.items()})
                row["saves"] = p.get("saves") if group == "goalies" else None
                players.append(row)
    return {"game_id": payload.get("id"), "state": STATE.get(payload.get("gameState"), "pre"), "players": players}


@router.get("/{game_id}/boxscore")
def live_boxscore(game_id: int) -> dict:
    """One game's per-player stats so far (see :func:`parse_boxscore`)."""
    return _cached(f"box/{game_id}", BOX_TTL_SECONDS, lambda: parse_boxscore(_get(NHL_BOX.format(game_id=game_id))))
