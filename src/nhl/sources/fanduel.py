"""FanDuel (Virginia) player props: the sportsbook's public JSON, plain HTTP, no login.

Two endpoints on ``sbapi.va.sportsbook.fanduel.com/api``, both keyed by the app key the
website itself sends (``_ak``):

* ``content-managed-page?page=CUSTOM&customPageId=nhl`` lists the NHL board. Games are the
  entries of ``attachments.events`` named ``"Away Team @ Home Team"`` with an
  ``openDate`` (UTC); futures and specials have other names and are skipped.
* ``event-page?eventId=…&tab=…`` returns one tab of a game's markets. Player props sit
  on four tabs (:data:`TABS`): ``goals``, ``points-assists``, ``goalies`` and ``shots``.
  Each market has a ``marketType``, a ``marketName`` and ``runners`` with the price in
  ``winRunnerOdds.americanDisplayOdds.americanOddsInt``.

Market types (probed 2026-10-08), folded into the shared props vocabulary:

* **Two-way** (:data:`OVER_UNDER`): ``PLAYER_TOTAL_GOALS``, ``PLAYER_TOTAL_SHOTS``,
  ``PLAYER_TOTAL_SAVES``. Runners are ``"{player} - Over"`` / ``"{player} - Under"``
  with the line in ``handicap``.
* **Ladders** (:data:`LADDERS`): ``PLAYER_TO_RECORD_{N}+_{POINTS|ASSISTS|SHOTS_ON_GOAL}``
  and ``PLAYER_TO_SCORE_{N}+_GOALS`` list one runner per player; ``ANY_TIME_GOAL_SCORER``
  is goals 1+. ``PLAYER_ALT_SAVES`` is one market per goalie (``"{goalie} Alt Saves"``)
  with ``"26+"``-style runners. All become over N−0.5.
* **Scorer markets** (:data:`YES_NO`): ``FIRST_GOAL_SCORER`` and the first home/away team
  goal scorer, side ``yes``; "No Goalscorer …" runners are skipped.

Period props, parlays ("Any Time Goal Scorer / Team to Win"), specials and team markets
stay in the raw archive only.

**Teams.** A player runner's ``logo`` is the team jersey
(``…/nhl/ottawa_senators_jersey.png``). It is used only when it names one of the two
teams in the game (a goalie's logo has been seen naming a third team); otherwise the
team is left for the roster match in :func:`nhl.odds.props.resolve_player`.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any

import polars as pl

from nhl.ingest.http import BROWSER_UA, SourceUnavailable, WebClient
from nhl.odds.props import milestone_line, props_frame, store_props
from nhl.sources.common import archive_raw, utcnow
from nhl.sources.dailyfaceoff import PlayerResolver
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

BOOK = "FanDuel"
SOURCE = "fanduel"
API_BASE = "https://sbapi.va.sportsbook.fanduel.com/api"
#: The public app key the FanDuel website sends with every request.
APP_KEY = "FhMFpcPWXMeyZxOx"
#: Event-page tabs holding full-game player props.
TABS = ("goals", "points-assists", "goalies", "shots")
#: Requests per second: one board call plus four per game, so ~25 s for a 12-game slate.
RPS = 2.0
#: Only games starting within this many hours are fetched (the board lists a few days out).
HORIZON_H = 36

HEADERS: dict[str, str] = {"Accept": "application/json", "User-Agent": BROWSER_UA}

#: Two-way markets: marketType -> prop_type.
OVER_UNDER: dict[str, str] = {"PLAYER_TOTAL_GOALS": "goals", "PLAYER_TOTAL_SHOTS": "shots",
                              "PLAYER_TOTAL_SAVES": "saves"}
#: "N+" ladders, one runner per player: marketType pattern -> prop_type.
LADDERS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"^PLAYER_TO_RECORD_(\d+)\+_POINTS$"), "points"),
    (re.compile(r"^PLAYER_TO_RECORD_(\d+)\+_ASSISTS$"), "assists"),
    (re.compile(r"^PLAYER_TO_RECORD_(\d+)\+_SHOTS_ON_GOAL$"), "shots"),
    (re.compile(r"^PLAYER_TO_SCORE_(\d+)\+_GOALS$"), "goals"),
)
#: Line-less scorer markets: marketType -> prop_type.
YES_NO: dict[str, str] = {"FIRST_GOAL_SCORER": "first_goal", "FIRST_HOME_TEAM_GOAL_SCORER": "first_team_goal",
                          "FIRST_AWAY_TEAM_GOAL_SCORER": "first_team_goal"}
ANYTIME = "ANY_TIME_GOAL_SCORER"
ALT_SAVES = "PLAYER_ALT_SAVES"
ALT_SAVES_NAME = re.compile(r"^(?P<name>.+?) Alt Saves$")
TWO_WAY_RUNNER = re.compile(r"^(?P<name>.+?) - (?P<side>Over|Under)$")
#: FanDuel disambiguates namesakes with a team suffix: "Sebastian Aho (CAR)".
NAME_SUFFIX = re.compile(r"\s*\([A-Z]{2,3}\)$")
LOGO_TEAM = re.compile(r"/nhl/(?P<team>[a-z_]+?)(?:_jersey)?\.png$")


# ----------------------------------------------------------------------------- fetch --
def make_client() -> WebClient:
    """A rate-limited client with a browser User-Agent."""
    return WebClient(SOURCE, rps=RPS, headers=HEADERS)


def board_events(board: dict[str, Any]) -> list[dict[str, Any]]:
    """NHL games on the board: ``{event_id, away, home, start}``, by start time.

    Args:
        board: The ``content-managed-page`` payload.
    """
    out = []
    for event in ((board.get("attachments") or {}).get("events") or {}).values():
        away, sep, home = str(event.get("name") or "").partition(" @ ")
        start = event.get("openDate")
        if not sep or not start or not (resolve_team(away) and resolve_team(home)):
            continue
        out.append({"event_id": str(event.get("eventId")), "away": away.strip(), "home": home.strip(), "start": start})
    return sorted(out, key=lambda e: e["start"])


def fetch(client: WebClient | None, store: Store | None = None,
          horizon_h: float = HORIZON_H) -> tuple[dict[str, Any], datetime]:
    """One poll: the NHL board, then each upcoming game's prop tabs. Archives the raw payloads.

    Args:
        client: Injected for tests; a fresh :func:`make_client` otherwise.
        store: Where to archive the raw poll (skipped when None).
        horizon_h: Fetch games starting within this many hours.

    Returns:
        ``({"board": …, "events": {event_id: {tab: payload}}}, captured_at)``.

    Raises:
        SourceUnavailable: The board was refused. A refused tab is logged and skipped.
    """
    client = client or make_client()
    captured_at = utcnow()
    board = client.get_json(f"{API_BASE}/content-managed-page",
                            params={"page": "CUSTOM", "customPageId": "nhl", "_ak": APP_KEY,
                                    "timezone": "America/New_York"})
    cutoff = captured_at + timedelta(hours=horizon_h)
    events: dict[str, dict[str, Any]] = {}
    for event in board_events(board):
        start = datetime.fromisoformat(event["start"].replace("Z", "+00:00"))
        if start > cutoff or start < captured_at - timedelta(minutes=5):
            continue
        tabs: dict[str, Any] = {}
        for tab in TABS:
            try:
                tabs[tab] = client.get_json(f"{API_BASE}/event-page",
                                            params={"_ak": APP_KEY, "eventId": event["event_id"], "tab": tab})
            except SourceUnavailable as exc:
                logger.warning("[fanduel] %s %s tab skipped: %s", event["event_id"], tab, exc)
        events[event["event_id"]] = tabs
    raw = {"board": board, "events": events}
    if store is not None:
        archive_raw(store, SOURCE, captured_at, "props", raw)
    return raw, captured_at


# ------------------------------------------------------------------------- normalize --
def _price(runner: dict[str, Any]) -> float | None:
    american = ((runner.get("winRunnerOdds") or {}).get("americanDisplayOdds") or {}).get("americanOddsInt")
    return float(american) if american not in (None, 0) else None


def _logo_team(runner: dict[str, Any], teams: tuple[str | None, str | None]) -> str | None:
    """The runner's team from its jersey logo, only if it is one of the game's two teams."""
    match = LOGO_TEAM.search(str(runner.get("logo") or ""))
    team = resolve_team(match.group("team").replace("_", " ")) if match else None
    return team if team in teams else None


def market_rows(market: dict[str, Any], base: dict[str, Any], teams: tuple[str | None, str | None]) -> list[dict[str, Any]]:
    """One market's prop rows (empty for markets outside the vocabulary). Pure.

    Args:
        market: One entry of ``attachments.markets``.
        base: Columns shared by every row of the game (book, times, teams, event id).
        teams: The game's (away, home) tricodes, to validate logo teams.
    """
    kind = str(market.get("marketType") or "")
    if market.get("marketStatus", "OPEN") != "OPEN" or market.get("inPlay"):
        return []
    runners = [r for r in market.get("runners") or [] if r.get("runnerStatus", "ACTIVE") == "ACTIVE"]
    rows: list[dict[str, Any]] = []

    def add(name: str, prop_type: str, line: float | None, side: str, runner: dict[str, Any]) -> None:
        price = _price(runner)
        if price is not None and name:
            rows.append({**base, "player_name": NAME_SUFFIX.sub("", name).strip(), "team": _logo_team(runner, teams),
                         "prop_type": prop_type, "line": line, "side": side, "price": price})

    if kind in OVER_UNDER:
        for r in runners:
            if m := TWO_WAY_RUNNER.match(str(r.get("runnerName") or "")):
                add(m.group("name"), OVER_UNDER[kind], float(r.get("handicap") or 0), m.group("side").lower(), r)
    elif kind == ANYTIME:
        for r in runners:
            add(str(r.get("runnerName") or ""), "goals", 0.5, "over", r)
    elif kind == ALT_SAVES:
        if m := ALT_SAVES_NAME.match(str(market.get("marketName") or "")):
            for r in runners:
                line = milestone_line(r.get("runnerName"))
                if line is not None:
                    add(m.group("name"), "saves", line, "over", r)
    elif kind in YES_NO:
        for r in runners:
            if not str(r.get("runnerName") or "").startswith("No Goalscorer"):
                add(str(r.get("runnerName") or ""), YES_NO[kind], None, "yes", r)
    else:
        for pattern, prop_type in LADDERS:
            if m := pattern.match(kind):
                for r in runners:
                    add(str(r.get("runnerName") or ""), prop_type, int(m.group(1)) - 0.5, "over", r)
                break
    return rows


def _order(kind: str) -> int:
    """Two-way markets first: :func:`props_frame` keeps the first of two rows on one key."""
    return 0 if kind in OVER_UNDER else 1


def normalize(raw: dict[str, Any], captured_at: datetime) -> pl.DataFrame:
    """Turn one poll into a props frame (``game_id`` still null). No network, no writes.

    Args:
        raw: Output of :func:`fetch` (or a replayed archive).
        captured_at: Poll time (UTC).
    """
    games = {e["event_id"]: e for e in board_events(raw.get("board") or {})}
    rows: list[dict[str, Any]] = []
    for event_id, tabs in (raw.get("events") or {}).items():
        game = games.get(str(event_id))
        if game is None:
            continue
        teams = (resolve_team(game["away"]), resolve_team(game["home"]))
        base = {"book": BOOK, "captured_at": captured_at, "start_time": game["start"], "away_team": game["away"],
                "home_team": game["home"], "source_event_id": str(event_id)}
        # The same market appears on several tabs; keep one copy by marketId.
        markets: dict[str, dict[str, Any]] = {}
        for payload in tabs.values():
            markets.update(((payload or {}).get("attachments") or {}).get("markets") or {})
        for market in sorted(markets.values(), key=lambda m: _order(str(m.get("marketType")))):
            rows.extend(market_rows(market, base, teams))
    return props_frame(rows)


# ------------------------------------------------------------------------------ poll --
def poll(store: Store, games: pl.DataFrame, client: WebClient | None = None,
         resolver: PlayerResolver | None = None) -> int:
    """Fetch, archive, normalize and store one FanDuel props poll.

    Args:
        store: S3 store (archive + props table).
        games: ``processed/games.parquet``, used to resolve ``game_id``.
        client: Injected for tests.
        resolver: Shared player resolver; built from the store when needed.

    Returns:
        Prop rows written; 0 when the board was refused (logged, not raised).
    """
    try:
        raw, captured_at = fetch(client, store)
    except SourceUnavailable as exc:
        logger.warning("[fanduel] skipped: %s", exc)
        return 0
    return store_props(store, normalize(raw, captured_at), games, SOURCE, resolver)


__all__ = ["BOOK", "SOURCE", "TABS", "board_events", "fetch", "make_client", "market_rows", "normalize", "poll"]
