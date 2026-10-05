"""LowVig (BetOnline's offering API): poll the NHL board and store the line as it moves.

LowVig and BetOnline are one book behind two skins. The board is public (no credentials)
but the edge in front of it refuses anything that does not look like a browser. Ported
from rebirtha-nfl, where each of these was established by measurement:

* ``gsetting: bolsassite`` is mandatory (a skin selector, not a token); without it the API
  answers 401.
* The ``User-Agent`` header name must be canonically cased; ``user-agent`` gets a 403.
  :class:`nhl.ingest.http.WebClient` sends header names exactly as given.
* HTTP/2 is refused; ``requests``/``urllib3`` only speak HTTP/1.1, so nothing to do.
* ``utc-offset: 0`` makes ``WagerCutOff`` UTC. ``PeriodWagerCutOff`` does not shift and is
  never read.

Feed shape, confirmed on the NHL board:

* The board (``offering-by-league``) lists games; prices come from one ``get-event`` per
  game. ``GamesDescription`` is a list of ``{GameDate, Game}`` blocks and ``Game`` is a
  dict for one game and a list for several.
* **A zero price means "not posted"**, never even money. Team totals are the usual case:
  they post on game day, so tomorrow's games carry ``TeamTotalLine`` blocks of zeros.
* ``PeriodEvents`` entries carry ``Number``/``Name`` on NHL (``0 Game``, ``1 1st
  Period``), unlike NFL where they were positional only. ``Number`` is used when present,
  the position otherwise; the name is the fallback for numbers outside :data:`PERIODS`.
  Game-day boards post the 1st period (moneyline, -0.5/+0.5 puck line, 1.5 total); the
  period moneyline is two-way (it sums to ~1.03, i.e. a tie is a push), so it is stored
  as ``moneyline`` with ``period="p1"``. Tomorrow's games carry only the full game.
* The puck line is the feed's ``SpreadLine`` (``Point`` from that team's perspective).
  The game total hangs off a third sibling block (``TotalLine.TotalLine``), team totals
  off each team's ``TeamTotalLine``. NHL totals may be whole numbers (6.0).
* **Alternates** come in the same ``get-event`` payload, under
  ``EventOffering.ContestTypes[].DescriptionGroup[]`` (the "NHL Props" contest type: the
  ``AdditionalMarketCount`` on the event counts these). Groups named ``Alternate Total
  7.5``, ``Alternate Puckline {team}`` and ``Alternate Team Total {team}`` hold one
  contestant per outcome: ``Name`` (team / Over / Under), ``ThresholdLine`` (the line from
  that contestant's perspective; ``ThresholdType`` ``S`` spread, ``P`` points) and the
  price in ``Line.MoneyLine.Line``. They are stored as ``is_alternate=True`` rows; a rung
  equal to the main line is skipped. The other groups (first to score, margin, odd/even
  ...) are game props outside the odds schema and stay in the archive only. The same
  groups are also served by ``get-contests-by-contest-type2`` (``ContestType: "NHL
  Props"``); no extra call is needed.
* **Regulation 3-way** is a separate league on the board, ``NHL 3 WAY``
  (``offering-by-league`` with ``League: "nhl 3 way"``), one extra call per poll: its
  games carry Home/Away/Draw ``MoneyLine`` prices and their own ``GameId``. Stored as
  ``moneyline_3way`` / ``period="reg"`` with sides home/away/draw.
* **No player props for anonymous clients.** BetOnline's player props are a third-party
  widget whose URL comes from ``api-offering-ext.../offexternal/get-url-widget``, which
  answers 401 without a customer login; nothing is captured.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import polars as pl

from nhl import config
from nhl.ingest.http import BROWSER_UA, SourceUnavailable, WebClient
from nhl.odds.core import MARKET_KEY, TRANSITION_VALUES, attach_game_ids, odds_frame
from nhl.odds.store import store_odds
from nhl.sources.common import archive_raw, utcnow
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

BOOK = "LowVig"
SOURCE = "lowvig"
API_BASE = "https://api-offering.betonline.ag/api/offering/Sports"
SPORT = "hockey"
LEAGUE = "nhl"
#: The skin selector. Absent, the API answers 401.
GSETTING = "bolsassite"
#: Requests per second: one board call plus one call per game, so ~8 s for a full slate.
RPS = 2.0
#: ``PeriodEvents`` number -> stored period.
PERIODS: dict[int, str] = {0: "game", 1: "p1", 2: "p2", 3: "p3"}
#: Period names, for entries whose number is missing or unexpected.
PERIOD_NAMES: dict[str, str] = {"game": "game", "1st period": "p1", "2nd period": "p2",
                                "3rd period": "p3", "regulation": "reg", "regulation time": "reg"}
#: Board league holding the regulation three-way moneyline.
THREE_WAY_LEAGUE = "nhl 3 way"
ALT_TOTAL = re.compile(r"^Alternate Total\b", re.I)
ALT_PUCKLINE = re.compile(r"^Alternate Puck ?line\b", re.I)
ALT_TEAM_TOTAL = re.compile(r"^Alternate Team Total\s+(?P<team>.+)$", re.I)

HEADERS: dict[str, str] = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": "https://www.betonline.ag",
    "Referer": "https://www.betonline.ag/sportsbook",
    # Canonical casing is load-bearing; see the module docstring.
    "User-Agent": BROWSER_UA,
    "gsetting": GSETTING,
    "utc-offset": "0",
}


# ----------------------------------------------------------------------------- fetch --
def make_client() -> WebClient:
    """A rate-limited client carrying the browser-shaped headers the edge requires."""
    return WebClient(SOURCE, rps=RPS, headers=HEADERS)


def game_ids(offering: dict[str, Any]) -> list[int]:
    """Every game id on an ``offering-by-league`` board, de-duplicated, in board order.

    Args:
        offering: Decoded board payload.

    Returns:
        BetOnline game ids.
    """
    blocks = ((offering or {}).get("GameOffering") or {}).get("GamesDescription") or []
    if isinstance(blocks, dict):
        blocks = [blocks]
    seen: dict[int, None] = {}
    for block in blocks:
        entry = (block or {}).get("Game")
        for game in entry if isinstance(entry, list) else [entry]:
            gid = (game or {}).get("GameId")
            if gid is not None:
                seen.setdefault(int(gid), None)
    return list(seen)


def fetch(client: WebClient | None = None, store: Store | None = None) -> tuple[dict[str, Any], datetime]:
    """Pull the board and every game's markets, archiving both raw payloads.

    One refused ``get-event`` is logged and skipped so it does not discard the games
    already collected; a refused board call propagates as :class:`SourceUnavailable`.

    Args:
        client: Injected for tests; built with :func:`make_client` otherwise.
        store: Where to archive raw payloads; nothing is archived when None.

    Returns:
        ``({"offering": ..., "events": {game_id: event}, "threeway": ...}, captured_at)``;
        ``threeway`` is the ``NHL 3 WAY`` board (None if that call was refused).
    """
    client = client or make_client()
    captured_at = utcnow()
    offering = client.post_json(
        f"{API_BASE}/offering-by-league",
        {"Sport": SPORT, "League": LEAGUE, "ScheduleText": None, "filterTime": 0},
    ) or {}
    ids = game_ids(offering)
    logger.info("[lowvig] board lists %d game(s)", len(ids))
    events: dict[str, Any] = {}
    for gid in ids:
        try:
            events[str(gid)] = client.post_json(
                f"{API_BASE}/get-event",
                {"Sport": SPORT, "League": LEAGUE, "gameID": gid, "ScheduleText": None},
            ) or {}
        except SourceUnavailable as exc:
            logger.warning("[lowvig] game %s refused; continuing (%s)", gid, exc)
    threeway: dict[str, Any] | None
    try:
        threeway = client.post_json(
            f"{API_BASE}/offering-by-league",
            {"Sport": SPORT, "League": THREE_WAY_LEAGUE, "ScheduleText": None, "filterTime": 0},
        ) or {}
    except SourceUnavailable as exc:
        logger.warning("[lowvig] 3-way board refused; continuing (%s)", exc)
        threeway = None
    if store is not None:
        archive_raw(store, SOURCE, captured_at, "offering", offering)
        archive_raw(store, SOURCE, captured_at, "events", events)
        if threeway is not None:
            archive_raw(store, SOURCE, captured_at, "threeway", threeway)
    return {"offering": offering, "events": events, "threeway": threeway}, captured_at


# ------------------------------------------------------------------------- normalize --
def _price(value: Any) -> float | None:
    """An American price, or None when the market is not posted (the feed's zero filler)."""
    if value in (None, 0, 0.0):
        return None
    return float(value)


def _point(block: dict | None) -> float | None:
    """The handicap/total on a market block, or None when it carries none."""
    point = (block or {}).get("Point")
    return None if point is None else float(point)


def _utc(text: str | None) -> str | None:
    """The cutoff as ISO-8601 UTC (the client asks for ``utc-offset: 0``), or None."""
    if not text or text.startswith("0001-01-01"):
        return None  # the feed's null date
    text = text.strip()
    return text if text.endswith("Z") else f"{text}Z"


def _period_events(event: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """``(period, Event block)`` for every stored period of a ``get-event`` payload.

    Uses the entry's ``Number`` when the feed provides it (NHL does) and falls back to the
    name, then to position, which is all the NFL feed offered. Unknown periods are skipped.
    """
    entries = ((event or {}).get("EventOffering") or {}).get("PeriodEvents") or []
    out: list[tuple[str, dict[str, Any]]] = []
    for index, entry in enumerate(entries):
        entry = entry or {}
        number = entry.get("Number", index)
        period = PERIODS.get(number) if isinstance(number, int) else None
        period = period or PERIOD_NAMES.get(str(entry.get("Name") or "").strip().lower())
        if period is None:
            logger.debug("[lowvig] period %r (%s) not stored", number, entry.get("Name"))
            continue
        if entry.get("Event"):
            out.append((period, entry["Event"]))
    return out


def _full_game(event: dict[str, Any]) -> dict[str, Any] | None:
    """The full-game ``Event`` block of a ``get-event`` payload, or None."""
    return next((block for period, block in _period_events(event) if period == "game"), None)


def _common(block: dict[str, Any], captured_at: datetime) -> dict[str, Any]:
    """Fields shared by every row of one game block."""
    return {
        "book": BOOK,
        "captured_at": captured_at,
        "start_time": _utc(block.get("WagerCutOff")),
        "away_team": block["AwayTeam"],
        "home_team": block["HomeTeam"],
        "price_point": "live",
        "source_event_id": str(block.get("GameId")),
    }


def _period_rows(block: dict[str, Any], period: str, common: dict[str, Any]) -> list[dict[str, Any]]:
    """Main-line rows (moneyline, puck line, total, team totals) of one period block."""
    rows: list[dict[str, Any]] = []

    def add(market: str, side: str, line: float | None, price: Any, subject: str = "game") -> None:
        american = _price(price)
        if american is None:
            return  # not posted; zero is filler
        rows.append({**common, "period": period, "is_alternate": False, "market": market, "subject": subject,
                     "side": side, "line": line, "price": american})

    home, away = block.get("HomeLine") or {}, block.get("AwayLine") or {}
    for side, side_block in (("home", home), ("away", away)):
        add("moneyline", side, None, (side_block.get("MoneyLine") or {}).get("Line"))
        puck = side_block.get("SpreadLine") or {}
        add("puckline", side, _point(puck), puck.get("Line"))

    total = (block.get("TotalLine") or {}).get("TotalLine") or {}
    for side in ("Over", "Under"):
        add("total", side.lower(), _point(total), (total.get(side) or {}).get("Line"))

    for subject, side_block in (("home", home), ("away", away)):
        team_total = side_block.get("TeamTotalLine") or {}
        for side in ("Over", "Under"):
            add("team_total", side.lower(), _point(team_total),
                (team_total.get(side) or {}).get("Line"), subject=subject)
    return rows


def _contests(event: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """``(group description, contest)`` for every contest under ``EventOffering.ContestTypes``."""
    out: list[tuple[str, dict[str, Any]]] = []
    for contest_type in ((event or {}).get("EventOffering") or {}).get("ContestTypes") or []:
        for group in (contest_type or {}).get("DescriptionGroup") or []:
            for contest in (group or {}).get("Contests") or []:
                out.append((str((group or {}).get("Description") or "").strip(), contest or {}))
    return out


def alternate_rows(
    event: dict[str, Any], block: dict[str, Any], common: dict[str, Any], main: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Alternate puck lines, totals and team totals from a game's ``ContestTypes``. Pure.

    Args:
        event: Decoded ``get-event`` payload.
        block: Its full-game ``Event`` block (team names).
        common: Shared row fields from :func:`_common`.
        main: The game's main-line rows; an alternate rung equal to the main line is skipped.

    Returns:
        ``is_alternate=True`` row dicts (period ``game``; a "1st Period" group gets ``p1``).
    """
    teams = {block["HomeTeam"].strip().lower(): "home", block["AwayTeam"].strip().lower(): "away"}
    main_lines = {(r["period"], r["market"], r["subject"], r["side"], r["line"]) for r in main}
    rows: list[dict[str, Any]] = []
    for description, contest in _contests(event):
        period = "p1" if "1st period" in description.lower() else "game"
        team_total = ALT_TEAM_TOTAL.match(description)
        if ALT_TOTAL.match(description):
            market, subject = "total", "game"
        elif ALT_PUCKLINE.match(description):
            market, subject = "puckline", "game"
        elif team_total:
            market, subject = "team_total", teams.get(team_total.group("team").strip().lower())
            if subject is None:
                continue
        else:
            continue  # a game prop outside the odds schema
        for contestant in contest.get("Contestants") or []:
            name = str(contestant.get("Name") or "").strip().lower()
            side = teams.get(name) if market == "puckline" else name
            price = _price(((contestant.get("Line") or {}).get("MoneyLine") or {}).get("Line"))
            line = contestant.get("ThresholdLine")
            if side not in ("home", "away", "over", "under") or price is None or line is None:
                continue
            if (period, market, subject, side, float(line)) in main_lines:
                continue  # the main line, already stored
            rows.append({**common, "period": period, "is_alternate": True, "market": market,
                         "subject": subject, "side": side, "line": float(line), "price": price})
    return rows


def event_rows(event: dict[str, Any], captured_at: datetime) -> list[dict[str, Any]]:
    """Flatten one game's markets into one row per posted price. Pure.

    Every stored period's main lines, then the full game's alternates.

    Args:
        event: Decoded ``get-event`` payload.
        captured_at: Poll time (UTC).

    Returns:
        Row dicts for :func:`nhl.odds.core.odds_frame`.
    """
    blocks = _period_events(event)
    game = next((block for period, block in blocks if period == "game"), None)
    if not game or not game.get("HomeTeam") or not game.get("AwayTeam"):
        return []
    common = _common(game, captured_at)
    rows: list[dict[str, Any]] = []
    for period, block in blocks:
        # Period blocks repeat the game's teams; their own GameId/cutoff are not used.
        rows.extend(_period_rows(block, period, common))
    rows.extend(alternate_rows(event, game, common, rows))
    return rows


def three_way_rows(board: dict[str, Any] | None, captured_at: datetime) -> list[dict[str, Any]]:
    """Regulation three-way moneylines from the ``NHL 3 WAY`` board. Pure.

    Returns:
        ``moneyline_3way`` rows, period ``reg``, sides home/away/draw.
    """
    blocks = ((board or {}).get("GameOffering") or {}).get("GamesDescription") or []
    rows: list[dict[str, Any]] = []
    for entry in blocks if isinstance(blocks, list) else [blocks]:
        games = (entry or {}).get("Game")
        for game in games if isinstance(games, list) else [games]:
            if not game or not game.get("HomeTeam") or not game.get("AwayTeam"):
                continue
            common = _common(game, captured_at)
            for side, key in (("home", "HomeLine"), ("away", "AwayLine"), ("draw", "DrawLine")):
                price = _price(((game.get(key) or {}).get("MoneyLine") or {}).get("Line"))
                if price is not None:
                    rows.append({**common, "period": "reg", "is_alternate": False, "market": "moneyline_3way",
                                 "subject": "game", "side": side, "line": None, "price": price})
    return rows


def normalize(raw: dict[str, Any], captured_at: datetime) -> pl.DataFrame:
    """Turn one poll (``{"offering", "events"}``) into odds rows. No network, no writes.

    Args:
        raw: Output of :func:`fetch` (or a replayed archive; ``threeway`` is optional).
        captured_at: Poll time (UTC).

    Returns:
        An :func:`~nhl.odds.core.odds_frame` with ``game_id`` still null.
    """
    rows: list[dict[str, Any]] = []
    for event in (raw.get("events") or {}).values():
        rows.extend(event_rows(event, captured_at))
    rows.extend(three_way_rows(raw.get("threeway"), captured_at))
    return odds_frame(rows)


# ------------------------------------------------------------------------------ poll --
def poll(store: Store, games: pl.DataFrame, client: WebClient | None = None) -> int:
    """Fetch, archive, normalize and store one LowVig poll.

    Args:
        store: S3 store (archive + odds tables).
        games: ``processed/games.parquet``, used to resolve ``game_id``.
        client: Injected for tests.

    Returns:
        Rows written; 0 when the edge refused (logged, not raised). LowVig offers no
        player props to anonymous clients, so there is no props call.
    """
    try:
        raw, captured_at = fetch(client, store)
    except SourceUnavailable as exc:
        logger.warning("[lowvig] skipped: %s", exc)
        return 0
    return store_odds(store, normalize(raw, captured_at), games, SOURCE)


__all__ = ["BOOK", "SOURCE", "alternate_rows", "fetch", "game_ids", "event_rows", "normalize", "poll",
           "make_client", "three_way_rows"]
