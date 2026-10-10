"""LowVig (= BetOnline) player props, through the Digital Sports Tech widget.

LowVig and BetOnline don't price player props themselves: both embed DST's bet builder
(``troya.xyz/betbuilder?sb=lowvig``), fed by ``bv2-us.digitalsportstech.com``. On
2026-10-08 ``sb=lowvig`` and ``sb=betonline`` returned identical prices, so one poll
covers both. It is stored as book ``LowVig``.

**Transport: a headless browser.** Every ``/api/dfm/*`` call is signed per request by the
widget's own HTTP client (single-use nonce), so plain HTTP gets
``403 invalid_security_headers``. As in ``ESPN_FFL/Scripts/bol_widget.py``, the poll opens
the widget in Chromium, clicks through the NHL markets and reads the responses the
widget fetches for itself. Nothing is forged and there's no login. Needs the ``browser``
extra (``pip install -e '.[browser]' && playwright install chromium``). A full walk is
~1-3 minutes.

**What the walk collects** (:data:`CATEGORIES`): each category tab (with its market rows
turned on where it opens empty), then a click on each game tile, which fetches that game's
market:

* ``dfm/marketsBySs`` (goals, assists, points, shots on goal, saves): ladders. Each
  player's ``markets[]`` have ``condition == 3`` ("at least") and ``value`` N, so they fold
  into over N−0.5.
* ``dfm/marketsByOu`` (points, shots on goal, saves): two-way at ``value``; ``condition``
  3 is the over and 1 the under.
* ``custom-markets/marketsByField`` (First Goalscorer): ``selections[]`` with the player
  name in ``type``; stored as ``first_goal`` / ``yes``.

Prices are decimal (``odds``) and are stored as American. Players carry a team code
(``"TB"``). Games come from the ``sgmGames?league=nhl`` response: the DST game id is
``providers[0].id``, ``team1`` is the home team and ``team2`` the away team, and ``date``
is UTC.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import polars as pl

from nhl.ingest.http import SourceUnavailable
from nhl.odds.core import american_price
from nhl.odds.props import props_frame, store_props
from nhl.sources.common import archive_raw, utcnow
from nhl.sources.dailyfaceoff import NAMESAKES, PlayerResolver, norm_name
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

BOOK = "LowVig"
SOURCE = "lowvig"
SKIN = "lowvig"
WIDGET_URL = f"https://troya.xyz/betbuilder?sb={SKIN}"
API_HOST = "bv2-us.digitalsportstech.com"
LEAGUE = "NHL"

NAV_TIMEOUT_MS = 60_000
RESPONSE_TIMEOUT_MS = 8_000
#: The beat the Angular app needs to re-render between clicks.
SETTLE_MS = 400

#: Payload ``statistic`` (lower case) -> prop_type.
STATISTICS: dict[str, str] = {"goals": "goals", "assists": "assists", "points": "points",
                              "shots on goal": "shots", "saves": "saves"}
FIRST_GOAL_FIELD = "first goalscorer"
#: DST tells namesakes apart by middle name; the roster can't (both are "Elias Pettersson"
#: on VAN). Checked 2026-10-08 by position and price: the forward is "Fredrik Elias". The
#: spellings live in :data:`nhl.sources.dailyfaceoff.NAMESAKES`, shared with every book.
#: ``condition`` codes on ``marketsByOu``.
OU_SIDES: dict[int, str] = {3: "over", 1: "under"}
#: ``condition`` code for "at least N" on ``marketsBySs``.
AT_LEAST = 3


@dataclass(frozen=True)
class Category:
    """One category tab in the widget and the market rows to turn on when it opens empty.

    Rows in a tab are toggles and stay on together: turning on all three "Over/Under" rows
    lists every game once per row (30 tiles for 10 games), and each tile fetches its own
    market. A tab that holds one market (Goals, Assists) lists its games as soon as it opens.
    """

    name: str
    rows: tuple[str, ...] = ()


#: Every category worth walking.
CATEGORIES: tuple[Category, ...] = (
    Category("Over/Under", ("Over/Under (Points)", "Over/Under (Shots On Goal)", "Over/Under (Saves)")),
    Category("Goals"),
    Category("Assists"),
    Category("Points"),
    Category("Shots On Goal", ("Shots On Goal",)),
    Category("Saves", ("Saves",)),
    Category("First Goal"),
)
#: Routes whose bodies are kept (a game tile click fetches one of these).
MARKET_ROUTES = ("dfm/marketsByOu", "dfm/marketsBySs", "custom-markets/marketsByField")

CATEGORY_SELECTOR = ".main-markets__item"
ROW_SELECTOR = ".main-stat__header"
LEAGUE_SELECTOR = ".ligues-slider__item"

#: Click the node in ``selector`` whose normalized text equals (else starts with) ``text``.
_JS_CLICK_IN = """
([selector, text]) => {
  const norm = s => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const want = norm(text);
  const nodes = [...document.querySelectorAll(selector)];
  const hit = nodes.find(e => norm(e.textContent) === want)
           || nodes.find(e => norm(e.textContent).startsWith(want));
  if (!hit) return false;
  hit.scrollIntoView({block: 'center'});
  let target = hit;
  if (getComputedStyle(hit).cursor !== 'pointer') {
    target = [hit, ...hit.querySelectorAll('*')].find(e => getComputedStyle(e).cursor === 'pointer') || hit;
  }
  target.click();
  return true;
}
"""
_JS_ROWS = "() => [...document.querySelectorAll('.main-stat__header')].length"
_JS_TILES = """
() => [...document.querySelectorAll('div.tiered-block__item__top')].filter(e => (e.innerText || '').includes('@')).length
"""
_JS_CLICK_TILE = """
(i) => {
  const t = [...document.querySelectorAll('div.tiered-block__item__top')].filter(e => (e.innerText || '').includes('@'));
  if (i >= t.length) return false;
  t[i].scrollIntoView({block: 'center'});
  t[i].click();
  return true;
}
"""


class WidgetError(SourceUnavailable):
    """The widget didn't render or changed shape. A skip for this poll, logged loudly."""


# ----------------------------------------------------------------------------- fetch --
def _parse(url: str) -> tuple[str, dict[str, str]]:
    """``(route, query)`` of a DST API URL (``dfm/marketsBySs``, ``{"gameId": "282744", …}``)."""
    parsed = urlparse(url)
    route = parsed.path.split("/api/", 1)[-1]
    query = {k: unquote(unquote(v[0])) for k, v in parse_qs(parsed.query).items()}
    return route, query


class Widget:
    """A live widget session that records the NHL games list and every market body fetched.

    Use as a context manager; :meth:`harvest` walks :data:`CATEGORIES`.
    """

    def __init__(self, url: str = WIDGET_URL, headless: bool = True) -> None:
        self.url, self.headless = url, headless
        self.games: list[dict[str, Any]] = []
        self.markets: dict[tuple[str, str, str], Any] = {}
        self._pw = self._browser = self._page = None

    def __enter__(self) -> Widget:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - install-time failure
            raise WidgetError("playwright is not installed: pip install -e '.[browser]' && "
                              "playwright install chromium") from exc
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.headless)
        context = self._browser.new_context(viewport={"width": 1600, "height": 1400}, locale="en-US",
                                            timezone_id="America/New_York")
        self._page = context.new_page()
        self._page.on("response", self._on_response)
        self._page.goto(self.url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        self._wait_for("() => document.querySelectorAll('[class*=tiered-block]').length > 0", "a market list")
        if not self._click(LEAGUE_SELECTOR, LEAGUE):
            raise WidgetError(f"no {LEAGUE} chip in the widget's league slider (no games listed?)")
        self._page.wait_for_timeout(2_500)
        return self

    def __exit__(self, *exc: object) -> None:
        for close in (lambda: self._browser.close(), lambda: self._pw.stop()):
            try:
                close()
            except Exception:  # noqa: BLE001 - teardown is best effort
                pass

    def _on_response(self, response: Any) -> None:
        if API_HOST not in response.url or response.status != 200:
            return
        route, query = _parse(response.url)
        if route == "sgmGames" and query.get("league") == "nhl":
            self.games = self._json(response) or self.games
        elif route in MARKET_ROUTES and "gameId" in query:
            body = self._json(response)
            if body is not None:
                self.markets[(route, query["gameId"], query.get("statistic", ""))] = body

    @staticmethod
    def _json(response: Any) -> Any:
        try:
            return response.json()
        except Exception:  # noqa: BLE001 - a body that isn't JSON is skipped
            return None

    def _wait_for(self, script: str, what: str) -> None:
        deadline = time.time() + NAV_TIMEOUT_MS / 1000
        while time.time() < deadline:
            try:
                if self._page.evaluate(script):
                    return
            except Exception:  # noqa: BLE001 - the page may still be navigating
                pass
            self._page.wait_for_timeout(500)
        raise WidgetError(f"{self.url} never rendered {what}; the widget may have changed")

    def _click(self, selector: str, text: str) -> bool:
        hit = self._page.evaluate(_JS_CLICK_IN, [selector, text])
        self._page.wait_for_timeout(SETTLE_MS)
        return bool(hit)

    def _tiles(self) -> int:
        return int(self._page.evaluate(_JS_TILES))

    def _open(self, category: Category) -> int:
        """Open a category tab and return how many game tiles it lists.

        A tab that opens empty gets its market rows turned on one by one. A row click is a
        toggle, so a click that lowers the tile count is undone.
        """
        if not self._click(CATEGORY_SELECTOR, category.name):
            logger.warning("[dst] no %r tab", category.name)
            return 0
        self._page.wait_for_timeout(900)
        if self._tiles() == 0:
            for row in category.rows:
                before = self._tiles()
                self._click(ROW_SELECTOR, row)
                self._page.wait_for_timeout(1_200)
                if self._tiles() < before or (before == 0 and self._tiles() == 0):
                    self._click(ROW_SELECTOR, row)
                    self._page.wait_for_timeout(1_200)
        return self._tiles()

    def harvest(self, categories: tuple[Category, ...] = CATEGORIES) -> None:
        """Open each category and click every game tile, recording the market responses."""
        for category in categories:
            tiles = self._open(category)
            got = 0
            for i in range(tiles):
                try:
                    # Any market route counts: tiles for rows left on (Exact, H2H) fetch routes not kept.
                    with self._page.expect_response(lambda r: API_HOST in r.url and "/markets" in r.url.lower(),
                                                    timeout=RESPONSE_TIMEOUT_MS):
                        self._page.evaluate(_JS_CLICK_TILE, i)
                    got += 1
                except Exception:  # noqa: BLE001 - a tile that fetched nothing is a gap, not a failure
                    logger.debug("[dst] %s tile %d fetched nothing", category.name, i)
                self._page.wait_for_timeout(SETTLE_MS)
            logger.info("[dst] %-14s %2d/%2d tiles fetched", category.name, got, tiles)


def fetch(store: Store | None = None, url: str = WIDGET_URL) -> tuple[dict[str, Any], datetime]:
    """One poll: walk the widget and return what it fetched. Archives the raw poll.

    Returns:
        ``({"games": sgmGames payload, "markets": [{route, game_id, statistic, payload}]}, captured_at)``.

    Raises:
        WidgetError: The widget didn't render, or listed no NHL markets at all.
    """
    captured_at = utcnow()
    with Widget(url) as widget:
        widget.harvest()
    raw = {"games": widget.games,
           "markets": [{"route": r, "game_id": g, "statistic": s, "payload": p}
                       for (r, g, s), p in widget.markets.items()]}
    if not raw["markets"]:
        raise WidgetError("the widget fetched no NHL markets")
    if store is not None:
        archive_raw(store, SOURCE, captured_at, "props", raw)
    return raw, captured_at


# ------------------------------------------------------------------------- normalize --
def game_meta(games: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """DST game id -> ``{away_team, home_team, start_time}`` from an ``sgmGames`` payload. Pure."""
    out = {}
    for game in games or []:
        ids = [str(p.get("id")) for p in game.get("providers") or [] if p.get("id") is not None]
        home, away = (game.get("team1") or [{}])[0], (game.get("team2") or [{}])[0]
        for gid in ids:
            out[gid] = {"home_team": home.get("title"), "away_team": away.get("title"), "start_time": game.get("date")}
    return out


def _american(decimal_odds: Any) -> float | None:
    try:
        decimal_odds = float(decimal_odds)
    except (TypeError, ValueError):
        return None
    return round(american_price(1.0 / decimal_odds)) if decimal_odds > 1.0 else None


def market_rows(entry: dict[str, Any], base: dict[str, Any]) -> list[dict[str, Any]]:
    """Prop rows for one harvested market body (one game, one statistic). Pure.

    Args:
        entry: ``{route, game_id, statistic, payload}`` from :func:`fetch`.
        base: Columns shared by every row of the game.
    """
    route, payload = entry["route"], entry["payload"]
    rows: list[dict[str, Any]] = []
    if route.endswith("marketsByField"):
        for block in payload or []:
            for sel in block.get("selections") or []:
                price = _american(sel.get("odds"))
                if sel.get("isActive", True) and price is not None and sel.get("type"):
                    rows.append({**base, "player_name": sel["type"], "team": None, "prop_type": "first_goal",
                                 "line": None, "side": "yes", "price": price})
        return rows
    two_way = route.endswith("marketsByOu")
    for block in payload or []:
        prop_type = STATISTICS.get(str(block.get("statistic") or "").strip().lower())
        if prop_type is None:
            continue
        for player in block.get("players") or []:
            for m in player.get("markets") or []:
                price, value = _american(m.get("odds")), m.get("value")
                if not m.get("isActive", True) or price is None or value is None:
                    continue
                if two_way:
                    side, line = OU_SIDES.get(m.get("condition")), float(value)
                else:
                    side, line = ("over" if m.get("condition") == AT_LEAST else None), float(value) - 0.5
                if side:
                    rows.append({**base, "player_name": player.get("name"), "team": player.get("team"),
                                 "prop_type": prop_type, "line": line, "side": side, "price": price})
    return rows


def normalize(raw: dict[str, Any], captured_at: datetime) -> pl.DataFrame:
    """Turn one harvest into a props frame (``game_id`` still null). No network, no writes.

    Two-way markets go first, so where a ladder rung lands on the same key (points 1+ and
    the points over 0.5) :func:`props_frame` keeps the two-way price.
    """
    meta = game_meta(raw.get("games") or [])
    entries = sorted(raw.get("markets") or [], key=lambda e: not e["route"].endswith("marketsByOu"))
    rows: list[dict[str, Any]] = []
    for entry in entries:
        game = meta.get(str(entry["game_id"]))
        if game is None:
            logger.debug("[dst] market for unknown game %s skipped", entry["game_id"])
            continue
        base = {"book": BOOK, "captured_at": captured_at, **game, "source_event_id": str(entry["game_id"])}
        rows.extend(market_rows(entry, base))
    for r in rows:
        r["player_id"] = NAMESAKES.get(norm_name(r["player_name"]))
    return props_frame(rows)


# ------------------------------------------------------------------------------ poll --
def poll(store: Store, games: pl.DataFrame, resolver: PlayerResolver | None = None) -> int:
    """Walk the widget, archive, normalize and store LowVig props.

    Returns:
        Prop rows written; 0 when the widget failed (logged, not raised).
    """
    try:
        raw, captured_at = fetch(store)
    except SourceUnavailable as exc:
        logger.warning("[dst] skipped: %s", exc)
        return 0
    return store_props(store, normalize(raw, captured_at), games, SOURCE, resolver)


__all__ = ["BOOK", "CATEGORIES", "SOURCE", "Widget", "WidgetError", "fetch", "game_meta", "market_rows", "normalize",
           "poll"]
