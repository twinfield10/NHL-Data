"""4Casters exchange: poll the NHL order book and store the takeable price as it moves.

One ``POST /exchange/getOrderbook {"leagueRequested": "NHL"}`` returns every game with full
depth; there is no per-game fan-out. Ported from rebirtha-nfl.

**Sign-in.** Until 2026-10-06 the order book was anonymous; since then an anonymous read
is refused with 403 ``ANON_READ_REFUSED`` ("Sign in to read the board"). With
``CAST4_USER`` / ``CAST4_PASS`` set, :class:`SignedInClient` logs in (``POST /user/login``,
the token is ``data.user.auth``), sends the token as the ``Authorization`` header, caches it
under the local cache directory so polls don't log in every few minutes, and signs in
again once when a call is refused (an expired token). Without credentials the source is
skipped with a warning saying so.

What shapes the normalizer:

* **A price is only real at a size.** Each resting order carries ``sumUntaken`` (dollars
  still available) and the top of book is often a few dollars deep. :func:`vwap` walks
  the book to :data:`UNIT_SIZE` of exposure and returns nothing when less than
  :data:`MIN_FILL_RATIO` of it fills, so a thin market is absent rather than quoted at a
  price nobody can take. :data:`EXCHANGE_VIG` is added back so exchange prices compare
  with a book's vigged ones.
* **Moneylines are lists; puck lines and totals are dicts keyed by line string**
  (``awaySpreads["1.5"]``, ``over["5.5"]``), each key from that side's perspective. The
  main rung is chosen by ``mainHomeSpread``/``mainAwaySpread``/``mainTotal``; every other
  rung is stored as an alternate (``is_alternate=True``) priced by the same VWAP, and only
  when it fills (a thin rung is simply absent).
* NHL quirks: games far enough out have ``mainTotal == 0`` and ``mainHomeSpread == 0``
  with empty ladders, meaning *no main line yet*, so 0 is treated as absent. Ladders carry
  whole-number and 0/±0.5 rungs (draw-no-bet style) beside the ±1.5 puck line; the main
  markers always point at ±1.5.
* Periods would be child games (``parentGameID`` set, ``periodName`` e.g. "1st Period");
  :data:`PERIODS` maps them. None has been posted on NHL so far (``getSingleOrderbook``
  returns an empty ``childGames``), so in practice only the full game is stored. There is
  no 1x2 (three-way) order book on NHL either, though the volume summary has the slot.
* **Props** are a separate league on the same endpoint: ``{"leagueRequested":
  "NHL-PROPS"}`` returns one "special" game per player market (``eventName`` "KYLE
  CONNOR (SHOTS ON GOAL)", ``parentGameID`` = the matchup's game, ``participants`` = the
  two teams) with an over/under ladder like a game total. Types seen: GOALS (over 0.5 is
  the anytime scorer), ASSISTS, POINTS, SHOTS ON GOAL, SAVES, plus "{TEAM} (TEAM TOTAL
  GOALS)" entries (``cheapDataUID`` "Bookmaker-TT-HOME/AWAY-...") that are team totals
  and go to the odds table. Prop books are a few hundred dollars deep, so they are priced
  at :data:`PROP_UNIT_SIZE`, not :data:`UNIT_SIZE`; ``depth`` keeps what was resting.
* ``live`` games are skipped: an in-play book is not a pregame price. The exchange also
  keeps quoting after puck drop, which is why it is not a CLV reference book.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from nhl import config
from nhl.ingest.http import SourceUnavailable, WebClient
from nhl.odds.core import american_price, implied_probability, odds_frame
from nhl.odds.props import props_frame, store_props
from nhl.odds.store import store_odds
from nhl.sources.common import archive_raw, utcnow
from nhl.sources.dailyfaceoff import PlayerResolver
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

BOOK = "4Casters"
SOURCE = "fourcasters"
API_BASE = "https://api.4casters.io"
LEAGUE = "NHL"
PROPS_LEAGUE = "NHL-PROPS"
RPS = 1.0

#: Target exposure the VWAP walks to (dollars risked on a dog, to win on a favourite).
UNIT_SIZE: float = 600.0
#: Share of the target that must fill for a price to count.
MIN_FILL_RATIO: float = 0.95
#: Vig added to the filled probability so exchange prices are comparable with a book's.
EXCHANGE_VIG: float = 0.0075
#: Exposure a prop price must fill. Prop books rest $25-$850 per side (median ~$150 on
#: 2026-10-05): at $600 nothing fills, at $50 nearly everything quoted does.
PROP_UNIT_SIZE: float = 50.0

#: ``periodName`` -> stored period. Anything else is skipped.
PERIODS: dict[str, str] = {"full time": "game", "": "game", "1st period": "p1", "2nd period": "p2",
                           "3rd period": "p3", "regulation": "reg", "regulation time": "reg"}
#: Prop ``eventName`` suffix -> prop_type.
PROP_TYPES: dict[str, str] = {"GOALS": "goals", "ASSISTS": "assists", "POINTS": "points",
                              "SHOTS ON GOAL": "shots", "SAVES": "saves", "BLOCKED SHOTS": "blocks"}
TEAM_TOTAL_PROP = "TEAM TOTAL GOALS"
PROP_EVENT = re.compile(r"^(?P<name>.+?)\s*\((?P<kind>[^()]+)\)\s*$")

HEADERS: dict[str, str] = {"Accept": "application/json", "Content-Type": "application/json"}


TOKEN_PATH: Path = Path(config.CACHE_DIR) / "fourcasters" / "auth_token"


# ------------------------------------------------------------------------------ auth --
class SignedInClient:
    """A :class:`WebClient` that signs in to 4Casters and re-signs-in once on a refusal.

    Args:
        web: The underlying rate-limited client.
        username: 4Casters account name.
        password: 4Casters password.
        token_path: Where the session token is cached between runs (None: not cached).
    """

    def __init__(self, web: WebClient, username: str, password: str, token_path: Path | None = TOKEN_PATH) -> None:
        self.web, self.username, self.password, self.token_path = web, username, password, token_path
        self.token: str | None = self._read_cached()
        if self.token:
            self.web.session.headers["Authorization"] = self.token

    def _read_cached(self) -> str | None:
        if self.token_path is None or not self.token_path.exists():
            return None
        return self.token_path.read_text().strip() or None

    def _write_cached(self, token: str) -> None:
        if self.token_path is None:
            return
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        self.token_path.write_text(token)
        os.chmod(self.token_path, 0o600)

    def sign_in(self) -> str:
        """Log in and use the new token from now on.

        Raises:
            SourceUnavailable: The login was refused or returned no token.
        """
        self.web.session.headers.pop("Authorization", None)
        try:
            reply = self.web.post_json(f"{API_BASE}/user/login", {"username": self.username, "password": self.password}) or {}
        except SourceUnavailable as exc:
            raise SourceUnavailable(f"{SOURCE}: login refused; check CAST4_USER / CAST4_PASS ({exc})") from exc
        token = ((reply.get("data") or {}).get("user") or {}).get("auth")
        if not token:
            raise SourceUnavailable(f"{SOURCE}: login returned no token")
        logger.info("[4casters] signed in")
        self.token = token
        self.web.session.headers["Authorization"] = token
        self._write_cached(token)
        return token

    def post_json(self, url: str, payload: dict[str, Any]) -> Any:
        """POST signed in; on a refusal with a cached token, sign in again and retry once."""
        fresh = False
        if self.token is None:
            self.sign_in()
            fresh = True
        try:
            return self.web.post_json(url, payload)
        except SourceUnavailable:
            if fresh:
                raise
            logger.info("[4casters] refused with the cached token; signing in again")
            self.sign_in()
            return self.web.post_json(url, payload)


# ----------------------------------------------------------------------------- fetch --
def make_client() -> WebClient | SignedInClient:
    """A rate-limited client for the exchange API, signed in when credentials are configured."""
    web = WebClient(SOURCE, rps=RPS, headers=HEADERS)
    if config.CAST4_USER and config.CAST4_PASS:
        return SignedInClient(web, config.CAST4_USER, config.CAST4_PASS)
    return web


def fetch(client: WebClient | SignedInClient | None = None, store: Store | None = None) -> tuple[dict[str, Any], datetime]:
    """Pull the whole NHL order book in one call and archive it.

    Args:
        client: Injected for tests; built with :func:`make_client` otherwise.
        store: Where to archive the raw payload; nothing is archived when None.

    Returns:
        ``(payload, captured_at)``; games are under ``payload["data"]["games"]``.

    Raises:
        SourceUnavailable: The exchange refused the request.
    """
    client = client or make_client()
    captured_at = utcnow()
    payload = client.post_json(f"{API_BASE}/exchange/getOrderbook", {"leagueRequested": LEAGUE}) or {}
    logger.info("[4casters] %d game(s) on the board", len(_games(payload)))
    if store is not None:
        archive_raw(store, SOURCE, captured_at, "orderbook", payload)
    return payload, captured_at


def fetch_props(
    client: WebClient | SignedInClient | None = None, store: Store | None = None, captured_at: datetime | None = None
) -> dict[str, Any]:
    """Pull the NHL prop order books (``leagueRequested: NHL-PROPS``) and archive them.

    Raises:
        SourceUnavailable: The exchange refused the request.
    """
    client = client or make_client()
    captured_at = captured_at or utcnow()
    payload = client.post_json(f"{API_BASE}/exchange/getOrderbook", {"leagueRequested": PROPS_LEAGUE}) or {}
    logger.info("[4casters] %d prop market(s) on the board", len(_games(payload)))
    if store is not None:
        archive_raw(store, SOURCE, captured_at, "props", payload)
    return payload


# -------------------------------------------------------------------------- odds math --
def vwap(
    levels: Iterable[tuple[float, float]],
    unit_size: float = UNIT_SIZE,
    vig: float = EXCHANGE_VIG,
) -> tuple[float, float] | None:
    """The price actually available for ``unit_size`` of exposure, walking the book.

    Levels are sorted best-first here: American odds descending is best-first on both
    signs (+217 beats +213, -105 beats -216).

    Args:
        levels: ``(american_odds, dollars_available)`` pairs, in any order.
        unit_size: Target exposure. An underdog risks this; a favourite bets to win it, so
            its target risk scales by ``|best odds| / 100``.
        vig: Added to the filled probability before converting back to a price.

    Returns:
        ``(american_price, dollars_filled)`` including vig, or None when the book cannot
        fill :data:`MIN_FILL_RATIO` of the target.
    """
    book = sorted(((float(o), float(a)) for o, a in levels if a and float(a) > 0),
                  key=lambda pair: pair[0], reverse=True)
    if not book:
        return None
    best = book[0][0]
    target = unit_size if best > 0 else unit_size * abs(best) / 100.0
    filled = weighted = 0.0
    for odds, available in book:
        take = min(available, target - filled)
        if take <= 0:
            break
        weighted += take * implied_probability(odds)
        filled += take
    if filled < target * MIN_FILL_RATIO:
        return None
    probability = min(weighted / filled + vig, 0.999)
    return american_price(probability), filled


def _levels(orders: Iterable[dict] | None) -> list[tuple[float, float]]:
    """``(odds, available)`` pairs from a list of resting orders."""
    return [(o["odds"], o.get("sumUntaken") or 0.0) for o in orders or [] if o and o.get("odds") is not None]


def _main(value: Any) -> float | None:
    """A ``main*`` marker as a line, or None when it is missing or 0 (no main line yet)."""
    if value in (None, 0, 0.0):
        return None
    return float(value)


# ------------------------------------------------------------------------- normalize --
def _games(raw: dict[str, Any] | list) -> list[dict[str, Any]]:
    """The games list from an orderbook payload (or a bare list of games)."""
    if isinstance(raw, list):
        return raw
    return ((raw or {}).get("data") or {}).get("games") or []


def _period(game: dict[str, Any]) -> str | None:
    """Stored period of a game entry, or None when it is not one we keep."""
    return PERIODS.get(str(game.get("periodName") or "").strip().lower())


def _teams(game: dict[str, Any]) -> dict[str, str]:
    """``{"home": name, "away": name}`` from a game's participants."""
    return {str(p.get("homeAway")): p.get("longName") for p in game.get("participants") or []}


def _quote(orders: list[dict] | None, unit_size: float = UNIT_SIZE) -> tuple[float, float] | None | bool:
    """``(whole American price, depth)``; None when nothing rests; False when too thin to fill."""
    levels = _levels(orders)
    if not levels:
        return None
    quote = vwap(levels, unit_size=unit_size)
    if quote is None:
        return False
    # Whole American prices: the exchange quotes half points no book posts, and rounding
    # keeps sub-cent VWAP drift from registering as a line move.
    return float(round(quote[0])), round(sum(a for _, a in levels), 2)


def _ladder(ladder: dict | None) -> list[tuple[float, list[dict]]]:
    """``(line, orders)`` for every rung of a line-keyed ladder (unparseable keys skipped)."""
    out: list[tuple[float, list[dict]]] = []
    for key, orders in (ladder or {}).items():
        try:
            out.append((float(key), orders or []))
        except (TypeError, ValueError):
            continue
    return sorted(out, key=lambda pair: pair[0])


def game_rows(game: dict[str, Any], captured_at: datetime) -> tuple[list[dict[str, Any]], int]:
    """Flatten one game's order book into one row per quotable side and rung. Pure.

    Args:
        game: One entry of ``data.games``.
        captured_at: Poll time (UTC).

    Returns:
        The rows, and the number of *main-line* market sides omitted for insufficient depth
        (alternate rungs that do not fill are omitted silently).
    """
    if game.get("live") or game.get("ended"):
        return [], 0
    period = _period(game)
    if period is None:
        return [], 0
    teams = _teams(game)
    if not teams.get("home") or not teams.get("away"):
        return [], 0
    common = {
        "book": BOOK,
        "captured_at": captured_at,
        "start_time": game.get("start"),
        "away_team": teams["away"],
        "home_team": teams["home"],
        "period": period,
        "subject": "game",
        "price_point": "live",
        "source_event_id": str(game.get("parentGameID") or game.get("id")),
    }
    rows: list[dict[str, Any]] = []
    thin = 0

    def add(market: str, side: str, line: float | None, orders: list[dict], alternate: bool = False) -> None:
        nonlocal thin
        quote = _quote(orders)
        if quote is None:
            return  # nothing resting at all: not a thin market, just no market
        if quote is False:
            thin += 0 if alternate else 1
            return
        price, depth = quote
        # depth: dollars resting on this side at this line (the market's depth).
        rows.append({**common, "market": market, "side": side, "line": line, "price": price,
                     "depth": depth, "is_alternate": alternate})

    add("moneyline", "home", None, game.get("homeMoneylines"))
    add("moneyline", "away", None, game.get("awayMoneylines"))
    mains = {
        ("puckline", "home"): _main(game.get("mainHomeSpread")),
        ("puckline", "away"): _main(game.get("mainAwaySpread")),
        ("total", "over"): _main(game.get("mainTotal")),
        ("total", "under"): _main(game.get("mainTotal")),
    }
    if mains[("puckline", "home")] is None or mains[("puckline", "away")] is None:
        mains[("puckline", "home")] = mains[("puckline", "away")] = None
    for market, side, key in (("puckline", "home", "homeSpreads"), ("puckline", "away", "awaySpreads"),
                              ("total", "over", "over"), ("total", "under", "under")):
        main = mains[(market, side)]
        for line, orders in _ladder(game.get(key)):
            add(market, side, line, orders, alternate=main is None or line != main)
    return rows, thin


def prop_rows(
    game: dict[str, Any], captured_at: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Flatten one ``NHL-PROPS`` game into player-prop rows or team-total odds rows. Pure.

    Args:
        game: One entry of the props payload's ``data.games``.
        captured_at: Poll time (UTC).

    Returns:
        ``(prop_rows, odds_rows, thin)``: rows for :func:`nhl.odds.props.props_frame`, team
        total rows for :func:`nhl.odds.core.odds_frame`, and the count of sides too thin to
        fill :data:`PROP_UNIT_SIZE`.
    """
    if game.get("live") or game.get("ended") or _period(game) != "game":
        return [], [], 0
    match = PROP_EVENT.match(str(game.get("eventName") or ""))
    teams = _teams(game)
    if not match or not teams.get("home") or not teams.get("away"):
        return [], [], 0
    name, kind = match.group("name").strip(), match.group("kind").strip().upper()
    common = {"book": BOOK, "captured_at": captured_at, "start_time": game.get("start"),
              "away_team": teams["away"], "home_team": teams["home"], "price_point": "live",
              "source_event_id": str(game.get("parentGameID") or game.get("id"))}
    props: list[dict[str, Any]] = []
    odds: list[dict[str, Any]] = []
    thin = 0
    if kind == TEAM_TOTAL_PROP:
        uid = str(game.get("cheapDataUID") or "").upper()
        subject = "home" if "-TT-HOME-" in uid else "away" if "-TT-AWAY-" in uid else None
        if subject is None:  # fall back to the team name
            subject = next((s for s, t in teams.items() if str(t).upper() == name.upper()), None)
        if subject is None:
            return [], [], 0
        main = _main(game.get("mainTotal"))
        for side in ("over", "under"):
            for line, orders in _ladder(game.get(side)):
                quote = _quote(orders, PROP_UNIT_SIZE)
                if quote is False:
                    thin += 1
                if not quote:
                    continue
                odds.append({**common, "period": "game", "market": "team_total", "subject": subject, "side": side,
                             "line": line, "price": quote[0], "depth": quote[1],
                             "is_alternate": main is None or line != main})
        return props, odds, thin
    prop_type = PROP_TYPES.get(kind)
    if prop_type is None:
        logger.debug("[4casters] prop type %r not stored (%s)", kind, game.get("eventName"))
        return [], [], 0
    for side in ("over", "under"):
        for line, orders in _ladder(game.get(side)):
            quote = _quote(orders, PROP_UNIT_SIZE)
            if quote is False:
                thin += 1
            if not quote:
                continue
            props.append({**common, "player_name": name.title(), "team": None, "prop_type": prop_type,
                          "line": line, "side": side, "price": quote[0], "depth": quote[1]})
    return props, odds, thin


def normalize_props(raw: dict[str, Any] | list, captured_at: datetime) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Turn one ``NHL-PROPS`` payload into (props, team-total odds). No network, no writes.

    Returns:
        ``(props_frame, odds_frame)``, both with ``game_id`` still null.
    """
    props: list[dict[str, Any]] = []
    odds: list[dict[str, Any]] = []
    thin = 0
    for game in _games(raw):
        p, o, t = prop_rows(game, captured_at)
        props.extend(p)
        odds.extend(o)
        thin += t
    if thin:
        logger.info("[4casters] %d prop side(s) too thin to fill $%.0f were omitted", thin, PROP_UNIT_SIZE)
    return props_frame(props), odds_frame(odds)


def normalize(raw: dict[str, Any] | list, captured_at: datetime) -> pl.DataFrame:
    """Turn one orderbook payload into odds rows. No network, no writes.

    Args:
        raw: The ``getOrderbook`` response (or a bare list of games).
        captured_at: Poll time (UTC).

    Returns:
        An :func:`~nhl.odds.core.odds_frame` with ``game_id`` still null.
    """
    rows: list[dict[str, Any]] = []
    thin = 0
    for game in _games(raw):
        game_rows_, skipped = game_rows(game, captured_at)
        rows.extend(game_rows_)
        thin += skipped
    if thin:
        logger.info("[4casters] %d market side(s) too thin to fill $%.0f were omitted", thin, UNIT_SIZE)
    return odds_frame(rows)


# ------------------------------------------------------------------------------ poll --
def poll(
    store: Store, games: pl.DataFrame, client: WebClient | SignedInClient | None = None,
    resolver: PlayerResolver | None = None,
) -> int:
    """Fetch, archive, normalize and store one 4Casters poll (game lines, then props).

    The props board is a second call: a refusal there is logged and the game lines are
    still stored. Team totals from the props board go to the odds table; player props to
    the ``fourcasters`` props table, logged separately.

    Args:
        store: S3 store (archive + odds tables).
        games: ``processed/games.parquet``, used to resolve ``game_id``.
        client: Injected for tests.
        resolver: Player resolver for props; built from the store when needed.

    Returns:
        Odds rows written; 0 when the exchange refused (logged, not raised).
    """
    client = client or make_client()
    try:
        raw, captured_at = fetch(client, store)
    except SourceUnavailable as exc:
        hint = "" if isinstance(client, SignedInClient) else \
            " (the order book needs a signed-in account since 2026-10-06: set CAST4_USER and CAST4_PASS in .env)"
        logger.warning("[4casters] skipped: %s%s", exc, hint)
        return 0
    odds = normalize(raw, captured_at)
    props = props_frame([])
    try:
        props, team_totals = normalize_props(fetch_props(client, store, captured_at), captured_at)
        odds = pl.concat([odds, team_totals])
    except SourceUnavailable as exc:
        logger.warning("[4casters] props skipped: %s", exc)
    written = store_odds(store, odds, games, SOURCE)
    store_props(store, props, games, SOURCE, resolver)
    return written


__all__ = ["BOOK", "EXCHANGE_VIG", "SignedInClient", "MIN_FILL_RATIO", "PROP_UNIT_SIZE", "UNIT_SIZE", "fetch", "fetch_props",
           "game_rows", "normalize", "normalize_props", "poll", "prop_rows", "vwap", "make_client"]
