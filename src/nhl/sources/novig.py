"""Novig exchange: poll the NHL catalog and order books and store the takeable price as it moves.

The v3 API (https://docs.novig.com) nests sport > league > event (one game) > market (one
question) > outcome (one answer, with its own order book). Shapes, confirmed on the NHL
catalog on 2026-10-09:

* **Two ways in.** The catalog and books are served without a signature under
  ``/v3/public/...``, throttled at the edge to ~2 requests/s per IP (3/s draws 429s). With
  ``NOVIG_KEY_ID`` / ``NOVIG_PEM`` set (a ``trading::read`` key: it can read the catalog and
  nothing else), :class:`NovigClient` signs every request with NOVIG-V3 and gets the
  ``read`` bucket (16/s, burst 64). There are ~250 markets per game.
* **Books in one message when signed.** A signed client opens the websocket (``/v3/ws``) and
  sends one ``snapshot`` naming every event on ``book``: the whole slate (3,671 books, full
  depth, ~2 MB) arrives in well under a second. The ``stream`` throttle charges a request at
  most its 512-token capacity, so one big snapshot costs no more than a small one; a
  connection may watch at most :data:`MAX_WS_MARKETS` markets, so larger slates are split.
  Any market the snapshot misses (or every market, without a key or when the websocket
  fails) is read over REST, **one request per market**, in priority order (game lines, then
  props, soonest game first) until :data:`TIME_BUDGET_S` runs out; the rest wait for the
  next poll. The public route manages ~750 books per poll this way.
* **Every order buys.** A market's book is ``{outcomeId: [resting bids]}``. A bid at ``q`` on
  one outcome is liquidity for the other: a taker buys the other outcome at ``1 - q``. So the
  price to bet a side is read off the *opposite* outcome's bids (:func:`take`).
* **Contracts pay 1 cent.** ``qty`` contracts at price ``q`` stake ``qty * q / 100`` dollars;
  prices are probabilities on a 0.001/0.005 grid.
* **A price is only real at a size.** :func:`take` walks the opposite bids until
  :data:`GAME_MIN_STAKE` (game lines) or :data:`PROP_MIN_STAKE` (props) of stake fills and
  returns the VWAP; a side that cannot fill it is omitted, not quoted at a price nobody can
  take. ``depth`` is every dollar of stake resting for that side.
* **No vig to add back.** Unlike 4Casters there is no fee pregame: NHL markets carry
  ``fee.charged == "WHEN_LIVE"`` (a 0.06 taker fee from 2026-10-08, only once the event is
  ``OPEN_INGAME``). In-play events are skipped, so the stored price is exactly what a taker
  pays. A market charged ``ALWAYS`` (futures) gets ``c * P * (1 - P)`` added.
* **Market types.** ``MONEY`` (outcomes are tricodes), ``SPREAD`` (outcome names like
  ``"DET -1.5"``, ``strike`` the home handicap; every rung from -4.5 to +2.5 is its own
  market), ``TOTAL`` and ``TEAM_TOTAL`` (``Over 5.5``/``Under 5.5``, one market per rung).
  There are no main-line markers: the main puck line is the favourite's -1.5 (favourite by
  the moneyline book's mid), the main total (and team total) the rung whose mid is nearest
  50%. Props: ``PLAYER_GOALS``, ``ASSISTS``, ``POINTS``, ``SHOTS_ON_GOAL``, ``SAVES``,
  ``POWER_PLAY_POINTS`` (over/under at ``strike``) and ``FIRST_GOAL_SCORER`` (yes/no). The
  player is the ``description`` minus its trailing ``"{strike} {TYPE}"``; the team comes
  from roster matching (:func:`nhl.odds.props.resolve_player`).
* The event ``description`` is ``"Away Team @ Home Team"``. Futures (``STANLEY_CUP_WINNER``)
  are left out.
"""

from __future__ import annotations

import base64
import json
import hashlib
import logging
import re
import time
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote, unquote

import polars as pl
import requests

from nhl import config
from nhl.ingest.http import BROWSER_UA, RateLimiter, SourceUnavailable
from nhl.odds.core import american_price, odds_frame
from nhl.odds.props import props_frame, store_props
from nhl.odds.store import store_odds
from nhl.sources.common import archive_raw, utcnow
from nhl.sources.dailyfaceoff import PlayerResolver
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

BOOK = "Novig"
SOURCE = "novig"
LEAGUE = "NHL"
SCHEME = "NOVIG-V3"

#: Stake (dollars) a game-line price must fill, and a prop price.
GAME_MIN_STAKE: float = 100.0
PROP_MIN_STAKE: float = 50.0

#: Request pacing: the public routes hold ~2/s per IP; a signed key refills 16/s.
PUBLIC_RPS: float = 1.8
SIGNED_RPS: float = 14.0
#: Events starting within this many hours are polled.
HORIZON_HOURS: float = 24.0
#: Wall-clock budget for book requests in one poll; markets not reached wait for the next.
TIME_BUDGET_S: float = 420.0
PAGE_LIMIT = 100
#: Markets one websocket connection may watch (``MAX_WATCHED_MARKETS``), with headroom.
MAX_WS_MARKETS = 8000
#: Seconds to wait for a websocket snapshot reply.
WS_TIMEOUT_S = 30.0
BOOK_DEPTH = 20

#: ``marketType`` -> odds-table market.
GAME_MARKETS: dict[str, str] = {"MONEY": "moneyline", "SPREAD": "puckline", "TOTAL": "total",
                                "TEAM_TOTAL": "team_total"}
#: ``marketType`` -> props-table ``prop_type``.
PROP_TYPES: dict[str, str] = {"PLAYER_GOALS": "goals", "ASSISTS": "assists", "POINTS": "points",
                              "SHOTS_ON_GOAL": "shots", "SAVES": "saves", "BLOCKS": "blocks",
                              "POWER_PLAY_POINTS": "pp_points", "FIRST_GOAL_SCORER": "first_goal"}
#: Book-fetch order within a game: what the models price first.
PRIORITY: tuple[str, ...] = ("MONEY", "SPREAD", "TOTAL", "TEAM_TOTAL", "SHOTS_ON_GOAL", "POINTS", "SAVES",
                             "ASSISTS", "PLAYER_GOALS", "BLOCKS", "POWER_PLAY_POINTS", "FIRST_GOAL_SCORER")
PREGAME = "OPEN_PREGAME"
OPEN = "OPEN"

UNRESERVED = "-._~"
EMPTY_BODY_HASH = hashlib.sha256(b"").hexdigest()


# --------------------------------------------------------------------------- signing --
def canonical_query(raw: str) -> str:
    """NOVIG-V3 line 5: the query re-encoded (RFC 3986 unreserved kept, uppercase hex),
    pairs sorted bytewise by name then value, repeats kept. A bare ``flag`` is ``flag=``;
    a ``+`` stays a literal plus.
    """
    if not raw:
        return ""
    pairs = []
    for token in raw.split("&"):
        name, _, value = token.partition("=")
        pairs.append((quote(unquote(name), safe=UNRESERVED), quote(unquote(value), safe=UNRESERVED)))
    return "&".join(f"{n}={v}" for n, v in sorted(pairs, key=lambda p: (p[0].encode(), p[1].encode())))


def string_to_sign(timestamp_ms: int, method: str, path: str, query: str = "", body: bytes = b"") -> str:
    """The six-line NOVIG-V3 canonical string (LF-joined, no trailing newline)."""
    return "\n".join([SCHEME, str(timestamp_ms), method.upper(), path, canonical_query(query),
                      hashlib.sha256(body).hexdigest()])


class Signer:
    """Signs NOVIG-V3 strings with an Ed25519 or P-256 PKCS#8 private key.

    Args:
        key_id: The key's UUID (sent as ``Novig-Key-Id``; not a secret).
        pem: The PKCS#8 PEM bytes of the private key.
    """

    def __init__(self, key_id: str, pem: bytes) -> None:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        self.key_id = key_id
        self._key = load_pem_private_key(pem, None)

    @classmethod
    def from_file(cls, key_id: str, path: str) -> Signer:
        """Load the private key from a PEM file."""
        with open(path, "rb") as fh:
            return cls(key_id, fh.read())

    def sign(self, text: str) -> str:
        """Standard padded base64 of the signature over ``text``."""
        from cryptography.hazmat.primitives.asymmetric import ec, ed25519
        from cryptography.hazmat.primitives.hashes import SHA256

        if isinstance(self._key, ed25519.Ed25519PrivateKey):
            raw = self._key.sign(text.encode())
        elif isinstance(self._key, ec.EllipticCurvePrivateKey):
            raw = self._key.sign(text.encode(), ec.ECDSA(SHA256()))  # DER, as Novig expects
        else:
            raise ValueError("Novig keys are Ed25519 or P-256")
        return base64.b64encode(raw).decode()

    def headers(self, method: str, path: str, query: str = "", body: bytes = b"",
                timestamp_ms: int | None = None) -> dict[str, str]:
        """The three auth headers for one request."""
        ts = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
        return {"Novig-Key-Id": self.key_id, "Novig-Timestamp": str(ts),
                "Novig-Signature": self.sign(string_to_sign(ts, method, path, query, body))}


# ------------------------------------------------------------------------------ fetch --
class NovigClient:
    """GETs against the Novig catalog: signed when a signer is given, public otherwise.

    Public requests go to ``/v3/public/catalog/...``; signed ones to ``/v3/catalog/...``.
    A 429 waits out ``Retry-After`` and retries; a refusal (401/403/451, or 423 when the
    account is locked) or a 429 that does not clear raises :class:`SourceUnavailable`;
    :func:`fetch` then falls back to the public routes.

    Args:
        host: API host (``NOVIG_HOST``).
        signer: A ``trading::read`` key's signer, or None for the public routes.
        rps: Request pacing; defaults to :data:`SIGNED_RPS` / :data:`PUBLIC_RPS`.
        timeout: Per-request timeout in seconds.
    """

    def __init__(self, host: str = config.NOVIG_HOST, signer: Signer | None = None, rps: float | None = None,
                 timeout: float = 20.0, ws_connect: Any = None) -> None:
        self.host, self.signer, self.timeout, self.rps = host.rstrip("/"), signer, timeout, rps
        self.limiter = RateLimiter(rps or (SIGNED_RPS if signer else PUBLIC_RPS))
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": BROWSER_UA, "Accept": "application/json"})
        self.ws_connect = ws_connect  # injected for tests; websocket.create_connection otherwise

    @property
    def signed(self) -> bool:
        return self.signer is not None

    def go_public(self) -> None:
        """Drop the key and use the public routes (and their pacing) from now on."""
        self.signer = None
        self.limiter = RateLimiter(self.rps or PUBLIC_RPS)

    def get_json(self, path: str, params: dict[str, Any] | None = None, retries: int = 5) -> Any:
        """GET ``path`` (``/v3/catalog/...``) with ``params``; the decoded JSON reply."""
        if not self.signed:
            path = path.replace("/v3/", "/v3/public/", 1)
        query = "&".join(f"{quote(str(k), safe=UNRESERVED)}={quote(str(v), safe=UNRESERVED)}"
                         for k, v in (params or {}).items() if v is not None)
        url = self.host + path + (f"?{query}" if query else "")
        for attempt in range(retries + 1):
            self.limiter.wait()
            headers = self.signer.headers("GET", path, query) if self.signer else {}
            try:
                resp = self.session.get(url, headers=headers, timeout=self.timeout)
            except requests.RequestException as exc:
                if attempt == retries:
                    raise SourceUnavailable(f"{SOURCE}: GET {path} failed ({exc})") from exc
                time.sleep(2.0 ** attempt)
                continue
            if resp.status_code == 429 and attempt < retries:
                time.sleep(float(resp.headers.get("Retry-After") or 1.0) * (attempt + 1))
                continue
            if resp.status_code in (401, 403, 423, 429, 451):
                raise SourceUnavailable(f"{SOURCE}: GET {path} refused with {resp.status_code}: {resp.text[:200]}")
            if resp.status_code >= 500 and attempt < retries:
                time.sleep(2.0 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        raise SourceUnavailable(f"{SOURCE}: GET {path} gave up after {retries} retries")

    def pages(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Every item of a paged catalog route (follows ``next``)."""
        items: list[dict[str, Any]] = []
        after = None
        while True:
            page = self.get_json(path, {**params, "limit": PAGE_LIMIT, "after": after}) or {}
            items.extend(page.get("items") or [])
            after = page.get("next")
            if not after or not page.get("items"):
                return items


def make_client() -> NovigClient:
    """A client for ``NOVIG_HOST``, signed when ``NOVIG_KEY_ID`` and ``NOVIG_PEM`` are set."""
    if config.NOVIG_KEY_ID and config.NOVIG_PEM:
        return NovigClient(config.NOVIG_HOST, Signer.from_file(config.NOVIG_KEY_ID, config.NOVIG_PEM))
    return NovigClient(config.NOVIG_HOST)


def snapshot_books(client: NovigClient, markets: list[dict[str, Any]]) -> dict[str, Any]:
    """Every book of ``markets``' events from websocket ``snapshot`` requests (signed only).

    Events are grouped so one snapshot names at most :data:`MAX_WS_MARKETS` markets; a
    snapshot over the ``stream`` capacity passes only with a full bucket (refill 32/s), so a
    throttled reply waits and retries.

    Returns:
        Market id -> book (``{"seq", "orders"}``), the same shape as the REST book.

    Raises:
        SourceUnavailable: No signer, or the exchange refused the upgrade or the snapshot.
    """
    if client.signer is None:
        raise SourceUnavailable(f"{SOURCE}: the websocket needs a key")
    counts: dict[str, int] = {}
    for m in markets:
        counts[m["eventId"]] = counts.get(m["eventId"], 0) + 1
    chunks: list[list[str]] = [[]]
    size = 0
    for event_id, n in counts.items():
        if chunks[-1] and size + n > MAX_WS_MARKETS:
            chunks.append([])
            size = 0
        chunks[-1].append(event_id)
        size += n
    connect = client.ws_connect
    if connect is None:
        import websocket

        connect = websocket.create_connection
    url = client.host.replace("https://", "wss://", 1) + "/v3/ws"
    headers = [f"{k}: {v}" for k, v in client.signer.headers("GET", "/v3/ws").items()]
    try:
        ws = connect(url, header=headers, timeout=WS_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 - any refused or failed upgrade means "use REST"
        raise SourceUnavailable(f"{SOURCE}: websocket upgrade failed ({exc})") from exc
    books: dict[str, Any] = {}
    nonce = 0
    try:
        for chunk in [c for c in chunks if c]:
            for attempt in range(4):
                nonce += 1
                ws.send(json.dumps({"nonce": nonce, "snapshot": {"events": {e: "book" for e in chunk}}}))
                reply = _await_reply(ws, nonce)
                if "snapshot" in reply:
                    break
                if "RATE_LIMIT" not in json.dumps(reply) or attempt == 3:
                    raise SourceUnavailable(f"{SOURCE}: snapshot refused: {json.dumps(reply)[:200]}")
                time.sleep(16.0)  # an over-capacity request needs a full bucket: 512 / 32 per s
            books.update({mid: entry["book"] for mid, entry in (reply.get("snapshot") or {}).items()
                          if isinstance(entry, dict) and entry.get("book")})
    finally:
        ws.close()
    return books


def _await_reply(ws: Any, nonce: int) -> dict[str, Any]:
    """The reply to ``nonce``, or an error frame (throttled frames come back without a nonce)."""
    deadline = time.monotonic() + WS_TIMEOUT_S
    while time.monotonic() < deadline:
        message = ws.recv()
        if isinstance(message, bytes):
            message = message.decode()
        reply = json.loads(message) if message else {}
        if reply.get("nonce") == nonce or (reply.get("nonce") is None and ("code" in reply or "error" in reply)):
            return reply
        # Anything else (a heartbeat) is skipped.
    raise SourceUnavailable(f"{SOURCE}: no snapshot reply within {WS_TIMEOUT_S:.0f}s")


def _priority(market: dict[str, Any]) -> tuple[int, int, int]:
    """Book-fetch order: game lines before props, then soonest game, then :data:`PRIORITY`."""
    kind = market.get("marketType")
    rank = PRIORITY.index(kind) if kind in PRIORITY else len(PRIORITY)
    return (0 if kind in GAME_MARKETS else 1, int(market.get("startsTs") or 0), rank)


def fetch(
    client: NovigClient | None = None, store: Store | None = None, now: datetime | None = None,
    horizon_hours: float = HORIZON_HOURS, budget_s: float = TIME_BUDGET_S,
) -> tuple[dict[str, Any], datetime]:
    """Pull pregame NHL events, their markets and as many books as the budget allows; archive.

    Args:
        client: Injected for tests; built with :func:`make_client` otherwise.
        store: Where to archive the raw payload; nothing is archived when None.
        now: Poll time (UTC); defaults to now.
        horizon_hours: Only events starting within this many hours.
        budget_s: Wall-clock seconds allowed for the book requests.

    Returns:
        ``({"events", "markets", "books"}, captured_at)``; ``books`` maps market id to book.

    Raises:
        SourceUnavailable: The exchange refused the catalog request.
    """
    client = client or make_client()
    captured_at = now or utcnow()
    window = {"league": LEAGUE, "startsAfter": int(captured_at.timestamp() * 1000),
              "startsBefore": int((captured_at + timedelta(hours=horizon_hours)).timestamp() * 1000)}
    try:
        events = [e for e in client.pages("/v3/catalog/events", window) if e.get("status") == PREGAME]
    except SourceUnavailable as exc:
        if not client.signed:
            raise
        # A refused key (revoked, or the account locked: 423) must not stop the capture.
        logger.warning("[novig] signed read refused, falling back to the public routes: %s", exc)
        client.go_public()
        events = [e for e in client.pages("/v3/catalog/events", window) if e.get("status") == PREGAME]
    ids = {e["eventId"] for e in events}
    markets = [m for m in client.pages("/v3/catalog/markets", window)
               if m.get("eventId") in ids and m.get("status") == OPEN
               and (m.get("marketType") in GAME_MARKETS or m.get("marketType") in PROP_TYPES)]
    books: dict[str, Any] = {}
    wanted = {m["marketId"] for m in markets}
    if client.signed and markets:
        try:
            books = {mid: b for mid, b in snapshot_books(client, markets).items() if mid in wanted}
            logger.info("[novig] websocket snapshot: %d of %d book(s)", len(books), len(markets))
        except (SourceUnavailable, OSError, ValueError) as exc:
            logger.warning("[novig] websocket snapshot failed, reading books over REST: %s", exc)
    deadline = time.monotonic() + budget_s
    for market in sorted((m for m in markets if m["marketId"] not in books), key=_priority):
        if time.monotonic() > deadline:
            break
        try:
            books[market["marketId"]] = client.get_json(f"/v3/catalog/markets/{market['marketId']}/book",
                                                        {"depth": BOOK_DEPTH})
        except SourceUnavailable as exc:
            logger.warning("[novig] book requests stopped after %d: %s", len(books), exc)
            break
        except requests.HTTPError as exc:  # a market that closed since the catalog read
            logger.debug("[novig] book %s: %s", market["marketId"], exc)
    left = len(markets) - len(books)
    logger.info("[novig] %d event(s), %d market(s), %d book(s) read%s (%s)", len(events), len(markets), len(books),
                f", {left} left for the next poll" if left else "", "signed" if client.signed else "public")
    raw = {"events": events, "markets": markets, "books": books}
    if store is not None:
        archive_raw(store, SOURCE, captured_at, "board", raw)
    return raw, captured_at


# -------------------------------------------------------------------------- odds math --
def take(
    bids: Iterable[dict[str, Any]], min_stake: float, fee: dict[str, Any] | None = None
) -> tuple[float, float] | None | bool:
    """The probability a taker pays to stake ``min_stake`` against the opposite side's bids.

    Each bid at ``q`` for ``qty`` contracts (1 cent each) lets a taker buy the other outcome
    at ``1 - q``, staking up to ``qty * (1 - q) / 100`` dollars. Bids are walked best (highest
    ``q``) first until ``min_stake`` fills.

    Args:
        bids: The *opposite* outcome's resting orders (``price``, ``qty``).
        min_stake: Dollars that must fill for the price to count.
        fee: The market's ``fee`` block; only ``charged == "ALWAYS"`` adds a fee pregame.

    Returns:
        ``(probability, depth)`` with depth every dollar of stake resting; None when nothing
        rests; False when less than ``min_stake`` does.
    """
    levels = sorted(((float(b["price"]), float(b.get("qty") or 0)) for b in bids or []
                     if b and b.get("price") is not None), key=lambda lv: lv[0], reverse=True)
    levels = [(q, n) for q, n in levels if 0.0 < q < 1.0 and n > 0]
    if not levels:
        return None
    depth = sum(n * (1.0 - q) for q, n in levels) / 100.0
    if depth < min_stake:
        return False
    staked = payout = 0.0
    for q, n in levels:
        cost = 1.0 - q
        stake = min(n * cost / 100.0, min_stake - staked)
        if stake <= 0:
            break
        staked += stake
        payout += stake / cost
    probability = staked / payout
    if fee and str(fee.get("charged")).upper() == "ALWAYS":
        probability += float(fee.get("coefficient") or 0.0) * probability * (1.0 - probability)
    return probability, round(depth, 2)


def _quote(book: dict[str, Any], outcome_id: str, other_id: str, min_stake: float,
           fee: dict[str, Any] | None) -> tuple[float, float] | None | bool:
    """``(whole American price, depth)`` to buy ``outcome_id``; None/False as in :func:`take`."""
    taken = take((book.get("orders") or {}).get(other_id) or [], min_stake, fee)
    if not taken:
        return taken
    # Whole American prices: rounding keeps sub-cent VWAP drift from registering as a move.
    return float(round(american_price(min(taken[0], 0.999)))), taken[1]


def _mid(book: dict[str, Any] | None, outcome_id: str, other_id: str) -> float | None:
    """Mid probability of ``outcome_id`` from the best bid on each side, or None."""
    orders = (book or {}).get("orders") or {}
    own = [float(o["price"]) for o in orders.get(outcome_id) or [] if o.get("price")]
    other = [float(o["price"]) for o in orders.get(other_id) or [] if o.get("price")]
    if not own or not other:
        return None
    return (max(own) + 1.0 - max(other)) / 2.0


# ------------------------------------------------------------------------- normalize --
def event_teams(event: dict[str, Any], start: datetime | None = None) -> tuple[str | None, str | None]:
    """``(away, home)`` tricodes from ``"Away Team @ Home Team"``."""
    away, sep, home = str(event.get("description") or "").partition(" @ ")
    if not sep:
        return None, None
    return resolve_team(away.strip(), start), resolve_team(home.strip(), start)


def _side_of(name: str) -> str | None:
    """``over``/``under``/``yes``/``no`` from an outcome name like ``"Over 5.5"``."""
    word = str(name or "").split(" ", 1)[0].lower()
    return word if word in ("over", "under", "yes", "no") else None


def _pair(market: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """``(outcome, other outcome)`` for each of a two-outcome market's outcomes."""
    outcomes = market.get("outcomes") or []
    if len(outcomes) != 2:
        return []
    return [(outcomes[0], outcomes[1]), (outcomes[1], outcomes[0])]


def player_name(market: dict[str, Any]) -> str:
    """The player in a prop's description (``"Andrew Copp 2.5 SHOTS_ON_GOAL"`` -> ``Andrew Copp``)."""
    text = str(market.get("description") or "")
    return re.sub(rf"\s+(?:[-+]?\d+(?:\.\d+)?\s+)?{re.escape(str(market.get('marketType')))}\s*$", "", text).strip()


def _main_rungs(markets: list[dict[str, Any]], books: dict[str, Any], home: str, start: datetime | None
                ) -> set[str]:
    """Market ids that are a game's main lines: the favourite's -1.5 and the totals nearest 50%."""
    main: set[str] = set()
    by_type: dict[str, list[dict[str, Any]]] = {}
    for m in markets:
        by_type.setdefault(m["marketType"], []).append(m)
    p_home = None
    for m in by_type.get("MONEY", []):
        main.add(m["marketId"])
        sides = {resolve_team(o["name"], start): o["outcomeId"] for o in m.get("outcomes") or []}
        others = [oid for team, oid in sides.items() if team != home]
        if home in sides and others:
            p_home = _mid(books.get(m["marketId"]), sides[home], others[0])
    if p_home is not None:
        target = -1.5 if p_home >= 0.5 else 1.5
        for m in by_type.get("SPREAD", []):
            if m.get("strike") is not None and float(m["strike"]) == target:
                main.add(m["marketId"])
    groups: dict[str, list[dict[str, Any]]] = {}
    for m in by_type.get("TOTAL", []) + by_type.get("TEAM_TOTAL", []):
        key = m["marketType"] if m["marketType"] == "TOTAL" else f"TT|{player_name(m)}"
        groups.setdefault(key, []).append(m)
    for rungs in groups.values():
        scored = []
        for m in rungs:
            over = next((o for o in m.get("outcomes") or [] if _side_of(o["name"]) == "over"), None)
            under = next((o for o in m.get("outcomes") or [] if _side_of(o["name"]) == "under"), None)
            if over and under and (p := _mid(books.get(m["marketId"]), over["outcomeId"], under["outcomeId"])) is not None:
                scored.append((abs(p - 0.5), float(m.get("strike") or 0), m["marketId"]))
        if scored:
            main.add(min(scored)[2])
    return main


def event_rows(
    event: dict[str, Any], markets: list[dict[str, Any]], books: dict[str, Any], captured_at: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
    """Flatten one event's priced markets into odds rows and prop rows. Pure.

    Args:
        event: One catalog event.
        markets: That event's markets.
        books: Market id -> order book (markets without a book are skipped).
        captured_at: Poll time (UTC).

    Returns:
        ``(odds_rows, prop_rows, thin_game_sides, thin_prop_sides)``; a side whose opposite
        bids cannot fill the minimum stake is omitted and counted.
    """
    if event.get("status") != PREGAME:
        return [], [], 0, 0
    start = datetime.fromtimestamp(int(event["startsTs"]) / 1000, tz=captured_at.tzinfo) if event.get("startsTs") else None
    away, home = event_teams(event, start)
    if not away or not home:
        logger.warning("[novig] event %r: teams not recognised", event.get("description"))
        return [], [], 0, 0
    common = {"book": BOOK, "captured_at": captured_at, "start_time": start, "away_team": away, "home_team": home,
              "price_point": "live", "source_event_id": str(event["eventId"])}
    priced = [m for m in markets if m["marketId"] in books and m.get("status") == OPEN]
    main = _main_rungs([m for m in priced if m["marketType"] in GAME_MARKETS], books, home, start)
    odds: list[dict[str, Any]] = []
    props: list[dict[str, Any]] = []
    thin_game = thin_prop = 0
    for m in priced:
        kind, book, fee = m["marketType"], books[m["marketId"]], m.get("fee")
        is_game = kind in GAME_MARKETS
        for outcome, other in _pair(m):
            quote_ = _quote(book, outcome["outcomeId"], other["outcomeId"],
                            GAME_MIN_STAKE if is_game else PROP_MIN_STAKE, fee)
            if quote_ is False:
                if is_game:
                    thin_game += 1
                else:
                    thin_prop += 1
            if not quote_:
                continue
            price, depth = quote_
            strike = float(m["strike"]) if m.get("strike") is not None else None
            if not is_game:
                side = _side_of(outcome["name"])
                if side is None:
                    continue
                props.append({**common, "player_name": player_name(m), "team": None, "prop_type": PROP_TYPES[kind],
                              "line": None if side in ("yes", "no") else strike, "side": side, "price": price,
                              "depth": depth})
                continue
            row = {**common, "period": "game", "market": GAME_MARKETS[kind], "subject": "game", "price": price,
                   "depth": depth, "is_alternate": m["marketId"] not in main}
            if kind == "MONEY":
                team = resolve_team(outcome["name"], start)
                if team not in (home, away):
                    continue
                odds.append({**row, "side": "home" if team == home else "away", "line": None, "is_alternate": False})
            elif kind == "SPREAD":
                label, _, line = str(outcome["name"]).rpartition(" ")
                team = resolve_team(label, start)
                if team not in (home, away):
                    continue
                odds.append({**row, "side": "home" if team == home else "away", "line": float(line)})
            else:
                side = _side_of(outcome["name"])
                if side not in ("over", "under"):
                    continue
                if kind == "TEAM_TOTAL":
                    team = resolve_team(player_name(m), start)
                    if team not in (home, away):
                        continue
                    row["subject"] = "home" if team == home else "away"
                odds.append({**row, "side": side, "line": strike})
    return odds, props, thin_game, thin_prop


def normalize(raw: dict[str, Any], captured_at: datetime) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Turn one :func:`fetch` payload into (odds, props) frames. No network, no writes.

    Returns:
        ``(odds_frame, props_frame)``, both with ``game_id`` still null.
    """
    by_event: dict[str, list[dict[str, Any]]] = {}
    for m in raw.get("markets") or []:
        by_event.setdefault(m["eventId"], []).append(m)
    odds: list[dict[str, Any]] = []
    props: list[dict[str, Any]] = []
    thin_game = thin_prop = 0
    for event in raw.get("events") or []:
        o, p, tg, tp = event_rows(event, by_event.get(event["eventId"], []), raw.get("books") or {}, captured_at)
        odds.extend(o)
        props.extend(p)
        thin_game += tg
        thin_prop += tp
    if thin_game or thin_prop:
        logger.info("[novig] sides too thin to omit: %d game (< $%.0f), %d prop (< $%.0f)",
                    thin_game, GAME_MIN_STAKE, thin_prop, PROP_MIN_STAKE)
    return odds_frame(odds), props_frame(props)


# ------------------------------------------------------------------------------ poll --
def poll(
    store: Store, games: pl.DataFrame, client: NovigClient | None = None, resolver: PlayerResolver | None = None,
    budget_s: float = TIME_BUDGET_S,
) -> tuple[int, int]:
    """Fetch, archive, normalize and store one Novig poll (game lines and props).

    Args:
        store: S3 store (archive, odds and props tables).
        games: ``processed/games.parquet``, used to resolve ``game_id``.
        client: Injected for tests.
        resolver: Player resolver for props; built from the store when needed.
        budget_s: Seconds allowed for book requests.

    Returns:
        ``(odds_rows_written, prop_rows_written)``; ``(0, 0)`` when the exchange refused
        (logged, not raised).
    """
    try:
        client = client or make_client()
        raw, captured_at = fetch(client, store, budget_s=budget_s)
    except (SourceUnavailable, OSError, ValueError) as exc:  # OSError/ValueError: an unreadable key file
        logger.warning("[novig] skipped: %s", exc)
        return 0, 0
    odds, props = normalize(raw, captured_at)
    return store_odds(store, odds, games, SOURCE), store_props(store, props, games, SOURCE, resolver)


__all__ = ["BOOK", "GAME_MIN_STAKE", "NovigClient", "PROP_MIN_STAKE", "Signer", "canonical_query", "event_rows",
           "event_teams", "fetch", "make_client", "normalize", "player_name", "poll", "snapshot_books", "string_to_sign",
           "take"]
