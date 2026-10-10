"""Novig signer, pricing and normalizer against a trimmed real payload (captured 2026-10-09).

``novig.json`` is SEA@DET's catalog event, its game markets and a sample of its props, each
with its order book (top six orders per outcome). ``novig_signing_vectors.json`` is Novig's
published NOVIG-V3 vector file, whose keypairs are public test keys.
"""

from __future__ import annotations

import base64
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.serialization import load_pem_public_key

from nhl.ingest.http import SourceUnavailable
from nhl.odds.core import implied_probability
from nhl.sources import novig
from nhl.storage import keys

FIXTURES = Path(__file__).parent / "fixtures" / "books"
CAPTURED = datetime(2026, 10, 9, 22, 0, tzinfo=timezone.utc)
VECTORS = json.loads((FIXTURES / "novig_signing_vectors.json").read_text())


@pytest.fixture
def raw() -> dict:
    return json.loads((FIXTURES / "novig.json").read_text())


@pytest.fixture
def frames(raw: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    return novig.normalize(raw, CAPTURED)


def pick(df: pl.DataFrame, **eq: Any) -> pl.DataFrame:
    """Rows of ``df`` equal to every keyword."""
    return df.filter(*(pl.col(k) == v for k, v in eq.items()))


# --------------------------------------------------------------------------- signing --
@pytest.mark.parametrize("vector", VECTORS["vectors"], ids=lambda v: v["id"])
def test_string_to_sign_matches_vectors(vector: dict) -> None:
    given = vector["input"]
    body = given["body"].encode() if isinstance(given["body"], str) else json.dumps(given["body"]).encode()
    assert novig.string_to_sign(given["timestamp"], given["method"], given["path"], given["query"], body) \
        == vector["string_to_sign"]


@pytest.mark.parametrize("vector", VECTORS["vectors"], ids=lambda v: v["id"])
def test_signature_verifies(vector: dict) -> None:
    pair = VECTORS["keypairs"][vector["keypair_id"]]
    signer = novig.Signer("test-key", pair["private_key_pkcs8_pem"].encode())
    signature = signer.sign(vector["string_to_sign"])
    if vector["algorithm"] == "ed25519":
        assert signature == vector["signature"]  # Ed25519 is deterministic
    else:  # ECDSA is randomized: verify instead
        public = load_pem_public_key(pair["public_key_spki_pem"].encode())
        public.verify(base64.b64decode(signature), vector["string_to_sign"].encode(), ec.ECDSA(SHA256()))


def test_headers_carry_key_and_timestamp() -> None:
    pair = VECTORS["keypairs"]["ed25519-test-1"]
    headers = novig.Signer("kid", pair["private_key_pkcs8_pem"].encode()).headers("GET", "/v3/x", "b=2&a=1",
                                                                                   timestamp_ms=1755000000000)
    assert headers["Novig-Key-Id"] == "kid" and headers["Novig-Timestamp"] == "1755000000000"
    assert len(base64.b64decode(headers["Novig-Signature"])) == 64


# ----------------------------------------------------------------------------- take --
def test_take_walks_opposite_bids() -> None:
    # Bids on the other side at 0.60 (stake 0.40/contract) and 0.55 (0.45/contract).
    bids = [{"price": "0.55", "qty": 100000}, {"price": "0.60", "qty": 10000}]
    p, depth = novig.take(bids, 100.0)
    # $40 fills at 0.40, the other $60 at 0.45: payout 100 + 133.33.
    assert p == pytest.approx(100.0 / (40 / 0.40 + 60 / 0.45))
    assert depth == pytest.approx(40.0 + 450.0)


def test_take_sizes_favourites_to_win_and_underdogs_to_stake() -> None:
    # Bids at 0.60 on the other side sell this side at 0.40 (an underdog): $100 staked fills.
    dog = [{"price": "0.60", "qty": 25000}]  # $100 of stake resting
    assert novig.take(dog, 100.0) == (pytest.approx(0.40), 100.0)
    # Bids at 0.25 sell this side at 0.75 (−300): to win $100 needs $300 staked.
    fav = [{"price": "0.25", "qty": 30000}]  # $225 resting
    assert novig.take(fav, 100.0) is False
    p, depth = novig.take([{"price": "0.25", "qty": 40000}], 100.0)  # $300 resting
    assert p == pytest.approx(0.75) and depth == 300.0


def test_take_thin_and_empty() -> None:
    assert novig.take([{"price": "0.70", "qty": 10000}], 50.0) is False  # $30 resting
    assert novig.take([], 50.0) is None
    assert novig.take([{"price": "0.70", "qty": 0}], 50.0) is None


def test_take_fee_only_when_always_charged() -> None:
    bids = [{"price": "0.50", "qty": 100000}]
    assert novig.take(bids, 50.0, {"coefficient": "0.06", "charged": "WHEN_LIVE"})[0] == pytest.approx(0.5)
    assert novig.take(bids, 50.0, {"coefficient": "0.06", "charged": "ALWAYS"})[0] == pytest.approx(0.515)


# ------------------------------------------------------------------------- normalize --
def test_moneyline(frames: tuple[pl.DataFrame, pl.DataFrame]) -> None:
    odds, _ = frames
    ml = pick(odds, market="moneyline")
    assert dict(ml.select("side", "price").iter_rows()) == {"home": -144.0, "away": 138.0}
    row = ml.row(0, named=True)
    assert (row["book"], row["away_team"], row["home_team"]) == ("Novig", "SEA", "DET")
    assert row["start_time"] == datetime(2026, 10, 9, 23, 0, tzinfo=timezone.utc)
    assert row["market_uid"] == "game|moneyline|game|main" and row["line"] is None


def test_main_lines(frames: tuple[pl.DataFrame, pl.DataFrame]) -> None:
    odds, _ = frames
    main = odds.filter(~pl.col("is_alternate"))
    # DET is the moneyline favourite, so its -1.5 is the main puck line.
    pl_main = pick(main, market="puckline")
    assert dict(pl_main.select("side", "line").iter_rows()) == {"home": -1.5, "away": 1.5}
    # One main total and one main team total per team; every other rung is an alternate.
    assert pick(main, market="total")["line"].unique().to_list() == [6.5]
    assert sorted(pick(main, market="team_total")["subject"].to_list()) == ["away", "away", "home", "home"]
    # 3.5 and 9.5 are lopsided: their favourite sides can't fill "to win $100", so they're absent.
    assert pick(odds, market="total", is_alternate=True)["line"].unique().sort().to_list() == [4.5, 5.5, 7.5, 8.5]
    # Both sides of an alternate rung share the home-perspective market_uid.
    rung = pick(odds, market="puckline", market_uid="game|puckline|game|-2.5")
    assert dict(rung.select("side", "line").iter_rows()) == {"home": -2.5, "away": 2.5}


def test_game_prices_need_100(raw: dict, frames: tuple[pl.DataFrame, pl.DataFrame]) -> None:
    odds, _ = frames
    assert (odds["depth"] >= novig.GAME_MIN_STAKE).all()
    # The 2.5 total has bids on Over only: Under is takeable, Over has nothing to take.
    t25 = pick(odds, market="total", line=2.5)
    assert t25.is_empty() or set(t25["side"]) == {"under"}


def test_props(frames: tuple[pl.DataFrame, pl.DataFrame]) -> None:
    _, props = frames
    assert (props["depth"] >= novig.PROP_MIN_STAKE).all()
    copp = pick(props, player_name="Andrew Copp")
    # Only Under bids rest, so only the Over can be taken.
    assert copp.select("prop_type", "line", "side").rows() == [("shots", 2.5, "over")]
    assert set(props["prop_type"]) == {"shots", "goals", "points", "assists", "saves", "pp_points"}
    # First-goal markets in the fixture are pennies deep: omitted, not quoted.
    assert pick(props, prop_type="first_goal").is_empty()
    eberle = pick(props, player_name="Jordan Eberle")
    overround = sum(implied_probability(p) for p in eberle["price"])
    assert 1.0 < overround < 1.1


def test_player_name() -> None:
    assert novig.player_name({"description": "Michael Brandsegg-Nygard 1.5 SHOTS_ON_GOAL",
                              "marketType": "SHOTS_ON_GOAL"}) == "Michael Brandsegg-Nygard"
    assert novig.player_name({"description": "Andre Lee FIRST_GOAL_SCORER",
                              "marketType": "FIRST_GOAL_SCORER"}) == "Andre Lee"


def test_live_event_skipped(raw: dict) -> None:
    raw["events"][0]["status"] = "OPEN_INGAME"
    odds, props = novig.normalize(raw, CAPTURED)
    assert odds.is_empty() and props.is_empty()


# ------------------------------------------------------------------------------ fetch --
class FakeSession:
    """Answers GETs from the fixture by path; ``throttle`` 429s the first N calls."""

    def __init__(self, raw: dict, throttle: int = 0) -> None:
        self.raw, self.throttle = raw, throttle
        self.headers: dict[str, str] = {}
        self.urls: list[str] = []
        self.sent_headers: list[dict] = []

    def get(self, url: str, headers: dict, timeout: float) -> Any:
        self.urls.append(url)
        self.sent_headers.append(headers)
        if getattr(self, "locked", False) and headers:
            return FakeResponse(423, {"code": "ACCOUNT_LOCKED"})
        if self.throttle:
            self.throttle -= 1
            return FakeResponse(429, {"code": "RATE_LIMIT_EXCEEDED"}, {"Retry-After": "0"})
        path = url.split("novig.com", 1)[1].split("?", 1)[0].replace("/public", "")
        if path == "/v3/catalog/events":
            return FakeResponse(200, {"items": self.raw["events"], "next": None})
        if path == "/v3/catalog/markets":
            return FakeResponse(200, {"items": self.raw["markets"], "next": None})
        return FakeResponse(200, self.raw["books"][path.split("/")[4]])


class FakeResponse:
    def __init__(self, status: int, body: Any, headers: dict | None = None) -> None:
        self.status_code, self.body, self.headers, self.text = status, body, headers or {}, json.dumps(body)

    def json(self) -> Any:
        return self.body

    def raise_for_status(self) -> None:
        assert self.status_code < 400


def no_websocket(url: str, header: list, timeout: float) -> Any:
    raise ConnectionRefusedError("no websocket in tests")


def client_for(raw: dict, signer: novig.Signer | None = None, throttle: int = 0,
               ws_connect: Any = no_websocket) -> tuple[novig.NovigClient, FakeSession]:
    client = novig.NovigClient("https://api.novig.com", signer, rps=1000, ws_connect=ws_connect)
    session = FakeSession(raw, throttle)
    client.session = session  # type: ignore[assignment]
    return client, session


class FakeWebSocket:
    """Answers ``snapshot`` frames from the fixture's books; ``replies`` queue frames first."""

    def __init__(self, raw: dict, skip: set[str] | None = None, replies: list[dict] | None = None) -> None:
        self.raw, self.skip, self.queued = raw, skip or set(), list(replies or [])
        self.sent: list[dict] = []
        self.pending: list[str] = []
        self.closed = False
        self.header: list[str] = []
        self.url = ""

    def __call__(self, url: str, header: list, timeout: float) -> FakeWebSocket:
        self.url, self.header = url, header
        return self

    def send(self, text: str) -> None:
        frame = json.loads(text)
        self.sent.append(frame)
        self.pending.append(json.dumps({"ts": 1}))  # a heartbeat-like frame to skip
        if self.queued:
            self.pending.append(json.dumps(self.queued.pop(0)))
            return
        events = set(frame["snapshot"]["events"])
        snap = {m["marketId"]: {"eventId": m["eventId"], "book": self.raw["books"][m["marketId"]],
                                "lifecycle": {"seq": 1, "status": "OPEN"}}
                for m in self.raw["markets"] if m["eventId"] in events and m["marketId"] not in self.skip}
        self.pending.append(json.dumps({"nonce": frame["nonce"], "snapshot": snap}))

    def recv(self) -> str:
        return self.pending.pop(0)

    def close(self) -> None:
        self.closed = True


def signer() -> novig.Signer:
    return novig.Signer("kid", VECTORS["keypairs"]["ed25519-test-1"]["private_key_pkcs8_pem"].encode())


def test_public_fetch_uses_public_routes_and_waits_out_429(raw: dict) -> None:
    client, session = client_for(raw, throttle=1)
    got, _ = novig.fetch(client, now=CAPTURED)
    assert len(got["books"]) == len(raw["markets"])
    assert all("/v3/public/catalog/" in u for u in session.urls)
    assert not any(session.sent_headers)
    # Game lines are read before props.
    kinds = {m["marketId"]: m["marketType"] for m in raw["markets"]}
    order = [kinds[u.split("/")[-2]] for u in session.urls if "/book" in u]
    n_game = sum(k in novig.GAME_MARKETS for k in order)
    assert all(k in novig.GAME_MARKETS for k in order[:n_game])


def test_signed_fetch_signs_catalog_routes(raw: dict) -> None:
    pair = VECTORS["keypairs"]["ed25519-test-1"]
    client, session = client_for(raw, novig.Signer("kid", pair["private_key_pkcs8_pem"].encode()))
    novig.fetch(client, now=CAPTURED)
    assert all("/v3/catalog/" in u and "/public/" not in u for u in session.urls)
    assert all(h["Novig-Key-Id"] == "kid" for h in session.sent_headers)


def test_refused_key_falls_back_to_public(raw: dict) -> None:
    pair = VECTORS["keypairs"]["ed25519-test-1"]
    client, session = client_for(raw, novig.Signer("kid", pair["private_key_pkcs8_pem"].encode()))
    session.locked = True
    got, _ = novig.fetch(client, now=CAPTURED)
    assert len(got["books"]) == len(raw["markets"])


def test_signed_fetch_reads_books_in_one_snapshot(raw: dict) -> None:
    ws = FakeWebSocket(raw)
    client, session = client_for(raw, signer(), ws_connect=ws)
    got, _ = novig.fetch(client, now=CAPTURED)
    assert len(got["books"]) == len(raw["markets"])
    assert not any("/book" in u for u in session.urls)  # no REST book reads
    assert len(ws.sent) == 1 and ws.sent[0]["snapshot"]["events"] == {raw["events"][0]["eventId"]: "book"}
    assert ws.url == "wss://api.novig.com/v3/ws" and any(h.startswith("Novig-Signature: ") for h in ws.header)
    assert ws.closed
    assert novig.normalize(got, CAPTURED)[0].height == 26


def test_snapshot_gaps_filled_over_rest(raw: dict) -> None:
    missing = {m["marketId"] for m in raw["markets"][:3]}
    client, session = client_for(raw, signer(), ws_connect=FakeWebSocket(raw, skip=missing))
    got, _ = novig.fetch(client, now=CAPTURED)
    assert len(got["books"]) == len(raw["markets"])
    assert {u.split("/")[-2] for u in session.urls if "/book" in u} == missing


def test_snapshot_refused_falls_back_to_rest(raw: dict) -> None:
    ws = FakeWebSocket(raw, replies=[{"nonce": 1, "code": "SCOPE_INSUFFICIENT"}])
    client, session = client_for(raw, signer(), ws_connect=ws)
    got, _ = novig.fetch(client, now=CAPTURED)
    assert len(got["books"]) == len(raw["markets"])
    assert sum("/book" in u for u in session.urls) == len(raw["markets"])


def test_throttled_snapshot_waits_and_retries(raw: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    monkeypatch.setattr(novig.time, "sleep", waits.append)
    ws = FakeWebSocket(raw, replies=[{"code": "RATE_LIMIT_EXCEEDED"}])
    client, _ = client_for(raw, signer(), ws_connect=ws)
    books = novig.snapshot_books(client, raw["markets"])
    assert len(books) == len(raw["markets"]) and waits == [16.0]
    assert [f["nonce"] for f in ws.sent] == [1, 2]


def test_large_slates_split_across_snapshots(raw: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(novig, "MAX_WS_MARKETS", 10)
    other = dict(raw["markets"][0], eventId="other-event", marketId="other-market")
    raw["books"]["other-market"] = raw["books"][raw["markets"][0]["marketId"]]
    raw["markets"].append(other)
    ws = FakeWebSocket(raw)
    client, _ = client_for(raw, signer(), ws_connect=ws)
    books = novig.snapshot_books(client, raw["markets"])
    assert len(ws.sent) == 2 and len(books) == len(raw["markets"])


def test_budget_stops_book_reads(raw: dict) -> None:
    client, _ = client_for(raw)
    got, _ = novig.fetch(client, now=CAPTURED, budget_s=-1)
    assert got["books"] == {} and len(got["markets"]) == len(raw["markets"])


def test_refused_catalog_raises(raw: dict) -> None:
    client, session = client_for(raw, throttle=100)
    with pytest.raises(SourceUnavailable):
        novig.fetch(client, now=CAPTURED)


# ------------------------------------------------------------------------------ poll --
class FakeStore:
    """In-memory stand-in for :class:`nhl.storage.s3.Store`."""

    def __init__(self) -> None:
        self.parquet: dict[str, pl.DataFrame] = {}
        self.json: dict[str, Any] = {}

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        return self.parquet.get(key)

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        self.parquet[key] = df
        return key

    def put_json_gz(self, key: str, obj: Any) -> str:
        self.json[key] = obj
        return key


class NoRosters:
    """A player resolver that knows nobody (ids stay null)."""

    stats: dict[str, int] = defaultdict(int)

    def roster(self, team: str) -> list:
        return []

    def resolve(self, team: str | None, name: str | None) -> None:
        return None


def test_poll_stores_odds_and_props(raw: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(novig, "utcnow", lambda: CAPTURED)
    games = pl.DataFrame({"game_id": [2026020061], "game_date": [date(2026, 10, 9)], "home_abbr": ["DET"],
                          "away_abbr": ["SEA"]})
    store = FakeStore()
    client, _ = client_for(raw)
    odds_n, props_n = novig.poll(store, games, client=client, resolver=NoRosters())  # type: ignore[arg-type]
    # Heavy-favourite sides (e.g. a 0.5-goal under) must fill "to win" the minimum, so a few drop.
    assert odds_n == 26 and props_n == 15
    assert set(store.parquet[keys.odds(20262027, "novig")]["game_id"]) == {2026020061}
    assert store.parquet["external/odds/props/20262027/novig.parquet"].height == 15
    assert any(k.startswith("raw/external/novig/") and k.endswith("-board.json.gz") for k in store.json)
    # A second poll of an unchanged board stores nothing.
    assert novig.poll(store, games, client=client, resolver=NoRosters()) == (0, 0)  # type: ignore[arg-type]


def test_poll_refused_is_skip(raw: dict) -> None:
    client, _ = client_for(raw, throttle=100)
    store = FakeStore()
    assert novig.poll(store, pl.DataFrame(), client=client) == (0, 0)
    assert not store.parquet
