"""NHL roster transactions from ESPN's public transactions feed.

``site.api.espn.com/apis/site/v2/sports/hockey/nhl/transactions`` returns
``{"count", "pageIndex", "pageCount", "transactions": [{"date", "description", "team"}]}``,
newest first. ``season=YYYY`` selects a *calendar* year (history reaches back to at
least 2004), ``limit`` (up to 1000) and ``page`` paginate. Entries have no id and no
athlete reference: the description is free text, often several actions in one entry
("Recalled F A from Iowa (AHL). Placed D B on injured reserve."). The core API
(``sports.core.api.espn.com/.../transactions``) answers with an empty list, and neither
api-web nor the stats API exposes transactions.

Each entry is split into clauses (sentences, plus ``"... and placed G X ..."``), each
clause is classified into :data:`TYPES`, and one row is written per player named in the
clause (position-tagged names such as ``"Fs A, B and C"``; a pronoun ("placed him on
waivers") reuses the previous clause's players). Clauses that name no player (coaching
moves, draft-pick-only trades) keep one row with a null player. For trades, players the
team receives get ``direction = "in"`` and players it gives up (after "for" /
"in exchange for") ``direction = "out"``.

Rows are keyed by ``transaction_id``, a hash of (date, ESPN team id, description, clause
index, player, direction): re-polling the same feed writes nothing.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterable, Sequence
from datetime import date, datetime
from typing import Any

import polars as pl

from nhl import config
from nhl.ingest.http import NHLClient, SourceUnavailable, WebClient
from nhl.sources.common import append_transitions, archive_raw, utcnow
from nhl.sources.dailyfaceoff import (
    EASTERN,
    PlayerResolver,
    parse_time,
    season_for_date,
)
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

TRANSACTIONS_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/transactions"
SOURCE = "espn"
PAGE_LIMIT = 1000
KEYS = ("transaction_id",)
VALUES: tuple[str, ...] = ()

TYPES: tuple[str, ...] = (
    "recalled", "assigned", "waived", "claimed", "traded", "signed",
    "placed_ir", "activated_ir", "suspended", "retired", "other",
)

SCHEMA: dict[str, pl.DataType] = {
    "transaction_id": pl.String(),
    "date": pl.Date(),
    "season": pl.Int32(),
    "team": pl.String(),
    "espn_team_id": pl.Int64(),
    "player_name": pl.String(),
    "player_id": pl.Int64(),
    "position": pl.String(),
    "type": pl.String(),
    "direction": pl.String(),
    "clause": pl.String(),
    "description": pl.String(),
    "source": pl.String(),
    "captured_at": pl.Datetime("us", "UTC"),
}

# --------------------------------------------------------------------------- text parsing
#: Words ending in "." that do not end a sentence.
_ABBREVIATIONS = {"st.", "ste.", "jr.", "sr.", "no.", "mt.", "ft.", "vs.", "dr.", "lt."}
_INITIALS_RE = re.compile(r"^(?:[A-Z]\.)+$")
_POSITION_RE = re.compile(r"^(?:(?:LW|RW|LD|RD|C|D|F|G|W)(?:/(?:LW|RW|LD|RD|C|D|F|G|W))*)s?$|^[cdfg]$")
#: Capitalized words that still end a name ("Recalled G Nico Daws From Utica").
_STOP_WORDS = {"from", "to", "for", "on", "in", "and", "with", "off", "as"}
_PARTICLES = {"van", "von", "de", "der", "den", "di", "da", "du", "la", "le", "af", "st.", "del", "dos"}
_CLAUSE_VERBS = (
    "recalled|placed|sent|assigned|reassigned|reinstated|activated|signed|re-signed|waived|loaned|"
    "designated|claimed|released|acquired|traded|moved|transferred|called|returned|suspended|"
    "agreed|brought|named|removed"
)
_CLAUSE_SPLIT_RE = re.compile(rf",?\s+and\s+(?=(?:{_CLAUSE_VERBS})\b)")
_TRADE_OUT_RE = re.compile(r"\s(?:in\s+exchange\s+for|for)\s")
_PRONOUN_RE = re.compile(r"\b(?:him|them)\b")
#: Verbs whose object is a player even without a position tag ("Reinstated Tomas Nosek").
_PLAYER_VERBS = {
    "recalled", "placed", "sent", "assigned", "reassigned", "reinstated", "activated", "signed",
    "re-signed", "waived", "loaned", "claimed", "released", "acquired", "suspended", "moved",
    "transferred", "returned", "promoted",
}
_NOT_NAMES = {"a", "an", "the", "two", "three", "four", "future", "conditional", "head", "assistant", "general"}


def split_sentences(text: str) -> list[str]:
    """Split a description into sentences without breaking "St. Louis", "A.J." or "No. 23"."""
    words = text.split()
    sentences: list[str] = []
    current: list[str] = []
    for i, word in enumerate(words):
        current.append(word)
        nxt = words[i + 1] if i + 1 < len(words) else ""
        ends = word.endswith((".", "!", "?")) and word.lower() not in _ABBREVIATIONS
        ends = ends and not _INITIALS_RE.match(word) and (not nxt or nxt[:1].isupper())
        if ends:
            sentences.append(" ".join(current))
            current = []
    if current:
        sentences.append(" ".join(current))
    return [s.strip() for s in sentences if s.strip()]


def split_clauses(description: str) -> list[str]:
    """Sentences, further split at ``", and <verb>"`` (one action per clause)."""
    out: list[str] = []
    for sentence in split_sentences(description):
        parts = _CLAUSE_SPLIT_RE.split(sentence)
        out += [p.strip().rstrip(".").strip() for p in parts if p.strip(" .")]
    return out


def classify(clause: str) -> str:
    """Map one clause to the normalized vocabulary in :data:`TYPES`."""
    text = clause.lower()
    first = text.split(" ", 1)[0]
    injured = bool(re.search(r"injured reserve|injured non-roster|non-roster injured|\bltir\b|\bir\b", text))
    if first in ("activated", "reinstated") and "waiv" not in text:
        if "conditioning" in text:
            return "recalled"
        if "suspension" in text:
            return "other"
        return "activated_ir"
    if "suspen" in text:
        return "suspended"
    if "retire" in text:
        return "retired"
    if "claimed" in text:
        return "claimed"
    if "waiv" in text:
        return "waived"
    if injured and "from injured" in text:
        return "activated_ir"
    if injured:
        return "placed_ir"
    if first == "recalled" or text.startswith(("called up", "brought up")) or (first == "promoted" and " from " in text):
        return "recalled"
    if first in ("assigned", "sent", "loaned", "reassigned", "returned"):
        return "assigned"
    if first in ("acquired", "traded") or " acquired " in text:
        return "traded"
    if first in ("signed", "re-signed") or text.startswith("agreed to terms") or " signed " in text:
        return "signed"
    return "other"


def _tokens(clause: str) -> list[str]:
    """Whitespace tokens with commas split off (``"A, B"`` -> ``["A", ",", "B"]``)."""
    return re.sub(r",", " , ", clause).split()


def _name_like(token: str) -> bool:
    """A token that can be part of a person's name."""
    core = token.strip("()")
    return bool(core) and (core[0].isupper() or core.lower() in _PARTICLES) and not core.startswith("(")


def _read_names(tokens: list[str], start: int) -> tuple[list[str], int]:
    """Read a ``"A B, C D and E F"`` name list starting at ``tokens[start]``.

    Stops at the first lowercase word that is not a separator or name particle, at a
    position tag that begins a new group, or at the end.

    Returns:
        (names, index of the first unread token)
    """
    names: list[str] = []
    current: list[str] = []
    i = start
    while i < len(tokens):
        tok = tokens[i]
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        if tok in (",", "and") or tok.lower() == "and":
            if current:
                names.append(" ".join(current))
                current = []
            if nxt.lower() in (",", "and"):  # Oxford comma: "A, B, and C"
                i += 1
                continue
            if _name_like(nxt) and not _POSITION_RE.match(nxt):
                i += 1
                continue
            break
        if _POSITION_RE.match(tok) and _name_like(nxt):
            break
        if not _name_like(tok) or tok.startswith("(") or tok.lower() in _STOP_WORDS:
            break
        if tok.lower() in _PARTICLES and not _name_like(nxt):
            break
        current.append(tok)
        i += 1
    if current:
        names.append(" ".join(current))
    return [n.strip(" ,.;:") for n in names if n.strip(" ,.;:")], i


def extract_players(clause: str, kind: str | None = None) -> list[tuple[str, str | None]]:
    """Players named in a clause as ``[(name, position)]`` in order of appearance.

    Position-tagged groups (``"Recalled Ds A and B, F C"``) are read wherever they occur;
    without any tag, a capitalized name right after a player verb is taken
    (``"Reinstated Tomas Nosek from ..."``) unless ``kind`` is ``"other"`` (staff moves).
    """
    tokens = _tokens(clause)
    found: list[tuple[str, str | None]] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        if _POSITION_RE.match(tok) and _name_like(nxt) and not _POSITION_RE.match(nxt):
            position = (tok[:-1] if tok.endswith("s") and len(tok) > 1 else tok).upper()
            names, i = _read_names(tokens, i + 1)
            found += [(n, position) for n in names]
            continue
        i += 1
    fallback = not found and kind != "other" and len(tokens) > 1 and tokens[0].lower() in _PLAYER_VERBS
    if fallback and _name_like(tokens[1]) and tokens[1].lower() not in _NOT_NAMES:
        names, _ = _read_names(tokens, 1)
        found = [(n, None) for n in names if len(n.split()) >= 2]
    return found


def _transaction_id(*parts: Any) -> str:
    """Stable 16-hex id from the identifying parts of one row."""
    return hashlib.sha1("|".join("" if p is None else str(p) for p in parts).encode()).hexdigest()[:16]


def _entry_date(value: str | None) -> date | None:
    """ESPN stamps entries at local midnight in UTC (``07:00Z``); take the Eastern date."""
    moment = parse_time(value)
    return moment.astimezone(EASTERN).date() if moment else None


# --------------------------------------------------------------------------- normalize
def normalize_transactions(
    payload: dict[str, Any], captured_at: datetime, resolver: PlayerResolver | None = None
) -> pl.DataFrame:
    """One row per (entry, clause, player) of an ESPN transactions page.

    Args:
        payload: Raw ESPN transactions JSON (one page).
        captured_at: Poll time (UTC).
        resolver: Optional player-id resolver; ``player_id`` is null without one.

    Returns:
        Frame with :data:`SCHEMA` columns.
    """
    rows: list[dict[str, Any]] = []
    for entry in payload.get("transactions") or []:
        description = " ".join((entry.get("description") or "").split())
        espn_team = entry.get("team") or {}
        team = resolve_team(espn_team.get("abbreviation")) or resolve_team(espn_team.get("displayName"))
        if team is None and espn_team:
            logger.warning("espn transactions: unknown team %r", espn_team.get("displayName"))
        day = _entry_date(entry.get("date"))
        team_id = int(espn_team["id"]) if str(espn_team.get("id") or "").isdigit() else None
        previous: list[tuple[str, str | None]] = []
        for idx, clause in enumerate(split_clauses(description) or [description]):
            kind = classify(clause)
            incoming, outgoing = clause, ""
            if kind == "traded":
                match = _TRADE_OUT_RE.search(clause)
                if match:
                    incoming, outgoing = clause[: match.start()], clause[match.end():]
            players = [(n, p, "in" if kind == "traded" else None) for n, p in extract_players(incoming, kind)]
            players += [(n, p, "out") for n, p in extract_players(outgoing, kind)] if outgoing else []
            continuation = clause[:1].islower() or bool(_PRONOUN_RE.search(clause))
            if not players and continuation and previous:
                players = [(n, p, None) for n, p in previous]
            previous = [(n, p) for n, p, _ in players] or previous
            for name, position, direction in players or [(None, None, None)]:
                rows.append({
                    "transaction_id": _transaction_id(day, team_id, description, idx, name, direction),
                    "date": day,
                    "season": season_for_date(day) if day else None,
                    "team": team,
                    "espn_team_id": team_id,
                    "player_name": name,
                    "player_id": resolver.resolve(team, name) if resolver and name else None,
                    "position": position,
                    "type": kind,
                    "direction": direction,
                    "clause": clause,
                    "description": description,
                    "source": SOURCE,
                    "captured_at": captured_at,
                })
    return pl.DataFrame(rows, schema=SCHEMA).unique("transaction_id", keep="first", maintain_order=True)


# --------------------------------------------------------------------------- fetch / store
def _client() -> WebClient:
    """ESPN JSON client (<= 1 req/s)."""
    return WebClient("espn", rps=1.0, headers={"Accept": "application/json"})


def fetch_year(client: WebClient, year: int | None = None, max_pages: int | None = None) -> list[dict[str, Any]]:
    """Every page of one calendar year's transactions (``year=None``: the current year).

    Args:
        client: ESPN client.
        year: Calendar year (ESPN's ``season`` parameter is a calendar year here).
        max_pages: Stop after this many pages (polls only need the newest).
    """
    pages: list[dict[str, Any]] = []
    page = 1
    while True:
        params: dict[str, Any] = {"limit": PAGE_LIMIT, "page": page}
        if year is not None:
            params["season"] = year
        payload = client.get_json(TRANSACTIONS_URL, params=params)
        pages.append(payload)
        if page >= int(payload.get("pageCount") or 1) or (max_pages and page >= max_pages):
            return pages
        page += 1


def write_transactions(store: Store, rows: pl.DataFrame, seasons: Iterable[int] | None = None) -> int:
    """Append unseen rows to :func:`keys.transactions`, partitioned by season.

    Args:
        store: S3 store.
        rows: Normalized rows.
        seasons: Only write these 8-digit seasons (default: all present).
    """
    rows = rows.filter(pl.col("season").is_not_null())
    wanted = set(seasons) if seasons is not None else None
    written = 0
    for (season,), part in sorted(rows.partition_by("season", as_dict=True).items()):
        if wanted is not None and season not in wanted:
            continue
        written += append_transitions(store, keys.transactions(int(season)), part, KEYS, VALUES)
    return written


def _resolver(store: Store) -> PlayerResolver:
    """Shared resolver; its api-web roster calls stay well under our 3 req/s budget."""
    return PlayerResolver.from_store(store, client=NHLClient(rps=2))


def poll_transactions(
    store: Store, client: WebClient | None = None, resolver: PlayerResolver | None = None
) -> int:
    """Fetch the newest page of ESPN transactions and append unseen rows.

    Args:
        store: S3 store.
        client: Optional ESPN client.
        resolver: Optional shared player-id resolver.

    Returns:
        Number of rows written to :func:`keys.transactions`.
    """
    client = client or _client()
    captured_at = utcnow()
    try:
        pages = fetch_year(client, None, max_pages=1)
    except SourceUnavailable as exc:
        logger.warning("espn transactions skipped: %r", exc)
        return 0
    archive_raw(store, SOURCE, captured_at, "transactions", pages[0])
    resolver = resolver or _resolver(store)
    rows = normalize_transactions(pages[0], captured_at, resolver)
    resolver.log_unresolved("espn transactions")
    return write_transactions(store, rows)


def backfill_transactions(
    store: Store,
    start_years: Sequence[int],
    client: WebClient | None = None,
    resolver: PlayerResolver | None = None,
) -> int:
    """Backfill whole seasons from ESPN's calendar-year history.

    A season ``Y`` (July ``Y`` - June ``Y+1``) spans calendar years ``Y`` and ``Y+1``; each
    needed calendar year is fetched once (all pages, archived raw) and only rows dated
    within the requested seasons are written.

    Args:
        store: S3 store.
        start_years: Season start years, e.g. ``range(2010, 2027)``.
        client: Optional ESPN client.
        resolver: Optional shared player-id resolver.

    Returns:
        Number of rows written.
    """
    client = client or _client()
    resolver = resolver or _resolver(store)
    seasons = {config.season_id(y) for y in start_years}
    this_year = utcnow().astimezone(EASTERN).year
    years = sorted({y for s in start_years for y in (s, s + 1) if y <= this_year})
    written = 0
    for year in years:
        captured_at = utcnow()
        try:
            pages = fetch_year(client, year)
        except SourceUnavailable as exc:
            logger.warning("espn transactions %d skipped: %r", year, exc)
            continue
        frames = []
        for n, payload in enumerate(pages, 1):
            archive_raw(store, SOURCE, captured_at, f"transactions-{year}-p{n}", payload)
            frames.append(normalize_transactions(payload, captured_at, resolver))
        rows = pl.concat(frames)
        logger.info("espn transactions %d: %d entries -> %d rows", year,
                    sum(len(p.get("transactions") or []) for p in pages), rows.height)
        written += write_transactions(store, rows, seasons)
    resolver.log_unresolved("espn transactions backfill")
    return written
