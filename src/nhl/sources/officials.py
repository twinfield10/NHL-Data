"""Game officials: referees and linesmen per game, plus pregame assignment captures.

Two sources:

* **NHL API (historical, post-game)** - ``/v1/gamecenter/{game_id}/right-rail`` ->
  ``gameInfo.referees[]`` / ``gameInfo.linesmen[]`` with ``fullName.default`` and
  ``sweaterNumber``. The lists stay empty until the game starts, so this is the
  record of who actually worked a game, not a pregame signal. Raw payloads are cached
  verbatim under ``raw/external/right_rail/{season}/{game_id}.json.gz`` (a stored payload
  is never refetched, so backfills are resumable) and normalized into
  ``keys.officials(season)``.
* **Scouting the Refs (pregame)** - scoutingtherefs.com publishes a "Tonight's NHL
  Referees and Linespersons" WordPress post each game day, usually 10:30 ET-3:30 ET, and
  edits it when assignments change. robots.txt only disallows ``/wp-admin/``; posts are
  found through the public category RSS feed and parsed from the post HTML (no
  ``wp-json`` API calls). Each game is an ``<h1>`` "Away Team at Home Team" followed by
  a REFEREES and a LINESPERSONS block whose names appear as ``<strong>Name #NN</strong>``.
  Captures are stored as transitions in ``keys.ref_assignments(season)`` keyed by
  (game_id, role, official_name); an official dropped from a later edit is recorded with
  ``is_assigned = False``.

Officials are identified by ``official_id``: an accent/punctuation-insensitive name
slug ("Frederick L'Ecuyer" -> ``frederick-lecuyer``). Sweater numbers are carried as a
check; :func:`identity_report` lists names with more than one number and numbers worn by
more than one name, and :data:`OFFICIAL_ALIASES` folds spelling variants together.
"""

from __future__ import annotations

import html
import logging
import re
import unicodedata
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from nhl.ingest.http import WEB_API, NHLClient, SourceUnavailable, WebClient
from nhl.sources.common import append_transitions, archive_raw, utcnow
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

EASTERN = ZoneInfo("America/New_York")
ROLES: dict[str, str] = {"referees": "referee", "linesmen": "linesman"}

#: Spelling variants of the same official -> canonical id (fill from :func:`identity_report`).
OFFICIAL_ALIASES: dict[str, str] = {}

OFFICIAL_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "season": pl.Int32(),
    "game_date": pl.Date(),
    "role": pl.String(),
    "slot": pl.Int8(),
    "official_id": pl.String(),
    "official_name": pl.String(),
    "sweater_number": pl.Int32(),
}

STR_BASE = "https://scoutingtherefs.com"
STR_FEED = f"{STR_BASE}/category/tonights-officials/nhl-tonights-officials/feed/"
ASSIGNMENT_KEYS = ("game_id", "role", "official_name")
ASSIGNMENT_VALUES = ("is_assigned", "sweater_number")

ASSIGNMENT_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Int64(),
    "season": pl.Int32(),
    "game_date": pl.Date(),
    "home_abbr": pl.String(),
    "away_abbr": pl.String(),
    "role": pl.String(),
    "official_id": pl.String(),
    "official_name": pl.String(),
    "sweater_number": pl.Int32(),
    "is_assigned": pl.Boolean(),
    "post_url": pl.String(),
    "post_published_at": pl.Datetime("us", "UTC"),
    "post_modified_at": pl.Datetime("us", "UTC"),
    "captured_at": pl.Datetime("us", "UTC"),
}


# --------------------------------------------------------------------------- identity
def clean_name(name: str | None) -> str | None:
    """Collapse whitespace and unescape entities in an official's display name."""
    if not name:
        return None
    text = re.sub(r"\s+", " ", html.unescape(name).replace("\xa0", " ")).strip()
    return text or None


def official_id(name: str | None) -> str | None:
    """Stable id for an official: accent-, case- and punctuation-insensitive name slug.

    ``"Frederick L’Ecuyer"`` -> ``"frederick-lecuyer"``; ``"Francois St-Laurent"`` ->
    ``"francois-st-laurent"``. Known spelling variants are folded via
    :data:`OFFICIAL_ALIASES`.

    Args:
        name: Display name.
    """
    name = clean_name(name)
    if not name:
        return None
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    ascii_name = re.sub(r"['’.]", "", ascii_name)
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_name).strip("-")
    return OFFICIAL_ALIASES.get(slug, slug)


def identity_report(officials: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Check that sweater numbers and names identify officials consistently.

    Args:
        officials: Rows with ``official_id``, ``official_name``, ``sweater_number``,
            ``season`` and ``role``.

    Returns:
        ``{"multi_number": ..., "shared_number": ...}``: officials seen with more than one
        sweater number (with the seasons each number was worn), and (season, number) pairs
        worn by more than one official id - the latter usually means a spelling variant.
    """
    per_number = officials.group_by("official_id", "sweater_number").agg(
        pl.col("official_name").unique().sort().alias("names"),
        pl.col("season").min().alias("first_season"),
        pl.col("season").max().alias("last_season"),
        pl.len().alias("games"),
    )
    multi = per_number.filter(pl.len().over("official_id") > 1).sort("official_id", "first_season")
    shared = (
        officials.group_by("season", "sweater_number")
        .agg(pl.col("official_id").unique().sort().alias("official_ids"), pl.len().alias("games"))
        .filter(pl.col("official_ids").list.len() > 1)
        .sort("season", "sweater_number")
    )
    return {"multi_number": multi, "shared_number": shared}


# --------------------------------------------------------------------------- NHL API
def raw_right_rail_key(season: int, game_id: int) -> str:
    """Key for one game's cached right-rail payload."""
    return f"raw/external/right_rail/{season}/{game_id}.json.gz"


def raw_right_rail_prefix(season: int) -> str:
    """Prefix holding every cached right-rail payload for a season."""
    return f"raw/external/right_rail/{season}/"


def fetch_right_rail(client: NHLClient, game_id: int) -> dict[str, Any]:
    """Gamecenter right-rail payload (series, officials, team stats) for one game."""
    return client.get_json(f"{WEB_API}/gamecenter/{game_id}/right-rail")


def normalize_officials(payload: dict[str, Any], game_id: int, season: int, game_date: date | None = None) -> pl.DataFrame:
    """One row per official listed in a right-rail payload.

    Args:
        payload: Raw right-rail JSON.
        game_id: NHL game id.
        season: 8-digit season id.
        game_date: Optional game date (carried through for convenience).

    Returns:
        Frame with :data:`OFFICIAL_SCHEMA` (empty when the game lists no officials).
    """
    info = (payload or {}).get("gameInfo") or {}
    rows: list[dict[str, Any]] = []
    for field_name, role in ROLES.items():
        for slot, person in enumerate(info.get(field_name) or [], 1):
            name = clean_name(((person or {}).get("fullName") or {}).get("default"))
            if not name:
                continue
            number = person.get("sweaterNumber")
            rows.append({
                "game_id": game_id,
                "season": season,
                "game_date": game_date,
                "role": role,
                "slot": slot,
                "official_id": official_id(name),
                "official_name": name,
                "sweater_number": int(number) if number is not None else None,
            })
    return pl.DataFrame(rows, schema=OFFICIAL_SCHEMA)


@dataclass
class OfficialsReport:
    """Outcome of an officials backfill for one season."""

    season: int
    candidates: int = 0
    fetched: int = 0
    cached: int = 0
    no_officials: list[int] = field(default_factory=list)
    failed: dict[int, str] = field(default_factory=dict)
    rows: int = 0

    def summary(self) -> str:
        """One-line human-readable summary."""
        return (
            f"{self.season}: {self.candidates} final games | fetched {self.fetched} | cached {self.cached} | "
            f"no officials {len(self.no_officials)} | failed {len(self.failed)} | {self.rows} rows"
        )


def final_games(games: pl.DataFrame, season: int) -> pl.DataFrame:
    """Final regular-season and playoff games of a season (game_id, game_date)."""
    return games.filter((pl.col("season") == season) & pl.col("is_final")).select("game_id", "game_date").sort("game_id")


def backfill_officials(
    store: Store,
    games: pl.DataFrame,
    season: int,
    client: NHLClient | None = None,
    workers: int = 3,
) -> OfficialsReport:
    """Cache right-rail payloads for a season's final games and write ``keys.officials``.

    Resumable: payloads already under ``raw/external/right_rail/{season}/`` are not
    refetched. The season table is rebuilt from the cache on every run.

    Args:
        store: S3 store.
        games: Games table (``processed/games.parquet``).
        season: 8-digit season id.
        client: NHL client; defaults to one limited to 3 req/s (leaves headroom for other
            ingest jobs sharing the API).
        workers: Concurrent download threads (they share the client's rate limit).

    Returns:
        An :class:`OfficialsReport`.
    """
    client = client or NHLClient(rps=3)
    todo_games = final_games(games, season)
    report = OfficialsReport(season=season, candidates=todo_games.height)
    stored = {keys.game_id_from_key(k) for k in store.list_keys(raw_right_rail_prefix(season))}
    todo = [g for g in todo_games["game_id"].to_list() if g not in stored]
    report.cached = report.candidates - len(todo)
    logger.info("%s: %d final games, %d right-rail payloads to fetch", season, report.candidates, len(todo))

    def fetch(game_id: int) -> None:
        store.put_json_gz(raw_right_rail_key(season, game_id), fetch_right_rail(client, game_id))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch, g): g for g in todo}
        for n, fut in enumerate(as_completed(futures), 1):
            game_id = futures[fut]
            try:
                fut.result()
                report.fetched += 1
            except Exception as exc:  # noqa: BLE001 - record and keep going; reported at the end
                report.failed[game_id] = repr(exc)
                logger.warning("right-rail %s failed: %r", game_id, exc)
            if n % 200 == 0:
                logger.info("%s: %d/%d right-rail fetched", season, n, len(todo))

    table = build_officials(store, todo_games, season, report)
    if not table.is_empty():
        store.put_parquet(keys.officials(season), table)
    report.rows = table.height
    logger.info(report.summary())
    return report


def build_officials(
    store: Store, season_games: pl.DataFrame, season: int, report: OfficialsReport | None = None
) -> pl.DataFrame:
    """Normalize every cached right-rail payload of a season into one officials table.

    Args:
        store: S3 store.
        season_games: (game_id, game_date) of the games to include.
        season: 8-digit season id.
        report: Optional report; games with no officials listed are appended to it.
    """
    frames: list[pl.DataFrame] = []
    for game_id, game_date in season_games.select("game_id", "game_date").iter_rows():
        payload = store.get_json_gz(raw_right_rail_key(season, game_id))
        if payload is None:
            continue
        frame = normalize_officials(payload, game_id, season, game_date)
        if frame.is_empty() and report is not None:
            report.no_officials.append(game_id)
        frames.append(frame)
    if not frames:
        return pl.DataFrame(schema=OFFICIAL_SCHEMA)
    return pl.concat(frames).sort("game_id", "role", "slot")


# --------------------------------------------------------------------------- Scouting the Refs
_DATE_IN_TITLE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{2,4})")
_H1 = re.compile(r"<h1\b([^>]*)>(.*?)</h1>", re.S | re.I)
_H3 = re.compile(r"<h3\b[^>]*>(.*?)</h3>", re.S | re.I)
_STRONG = re.compile(r"<strong>(.*?)</strong>", re.S | re.I)
_NAME_NUMBER = re.compile(r"^(.+?)\s*#\s*(\d{1,2})$")
_TAG = re.compile(r"<[^>]+>")
_META_TIME = re.compile(r'<meta[^>]+property="article:(published|modified)_time"[^>]+content="([^"]+)"', re.I)


def _str_client() -> WebClient:
    """Polite HTML client for scoutingtherefs.com (<= 0.5 req/s)."""
    return WebClient("scoutingtherefs", rps=0.5, headers={"Accept": "text/html,application/xhtml+xml,application/xml"})


def _text(fragment: str) -> list[str]:
    """Visible text lines of an HTML fragment (``<br>`` separates lines)."""
    fragment = re.sub(r"<br\s*/?>", "\n", fragment, flags=re.I)
    text = html.unescape(_TAG.sub("", fragment)).replace("\xa0", " ")
    return [line.strip() for line in text.splitlines() if line.strip()]


def slate_date(title: str) -> date | None:
    """Game date from a post title such as ``"Tonight's NHL Referees and Linespersons – 10/4/26"``."""
    match = _DATE_IN_TITLE.search(html.unescape(title or ""))
    if not match:
        return None
    month, day, year = (int(p) for p in match.groups())
    return date(year + 2000 if year < 100 else year, month, day)


def parse_feed(xml_text: str) -> list[dict[str, Any]]:
    """Items of the NHL "Tonight's Officials" RSS feed: title, url, published_at, slate_date.

    Args:
        xml_text: RSS 2.0 document.
    """
    items: list[dict[str, Any]] = []
    for item in ET.fromstring(xml_text).iter("item"):
        title = html.unescape(item.findtext("title") or "")
        pub = item.findtext("pubDate")
        items.append({
            "title": title,
            "url": (item.findtext("link") or "").strip(),
            "published_at": parsedate_to_datetime(pub) if pub else None,
            "slate_date": slate_date(title),
        })
    return items


def post_times(page_html: str) -> dict[str, datetime | None]:
    """``published`` / ``modified`` times from a post's ``article:*_time`` meta tags."""
    found: dict[str, datetime | None] = {"published": None, "modified": None}
    for kind, value in _META_TIME.findall(page_html):
        try:
            found[kind] = datetime.fromisoformat(value).astimezone(ZoneInfo("UTC"))
        except ValueError:
            logger.warning("unparseable %s time %r", kind, value)
    return found


def parse_assignment_post(page_html: str) -> list[dict[str, Any]]:
    """Officials per game from one Scouting the Refs assignment post.

    Args:
        page_html: Full post HTML.

    Returns:
        One dict per (game, official): away_team / home_team (tricodes, None if
        unrecognized), matchup (header text), role, slot, official_name, sweater_number.
        An official appears once per role even though the stat tables repeat names.
    """
    start = page_html.find("<article")
    end = page_html.find("</article>", start)
    body = page_html[start:end] if start >= 0 else page_html
    headers = [
        m for m in _H1.finditer(body)
        if "entry-title" not in m.group(1) and any(" at " in line for line in _text(m.group(2)))
    ]
    rows: list[dict[str, Any]] = []
    for i, header in enumerate(headers):
        section = body[header.end(): headers[i + 1].start() if i + 1 < len(headers) else len(body)]
        matchup = next(line for line in _text(header.group(2)) if " at " in line)
        away_name, home_name = (part.strip() for part in matchup.split(" at ", 1))
        # Role blocks start at their <h3> labels; names are <strong>Name #NN</strong>.
        marks = [(m.start(), " ".join(_text(m.group(1))).upper()) for m in _H3.finditer(section)]
        for j, (pos, label) in enumerate(marks):
            role = "referee" if "REFEREE" in label else "linesman" if "LINES" in label else None
            if role is None:
                continue
            block = section[pos: marks[j + 1][0] if j + 1 < len(marks) else len(section)]
            seen: list[str] = []
            for strong in _STRONG.finditer(block):
                text = " ".join(_text(strong.group(1)))
                match = _NAME_NUMBER.match(text)
                if not match or match.group(1) in seen:
                    continue
                seen.append(match.group(1))
                rows.append({
                    "away_team": resolve_team(away_name),
                    "home_team": resolve_team(home_name),
                    "matchup": matchup,
                    "role": role,
                    "slot": len(seen),
                    "official_name": clean_name(match.group(1)),
                    "sweater_number": int(match.group(2)),
                })
    return rows


def normalize_assignments(
    rows: list[dict[str, Any]],
    games: pl.DataFrame,
    day: date,
    captured_at: datetime,
    post_url: str | None = None,
    times: dict[str, datetime | None] | None = None,
) -> pl.DataFrame:
    """Attach game ids to parsed assignments.

    Args:
        rows: Output of :func:`parse_assignment_post`.
        games: Games table.
        day: Slate date from the post title.
        captured_at: Poll time (UTC).
        post_url: Post URL.
        times: Post published/modified times from :func:`post_times`.

    Returns:
        Frame with :data:`ASSIGNMENT_SCHEMA`; rows whose matchup does not resolve to a
        game on ``day`` are dropped with a warning.
    """
    times = times or {}
    slate = games.filter(pl.col("game_date") == day).select("game_id", "season", "game_date", "home_abbr", "away_abbr")
    lookup = {(r["home_abbr"], r["away_abbr"]): r for r in slate.iter_rows(named=True)}
    out: list[dict[str, Any]] = []
    for row in rows:
        game = lookup.get((row["home_team"], row["away_team"]))
        if game is None:
            logger.warning("no game on %s for %r", day, row["matchup"])
            continue
        out.append({
            **game,
            "role": row["role"],
            "official_id": official_id(row["official_name"]),
            "official_name": row["official_name"],
            "sweater_number": row["sweater_number"],
            "is_assigned": True,
            "post_url": post_url,
            "post_published_at": times.get("published"),
            "post_modified_at": times.get("modified"),
            "captured_at": captured_at,
        })
    return pl.DataFrame(out, schema=ASSIGNMENT_SCHEMA)


def with_removals(incoming: pl.DataFrame, existing: pl.DataFrame | None) -> pl.DataFrame:
    """Add ``is_assigned = False`` rows for officials dropped from a game since the last capture.

    Only games present in ``incoming`` are considered (a post that no longer lists a game
    says nothing about it).

    Args:
        incoming: Current capture (all ``is_assigned``).
        existing: Stored transitions for the season, or None.
    """
    if existing is None or existing.is_empty() or incoming.is_empty():
        return incoming
    latest = (
        existing.filter(pl.col("game_id").is_in(incoming["game_id"].unique().implode()))
        .sort("captured_at")
        .group_by(ASSIGNMENT_KEYS, maintain_order=True)
        .last()
        .filter(pl.col("is_assigned"))
    )
    dropped = latest.join(incoming.select(ASSIGNMENT_KEYS), on=list(ASSIGNMENT_KEYS), how="anti")
    if dropped.is_empty():
        return incoming
    meta = incoming.select("game_id", "post_url", "post_published_at", "post_modified_at", "captured_at").unique("game_id")
    dropped = (
        dropped.drop("post_url", "post_published_at", "post_modified_at", "captured_at")
        .join(meta, on="game_id")
        .with_columns(pl.lit(False).alias("is_assigned"))
        .select(list(ASSIGNMENT_SCHEMA))
    )
    return pl.concat([incoming, dropped.cast(ASSIGNMENT_SCHEMA)])


def poll_assignments(
    store: Store,
    games: pl.DataFrame,
    client: WebClient | None = None,
    today: date | None = None,
    days_back: int = 1,
) -> int:
    """Capture tonight's referee/linesman assignments from Scouting the Refs.

    Reads the NHL "Tonight's Officials" RSS feed, fetches every post whose slate date is
    within ``days_back`` days of ``today`` (Eastern), archives the HTML, and appends
    transitions to ``keys.ref_assignments(season)``.

    Args:
        store: S3 store.
        games: Games table (``processed/games.parquet``).
        client: Optional Scouting the Refs web client.
        today: Override for "today" in US/Eastern (tests, replays).
        days_back: Also re-read posts this many days old (catches late edits).

    Returns:
        Rows written.
    """
    client = client or _str_client()
    captured_at = utcnow()
    today = today or captured_at.astimezone(EASTERN).date()
    try:
        feed = client.get_text(STR_FEED)
    except SourceUnavailable as exc:
        logger.warning("scoutingtherefs feed skipped: %s", exc)
        return 0
    archive_raw(store, "scoutingtherefs", captured_at, "feed", {"url": STR_FEED, "xml": feed})
    posts = [
        p for p in parse_feed(feed)
        if p["slate_date"] and today - timedelta(days=days_back) <= p["slate_date"] <= today + timedelta(days=1)
        and "NHL" in p["title"]
    ]
    written = 0
    for post in posts:
        try:
            page = client.get_text(post["url"])
        except SourceUnavailable as exc:
            logger.warning("scoutingtherefs post skipped: %s", exc)
            continue
        day = post["slate_date"]
        archive_raw(store, "scoutingtherefs", captured_at, f"assignments-{day:%Y-%m-%d}", {"url": post["url"], "html": page})
        frame = normalize_assignments(parse_assignment_post(page), games, day, captured_at, post["url"], post_times(page))
        for (season,), part in frame.partition_by("season", as_dict=True).items():
            key = keys.ref_assignments(int(season))
            part = with_removals(part, store.get_parquet(key))
            written += append_transitions(store, key, part, ASSIGNMENT_KEYS, ASSIGNMENT_VALUES)
    return written
