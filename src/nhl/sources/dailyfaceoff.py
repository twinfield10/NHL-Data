"""DailyFaceoff starting goalies and line combinations, parsed from public HTML pages.

DailyFaceoff is a Next.js site: every page embeds its full data as JSON in
``<script id="__NEXT_DATA__">``. Only those public pages are fetched (politely, <= 0.5
req/s); robots.txt disallows ``/api/``, so the JSON API is never called.

* ``/starting-goalies/{YYYY-MM-DD}`` -> ``props.pageProps.data``: one entry per game with
  ``home*`` / ``away*`` goalie, report strength ("Confirmed", "Likely", or null when
  DailyFaceoff only projects a starter) and the source tweet. Stored as transitions keyed
  by (game_date, team).
* ``/teams/{slug}/line-combinations`` -> ``props.pageProps.combinations``: the current
  lineup with ``updatedAt``. There is no history page, so history starts with our captures:
  every new (team, updated_at) version is stored in full, one row per player slot.

Players are mapped to NHL ids by :class:`PlayerResolver` (also used by the ESPN injury
source).
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from nhl import config
from nhl.ingest.http import WEB_API, NHLClient, SourceUnavailable, WebClient
from nhl.sources.common import append_transitions, archive_raw, utcnow
from nhl.sources.tweets import canonical_tweet_url, fetch_tweets
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

BASE_URL = "https://www.dailyfaceoff.com"
EASTERN = ZoneInfo("America/New_York")
_NEXT_DATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)

GOALIE_KEYS = ("game_date", "team")
GOALIE_VALUES = ("goalie_name", "status", "news_created_at", "news_source_url")

GOALIE_SCHEMA: dict[str, pl.DataType] = {
    "game_date": pl.Date(),
    "start_time": pl.Datetime("us", "UTC"),
    "team": pl.String(),
    "opponent": pl.String(),
    "is_home": pl.Boolean(),
    "goalie_name": pl.String(),
    "df_goalie_id": pl.Int64(),
    "player_id": pl.Int64(),
    "status": pl.String(),
    "status_id": pl.Int64(),
    "news_details": pl.String(),
    "news_source_name": pl.String(),
    "news_source_url": pl.String(),
    "news_created_at": pl.Datetime("us", "UTC"),
    "captured_at": pl.Datetime("us", "UTC"),
}

LINE_SCHEMA: dict[str, pl.DataType] = {
    "team": pl.String(),
    "updated_at": pl.Datetime("us", "UTC"),
    "source_name": pl.String(),
    "source_url": pl.String(),
    "category": pl.String(),
    "group": pl.String(),
    "position": pl.String(),
    "slot": pl.Int32(),
    "player_name": pl.String(),
    "df_player_id": pl.Int64(),
    "jersey": pl.Int32(),
    "player_id": pl.Int64(),
    "injury_status": pl.String(),
    "game_time_decision": pl.Boolean(),
    "news_created_at": pl.Datetime("us", "UTC"),
    "news_details": pl.String(),
    "captured_at": pl.Datetime("us", "UTC"),
}


# --------------------------------------------------------------------------- helpers
def season_for_date(day: date) -> int:
    """8-digit season id a calendar date belongs to (seasons roll over on July 1).

    Args:
        day: Calendar date.
    """
    return config.season_id(day.year if day.month >= 7 else day.year - 1)


def parse_time(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (``Z`` or offset) into an aware UTC datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        logger.warning("unparseable timestamp %r", value)
        return None


def next_data(page_html: str) -> dict[str, Any]:
    """Extract the ``__NEXT_DATA__`` JSON from a Next.js page.

    Raises:
        ValueError: If the page has no ``__NEXT_DATA__`` script (e.g. a Cloudflare block page).
    """
    match = _NEXT_DATA_RE.search(page_html)
    if not match:
        raise ValueError("page has no __NEXT_DATA__ script")
    return json.loads(match.group(1))


def norm_name(name: str | None) -> str:
    """Accent-, case- and punctuation-insensitive name key ("J.J. Moser" -> "jjmoser")."""
    if not name:
        return ""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", ascii_name.lower())


#: Namesakes a book tells apart by a middle name or initial, by :func:`norm_name` key. The NHL
#: lists both Vancouver players as "Elias Pettersson": the forward (#40, born Fredrik Elias)
#: and the defenseman (#25, Elias Nils). FanDuel adds the sweater number instead
#: ("Elias Pettersson #25"), which :func:`split_jersey` reads. Every book that lists the
#: defenseman marks him, so the plain name is the forward: checked by price 2026-10-10 (goals
#: over 0.5: plain +280/+286 at DraftKings/4Casters, like FanDuel's #40 at +350; the
#: defenseman +1800/+2400). A new same-name pair has no entry here and stays unresolved.
NAMESAKES = {
    "eliaspettersson": 8480012, "fredrikeliaspettersson": 8480012, "fredrickeliaspettersson": 8480012,
    "eliasnilspettersson": 8483678, "eliasnpettersson": 8483678,
}
_JERSEY_SUFFIX = re.compile(r"\s*#\s*(\d{1,2})\s*$")


def split_jersey(name: str | None) -> tuple[str | None, int | None]:
    """``("Elias Pettersson", 25)`` from ``"Elias Pettersson #25"``; names without a trailing
    ``#NN`` come back unchanged with no jersey."""
    if not name or not (m := _JERSEY_SUFFIX.search(name)):
        return name, None
    return name[:m.start()], int(m.group(1))


def name_variants(name: str | None) -> list[str]:
    """Other keys a book may have meant: surname first ("Yurov Danila") and German
    transliterations ("Stuetzle" for "Stützle"). Used only after the exact keys fail."""
    if not name:
        return []
    parts = name.split()
    out = [norm_name(" ".join(parts[1:] + parts[:1]))] if len(parts) == 2 else []
    key = norm_name(name)
    for a, b in (("ue", "u"), ("oe", "o"), ("ae", "a")):
        if a in key:
            out.append(key.replace(a, b))
    return [v for v in dict.fromkeys(out) if v and v != key]


# --------------------------------------------------------------------------- player ids
class PlayerResolver:
    """Map third-party (team, name, jersey) to NHL ``player_id``.

    Order: a known namesake spelling (:data:`NAMESAKES`); (team, jersey) on the NHL current
    roster confirmed by a name check (a trailing ``#NN`` in the name counts as the jersey);
    then full-name match within the team roster; then unique last name + first initial within
    the roster; then a full-name match across the league in ``processed/players.parquet``
    (most recent player wins when names collide). Two players with the same name on one
    roster, or both active in the latest season, are never guessed between: without a
    jersey the name stays unresolved. Rosters are fetched once per instance.

    Args:
        players: Players table (player_id, player_name, last_season); optional.
        client: NHL API client used to fetch rosters.
        roster_loader: Override ``tricode -> roster JSON`` (tests).
    """

    def __init__(
        self,
        players: pl.DataFrame | None = None,
        client: NHLClient | None = None,
        roster_loader: Callable[[str], dict[str, Any]] | None = None,
    ) -> None:
        self._client = client
        self._loader = roster_loader
        self._rosters: dict[str, list[dict[str, Any]]] = {}
        self._league: dict[str, int] = self._league_index(players)
        self._league_namesakes: set[str] = self._active_namesakes(players)
        self.stats: Counter[str] = Counter()
        self.unresolved: set[tuple[str | None, str]] = set()

    @classmethod
    def from_store(cls, store: Store, client: NHLClient | None = None) -> PlayerResolver:
        """Build a resolver backed by the stored players table and live NHL rosters."""
        return cls(players=store.get_parquet(keys.PLAYERS), client=client or NHLClient(rps=4))

    @staticmethod
    def _league_index(players: pl.DataFrame | None) -> dict[str, int]:
        """Normalized full name -> player_id, keeping the most recent player per name."""
        if players is None or players.is_empty():
            return {}
        order = "last_season" if "last_season" in players.columns else "player_id"
        index: dict[str, int] = {}
        for pid, name in players.sort(order).select("player_id", "player_name").iter_rows():
            if name:
                index[norm_name(name)] = int(pid)  # later (more recent) rows overwrite
        return index

    @staticmethod
    def _active_namesakes(players: pl.DataFrame | None) -> set[str]:
        """Name keys shared by two or more players whose last season is the latest for that name
        (both still active), where "most recent wins" would be a coin flip."""
        if players is None or players.is_empty() or "last_season" not in players.columns:
            return set()
        keyed = players.filter(pl.col("player_name").is_not_null()).with_columns(
            pl.col("player_name").map_elements(norm_name, return_dtype=pl.Utf8).alias("_key"))
        latest = keyed.filter(pl.col("last_season") == pl.col("last_season").max().over("_key"))
        return set(latest.group_by("_key").agg(pl.len()).filter(pl.col("len") > 1)["_key"].to_list())

    def roster(self, team: str | None) -> list[dict[str, Any]]:
        """Current NHL roster for a tricode as ``[{player_id, first, last, number}]`` (cached)."""
        if not team:
            return []
        if team not in self._rosters:
            try:
                if self._loader is not None:
                    raw = self._loader(team)
                else:
                    self._client = self._client or NHLClient(rps=4)
                    raw = self._client.get_json(f"{WEB_API}/roster/{team}/current")
            except Exception as exc:  # noqa: BLE001 - a missing roster only weakens matching
                logger.warning("roster %s unavailable: %r", team, exc)
                raw = {}
            self._rosters[team] = [
                {
                    "player_id": int(p["id"]),
                    "first": norm_name((p.get("firstName") or {}).get("default")),
                    "last": norm_name((p.get("lastName") or {}).get("default")),
                    "number": p.get("sweaterNumber"),
                }
                for group in ("forwards", "defensemen", "goalies")
                for p in raw.get(group) or []
            ]
        return self._rosters[team]

    def resolve(self, team: str | None, name: str | None, jersey: int | None = None) -> int | None:
        """Return the NHL player_id, or None (recorded in :attr:`unresolved`).

        Args:
            team: NHL tricode the source lists the player under.
            name: Player name as published.
            jersey: Sweater number as published, if any.
        """
        name, suffix = split_jersey(name)
        jersey = jersey if jersey is not None else suffix
        key = norm_name(name)
        if not key:
            return None
        roster = self.roster(team)
        if key in NAMESAKES and jersey is None:
            self.stats["resolved"] += 1
            return NAMESAKES[key]
        pid = None
        if jersey is not None:
            # Confirm the jersey hit by last name + first initial: tolerates "Zack"/"Zachary"
            # but rejects a brother wearing the published number (Nick vs Marcus Foligno).
            for p in roster:
                if p["number"] == jersey and p["last"] and key.endswith(p["last"]) and p["first"][:1] == key[:1]:
                    pid = p["player_id"]
                    break
        exact = [p["player_id"] for p in roster if p["first"] + p["last"] == key]
        if pid is None and len(exact) > 1:  # namesakes on one roster and no jersey to tell them apart
            self.stats["unresolved"] += 1
            self.unresolved.add((team, name or ""))
            return None
        if pid is None and exact:
            pid = exact[0]
        if pid is None:
            same_last = [p for p in roster if p["last"] and key.endswith(p["last"]) and p["first"][:1] == key[:1]]
            if len(same_last) == 1:
                pid = same_last[0]["player_id"]
        if pid is None and key not in self._league_namesakes:
            pid = self._league.get(key)
        for alt in name_variants(name) if pid is None else []:
            pid = next((p["player_id"] for p in roster if p["first"] + p["last"] == alt), None) or self._league.get(alt)
            if pid is not None:
                break
        self.stats["resolved" if pid is not None else "unresolved"] += 1
        if pid is None:
            self.unresolved.add((team, name or ""))
        return pid

    def log_unresolved(self, source: str) -> None:
        """Log the resolution rate and every unresolved name for one source."""
        total = self.stats["resolved"] + self.stats["unresolved"]
        logger.info("%s: resolved %d/%d player ids", source, self.stats["resolved"], total)
        if self.unresolved:
            names = ", ".join(f"{t or '?'}:{n}" for t, n in sorted(self.unresolved, key=str))
            logger.warning("%s: unresolved players: %s", source, names)


# --------------------------------------------------------------------------- fetch
def _client() -> WebClient:
    """Polite HTML client for DailyFaceoff (<= 0.5 req/s)."""
    return WebClient("dailyfaceoff", rps=0.5, headers={"Accept": "text/html,application/xhtml+xml"})


def fetch_page_props(client: WebClient, path: str) -> dict[str, Any]:
    """GET a public DailyFaceoff page and return its ``props.pageProps``.

    Args:
        client: DailyFaceoff web client.
        path: Page path, e.g. ``/starting-goalies/2026-10-06``. Never under ``/api/``.
    """
    if path.startswith("/api"):
        raise ValueError("robots.txt disallows /api/ on dailyfaceoff.com")
    return next_data(client.get_text(BASE_URL + path))["props"]["pageProps"]


# --------------------------------------------------------------------------- goalies
def normalize_goalies(
    page_props: dict[str, Any], captured_at: datetime, resolver: PlayerResolver | None = None
) -> pl.DataFrame:
    """One row per game side from a starting-goalies page.

    Args:
        page_props: ``props.pageProps`` of ``/starting-goalies[/date]``.
        captured_at: Poll time (UTC).
        resolver: Optional player-id resolver; ``player_id`` is null without one.

    Returns:
        Frame with :data:`GOALIE_SCHEMA` columns. ``status`` is null when DailyFaceoff has
        no report yet (its projected starter only).
    """
    rows: list[dict[str, Any]] = []
    for game in page_props.get("data") or []:
        sides = {s: resolve_team(game.get(f"{s}TeamName")) or resolve_team(game.get(f"{s}TeamSlug")) for s in ("home", "away")}
        for side, other in (("home", "away"), ("away", "home")):
            team = sides[side]
            name = game.get(f"{side}GoalieName")
            if team is None:
                logger.warning("dailyfaceoff: unknown team %r", game.get(f"{side}TeamName"))
            rows.append({
                "game_date": date.fromisoformat(game["date"]),
                "start_time": parse_time(game.get("dateGmt")),
                "team": team,
                "opponent": sides[other],
                "is_home": side == "home",
                "goalie_name": name,
                "df_goalie_id": game.get(f"{side}GoalieId"),
                "player_id": resolver.resolve(team, name) if resolver and name else None,
                "status": game.get(f"{side}NewsStrengthName"),
                "status_id": game.get(f"{side}NewsStrengthId"),
                "news_details": (game.get(f"{side}NewsDetails") or "").strip() or None,
                "news_source_name": game.get(f"{side}NewsSourceName"),
                "news_source_url": canonical_tweet_url(game.get(f"{side}NewsSourceUrl")),
                "news_created_at": parse_time(game.get(f"{side}NewsCreatedAt")),
                "captured_at": captured_at,
            })
    return pl.DataFrame(rows, schema=GOALIE_SCHEMA)


def attach_game_ids(goalies: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """Add ``game_id`` and ``season`` by matching date + home/away teams to the games table.

    Unmatched rows keep a null ``game_id`` and get the season implied by their date.
    """
    lookup = games.select("game_id", "season", "game_date", "home_abbr", "away_abbr")
    keyed = goalies.with_columns(
        home_abbr=pl.when(pl.col("is_home")).then(pl.col("team")).otherwise(pl.col("opponent")),
        away_abbr=pl.when(pl.col("is_home")).then(pl.col("opponent")).otherwise(pl.col("team")),
    )
    out = keyed.join(lookup, on=["game_date", "home_abbr", "away_abbr"], how="left").drop("home_abbr", "away_abbr")
    fallback = pl.col("game_date").map_elements(season_for_date, return_dtype=pl.Int32)
    out = out.with_columns(pl.col("season").cast(pl.Int32).fill_null(fallback))
    missing = out.filter(pl.col("game_id").is_null())
    if not missing.is_empty():
        logger.warning("dailyfaceoff: %d goalie rows without a game_id", missing.height)
    return out


def poll_goalies(
    store: Store,
    games: pl.DataFrame,
    days: int = 2,
    client: WebClient | None = None,
    resolver: PlayerResolver | None = None,
) -> int:
    """Capture today's and tomorrow's starting-goalie reports (dated pages, Eastern dates).

    Archives each page's raw ``pageProps``, attaches ``game_id``, appends transitions keyed
    (game_date, team) into :func:`keys.dailyfaceoff_goalies`, then fetches source tweets.

    Args:
        store: S3 store.
        games: Games table (game_id, season, game_date, home_abbr, away_abbr).
        days: Number of slates starting today.
        client: Optional DailyFaceoff client.
        resolver: Optional shared resolver (rosters cached across calls).

    Returns:
        Number of goalie rows written.
    """
    client = client or _client()
    resolver = resolver or PlayerResolver.from_store(store)
    captured_at = utcnow()
    today = captured_at.astimezone(EASTERN).date()
    frames: list[pl.DataFrame] = []
    for offset in range(days):
        slate = (today + timedelta(days=offset)).isoformat()
        try:
            props = fetch_page_props(client, f"/starting-goalies/{slate}")
        except (SourceUnavailable, ValueError) as exc:
            logger.warning("dailyfaceoff goalies %s skipped: %r", slate, exc)
            continue
        archive_raw(store, "dailyfaceoff", captured_at, f"starting-goalies-{slate}", props)
        frames.append(normalize_goalies(props, captured_at, resolver))
    resolver.log_unresolved("dailyfaceoff goalies")
    if not frames:
        return 0
    goalies = attach_game_ids(pl.concat(frames), games)
    written = 0
    for (season,), part in goalies.partition_by("season", as_dict=True).items():
        written += append_transitions(store, keys.dailyfaceoff_goalies(int(season)), part, GOALIE_KEYS, GOALIE_VALUES)
    _fetch_sources(store, goalies.get_column("news_source_url").to_list())
    return written


# --------------------------------------------------------------------------- lines
def normalize_lines(
    combinations: dict[str, Any], captured_at: datetime, resolver: PlayerResolver | None = None
) -> pl.DataFrame:
    """One row per player slot of one published line-combination version.

    ``slot`` is the 1-based order within (category, group) as published, e.g. LW=1, C=2,
    RW=3 on a forward line, sk1..sk5 on a power-play unit. Structural-validation columns
    from :func:`validate_lines` are attached (flags only; the published data is untouched).

    Args:
        combinations: ``props.pageProps.combinations`` of a team's line-combinations page.
        captured_at: Poll time (UTC).
        resolver: Optional player-id resolver; ``player_id`` is null without one.
    """
    team = resolve_team(combinations.get("teamAbbreviation")) or resolve_team(combinations.get("teamSlug"))
    updated_at = parse_time(combinations.get("updatedAt"))
    source_url = canonical_tweet_url(combinations.get("source"))
    slots: Counter[tuple[str, str]] = Counter()
    rows: list[dict[str, Any]] = []
    for p in combinations.get("players") or []:
        category, group = p.get("categoryIdentifier"), p.get("groupIdentifier")
        slots[(category, group)] += 1
        news = p.get("latestNews") or {}
        jersey = p.get("jerseyNumber")
        rows.append({
            "team": team,
            "updated_at": updated_at,
            "source_name": combinations.get("sourceName"),
            "source_url": source_url,
            "category": category,
            "group": group,
            "position": p.get("positionIdentifier"),
            "slot": slots[(category, group)],
            "player_name": p.get("name"),
            "df_player_id": p.get("playerId"),
            "jersey": jersey,
            "player_id": resolver.resolve(team, p.get("name"), jersey) if resolver else None,
            "injury_status": p.get("injuryStatus"),
            "game_time_decision": bool(p.get("gameTimeDecision")),
            "news_created_at": parse_time(news.get("createdAt")),
            "news_details": (news.get("details") or "").strip() or None,
            "captured_at": captured_at,
        })
    return validate_lines(pl.DataFrame(rows, schema=LINE_SCHEMA))


#: Expected players per group. pp units of 4 are legal (4 forwards + 1 D variants aside)
#: but are flagged; only EV_GROUPS count toward ``is_valid``.
GROUP_EXPECTED: dict[str, int] = {
    **{f"f{i}": 3 for i in range(1, 5)},
    **{f"d{i}": 2 for i in range(1, 4)},
    "pp1": 5, "pp2": 5, "pk1": 4, "pk2": 4, "g": 2,
}
EV_GROUPS: tuple[str, ...] = ("f1", "f2", "f3", "f4", "d1", "d2", "d3", "g")
FORWARD_GROUPS: tuple[str, ...] = ("f1", "f2", "f3", "f4")
#: ``d4`` is DailyFaceoff's seventh defenseman: with two forwards on f4 it is an 11F/7D lineup.
DEFENSE_GROUPS: tuple[str, ...] = ("d1", "d2", "d3", "d4")
INJURY_GROUPS: tuple[str, ...] = ("ir",)
VALIDATION_COLUMNS: tuple[str, ...] = (
    "group_size", "group_expected", "group_complete", "conflict", "questionable", "lineup_shape", "issues", "is_valid",
)


def _is_active(category: str | None, group: str | None) -> bool:
    """Active (dressed) slots: even-strength forward lines, D pairs (incl. extras) and goalies."""
    return category == "ev" and bool(group) and (group[0] in "fd" or group == "g")


def _validate_version(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Structural checks for one published (team, updated_at) version. Flags, never fixes.

    * **Shape:** f1-f4 of 3 and d1-d3 of 2; f4 with 2 is complete when ``d4`` holds a seventh
      defenseman (11F/7D, as teams dress it). The shape counts f1-f4 and d1-d4.
    * **Conflict** (invalidates): a player in two even-strength groups.
    * **Questionable** (doesn't invalidate): a player in an active slot who is also on the
      injury list or carries an injury status. DailyFaceoff's injury list includes day-to-day
      players who are still expected to play, so this is "questionable", not a contradiction;
      :mod:`nhl.pregame.lineups` settles it with ESPN.

    Args:
        rows: The version's line rows (dicts with category, group, player fields).

    Returns:
        The rows with :data:`VALIDATION_COLUMNS` filled in.
    """
    sizes: Counter[str] = Counter(r["group"] for r in rows if r["category"] != "oi")
    expected = dict(GROUP_EXPECTED)
    if sizes["d4"] == 1 and sizes["f4"] == 2:
        expected["f4"] = 2
    issues: list[str] = []
    for group, n in expected.items():
        if sizes[group] != n:
            issues.append(f"{group} missing" if sizes[group] == 0 else f"{group} has {sizes[group]}/{n}")
    incomplete = any(sizes[g] != expected[g] for g in EV_GROUPS)

    def ident(r: dict[str, Any]) -> Any:
        return r["df_player_id"] if r["df_player_id"] is not None else norm_name(r["player_name"])

    active: dict[Any, set[str]] = {}
    injured: set[Any] = set()
    for r in rows:
        if _is_active(r["category"], r["group"]):
            active.setdefault(ident(r), set()).add(r["group"])
        if r["group"] in INJURY_GROUPS or r["category"] == "oi":
            injured.add(ident(r))
    names = {ident(r): r["player_name"] for r in rows}
    doubled = {pid for pid, groups in active.items() if len(groups) > 1}
    tagged = {ident(r) for r in rows if r["injury_status"] and _is_active(r["category"], r["group"])}
    questionable = {pid for pid in active if pid in injured or pid in tagged}
    for pid in doubled:
        issues.append(f"{(names[pid] or '?').split(' ')[-1]} in {'+'.join(sorted(active[pid]))}")
    for pid in questionable:
        issues.append(f"{(names[pid] or '?').split(' ')[-1]} active+injury list")

    forwards = sum(sizes[g] for g in FORWARD_GROUPS)
    defense = sum(sizes[g] for g in DEFENSE_GROUPS)
    shape = {(12, 6): "12F6D", (11, 7): "11F7D"}.get((forwards, defense), "irregular")
    if shape == "irregular":
        issues.append(f"dressed {forwards}F/{defense}D")
    summary = "; ".join(issues) or None
    is_valid = not incomplete and not doubled and shape != "irregular"
    out = []
    for r in rows:
        exp = expected.get(r["group"]) if r["category"] != "oi" else None
        size = sizes[r["group"]] if r["category"] != "oi" else None
        out.append({
            **r,
            "group_size": size,
            "group_expected": exp,
            "group_complete": (size == exp) if exp is not None else None,
            "conflict": ident(r) in doubled,
            "questionable": ident(r) in questionable,
            "lineup_shape": shape,
            "issues": summary,
            "is_valid": is_valid,
        })
    return out


def validate_lines(lines: pl.DataFrame) -> pl.DataFrame:
    """Add structural-validation columns to line rows, per (team, updated_at) version.

    Rules (flag, never impute) are in :func:`_validate_version`: group sizes vs
    :data:`GROUP_EXPECTED` (f4 of 2 plus a ``d4`` is 11F/7D); dressed shape ("12F6D",
    "11F7D" or "irregular"); a player in two EV groups is a ``conflict``; an active player
    also on the injury list is ``questionable``. ``is_valid`` = every EV group complete, a
    regular shape and no conflicts. ``issues`` summarizes everything flagged, e.g.
    ``"Lilleberg active+injury list"``.

    Idempotent: existing validation columns are recomputed.

    Args:
        lines: Rows shaped like :func:`normalize_lines` output (any number of versions).
    """
    base = lines.drop([c for c in VALIDATION_COLUMNS if c in lines.columns])
    if base.is_empty():
        return base.with_columns(**{c: pl.lit(None, dtype=t) for c, t in VALIDATION_SCHEMA.items()})
    out: list[dict[str, Any]] = []
    for part in base.partition_by(["team", "updated_at"], maintain_order=True):
        out += _validate_version(part.to_dicts())
    return pl.DataFrame(out, schema={**base.schema, **VALIDATION_SCHEMA})


VALIDATION_SCHEMA: dict[str, pl.DataType] = {
    "group_size": pl.Int32(),
    "group_expected": pl.Int32(),
    "group_complete": pl.Boolean(),
    "conflict": pl.Boolean(),
    "questionable": pl.Boolean(),
    "lineup_shape": pl.String(),
    "issues": pl.String(),
    "is_valid": pl.Boolean(),
}


def _stored_versions(existing: pl.DataFrame | None) -> set[tuple[str, datetime]]:
    """(team, updated_at) pairs already in the lines table."""
    if existing is None or existing.is_empty():
        return set()
    return set(existing.select("team", "updated_at").unique().iter_rows())


def poll_lines(
    store: Store,
    client: WebClient | None = None,
    resolver: PlayerResolver | None = None,
    slugs: list[str] | None = None,
) -> int:
    """Capture every team's line combinations; store only versions not seen before.

    Fetches all 32 team pages at <= 0.5 req/s (the team list comes from the first page's
    ``sortedTeams``). A team's raw ``pageProps`` is archived only when its
    ``(team, updated_at)`` version is new. Stops early if DailyFaceoff refuses a request.

    Args:
        store: S3 store.
        client: Optional DailyFaceoff client.
        resolver: Optional shared resolver.
        slugs: Restrict to these team slugs (default: all teams).

    Returns:
        Number of player rows written.
    """
    client = client or _client()
    resolver = resolver or PlayerResolver.from_store(store)
    captured_at = utcnow()
    season = season_for_date(captured_at.astimezone(EASTERN).date())
    key = keys.dailyfaceoff_lines(season)
    existing = store.get_parquet(key)
    seen = _stored_versions(existing)

    queue = list(slugs) if slugs else ["anaheim-ducks"]
    discovered = slugs is not None
    fresh: list[pl.DataFrame] = []
    i = 0
    while i < len(queue):
        slug = queue[i]
        i += 1
        try:
            props = fetch_page_props(client, f"/teams/{slug}/line-combinations")
        except SourceUnavailable as exc:
            logger.warning("dailyfaceoff refused %s; stopping this poll: %r", slug, exc)
            break
        except Exception as exc:  # noqa: BLE001 - one bad page should not lose the rest
            logger.warning("dailyfaceoff lines %s failed: %r", slug, exc)
            continue
        if not discovered:
            queue += [t["slug"] for t in props.get("sortedTeams") or [] if t.get("slug") and t["slug"] != slug]
            discovered = True
        combos = props.get("combinations") or {}
        team = resolve_team(combos.get("teamAbbreviation")) or resolve_team(slug)
        version = (team, parse_time(combos.get("updatedAt")))
        if version in seen or not combos.get("players"):
            continue
        seen.add(version)
        archive_raw(store, "dailyfaceoff", captured_at, f"lines-{slug}", props)
        fresh.append(normalize_lines(combos, captured_at, resolver))
    resolver.log_unresolved("dailyfaceoff lines")
    if not fresh:
        logger.info("no new line versions")
        return 0
    new_rows = pl.concat(fresh)
    table = new_rows if existing is None else pl.concat([existing, new_rows], how="diagonal_relaxed")
    store.put_parquet(key, table)
    logger.info("%d new line version(s), %d rows -> %s", len(fresh), new_rows.height, key)
    _fetch_sources(store, new_rows.get_column("source_url").unique().to_list())
    return new_rows.height


def _fetch_sources(store: Store, urls: list[str | None]) -> None:
    """Hand source tweet URLs to :func:`nhl.sources.tweets.fetch_tweets`; never fails the poll."""
    try:
        fetch_tweets(store, urls)
    except Exception as exc:  # noqa: BLE001 - tweets are enrichment; the poll already succeeded
        logger.warning("source tweet fetch failed: %r", exc)
