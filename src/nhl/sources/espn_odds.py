"""ESPN odds: live polls of every provider ESPN lists, and a historical open/close backfill.

Two public endpoints, no auth:

* Scoreboard per date (``site.api.espn.com/.../scoreboard?dates=YYYYMMDD``) gives the event
  ids, UTC start times and home/away team codes (ESPN codes: TB, NJ, LA, SJ, UTAH ...).
* Per-event odds (``sports.core.api.espn.com/.../events/{id}/competitions/{id}/odds``) give
  one item per provider (book), paginated (``pageCount``; 25 per page by default).

Payload shape by era (probed 2015-2026):

* Before 2019-20: ``items`` is empty.
* 2019-20 to 2022-23: top-level numbers per item (``overUnder``, ``overOdds``,
  ``underOdds``, ``spread`` from the home side, ``homeTeamOdds``/``awayTeamOdds`` with
  ``moneyLine`` and ``spreadOdds``) plus a nested ``current`` block that repeats them as
  strings. No timestamps and no open/close, so these become ``price_point="last"``.
* 2023-24 on: nested ``open`` / ``close`` / ``current`` blocks on each team
  (``moneyLine``, ``spread`` = puckline price, ``pointSpread`` = puckline) and on the item
  (``over``, ``under``, ``total``), values as ``american`` strings. Games not yet closed
  (and in-game providers) have ``open`` + ``current`` only.

Per-provider ``propBets`` (live only; DraftKings is the only provider that links them on
NHL) carry DraftKings' period markets and player props. Each item is one price on one
outcome, identified only by ``athlete``/``team`` refs, ``type`` and ``odds.total``; there
is no side label. Verified against the ladder markets (the "3+ shots" price equals the
first price of "Total Shots on Goal 2.5"), the **first** item of an (athlete/team, type,
line) group is the over/yes and the second the under/no. Athlete names come from the two
ESPN team rosters (``site.api.espn.com/.../teams/{id}/roster``). Period moneylines are
three-way (two teams plus a team-less draw item) and stored as ``moneyline_3way``; "Team
Total Goals (Excl OT)" is a regulation team total ladder (main rung = the most balanced).

**Three-way regulation moneylines.** Several Kambi/European feeds (Bet365, Unibet,
PointsBet, Titanbets, SugarHouse, often DraftKings) report the 60-minute three-way
moneyline as if it were two-way: the two team prices with the draw missing. The rule
(:func:`classify_three_way`): a complete home/away moneyline pair from one book, event and
price point whose implied probabilities (vig included) sum into :data:`THREE_WAY_RANGE`
is relabelled ``market="moneyline_3way"``, ``period="reg"``, and kept out of the two-way
sanity filter. Measured on the 2023-24 cache (29,921 complete home/away pairs): 15,854
sum into (1.00, 1.15] (two-way), 14,056 into (0.75, 0.95] (three-way), and only 11 fall
in between (stale quotes, still dropped). Half of the three-way pairs have a minus-money
favourite (COL -175 / SJS +390), so "both prices positive" would miss them; the sum alone
separates the two cleanly. No ESPN provider has ever reported the draw price.

Known data quirks the sanity filter exists for:

* Some single-value rows are in-game or stale quotes (a 5.5 total at -240 when every
  other book hangs 7.0). Totals >= 1.5 goals from the event's median are dropped.
* Providers whose name contains "Live" (e.g. "ESPN Bet - Live Odds") are in-play feeds
  and are never read.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

import polars as pl

from nhl import config
from nhl.ingest.http import SourceUnavailable, WebClient
from nhl.odds.core import (
    MARKET_KEY,
    TRANSITION_VALUES,
    attach_game_ids,
    implied_probability,
    market_uid_expr,
    odds_frame,
)
from nhl.odds.props import milestone_line, props_frame, store_props
from nhl.odds.store import store_odds
from nhl.sources.dailyfaceoff import PlayerResolver
from nhl.sources.common import append_transitions, archive_raw, utcnow
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

SOURCE = "espn_odds"
SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard"
ROSTER_URL = "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/roster"
ODDS_URL = "https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl/events/{id}/competitions/{id}/odds"
RPS = 3.0
#: ESPN groups the scoreboard by US Eastern date.
EASTERN = ZoneInfo("America/New_York")
#: ESPN ``season.type``: 2 regular season, 3 postseason.
SEASON_TYPES = (2, 3)
#: Events that were never played as scheduled. A postponed game gets a *new* ESPN event
#: when rescheduled (BUF-CHI 2024-01-17 -> 401632079), so the old one must be skipped.
DEAD_STATUSES = ("STATUS_POSTPONED", "STATUS_CANCELED", "STATUS_CANCELLED")

#: Two-way implied probabilities (vig included) must sum into this range.
VIG_RANGE = (0.98, 1.15)
#: A home/away moneyline pair summing into this range is a three-way line missing its draw.
THREE_WAY_RANGE = (0.70, 0.95)
#: A book's total this far (goals) from the event median is treated as in-game/stale.
TOTAL_TOLERANCE = 1.5

Mode = Literal["live", "history"]


def history_prefix(season: int) -> str:
    """Prefix holding cached raw history payloads for an 8-digit season id."""
    return f"raw/external/espn_odds_history/{season}/"


def history_event_key(season: int, event_id: str) -> str:
    """Cache key for one event's raw odds payload."""
    return f"{history_prefix(season)}{event_id}.json.gz"


def history_scoreboard_key(season: int, day: date) -> str:
    """Cache key for one date's raw scoreboard."""
    return f"{history_prefix(season)}scoreboards/{day:%Y%m%d}.json.gz"


# --------------------------------------------------------------------------- parsing
def _num(value: Any) -> float | None:
    """Parse an ESPN odds value (number, ``"+1.5"``, ``"-185"``, ``"EVEN"``) to float."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip().upper()
    if text in ("EVEN", "EV", "PK", "PICK"):
        return 100.0 if text.startswith("EV") else 0.0
    try:
        return float(text.replace("+", ""))
    except ValueError:
        return None


def _american(block: Any, *path: str) -> float | None:
    """Read ``block[path...]["american"]`` from a nested odds block, or None."""
    for part in path:
        if not isinstance(block, dict):
            return None
        block = block.get(part)
    if not isinstance(block, dict):
        return None
    return _num(block.get("american", block.get("alternateDisplayValue")))


def _ml_price(price: float | None) -> float | None:
    """Reject impossible American prices (|price| < 100, e.g. a 0 placeholder)."""
    return price if price is not None and abs(price) >= 100 else None


def _quotes_at(item: dict[str, Any], point: str) -> dict[str, float | None]:
    """All seven values of one provider item at a nested price point (open/close/current)."""
    home, away = item.get("homeTeamOdds") or {}, item.get("awayTeamOdds") or {}
    return {
        "ml_home": _american(home, point, "moneyLine"),
        "ml_away": _american(away, point, "moneyLine"),
        "pl_home_line": _american(home, point, "pointSpread"),
        "pl_away_line": _american(away, point, "pointSpread"),
        "pl_home": _american(home, point, "spread"),
        "pl_away": _american(away, point, "spread"),
        "total": _american(item, point, "total"),
        "over": _american(item, point, "over"),
        "under": _american(item, point, "under"),
    }


def _quotes_top(item: dict[str, Any]) -> dict[str, float | None]:
    """The current values from the item's top-level numbers (present in every era)."""
    home, away = item.get("homeTeamOdds") or {}, item.get("awayTeamOdds") or {}
    spread = _num(item.get("spread"))
    return {
        "ml_home": _num(home.get("moneyLine")),
        "ml_away": _num(away.get("moneyLine")),
        "pl_home_line": spread,
        "pl_away_line": -spread if spread is not None else None,
        "pl_home": _num(home.get("spreadOdds")),
        "pl_away": _num(away.get("spreadOdds")),
        "total": _num(item.get("overUnder")),
        "over": _num(item.get("overOdds")),
        "under": _num(item.get("underOdds")),
    }


def _merge(primary: dict[str, float | None], fallback: dict[str, float | None]) -> dict[str, float | None]:
    """Fill a quote dict's gaps from another quote dict."""
    return {k: primary[k] if primary[k] is not None else fallback.get(k) for k in primary}


def _has_any(quotes: dict[str, float | None]) -> bool:
    """True if a price point carries at least one price."""
    return any(quotes[k] is not None for k in ("ml_home", "ml_away", "pl_home", "pl_away", "over", "under"))


def _quote_rows(quotes: dict[str, float | None]) -> list[dict[str, Any]]:
    """Expand one price point's values into (market, side, line, price) rows."""
    out = [
        ("moneyline", "home", None, _ml_price(quotes["ml_home"])),
        ("moneyline", "away", None, _ml_price(quotes["ml_away"])),
        ("puckline", "home", quotes["pl_home_line"], _ml_price(quotes["pl_home"])),
        ("puckline", "away", quotes["pl_away_line"], _ml_price(quotes["pl_away"])),
        ("total", "over", quotes["total"], _ml_price(quotes["over"])),
        ("total", "under", quotes["total"], _ml_price(quotes["under"])),
    ]
    return [
        {"market": m, "side": s, "line": line, "price": p}
        for m, s, line, p in out
        if p is not None and (m == "moneyline" or line is not None)
    ]


def _price_points(item: dict[str, Any], mode: Mode) -> list[tuple[str, dict[str, float | None]]]:
    """The (price_point, quotes) pairs to emit for one provider item.

    Live: current values (top-level numbers, gaps filled from the nested ``current``).
    History: nested ``open`` when present; nested ``close`` when present, otherwise the
    current/top-level values as ``last``.
    """
    current = _merge(_quotes_top(item), _quotes_at(item, "current"))
    if mode == "live":
        return [("live", current)]
    points: list[tuple[str, dict[str, float | None]]] = []
    opened = _quotes_at(item, "open")
    if _has_any(opened):
        points.append(("open", opened))
    closed = _quotes_at(item, "close")
    if _has_any(closed):
        points.append(("close", closed))
        # A market missing from the close block (e.g. totals) falls back to "last".
        missing = {k: (current[k] if closed[k] is None else None) for k in current}
        if _has_any(missing):
            points.append(("last", missing))
    else:
        points.append(("last", current))
    return points


def is_live_provider(name: str | None) -> bool:
    """True for ESPN's in-game provider feeds (name contains "Live")."""
    return "live" in (name or "").lower()


def _parse_time(text: str | None) -> datetime | None:
    """Parse ESPN's ``2023-11-01T23:00Z`` start stamps to an aware UTC datetime."""
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def event_meta(event: dict[str, Any]) -> dict[str, Any]:
    """Extract id, UTC start and home/away codes from a scoreboard event.

    Returns:
        ``{"event_id", "start_time", "home_team", "away_team", "home_team_id",
        "away_team_id", "season_type", "state", "status"}`` (team ids are ESPN's).
    """
    comp = (event.get("competitions") or [{}])[0]
    teams = {c.get("homeAway"): (c.get("team") or {}) for c in comp.get("competitors", [])}
    status = (comp.get("status") or event.get("status") or {}).get("type") or {}
    return {
        "event_id": str(event["id"]),
        "start_time": _parse_time(event.get("date")),
        "home_team": teams.get("home", {}).get("abbreviation"),
        "away_team": teams.get("away", {}).get("abbreviation"),
        "home_team_id": str(teams.get("home", {}).get("id") or "") or None,
        "away_team_id": str(teams.get("away", {}).get("id") or "") or None,
        "season_type": (event.get("season") or {}).get("type"),
        "state": status.get("state"),
        "status": status.get("name"),
    }


def event_rows(
    payload: dict[str, Any], meta: dict[str, Any], captured_at: datetime, mode: Mode
) -> list[dict[str, Any]]:
    """Raw odds rows (before typing and the sanity filter) for one event's odds payload."""
    rows: list[dict[str, Any]] = []
    for item in payload.get("items") or []:
        book = (item.get("provider") or {}).get("name")
        if not book or is_live_provider(book):
            continue
        for point, quotes in _price_points(item, mode):
            for row in _quote_rows(quotes):
                rows.append({
                    "book": book,
                    "captured_at": captured_at,
                    "start_time": meta["start_time"],
                    "away_team": meta["away_team"],
                    "home_team": meta["home_team"],
                    "price_point": point,
                    "source_event_id": str(meta["event_id"]),
                    **row,
                })
    return rows


# ------------------------------------------------------------------------ prop bets
#: Two-way player markets: ESPN type name -> prop_type (first item over, second under).
OVER_UNDER_PROPS: dict[str, str] = {
    "Total Points": "points", "Total Assists": "assists", "Total Goals": "goals",
    "Total Shots on Goal": "shots", "Total Saves": "saves", "Total Blocked Shots": "blocks",
    "Total Powerplay Points": "pp_points",
}
#: Ladder markets quoted as "N+" (folded into over N-0.5).
MILESTONE_PROPS: dict[str, str] = {
    "Points Milestones": "points", "Assists Milestones": "assists", "Goals Milestones": "goals",
    "Shots on Goal Milestones": "shots", "Goalkeeper Saves Milestones": "saves",
    "Blocked Shots Milestones": "blocks", "Powerplay Points Milestones": "pp_points",
}
#: Fixed-threshold scorer markets: type -> (prop_type, line), quoted as the over.
THRESHOLD_PROPS: dict[str, tuple[str, float]] = {
    "Anytime Goalscorer": ("goals", 0.5), "To Score 2+ Goals": ("goals", 1.5), "To Score 3+ Goals": ("goals", 2.5),
}
#: Line-less yes/no scorer markets.
YES_NO_PROPS: dict[str, str] = {
    "First Goalscorer": "first_goal", "Last Goalscorer": "last_goal", "First Team Goalscorer": "first_team_goal",
}
PERIOD_TYPE = re.compile(r"^(1st|2nd|3rd) Period (Moneyline|Spread|Total Goals)$")
PERIOD_CODES = {"1st": "p1", "2nd": "p2", "3rd": "p3"}
TEAM_TOTAL_REG = "Team Total Goals (Excl OT)"


def _ref_id(block: Any, kind: str) -> str | None:
    """The id in an ESPN ``$ref`` (``.../athletes/5118?lang=en`` -> ``"5118"``)."""
    ref = (block or {}).get("$ref") if isinstance(block, dict) else None
    match = re.search(rf"/{kind}/(\d+)", ref or "")
    return match.group(1) if match else None


def prop_bets_ref(item: dict[str, Any]) -> str | None:
    """HTTPS URL of a provider item's ``propBets`` collection, or None."""
    ref = (item.get("propBets") or {}).get("$ref")
    return ref.replace("http://", "https://", 1) if ref else None


def roster_athletes(roster: dict[str, Any]) -> dict[str, tuple[str, str | None]]:
    """``athlete_id -> (full name, ESPN team code)`` from a site-API team roster payload."""
    team = (roster.get("team") or {}).get("abbreviation")
    out: dict[str, tuple[str, str | None]] = {}
    for group in roster.get("athletes") or []:
        for athlete in (group.get("items") if isinstance(group, dict) and "items" in group else [group]) or []:
            if athlete.get("id") and (athlete.get("fullName") or athlete.get("displayName")):
                out[str(athlete["id"])] = (athlete.get("fullName") or athlete["displayName"], team)
    return out


def _mark_alternates(rows: list[dict[str, Any]]) -> None:
    """Flag every rung but the most balanced as alternate, per (period, market, subject). In place.

    Ladders such as "Team Total Goals (Excl OT)" list 0.5 to 5.5; the main rung is the one
    whose two sides' implied probabilities are closest.
    """
    groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["market"] in ("puckline", "total", "team_total") and row.get("line") is not None:
            groups[(row["period"], row["market"], row.get("subject", "game"))].append(row)

    def rung(row: dict[str, Any]) -> float:
        return -row["line"] if row["market"] == "puckline" and row["side"] == "away" else row["line"]

    for members in groups.values():
        ladder: dict[float, list[float]] = defaultdict(list)
        for row in members:
            ladder[rung(row)].append(implied_probability(row["price"]))
        if len(ladder) <= 1:
            continue
        main = min(ladder, key=lambda k: abs(ladder[k][0] - ladder[k][1]) if len(ladder[k]) == 2 else 9.0)
        for row in members:
            row["is_alternate"] = rung(row) != main


def prop_bet_rows(
    payload: dict[str, Any],
    meta: dict[str, Any],
    book: str,
    captured_at: datetime,
    athletes: dict[str, tuple[str, str | None]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split one provider's ``propBets`` into period/team-total odds rows and player props. Pure.

    Args:
        payload: The ``propBets`` collection (``items`` merged across pages).
        meta: Output of :func:`event_meta` (needs the ESPN team ids).
        book: Provider name.
        captured_at: Poll time (UTC).
        athletes: ``athlete_id -> (name, ESPN team code)`` from :func:`roster_athletes`.

    Returns:
        ``(odds_rows, prop_rows)``: rows for :func:`nhl.odds.core.odds_frame` and for
        :func:`nhl.odds.props.props_frame`. Unknown types are skipped (logged at debug).
    """
    sides_by_team = {meta.get("home_team_id"): "home", meta.get("away_team_id"): "away"}
    common = {"book": book, "captured_at": captured_at, "start_time": meta["start_time"],
              "away_team": meta["away_team"], "home_team": meta["home_team"],
              "price_point": "live", "source_event_id": str(meta["event_id"])}
    seen: dict[tuple, int] = defaultdict(int)
    odds_rows: list[dict[str, Any]] = []
    two_way: list[dict[str, Any]] = []
    other_props: list[dict[str, Any]] = []
    skipped: set[str] = set()
    for item in payload.get("items") or []:
        kind = (item.get("type") or {}).get("name") or ""
        odds = item.get("odds") or {}
        price = _ml_price(_num((odds.get("american") or {}).get("value")))
        if price is None:
            continue
        threshold = (odds.get("total") or {}).get("value")
        athlete_id = _ref_id(item.get("athlete"), "athletes")
        team_id = _ref_id(item.get("team"), "teams")
        team_side = sides_by_team.get(team_id)
        if team_id is not None and team_side is None:
            skipped.add(f"{kind} (team {team_id} not in this event)")
            continue
        order_key = (athlete_id or team_side or "game", kind, threshold)
        index = seen[order_key]
        seen[order_key] += 1
        if athlete_id is None:
            period_match = PERIOD_TYPE.match(kind)
            if period_match:
                period = PERIOD_CODES[period_match.group(1)]
                what = period_match.group(2)
                if what == "Moneyline":
                    odds_rows.append({**common, "period": period, "market": "moneyline_3way",
                                      "side": team_side or "draw", "line": None, "price": price})
                elif what == "Spread" and team_side and _num(threshold) is not None:
                    odds_rows.append({**common, "period": period, "market": "puckline", "side": team_side,
                                      "line": _num(threshold), "price": price})
                elif what == "Total Goals" and index < 2 and _num(threshold) is not None:
                    odds_rows.append({**common, "period": period, "market": "total",
                                      "side": ("over", "under")[index], "line": _num(threshold), "price": price})
            elif kind == TEAM_TOTAL_REG and team_side and index < 2 and _num(threshold) is not None:
                odds_rows.append({**common, "period": "reg", "market": "team_total", "subject": team_side,
                                  "side": ("over", "under")[index], "line": _num(threshold), "price": price})
            else:
                skipped.add(kind)
            continue
        name, team = athletes.get(athlete_id, (None, None))
        if not name:
            skipped.add(f"{kind} (unknown athlete {athlete_id})")
            continue
        base = {**common, "player_name": name, "team": team, "price": price}
        if kind in OVER_UNDER_PROPS and index < 2 and _num(threshold) is not None:
            two_way.append({**base, "prop_type": OVER_UNDER_PROPS[kind], "line": _num(threshold),
                            "side": ("over", "under")[index]})
        elif kind in MILESTONE_PROPS and index == 0 and milestone_line(threshold) is not None:
            other_props.append({**base, "prop_type": MILESTONE_PROPS[kind], "line": milestone_line(threshold),
                                "side": "over"})
        elif kind in THRESHOLD_PROPS and index < 2:
            prop_type, line = THRESHOLD_PROPS[kind]
            other_props.append({**base, "prop_type": prop_type, "line": line, "side": ("over", "under")[index]})
        elif kind in YES_NO_PROPS and index < 2:
            other_props.append({**base, "prop_type": YES_NO_PROPS[kind], "line": None, "side": ("yes", "no")[index]})
        else:
            skipped.add(kind)
    _mark_alternates(odds_rows)
    if skipped:
        logger.debug("event %s %s: prop types not stored: %s", meta.get("event_id"), book, sorted(skipped))
    # Two-way markets first: props_frame keeps the first of two rows that land on one key.
    return odds_rows, two_way + other_props


# ---------------------------------------------------------------------- sanity filter
def _implied() -> pl.Expr:
    """Raw implied probability of the ``price`` column."""
    return (
        pl.when(pl.col("price") > 0)
        .then(100.0 / (pl.col("price") + 100.0))
        .otherwise(-pl.col("price") / (-pl.col("price") + 100.0))
    )


def three_way_mask(odds: pl.DataFrame) -> pl.Series:
    """Flag moneyline rows that are really a three-way regulation line (see module docstring).

    A row qualifies when its (book, event, price point, market_uid) moneyline group is
    exactly one home and one away price whose implied probabilities sum into
    :data:`THREE_WAY_RANGE`.

    Returns:
        Boolean series aligned with ``odds`` (True = three-way).
    """
    if odds.is_empty():
        return pl.Series("three_way", [], dtype=pl.Boolean)
    pair = ["book", "source_event_id", "price_point", "market_uid"]
    return (
        odds.with_columns(
            _implied().sum().over(pair).alias("_sum"),
            pl.len().over(pair).alias("_n"),
            pl.col("side").n_unique().over(pair).alias("_sides"),
        )
        .select(
            (
                (pl.col("market") == "moneyline")
                & (pl.col("_n") == 2)
                & (pl.col("_sides") == 2)
                & pl.col("side").is_in(["home", "away"])
                & pl.col("_sum").is_between(*THREE_WAY_RANGE)
            ).alias("three_way")
        )
        .get_column("three_way")
    )


def classify_three_way(odds: pl.DataFrame) -> pl.DataFrame:
    """Relabel three-way moneyline pairs as ``moneyline_3way`` (period ``reg`` for full-game rows).

    A period line (``p1``) keeps its period: a three-way period moneyline settles on that
    period. ``market_uid`` is recomputed.
    """
    if odds.is_empty():
        return odds
    mask = three_way_mask(odds)
    return odds.with_columns(
        pl.when(mask).then(pl.lit("moneyline_3way")).otherwise(pl.col("market")).alias("market"),
        pl.when(mask & (pl.col("period") == "game")).then(pl.lit("reg")).otherwise(pl.col("period")).alias("period"),
    ).with_columns(market_uid_expr())


def suspect_mask(odds: pl.DataFrame) -> pl.Series:
    """Flag rows that are almost certainly in-game or stale quotes.

    Run :func:`classify_three_way` first: ``moneyline_3way`` rows are never flagged (their
    prices do not form a two-way book). Any other row is suspect when its two-way pair
    (same book, event, price point, ``market_uid``):

    * is a main line and incomplete (only one side quoted; an alternate rung quoted on one
      side only is normal on DraftKings' team-total ladders and is kept), or
    * is complete and has implied probabilities (vig included) summing outside
      :data:`VIG_RANGE`, or
    * is complete and is a puckline whose two lines are not mirror images, or a total whose
      sides disagree on the line, or
    * is a main total >= :data:`TOTAL_TOLERANCE` goals from the median main total across
      books for that event, price point and period.

    Args:
        odds: Output of :func:`nhl.odds.core.odds_frame`.

    Returns:
        Boolean series aligned with ``odds`` (True = drop).
    """
    if odds.is_empty():
        return pl.Series("suspect", [], dtype=pl.Boolean)
    pair = ["book", "source_event_id", "price_point", "market_uid"]
    signed_line = pl.when(pl.col("market") == "puckline").then(pl.col("line")).otherwise(-pl.col("line"))
    main_total = (pl.col("market") == "total") & ~pl.col("is_alternate")
    flagged = (
        odds.with_row_index("_r")
        .with_columns(
            _implied().sum().over(pair).alias("_sum"),
            pl.len().over(pair).alias("_n"),
            # puckline: home + away lines sum to 0; total: over/under lines agree.
            pl.when(pl.col("side").is_in(["home", "over"])).then(signed_line).otherwise(pl.col("line"))
            .sum().over(pair).alias("_line_sum"),
        )
        .with_columns(
            pl.when(main_total)
            .then(pl.col("line").filter(main_total).median().over(["source_event_id", "price_point", "period"]))
            .alias("_median_total"),
        )
        .with_columns(
            (
                (pl.col("market") != "moneyline_3way")
                & (
                    ((pl.col("_n") != 2) & ~(pl.col("is_alternate") & (pl.col("_n") == 1)))
                    | ((pl.col("_n") == 2) & ~pl.col("_sum").is_between(*VIG_RANGE))
                    | ((pl.col("_n") == 2) & (pl.col("market") != "moneyline") & (pl.col("_line_sum").abs() > 1e-9))
                    | ((pl.col("line") - pl.col("_median_total")).abs() >= TOTAL_TOLERANCE).fill_null(False)
                )
            ).alias("suspect")
        )
        .sort("_r")
    )
    return flagged.get_column("suspect")


def split_suspect(odds: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Reclassify three-way moneylines, then split into (kept, dropped) by :func:`suspect_mask`."""
    if odds.is_empty():
        return odds, odds
    odds = classify_three_way(odds)
    mask = suspect_mask(odds)
    return odds.filter(~mask), odds.filter(mask)


def _normalize_rows(rows: list[dict[str, Any]], label: str) -> tuple[pl.DataFrame, int]:
    """Type rows, reclassify three-way lines, apply the sanity filter and log both."""
    frame = odds_frame(rows)
    kept, dropped = split_suspect(frame)
    three_way = kept.filter((pl.col("market") == "moneyline_3way") & (pl.col("period") == "reg"))
    if three_way.height:
        pairs = three_way.select("book", "source_event_id", "price_point").unique()
        logger.info("%s: %d three-way regulation moneyline pair(s) reclassified (books: %s)", label, pairs.height,
                    sorted(pairs.get_column("book").unique().to_list()))
    if dropped.height:
        summary = dropped.group_by("book", "market").len().sort("len", descending=True).head(6).rows()
        logger.info("%s: sanity filter dropped %d/%d row(s) (top: %s)", label, dropped.height, frame.height, summary)
    return kept, dropped.height


def normalize_event_odds(
    payload: dict[str, Any], event_meta: dict[str, Any], captured_at: datetime, mode: Mode
) -> pl.DataFrame:
    """Normalize one event's ESPN odds payload into the shared odds table (pure).

    Args:
        payload: The per-event odds response (``items`` of every page merged).
        event_meta: Output of :func:`event_meta` (event id, UTC start, home/away codes).
        captured_at: Poll time for live rows; fetch time for history rows.
        mode: ``"live"``: current prices as ``price_point="live"``. ``"history"``:
            ``open``/``close`` from the nested blocks, else the current values as ``last``.

    Returns:
        Typed odds frame (``game_id`` null; see :func:`nhl.odds.core.attach_game_ids`)
        with three-way moneylines relabelled, suspect quotes removed and "Live"
        providers excluded.
    """
    rows = event_rows(payload, event_meta, captured_at, mode)
    kept, _ = _normalize_rows(rows, f"event {event_meta.get('event_id')}")
    return kept


# ---------------------------------------------------------------------------- fetch
def make_client(rps: float = RPS) -> WebClient:
    """ESPN client limited to ``rps`` requests per second across threads."""
    return WebClient("espn", rps=rps, headers={"Accept": "application/json"})


def fetch_scoreboard(client: WebClient, day: date) -> dict[str, Any]:
    """Scoreboard for one (US Eastern) date."""
    return client.get_json(SCOREBOARD_URL, params={"dates": f"{day:%Y%m%d}", "limit": 100})


def fetch_event_odds(client: WebClient, event_id: str) -> dict[str, Any]:
    """Every provider's odds for one event, following pagination into one ``items`` list."""
    url = ODDS_URL.format(id=event_id)
    first = client.get_json(url, params={"limit": 100})
    items = list(first.get("items") or [])
    for page in range(2, int(first.get("pageCount") or 1) + 1):
        items.extend(client.get_json(url, params={"limit": 100, "page": page}).get("items") or [])
    return {**first, "items": items}


def fetch_prop_bets(client: WebClient, url: str) -> dict[str, Any]:
    """One provider's ``propBets`` collection, every page merged into one ``items`` list."""
    first = client.get_json(url, params={"limit": 1000})
    items = list(first.get("items") or [])
    for page in range(2, int(first.get("pageCount") or 1) + 1):
        items.extend(client.get_json(url, params={"limit": 1000, "page": page}).get("items") or [])
    return {**first, "items": items}


def fetch_roster(client: WebClient, team_id: str) -> dict[str, Any]:
    """ESPN's current roster for one team (athlete ids and names)."""
    return client.get_json(ROSTER_URL.format(team_id=team_id))


def _season_of(game_id: pl.Expr) -> pl.Expr:
    """8-digit season id of an NHL game id (``2023020001 -> 20232024``)."""
    start = game_id // 1_000_000
    return start * 10000 + start + 1


def _drop_unmatched(odds: pl.DataFrame, label: str) -> pl.DataFrame:
    """Drop (and log) rows whose event could not be matched to an NHL game id."""
    unmatched = odds.filter(pl.col("game_id").is_null())
    if unmatched.height:
        events = unmatched.select("source_event_id", "away_team", "home_team", "start_time").unique().rows()
        logger.warning("%s: %d row(s) from %d event(s) unmatched to games: %s",
                       label, unmatched.height, len(events), events[:10])
    return odds.filter(pl.col("game_id").is_not_null())


# ----------------------------------------------------------------------------- live
def _event_props(
    client: WebClient,
    payload: dict[str, Any],
    meta: dict[str, Any],
    captured_at: datetime,
    rosters: dict[str, dict[str, Any]],
    bundle: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Fetch and flatten every provider's ``propBets`` for one event (rosters cached in ``rosters``)."""
    odds_rows: list[dict[str, Any]] = []
    prop_rows: list[dict[str, Any]] = []
    for item in payload.get("items") or []:
        book = (item.get("provider") or {}).get("name")
        url = prop_bets_ref(item)
        if not book or not url or is_live_provider(book):
            continue
        try:
            props = fetch_prop_bets(client, url)
        except SourceUnavailable as exc:
            logger.warning("prop bets for event %s (%s) skipped: %s", meta["event_id"], book, exc)
            continue
        bundle["props"][f"{meta['event_id']}/{book}"] = props
        athletes: dict[str, tuple[str, str | None]] = {}
        if any(i.get("athlete") for i in props.get("items") or []):
            for team_id in (meta.get("home_team_id"), meta.get("away_team_id")):
                if team_id and team_id not in rosters:
                    try:
                        rosters[team_id] = fetch_roster(client, team_id)
                    except SourceUnavailable as exc:
                        logger.warning("ESPN roster %s unavailable: %s", team_id, exc)
                        rosters[team_id] = {}
                    bundle["rosters"][team_id] = rosters[team_id]
                athletes.update(roster_athletes(rosters.get(team_id) or {}))
        game_rows, player_rows = prop_bet_rows(props, meta, book, captured_at, athletes)
        odds_rows.extend(game_rows)
        prop_rows.extend(player_rows)
    return odds_rows, prop_rows


def poll(
    store: Store,
    games: pl.DataFrame,
    dates: list[date] | None = None,
    client: WebClient | None = None,
    resolver: PlayerResolver | None = None,
) -> int:
    """Poll ESPN odds (and DraftKings' prop bets) for pre-game events; store transitions.

    Game markets (including the period markets from ``propBets``) go to the ``espn`` odds
    table; player props go to the ``espn`` props table (:func:`nhl.odds.props.props_key`),
    each with its own log line.

    Args:
        store: S3 store.
        games: ``processed/games.parquet``.
        dates: US Eastern dates to read; defaults to today and tomorrow.
        client: Optional shared client (default ~3 req/s).
        resolver: Player resolver for props; built from the store when needed.

    Returns:
        Number of odds transition rows written (props are logged separately).
    """
    client = client or make_client()
    captured_at = utcnow()
    if dates is None:
        today = datetime.now(EASTERN).date()
        dates = [today, today + timedelta(days=1)]
    rows: list[dict[str, Any]] = []
    prop_rows: list[dict[str, Any]] = []
    rosters: dict[str, dict[str, Any]] = {}
    for day in dates:
        try:
            board = fetch_scoreboard(client, day)
        except SourceUnavailable as exc:
            logger.warning("scoreboard %s skipped: %s", day, exc)
            continue
        bundle: dict[str, Any] = {"scoreboard": board, "odds": {}, "props": {}, "rosters": {}}
        for event in board.get("events") or []:
            meta = event_meta(event)
            if meta["state"] != "pre" or meta["season_type"] not in SEASON_TYPES:
                continue
            try:
                payload = fetch_event_odds(client, meta["event_id"])
            except SourceUnavailable as exc:
                logger.warning("odds for event %s skipped: %s", meta["event_id"], exc)
                continue
            bundle["odds"][meta["event_id"]] = payload
            rows.extend(event_rows(payload, meta, captured_at, "live"))
            game_rows, player_rows = _event_props(client, payload, meta, captured_at, rosters, bundle)
            rows.extend(game_rows)
            prop_rows.extend(player_rows)
        archive_raw(store, SOURCE, captured_at, f"{day:%Y%m%d}", bundle)
    odds, _ = _normalize_rows(rows, "espn live poll")
    written = 0
    if odds.is_empty():
        logger.info("espn live poll: no odds")
    else:
        odds = _drop_unmatched(attach_game_ids(odds, games), "espn live poll")
        written = store_odds(store, odds, games, "espn")
        logger.info("espn live poll: %d priced row(s), %d transition(s) written", odds.height, written)
    props = props_frame(prop_rows)
    props_written = store_props(store, props, games, "espn", resolver) if props.height else 0
    logger.info("espn live poll: %d prop price(s), %d prop transition(s) written", props.height, props_written)
    return written


# -------------------------------------------------------------------------- history
def _cached_json(store: Store, key: str, existing: set[str], fetch: Any) -> Any:
    """Return the cached payload at ``key`` or fetch, store and return it."""
    if key in existing:
        cached = store.get_json_gz(key)
        if cached is not None:
            return cached
    payload = fetch()
    store.put_json_gz(key, payload)
    return payload


def _season_events(
    store: Store, client: WebClient, season: int, days: Iterable[date], existing: set[str]
) -> dict[str, dict[str, Any]]:
    """Regular-season and playoff events (by id -> meta) on the given dates, minus dead ones."""
    events: dict[str, dict[str, Any]] = {}
    for day in days:
        try:
            board = _cached_json(store, history_scoreboard_key(season, day), existing,
                                 lambda d=day: fetch_scoreboard(client, d))
        except SourceUnavailable as exc:
            logger.warning("scoreboard %s skipped: %s", day, exc)
            continue
        for event in board.get("events") or []:
            meta = event_meta(event)
            if meta["season_type"] in SEASON_TYPES and meta["status"] not in DEAD_STATUSES:
                events[meta["event_id"]] = meta
    return events


def _coverage(odds: pl.DataFrame, final_ids: set[int]) -> float:
    """Share of final games with at least one book's close/last moneyline."""
    if not final_ids:
        return 0.0
    covered = set(
        odds.filter((pl.col("market") == "moneyline") & pl.col("price_point").is_in(["close", "last"]))
        .get_column("game_id").unique().to_list()
    )
    return len(covered & final_ids) / len(final_ids)


def backfill_history(
    store: Store, games: pl.DataFrame, start_years: list[int], workers: int = 1, client: WebClient | None = None
) -> dict[int, int]:
    """Backfill historical open/close/last prices from ESPN, one table per season.

    Resumable: each date's scoreboard and each event's raw odds are cached under
    ``raw/external/espn_odds_history/{season}/`` and never refetched. The season table at
    :func:`nhl.storage.keys.odds_history` is rebuilt from the cache and overwritten.

    Args:
        store: S3 store.
        games: ``processed/games.parquet``.
        start_years: Season start years, e.g. ``[2023]`` for 2023-24.
        workers: Concurrent event fetches (all share one ~3 req/s limiter).
        client: Optional shared client.

    Returns:
        Rows written per start year.
    """
    client = client or make_client()
    out: dict[int, int] = {}
    for year in start_years:
        season = config.season_id(year)
        finals = games.filter(
            (pl.col("season") == season) & pl.col("is_final") & pl.col("season_type").is_in(["R", "P"])
        )
        final_ids = set(finals.get_column("game_id").to_list())
        days = sorted(finals.get_column("game_date").unique().to_list())
        existing = set(store.list_keys(history_prefix(season)))
        events = _season_events(store, client, season, days, existing)
        logger.info("%s: %d dates, %d ESPN events, %d final games", season, len(days), len(events), len(final_ids))

        fetched_at = utcnow()

        def load(meta: dict[str, Any]) -> list[dict[str, Any]]:
            """Cached-or-fetched history rows for one event."""
            eid = meta["event_id"]
            try:
                payload = _cached_json(store, history_event_key(season, eid), existing,
                                       lambda: fetch_event_odds(client, eid))
            except SourceUnavailable as exc:
                logger.warning("odds for event %s skipped: %s", eid, exc)
                return []
            except Exception as exc:  # noqa: BLE001 - one bad event must not stop the season
                logger.warning("odds for event %s failed: %r", eid, exc)
                return []
            return event_rows(payload, meta, fetched_at, "history")

        rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            for n, part in enumerate(pool.map(load, events.values()), 1):
                rows.extend(part)
                if n % 200 == 0:
                    logger.info("%s: %d/%d events", season, n, len(events))

        odds, dropped = _normalize_rows(rows, f"espn history {season}")
        odds = _drop_unmatched(attach_game_ids(odds, games), f"espn history {season}")
        odds = odds.filter(_season_of(pl.col("game_id")) == season)
        store.put_parquet(keys.odds_history(season), odds)
        books = sorted(odds.get_column("book").unique().to_list()) if odds.height else []
        three_way = odds.filter(pl.col("market") == "moneyline_3way").select(
            "book", "game_id", "price_point").unique().height
        logger.info(
            "%s: %d row(s) written, %d dropped by sanity filter, %d three-way moneyline pair(s), "
            "coverage %.1f%% of %d final games, books: %s",
            season, odds.height, dropped, three_way, 100 * _coverage(odds, final_ids), len(final_ids), books,
        )
        out[year] = odds.height
    return out
