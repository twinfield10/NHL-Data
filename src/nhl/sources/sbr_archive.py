"""SBR odds archive: one Excel workbook per NHL season, recovered from the Wayback Machine.

SportsbookReviewsOnline published ``scoresoddsarchives/nhl/nhl odds YYYY-YY.xlsx`` with
every game's opening and closing prices (2007-08 .. 2022-23; the 2020-21 season is
``nhl odds 2021.xlsx``). The live site dropped them; the Internet Archive kept snapshots.
:func:`list_archive_files` reads the CDX index (latest 200 capture per file) and
:func:`download_workbook` fetches the raw bytes (``/web/{ts}id_/{url}``) once, caching them
in S3 at ``raw/external/sbr/{filename}``. archive.org is hit at most once per 2 s, with an
identifying User-Agent (it answers spoofed browser agents from scripts with 429s).

Workbook formats (one sheet of two rows per game, visitor then home; ``N``/``N`` for
neutral sites; dates as ``MMDD``):

* 2007-08 .. 2013-14 (14 columns): Date, Rot, VH, Team, 1st, 2nd, 3rd, Final, Open, Close
  (moneylines), OpenOU + price, CloseOU + price.
* 2014-15 .. 2022-23 (16 columns): adds ``PuckLine`` + price (one quote, stored as close)
  between Close and OpenOU. Single sheet from 2019-20 (three sheets before).

Rows land in the shared odds schema (:func:`nhl.odds.core.odds_frame`) as book
``SBR consensus`` with ``price_point`` ``open``/``close``. Timestamps, which SBR does not
give: open = game date 00:00 UTC; close = the NHL scheduled start (``captured_at ==
start_time``), falling back to the game date 23:59 UTC before a game is matched.

Known quirks handled in :func:`parse_workbook`: team labels as city names with typos
("Tampa", "Arizonas", "SeattleKraken", "Los Angeles"); ``NL``/``-``/``0``/``a100`` cells
(treated as missing); 2014-15 puck lines merged into one cell (repaired); orientation
that disagrees with the NHL's home team (retried swapped in :func:`import_season`).
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from urllib.parse import unquote

import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nhl import config
from nhl.ingest.http import RateLimiter
from nhl.odds.core import attach_game_ids, odds_frame
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

SOURCE = "sbr"
CDX_URL = "https://web.archive.org/cdx/search/cdx"
CDX_PATTERN = "sportsbookreviewsonline.com/scoresoddsarchives/nhl/*"
RAW_URL = "https://web.archive.org/web/{timestamp}id_/{original}"
#: archive.org asks for politeness: at most one request every two seconds.
RPS = 0.5
#: An identifying agent: archive.org throttles (429) spoofed browser agents from scripts.
USER_AGENT = "nhl-data-research/0.2 (historical odds import; github.com/twinfield10/NHL-Data)"
#: SBR does not name a sportsbook; its lines are a consensus/representative quote.
BOOK = "SBR consensus"
#: First season in ``processed/games.parquet``; earlier workbooks cannot be matched.
FIRST_SEASON = 2010
#: ``games.start_time_et`` is US Eastern wall time.
EASTERN = "America/New_York"

_LIMITER = RateLimiter(RPS)
_SESSION: requests.Session | None = None
_FILE_RE = re.compile(r"nhl odds (\d{4})(?:-(\d{2}))?\.xlsx$", re.IGNORECASE)


def raw_key(filename: str) -> str:
    """S3 key of one cached workbook."""
    return f"raw/external/{SOURCE}/{filename}"


def _session() -> requests.Session:
    """Shared session retrying archive.org's frequent 5xx/429 with long backoff."""
    global _SESSION
    if _SESSION is None:
        retry = Retry(total=5, backoff_factor=10.0, backoff_max=120.0,
                      status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",),
                      respect_retry_after_header=False)
        _SESSION = requests.Session()
        _SESSION.mount("https://", HTTPAdapter(max_retries=retry))
        _SESSION.headers["User-Agent"] = USER_AGENT
    return _SESSION


def _get(url: str, params: dict[str, Any] | None = None, timeout: float = 60.0) -> requests.Response:
    """Rate-limited GET against archive.org (retried; raises on HTTP errors)."""
    _LIMITER.wait()
    resp = _session().get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp


def season_start_year(filename: str) -> int | None:
    """Season start year a workbook covers, from its name.

    ``nhl odds 2010-11.xlsx`` -> 2010. The single-year ``nhl odds 2021.xlsx`` is the
    COVID-shortened 2020-21 season (Jan-Jul 2021), so it maps to 2020.
    """
    match = _FILE_RE.search(unquote(filename))
    if not match:
        return None
    year = int(match.group(1))
    return year if match.group(2) else year - 1


def list_archive_files() -> list[dict[str, Any]]:
    """Every archived NHL odds workbook, latest 200 snapshot per file.

    Returns:
        Dicts ``{"filename", "start_year", "timestamp", "original", "length"}`` sorted by
        season. http/https and www/bare-host captures of one file are merged.
    """
    params = {"url": CDX_PATTERN, "output": "json", "filter": "statuscode:200",
              "fl": "timestamp,original,length"}
    rows = _get(CDX_URL, params=params).json()
    latest: dict[str, dict[str, Any]] = {}
    for timestamp, original, length in rows[1:]:
        filename = unquote(original.rsplit("/", 1)[-1])
        year = season_start_year(filename)
        if year is None:
            continue
        entry = {"filename": filename, "start_year": year, "timestamp": timestamp,
                 "original": original, "length": int(length) if str(length).isdigit() else None}
        if filename not in latest or timestamp > latest[filename]["timestamp"]:
            latest[filename] = entry
    return sorted(latest.values(), key=lambda e: e["start_year"])


def download_workbook(store: Store, entry: dict[str, Any]) -> bytes:
    """Workbook bytes for one archive entry, from the S3 cache or (once) from Wayback.

    Args:
        store: S3 store.
        entry: One item of :func:`list_archive_files`.

    Returns:
        Raw ``.xlsx`` bytes.
    """
    key = raw_key(entry["filename"])
    cached = store.get_bytes(key)
    if cached is not None:
        return cached
    url = RAW_URL.format(timestamp=entry["timestamp"], original=entry["original"])
    data = _get(url).content
    if not data.startswith(b"PK"):
        raise ValueError(f"{url} did not return an xlsx (got {data[:40]!r})")
    store.put_bytes(key, data, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    logger.info("cached %s (%d bytes) -> %s", entry["filename"], len(data), store.uri(key))
    return data


# --------------------------------------------------------------------------- parsing
#: SBR team labels (city names, spaces removed) -> NHL tricode. Fed to ``resolve_team``
#: so the stored codes follow the shared, season-aware resolution (Phoenix -> PHX, Atlanta -> ATL).
SBR_TEAMS: dict[str, str] = {
    "anaheim": "ANA", "arizona": "ARI", "phoenix": "PHX", "atlanta": "ATL", "boston": "BOS",
    "buffalo": "BUF", "carolina": "CAR", "columbus": "CBJ", "calgary": "CGY", "chicago": "CHI",
    "colorado": "COL", "dallas": "DAL", "detroit": "DET", "edmonton": "EDM", "florida": "FLA",
    "losangeles": "LAK", "minnesota": "MIN", "montreal": "MTL", "newjersey": "NJD",
    "nashville": "NSH", "nyislanders": "NYI", "nyrangers": "NYR", "ottawa": "OTT",
    "philadelphia": "PHI", "pittsburgh": "PIT", "seattle": "SEA", "sanjose": "SJS",
    "stlouis": "STL", "tampabay": "TBL", "toronto": "TOR", "vancouver": "VAN", "vegas": "VGK",
    "winnipeg": "WPG", "washington": "WSH", "utah": "UTA",
    # Typos seen in the 2019-20 workbook.
    "tampa": "TBL", "arizonas": "ARI",
}

#: Normalized header -> canonical column. A blank header is the price of the column before it.
HEADERS: dict[str, str] = {
    "date": "date", "rot": "rot", "vh": "vh", "team": "team", "final": "final",
    "open": "ml_open", "close": "ml_close", "puckline": "pl_line", "puckline_price": "pl_price",
    "openou": "ou_open_line", "openou_price": "ou_open_price",
    "closeou": "ou_close_line", "closeou_price": "ou_close_price",
}

#: Per-game output of :func:`parse_workbook`.
GAME_SCHEMA: dict[str, pl.DataType] = {
    "game_date": pl.Date,
    "away_label": pl.Utf8, "home_label": pl.Utf8,
    "away_team": pl.Utf8, "home_team": pl.Utf8,
    "neutral": pl.Boolean,
    "away_final": pl.Int64, "home_final": pl.Int64,
    "ml_open_away": pl.Float64, "ml_open_home": pl.Float64,
    "ml_close_away": pl.Float64, "ml_close_home": pl.Float64,
    "pl_away_line": pl.Float64, "pl_away": pl.Float64,
    "pl_home_line": pl.Float64, "pl_home": pl.Float64,
    "total_open": pl.Float64, "over_open": pl.Float64, "under_open": pl.Float64,
    "total_close": pl.Float64, "over_close": pl.Float64, "under_close": pl.Float64,
    "notes": pl.Utf8,
    "source_event_id": pl.Utf8,
}


def resolve_sbr_team(label: str | None) -> str | None:
    """NHL tricode for an SBR team label ("NYRangers", "TampaBay", "St.Louis", "Phoenix")."""
    if not label:
        return None
    norm = re.sub(r"[^a-z]", "", str(label).lower())
    return resolve_team(SBR_TEAMS.get(norm, str(label)))


def _num(value: Any) -> float | None:
    """Parse a workbook cell to float; ``pk``/``NL``/blank/garbage -> None, ``even`` -> 100."""
    if value is None:
        return None
    text = str(value).strip().lower().replace("+", "")
    if text in ("ev", "even"):
        return 100.0
    try:
        return float(text)
    except ValueError:
        return None


def _price(value: Any) -> float | None:
    """An American price, or None for impossible values (|price| < 100, e.g. 0 or a line)."""
    num = _num(value)
    return num if num is not None and abs(num) >= 100 else None


def _norm_header(text: Any) -> str:
    """Lower-case, alphanumeric-only header text."""
    return re.sub(r"[^a-z0-9]", "", str(text or "").lower())


def _column_map(header: tuple[Any, ...]) -> dict[str, int]:
    """Canonical column -> position, from the workbook's header row.

    A blank header cell is the price paired with the named column before it
    (``OpenOU``, blank -> ``ou_open_line``, ``ou_open_price``).
    """
    out: dict[str, int] = {}
    prev = ""
    for idx, cell in enumerate(header):
        name = _norm_header(cell)
        key = f"{prev}_price" if not name and prev else name
        if name:
            prev = name
        canonical = HEADERS.get(key)
        if canonical and canonical not in out:
            out[canonical] = idx
    missing = {"date", "vh", "team", "final", "ml_open", "ml_close"} - out.keys()
    if missing:
        raise ValueError(f"SBR workbook header lacks {sorted(missing)}: {header}")
    return out


def _dates(mmdd: list[Any], start_year: int) -> list[date | None]:
    """Turn the ``MMDD`` column into dates; the year rolls over when the month goes back.

    Rows are in date order, so Dec -> Jan bumps the year. This also handles the 2019-20
    bubble playoffs (Aug-Sep 2020) and the Jan-Jul 2020-21 season, where a month-based
    rule would fail.
    """
    out: list[date | None] = []
    year, prev_month = start_year, None
    for raw in mmdd:
        num = _num(raw)
        if num is None:
            out.append(None)
            continue
        month, day = divmod(int(num), 100)
        if prev_month is not None and month < prev_month - 3:
            year += 1
        if prev_month is None and month < 7:
            year = start_year + 1  # a season file that starts in the new year (2020-21)
        prev_month = month
        try:
            out.append(date(year, month, day))
        except ValueError:
            out.append(None)
    return out


def _repair_puckline(game: dict[str, Any], vis_cell: Any, home_cell: Any) -> bool:
    """Fix a puck line whose line and price were merged into the line cell.

    Seen in 2014-15 (Feb 13): ``PuckLine`` holds ``-278.5`` / ``238.5`` and the price cell
    is blank, i.e. price -278 / +238 with the 1.5 line's sign lost. The favorite (lower
    closing moneyline; if level, the side with the plus price) gets -1.5.

    Returns:
        True if the game was repaired.
    """
    away_raw, home_raw = _num(vis_cell), _num(home_cell)
    if (away_raw is None or home_raw is None or abs(away_raw) < 100 or abs(home_raw) < 100
            or game["pl_away"] is not None or game["pl_home"] is not None):
        return False
    game["pl_away"], game["pl_home"] = float(int(away_raw)), float(int(home_raw))
    ml_away, ml_home = game["ml_close_away"], game["ml_close_home"]
    if ml_away is not None and ml_home is not None and ml_away != ml_home:
        away_fav = ml_away < ml_home
    else:
        away_fav = game["pl_away"] > 0
    game["pl_away_line"], game["pl_home_line"] = (-1.5, 1.5) if away_fav else (1.5, -1.5)
    return True


def parse_workbook(data: bytes, start_year: int) -> pl.DataFrame:
    """Parse one SBR season workbook into one row per game (pure).

    The sheet has two rows per game, visitor (``V``) then home (``H``); neutral-site
    games use ``N`` for both and keep that order. Columns are located by header name
    (formats changed over the years, see the module docstring). Per pair:

    * moneyline: ``Open``/``Close`` from each team's row;
    * puck line (2014-15 on): ``PuckLine`` + price, each row from its own perspective;
    * totals: ``OpenOU``/``CloseOU`` + price. The visitor row carries the **over** price
      and the home row the **under** price (verified on 2010-16: the visitor row's
      implied probability correlates positively with the over hitting in every season).
      Where the two rows disagree on the line the pair is dropped.

    Data problems are recorded in ``notes`` (comma-separated): ``puckline_merged_cell``
    (repaired, see :func:`_repair_puckline`), ``puckline_not_mirrored`` (dropped),
    ``total_{open,close}_line_mismatch`` (dropped), ``no_close_moneyline`` (``0``/``NL``).

    Args:
        data: ``.xlsx`` bytes.
        start_year: Season start year (2020 for the 2020-21 file ``nhl odds 2021.xlsx``).

    Returns:
        Frame with :data:`GAME_SCHEMA` columns; unpaired rows are skipped (and logged).
    """
    sheet = pl.read_excel(io.BytesIO(data), sheet_id=1, has_header=False, infer_schema_length=0)
    rows = sheet.rows()
    if not rows:
        return pl.DataFrame(schema=GAME_SCHEMA)
    cols = _column_map(rows[0])
    body = [r for r in rows[1:] if str(r[cols["vh"]] or "").strip().upper() in ("V", "H", "N")]
    dates = _dates([r[cols["date"]] for r in body], start_year)

    def cell(row: tuple[Any, ...], name: str) -> Any:
        return row[cols[name]] if name in cols else None

    games: list[dict[str, Any]] = []
    skipped = 0
    i = 0
    while i < len(body) - 1:
        vis, home = body[i], body[i + 1]
        vh = (str(cell(vis, "vh")).strip().upper(), str(cell(home, "vh")).strip().upper())
        if vh not in (("V", "H"), ("N", "N")) or dates[i] is None:
            skipped += 1
            i += 1
            continue
        i += 2
        game: dict[str, Any] = {
            "game_date": dates[i - 2],
            "away_label": str(cell(vis, "team") or "").strip(),
            "home_label": str(cell(home, "team") or "").strip(),
            "neutral": vh == ("N", "N"),
            "away_final": _num(cell(vis, "final")),
            "home_final": _num(cell(home, "final")),
            "ml_open_away": _price(cell(vis, "ml_open")), "ml_open_home": _price(cell(home, "ml_open")),
            "ml_close_away": _price(cell(vis, "ml_close")), "ml_close_home": _price(cell(home, "ml_close")),
            "pl_away_line": _num(cell(vis, "pl_line")), "pl_away": _price(cell(vis, "pl_price")),
            "pl_home_line": _num(cell(home, "pl_line")), "pl_home": _price(cell(home, "pl_price")),
        }
        notes: list[str] = []
        if _repair_puckline(game, cell(vis, "pl_line"), cell(home, "pl_line")):
            notes.append("puckline_merged_cell")
        elif game["pl_away_line"] is not None and game["pl_away_line"] != -(game["pl_home_line"] or 0):
            notes.append("puckline_not_mirrored")
            game.update(pl_away_line=None, pl_away=None, pl_home_line=None, pl_home=None)
        if game["ml_close_away"] is None or game["ml_close_home"] is None:
            notes.append("no_close_moneyline")
        for point in ("open", "close"):
            over_line, under_line = _num(cell(vis, f"ou_{point}_line")), _num(cell(home, f"ou_{point}_line"))
            same = over_line is not None and over_line == under_line
            if over_line is not None and under_line is not None and not same:
                notes.append(f"total_{point}_line_mismatch")
            game[f"total_{point}"] = over_line if same else None
            game[f"over_{point}"] = _price(cell(vis, f"ou_{point}_price")) if same else None
            game[f"under_{point}"] = _price(cell(home, f"ou_{point}_price")) if same else None
        game["notes"] = ",".join(notes) or None
        games.append(game)
    if skipped:
        logger.info("sbr %d: skipped %d unpaired row(s)", start_year, skipped)
    if not games:
        return pl.DataFrame(schema=GAME_SCHEMA)
    df = pl.DataFrame(games, infer_schema_length=None, strict=False).with_columns(
        pl.col("away_label").map_elements(resolve_sbr_team, return_dtype=pl.Utf8).alias("away_team"),
        pl.col("home_label").map_elements(resolve_sbr_team, return_dtype=pl.Utf8).alias("home_team"),
    )
    return _with_event_id(df).select([pl.col(c).cast(t) for c, t in GAME_SCHEMA.items()])


def _with_event_id(parsed: pl.DataFrame) -> pl.DataFrame:
    """(Re)compute ``source_event_id = {YYYYMMDD}-{away}-{home}`` (SBR has no event ids)."""
    return parsed.with_columns(
        pl.format("{}-{}-{}", pl.col("game_date").dt.strftime("%Y%m%d"),
                  pl.col("away_team").fill_null("?"), pl.col("home_team").fill_null("?"))
        .alias("source_event_id")
    )


def swap_sides(parsed: pl.DataFrame) -> pl.DataFrame:
    """Swap visitor and home in parsed games (totals unchanged).

    SBR's visitor/home order need not match the NHL's home designation for neutral-site
    (``N``/``N``: 2019-20 bubble, global series) and outdoor games.
    """
    pairs = [("away_label", "home_label"), ("away_team", "home_team"), ("away_final", "home_final"),
             ("ml_open_away", "ml_open_home"), ("ml_close_away", "ml_close_home"),
             ("pl_away_line", "pl_home_line"), ("pl_away", "pl_home")]
    renamed = {a: b for a, b in pairs} | {b: a for a, b in pairs}
    swapped = parsed.rename(renamed).select(parsed.columns)
    return _with_event_id(swapped).with_columns(
        pl.concat_str(pl.col("notes"), pl.lit("sides_swapped"), separator=",", ignore_nulls=True).alias("notes")
    )


def _utc_midnight(day: date) -> datetime:
    """``day`` at 00:00 UTC."""
    return datetime.combine(day, time(0, 0), tzinfo=timezone.utc)


def odds_rows(parsed: pl.DataFrame) -> pl.DataFrame:
    """Expand :func:`parse_workbook` output into the shared odds table (pure).

    * ``start_time`` and ``captured_at`` are placeholders here: the game date at 00:00 UTC
      (and 23:59 UTC for close). :func:`import_seasons` replaces ``start_time`` and the
      close ``captured_at`` with the NHL scheduled start once ``game_id`` is attached.
    * The workbook's ``PuckLine`` column is a single (closing) quote, so it is stored as
      ``price_point="close"``.
    * Only complete two-way pairs are emitted (a lone over price is dropped).
    """
    rows: list[dict[str, Any]] = []
    for g in parsed.iter_rows(named=True):
        day = g["game_date"]
        open_at = _utc_midnight(day)
        close_at = open_at + timedelta(hours=23, minutes=59)
        base = {
            "book": BOOK, "start_time": open_at, "away_team": g["away_team"], "home_team": g["home_team"],
            "source_event_id": g["source_event_id"],
        }
        pairs = [
            ("open", "moneyline", ("away", None, g["ml_open_away"]), ("home", None, g["ml_open_home"])),
            ("close", "moneyline", ("away", None, g["ml_close_away"]), ("home", None, g["ml_close_home"])),
            ("close", "puckline", ("away", g["pl_away_line"], g["pl_away"]),
             ("home", g["pl_home_line"], g["pl_home"])),
            ("open", "total", ("over", g["total_open"], g["over_open"]),
             ("under", g["total_open"], g["under_open"])),
            ("close", "total", ("over", g["total_close"], g["over_close"]),
             ("under", g["total_close"], g["under_close"])),
        ]
        for point, market, *sides in pairs:
            # Only complete two-way pairs: one quoted side is not a usable market.
            if any(price is None or (market != "moneyline" and line is None) for _, line, price in sides):
                continue
            for side, line, price in sides:
                rows.append({**base, "captured_at": open_at if point == "open" else close_at,
                             "price_point": point, "market": market, "side": side, "line": line,
                             "price": price})
    return odds_frame(rows)


# ---------------------------------------------------------------------------- import
def workbook_filename(start_year: int) -> str:
    """Archive filename for a season (``nhl odds 2021.xlsx`` for the 2020-21 season)."""
    if start_year == 2020:
        return "nhl odds 2021.xlsx"
    return f"nhl odds {start_year}-{(start_year + 1) % 100:02d}.xlsx"


def _normalized_games(games: pl.DataFrame, season: int) -> pl.DataFrame:
    """Final games of one season with team codes run through ``resolve_team``.

    The games table keeps the historical ``PHX`` while :func:`nhl.odds.core.odds_frame`
    resolves every Coyotes label to ``ARI``; both sides must agree for the join.
    """
    resolve = {c: resolve_team(c) or c for c in games.select("home_abbr", "away_abbr").unpivot()["value"].unique()}
    return games.filter((pl.col("season") == season) & pl.col("is_final")).with_columns(
        pl.col("home_abbr").replace(resolve), pl.col("away_abbr").replace(resolve),
        pl.col("start_time_et").str.to_datetime(strict=False)
        .dt.replace_time_zone(EASTERN).dt.convert_time_zone("UTC").alias("_start"),
    )


def match_games(odds: pl.DataFrame, season_games: pl.DataFrame) -> pl.DataFrame:
    """Attach ``game_id`` and the NHL scheduled start; drop rows that match no game.

    After the match, ``start_time`` becomes the scheduled start (UTC) and close prices are
    stamped ``captured_at = start_time`` (the line at puck drop). Open prices keep the
    game date at 00:00 UTC: SBR does not say when a line opened, only that it was the
    first one posted, which is always before the game day starts in UTC.

    Args:
        odds: Output of :func:`odds_rows`.
        season_games: Output of :func:`_normalized_games`.
    """
    matched = attach_game_ids(odds, season_games).filter(pl.col("game_id").is_not_null())
    starts = season_games.select("game_id", "_start")
    return (
        matched.join(starts, on="game_id", how="left")
        .with_columns(pl.coalesce("_start", "start_time").alias("start_time"))
        .with_columns(
            pl.when(pl.col("price_point") == "close").then(pl.col("start_time"))
            .otherwise(pl.col("captured_at")).alias("captured_at")
        )
        .drop("_start")
    )


def _markets(odds: pl.DataFrame) -> dict[str, int]:
    """Games priced per ``market/price_point``."""
    if odds.is_empty():
        return {}
    counts = odds.group_by("market", "price_point").agg(pl.col("game_id").n_unique()).sort("market", "price_point")
    return {f"{m}/{p}": n for m, p, n in counts.rows()}


def import_season(store: Store, games: pl.DataFrame, start_year: int, data: bytes) -> dict[str, Any]:
    """Parse, match, validate and write one season's workbook.

    Returns:
        Stats: games in file, matched, unmatched sample, score mismatches, duplicates,
        markets present, rows written and coverage of the season's final games.
    """
    season = config.season_id(start_year)
    parsed = parse_workbook(data, start_year)
    season_games = _normalized_games(games, season)
    odds = match_games(odds_rows(parsed), season_games)

    # Games SBR lists in the opposite orientation to the NHL (neutral sites, outdoor games
    # like the 2018 Winter Classic where BUF was the designated home team): retry with sides
    # swapped, accepting a swap only when the swapped final score agrees with the NHL's.
    retry = parsed.filter(~pl.col("source_event_id").is_in(odds["source_event_id"].implode()))
    swapped = 0
    if retry.height:
        flipped = swap_sides(retry)
        flipped_odds = match_games(odds_rows(flipped), season_games)
        confirmed = (
            flipped_odds.select("source_event_id", "game_id").unique()
            .join(flipped.select("source_event_id", "away_final", "home_final"), on="source_event_id")
            .join(season_games.select("game_id", "away_score", "home_score"), on="game_id")
            .filter((pl.col("away_final") == pl.col("away_score")) & (pl.col("home_final") == pl.col("home_score")))
        )
        hit = flipped.filter(pl.col("source_event_id").is_in(confirmed["source_event_id"].implode()))
        if hit.height:
            originals = hit.select("game_date", pl.col("home_team").alias("_a"), pl.col("away_team").alias("_h"))
            parsed = pl.concat([
                parsed.join(originals, left_on=["game_date", "away_team", "home_team"],
                            right_on=["game_date", "_a", "_h"], how="anti", nulls_equal=True),
                hit,
            ])
            odds = pl.concat([odds, flipped_odds.filter(pl.col("source_event_id").is_in(hit["source_event_id"].implode()))])
            swapped = hit.height

    # One SBR game per NHL game: if two workbook games hit the same game id, keep the first.
    ids = odds.select("source_event_id", "game_id").unique().sort("source_event_id")
    dupes = ids.filter(pl.col("game_id").is_duplicated())
    if dupes.height:
        keep = ids.unique("game_id", keep="first", maintain_order=True)
        odds = odds.join(keep, on=["source_event_id", "game_id"], how="semi")
        ids = keep

    checked = (
        parsed.join(ids, on="source_event_id", how="inner")
        .join(season_games.select("game_id", "home_score", "away_score"), on="game_id", how="left")
    )
    mismatched = checked.filter(
        (pl.col("away_final") != pl.col("away_score")) | (pl.col("home_final") != pl.col("home_score"))
    )
    unmatched = parsed.join(ids, on="source_event_id", how="anti")
    store.put_parquet(keys.odds_history_sbr(season), odds.sort("game_id", "price_point", "market", "side"))

    finals = season_games.height
    stats = {
        "season": season,
        "games_in_file": parsed.height,
        "games_matched": ids.height,
        "unmatched": unmatched.height,
        "unmatched_sample": unmatched.select("game_date", "away_label", "home_label").head(10).rows(),
        "duplicate_matches": dupes.height,
        "sides_swapped": swapped,
        "notes": dict(parsed.filter(pl.col("notes").is_not_null()).group_by("notes").len().rows()),
        "score_mismatches": mismatched.height,
        "score_mismatch_sample": mismatched.select(
            "game_id", "away_team", "home_team", "away_final", "home_final", "away_score", "home_score"
        ).head(10).rows(),
        "markets": _markets(odds),
        "rows": odds.height,
        "final_games": finals,
        "coverage": ids.height / finals if finals else 0.0,
    }
    logger.info("sbr %s: %d/%d games matched (%.1f%% of %d finals), %d score mismatch(es), %d row(s)",
                season, ids.height, parsed.height, 100 * stats["coverage"], finals, mismatched.height, odds.height)
    return stats


def import_seasons(store: Store, games: pl.DataFrame, start_years: list[int]) -> dict[int, dict[str, Any]]:
    """Import SBR seasons (2010-11 on) into :func:`nhl.storage.keys.odds_history_sbr`.

    Workbooks come from the S3 cache when present; otherwise the CDX listing is read once
    and the missing files are downloaded (and cached).

    Args:
        store: S3 store.
        games: ``processed/games.parquet``.
        start_years: Season start years, e.g. ``[2010, 2011]``.

    Returns:
        Per start year, the stats from :func:`import_season` (or ``{"error": ...}``).
    """
    entries: dict[int, dict[str, Any]] | None = None
    out: dict[int, dict[str, Any]] = {}
    for year in start_years:
        if year < FIRST_SEASON:
            out[year] = {"error": f"games table starts in {FIRST_SEASON}; not imported"}
            continue
        data = store.get_bytes(raw_key(workbook_filename(year)))
        if data is None:
            if entries is None:
                entries = {e["start_year"]: e for e in list_archive_files()}
            if year not in entries:
                out[year] = {"error": "no archived workbook"}
                logger.warning("sbr %d: no archived workbook", year)
                continue
            data = download_workbook(store, entries[year])
        out[year] = import_season(store, games, year, data)
    return out
