"""Fallback shift source: the NHL's per-game HTML time-on-ice reports.

Some games have an empty API shift chart (57 late in 2024-25). The NHL still publishes
HTML time-on-ice reports for every game:

* ``https://www.nhl.com/scores/htmlreports/{season}/TH{nnnnnn}.HTM`` (home);
* ``.../TV{nnnnnn}.HTM`` (visitor).

Each lists every shift per player (sweater number and name) with period, start and end
(elapsed in period). They are stored verbatim under ``raw/toi_html/{season}/``, then
converted to the API's shift-chart shape (:func:`to_shift_chart`), so
:func:`nhl.transform.shifts.parse_shifts` handles both sources identically. Players are
matched by sweater number and team through the play-by-play ``rosterSpots``.
"""

from __future__ import annotations

import gzip
import html
import logging
import re
from typing import Any

import polars as pl

from nhl.ingest.http import NHLClient
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

REPORT_URL = "https://www.nhl.com/scores/htmlreports/{season}/T{side}{suffix}.HTM"
SIDES = {"H": True, "V": False}  # report letter -> is_home

_PLAYER = re.compile(r'class="playerHeading[^"]*"[^>]*>\s*(\d+)\s+([^<]+)</td>')
_ROW = re.compile(r'<tr class="\s*(?:odd|even)Color">(.*?)</tr>', re.S)
_CELL = re.compile(r"<td[^>]*>(.*?)</td>", re.S)


def raw_key(season: int, game_id: int, side: str) -> str:
    """Key for one stored HTML report (``side`` is ``H`` or ``V``)."""
    return f"raw/toi_html/{season}/{game_id}_{side}.html.gz"


def raw_prefix(season: int) -> str:
    """Prefix holding a season's stored HTML reports."""
    return f"raw/toi_html/{season}/"


def report_url(season: int, game_id: int, side: str) -> str:
    """URL of a game's home (``H``) or visitor (``V``) time-on-ice report."""
    return REPORT_URL.format(season=season, side=side, suffix=str(game_id)[4:])


def fetch_reports(store: Store, client: NHLClient, season: int, game_id: int) -> None:
    """Download and store both HTML reports for a game.

    Raises:
        requests.HTTPError: If a report is unavailable.
    """
    for side in SIDES:
        client.limiter.wait()
        resp = client.session.get(report_url(season, game_id, side), timeout=client.timeout)
        resp.raise_for_status()
        store.put_bytes(raw_key(season, game_id, side), gzip.compress(resp.content), content_type="application/gzip")


def load_reports(store: Store, season: int, game_id: int) -> dict[str, str] | None:
    """Both stored reports as text, or None if either is missing."""
    out = {}
    for side in SIDES:
        data = store.get_bytes(raw_key(season, game_id, side))
        if data is None:
            return None
        out[side] = gzip.decompress(data).decode("utf-8", errors="replace")
    return out


def _period(label: str) -> int | None:
    label = label.strip().upper()
    if label.isdigit():
        return int(label)
    match = re.fullmatch(r"OT(\d*)", label)
    if match:
        return 3 + int(match.group(1) or 1)
    return None


def parse_report(text: str) -> list[dict[str, Any]]:
    """Shifts in one HTML report.

    Returns:
        ``{"sweater", "name", "period", "start", "end"}`` per shift, ``start``/``end`` as
        ``m:ss`` elapsed in the period (the shift chart's format).
    """
    shifts: list[dict[str, Any]] = []
    headings = list(_PLAYER.finditer(text))
    for i, head in enumerate(headings):
        block = text[head.end(): headings[i + 1].start() if i + 1 < len(headings) else len(text)]
        sweater, name = int(head.group(1)), html.unescape(head.group(2)).strip()
        for row in _ROW.finditer(block):
            cells = [re.sub(r"<[^>]+>", "", c).replace("&nbsp;", "").strip() for c in _CELL.findall(row.group(1))]
            # Shift rows show "elapsed / game" times; the per-period summary rows don't.
            if len(cells) < 5 or not cells[0].isdigit() or "/" not in cells[2] or "/" not in cells[3]:
                continue
            period = _period(cells[1])
            start, end = cells[2].split("/")[0].strip(), cells[3].split("/")[0].strip()
            if period is None or ":" not in start or ":" not in end:
                continue
            shifts.append({"sweater": sweater, "name": name, "period": period, "start": start, "end": end})
    return shifts


def to_shift_chart(reports: dict[str, str], pbp: dict[str, Any]) -> dict[str, Any]:
    """Convert a game's two HTML reports into the API ``/shiftcharts`` payload shape.

    Args:
        reports: ``{"H": html, "V": html}``.
        pbp: The game's raw play-by-play (for ``rosterSpots`` and team ids).

    Returns:
        ``{"data": [...], "source": "html", "unmatched": [...]}``. Shifts of players whose
        sweater number isn't on that team's roster are listed in ``unmatched``.
    """
    team_ids = {True: pbp["homeTeam"]["id"], False: pbp["awayTeam"]["id"]}
    by_sweater = {
        (s["teamId"], s.get("sweaterNumber")): s["playerId"] for s in pbp.get("rosterSpots") or []
    }
    data, unmatched = [], []
    for side, text in reports.items():
        team = team_ids[SIDES[side]]
        for shift in parse_report(text):
            player = by_sweater.get((team, shift["sweater"]))
            if player is None:
                unmatched.append({"team_id": team, **shift})
                continue
            data.append(
                {
                    "typeCode": 517, "duration": "html", "playerId": player, "teamId": team,
                    "period": shift["period"], "startTime": shift["start"], "endTime": shift["end"],
                }
            )
    return {"data": data, "source": "html", "unmatched": unmatched}


def compare_with_api(api_shifts: pl.DataFrame, html_shifts: pl.DataFrame, tolerance_s: int = 1) -> dict[str, float]:
    """Agreement between merged API shifts and merged HTML shifts for the same game(s).

    Args:
        api_shifts: :func:`nhl.transform.shifts.parse_shifts` output from the API chart
            (with ``game_id``).
        html_shifts: The same from :func:`to_shift_chart` (with ``game_id``).
        tolerance_s: Allowed start/end difference.

    Returns:
        ``api_shifts``, ``html_shifts``, ``matched_share`` (API shifts with an HTML shift
        for the same player and period whose start and end are within tolerance) and
        ``toi_diff_s_p99`` (99th percentile of per-player TOI differences).
    """
    k = ["game_id", "player_id", "period"]
    pairs = api_shifts.join(html_shifts, on=k, how="left", suffix="_h").with_columns(
        (((pl.col("start") - pl.col("start_h")).abs() <= tolerance_s)
         & ((pl.col("end") - pl.col("end_h")).abs() <= tolerance_s)).fill_null(False).alias("ok")
    )
    matched = pairs.group_by(*k, "start", "end").agg(pl.col("ok").any())

    def toi(df: pl.DataFrame) -> pl.DataFrame:
        return df.group_by("game_id", "player_id").agg((pl.col("end") - pl.col("start")).sum().alias("toi"))

    diff = toi(api_shifts).join(toi(html_shifts), on=["game_id", "player_id"], how="full", coalesce=True).with_columns(
        (pl.col("toi").fill_null(0) - pl.col("toi_right").fill_null(0)).abs().alias("d")
    )
    return {
        "api_shifts": float(api_shifts.height),
        "html_shifts": float(html_shifts.height),
        "matched_share": float(matched["ok"].mean()),
        "toi_diff_s_p99": float(diff["d"].quantile(0.99)),
    }
