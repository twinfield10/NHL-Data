"""Polars -> JSON helpers shared by the routers."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
from fastapi import Response

EASTERN = ZoneInfo("America/New_York")


def today_et() -> date:
    """Today's date in Eastern time (game dates are Eastern)."""
    return datetime.now(EASTERN).date()


def rows(df: pl.DataFrame | None, drop: tuple[str, ...] = ()) -> list[dict]:
    """Frame -> list of dicts, with NaN floats as null (JSON has no NaN)."""
    if df is None or df.is_empty():
        return []
    df = df.drop([c for c in drop if c in df.columns])
    floats = [c for c, t in df.schema.items() if t in (pl.Float32, pl.Float64)]
    if floats:
        df = df.with_columns(pl.col(floats).fill_nan(None))
    return df.to_dicts()


def selection(market: str, side: int, line: float | None, home: str, away: str) -> str:
    """Human label for a bet side: ``STL``, ``SEA +1.5``, ``Over 5.5``."""
    team = home if side == 1 else away
    if market == "moneyline":
        return team
    if market == "puckline":
        return f"{team} {(line if side == 1 else -line):+.1f}"
    return f"{'Over' if side == 1 else 'Under'} {line:g}"


def with_selection(df: pl.DataFrame, home: str = "home_abbr", away: str = "away_abbr") -> pl.DataFrame:
    """Add a ``selection`` label column to edge or ledger rows."""
    labels = [selection(m, s, ln, h, a) for m, s, ln, h, a in
              zip(df["market"], df["side"], df["line"], df[home], df[away])]
    return df.with_columns(pl.Series("selection", labels, dtype=pl.String))


def starts_utc(games: pl.DataFrame) -> pl.DataFrame:
    """``game_id, start_utc`` from the catalog's Eastern start times."""
    return games.filter(pl.col("start_time_et").is_not_null()).select(
        "game_id", pl.col("start_time_et").str.to_datetime().dt.replace_time_zone(str(EASTERN))
        .dt.convert_time_zone("UTC").alias("start_utc"))


def json_view(raw: bytes) -> Response:
    """A prebuilt view (:mod:`nhl.site.views`) sent as stored, with no re-encoding."""
    return Response(content=raw, media_type="application/json")
