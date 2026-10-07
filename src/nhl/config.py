"""Project-wide settings, read once from the environment (and an optional ``.env``)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

S3_BUCKET: str = os.getenv("NHL_S3_BUCKET", "tmw-nhl-data")
S3_REGION: str = os.getenv("NHL_S3_REGION", "us-east-2")
CACHE_DIR: Path = Path(os.getenv("NHL_CACHE_DIR", REPO_ROOT / "data" / "cache"))
API_RPS: float = float(os.getenv("NHL_API_RPS", "8"))

#: 4Casters account. The exchange stopped serving its order book anonymously on 2026-10-06
#: ("Sign in to read the board", ANON_READ_REFUSED); without these the source is skipped.
CAST4_USER: str | None = os.getenv("CAST4_USER") or None
CAST4_PASS: str | None = os.getenv("CAST4_PASS") or None

#: First season (by start year) with usable play-by-play coordinates and shift charts.
FIRST_SEASON: int = 2010

#: NHL API game types we keep: regular season and playoffs.
GAME_TYPES: dict[int, str] = {2: "R", 3: "P"}

#: ``gameStateId`` in the stats API meaning the game is final.
FINAL_GAME_STATE: int = 7


def season_id(start_year: int) -> int:
    """Convert a season start year to the NHL's 8-digit id.

    Args:
        start_year: Calendar year the season starts in, e.g. 2024.

    Returns:
        Season id such as ``20242025``.
    """
    return start_year * 10000 + start_year + 1


def season_start_year(sid: int) -> int:
    """Inverse of :func:`season_id`: ``20242025 -> 2024``."""
    return sid // 10000


def parse_seasons(spec: str) -> list[int]:
    """Parse a CLI season spec into a list of start years.

    Accepts ``"2024"``, ``"2010-2025"`` (inclusive) or comma lists like ``"2019,2021"``.

    Args:
        spec: The season specification string.

    Returns:
        Sorted list of season start years.
    """
    years: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = (int(p) for p in part.split("-"))
            years.update(range(lo, hi + 1))
        elif part:
            years.add(int(part))
    return sorted(years)
