"""Teams, franchises and lineages, and resolving any team label to the right team.

The curated table lives in ``src/nhl/reference/teams.csv`` (one row per NHL ``team_id``
that appears in our data). It carries three different groupings because they answer
different questions:

* ``team_id`` / ``tricode``: the team as it existed in a given season. The games table
  uses these, so ``PHX`` (team 27) for 2010-11..2013-14 and ``ARI`` (53) afterwards,
  ``UTA`` is team 59 (Utah Hockey Club) in 2024-25 and 68 (Utah Mammoth) from 2025-26.
* ``franchise_id``: the NHL's official franchise. Phoenix and Arizona share franchise 28;
  Atlanta and Winnipeg share 35; Utah is franchise 40 — officially a new franchise,
  not a Coyotes relocation.
* ``lineage_id``: our continuity key for modeling. Utah received Arizona's players and
  hockey operations, so 27 → 53 → 59 → 68 share lineage ``UTA`` even though the
  franchise changed; priors and roster history follow lineage.

Labels are matched against tricodes, full names, place names, common names and the
``aliases`` column (book, ESPN and DailyFaceoff spellings). Full era-specific names
resolve to exactly one team; ambiguous labels ("Coyotes", "UTA", "Utah") use the season
when given, otherwise the most recent team.

:func:`validate_against_api` flags any NHL team id or franchise change we haven't
curated — the guard against a future expansion or relocation silently mis-resolving.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

TEAMS_CSV = Path(__file__).parent / "reference" / "teams.csv"


@dataclass(frozen=True)
class Team:
    """One NHL team identity (a ``team_id``) and the seasons it played."""

    team_id: int
    tricode: str
    full_name: str
    place_name: str
    common_name: str
    franchise_id: int
    lineage_id: str
    first_season: int
    last_season: int | None
    aliases: tuple[str, ...]
    notes: str

    def active_in(self, season: int) -> bool:
        """Whether this team played in the given 8-digit season."""
        return self.first_season <= season and (self.last_season is None or season <= self.last_season)


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", text.lower())


@lru_cache(maxsize=1)
def load_teams() -> tuple[Team, ...]:
    """Read the curated team table (cached)."""
    with TEAMS_CSV.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return tuple(
        Team(
            team_id=int(r["team_id"]),
            tricode=r["tricode"],
            full_name=r["full_name"],
            place_name=r["place_name"],
            common_name=r["common_name"],
            franchise_id=int(r["franchise_id"]),
            lineage_id=r["lineage_id"],
            first_season=int(r["first_season"]),
            last_season=int(r["last_season"]) if r["last_season"] else None,
            aliases=tuple(a for a in r["aliases"].split("|") if a),
            notes=r["notes"],
        )
        for r in rows
    )


@lru_cache(maxsize=1)
def _index() -> tuple[dict[str, tuple[Team, ...]], dict[str, Team]]:
    """(label -> candidate teams, exact full-name -> team)."""
    labels: dict[str, list[Team]] = {}
    exact: dict[str, Team] = {}
    for t in load_teams():
        exact[_norm(t.full_name)] = t
        names = {t.tricode, t.full_name, t.place_name, t.common_name, f"{t.place_name} {t.common_name}",
                 t.full_name.replace(" ", "-"), *t.aliases}
        for name in names:
            labels.setdefault(_norm(name), [])
            if t not in labels[_norm(name)]:
                labels[_norm(name)].append(t)
    return {k: tuple(v) for k, v in labels.items()}, exact


def season_for(moment: date | datetime | str | int | None) -> int | None:
    """8-digit season id for a date (seasons roll over on July 1), or pass an id through."""
    if moment is None:
        return None
    if isinstance(moment, int):
        return moment if moment > 10_000_000 else moment * 10000 + moment + 1
    if isinstance(moment, str):
        moment = date.fromisoformat(moment[:10])
    if isinstance(moment, datetime):
        moment = moment.date()
    start = moment.year if moment.month >= 7 else moment.year - 1
    return start * 10000 + start + 1


def resolve(label: str | None, season: date | datetime | str | int | None = None) -> Team | None:
    """Resolve any team label to the :class:`Team` it meant.

    Args:
        label: Team label from any source (name, tricode, ESPN code, slug...).
        season: 8-digit season id, season start year, or a date. Picks among teams
            sharing a label (e.g. "Coyotes", "UTA"). Without it the most recent wins.

    Returns:
        The team, or None if the label is unknown or no candidate played that season.
    """
    if not label:
        return None
    labels, exact = _index()
    key = _norm(label)
    if key in exact:
        return exact[key]
    candidates = labels.get(key)
    if not candidates:
        return None
    sid = season_for(season)
    if sid is not None:
        active = [t for t in candidates if t.active_in(sid)]
        if not active:
            return None
        candidates = tuple(active)
    lineages = {t.lineage_id for t in candidates}
    if len(lineages) > 1 and sid is None:
        return None  # e.g. "New York": genuinely ambiguous without more context
    return max(candidates, key=lambda t: (t.last_season or 99999999, t.first_season))


def resolve_team(label: str | None, season: date | datetime | str | int | None = None) -> str | None:
    """Return the NHL tricode (as used in that season) for any team label, or None.

    Backwards compatible: ``resolve_team("Tampa Bay Lightning") == "TBL"``. Pass
    ``season`` whenever the label may be historical ("Phoenix" -> ``PHX``; "Coyotes"
    in 2012-13 -> ``PHX``, in 2016-17 -> ``ARI``).
    """
    team = resolve(label, season)
    return team.tricode if team else None


def resolve_team_id(label: str | None, season: date | datetime | str | int | None = None) -> int | None:
    """Like :func:`resolve_team` but returns the NHL ``team_id``."""
    team = resolve(label, season)
    return team.team_id if team else None


def by_id(team_id: int) -> Team | None:
    """Look up a team by NHL ``team_id``."""
    return next((t for t in load_teams() if t.team_id == team_id), None)


def lineage_of(team_id: int) -> str | None:
    """Continuity key used for priors across renames, relocations and the ARI→UTA transfer."""
    team = by_id(team_id)
    return team.lineage_id if team else None


def validate_against_api(api_teams: list[dict], games_team_ids: set[int] | None = None) -> list[str]:
    """Compare the curated table with the NHL ``/stats/rest/en/team`` list.

    Args:
        api_teams: Records from :meth:`nhl.ingest.http.NHLClient.teams`.
        games_team_ids: Team ids appearing in the games table (every one must be curated).

    Returns:
        Human-readable problems; empty when everything is consistent.
    """
    curated = {t.team_id: t for t in load_teams()}
    api = {int(t["id"]): t for t in api_teams}
    problems = []
    for tid in sorted(games_team_ids or set()):
        if tid not in curated:
            name = api.get(tid, {}).get("fullName", "?")
            problems.append(f"team_id {tid} ({name}) appears in games but is not in teams.csv — add a row")
    for tid, t in curated.items():
        a = api.get(tid)
        if a is None:
            problems.append(f"team_id {tid} ({t.full_name}) not in the NHL team list")
            continue
        if a.get("triCode") != t.tricode:
            problems.append(f"team_id {tid}: tricode {t.tricode} in csv, {a.get('triCode')} in API")
        if a.get("franchiseId") != t.franchise_id:
            problems.append(f"team_id {tid}: franchise {t.franchise_id} in csv, {a.get('franchiseId')} in API")
    return problems


#: Backwards-compatible view: tricode -> names for teams active today.
TEAMS: dict[str, tuple[str, ...]] = {
    t.tricode: (t.full_name, *t.aliases) for t in load_teams() if t.last_season is None
}
