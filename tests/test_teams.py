from __future__ import annotations

from datetime import date

import pytest

from nhl.teams import TEAMS, by_id, lineage_of, load_teams, resolve_team, resolve_team_id, validate_against_api


@pytest.mark.parametrize("label,season,expected", [
    ("Phoenix Coyotes", None, "PHX"),
    ("Phoenix", 20122013, "PHX"),
    ("Coyotes", 20122013, "PHX"),
    ("Coyotes", 20162017, "ARI"),
    ("Arizona Coyotes", None, "ARI"),
    ("ARI", 2015, "ARI"),
    ("Utah Hockey Club", None, "UTA"),
    ("Atlanta Thrashers", 20102011, "ATL"),
    ("Winnipeg", 20102011, None),        # the Jets didn't exist yet
    ("Tampa Bay Lightning", None, "TBL"),
    ("tampa-bay-lightning", None, "TBL"),
    ("TB", 20262027, "TBL"),
    ("Montréal Canadiens", None, "MTL"),
    ("NY Rangers", None, "NYR"),
    ("New York", None, None),            # ambiguous without more context
])
def test_resolve_team(label, season, expected):
    assert resolve_team(label, season) == expected


def test_utah_team_ids_by_season():
    assert resolve_team_id("UTA", 20242025) == 59
    assert resolve_team_id("UTA", date(2026, 1, 15)) == 68
    assert resolve_team_id("Utah", None) == 68                 # latest when no season
    assert resolve_team_id("Utah Hockey Club", 20252026) == 59  # exact names are era-specific


def test_franchise_and_lineage():
    assert by_id(27).franchise_id == by_id(53).franchise_id == 28    # PHX -> ARI rename
    assert by_id(59).franchise_id == by_id(68).franchise_id == 40    # Utah is a new franchise
    assert lineage_of(27) == lineage_of(53) == lineage_of(59) == lineage_of(68) == "UTA"
    assert lineage_of(11) == lineage_of(52) == "WPG"


def test_seasons_do_not_overlap_within_lineage():
    teams = load_teams()
    for lineage in {t.lineage_id for t in teams}:
        spans = sorted((t.first_season, t.last_season or 99999999) for t in teams if t.lineage_id == lineage)
        for (_, end), (start, _) in zip(spans, spans[1:]):
            assert end < start, lineage


def test_validate_flags_unknown_team():
    api = [{"id": t.team_id, "triCode": t.tricode, "franchiseId": t.franchise_id} for t in load_teams()]
    assert validate_against_api(api, {t.team_id for t in load_teams()}) == []
    problems = validate_against_api(api + [{"id": 99, "triCode": "NEW", "franchiseId": 41, "fullName": "Expansion"}], {99})
    assert any("99" in p for p in problems)


def test_active_teams_compat():
    assert len(TEAMS) == 32 and "UTA" in TEAMS and "ARI" not in TEAMS
