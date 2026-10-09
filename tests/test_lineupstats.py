"""Special-teams usage and team PP% / PK% behind the game page's lineups tab."""

import polars as pl
import pytest

from nhl.api import lineupstats


def test_special_teams_sums_units_per_player():
    units = pl.DataFrame({
        "team_id": [1, 1, 1],
        "player_ids": [[10, 11], [10, 12], [10, 11]],
        "toi_s": [120.0, 60.0, 30.0],
        "xgf": [0.4, 0.2, 0.0],
        "xga": [0.0, 0.0, 0.1],
        "kind": ["PP", "PP", "PK"],
        "team_toi_s": [200.0, 200.0, 100.0],
    })
    t = lineupstats.special_by_player(lineupstats.special_teams(units), [10, 11, 99])
    assert t[10]["pp"]["toi_s"] == 180
    assert t[10]["pp"]["share"] == pytest.approx(0.9)  # team time counted once per team
    assert t[10]["pp"]["xgf60"] == pytest.approx(0.6 * 3600 / 180)
    assert t[11]["pk"]["xga60"] == pytest.approx(12.0)
    assert t[99] == {"pp": None, "pk": None}


def test_team_special_teams_rates_and_ranks():
    # Team 1 scores 1 PP goal on 4 chances and allows 1 on 2; team 2 the reverse. Playoffs ignored.
    logs = pl.DataFrame({
        "game_id": [2025020001, 2025020001, 2025020001, 2025020001, 2025030001],
        "team_id": [1, 2, 1, 2, 1],
        "opp_team_id": [2, 1, 2, 1, 2],
        "strength": ["all", "all", "PP", "PP", "all"],
        "pp_opportunities": [4, 2, None, None, 9],
        "gf": [0, 0, 1, 1, 0],
        "ga": [0, 0, 0, 0, 0],
    })
    sh = pl.DataFrame({"game_id": [2025020001, 2025020001], "team_id": [1, 2], "opp_team_id": [2, 1],
                       "strength": ["SH", "SH"], "pp_opportunities": [None, None], "gf": [0, 0], "ga": [1, 1]})
    r = lineupstats.team_special_teams(pl.concat([logs, sh], how="vertical_relaxed"))
    assert r[1]["pp"] == {"pct": 0.25, "rank": 2, "goals": 1, "opps": 4, "teams": 2}
    assert r[2]["pp"]["pct"] == 0.5 and r[2]["pp"]["rank"] == 1
    assert r[1]["pk"]["pct"] == pytest.approx(0.5) and r[1]["pk"]["rank"] == 2
    assert r[2]["pk"]["pct"] == pytest.approx(0.75)


def test_spread_ignores_missing():
    assert lineupstats.spread([None, 1.0, -2.0, float("nan")], q=1.0) == 2.0
    assert lineupstats.spread([]) == 1.0
