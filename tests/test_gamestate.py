from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from conftest import A_GOALIE, A_SKATER, AWAY, H_D, H_GOALIE, H_SKATER, HOME
from nhl.gamestate.goalies import build_goalie_starts
from nhl.gamestate.lineups import build_lineups
from nhl.gamestate.logs import team_game_logs
from nhl.gamestate.rosters import parse_right_rail_coaches
from nhl.gamestate.stints import attribute_events, build_stints
from nhl.ingest.toi_html import parse_report, to_shift_chart
from nhl.transform.events import parse_events, roster_from_pbp
from nhl.transform.shifts import parse_shifts

GAME = 2024020001


@pytest.fixture
def game(raw_pbp, raw_shifts):
    events = parse_events(raw_pbp)
    shifts = parse_shifts(raw_shifts, roster_from_pbp(raw_pbp)).with_columns(pl.lit(GAME, pl.Int64).alias("game_id"))
    return events, shifts


def test_stint_boundaries_follow_shifts_faceoffs_and_goals(game):
    events, shifts = game
    st = build_stints(events, shifts)
    # Boundaries: 0 (faceoff), 12 (goal+faceoff), 20, 45 (goalie pulled), 50 (goal), 60, end.
    assert st.select("start_s", "end_s").rows()[:5] == [(0, 12), (12, 20), (20, 45), (45, 50), (50, 60)]
    first = st.row(0, named=True)
    assert first["home_skaters"] == [H_SKATER] and first["away_skaters"] == [A_SKATER]
    assert st.row(1, named=True)["home_skaters"] == [H_SKATER, H_D]
    assert st.row(3, named=True)["away_goalie"] is None


def test_stint_events_and_score(game):
    events, shifts = game
    st = build_stints(events, shifts)
    # The goal at 0:12 belongs to the stint ending then; the faceoff at 0:12 to the next.
    assert (st["home_gf"][0], st["home_cf"][0], st["home_fo_won"][0]) == (1, 2, 1)
    assert st["away_fo_won"][1] == 1
    assert (st["away_cf"][2], st["away_ff"][2], st["away_sf"][2]) == (3, 2, 1)
    assert st["away_gf"][3] == 1
    assert (st["home_score"][4], st["away_score"][4]) == (1, 1)
    # Every event lands in exactly one stint, with the same personnel as its on-ice list.
    attributed = attribute_events(st, events.filter(pl.col("event_type").is_in(["SHOT", "GOAL", "FACEOFF"])))
    assert attributed.height == 6
    assert attributed.filter(pl.col("event_idx") == 4)["stint_id"].item() == 1


def test_faceoff_context(game):
    events, shifts = game
    st = build_stints(events, shifts)
    assert st["start_type"][0] == "faceoff_unknown"  # the fixture's faceoffs carry no zone
    assert st["start_type"][2] == "on_the_fly"
    assert st["last_faceoff_s"][2] == 12
    assert st["home_changed_since_faceoff"].to_list()[:3] == [False, False, True]
    assert st["away_changed_since_faceoff"][2] is False


def _stint_frame(rows):
    """Minimal hand-built stints (one game, period 1) for the downstream builders."""
    base = {
        "game_id": GAME, "season": 20242025, "period": 1, "home_team_id": HOME, "away_team_id": AWAY,
        "home_goalie": H_GOALIE, "away_goalie": A_GOALIE, "start_type": "on_the_fly",
        **{f"{s}_{c}": 0 for s in ("home", "away") for c in ("cf", "ff", "sf", "gf", "pen_taken", "pim", "fo_won")},
        "home_xgf": 0.0, "away_xgf": 0.0,
    }
    out, t = [], 0
    for i, (dur, home, away, *extra) in enumerate(rows):
        row = {**base, "stint_id": i, "start_s": t, "end_s": t + dur, "duration_s": dur,
               "home_skaters": sorted(home), "away_skaters": sorted(away),
               "home_n": len(home), "away_n": len(away), **(extra[0] if extra else {})}
        out.append(row)
        t += dur
    return pl.DataFrame(out)


def test_post_penalty_shadow_needs_a_real_power_play(raw_pbp):
    events = parse_events(raw_pbp)
    # Shorthanded from a faceoff, then the penalty expires in play: shadow until next faceoff.
    sh = pl.DataFrame(
        {
            "player_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, H_GOALIE, A_GOALIE],
            "team_id": [HOME] * 5 + [AWAY] * 5 + [HOME, AWAY],
            "period": [1] * 12,
            "start": [0] * 9 + [120, 0, 0],  # away skater 10 leaves the box at 2:00
            "end": [300] * 12,
            "is_goalie": [False] * 10 + [True, True],
        },
        schema_overrides={"team_id": pl.Int32, "period": pl.Int8, "start": pl.Int32, "end": pl.Int32},
    ).with_columns(pl.lit(GAME, pl.Int64).alias("game_id"))
    fo = events.filter(pl.col("event_idx") == 1)
    fo2 = fo.with_columns(pl.lit(200, pl.Int32).alias("period_seconds"), pl.lit(99, pl.Int32).alias("event_idx"))
    st = build_stints(pl.concat([fo, fo2]), sh)
    assert st.select("start_s", "strength_state", "post_penalty_5v5").rows() == [
        (0, "5v4", False), (120, "5v5", True), (200, "5v5", False)
    ]


def test_lineups_recover_trios_and_pairs():
    f1, f2, d1 = [11, 12, 13], [14, 15, 16], [21, 22]
    stints = _stint_frame([
        (40, [*f1, *d1], [91, 92, 93, 94, 95]),
        (35, [*f2, *d1], [91, 92, 93, 94, 95]),
        (5, [11, 15, 16, *d1], [91, 92, 93, 94, 95]),
    ])
    rosters = pl.DataFrame({
        "game_id": [GAME] * 8, "team_id": [HOME] * 8, "player_id": [*f1, *f2, *d1],
        "is_forward": [True] * 6 + [False] * 2, "is_defense": [False] * 6 + [True] * 2,
    })
    lines = build_lineups(stints, rosters).filter(pl.col("team_id") == HOME)
    fwd = lines.filter(pl.col("unit_type") == "F").sort("unit_rank")
    assert fwd["members"].to_list() == [f1, f2]
    assert fwd["shared_toi_s"].to_list() == [40, 35]
    assert lines.filter(pl.col("unit_type") == "D")["members"].to_list() == [d1]


def test_goalie_starts_relief_and_freezes():
    stints = _stint_frame([
        (600, [1, 2, 3, 4, 5], [6, 7, 8, 9, 10]),
        (600, [1, 2, 3, 4, 5], [6, 7, 8, 9, 10], {"home_goalie": 198}),
    ])
    events = pl.DataFrame({
        "game_id": [GAME] * 3, "event_idx": [1, 2, 3], "season": [20242025] * 3,
        "game_date": [date(2024, 10, 8)] * 3, "home_team_id": [HOME] * 3,
        "event_type": ["SHOT", "STOPPAGE", "GOAL"], "reason": [None, "goalie-stopped-after-sog", None],
        "goalie_in_net_id": [H_GOALIE, None, H_GOALIE], "is_penalty_shot": [False] * 3,
    })
    gs = build_goalie_starts(stints, events).filter(pl.col("team_id") == HOME).row(0, named=True)
    assert gs["starter"] == H_GOALIE and gs["finished"] is False
    assert (gs["relief_goalie"], gs["relief_entry_s"], gs["pulled_at_s"]) == (198, 600, 600)
    assert (gs["shots_against"], gs["goals_against"], gs["saves"], gs["sog_frozen"]) == (2, 1, 1, 1)


def test_team_logs_partition_strengths(game):
    events, shifts = game
    logs = team_game_logs(build_stints(events, shifts), events)
    parts = logs.filter(~pl.col("strength").is_in(["all", "5v5"])).group_by("team_id").agg(pl.col("toi_s", "gf", "cf").sum())
    total = logs.filter(pl.col("strength") == "all").select("team_id", "toi_s", "gf", "cf")
    assert parts.sort("team_id").rows() == total.sort("team_id").rows()
    home_en = logs.filter((pl.col("team_id") == HOME) & (pl.col("strength") == "EN_opp"))
    assert home_en["toi_s"].item() > 0  # the away goalie was pulled at 0:45


TOI_HTML = """
<td align="center" valign="top" class="playerHeading + border" colspan="8">8 SMITH, JOHN</td>
<tr class="oddColor"><td>1</td><td>1</td><td>0:36 / 19:24</td><td>1:16 / 18:44</td><td>00:40</td><td>&nbsp;</td></tr>
<tr class="\tevenColor"><td>2</td><td>OT</td><td>2:00 / 3:00</td><td>2:45 / 2:15</td><td>00:45</td><td>G</td></tr>
<tr class="oddColor"><td>1</td><td>2</td><td>20.0</td><td>13:20</td><td>1</td><td>2</td><td>3</td></tr>
"""


def test_html_toi_report_parses_shifts_and_skips_summary():
    shifts = parse_report(TOI_HTML)
    assert shifts == [
        {"sweater": 8, "name": "SMITH, JOHN", "period": 1, "start": "0:36", "end": "1:16"},
        {"sweater": 8, "name": "SMITH, JOHN", "period": 4, "start": "2:00", "end": "2:45"},
    ]
    pbp = {"homeTeam": {"id": HOME}, "awayTeam": {"id": AWAY},
           "rosterSpots": [{"teamId": HOME, "sweaterNumber": 8, "playerId": 555}]}
    chart = to_shift_chart({"H": TOI_HTML, "V": TOI_HTML}, pbp)
    assert {r["playerId"] for r in chart["data"]} == {555}
    assert len(chart["unmatched"]) == 2  # sweater 8 isn't on the away roster


def test_right_rail_coaches():
    payload = {"gameInfo": {
        "homeTeam": {"headCoach": {"default": "Paul Maurice"}, "scratches": [{"id": 1}, {"id": 2}]},
        "awayTeam": {"headCoach": {"default": "Jeff  Blashill"}},
    }}
    df = parse_right_rail_coaches(payload, GAME)
    assert df.sort("is_home").rows() == [(GAME, False, "Jeff Blashill", []), (GAME, True, "Paul Maurice", [1, 2])]
