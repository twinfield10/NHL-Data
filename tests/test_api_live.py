"""Live scores route parsing (nhl.api.routers.live)."""

from __future__ import annotations

from datetime import date

from nhl.api.routers import live


def _game(state, period=None, kind="REG", clock="05:00", intermission=False, outcome=None):
    return {"id": 2026020072, "gameState": state, "startTimeUTC": "2026-10-10T20:00:00Z",
            "periodDescriptor": {"number": period, "periodType": kind} if period else {},
            "clock": {"timeRemaining": clock, "inIntermission": intermission},
            "homeTeam": {"abbrev": "SJS", "score": 0, "sog": 12}, "awayTeam": {"abbrev": "EDM", "score": 1, "sog": 8},
            "gameOutcome": outcome, "gameCenterLink": "/gamecenter/edm-vs-sjs/2026/10/10/2026020072"}


def test_parse_score_states_and_details():
    rows = live.parse_score({"games": [
        _game("FUT"), _game("LIVE", 2, clock="12:34"), _game("CRIT", 2, intermission=True),
        _game("LIVE", 4, kind="OT", clock="03:10"), _game("OFF", 5, kind="SO", outcome={"lastPeriodType": "SO"}),
        _game("FINAL", 3, outcome={"lastPeriodType": "REG"}),
    ]})
    assert [(r["state"], r["detail"]) for r in rows] == [
        ("pre", None), ("live", "12:34 2nd"), ("live", "2nd INT"), ("live", "03:10 OT"), ("final", "Final/SO"), ("final", "Final"),
    ]
    assert rows[1]["gamecenter_url"] == "https://www.nhl.com/gamecenter/edm-vs-sjs/2026/10/10/2026020072"
    assert (rows[1]["away_abbr"], rows[1]["away_score"], rows[1]["home_sog"]) == ("EDM", 1, 12)


def test_parse_espn_resolves_espn_codes():
    payload = {"events": [{"id": "401892471", "competitions": [{"competitors": [
        {"homeAway": "home", "team": {"abbreviation": "SJ"}}, {"homeAway": "away", "team": {"abbreviation": "EDM"}}]}]}]}
    assert live.parse_espn(payload, date(2026, 10, 10)) == {("EDM", "SJS"): "https://www.espn.com/nhl/game/_/gameId/401892471"}


def test_parse_boxscore_prop_stats():
    payload = {"id": 1, "gameState": "LIVE", "awayTeam": {"abbrev": "EDM"}, "homeTeam": {"abbrev": "SJS"},
               "playerByGameStats": {
                   "awayTeam": {"forwards": [{"playerId": 97, "position": "C", "goals": 1, "assists": 1, "points": 2, "sog": 4,
                                              "blockedShots": 0}],
                                "goalies": [{"playerId": 74, "position": "G", "saves": 12}]},
                   "homeTeam": {"defense": [{"playerId": 8, "position": "D", "goals": 0, "assists": 0, "points": 0, "sog": 1,
                                             "blockedShots": 3}]}}}
    out = live.parse_boxscore(payload)
    by_id = {p["player_id"]: p for p in out["players"]}
    assert out["state"] == "live"
    assert (by_id[97]["points"], by_id[97]["shots"], by_id[97]["saves"]) == (2, 4, None)
    assert (by_id[74]["saves"], by_id[8]["blocks"], by_id[8]["team"]) == (12, 3, "SJS")
