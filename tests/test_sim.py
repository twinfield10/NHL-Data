from __future__ import annotations

import numpy as np
import polars as pl

from nhl.sim import engine, markets
from nhl.sim.inputs import SeasonInputs

CONSTANTS = {
    "goals60_5v5": 2.5, "goals60_pp": 7.0, "goals60_sh": 1.0, "goals60_4v4": 3.0, "goals60_3v3": 6.0,
    "goals60_en_own": 7.0, "goals60_en_opp": 17.0, "penalty_rate60": 0.0, "penalty_mix": {"2": 1.0},
    "shootout_home_win": 0.5, "pull_hazard": {"1": [0.0] * 40, "2": [0.0] * 40, "3": [0.0] * 40}, "pull_bucket_s": 15,
}


def _inputs(n_games: int, home5: float, away5: float) -> SeasonInputs:
    games = pl.DataFrame({
        "game_id": list(range(n_games)), "season_type": ["R"] * n_games,
        "home_team_id": [1] * n_games, "away_team_id": [2] * n_games,
    })
    g = {}
    for side, five in (("home", home5), ("away", away5)):
        g[f"5v5_{side}"] = np.full(n_games, five)
        for st, key in (("pp", "goals60_pp"), ("sh", "goals60_sh"), ("4v4", "goals60_4v4"), ("3v3", "goals60_3v3"),
                        ("en_own", "goals60_en_own"), ("en_opp", "goals60_en_opp")):
            g[f"{st}_{side}"] = np.full(n_games, CONSTANTS[key])
    return SeasonInputs(
        games=games, goals60=g, penalties60={"home": np.zeros(n_games), "away": np.zeros(n_games)},
        score_terms=np.zeros((7, 3)), xg60_5v5={"home": np.full(n_games, home5), "away": np.full(n_games, away5)},
    )


def test_equal_teams_split_and_scoring_rate():
    res = engine.simulate(_inputs(4, 2.5, 2.5), CONSTANTS, 20232024, n_sims=4000, seed=1)
    p = markets.prices(res)
    assert abs(p["p_home_win"].mean() - 0.5) < 0.02
    regulation = res.ended == 0
    # 2.5 goals/60 per team over 60 minutes -> ~5 goals per regulation game.
    assert abs((res.home + res.away)[regulation].mean() - 5.0) < 0.25


def test_stronger_team_wins_more_and_scales_apply():
    inp = _inputs(4, 3.5, 2.0)
    base = markets.prices(engine.simulate(inp, CONSTANTS, 20232024, n_sims=3000, seed=2))["p_home_win"].mean()
    stretched = markets.prices(engine.simulate(inp, CONSTANTS, 20232024, n_sims=3000, seed=2, scale=1.5))["p_home_win"].mean()
    flat = markets.prices(engine.simulate(inp, CONSTANTS, 20232024, n_sims=3000, seed=2, scale=0.0))["p_home_win"].mean()
    assert base > 0.6 and stretched > base and abs(flat - 0.5) < 0.03


def test_shootout_adds_one_goal_and_everyone_has_a_winner():
    inp = _inputs(2, 0.01, 0.01)
    for side in ("home", "away"):
        inp.goals60[f"3v3_{side}"] = np.full(2, 0.01)
    res = engine.simulate(inp, {**CONSTANTS, "goals60_3v3": 0.01}, 20232024, n_sims=500, seed=3)
    assert (res.home != res.away).all()
    so = res.ended == 2
    assert so.mean() > 0.9 and (np.abs(res.home - res.away)[so] == 1).all()


def testsum_before_is_strict_and_covers_future_dates():
    from datetime import date

    from nhl.sim.inputs import sum_before

    daily = pl.DataFrame({
        "team_id": [1, 1, 2], "game_date": [date(2026, 10, 1), date(2026, 10, 3), date(2026, 10, 2)], "x": [1.0, 2.0, 5.0],
    })
    left = pl.DataFrame({"game_date": [date(2026, 10, 9), date(2026, 10, 3), date(2026, 10, 1)], "team_id": [1, 1, 2]})
    out = sum_before(left, daily, ["x"], by="team_id")
    # Row order kept; same-day games excluded; a date with no row of its own sums everything before.
    assert out["game_date"].to_list() == left["game_date"].to_list()
    assert out["x_td"].to_list() == [3.0, 1.0, 0.0]
