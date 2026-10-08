"""Phase B props projection: team goal distributions, player shares, tail probabilities (no network)."""

from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl
import pytest
from scipy.stats import binom

from nhl.props import project, rates
from nhl.sim.markets import MAX_GOALS

K = MAX_GOALS + 1


def matrix(cells: dict[tuple[int, int, int], float]) -> list[float]:
    """A flattened score matrix from {(ended, home, away): p}."""
    m = np.zeros((3, K, K))
    for (e, h, a), p in cells.items():
        m[e, h, a] = p
    return list(m.ravel())


def history(cells: dict[tuple[int, int, int], float]) -> pl.DataFrame:
    return pl.DataFrame({"game_id": [1], "home_team_id": [10], "away_team_id": [20], "score_matrix": [matrix(cells)]})


def test_goal_dists_regulation() -> None:
    d = project.team_goal_dists(history({(0, 3, 1): 0.5, (0, 2, 2): 0.0, (1, 2, 1): 0.5}))
    home = d.filter(pl.col("team_id") == 10)
    assert home["dist"][0].to_list()[2:4] == pytest.approx([0.5, 0.5])
    assert home["mean_goals"][0] == pytest.approx(2.5)
    assert d.filter(pl.col("team_id") == 20)["mean_goals"][0] == pytest.approx(1.0)


def test_goal_dists_take_back_the_shootout_goal() -> None:
    # 3-2 home shootout win: both teams scored 2; the booked third home goal is nobody's.
    d = project.team_goal_dists(history({(2, 3, 2): 0.6, (2, 1, 2): 0.4}))  # 1-2: away won the shootout 1-1
    home = dict(zip(d["team_id"], d["dist"].to_list(), strict=True))[10]
    away = dict(zip(d["team_id"], d["dist"].to_list(), strict=True))[20]
    assert home[2] == pytest.approx(0.6) and home[1] == pytest.approx(0.4) and home[3] == pytest.approx(0.0)
    assert away[2] == pytest.approx(0.6) and away[1] == pytest.approx(0.4)
    assert sum(home) == pytest.approx(1.0) and sum(away) == pytest.approx(1.0)


def deployment() -> pl.DataFrame:
    # Two skaters on one team: equal ice time everywhere.
    return pl.DataFrame({"game_id": [1, 1], "team_id": [10, 10], "player_id": [1, 2], "position": ["C", "D"],
                         "s5": [2.5, 2.5], "spp": [2.5, 2.5], "spk": [2.0, 2.0]})


def rate_rows(g60: tuple[float, float], a60: tuple[float, float]) -> pl.DataFrame:
    return pl.DataFrame({"game_id": [1, 1], "player_id": [1, 2],
                         **{f"g60_{b}": list(g60) for b in rates.BUCKET_NAMES},
                         **{f"a60_{b}": list(a60) for b in rates.BUCKET_NAMES}})


MIX = {"f": {"ev": 0.8, "pp": 0.18, "sh": 0.02}, "apg": {"ev": 1.6, "pp": 1.8, "sh": 1.0}}


def test_shares_split_team_goals() -> None:
    sh = project.shares(deployment(), rate_rows((3.0, 1.0), (1.0, 1.0)), MIX)
    p = dict(zip(sh["player_id"], sh["p_goal"], strict=True))
    assert p[1] + p[2] == pytest.approx(1.0)  # every team goal has a scorer
    assert p[1] == pytest.approx(0.75)  # three times the rate at equal ice time
    apg = sum(MIX["f"][b] * MIX["apg"][b] for b in MIX["f"])
    assert sh["p_assist"].sum() == pytest.approx(apg)
    assert sh["p_point"].to_list() == pytest.approx((sh["p_goal"] + sh["p_assist"]).clip(0, 1).to_list())


def test_share_conditional_on_dressing() -> None:
    dep = deployment().with_columns(pl.Series("p_dressed", [1.0, 0.5]))
    sh = project.shares(dep, rate_rows((1.0, 1.0), (1.0, 1.0)), MIX)
    p = dict(zip(sh["player_id"], sh["p_goal"], strict=True))
    # Player 2 counts half for his teammate, but his own share assumes he plays.
    assert p[1] == pytest.approx(1 / 1.5) and p[2] == pytest.approx(1 / 1.5)


def test_missing_rates_take_group_mean() -> None:
    dep = deployment().vstack(pl.DataFrame({"game_id": [1], "team_id": [10], "player_id": [3], "position": ["L"],
                                            "s5": [0.0], "spp": [0.0], "spk": [0.0]}))
    sh = project.shares(dep, rate_rows((2.0, 1.0), (1.0, 1.0)), MIX)
    assert sh.filter(pl.col("player_id") == 3)["p_goal"][0] == 0.0  # no ice time, no goals
    assert sh["p_goal"].null_count() == 0


def test_tail_probs_mixture() -> None:
    dist = np.zeros((1, K))
    dist[0, 2], dist[0, 4] = 0.5, 0.5
    out = project.tail_probs(np.array([0.3]), dist, (1, 2))
    assert out[1][0] == pytest.approx(0.5 * (1 - 0.7**2) + 0.5 * (1 - 0.7**4))
    assert out[2][0] == pytest.approx(0.5 * binom.sf(1, 2, 0.3) + 0.5 * binom.sf(1, 4, 0.3))


def test_project_players_expected_counts() -> None:
    sh = project.shares(deployment(), rate_rows((3.0, 1.0), (1.0, 1.0)), MIX)
    dists = project.poisson_goal_dists(pl.DataFrame({"game_id": [1], "team_id": [10]}), 3.0)
    out = project.project_players(sh, dists)
    assert out["exp_goals"].sum() == pytest.approx(3.0)
    assert {f"p_points_{k}" for k in (1, 2, 3)} <= set(out.columns)
    assert (out["p_points_1"] >= out["p_goals_1"]).all()


def test_season_rates_point_in_time() -> None:
    def logs(season: int, days: list[date], goals: list[float]) -> pl.DataFrame:
        n = len(days)
        base = {"game_id": list(range(season, season + n)), "game_date": days, "season": [season] * n,
                "player_id": [7] * n, "team_id": [10] * n, "grp": ["F"] * n}
        cols = {f"{s}_{b}": [0.0] * n for s in rates.STATS for b in rates.BUCKET_NAMES}
        cols["toi_ev"], cols["ixg_ev"], cols["goals_ev"] = [15.0] * n, [0.3] * n, goals
        return pl.DataFrame({**base, **cols})

    last = logs(20232024, [date(2024, 1, d) for d in (1, 2, 3, 4)], [0.0, 0.0, 0.0, 1.0])
    cur = logs(20242025, [date(2024, 11, d) for d in (1, 2, 3)], [3.0, 0.0, 0.0])
    empty = last.clear()
    out, _ = rates.season_rates(None, 20242025, rates.Shrink(),  # type: ignore[arg-type]
                                {20242025: cur, 20232024: last, 20222023: empty})
    out = out.sort("game_date")
    # The hat trick on Nov 1 is in that game's outcome, not its rate; it lifts the next game's.
    assert out["goals"].to_list() == [3.0, 0.0, 0.0]
    assert out["fin"][1] > out["fin"][0]
    assert out["fin"][1] == pytest.approx(out["fin"][2], rel=0.2)
