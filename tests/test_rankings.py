from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from nhl.ratings import rankings as R

C = {"xg60_5v5": 2.5, "xg60_pp": 7.0, "goals_per_xg_5v5": 1.0}


def _tables() -> dict[str, pl.DataFrame]:
    """Player 1 is +0.2 offence and allows 0.1 less; everyone else is average."""
    ids = [1, 2, 3]
    ev = pl.DataFrame({
        "player_id": ids * 2, "side": ["O"] * 3 + ["D"] * 3,
        "mean": [0.2, 0.0, 0.0, -0.1, 0.0, 0.0], "toi_s": [50_000.0] * 6,
        "sd_s": [0.01] * 6, "prior_mean": [0.0] * 6,
    })
    st = ev.with_columns(pl.lit(0.0).alias("mean"))
    fin = pl.DataFrame({
        "role": ["shooter"] * 3 + ["goalie", "goalie"], "player_id": [1, 2, 3, 30, 31],
        "mean": [0.0, 0.0, 0.0, -0.1, 0.1], "shots": [500, 500, 500, 2000, 2000], "defense": [0.0] * 5,
    })
    pen = pl.DataFrame({"player_id": ids * 2, "kind": ["taken"] * 3 + ["drawn"] * 3, "rate": [1.0] * 6})
    return {"ev": ev, "st": st, "finishing": fin, "penalties": pen}


def _dep(team: int, players: list[int]) -> pl.DataFrame:
    n = len(players)
    return pl.DataFrame({
        "game_id": -team, "team_id": team, "player_id": players,
        "s5": [5 / n] * n, "spp": [5 / n] * n, "spk": [4 / n] * n, "sshot": [1 / n] * n, "is_d": [False] * n,
    })


def test_team_board_weights_players_by_share_and_uses_the_goalie():
    dep = pl.concat([_dep(10, [1, 2]), _dep(20, [2, 3])])
    goalies = pl.DataFrame({"team_id": [10, 20], "player_id": [30, 31], "weight": [1.0, 1.0]})
    b = R.team_board(_tables(), dep, goalies, C).sort("team_id")
    good, avg = b.row(0, named=True), b.row(1, named=True)
    assert good["xgf60"] == pytest.approx(2.5 + 2.5 * 0.2)
    assert good["xga60"] == pytest.approx(2.5 - 2.5 * 0.1)
    assert avg["xgf60"] == pytest.approx(2.5) and avg["xga60"] == pytest.approx(2.5)
    # Goalie 30 allows exp(-0.1) of xG; goalie 31 exp(+0.1).
    assert good["save"] > 0 > avg["save"]
    assert good["gd60"] > avg["gd60"]


def test_goalie_weights_drop_departed_goalies_and_preseason():
    games = pl.DataFrame({"game_id": [1, 2, 3, 4], "season_type": ["PR", "R", "R", "R"]})
    starts = pl.DataFrame({
        "game_id": [1, 2, 3, 4], "game_date": [date(2026, 9, 30), date(2026, 10, 1), date(2026, 10, 3), date(2026, 10, 5)],
        "team_id": [10] * 4, "starter": [99, 30, 30, 31],
    })
    teams = pl.DataFrame({"player_id": [30, 31, 99], "team_id": [10, 20, 10]})  # 31 has since moved on
    w = R.goalie_weights(starts, games, teams)
    assert w.select("team_id", "player_id", "weight").to_dicts() == [{"team_id": 10, "player_id": 30, "weight": 1.0}]
