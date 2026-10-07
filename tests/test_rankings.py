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


def test_line_board_sums_players():
    dep = pl.DataFrame({
        "team_id": [10] * 4, "player_id": [1, 2, 3, 4], "position": ["L", "C", "D", "D"],
        "slot": ["f1", "f1", "d1", "d1"], "s5": [0.3, 0.3, 0.4, 0.4], "p_dressed": [1.0] * 4,
    })
    lines = R.line_board(_tables(), dep).sort("slot")
    d1, f1 = lines.row(0, named=True), lines.row(1, named=True)
    assert f1["kind"] == "F" and f1["player_ids"] == [1, 2]
    assert f1["xgf60"] == pytest.approx(0.2) and f1["xga60"] == pytest.approx(-0.1) and f1["xgd60"] == pytest.approx(0.3)
    assert d1["kind"] == "D"  # player 4 is unrated: replacement level


def _stints() -> pl.DataFrame:
    """Team 10 (home): forwards 1, 2, 5, 6; defensemen 3, 4, 7. Two 5v5 stints with line 1-2-5,
    one 5v5 stint with 1-2-6, a 5v4 power play, and a preseason stint and an empty-net stint
    (5 skaters a side, away net empty) that mustn't count."""
    gid, pre = 2026020001, 2026010001
    return pl.DataFrame({
        "game_id": [gid, gid, gid, gid, pre, gid], "stint_id": [0, 1, 2, 3, 0, 4],
        "valid_personnel": [True] * 6, "duration_s": [60, 30, 45, 120, 600, 50],
        "home_team_id": [10] * 6, "away_team_id": [20] * 6,
        "home_n": [5, 5, 5, 5, 5, 5], "away_n": [5, 5, 5, 4, 5, 5],
        "home_skaters": [[1, 2, 5, 3, 4], [1, 2, 5, 3, 7], [1, 2, 6, 3, 4], [1, 2, 5, 6, 3], [1, 2, 5, 3, 4], [1, 2, 5, 3, 4]],
        "away_skaters": [[11, 12, 13, 14, 15]] * 3 + [[11, 12, 14, 15]] + [[11, 12, 13, 14, 15]] * 2,
        "home_xgf": [0.5, 0.1, 0.2, 1.0, 9.0, 9.0], "away_xgf": [0.2, 0.0, 0.1, 0.0, 9.0, 9.0],
        "home_gf": [1, 0, 0, 1, 9, 9], "away_gf": [0, 0, 0, 0, 9, 9],
        "home_goalie": [30] * 6, "away_goalie": [31] * 5 + [None],
    })


def test_observed_units_from_shifts():
    positions = pl.DataFrame({"game_id": 2026020001, "player_id": [1, 2, 5, 6, 3, 4, 7, 11, 12, 13, 14, 15],
                              "position": ["L", "C", "R", "R", "D", "D", "D", "C", "C", "C", "D", "D"]})
    u = R.observed_units(_stints(), positions)
    home = {(r["kind"], tuple(r["player_ids"])): r for r in u.filter(pl.col("team_id") == 10).to_dicts()}
    line = home[("F", (1, 2, 5))]
    assert line["toi_s"] == 90 and line["xgf"] == pytest.approx(0.6) and line["gf"] == 1 and line["games"] == 1
    assert line["team_toi_s"] == 135 and line["toi_share"] == pytest.approx(90 / 135)
    assert home[("F", (1, 2, 6))]["toi_s"] == 45
    assert home[("D", (3, 4))]["toi_s"] == 105 and home[("D", (3, 7))]["toi_s"] == 30
    pp = home[("PP", (1, 2, 3, 5, 6))]
    assert pp["toi_s"] == 120 and pp["toi_share"] == pytest.approx(1.0)
    pk = u.filter((pl.col("team_id") == 20) & (pl.col("kind") == "PK"))
    assert pk["player_ids"].to_list() == [[11, 12, 14, 15]] and pk["xga"][0] == pytest.approx(1.0)


def test_rate_units_and_current_slots():
    units = pl.DataFrame({"team_id": [10, 10], "kind": ["F", "PP"], "player_ids": [[1, 2, 3], [1, 2, 3]]})
    rated = R.rate_units(_tables(), units).sort("kind")
    f, pp = rated.row(0, named=True), rated.row(1, named=True)
    assert f["xgd60"] == pytest.approx(0.3) and pp["xgd60"] == pytest.approx(0.0)  # ST terms are all zero
    lines = pl.DataFrame({"team_id": [10], "slot": ["f1"], "kind": ["F"], "player_ids": [[3, 1, 2]]})
    dep = pl.DataFrame({"team_id": [10] * 3, "player_id": [1, 2, 3], "pp_unit": [1, 1, 2], "pk_unit": [None, None, 1]})
    slots = {(r["kind"], tuple(r["player_ids"])): r["slot"] for r in R.current_slots(lines, dep).to_dicts()}
    assert slots == {("F", (1, 2, 3)): "f1", ("PP", (1, 2)): "pp1", ("PP", (3,)): "pp2", ("PK", (3,)): "pk1"}
