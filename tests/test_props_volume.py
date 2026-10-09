"""Phase C: team shot volume, player shot/block shares, saves (no network)."""

from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl
import pytest
from scipy.stats import binom, nbinom

from nhl.props import volume as V
from nhl.props.rates import BUCKET_NAMES


def logs(rows: list[tuple[int, date, int, int, bool, float]]) -> pl.DataFrame:
    """(game_id, date, team, opp, home, sf) rows; sa = the opponent's sf, cf = 2 sf, blocks = sf / 2."""
    df = pl.DataFrame(rows, schema=["game_id", "game_date", "team_id", "opp_team_id", "is_home", "sf"], orient="row")
    sa = df.select("game_id", pl.col("opp_team_id").alias("team_id"), pl.col("sf").alias("sa"))
    return df.join(sa, on=["game_id", "team_id"]).with_columns((pl.col("sf") * 2).alias("cf"), (pl.col("sf") / 2).alias("blocks"))


def test_team_rates_point_in_time() -> None:
    d1, d2 = date(2024, 10, 1), date(2024, 10, 3)
    cur = logs([(1, d1, 1, 2, True, 40.0), (1, d1, 2, 1, False, 20.0), (2, d2, 1, 2, True, 30.0), (2, d2, 2, 1, False, 30.0)])
    last = logs([(9, date(2024, 3, 1), 1, 2, True, 30.0), (9, date(2024, 3, 1), 2, 1, False, 30.0)])
    r = V.team_rates(cur, last).sort("game_id", "team_id")
    first = r.filter(pl.col("game_id") == 1)
    # Opening night: no games this season yet, so the league level is last season's (30).
    assert first["lg_sf"].to_list() == [30.0, 30.0]
    second = r.filter((pl.col("game_id") == 2) & (pl.col("team_id") == 1)).row(0, named=True)
    # Second game: the league level now counts opening night (60 shots in 2 team-games)...
    assert second["lg_sf"] == pytest.approx((60 + V.LEAGUE_PRIOR_GAMES * 30) / (2 + V.LEAGUE_PRIOR_GAMES))
    # ...and team 1's rate counts its 40-shot game, not tonight's 30.
    w, k, lg = V.LAST_SEASON_WEIGHT, V.TEAM_PRIOR_GAMES, second["lg_sf"]
    assert second["sf_pg"] == pytest.approx((40 + w * 30 * lg / 30 + k * lg) / (1 + w + k))


def test_features_and_expected() -> None:
    d = date(2024, 10, 1)
    cur = logs([(1, d, 1, 2, True, 30.0), (1, d, 2, 1, False, 30.0)])
    hist = pl.DataFrame({"game_id": [1], "home_team_id": [1], "p_home_win": [0.5]})
    f = V.features(V.team_rates(cur, cur), hist)
    assert f.height == 2 and f["z"].to_list() == pytest.approx([0.0, 0.0])
    coef = {"sog": {"b0": 0.0, "own": 1.0, "opp": 1.0, "home": 0.0, "win": 0.0, "r": 50.0},
            "blk": {"b0": 0.0, "own": 1.0, "opp": 1.0, "home": 0.0, "win": 0.0, "r": 50.0}}
    assert V.expected(f, "sog", coef).to_list() == pytest.approx([30.0, 30.0])  # league-average teams


def test_player_shares_sum_to_one_and_follow_rates() -> None:
    dep = pl.DataFrame({"game_id": [1, 1], "team_id": [10, 10], "player_id": [1, 2], "position": ["C", "D"],
                        "s5": [2.5, 2.5], "spp": [2.5, 2.5], "spk": [2.0, 2.0]})
    rates = pl.DataFrame({"game_id": [1, 1], "player_id": [1, 2], "en_pg": [0.5, 0.5],
                          **{f"sog60_{b}": [9.0, 3.0] for b in BUCKET_NAMES}})
    mix = {"ev": 0.75, "pp": 0.2, "sh": 0.02, "en": 0.03}
    sh = V.player_shares(dep, rates, mix, "sog")
    share = dict(zip(sh["player_id"], sh["share"], strict=True))
    assert share[1] + share[2] == pytest.approx(1.0)
    assert share[1] == pytest.approx(0.75)


def test_thinned_tails_match_direct_sum() -> None:
    mu, r, s = np.array([30.0]), 50.0, 0.1
    got = V.thinned_tails(np.array([s]), mu, r, 90, (2, 4))
    t = np.arange(91)
    pmf = nbinom.pmf(t, r, r / (r + 30.0))
    pmf[-1] += 1 - pmf.sum()
    assert got[2][0] == pytest.approx((binom.sf(1, t, s) * pmf).sum())
    assert got[4][0] < got[2][0]


def test_saves_mixture() -> None:
    starters = pl.DataFrame({"mu_against": [30.0, 30.0], "goals_against": [2.0, 4.0]})
    out = V.saves_props(starters, (20, 25))
    # More goals against means fewer saves at the same shots.
    assert out["exp_saves"][0] > out["exp_saves"][1]
    assert (out["p_saves_20"] > out["p_saves_25"]).all()
    q = 2.0 * (1 - V.EN_GOAL_SHARE) / 30.0
    assert out["exp_saves"][0] == pytest.approx(30 * (1 - q) * (V.P_FINISH + (1 - V.P_FINISH) * V.PULLED_SHARE))
