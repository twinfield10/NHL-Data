from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl

from nhl.archetypes.features import FEATURES, Feature, aggregate, fit_prior, shrink, split_half

RATE = Feature("hits60", "rate", "hits", "toi_s")
SHARE = Feature("slot_share", "share", "n_slot", "n_shots")


def _counts(n_players: int = 40, games: int = 60, seed: int = 0) -> pl.DataFrame:
    """Skater-games with every count column; player i's true hit rate grows with i."""
    rng = np.random.default_rng(seed)
    rows = []
    for p in range(n_players):
        rate = 1.0 + 0.2 * p  # hits per 60
        for g in range(games):
            toi = 900
            row = {c: 0 for c in ("toi_all_s", "iff", "goals", "a1", "pen_taken", "pen_drawn", "faceoffs", "n_shots",
                                  "n_slot", "n_net_front", "n_point", "n_off_wing", "n_dist", "dist_sum", "dist_sq",
                                  "n_wristsnap", "n_slap", "n_backhand", "n_tip", "n_rebound", "n_rush",
                                  "n_turnover", "share_pp", "share_pk", "games_pp1", "oz_starts", "dz_starts",
                                  "giveaways", "takeaways", "blocks", "ca", "hits_taken")}
            row.update({"game_id": g, "game_date": date(2025, 10, 1), "player_id": p, "position": "C",
                        "is_home": g % 4 < 2, "toi_5v5_s": toi, "toi_all_s": toi + 120,
                        "hits": int(rng.poisson(rate * toi / 3600)), "share_pp": 0.5})
            rows.append(row)
    return pl.DataFrame(rows)


def test_aggregate_counts_games_and_road_split():
    agg = aggregate(_counts(n_players=2, games=10))
    row = agg.filter(pl.col("player_id") == 0).row(0, named=True)
    assert row["games"] == 10 and row["games_raw"] == 10
    assert row["toi_5v5_s"] == 9000 and row["toi_5v5_road_s"] == 3600  # games 2, 3, 6, 7 are road
    assert row["group"] == "F" and row["share_pp"] == 5.0


def test_aggregate_weights_previous_season():
    c = _counts(n_players=1, games=4).with_columns(pl.lit(0.5).alias("_w"))
    agg = aggregate(c, pl.col("_w"))
    assert agg["games"].item() == 2.0 and agg["toi_5v5_s"].item() == 1800


def test_rate_prior_recovers_spread_and_shrinks_low_minutes():
    pop = pl.DataFrame({"hits": [10.0, 30.0, 50.0, 70.0] * 25, "toi_s": [36000.0] * 100})
    m, k = fit_prior(pop, RATE)
    assert abs(m - 4.0) < 1e-9
    assert 0 < k < 10  # true spread (sd ≈ 2.2/60) is far above Poisson noise at 10 h
    small = pl.DataFrame({"hits": [5.0], "toi_s": [1800.0]})  # 10/60 raw, half an hour
    raw, shrunk = shrink(small, RATE, m, k)
    assert raw[0] == 10.0 and m < shrunk[0] < raw[0]


def test_share_prior_is_infinite_when_no_true_spread():
    rng = np.random.default_rng(1)
    n = np.full(400, 200.0)
    s = rng.binomial(200, 0.3, size=400).astype(float)
    m, k = fit_prior(pl.DataFrame({"n_slot": s, "n_shots": n}), SHARE)
    assert abs(m - 0.3) < 0.01
    assert not np.isfinite(k) or k > 1000  # pure binomial noise: shrink (almost) all the way


def test_shrink_handles_zero_exposure():
    raw, shrunk = shrink(pl.DataFrame({"n_slot": [0.0], "n_shots": [0.0]}), SHARE, 0.5, 20.0)
    assert np.isnan(raw[0]) and shrunk[0] == 0.5


def test_split_half_detects_real_rate_differences():
    sh = split_half(_counts(), min_minutes=300)
    hits = sh.filter((pl.col("group") == "F") & (pl.col("feature") == "hits60")).row(0, named=True)
    assert hits["n"] == 40 and hits["split_half"] > 0.8


def test_feature_names_unique():
    names = [f.name for f in FEATURES]
    assert len(names) == len(set(names))


# --- phase B: axes, naming, comps ------------------------------------------------------------

from nhl.archetypes import model as am  # noqa: E402


def test_name_forward_components_by_rule():
    ax = list(am.AXES["F"])
    def row(**kw):
        return [kw.get(a, 0.0) for a in ax]
    means = np.array([
        row(centre=1.3, defensive=-0.2),            # offensive centre
        row(centre=1.0, defensive=0.8),             # two-way centre
        row(centre=-1.0, physical=1.2, size=0.9),   # power forward
        row(centre=-1.0, shooter=0.5, perimeter=0.3, release=0.4),  # skill winger
        row(centre=-0.4),                           # balanced winger
    ])
    assert am.name_forward_components(means) == [
        "offensive centre", "two-way centre", "power forward", "skill winger", "balanced winger"]


def test_transform_rates_and_shares():
    df = pl.DataFrame({"shots60": [4.0], "slot_share": [0.5]})
    t = am.transform(df, ["shots60", "slot_share"])
    assert t["shots60"].item() == 2.0 and abs(t["slot_share"].item()) < 1e-12


def test_axis_raw_signs_and_means():
    feats = am._features("D")
    df = pl.DataFrame({f: [1.0] for f in feats}).with_columns(pl.lit(0.75).alias("wristsnap_share"),
                                                              pl.lit(0.25).alias("slap_share"))
    norms = {f: (0.0, 1.0) for f in feats}
    raw = am.axis_raw(df, "D", norms)
    logit = np.log(3.0)
    wrister = list(am.AXES["D"]).index("wrister")
    assert abs(raw[0, wrister] - (logit - (-logit)) / 2) < 1e-9  # (wrist − slap) / 2


def test_comps_exclude_self_and_dedupe_players():
    model = {"groups": {"F": {}}, "axes": {"F": {"a": {}, "b": {}}}}
    q = pl.DataFrame({"season": [1], "window": ["season"], "player_id": [1], "group": ["F"], "a": [0.0], "b": [0.0]})
    pool = pl.DataFrame({"season": [1, 1, 2, 3], "player_id": [1, 2, 2, 3], "group": ["F"] * 4,
                         "a": [0.0, 0.1, 0.05, 1.0], "b": [0.0] * 4})
    c = am.comps(q, pool, model, n=5)["comps"].item()
    assert [x["player_id"] for x in c] == [2, 3]
    assert c[0]["season"] == 2  # the closer of player 2's seasons


# --- phase C: usefulness tests ------------------------------------------------------------------

from nhl.archetypes import usefulness as uf  # noqa: E402


def _goal(game: int, idx: int, scorer: int, a1, a2, home_on=(1, 2, 3, 4, 5), away_on=(6, 7, 8, 9, 10), n=(5, 5)):
    return {"event_type": "GOAL", "season_type": "R", "season": 20252026, "game_id": game, "event_idx": idx,
            "game_date": date(2025, 10, game), "is_home_event": True, "home_skaters_on": n[0], "away_skaters_on": n[1],
            "home_net_empty": False, "away_net_empty": False, "home_skater_ids": list(home_on),
            "away_skater_ids": list(away_on), "player_1_id": scorer, "player_2_id": a1, "player_3_id": a2}


def test_goal_rows_outcomes_and_states():
    ev = pl.DataFrame([_goal(1, 1, 1, 2, None), _goal(1, 2, 3, 4, 5, home_on=(1, 2, 3, 4, 5), n=(5, 4)),
                       _goal(2, 1, 1, None, None, n=(4, 4))])
    rows = uf.goal_rows(ev)
    assert rows.filter(pl.col("event_idx") == 1, pl.col("game_id") == 1).sort("player_id")["outcome"].to_list() == \
        ["G", "A1", "none", "none", "none"]
    assert set(rows["state"]) == {"5v5", "PP"}  # the 4v4 goal is dropped


def test_history_counts_exclude_same_game():
    ev = pl.DataFrame([_goal(1, 1, 1, 2, None), _goal(1, 2, 1, 2, None), _goal(2, 1, 3, 1, None)])
    rows = uf.goal_rows(ev)
    hist = rows.head(0).with_columns(pl.lit(0.0).alias("w"))
    h = uf.history_counts(rows, hist)
    p1 = h.filter(pl.col("player_id") == 1).sort("game_id", "event_idx")
    assert p1["h_G"].to_list() == [0, 0, 2]  # game 1 goals don't count for each other
    assert p1["h_n"].to_list() == [0, 0, 2]


def test_pair_terms_match_explicit_sum():
    style = pl.DataFrame({"player_id": [1, 2, 3], "has_style": [True] * 3, "p_a": [0.2, 0.5, 1.0], "p_b": [0.8, 0.5, 0.0],
                          **{f"{g}_{a}": [0.0] * 3 for g, axes in am.AXES.items() for a in axes}})
    rows = pl.DataFrame({"_i": [0], "offence": [[1, 2, 3]]})
    pos = pl.DataFrame({"player_id": [1, 2, 3], "is_d": [False] * 3})
    u = uf._unit_features(rows, "offence", style, pos)
    p = {1: (0.2, 0.8), 2: (0.5, 0.5), 3: (1.0, 0.0)}
    pairs = [(1, 2), (1, 3), (2, 3)]
    aa = sum(p[i][0] * p[j][0] for i, j in pairs)
    ab = sum(p[i][0] * p[j][1] + p[i][1] * p[j][0] for i, j in pairs)
    assert abs(u["att_pair_p_a_p_a"].item() - aa) < 1e-12
    assert abs(u["att_pair_p_a_p_b"].item() - ab) < 1e-12


def test_aging_features_are_side_specific():
    players = pl.DataFrame({"side": ["O", "D"], "age": [32.0, 22.0], "is_d": [True, False], "p_a": [0.5, 0.5],
                            "p_b": [0.5, 0.5], "F_x": [1.0, 2.0]})
    f, names = uf._aging_features(players, "position", ["p_a", "p_b"])
    assert names == ["O:one", "O:a", "O:a2", "O:is_d", "O:is_d*a", "D:one", "D:a", "D:a2", "D:is_d", "D:is_d*a"]
    assert f[0, names.index("O:is_d*a")] == 1.0 and f[0, names.index("D:one")] == 0.0
    assert f[1, names.index("D:a")] == -1.0
