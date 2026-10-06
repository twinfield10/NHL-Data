from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl

from nhl.pregame import goalies as G

A, B, C = 100, 200, 300


def _team(starters: list[int], b2b_2nd: set[int] = frozenset()) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """One team's games with the given starters (None = future game); A and B always dress."""
    n = len(starters)
    day0 = date(2025, 10, 1)
    streak, prev = [], None
    for s in starters:
        streak.append((streak[-1] + 1 if s == prev else 1) if s is not None else None)
        prev = s
    tg = pl.DataFrame({
        "game_id": [2025020000 + i for i in range(n)], "season": [20252026] * n,
        "game_date": [day0 + timedelta(days=2 * i) for i in range(n)], "team_id": [1] * n,
        "home": [i % 2 == 0 for i in range(n)], "playoff": [False] * n, "starter": starters,
        "consecutive_starts": streak, "n": list(range(n)),
        "b2b_2nd": [i in b2b_2nd for i in range(n)], "b2b_1st": [i + 1 in b2b_2nd for i in range(n)],
        "opp_str": [0.0] * n,
    }, schema_overrides={"starter": pl.Int64, "consecutive_starts": pl.Int16})
    played = tg.filter(pl.col("starter").is_not_null())
    dressed = pl.concat([
        played.select("game_id", "team_id", pl.lit(g, dtype=pl.Int64).alias("player_id")) for g in (A, B)
    ] + [played.filter(pl.col("starter") == C).select("game_id", "team_id", pl.lit(C, dtype=pl.Int64).alias("player_id"))])
    apps = played.select(pl.col("starter").alias("player_id"), "game_date", pl.lit(True).alias("started"),
                         pl.lit(0.0).alias("gsax"), pl.lit(1.0).alias("xga"))
    return tg, dressed, apps


def test_candidate_features_lag_only_and_other():
    starters = [A, A, B, A, A, C, None]
    cand = G.candidate_features(*_team(starters))
    # One chosen row per played team-game; game 0 has no history, so only `other`.
    played = cand.filter(pl.col("starter").is_not_null())
    assert played.group_by("game_id").agg(pl.col("chosen").sum())["chosen"].to_list() == [1] * 6
    g0 = cand.filter(pl.col("game_id") == 2025020000)
    assert g0.height == 1 and g0["is_other"][0] == 1 and g0["chosen"][0]
    # Game 4: A started game 3 (a streak of 1 before this game); 3 of 4 earlier starts were A.
    g4 = cand.filter((pl.col("game_id") == 2025020004) & (pl.col("player_id") == A)).row(0, named=True)
    assert g4["started_last"] == 1.0 and g4["streak"] == 1.0 and g4["share_5"] == 0.75
    # Game 5: C never dressed before, so the starter is `other`.
    g5 = cand.filter(pl.col("game_id") == 2025020005)
    assert g5.filter(pl.col("chosen"))["is_other"].to_list() == [1.0]
    # The future game gets candidates from earlier games only, and nobody is chosen.
    fut = cand.filter(pl.col("game_id") == 2025020006)
    assert set(fut["player_id"].drop_nulls().to_list()) == {A, B, C} and not fut["chosen"].any()
    assert fut.filter(pl.col("player_id") == C)["started_last"][0] == 1.0


def test_fit_learns_back_to_back_switch():
    # A starts every game except the 2nd night of each back-to-back, which B starts.
    b2b = {i for i in range(3, 200, 4)}
    starters = [B if i in b2b else A for i in range(200)]
    cand = G.candidate_features(*_team(starters, b2b))
    model = G.fit(cand, G.FEATURES)
    pred = model.predict(cand)
    sums = pred.group_by("game_id").agg(pl.col("p_start").sum())["p_start"].to_numpy()
    assert np.allclose(sums, 1.0)
    late = pred.filter(pl.col("game_id") > 2025020050)
    p_b_on_b2b = late.filter(pl.col("player_id") == B, pl.col("b2b_2nd") == 1)["p_start"].mean()
    p_a_else = late.filter(pl.col("player_id") == A, pl.col("b2b_2nd") == 0)["p_start"].mean()
    assert p_b_on_b2b > 0.8 and p_a_else > 0.8
    assert G.log_loss(late) < 0.3


# --------------------------------------------------------------------------- lineups

def _lineup_fixture(events: list[dict]):
    """Team 1 played 3 games; skaters 1-12 F and 21-26 D dressed in all, 13 (F) in game 1 only."""
    from nhl.pregame import lineups as L

    rows = []
    for g, day in enumerate([date(2026, 10, 1), date(2026, 10, 3), date(2026, 10, 5)]):
        players = [(p, "C") for p in range(1, 13)] + [(p, "D") for p in range(21, 27)]
        if g == 0:
            players = [(p, pos) for p, pos in players if p != 12] + [(13, "L")]
        for p, pos in players:
            rows.append({"game_id": 100 + g, "game_date": day, "team_id": 1, "player_id": p, "position": pos,
                         "s5": (0.06 if pos == "D" else 0.04) + (0.03 if p in (1, 2) else 0.0),
                         "spp": 0.1, "spk": 0.1, "sshot": 0.05})
    hist = pl.DataFrame(rows)
    ev = pl.DataFrame(events, schema=L._EVENT_SCHEMA) if events else pl.DataFrame(schema=L._EVENT_SCHEMA)
    return L, L._build_index(hist, ev)


def _ev(pid, kind, known, until=None, cause="espn:Injured Reserve", injury=True):
    from datetime import datetime, timezone

    return {"team_id": 1, "player_id": pid, "known_at": datetime(*known, 15, tzinfo=timezone.utc), "kind": kind,
            "until": until, "cause": cause, "injury": injury}


def test_lineup_default_is_last_game():
    L, ix = _lineup_fixture([])
    rows = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    assert sorted(r["player_id"] for r in rows) == list(range(1, 13)) + list(range(21, 27))
    assert {r["source"] for r in rows} == {"last_game"}


def test_lineup_injury_until_return_date_then_back():
    # Player 1 goes on IR after the last game, expected back 10/12.
    L, ix = _lineup_fixture([_ev(1, "out", (2026, 10, 6), until=date(2026, 10, 12))])
    before = L.project_team_game(ix, 1, 999, date(2026, 10, 9), L.morning(date(2026, 10, 9)))
    ids = {r["player_id"] for r in before}
    assert 1 not in ids and 13 in ids  # the recent healthy scratch fills his slot
    fill = next(r for r in before if r["player_id"] == 13)
    assert fill["source"] == "fill" and fill["confidence"] == "low" and "until 2026-10-12" in fill["issues"]
    # Same knowledge, a game after his expected return: he's back and nobody is filled.
    after = L.project_team_game(ix, 1, 998, date(2026, 10, 13), L.morning(date(2026, 10, 9)))
    assert 1 in {r["player_id"] for r in after} and 13 not in {r["player_id"] for r in after}


def test_lineup_ignores_events_before_last_game_and_returns_displace():
    # Player 2 was "assigned" before the last game but played in it: stale, ignored.
    # Player 13 (a regular, missing the last two games) is activated from IR after it.
    L, ix = _lineup_fixture([
        _ev(2, "out", (2026, 10, 2), cause="tx:assigned", injury=False),
        _ev(13, "in", (2026, 10, 6), cause="tx:activated_ir"),
    ])
    rows = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    ids = {r["player_id"] for r in rows}
    assert 2 in ids
    # 13's usage (0.04) isn't clearly above the weakest forward's, so he doesn't displace anyone.
    assert 13 not in ids and len(rows) == 18


def test_lineup_placeholder_when_nobody_available_and_normalise():
    L, ix = _lineup_fixture([_ev(21, "out", (2026, 10, 6), until=None, cause="tx:traded", injury=False)])
    rows = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    ph = [r for r in rows if r["player_id"] is None]
    assert len(ph) == 1 and ph[0]["position"] == "D" and "placeholder" in ph[0]["issues"]
    dep = L.normalise(pl.DataFrame(rows, schema_overrides={"player_id": pl.Int64}))
    sums = dep.group_by("team_id").agg(pl.col("s5", "spp", "spk", "sshot").sum()).row(0, named=True)
    assert np.isclose(sums["s5"], 5) and np.isclose(sums["spp"], 5) and np.isclose(sums["spk"], 4) and np.isclose(sums["sshot"], 1)
    assert L.lineup_shape(dep)["shape"][0] == "12F6D"
