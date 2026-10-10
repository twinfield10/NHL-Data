from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

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


def _dfo(valid: bool, groups: dict, complete: set, out: set = frozenset()):
    from datetime import datetime, timezone

    from nhl.pregame import lineups as L

    return L.DfoVersion(datetime(2026, 10, 6, 15, tzinfo=timezone.utc), valid, None if valid else "f4 has 2/3",
                        groups, complete, set(out))


def test_dfo_valid_version_sets_lineup_and_slot_shares():
    L, ix = _lineup_fixture([])
    groups = {f"f{i}": [(3 * i - 2, "C"), (3 * i - 1, "L"), (3 * i, "R")] for i in range(1, 5)}
    groups["f4"] = [(10, "C"), (11, "L"), (13, "R")]  # 13 replaces 12
    groups.update({f"d{i}": [(19 + 2 * i, "D"), (20 + 2 * i, "D")] for i in range(1, 4)})
    groups["pp1"] = [(1, "C"), (2, "L"), (3, "R"), (21, "D"), (4, "C")]
    ix.dfo = {1: ([datetime(2026, 10, 6, 15, tzinfo=timezone.utc)], [_dfo(True, groups, set(groups))])}
    rows = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    by = {r["player_id"]: r for r in rows}
    assert 13 in by and 12 not in by and len(rows) == 18
    assert by[13]["slot"] == "f4" and by[1]["slot"] == "f1" and by[1]["pp_unit"] == 1 and by[5]["pp_unit"] is None
    # Shares pulled halfway toward the slot: an f1 forward gains on an f4 forward with equal history.
    assert by[4]["s5"] > by[10]["s5"]


def test_dfo_invalid_version_uses_complete_groups_and_fills_shortfall():
    L, ix = _lineup_fixture([])
    # Only d1 is usable; DFO lists player 2 on IR. The lineup keeps the projection elsewhere and
    # fills 2's forward slot with the recent scratch (13).
    groups = {"d1": [(25, "D"), (26, "D")], "f4": [(10, "C"), (11, "L")]}
    ix.dfo = {1: ([datetime(2026, 10, 6, 15, tzinfo=timezone.utc)], [_dfo(False, groups, {"d1"}, out={2})])}
    rows = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    by = {r["player_id"]: r for r in rows}
    assert len(rows) == 18 and 2 not in by and 13 in by and by[13]["source"] == "fill"
    assert by[25]["slot"] == "d1" and by[25]["source"] == "dfo"


def test_dfo_goalie_override_redistributes_and_adds_unknown_goalie():
    from nhl.pregame import price as P
    from nhl.storage import keys

    class Mem:
        def __init__(self, data):
            self.data = data

        def get_parquet(self, key):
            return self.data.get(key)

        def read_parquet_required(self, key):
            return self.data[key]

    as_of = datetime(2026, 10, 6, 18, tzinfo=timezone.utc)
    probs = pl.DataFrame({"game_id": [1, 1, 1, 2, 2], "team_id": [10, 10, 10, 20, 20],
                          "player_id": [A, B, None, A + 1, B + 1], "p_start": [0.6, 0.3, 0.1, 0.7, 0.3]})
    dfo = pl.DataFrame({
        "game_id": [1, 1, 2], "team": ["TOR", "TOR", "BOS"], "player_id": [A, B, C],
        "status": ["Likely", "Confirmed", "Likely"],
        "captured_at": [as_of.replace(hour=12), as_of.replace(hour=15), as_of.replace(hour=19)],  # BOS report comes after as_of
    })
    teams = pl.DataFrame({"team_id": [10, 20], "team_abbr": ["TOR", "BOS"]})
    out = P.apply_dfo_goalies(probs, Mem({keys.dailyfaceoff_goalies(20262027): dfo, keys.TEAMS: teams}), 20262027, as_of)
    tor = out.filter(pl.col("team_id") == 10)
    p = dict(zip(tor["player_id"].to_list(), tor["p_start"].to_list()))
    assert abs(p[B] - 0.98) < 1e-12  # the latest TOR report (Confirmed B) wins
    assert abs(p[A] - 0.02 * 0.6 / 0.7) < 1e-12 and abs(sum(p.values()) - 1) < 1e-12
    assert out.filter(pl.col("team_id") == 20)["source"].to_list() == ["model", "model"]  # not known yet
    # A named goalie who isn't a candidate is added.
    dfo2 = dfo.with_columns(pl.lit(as_of.replace(hour=10)).alias("captured_at"))
    out2 = P.apply_dfo_goalies(probs, Mem({keys.dailyfaceoff_goalies(20262027): dfo2, keys.TEAMS: teams}), 20262027, as_of)
    bos = out2.filter(pl.col("team_id") == 20)
    assert C in bos["player_id"].to_list() and abs(bos["p_start"].sum() - 1) < 1e-12
    assert abs(bos.filter(pl.col("player_id") == C)["p_start"][0] - 0.85) < 1e-12


def _full_dfo_groups():
    groups = {f"f{i}": [(3 * i - 2, "C"), (3 * i - 1, "L"), (3 * i, "R")] for i in range(1, 5)}
    groups.update({f"d{i}": [(19 + 2 * i, "D"), (20 + 2 * i, "D")] for i in range(1, 4)})
    return groups


def test_questionable_player_stays_espn_out_player_is_replaced():
    # DailyFaceoff has 21 (d1) and 4 (f2) in slots and on its injury list. ESPN: 21 out until
    # 10/13 (ESPN wins: replaced), 4 day-to-day (a game-time decision: 75/25 with a backup).
    L, ix = _lineup_fixture([
        _ev(21, "out", (2026, 10, 6), until=date(2026, 10, 13), cause="espn:Out"),
        _ev(4, "dtd", (2026, 10, 6), until=date(2026, 10, 8), cause="espn:Day-To-Day"),
    ])
    groups = _full_dfo_groups()
    version = _dfo(True, groups, set(groups))
    version.questionable = {21, 4}
    ix.dfo = {1: ([datetime(2026, 10, 6, 15, tzinfo=timezone.utc)], [version])}
    rows = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    by = {r["player_id"]: r for r in rows}
    assert 21 not in by
    assert by[4]["p_dressed"] == L.GTD_P_DRESSED and "game-time decision" in by[4]["issues"]
    backup = next(r for r in rows if r["source"] == "gtd_backup")
    assert backup["p_dressed"] == 1 - L.GTD_P_DRESSED and backup["slot"] == "f2"
    assert abs(backup["s5"] / by[4]["s5"] - (1 - L.GTD_P_DRESSED) / L.GTD_P_DRESSED) < 1e-12
    dep = L.normalise(pl.DataFrame(rows, schema_overrides={"player_id": pl.Int64}))
    assert L.lineup_shape(dep)["regular"].all()  # the backup doesn't count toward the shape
    assert np.isclose(dep["s5"].sum(), 5.0)


def test_questionable_without_espn_dtd_plays_normally():
    L, ix = _lineup_fixture([])
    groups = _full_dfo_groups()
    version = _dfo(True, groups, set(groups))
    version.questionable = {4}
    ix.dfo = {1: ([datetime(2026, 10, 6, 15, tzinfo=timezone.utc)], [version])}
    rows = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    assert {r["p_dressed"] for r in rows} == {1.0} and len(rows) == 18


def test_dfo_index_reads_d4_as_seventh_defenseman():
    from nhl.pregame import lineups as L

    t = datetime(2026, 10, 6, 15, tzinfo=timezone.utc)
    rows = []
    for g, members in {**{f"f{i}": [(3 * i - 2, "c"), (3 * i - 1, "lw"), (3 * i, "rw")] for i in range(1, 4)},
                       "f4": [(10, "c"), (11, "lw")], **{f"d{i}": [(19 + 2 * i, "ld"), (20 + 2 * i, "rd")] for i in range(1, 4)},
                       "d4": [(27, "ld")], "ir": [(12, "rw")]}.items():
        for pid, pos in members:
            rows.append({"team_id": 1, "updated_at": t, "captured_at": t, "category": "oi" if g == "ir" else "ev", "group": g,
                         "position": pos, "player_id": pid, "injury_status": "ir" if g == "ir" else None,
                         "game_time_decision": False, "conflict": False, "questionable": False,
                         "group_complete": g != "ir", "lineup_shape": "11F7D", "issues": None, "is_valid": True})
    version = L._dfo_index(pl.DataFrame(rows))[1][1][0]
    assert "d4" in version.complete and version.out == {12}
    L_, ix = _lineup_fixture([])
    ix.dfo = {1: ([t], [version])}
    out = L.project_team_game(ix, 1, 999, date(2026, 10, 7), L.morning(date(2026, 10, 7)))
    dep = L.normalise(pl.DataFrame(out, schema_overrides={"player_id": pl.Int64}))
    assert L.lineup_shape(dep)["shape"][0] == "11F7D"


def test_drop_injured_goalies_renormalises():
    from nhl.pregame import price as P

    probs = pl.DataFrame({"game_id": [1, 1, 1], "team_id": [10, 10, 10], "player_id": [A, B, None], "p_start": [0.2, 0.7, 0.1]})
    events = pl.DataFrame({
        "team_id": [10, 10], "player_id": [B, A], "known_at": [datetime(2026, 10, 5, tzinfo=timezone.utc)] * 2,
        "kind": ["out", "out"], "until": [date(2026, 10, 13), date(2026, 10, 6)], "cause": ["espn:Injured Reserve", "espn:Out"],
        "injury": [True, True],
    })
    out = P.drop_injured_goalies(probs, events, date(2026, 10, 6), datetime(2026, 10, 6, 18, tzinfo=timezone.utc))
    assert B not in out["player_id"].to_list()  # out past the game; A's return date is the game day: kept
    assert abs(out["p_start"].sum() - 1) < 1e-12 and abs(out.filter(pl.col("player_id") == A)["p_start"][0] - 2 / 3) < 1e-12


class _SlateStore:
    """In-memory stand-in for the S3 store (keys -> frames / bytes)."""

    def __init__(self, data: dict):
        self.data = data
        self.bytes: dict = {}

    def get_parquet(self, key):
        return self.data.get(key)

    def read_parquet_required(self, key):
        return self.data[key]

    def list_keys(self, prefix):
        return [k for k in self.data if k.startswith(prefix)]

    def put_parquet(self, key, df):
        self.data[key] = df

    def put_bytes(self, key, data, content_type=None):
        self.bytes[key] = data


def test_freshness_flags_stale_and_missing_sources():
    from nhl.pregame import slate
    from nhl.storage import keys

    season, now = 20262027, datetime(2026, 10, 6, 18, tzinfo=timezone.utc)
    cap = lambda h: pl.DataFrame({"captured_at": [now - timedelta(hours=h)]})  # noqa: E731
    store = _SlateStore({
        keys.dailyfaceoff_goalies(season): cap(1), keys.dailyfaceoff_lines(season): cap(30),
        keys.injuries(season): cap(2), keys.transactions(season): cap(3),
        f"{keys.odds_live_prefix(season)}lowvig.parquet": cap(0.5),
        "ratings/2026-10-06/ev.parquet": pl.DataFrame(),  # written by this morning's nightly run
        keys.GAMES: pl.DataFrame({"season": [season], "is_final": [True], "game_date": [date(2026, 10, 5)]}),
        keys.player_game_logs(season): pl.DataFrame({"game_date": [date(2026, 10, 5)]}),
    })
    f = slate.freshness(store, season, now)
    stale = dict(zip(f["source"], f["stale"]))
    assert stale["dailyfaceoff_lines"] and stale["ref_assignments"]  # 30 h old; never captured
    assert not stale["dailyfaceoff_goalies"] and not stale["odds"] and not stale["ratings"] and not stale["game_state"]


def test_slate_is_one_row_per_game_and_writes_latest_pointer():
    import json

    from nhl.pregame import slate
    from nhl.storage import keys

    as_of = datetime(2026, 10, 6, 18, tzinfo=timezone.utc)
    games = pl.DataFrame({"game_id": [1, 2], "game_date": [date(2026, 10, 6)] * 2, "start_utc": [as_of + timedelta(hours=5)] * 2,
                          "home_team_id": [10, 30], "away_team_id": [20, 40]})
    dep = pl.DataFrame({"game_id": [1, 1, 2, 2], "team_id": [10, 20, 30, 40], "source": ["dfo", "fill", "last_game", "gtd_backup"],
                        "issues": ["", "x out", "", "backup"]})
    probs = pl.DataFrame({"game_id": [1, 1, 1, 2, 2], "team_id": [10, 10, 20, 30, 40], "player_id": [A, B, C, A + 1, B + 1],
                          "p_start": [0.9, 0.1, 1.0, 1.0, 1.0], "dfo_status": ["Confirmed", None, None, None, None]})
    prices = pl.DataFrame({"game_id": [1, 2], "game_date": [date(2026, 10, 6)] * 2, "home_team_id": [10, 30], "away_team_id": [20, 40],
                           "p_home_win": [0.6, 0.45], "mean_home_goals": [3.1, 2.8], "mean_away_goals": [2.7, 3.0]})
    store = _SlateStore({
        keys.TEAMS: pl.DataFrame({"team_id": [10, 20, 30, 40], "team_abbr": ["TOR", "BOS", "NYR", "NJD"]}),
        keys.PLAYERS: pl.DataFrame({"player_id": [A, B, C, A + 1, B + 1], "player_name": ["a", "b", "c", "d", "e"]}),
    })
    fresh = pl.DataFrame({"source": ["odds"], "last_update": [as_of], "age_hours": [30.0], "max_age_hours": [6.0],
                          "stale": [True], "note": [""]})
    rows = slate.build(store, 20262027, games, dep, probs, prices, fresh, as_of, "STAMP")
    assert rows.height == 2 and rows["game_id"].to_list() == [1, 2]
    r = rows.row(0, named=True)
    assert r["home_starter"] == "a" and r["home_starter_p"] == 0.9 and r["home_alt_starter"] == "b"
    assert r["away_fills"] == 1 and r["stale_inputs"] == "odds" and r["stamp"] == "STAMP"
    assert rows.row(1, named=True)["away_game_time_decisions"] == 1
    slate.write(store, date(2026, 10, 6), "STAMP", rows, fresh)
    assert json.loads(store.bytes[keys.pregame_latest(date(2026, 10, 6))])["stamp"] == "STAMP"
    assert "TOR" in slate.render(rows, fresh) and "STALE INPUTS: odds" in slate.render(rows, fresh)


def test_assume_treats_an_unplayed_game_as_played():
    # A starts games 0-3; game 4 (tonight) is unplayed; game 5 (tomorrow) is the 2nd of a back-to-back.
    tg, dressed, apps = _team([A, A, A, A, None, None], b2b_2nd={5})
    tomorrow = lambda c: {r["player_id"]: r for r in c.filter(pl.col("game_id") == 2025020005).iter_rows(named=True)}  # noqa: E731
    raw = tomorrow(G.candidate_features(tg, dressed, apps))
    assert raw[A]["started_last"] == 0.0  # tonight unplayed: nobody "started last"
    tonight = tg.filter(pl.col("game_id") == 2025020004)
    assumed = pl.DataFrame({"game_id": [2025020004], "team_id": [1], "game_date": tonight["game_date"],
                            "starter": [B], "backup": [A]})
    after = tomorrow(G.candidate_features(*G.assume(tg, dressed, apps, assumed)))
    assert after[B]["started_last"] == 1.0 and after[B]["b2b_2nd_x_last"] == 1.0 and after[B]["streak"] == 1.0
    assert after[A]["started_last"] == 0.0 and after[A]["b2b_2nd_x_last"] == 0.0
    assert after[B]["log_rest"] == np.log(2)  # B's assumed start tonight, two days before
