"""Lineup / goalie change ledger (nhl.pregame.changes)."""

from __future__ import annotations

import io
from datetime import date, datetime, timezone

import polars as pl

from nhl.pregame import changes
from nhl.storage import keys

DAY = date(2026, 10, 10)
GAME, HOME, AWAY = 2026020072, 28, 22


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        buf = io.BytesIO()
        df.write_parquet(buf)
        self.objects[key] = buf.getvalue()
        return "etag"

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        data = self.objects.get(key)
        return None if data is None else pl.read_parquet(io.BytesIO(data))

    def read_parquet_required(self, key: str) -> pl.DataFrame:
        df = self.get_parquet(key)
        assert df is not None, key
        return df

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))


def _lineup(rows: list[tuple]) -> pl.DataFrame:
    """(team_id, player_id, slot, pp_unit, p_dressed) rows."""
    return pl.DataFrame(rows, schema={"team_id": pl.Int64, "player_id": pl.Int64, "slot": pl.String,
                                      "pp_unit": pl.Int8, "p_dressed": pl.Float64}, orient="row").with_columns(game_id=pl.lit(GAME, pl.Int64))


def _goalies(rows: list[tuple]) -> pl.DataFrame:
    """(team_id, player_id, p_start, dfo_status) rows."""
    return pl.DataFrame(rows, schema={"team_id": pl.Int32, "player_id": pl.Int64, "p_start": pl.Float64,
                                      "dfo_status": pl.String}, orient="row").with_columns(game_id=pl.lit(GAME, pl.Int64))


def _prices(p: float, hg: float, ag: float, at: datetime) -> pl.DataFrame:
    return pl.DataFrame({"game_id": [GAME], "p_home_win": [p], "mean_home_goals": [hg], "mean_away_goals": [ag],
                         "as_of": [at]}).with_columns(pl.col("as_of").dt.cast_time_unit("us"))


def test_lineup_changes_in_out_line_pp_gtd():
    prev = _lineup([(HOME, 1, "f1", 1, 1.0), (HOME, 2, "f4", None, 1.0), (HOME, 3, "f2", None, 0.75), (HOME, 4, None, None, 0.25)])
    cur = _lineup([(HOME, 1, "f2", 2, 1.0), (HOME, 3, "f2", None, 1.0), (HOME, 5, "f4", None, 1.0)])
    got = {(c["player_id"], c["kind"]): (c["before"], c["after"]) for c in changes.lineup_changes(prev, cur)}
    assert got == {
        (1, "line"): ("F1", "F2"),
        (1, "pp"): ("PP1", "PP2"),
        (2, "out"): ("F4", "not dressed"),
        (3, "gtd"): ("75% to dress", "100% to dress"),
        (5, "in"): ("not dressed", "F4"),
    }


def test_lineup_changes_backup_becoming_dressed_is_in():
    prev = _lineup([(HOME, 4, None, None, 0.25)])
    cur = _lineup([(HOME, 4, "d3", None, 0.75)])
    [c] = changes.lineup_changes(prev, cur)
    assert (c["kind"], c["before"], c["after"]) == ("in", "25% to dress", "D3")


def test_goalie_changes_starter_status_and_probability():
    prev = _goalies([(HOME, 30, 0.6, None), (HOME, 31, 0.4, None), (AWAY, 40, 0.85, "Likely"), (AWAY, 41, 0.15, None)])
    cur = _goalies([(HOME, 30, 0.3, None), (HOME, 31, 0.7, None), (AWAY, 40, 0.98, "Confirmed"), (AWAY, 41, 0.02, None)])
    got = {c["team_id"]: c for c in changes.goalie_changes(prev, cur)}
    assert got[HOME]["kind"] == "starter" and got[HOME]["player_id"] == 31 and got[HOME]["before_id"] == 30
    assert (got[AWAY]["kind"], got[AWAY]["before"], got[AWAY]["after"]) == ("goalie_status", "Likely", "Confirmed")

    moved = _goalies([(HOME, 30, 0.7, None), (HOME, 31, 0.3, None)])
    [c] = changes.goalie_changes(_goalies([(HOME, 30, 0.6, None), (HOME, 31, 0.4, None)]), moved)
    assert (c["kind"], c["before"], c["after"]) == ("goalie_p", "60%", "70%")


def _store() -> MemoryStore:
    s = MemoryStore()
    s.put_parquet(keys.TEAMS, pl.DataFrame({"team_id": [HOME, AWAY], "team_abbr": ["SJS", "EDM"]}))
    s.put_parquet(keys.PLAYERS, pl.DataFrame({"player_id": [1, 2, 5, 30, 31], "player_name": ["A", "B", "E", "G1", "G2"]}))
    t = [datetime(2026, 10, 10, h, tzinfo=timezone.utc) for h in (16, 17, 18, 19)]
    g = _goalies([(HOME, 30, 0.6, None), (HOME, 31, 0.4, None)])
    snaps = [
        (_lineup([(HOME, 1, "f1", None, 1.0), (HOME, 2, "f4", None, 1.0)]), g, _prices(0.30, 2.8, 3.4, t[0])),
        # Nothing changes: no row.
        (_lineup([(HOME, 1, "f1", None, 1.0), (HOME, 2, "f4", None, 1.0)]), g, _prices(0.30, 2.8, 3.4, t[1])),
        # Player 5 in for 2, and the price moves.
        (_lineup([(HOME, 1, "f1", None, 1.0), (HOME, 5, "f4", None, 1.0)]), g, _prices(0.33, 3.0, 3.4, t[2])),
        # Same lineup, price moves (new ratings): one "price" row.
        (_lineup([(HOME, 1, "f1", None, 1.0), (HOME, 5, "f4", None, 1.0)]), g, _prices(0.32, 3.0, 3.4, t[3])),
    ]
    for at, (lu, go, pr) in zip(t, snaps):
        st = at.strftime("%Y%m%dT%H%M%SZ")
        s.put_parquet(keys.pregame_lineups(DAY, st), lu)
        s.put_parquet(keys.pregame_goalies(DAY, st), go)
        s.put_parquet(keys.pregame_prices(DAY, st), pr)
    return s


def test_build_attributes_step_price_moves():
    df = changes.build(_store(), DAY)
    assert df.select("kind", "player_name", "team").rows() == [("out", "B", "SJS"), ("in", "E", "SJS"), ("price", None, None)]
    lineup_step = df.filter(pl.col("kind") == "in").row(0, named=True)
    assert abs(lineup_step["dp_home_win"] - 0.03) < 1e-9 and abs(lineup_step["d_home_goals"] - 0.2) < 1e-9
    assert lineup_step["step_changes"] == 2
    assert df.filter(pl.col("kind") == "price")["step_changes"].to_list() == [0]


def test_update_appends_only_the_new_step_and_is_idempotent():
    s = _store()
    stamps = changes._stamps(s, DAY)
    full = changes.write(s, DAY)
    # Drop the last step, then add it back incrementally (twice: the second is a no-op).
    s.put_parquet(keys.pregame_changes(DAY), full.filter(pl.col("stamp") != stamps[-1]))
    changes.update(s, DAY, stamps[-1])
    again = changes.update(s, DAY, stamps[-1])
    assert again.equals(full)
    assert "SJS" in changes.render(again)
