"""``/api/props``, ``/api/games/{id}/props`` and ``/api/props/bets`` on an in-memory store."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from nhl.api.data import SiteData
from nhl.storage import keys

DAY = date(2026, 10, 8)
GID = 2026020065
T0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 8, 20, 0, tzinfo=timezone.utc)


class FakeStore:
    def __init__(self, objects: dict):
        self.objects = objects

    def get_parquet(self, key):
        return self.objects.get(key)

    def read_parquet_required(self, key):
        return self.objects[key]

    def get_bytes(self, key):
        return self.objects.get(key)

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]


def edge(player_id: int, edge_: float, flagged: bool, **kw) -> dict:
    base = {"game_id": GID, "start_utc": datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc), "away_abbr": "TOR",
            "home_abbr": "VGK", "player_id": player_id, "player_name": f"P{player_id}", "team": "TOR", "position": "C",
            "slot": "f1", "pp_unit": 1, "prop_type": "points", "line": 0.5, "side": "over", "book": "DraftKings",
            "price": 120.0, "books": 3, "p_model_side": 0.5, "p_market_side": 0.45, "p": 0.48, "edge": edge_,
            "flagged": flagged, "stake_units": 0.2 if flagged else 0.0, "stamp": "T1"}
    return base | kw


def quote(player_id: int, side: str, price: float, at: datetime, book: str = "DraftKings") -> dict:
    return {"book": book, "game_id": GID, "captured_at": at, "player_name": f"P{player_id}", "player_id": player_id,
            "team": "TOR", "prop_type": "points", "line": 0.5, "side": side, "price": price}


@pytest.fixture
def client(monkeypatch):
    from fastapi.testclient import TestClient

    from nhl.api.deps import get_data
    from nhl.api.main import app

    games = pl.DataFrame({"game_id": [GID], "season": [20262027], "game_date": [DAY], "start_time_et": ["2026-10-08T22:00:00"],
                          "home_team_id": [54], "away_team_id": [10], "home_abbr": ["VGK"], "away_abbr": ["TOR"],
                          "is_final": [False]})
    edges = pl.DataFrame([edge(1, 0.08, True), edge(2, -0.10, False), edge(3, -0.06, False)])
    ledger = pl.DataFrame({"bet_id": ["b3"], "kind": ["paper"], "placed_at": [T0], "game_id": [GID], "game_date": [DAY],
                           "player_id": [3], "player_name": ["P3"], "team": ["TOR"], "prop_type": ["points"], "line": [0.5],
                           "side": ["over"], "book": ["FanDuel"], "price": [150.0], "stake_units": [0.25], "edge": [0.07],
                           "clv": [None], "pnl_units": [None], "result": [None], "graded_at": [None]},
                          schema_overrides={"clv": pl.Float64, "pnl_units": pl.Float64, "result": pl.String,
                                            "graded_at": pl.Datetime("us", "UTC")})
    quotes = pl.DataFrame([quote(1, "over", 110.0, T0), quote(1, "over", 120.0, T1), quote(1, "under", -150.0, T0)])
    proj = pl.DataFrame({"game_id": [GID, GID], "team_id": [10, 54], "player_id": [1, 7], "position": ["C", "D"],
                         "slot": ["f1", "d1"], "pp_unit": [1, None], "p_dressed": [1.0, 1.0], "confidence": ["high", "high"],
                         "source": ["dfo", "dfo"], "p_points_1": [0.55, 0.2], "p_goals_1": [0.3, 0.05],
                         "exp_goals": [0.35, 0.05], "exp_ast": [0.4, 0.2], "exp_points": [0.75, 0.25], "stamp": ["P1", "P1"]})
    store = FakeStore({
        keys.GAMES: games, keys.PLAYERS: pl.DataFrame({"player_id": [1, 7], "player_name": ["Auston Matthews", "Shea Theodore"]}),
        keys.props_edges(DAY, "T1"): edges, keys.PROPS_LEDGER: ledger, keys.props_projections(DAY, "P1"): proj,
        "external/odds/props/20262027/espn.parquet": quotes,
    })
    monkeypatch.setitem(app.dependency_overrides, get_data, lambda: SiteData(store))
    return TestClient(app)


def test_props_lists_plays_placed_bets_and_movement(client):
    body = client.get("/api/props", params={"date": "2026-10-08"}).json()
    got = {r["player_id"]: r for r in body["props"]}
    assert set(got) == {1, 3}  # player 2's -10% edge is below the floor; player 3 is in the ledger
    assert got[1]["flagged"] and got[1]["open_price"] == 110.0 and got[1]["moves"] == 1
    assert got[3]["bet_stake"] == 0.25 and got[3]["bet_price"] == 150.0 and got[3]["bet_book"] == "FanDuel"
    assert got[1]["bet_stake"] is None
    assert got[3]["bet_clv"] == pytest.approx(0.45 * 2.5 - 1)  # +150 taken, 45% consensus now
    assert body["props"][0]["player_id"] == 1  # flagged first
    assert body["max_price"] == 400.0


def test_game_props_board(client):
    body = client.get(f"/api/games/{GID}/props").json()
    players = {p["player_id"]: p for p in body["players"]}
    assert players[1]["player_name"] == "Auston Matthews" and players[1]["team"] == "TOR"
    assert players[7]["team"] == "VGK"
    (q,) = body["quotes"]
    assert q["price_over"] == 120.0 and q["price_under"] == -150.0  # latest over, the under it pairs with
    assert 0 < q["p_book"] < 1 and q["books"] == 1
    assert {e["player_id"] for e in body["edges"]} == {1, 2, 3}


def test_game_props_unknown_game(client):
    assert client.get("/api/games/1/props").status_code == 404


def test_props_bets(client):
    body = client.get("/api/props/bets").json()
    assert len(body["bets"]) == 1 and body["bets"][0]["home_abbr"] == "VGK"
    assert body["totals"]["bets"] == 0 and body["breakdown"] == [] and body["timing"] == []  # nothing graded yet
    (b,) = body["bets"]
    assert b["now_price"] == 120.0 and b["now_book"] == "DraftKings"
    assert b["clv_now"] == pytest.approx(0.45 * 2.5 - 1)
    assert b["lead_minutes"] == pytest.approx(14 * 60)  # placed 12:00 UTC, puck drop 02:00 UTC
    assert b["status"] in ("faded", "closed")  # unflagged now; "closed" once the game's start has passed
    assert body["open"]["bets"] == 1 and body["open"]["mean_clv"] == pytest.approx(0.125)
