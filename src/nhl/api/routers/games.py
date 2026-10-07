"""``/api/games/{game_id}``: everything behind one slate row."""

from __future__ import annotations

import polars as pl
from fastapi import APIRouter, Depends, HTTPException

from nhl.api.data import SiteData
from nhl.api.deps import get_data
from nhl.api.routers.slate import with_venue
from nhl.api.serialize import rows, with_selection
from nhl.api.teaminfo import team_context

router = APIRouter(prefix="/api/games", tags=["games"])

#: Lineup slots in display order: forward lines, D pairs, then anything unslotted.
SLOT_ORDER = ["f1", "f2", "f3", "f4", "d1", "d2", "d3"]


def _named(df: pl.DataFrame, names: dict[int, str]) -> pl.DataFrame:
    """Add ``player_name`` from the catalog (``Unknown`` for a null/unknown id)."""
    labels = [names.get(pid, "Unknown") if pid is not None else "Unknown" for pid in df["player_id"]]
    return df.with_columns(pl.Series("player_name", labels, dtype=pl.String))


def _lineup(df: pl.DataFrame | None, team_id: int, names: dict[int, str]) -> list[dict]:
    """One team's projected lineup, ordered by slot then position."""
    if df is None or df.is_empty():
        return []
    team = _named(df.filter(pl.col("team_id") == team_id), names)
    order = {s: i for i, s in enumerate(SLOT_ORDER)}
    pos = {"L": 0, "C": 1, "R": 2, "D": 3}
    team = team.with_columns(
        pl.col("slot").replace_strict(order, default=len(SLOT_ORDER), return_dtype=pl.Int32).alias("_slot"),
        pl.col("position").replace_strict(pos, default=9, return_dtype=pl.Int32).alias("_pos"),
    ).sort("_slot", "_pos", -pl.col("s5"))
    return rows(team, drop=("_slot", "_pos", "as_of", "stamp", "game_id"))


def _goalies(df: pl.DataFrame | None, team_id: int, names: dict[int, str]) -> list[dict]:
    """One team's starter probabilities, most likely first (a null id is 'someone else')."""
    if df is None or df.is_empty():
        return []
    team = _named(df.filter(pl.col("team_id") == team_id), names).with_columns(
        pl.when(pl.col("player_id").is_null()).then(pl.lit("Other")).otherwise(pl.col("player_name")).alias("player_name")
    )
    return rows(team.sort("p_start", descending=True), drop=("as_of", "stamp", "game_id"))


@router.get("/{game_id}")
def get_game(game_id: int, data: SiteData = Depends(get_data)) -> dict:
    """Slate row, lineups, goalie probabilities, edges and the day's price history for one game."""
    game = data.game(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail=f"unknown game {game_id}")
    day = game["game_date"]
    names = data.player_names()

    slate = data.day_slate(day)
    row = slate.filter(pl.col("game_id") == game_id) if slate is not None else None
    stamp = row["stamp"][0] if row is not None and row.height else None  # the game's last pregame run
    lineups, goalies = data.lineups(day, game_id, stamp), data.goalies(day, game_id, stamp)
    edges = data.card_edges(day)
    if edges is not None:
        edges = edges.filter(pl.col("game_id") == game_id)
        edges = with_selection(edges).sort("edge", descending=True) if edges.height else None

    venue = with_venue(pl.DataFrame({"game_id": [game_id]}), data).row(0, named=True)
    teams = team_context(data.games(), [game["away_abbr"], game["home_abbr"]], day, game["season"])
    return {
        "game": {**game, "venue_name": venue["venue_name"], "venue_location": venue["venue_location"]},
        "teams": teams,
        "pregame": (rows(row) or [None])[0],
        "lineups": {"home": _lineup(lineups, game["home_team_id"], names),
                    "away": _lineup(lineups, game["away_team_id"], names)},
        "goalies": {"home": _goalies(goalies, game["home_team_id"], names),
                    "away": _goalies(goalies, game["away_team_id"], names)},
        "edges": rows(edges),
        "history": rows(data.price_history(day, game_id)),
    }
