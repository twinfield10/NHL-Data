"""``/api/ratings/players`` and ``/api/ratings/teams``: rankings from the latest rating snapshot."""

from __future__ import annotations

from datetime import date

import polars as pl
from fastapi import APIRouter, Depends, HTTPException

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import rows
from nhl.api.teaminfo import team_context

router = APIRouter(prefix="/api/ratings", tags=["ratings"])

#: Lineup slots in display order (as on the game page).
SLOT_ORDER = ["f1", "f2", "f3", "f4", "d1", "d2", "d3"]


def _rankings(data: SiteData, day: date) -> dict:
    out = data.rankings(day)
    if out is None:
        raise HTTPException(status_code=404, detail=f"no rating snapshot on or before {day}")
    return out


def _meta(r: dict) -> dict:
    return {"snapshot": r["snapshot"].isoformat(), "as_of": r["as_of"].isoformat(), "season": r["season"]}


@router.get("/players")
def get_players(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """Every rated skater and goalie, best 5v5 net / save talent first."""
    r = _rankings(data, day)
    return {
        **_meta(r),
        "skaters": rows(r["players"].sort("ev_net", descending=True, nulls_last=True)),
        "goalies": rows(r["goalies"].sort("save", descending=True, nulls_last=True)),
    }


@router.get("/teams")
def get_teams(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """Every team's composed rating (best 5v5 goal differential first), its likely goalies and
    the projected lineup behind it."""
    r = _rankings(data, day)
    games = data.games()
    abbrs = games.filter(pl.col("season") == r["season"]).select(
        pl.col("home_team_id").cast(pl.Int64).alias("team_id"), pl.col("home_abbr").alias("abbr")).unique("team_id")
    board = r["teams"].join(abbrs, on="team_id", how="left").sort("gd60", descending=True)
    context = team_context(games, board["abbr"].to_list(), day, r["season"])

    names = data.player_names()
    goalies = r["goalie_weights"].with_columns(
        pl.col("player_id").replace_strict(names, default=None, return_dtype=pl.String).alias("player_name"))
    skaters = r["players"].select("player_id", "player_name", "ev_off", "ev_def", "ev_net")
    dep = r["lineups"].join(skaters, on="player_id", how="left").with_columns(
        pl.col("slot").replace_strict({s: i for i, s in enumerate(SLOT_ORDER)}, default=len(SLOT_ORDER),
                                      return_dtype=pl.Int32).alias("_slot"),
        pl.col("player_name").fill_null("Replacement"),
    ).sort("_slot", -pl.col("s5"))

    teams = []
    for row in rows(board):
        tid = row["team_id"]
        teams.append({
            **row,
            **context.get(row["abbr"], {}),
            "goalies": rows(goalies.filter(pl.col("team_id") == tid).sort("weight", descending=True), drop=("team_id",)),
            "lineup": rows(dep.filter(pl.col("team_id") == tid).select(
                "player_id", "player_name", "position", "slot", "s5", "spp", "spk", "ev_off", "ev_def", "ev_net")),
        })
    return {**_meta(r), "teams": teams}
