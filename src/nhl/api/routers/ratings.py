"""``/api/ratings/players`` and ``/api/ratings/teams``: rankings from the latest rating snapshot."""

from __future__ import annotations

from datetime import date

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import rows
from nhl.api.routers.style import archetype_lookup
from nhl.api.teaminfo import team_context
from nhl.ratings import rankings
from nhl.storage import keys

router = APIRouter(prefix="/api/ratings", tags=["ratings"])

#: Lineup slots in display order (as on the game page).
SLOT_ORDER = ["f1", "f2", "f3", "f4", "d1", "d2", "d3"]
#: Units with less than this share of their team's time in that state together aren't sent (one-shift oddities).
MIN_UNIT_SHARE = 0.005


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
    styles = archetype_lookup(data, r["season"])
    skaters = rows(r["players"].sort("ev_net", descending=True, nulls_last=True))
    for p in skaters:
        p.update(styles.get(p["player_id"], {"archetype": None, "archetype_conf": None, "style_group": None}))
    return {
        **_meta(r),
        "skaters": skaters,
        "goalies": rows(r["goalies"].sort("save", descending=True, nulls_last=True)),
    }


@router.get("/teams")
def get_teams(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """Every team's composed rating (best 5v5 goal differential first), its likely goalies and
    the projected lineup behind it, plus the league-average 5v5 and PP xG/60 they're measured against."""
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
    return {**_meta(r), "league": r["league"], "teams": teams}


def _unit_key(df: pl.DataFrame) -> pl.DataFrame:
    """A string key for a unit's sorted player ids (joins on list columns are awkward)."""
    return df.with_columns(pl.col("player_ids").list.sort().cast(pl.List(pl.String)).list.join("-").alias("_key"))


@router.get("/lines")
def get_lines(
    day: date = Depends(game_day),
    season: int | None = Query(None, description="e.g. 20252026; default the current season"),
    data: SiteData = Depends(get_data),
) -> dict:
    """Every forward line, D pair, power-play and penalty-kill unit a team has iced in ``season``
    (from the shifts) for at least :data:`MIN_UNIT_SHARE` of its time in that state: players, time
    and results together, summed current ratings (best xGD first), and the slot when the unit is in
    today's projected lineup."""
    r = _rankings(data, day)
    current = r["season"]
    season = season or current
    if season not in (current, current - 10001):
        raise HTTPException(status_code=400, detail=f"season must be {current} or {current - 10001}")
    units = data.units(season, season == current)
    if units is None or units.is_empty():
        return {**_meta(r), "line_season": season, "seasons": [current, current - 10001], "lines": []}

    units = units.filter(pl.col("toi_share") >= MIN_UNIT_SHARE)
    lines = _unit_key(rankings.rate_units(r["tables"], units))
    if season == current:
        slots = _unit_key(rankings.current_slots(r["lines"], r["lineups"])).select("team_id", "kind", "_key", "slot")
        lines = lines.join(slots, on=["team_id", "kind", "_key"], how="left")
    else:
        lines = lines.with_columns(pl.lit(None, dtype=pl.String).alias("slot"))
    abbrs = data.games().filter(pl.col("season") == season).select(
        pl.col("home_team_id").cast(pl.Int64).alias("team_id"), pl.col("home_abbr").alias("team_abbr")).unique("team_id")
    lines = lines.join(abbrs, on="team_id", how="left")
    uc = data.processed(keys.unit_context(season), season == current)
    if uc is not None:
        # 5v5 decomposition while the whole unit was on the ice (forward lines and D pairs).
        uc = _unit_key(uc).select(
            "team_id", "kind", "_key", "tier", pl.col("toi_s").alias("ctx_toi_s"),
            *[pl.col(f"{p}_{s}").alias(f"ctx_{p}_{s}") for p in ("actual", "own", "mates", "comp", "zone", "ctx", "resid") for s in ("f", "a")])
        lines = lines.join(uc, on=["team_id", "kind", "_key"], how="left")
    lines = lines.drop("_key").sort("xgd60", descending=True)

    skaters = {p["player_id"]: p for p in r["players"].select(
        "player_id", "player_name", "position", "ev_off", "ev_def", "ev_net").to_dicts()}
    names = data.player_names()
    styles = archetype_lookup(data, season)
    order = {"L": 0, "C": 1, "R": 2, "D": 3}  # forwards then defensemen (PP/PK units mix both)
    out = []
    for row in rows(lines):
        players = [{**skaters.get(pid, {}), "player_id": pid,
                    "player_name": (skaters.get(pid) or {}).get("player_name") or names.get(pid) or str(pid),
                    "archetype": (styles.get(pid) or {}).get("archetype")}
                   for pid in row.pop("player_ids")]
        row["players"] = sorted(players, key=lambda p: order.get(p.get("position") or "", 9))
        out.append(row)
    return {**_meta(r), "line_season": season, "seasons": [current, current - 10001], "lines": out}
