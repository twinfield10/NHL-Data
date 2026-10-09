"""``/api/ratings/players`` and ``/api/ratings/teams``: rankings from the latest rating snapshot."""

from __future__ import annotations

from datetime import date

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.lineorder import order_lineup, order_unit
from nhl.api.lineupstats import spread
from nhl.api.serialize import json_view, rows
from nhl.api.routers.style import archetype_lookup
from nhl.api.teaminfo import team_context
from nhl.ratings import rankings
from nhl.site import views
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


def _scales(df: pl.DataFrame, cols: dict[str, str], center: dict[str, float] | None = None, q: float = 0.95) -> dict[str, float]:
    """League heat-color scales for the site (``heat(value, scale)``): ``name -> spread`` of each column."""
    center = center or {}
    return {k: spread(df[c].to_list(), center.get(k, 0.0), q) for k, c in cols.items()}


@router.get("/players")
def get_players(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """Every rated skater and goalie, best 5v5 net / save talent first."""
    if (raw := data.view(views.ratings_key("players"), day)) is not None:
        return json_view(raw)
    return build_players(data, day)


def build_players(data: SiteData, day: date) -> dict:
    """The ``/api/ratings/players`` payload, with league color ``scales`` from every rated player on a team."""
    r = _rankings(data, day)
    styles = archetype_lookup(data, r["season"])
    skaters = rows(r["players"].sort("ev_net", descending=True, nulls_last=True))
    for p in skaters:
        p.update(styles.get(p["player_id"], {"archetype": None, "archetype_conf": None, "style_group": None}))
    on_team = pl.col("team_abbr").is_not_null()
    return {
        **_meta(r),
        "skaters": skaters,
        "goalies": rows(r["goalies"].sort("save", descending=True, nulls_last=True)),
        "scales": {
            "skaters": _scales(r["players"].filter(on_team), {"xgd": "ev_net", "xgf": "ev_off", "xga": "ev_def", "pp": "pp_off",
                                                              "pk": "pk_def", "fin": "finishing"}),
            "goalies": _scales(r["goalies"].filter(on_team), {"save": "save", "gsax": "gsax"}),
        },
    }


@router.get("/teams")
def get_teams(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """Every team's composed rating (best 5v5 goal differential first), its likely goalies and
    the projected lineup behind it, plus the league-average 5v5 and PP xG/60 they're measured against."""
    if (raw := data.view(views.ratings_key("teams"), day)) is not None:
        return json_view(raw)
    return build_teams(data, day)


def build_teams(data: SiteData, day: date) -> dict:
    """The ``/api/ratings/teams`` payload, with heat ``scales`` across all teams (xGF/xGA and PP/PK
    centred on the league average)."""
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

    hands = data.player_hands()
    teams = []
    for row in rows(board):
        tid = row["team_id"]
        teams.append({
            **row,
            **context.get(row["abbr"], {}),
            "goalies": rows(goalies.filter(pl.col("team_id") == tid).sort("weight", descending=True), drop=("team_id",)),
            "lineup": order_lineup(rows(dep.filter(pl.col("team_id") == tid).select(
                "player_id", "player_name", "position", "slot", "s5", "spp", "spk", "ev_off", "ev_def", "ev_net")), hands),
        })
    ev, pp = r["league"]["xg60_5v5"], r["league"]["xg60_pp"]
    scales = _scales(board, {"gd": "gd60", "xgd": "xgd60", "xgf": "xgf60", "xga": "xga60", "fin": "finishing", "save": "save",
                             "pp": "pp_xgf60", "pk": "pk_xga60"}, {"xgf": ev, "xga": ev, "pp": pp, "pk": pp}, q=1.0)
    return {**_meta(r), "league": r["league"], "teams": teams, "scales": scales}


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
    if (raw := data.view(views.ratings_key(f"lines_{season}"), day)) is not None:
        return json_view(raw)
    return build_lines(data, day, season)


def build_lines(data: SiteData, day: date, season: int) -> dict:
    """The ``/api/ratings/lines`` payload for ``season``, with heat ``scales`` per unit kind (from
    every unit of that kind sent, so filtering doesn't change the colors)."""
    r = _rankings(data, day)
    current = r["season"]
    units = data.units(season, season == current)
    if units is None or units.is_empty():
        return {**_meta(r), "line_season": season, "seasons": [current, current - 10001], "lines": [], "scales": {}}

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
    scales = {k: _scales(lines.filter(pl.col("kind") == k), {"xgd": "xgd60", "xgf": "xgf60", "xga": "xga60"})
              for k in lines["kind"].unique().sort().to_list()}

    skaters = {p["player_id"]: p for p in r["players"].select(
        "player_id", "player_name", "position", "ev_off", "ev_def", "ev_net").to_dicts()}
    names = data.player_names()
    styles = archetype_lookup(data, season)
    hands = data.player_hands()
    out = []
    for row in rows(lines):
        players = [{**skaters.get(pid, {}), "player_id": pid,
                    "player_name": (skaters.get(pid) or {}).get("player_name") or names.get(pid) or str(pid),
                    "archetype": (styles.get(pid) or {}).get("archetype")}
                   for pid in row.pop("player_ids")]
        row["players"] = order_unit(players, hands)
        out.append(row)
    return {**_meta(r), "line_season": season, "seasons": [current, current - 10001], "lines": out, "scales": scales}
