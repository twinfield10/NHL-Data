"""Usage and context (usage plan phase E): a skater's on-ice decomposition, deployment and
linemates, and a team's line-matchup matrices.

* ``/api/ratings/players/{player_id}/context``: this season and last, one row per team.
* ``/api/ratings/teams/{team_id}/matchups``: own tier × opponent tier for one season.
"""

from __future__ import annotations

from datetime import date

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import json_view, rows
from nhl.site import views
from nhl.storage import keys
from nhl.usage import matchups

router = APIRouter(prefix="/api/ratings", tags=["context"])

#: Decomposition parts, in display order.
PARTS = ("own", "mates", "comp", "zone", "ctx", "resid")
LINEMATES = 6


def _seasons(data: SiteData, day: date) -> tuple[int, int]:
    r = data.rankings(day)
    if r is None:
        raise HTTPException(status_code=404, detail=f"no rating snapshot on or before {day}")
    return r["season"], r["season"] - 10001


def _abbrs(data: SiteData, season: int) -> dict[int, str]:
    g = data.games().filter(pl.col("season") == season)
    return dict(zip(g["home_team_id"].cast(pl.Int64).to_list(), g["home_abbr"].to_list()))


def parts(row: dict) -> dict:
    """``{part: {f, a, d}}`` per 60 from a summary row (d = f − a; for xGA lower is better)."""
    out = {p: {"f": row.get(f"{p}_f"), "a": row.get(f"{p}_a")} for p in (*PARTS, "actual", "league")}
    for v in out.values():
        v["d"] = None if v["f"] is None or v["a"] is None else v["f"] - v["a"]
    return out


@router.get("/players/{player_id}/context")
def player_context(player_id: int, day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """On-ice decomposition, QoT/QoC, deployment and top linemates, this season and last."""
    current, previous = _seasons(data, day)
    names = data.player_names()
    ratings = data.rankings(day)["players"].select("player_id", "ev_net")
    net = dict(zip(ratings["player_id"].to_list(), ratings["ev_net"].to_list()))
    out = []
    for season in (current, previous):
        is_cur = season == current
        oc = data.processed(keys.onice_context_summary(season), is_cur)
        us = data.processed(keys.usage_summary(season), is_cur)
        lm = data.processed(keys.linemates(season), is_cur)
        if oc is None:
            continue
        mine = oc.filter(pl.col("player_id") == player_id)
        abbr = _abbrs(data, season)
        for row in rows(mine.sort("toi_s", descending=True)):
            tid = row["team_id"]
            use = {}
            if us is not None:
                u = rows(us.filter((pl.col("player_id") == player_id) & (pl.col("team_id") == tid)))
                use = u[0] if u else {}
            mates = []
            if lm is not None:
                top = lm.filter((pl.col("player_id") == player_id) & (pl.col("team_id") == tid)).sort("shared_s", descending=True).head(LINEMATES)
                mates = [{"player_id": m["mate_id"], "player_name": names.get(m["mate_id"]) or str(m["mate_id"]),
                          "shared_s": m["shared_s"], "share": m["shared_s"] / row["toi_s"] if row["toi_s"] else None,
                          "games": m["games"], "ev_net": net.get(m["mate_id"])} for m in rows(top)]
            out.append({
                "season": season, "team_id": tid, "team_abbr": abbr.get(tid), "group": row["group"],
                "games": row["games"], "toi_s": row["toi_s"], "parts": parts(row),
                **{k: row.get(k) for k in ("qot_net", "qoc_net", "qot_net_pct", "qoc_net_pct", "qot_toi", "qoc_toi",
                                           "own_xgd_pct", "mates_xgd_pct", "comp_xgd_pct")},
                "usage": {k: v for k, v in use.items() if k not in ("season", "player_id", "team_id")},
                "linemates": mates,
            })
    return {"player_id": player_id, "player_name": names.get(player_id), "season": current, "rows": out}


@router.get("/teams/{team_id}/matchups")
def team_matchups(
    team_id: int,
    day: date = Depends(game_day),
    season: int | None = Query(None, description="e.g. 20252026; default the current season"),
    data: SiteData = Depends(get_data),
) -> dict:
    """Own tier × opponent tier matching ratios (1 = no matching), the coach matching index
    (forward-tier mutual information, with its league percentile that season) and top pair vs
    opponent top line at home (last change) and away."""
    current, previous = _seasons(data, day)
    season = season or current
    if season not in (current, previous):
        raise HTTPException(status_code=400, detail=f"season must be {current} or {previous}")
    if (raw := data.view(views.matchups_key(season, team_id))) is not None:
        return json_view(raw)
    base = {"team_id": team_id, "season": season, "seasons": [current, previous]}
    return build_matchups(data, season, current, previous).get(team_id) or {**base, "cells": [], "coaches": [], "index": None}


def build_matchups(data: SiteData, season: int, current: int, previous: int) -> dict[int, dict]:
    """``team_id -> /api/ratings/teams/{team_id}/matchups`` payload for every team in ``season``
    (the league matrices and matching index are computed once for all of them)."""
    m = data.processed(keys.usage_matchups(season), season == current)
    if m is None or m.is_empty():
        return {}
    by = ["season", "team_id"]
    league = matchups.matching_matrix(m.group_by(*by, "own_tier", "opp_tier").agg(pl.col("seconds").sum()), by)
    venue = matchups.matching_matrix(m.group_by(*by, "venue", "own_tier", "opp_tier").agg(pl.col("seconds").sum()), [*by, "venue"])
    mi = matchups.intensity(league, by)
    mi = mi.with_columns((pl.col("mi_bits").rank("average") / pl.len()).alias("pct"))
    ct = data.processed(keys.coaches(season), season == current)
    coach_names = {} if ct is None else dict(zip(ct["coach_id"].to_list(), ct["head_coach"].to_list()))
    out = {}
    for team_id in m["team_id"].drop_nulls().unique().sort().to_list():
        hit = mi.filter(pl.col("team_id") == team_id)
        if hit.is_empty():
            continue
        me = hit.row(0, named=True)
        cells = pl.concat([
            league.filter(pl.col("team_id") == team_id).with_columns(pl.lit("all").alias("venue")),
            venue.filter(pl.col("team_id") == team_id),
        ], how="diagonal_relaxed").select("venue", "own_tier", "opp_tier", "seconds", "share", "ratio")
        d1 = {r["venue"]: r["ratio"] for r in rows(cells.filter((pl.col("own_tier") == "D1") & (pl.col("opp_tier") == "F1")))}
        coaches = m.filter(pl.col("team_id") == team_id).group_by("coach_id").agg(pl.col("seconds").sum()).sort("seconds", descending=True)
        out[team_id] = {
            "team_id": team_id, "season": season, "seasons": [current, previous], "cells": rows(cells),
            "coaches": [coach_names.get(c) or c for c in coaches["coach_id"].drop_nulls().to_list()],
            "index": {"mi_bits": me["mi_bits"], "pct": me["pct"], "f1_vs_f1": me["f1_vs_f1"],
                      "d1_f1_home": d1.get("home"), "d1_f1_away": d1.get("away"), "teams": mi.height},
        }
    return out
