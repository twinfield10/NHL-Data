"""Player style (archetypes plan phase D): style axes, forward archetype and style comps.

* ``/api/ratings/players/{player_id}/style``: the current view (this season plus half of last,
  the ``2yr`` window) and last season, plus the player's archetype by season since 2010-11.

The axes lead; the forward archetype is a summary (phase C found the axes carry the signal).
Defencemen have axes and comps but no archetype (no stable defence clusters).
"""

from __future__ import annotations

from datetime import date

import polars as pl
from fastapi import APIRouter, Depends, HTTPException

from nhl import config
from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.archetypes.model import AXES, AXIS_LABELS, FIT_MIN_MINUTES
from nhl.storage import keys

router = APIRouter(prefix="/api/ratings", tags=["style"])

#: Short display names for the axes.
AXIS_NAMES = {
    "perimeter": "Perimeter", "shooter": "Shot volume", "release": "Release", "physical": "Physical",
    "size": "Size", "defensive": "Defensive role", "centre": "Centre", "wrister": "Wrist shot",
    "activation": "Activation", "shot_blocker": "Shot blocking",
}
FIRST_SEASON = config.season_id(2010)


def _seasons(data: SiteData, day: date) -> tuple[int, int]:
    r = data.rankings(day)
    if r is None:
        raise HTTPException(status_code=404, detail=f"no rating snapshot on or before {day}")
    return r["season"], r["season"] - 10001


def archetype_lookup(data: SiteData, season: int) -> dict[int, dict]:
    """``player_id -> {archetype, archetype_conf, style_group}`` from ``season``'s ``2yr`` window
    (falls back to the previous season's when the current table doesn't exist yet)."""
    for s, current in ((season, True), (season - 10001, False)):
        a = data.processed(keys.archetypes(s), current)
        if a is not None:
            a = a.filter(pl.col("window") == "2yr")
            cols = [c for c in ("archetype", "confidence") if c in a.columns]
            return {r["player_id"]: {"archetype": r.get("archetype"), "archetype_conf": r.get("confidence"),
                                     "style_group": r["group"]}
                    for r in a.select("player_id", "group", *cols).iter_rows(named=True)}
    return {}


def _view(row: dict, names: dict[int, str], label: str) -> dict:
    g = row["group"]
    probs = sorted(
        ({"name": k[2:].replace("_", " ").replace("two way", "two-way"), "p": v}
         for k, v in row.items() if k.startswith("p_") and v is not None),
        key=lambda x: -x["p"])
    return {
        "season": row["season"], "window": row["window"], "label": label, "group": g,
        "toi_5v5_min": row["toi_5v5_min"], "reliable": row["toi_5v5_min"] >= FIT_MIN_MINUTES,
        "archetype": row.get("archetype"), "confidence": row.get("confidence"), "probs": probs,
        "axes": [{"key": a, "name": AXIS_NAMES[a], "label": AXIS_LABELS[a], "value": row.get(a),
                  "pct": row.get(f"{a}_pct")} for a in AXES[g]],
        "comps": [{"player_id": c["player_id"], "player_name": names.get(c["player_id"]) or str(c["player_id"]),
                   "season": c["season"], "distance": c["distance"]} for c in (row.get("comps") or [])],
    }


@router.get("/players/{player_id}/style")
def player_style(player_id: int, day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """Style axes (percentiles within F or D), forward archetype probabilities, style comps and
    archetype history for one skater."""
    current, previous = _seasons(data, day)
    names = data.player_names()
    views = []
    for season, window, label in ((current, "2yr", "Current (this season + ½ last)"), (previous, "season", "Last season")):
        a = data.processed(keys.archetypes(season), season == current)
        if a is None:
            continue
        hit = a.filter((pl.col("player_id") == player_id) & (pl.col("window") == window))
        if hit.height:
            views.append(_view(hit.row(0, named=True), names, label))
    history = []
    for season in range(FIRST_SEASON, current + 1, 10001):
        a = data.processed(keys.archetypes(season), season == current)
        if a is None:
            continue
        hit = a.filter((pl.col("player_id") == player_id) & (pl.col("window") == "season"))
        if hit.height:
            r = hit.row(0, named=True)
            history.append({"season": season, "archetype": r.get("archetype"), "confidence": r.get("confidence"),
                            "toi_5v5_min": r["toi_5v5_min"]})
    return {"player_id": player_id, "player_name": names.get(player_id), "season": current,
            "views": views, "history": history}
