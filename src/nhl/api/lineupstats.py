"""Stats behind a game's projected lineups: skater ratings and on-ice results, how each projected
line / pair / special-teams unit has done together, goalie workload and save talent, and the
DailyFaceoff reports (and source tweets) the lineup and starters come from.

Season blocks are ``{"cur": ..., "prev": ...}`` (this season and last); a skater traded within a
season is summed across his teams.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

#: Lineup slot -> (unit kind as in :func:`nhl.ratings.rankings.observed_units`, label).
UNIT_SLOTS = {"f1": "F", "f2": "F", "f3": "F", "f4": "F", "d1": "D", "d2": "D", "d3": "D"}
#: Players per unit kind.
UNIT_SIZE = {"F": 3, "D": 2, "PP": 5, "PK": 4}


def onice(summary: pl.DataFrame | None, pids: list[int]) -> dict[int, dict]:
    """``player_id -> {games, toi_s, xgf60, xga60}``: on-ice 5v5 rates from a season's
    ``onice_context_summary`` (TOI-weighted across teams)."""
    if summary is None or not pids:
        return {}
    df = summary.filter(pl.col("player_id").is_in(pids) & (pl.col("toi_s") > 0)).group_by("player_id").agg(
        pl.col("games").sum(), pl.col("toi_s").sum(),
        ((pl.col("actual_f") * pl.col("toi_s")).sum() / pl.col("toi_s").sum()).alias("xgf60"),
        ((pl.col("actual_a") * pl.col("toi_s")).sum() / pl.col("toi_s").sum()).alias("xga60"),
    )
    return {r.pop("player_id"): r for r in df.iter_rows(named=True)}


def unit_groups(lineup: list[dict]) -> list[dict]:
    """Projected units from lineup rows: forward lines and pairs by ``slot``, power-play and
    penalty-kill units by ``pp_unit`` / ``pk_unit`` (``slot, kind, player_ids``). A slot holding a
    game-time decision and his backup keeps the players most likely to dress, up to the unit's size."""
    groups: dict[str, dict] = {}
    for p in sorted(lineup, key=lambda r: -(r.get("p_dressed") if r.get("p_dressed") is not None else 1.0)):
        pid = p.get("player_id")
        if pid is None:
            continue
        keys = []
        if p.get("slot") in UNIT_SLOTS:
            keys.append((p["slot"], UNIT_SLOTS[p["slot"]]))
        if p.get("pp_unit"):
            keys.append((f"pp{p['pp_unit']}", "PP"))
        if p.get("pk_unit"):
            keys.append((f"pk{p['pk_unit']}", "PK"))
        for slot, kind in keys:
            ids = groups.setdefault(slot, {"slot": slot, "kind": kind, "player_ids": []})["player_ids"]
            if len(ids) < UNIT_SIZE[kind]:
                ids.append(pid)
    order = [*UNIT_SLOTS, "pp1", "pp2", "pk1", "pk2"]
    return sorted(groups.values(), key=lambda g: order.index(g["slot"]) if g["slot"] in order else len(order))


def unit_record(units: pl.DataFrame | None, kind: str, player_ids: list[int]) -> dict | None:
    """Time and results a group of players has had on the ice together in a season (summed over
    teams): ``toi_s, games, xgf, xga, gf, ga``; None if never together."""
    if units is None or units.is_empty():
        return None
    key = sorted(player_ids)
    hit = units.filter((pl.col("kind") == kind) & (pl.col("player_ids").list.sort() == key))
    if hit.is_empty():
        return None
    return {c: hit[c].sum() for c in ("toi_s", "games", "xgf", "xga", "gf", "ga")}


def goalie_season(starts: pl.DataFrame | None, pid: int | None) -> dict | None:
    """A goalie's starts in a season: ``starts, shots_against, goals_against, xga, gsax, sv_pct``."""
    if starts is None or pid is None:
        return None
    mine = starts.filter(pl.col("starter") == pid)
    if mine.is_empty():
        return None
    sa, ga = int(mine["shots_against"].sum()), int(mine["goals_against"].sum())
    return {"starts": mine.height, "shots_against": sa, "goals_against": ga, "xga": float(mine["xga"].sum()),
            "gsax": float(mine["gsax"].sum()), "sv_pct": (sa - ga) / sa if sa else None}


def _tweet(tweets: pl.DataFrame | None, url: str | None) -> dict | None:
    if tweets is None or not url:
        return None
    hit = tweets.filter((pl.col("url") == url) & pl.col("available").fill_null(False))
    if hit.is_empty():
        return None
    r = hit.row(0, named=True)
    return {"text": r["text"], "author_name": r["author_name"], "author_handle": r["author_handle"], "created_at": r["created_at"]}


def lines_source(dfo_lines: pl.DataFrame | None, tweets: pl.DataFrame | None, team: str, before: datetime | None) -> dict | None:
    """The DailyFaceoff line-combination version in effect for ``team`` (latest captured before
    ``before``): who it cites, the source link and tweet, and when it was updated."""
    if dfo_lines is None:
        return None
    df = dfo_lines.filter(pl.col("team") == team)
    if before is not None:
        df = df.filter(pl.col("captured_at") < before)
    if df.is_empty():
        return None
    r = df.sort("updated_at", "captured_at").row(-1, named=True)
    return {"source_name": r["source_name"], "url": r["source_url"], "updated_at": r["updated_at"],
            "tweet": _tweet(tweets, r["source_url"])}


def goalie_source(dfo_goalies: pl.DataFrame | None, tweets: pl.DataFrame | None, game_id: int, team: str,
                  before: datetime | None) -> dict | None:
    """DailyFaceoff's latest starter report for ``team`` in ``game_id``: goalie, status, details,
    the source link and tweet."""
    if dfo_goalies is None:
        return None
    df = dfo_goalies.filter((pl.col("game_id") == game_id) & (pl.col("team") == team))
    if before is not None:
        df = df.filter(pl.col("captured_at") < before)
    if df.is_empty():
        return None
    r = df.sort("captured_at").row(-1, named=True)
    return {"goalie_name": r["goalie_name"], "status": r["status"], "details": r["news_details"],
            "source_name": r["news_source_name"], "url": r["news_source_url"], "updated_at": r["news_created_at"],
            "tweet": _tweet(tweets, r["news_source_url"])}
