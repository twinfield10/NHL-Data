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
    teams): ``toi_s, games, xgf, xga, gf, ga`` and ``toi_share`` (of those teams' time in that
    state, when known); None if never together."""
    if units is None or units.is_empty():
        return None
    key = sorted(player_ids)
    hit = units.filter((pl.col("kind") == kind) & (pl.col("player_ids").list.sort() == key))
    if hit.is_empty():
        return None
    out = {c: hit[c].sum() for c in ("toi_s", "games", "xgf", "xga", "gf", "ga")}
    team_toi = hit["team_toi_s"].sum() if "team_toi_s" in hit.columns else None
    out["toi_share"] = out["toi_s"] / team_toi if team_toi else None
    return out


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


#: Special-teams unit kind -> per-player prefix.
SPECIAL = {"PP": "pp", "PK": "pk"}
#: A skater counts toward a league color scale with at least this share of his team's time in
#: that state (a regular, so early-season one-shift samples don't stretch the scale).
REGULAR_SHARE = 0.25


def special_teams(units: pl.DataFrame | None) -> pl.DataFrame | None:
    """Every skater's power-play (5v4) and penalty-kill (4v5) usage in a season, from the observed
    units: ``player_id, kind, toi_s, share`` (of his teams' time in that state), ``xgf60, xga60``
    on ice. Summed over teams for a traded player."""
    if units is None or units.is_empty():
        return None
    per_team = (
        units.filter(pl.col("kind").is_in(list(SPECIAL))).explode("player_ids", empty_as_null=False).rename({"player_ids": "player_id"})
        .group_by("player_id", "team_id", "kind")
        .agg(pl.col("toi_s").sum(), pl.col("xgf").sum(), pl.col("xga").sum(), pl.col("team_toi_s").first())
    )
    return per_team.group_by("player_id", "kind").agg(
        pl.col("toi_s").sum(), pl.col("xgf").sum(), pl.col("xga").sum(), pl.col("team_toi_s").sum(),
    ).filter(pl.col("toi_s") > 0).select(
        "player_id", "kind", "toi_s",
        (pl.col("toi_s") / pl.col("team_toi_s")).alias("share"),
        (pl.col("xgf") * 3600 / pl.col("toi_s")).alias("xgf60"),
        (pl.col("xga") * 3600 / pl.col("toi_s")).alias("xga60"),
    )


def special_by_player(table: pl.DataFrame | None, pids: list[int]) -> dict[int, dict]:
    """``player_id -> {pp: {toi_s, share, xgf60, xga60} | None, pk: ...}`` for ``pids``."""
    out: dict[int, dict] = {pid: {"pp": None, "pk": None} for pid in pids}
    if table is None:
        return out
    for r in table.filter(pl.col("player_id").is_in(pids)).iter_rows(named=True):
        out[r.pop("player_id")][SPECIAL[r.pop("kind")]] = r
    return out


def spread(values: list[float | None], center: float = 0.0, q: float = 0.95) -> float:
    """The ``q`` quantile of ``|value - center|``: the saturation point of a heat column (as
    ``heatScale`` on the ratings pages), 1 when there is nothing to scale."""
    d = sorted(abs(v - center) for v in values if v is not None and v == v)
    if not d:
        return 1.0
    return d[min(len(d) - 1, int(q * (len(d) - 1)))] or 1.0


def _centered(rates: pl.Series, toi: pl.Series) -> dict:
    """``{center, scale}`` for a rate: TOI-weighted league mean and its spread."""
    if rates.is_empty() or toi.sum() <= 0:
        return {"center": 0.0, "scale": 1.0}
    center = float((rates * toi).sum() / toi.sum())
    return {"center": center, "scale": spread(rates.to_list(), center)}


def season_scales(onice_summary: pl.DataFrame | None, special: pl.DataFrame | None, starts: pl.DataFrame | None) -> dict:
    """League color scales for one season's results: on-ice 5v5 xGF / xGA / xGD per 60 (regular
    skaters: at least a quarter of the 90th-percentile TOI), power-play xGF/60 and penalty-kill
    xGA/60 (regulars on that unit), goalie Sv% and GSAx. Each is ``{center, scale}``."""
    out: dict[str, dict] = {}
    if onice_summary is not None and not onice_summary.is_empty():
        p = onice(onice_summary, onice_summary["player_id"].unique().to_list())
        df = pl.DataFrame(list(p.values()))
        df = df.filter(pl.col("toi_s") >= 0.25 * df["toi_s"].quantile(0.9))
        out["xgf"] = _centered(df["xgf60"], df["toi_s"])
        out["xga"] = _centered(df["xga60"], df["toi_s"])
        out["xgd"] = {"center": 0.0, "scale": spread((df["xgf60"] - df["xga60"]).to_list())}
    if special is not None:
        for kind, col in (("PP", "xgf60"), ("PK", "xga60")):
            reg = special.filter((pl.col("kind") == kind) & (pl.col("share") >= REGULAR_SHARE))
            out[SPECIAL[kind]] = _centered(reg[col], reg["toi_s"])
    if starts is not None and not starts.is_empty():
        g = starts.group_by("starter").agg(pl.col("shots_against").sum(), pl.col("goals_against").sum(), pl.col("gsax").sum(),
                                           pl.len().alias("n"))
        g = g.filter((pl.col("n") >= max(1, 0.25 * g["n"].quantile(0.9))) & (pl.col("shots_against") > 0))
        sv = 1 - g["goals_against"] / g["shots_against"]
        out["sv_pct"] = _centered(sv, g["shots_against"].cast(pl.Float64))
        out["gsax"] = {"center": 0.0, "scale": spread(g["gsax"].to_list())}
    return out


def rating_scales(skaters: pl.DataFrame | None, goalies: pl.DataFrame | None) -> dict:
    """League color scales for the talent ratings, from every rated player (centered at 0, the
    same scales as the ratings pages)."""
    out = {}
    if skaters is not None:
        for k in ("ev_off", "ev_def", "ev_net", "pp_off", "pk_def", "finishing"):
            out[k] = spread(skaters[k].to_list())
    if goalies is not None:
        out["save"] = spread(goalies["save"].to_list())
    return out


def team_special_teams(logs: pl.DataFrame | None) -> dict[int, dict]:
    """``team_id -> {pp: {pct, rank, goals, opps}, pk: {...}}``: regular-season power-play
    conversion (PP goals / opportunities) and penalty-kill success (1 − PP goals against / times
    shorthanded), ranked across the league (1 = best, ties share the better rank)."""
    if logs is None or logs.is_empty():
        return {}
    t = logs.filter(pl.col("game_id").cast(pl.String).str.slice(4, 2) == "02")
    opps = t.filter(pl.col("strength") == "all").select("team_id", "opp_team_id", "pp_opportunities")
    df = (
        opps.group_by("team_id").agg(pl.col("pp_opportunities").sum().alias("pp_opps"))
        .join(opps.group_by(pl.col("opp_team_id").alias("team_id")).agg(pl.col("pp_opportunities").sum().alias("pk_opps")),
              on="team_id", how="full", coalesce=True)
        .join(t.filter(pl.col("strength") == "PP").group_by("team_id").agg(pl.col("gf").sum().alias("pp_goals")),
              on="team_id", how="left")
        .join(t.filter(pl.col("strength") == "SH").group_by("team_id").agg(pl.col("ga").sum().alias("pk_goals")),
              on="team_id", how="left")
        .fill_null(0)
        .with_columns(
            pl.when(pl.col("pp_opps") > 0).then(pl.col("pp_goals") / pl.col("pp_opps")).alias("pp_pct"),
            pl.when(pl.col("pk_opps") > 0).then(1 - pl.col("pk_goals") / pl.col("pk_opps")).alias("pk_pct"),
        )
        .with_columns(
            pl.col("pp_pct").rank("min", descending=True).alias("pp_rank"),
            pl.col("pk_pct").rank("min", descending=True).alias("pk_rank"),
        )
    )
    return {r["team_id"]: {k: {"pct": r[f"{k}_pct"], "rank": r[f"{k}_rank"], "goals": r[f"{k}_goals"], "opps": r[f"{k}_opps"],
                               "teams": df.height} for k in ("pp", "pk")}
            for r in df.iter_rows(named=True)}
