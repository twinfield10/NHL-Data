"""Team context for game cards: names, records, last-10 and streaks as of a game date."""

from __future__ import annotations

from datetime import date

import polars as pl

from nhl import teams as teams_mod

L10 = 10


def team_games(games: pl.DataFrame) -> pl.DataFrame:
    """Final regular-season games as one row per team: ``abbr, season, game_date, game_id, result``.

    ``result`` is ``W``, ``L`` or ``OTL`` (lost in overtime or a shootout).
    """
    final = games.filter(pl.col("is_final") & (pl.col("season_type") == "R"))
    sides = []
    for us, them in (("home", "away"), ("away", "home")):
        sides.append(final.select(
            pl.col(f"{us}_abbr").alias("abbr"), "season", "game_date", "game_id",
            pl.when(pl.col(f"{us}_score") > pl.col(f"{them}_score")).then(pl.lit("W"))
            .when(pl.col("last_period") > 3).then(pl.lit("OTL"))
            .otherwise(pl.lit("L")).alias("result"),
        ))
    return pl.concat(sides).sort("abbr", "game_date", "game_id")


def _record(results: pl.Series) -> dict:
    """W-L-OTL counts, points and points % for a series of results."""
    w, l, otl = (int((results == r).sum()) for r in ("W", "L", "OTL"))
    gp = w + l + otl
    return {"gp": gp, "w": w, "l": l, "otl": otl, "pts": 2 * w + otl, "pts_pct": (2 * w + otl) / (2 * gp) if gp else None}


def _streak(results: list[str]) -> str | None:
    """Current streak in the NHL's notation, e.g. ``W3``, ``L2`` or ``OT1``, from oldest-first results."""
    if not results:
        return None
    last = results[-1]
    n = 0
    for r in reversed(results):
        if r != last:
            break
        n += 1
    return f"{'OT' if last == 'OTL' else last}{n}"


def team_context(games: pl.DataFrame, abbrs: list[str], day: date, season: int) -> dict[str, dict]:
    """Per team: names, season record before ``day``, last 10, streak, and last season's record
    when the team hasn't played yet this season."""
    tg = team_games(games).filter(pl.col("abbr").is_in(abbrs))
    current = tg.filter((pl.col("season") == season) & (pl.col("game_date") < day))
    prev_season = (season // 10000 - 1) * 10001 + 1  # 20262027 -> 20252026
    previous = tg.filter(pl.col("season") == prev_season)

    out = {}
    for abbr in abbrs:
        mine = current.filter(pl.col("abbr") == abbr)["result"]
        info = teams_mod.resolve(abbr, season)
        entry = {
            "abbr": abbr,
            "place": info.place_name if info else abbr,
            "name": info.common_name if info else abbr,
            "record": _record(mine),
            "l10": _record(mine.tail(L10)),
            "streak": _streak(mine.to_list()),
            "prev_record": None,
        }
        if entry["record"]["gp"] == 0:
            prev = previous.filter(pl.col("abbr") == abbr)["result"]
            entry["prev_record"] = _record(prev) if prev.len() else None
        out[abbr] = entry
    return out
