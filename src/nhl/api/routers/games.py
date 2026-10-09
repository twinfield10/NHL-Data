"""``/api/games/{game_id}``: everything behind one slate row."""

from __future__ import annotations

import logging
import numpy as np
import polars as pl
from fastapi import APIRouter, Depends, HTTPException

from nhl.api import lineupstats
from nhl.api import markets as mk
from nhl.api.data import SiteData
from nhl.api.deps import get_data
from nhl.api.lineorder import order_lineup, order_unit
from nhl.api.routers.slate import with_venue
from nhl.api.serialize import rows, with_selection
from nhl.api.teaminfo import team_context
from nhl.betting import blend
from nhl.storage import keys

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/games", tags=["games"])

#: Lineup slots in display order: forward lines, D pairs, then anything unslotted.
SLOT_ORDER = ["f1", "f2", "f3", "f4", "d1", "d2", "d3"]


def _named(df: pl.DataFrame, names: dict[int, str]) -> pl.DataFrame:
    """Add ``player_name`` from the catalog (``Unknown`` for a null/unknown id)."""
    labels = [names.get(pid, "Unknown") if pid is not None else "Unknown" for pid in df["player_id"]]
    return df.with_columns(pl.Series("player_name", labels, dtype=pl.String))


def _lineup(df: pl.DataFrame | None, team_id: int, names: dict[int, str], hands: dict[int, str] | None = None) -> list[dict]:
    """One team's projected lineup, ordered by slot, then LW-C-RW / LD-RD within each line and pair."""
    if df is None or df.is_empty():
        return []
    team = _named(df.filter(pl.col("team_id") == team_id), names)
    order = {s: i for i, s in enumerate(SLOT_ORDER)}
    pos = {"L": 0, "C": 1, "R": 2, "D": 3}
    team = team.with_columns(
        pl.col("slot").replace_strict(order, default=len(SLOT_ORDER), return_dtype=pl.Int32).alias("_slot"),
        pl.col("position").replace_strict(pos, default=9, return_dtype=pl.Int32).alias("_pos"),
    ).sort("_slot", "_pos", -pl.col("s5"))
    return order_lineup(rows(team, drop=("_slot", "_pos", "as_of", "stamp", "game_id")), hands)


def _goalies(df: pl.DataFrame | None, team_id: int, names: dict[int, str]) -> list[dict]:
    """One team's starter probabilities, most likely first (a null id is 'someone else')."""
    if df is None or df.is_empty():
        return []
    team = _named(df.filter(pl.col("team_id") == team_id), names).with_columns(
        pl.when(pl.col("player_id").is_null()).then(pl.lit("Other")).otherwise(pl.col("player_name")).alias("player_name")
    )
    return rows(team.sort("p_start", descending=True), drop=("as_of", "stamp", "game_id"))


def market_view(data: SiteData, game: dict) -> dict:
    """Every book's current line, the consensus and the model through the day, and the
    regulation three-way, per market (see :mod:`nhl.api.markets`)."""
    start = mk.start_utc(game)
    try:
        quotes = mk.game_quotes(data.live_odds(int(game["season"])), game["game_id"], start)
    except Exception:  # a bad odds file must not take the page down
        logger.exception("odds failed for game %s", game["game_id"])
        quotes = mk.game_quotes(pl.DataFrame(), game["game_id"], None)
    history, state = mk.replay(quotes)
    now = mk.current(history)
    prices = data.game_prices(game["game_date"], game["game_id"])
    blend_model = data.blend_model()
    model = mk.model_history(prices, {m: c["line"] for m, c in now.items()}, blend_model) if prices is not None else []
    matrix = np.asarray(prices.sort("as_of")["score_matrix"][-1]) if prices is not None else None
    books = {m: sorted(({"book": b, **v} for b, v in bs.items()), key=lambda r: r["book"]) for m, bs in state.items()}
    return {
        "start": start, "history": history, "model": model, "books": books, "consensus": now,
        "three_way": mk.three_way_card(matrix, now.get("moneyline_3way"), now.get("moneyline"), blend_model,
                                       blend.segment_of(game["game_date"])),
    }


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
        "lineups": {"home": _lineup(lineups, game["home_team_id"], names, data.player_hands()),
                    "away": _lineup(lineups, game["away_team_id"], names, data.player_hands())},
        "goalies": {"home": _goalies(goalies, game["home_team_id"], names),
                    "away": _goalies(goalies, game["away_team_id"], names)},
        "edges": rows(edges),
        "history": rows(data.price_history(day, game_id)),
        "markets": market_view(data, game),
    }


def _season_tables(data: SiteData, season: int, current: bool) -> dict:
    """On-ice summaries, observed units and goalie starts for one season."""
    return {
        "onice": data.processed(keys.onice_context_summary(season), current),
        "units": data.units(season, current),
        "starts": data.processed(keys.goalie_starts(season), current),
    }


@router.get("/{game_id}/lineups")
def get_game_lineups(game_id: int, data: SiteData = Depends(get_data)) -> dict:
    """Projected lineups and starters with the stats behind them: skater ratings and on-ice 5v5
    results, each projected unit's record together, goalie workload and save talent (this season
    and last), and the DailyFaceoff reports and source tweets they come from."""
    game = data.game(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail=f"unknown game {game_id}")
    day, season = game["game_date"], int(game["season"])
    names = data.player_names()
    slate = data.day_slate(day)
    row = slate.filter(pl.col("game_id") == game_id) if slate is not None else None
    stamp = row["stamp"][0] if row is not None and row.height else None
    lineups, goalies = data.lineups(day, game_id, stamp), data.goalies(day, game_id, stamp)

    try:
        r = data.rankings(day)
    except Exception:  # ratings are enrichment; the lineup itself still shows
        logger.exception("rankings failed for %s", day)
        r = None
    skaters = {} if r is None else {
        p["player_id"]: p for p in r["players"].select("player_id", "ev_off", "ev_def", "ev_net", "ev_toi_s").to_dicts()}
    goalie_ratings = {} if r is None else {
        g["player_id"]: g for g in r["goalies"].select("player_id", "save", "save_sd", "save_prior").to_dicts()}
    seasons = {"cur": _season_tables(data, season, True), "prev": _season_tables(data, season - 10001, False)}
    start = mk.start_utc(game)
    dfo_lines, dfo_goalies, tweets = data.dfo_lines(season), data.dfo_goalies(season), data.tweets()

    out = {}
    for side in ("home", "away"):
        team_id, abbr = game[f"{side}_team_id"], game[f"{side}_abbr"]
        players = _lineup(lineups, team_id, names, data.player_hands())
        pids = [p["player_id"] for p in players if p["player_id"] is not None]
        onice = {k: lineupstats.onice(t["onice"], pids) for k, t in seasons.items()}
        for p in players:
            p["rating"] = skaters.get(p["player_id"])
            p["onice"] = {k: onice[k].get(p["player_id"]) for k in seasons}
        by_id = {p["player_id"]: p for p in players}
        units = [{**u, "player_ids": [p["player_id"] for p in order_unit([by_id[i] for i in u["player_ids"]], data.player_hands())],
                  "record": {k: lineupstats.unit_record(t["units"], u["kind"], u["player_ids"]) for k, t in seasons.items()}}
                 for u in lineupstats.unit_groups(players)]
        gs = _goalies(goalies, team_id, names)
        for g in gs:
            g["rating"] = goalie_ratings.get(g["player_id"])
            g["season"] = {k: lineupstats.goalie_season(t["starts"], g["player_id"]) for k, t in seasons.items()}
        out[side] = {
            "players": players, "units": units, "goalies": gs,
            "sources": {"lines": lineupstats.lines_source(dfo_lines, tweets, abbr, start),
                        "goalie": lineupstats.goalie_source(dfo_goalies, tweets, game_id, abbr, start)},
        }
    return {"season": season, **out}
