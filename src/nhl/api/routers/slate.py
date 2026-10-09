"""``/api/slate``: one row per game for a date, plus input freshness."""

from __future__ import annotations

from datetime import date, timedelta

import logging

import numpy as np
import polars as pl
from fastapi import APIRouter, Depends

from nhl.api import markets as mk
from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import json_view, rows, with_selection
from nhl.api.teaminfo import team_context
from nhl.betting import blend
from nhl.site import views

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["slate"])


def _with_scores(slate: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """Attach final scores (null until the game is final) from the catalog."""
    scores = games.select("game_id", "is_final", "home_score", "away_score", "last_period")
    return slate.join(scores, on="game_id", how="left")


def with_venue(df: pl.DataFrame, data: SiteData) -> pl.DataFrame:
    """Attach ``venue_name`` and ``venue_location``: the arena recorded in the play-by-play, or
    for a game not played yet, the arena of the home team's latest home game (so current names)."""
    venues = data.venues()
    if venues is None:
        return df.with_columns(pl.lit(None, dtype=pl.String).alias("venue_name"), pl.lit(None, dtype=pl.String).alias("venue_location"))
    played = df.join(venues.select("game_id", "venue_name", "venue_location"), on="game_id", how="left")
    latest_home = venues.filter(~pl.col("is_neutral_site").fill_null(False)).sort("game_date").group_by("home_abbr").last().select(
        "home_abbr", pl.col("venue_name").alias("_arena"), pl.col("venue_location").alias("_city"))
    home = df.select("game_id").join(data.games().select("game_id", "home_abbr"), on="game_id", how="left").join(
        latest_home, on="home_abbr", how="left").drop("home_abbr")
    return played.join(home, on="game_id", how="left").with_columns(
        pl.coalesce("venue_name", "_arena").alias("venue_name"), pl.coalesce("venue_location", "_city").alias("venue_location"),
    ).drop("_arena", "_city")


def _teams(data: SiteData, games: pl.DataFrame, day: date) -> dict[str, dict]:
    """Team names, records and form for every team playing on ``day``."""
    todays = games.filter(pl.col("game_date") == day)
    if todays.is_empty():
        return {}
    abbrs = sorted(set(todays["home_abbr"].to_list()) | set(todays["away_abbr"].to_list()))
    return team_context(games, abbrs, day, int(todays["season"][0]))


def _unpriced(data: SiteData, games: pl.DataFrame, day: date, priced: list[int]) -> pl.DataFrame:
    """Catalog games on ``day`` with no pregame price (e.g. the morning run hasn't happened yet),
    with the start time in UTC and the venue."""
    rows = games.filter((pl.col("game_date") == day) & ~pl.col("game_id").is_in(priced)).select(
        "game_id", "game_date", "start_time_et",
        pl.col("start_time_et").str.to_datetime().dt.replace_time_zone("America/New_York")
        .dt.convert_time_zone("UTC").alias("start_time"),
        "home_abbr", "away_abbr", "is_final", "home_score", "away_score", "last_period",
    ).sort("start_time_et", "game_id")
    return with_venue(rows, data)


def _day_edges(data: SiteData, day: date) -> pl.DataFrame | None:
    """Best book per side for each game: closing prices once it has started, else the latest."""
    edges = data.card_edges(day)
    return with_selection(edges).sort("game_id", "edge", descending=[False, True]) if edges is not None and edges.height else None


def _day_bets(data: SiteData, day: date, games: pl.DataFrame) -> pl.DataFrame | None:
    """Ledger bets on ``day``'s games, labelled."""
    ledger = data.ledger()
    if ledger is None:
        return None
    bets = ledger.filter(pl.col("game_date") == day)
    if bets.is_empty():
        return None
    return with_selection(bets.join(games.select("game_id", "home_abbr", "away_abbr"), on="game_id", how="left"))


def _market_lines(data: SiteData, games: pl.DataFrame, day: date, unpriced: pl.DataFrame) -> pl.DataFrame | None:
    """Best market prices for games not started and not priced by the model yet, labelled."""
    todo = unpriced.filter(~pl.col("is_final"))
    if todo.is_empty():
        return None
    season = int(games.filter(pl.col("game_id") == todo["game_id"][0])["season"][0])
    lines = data.market_lines(day, todo["game_id"].to_list(), season)
    if lines is None:
        return None
    lines = lines.join(todo.select("game_id", "home_abbr", "away_abbr"), on="game_id")
    return with_selection(lines).sort("game_id", "market", "side")


def _three_way(data: SiteData, games: pl.DataFrame, day: date) -> list[dict]:
    """Regulation three-way per game on ``day``: the model (from its last pregame run) against the
    market's consensus and best prices before puck drop (see :func:`nhl.api.markets.three_way_card`)."""
    todays = games.filter(pl.col("game_date") == day)
    if todays.is_empty():
        return []
    try:
        odds = data.live_odds(int(todays["season"][0]))
        odds = odds.filter(pl.col("market").is_in(["moneyline_3way", "moneyline"])) if odds.height else odds
    except Exception:  # a bad odds file must not take the page down
        logger.exception("three-way odds failed for %s", day)
        odds = pl.DataFrame()
    prices = data.day_prices(day)
    matrices = {} if prices is None else dict(zip(prices["game_id"].to_list(), prices["score_matrix"].to_list()))
    blend_model, segment = data.blend_model(), blend.segment_of(day)
    out = []
    for g in todays.iter_rows(named=True):
        now = mk.current(mk.replay(mk.game_quotes(odds, g["game_id"], mk.start_utc(g)))[0])
        m = matrices.get(g["game_id"])
        card = mk.three_way_card(None if m is None else np.asarray(m), now.get("moneyline_3way"), now.get("moneyline"),
                                 blend_model, segment)
        if card is not None:
            out.append({"game_id": g["game_id"], **card})
    return out


@router.get("/slate")
def get_slate(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """Each game's final pregame view for ``date``: prices, starters, flags, edges, bets, freshness."""
    if (raw := data.view(views.slate_key(day))) is not None:
        return json_view(raw)
    return build_slate(data, day)


def build_slate(data: SiteData, day: date) -> dict:
    """The ``/api/slate`` payload for ``day`` (also prebuilt by :mod:`nhl.site.publish`)."""
    games = data.games()
    slate = data.day_slate(day)
    priced = [] if slate is None else slate["game_id"].to_list()
    if slate is not None:
        slate = with_venue(_with_scores(slate, games), data).sort("start_time", "game_id")
    unpriced = _unpriced(data, games, day, priced)
    return {
        "date": day.isoformat(),
        "prev_date": (day - timedelta(days=1)).isoformat(),
        "next_date": (day + timedelta(days=1)).isoformat(),
        "stamp": data.latest_stamp(day),
        "games": rows(slate),
        "unpriced": rows(unpriced),
        "freshness": rows(data.freshness(day), drop=("stamp",)),
        "teams": _teams(data, games, day),
        "edges": rows(_day_edges(data, day)),
        "lines": rows(_market_lines(data, games, day, unpriced)),
        "bets": rows(_day_bets(data, day, games)),
        "three_way": _three_way(data, games, day),
    }
