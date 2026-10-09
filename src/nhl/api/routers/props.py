"""``/api/props``: player-prop edges, one game's prop board, and the props ledger (M9 phase E)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import EASTERN, rows
from nhl.props import live

router = APIRouter(prefix="/api", tags=["props"])

#: Edge columns the site uses (the snapshot carries more).
EDGE_COLS = ["game_id", "start_utc", "away_abbr", "home_abbr", "player_id", "player_name", "team", "position", "slot",
             "pp_unit", "prop_type", "line", "side", "book", "price", "books", "p_model_side", "p_market_side", "p",
             "edge", "flagged", "stake_units", "stamp"]
SERIES = ["book", "game_id", "player_id", "prop_type", "line", "side"]


BET_KEY = ["game_id", "player_id", "prop_type", "line", "side"]


def _with_bets(edges: pl.DataFrame, data: SiteData) -> pl.DataFrame:
    """Add the paper bet already recorded for each row (``bet_price``, ``bet_book``,
    ``bet_stake``, ``placed_at``): later runs resize stakes as the day's cap fills, but the
    ledger keeps the bet as first placed."""
    ledger = data.props_ledger()
    if ledger is None or ledger.is_empty():
        return edges.with_columns(pl.lit(None, dtype=pl.Float64).alias("bet_price"), pl.lit(None, dtype=pl.String).alias("bet_book"),
                                  pl.lit(None, dtype=pl.Float64).alias("bet_stake"),
                                  pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("placed_at"))
    bets = ledger.filter(pl.col("kind") == "paper").select(
        *BET_KEY, pl.col("price").alias("bet_price"), pl.col("book").alias("bet_book"),
        pl.col("stake_units").alias("bet_stake"), "placed_at")
    return edges.join(bets, on=BET_KEY, how="left")


def _with_movement(edges: pl.DataFrame, data: SiteData) -> pl.DataFrame:
    """Add each quote's opening price at the same book (see :func:`_movement`)."""
    if edges.is_empty():
        return edges
    quotes = data.props_quotes(_season(int(edges["game_id"][0])))
    if quotes.is_empty():
        return edges
    return edges.join(_movement(quotes.filter(pl.col("game_id").is_in(edges["game_id"].unique().implode()))),
                      on=SERIES, how="left")


def _season(game_id: int) -> int:
    year = int(str(game_id)[:4])
    return year * 10000 + year + 1


def _movement(quotes: pl.DataFrame) -> pl.DataFrame:
    """Per price series: the opening price and when it was first seen, and how often it moved."""
    return quotes.sort("captured_at").group_by(SERIES).agg(
        pl.col("price").first().alias("open_price"), pl.col("captured_at").first().alias("opened_at"),
        (pl.len() - 1).alias("moves"))


@router.get("/props")
def get_props(day: date = Depends(game_day), min_edge: float = Query(-0.02, description="lowest edge returned"),
              data: SiteData = Depends(get_data)) -> dict:
    """The day's best prop quote per (game, player, stat, line, side), flagged first, with each
    quote's opening price at the same book. Started games keep their last pregame view."""
    edges = data.props_edges(day)
    if edges is None or edges.is_empty():
        return {"date": day.isoformat(), "stamp": None, "props": []}
    # Every bet already placed stays listed even if its edge has since faded.
    edges = _with_bets(edges.select([c for c in EDGE_COLS if c in edges.columns]), data)
    edges = edges.filter((pl.col("edge") >= min_edge) | pl.col("flagged") | pl.col("bet_stake").is_not_null())
    edges = _with_movement(edges, data).sort(["flagged", "edge"], descending=[True, True])
    return {"date": day.isoformat(), "stamp": edges["stamp"].max(), "max_price": live.MAX_PRICE, "props": rows(edges)}


@router.get("/games/{game_id}/props")
def get_game_props(game_id: int, data: SiteData = Depends(get_data)) -> dict:
    """One game's prop board: every projected player's probabilities, every book's latest
    quote (devigged, with the consensus), and the game's best edges."""
    game = data.game(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail=f"unknown game {game_id}")
    day = game["game_date"]
    proj = data.props_projections(day)
    proj = proj.filter(pl.col("game_id") == game_id) if proj is not None else None
    if proj is None or proj.is_empty():
        return {"game_id": game_id, "stamp": None, "players": [], "quotes": [], "edges": []}

    quotes = data.props_quotes(_season(game_id)).filter(
        (pl.col("game_id") == game_id) & pl.col("prop_type").is_in(list(live.STATS)) & pl.col("side").is_in(["over", "under"])
        & pl.col("player_id").is_not_null() & pl.col("line").is_not_null())
    # A player new to the catalog (a call-up) still has the name the books use.
    names = {**dict(quotes.select("player_id", "player_name").unique("player_id").iter_rows()), **data.player_names()}
    abbr = {game["home_team_id"]: game["home_abbr"], game["away_team_id"]: game["away_abbr"]}
    players = proj.with_columns(
        pl.Series("player_name", [names.get(p, "Unknown") for p in proj["player_id"]], dtype=pl.String),
        pl.col("team_id").replace_strict(abbr, default=None, return_dtype=pl.String).alias("team"),
    ).sort("team", -pl.col("exp_points").fill_null(-1.0))

    cutoff = datetime.now(timezone.utc)
    if game.get("start_time_et"):
        start = datetime.fromisoformat(game["start_time_et"]).replace(tzinfo=EASTERN).astimezone(timezone.utc)
        cutoff = min(cutoff, start)  # a started game shows its closing quotes
    latest = quotes.filter(pl.col("captured_at") <= cutoff).sort("captured_at").group_by(SERIES).last()
    probs = live.market_probs(latest) if latest.height else latest

    edges = data.props_edges(day)
    if edges is not None:
        edges = edges.filter(pl.col("game_id") == game_id).select([c for c in EDGE_COLS if c in edges.columns])
        edges = _with_bets(_with_movement(edges, data), data)
    keep = ["player_id", "player_name", "team", "position", "slot", "pp_unit", "p_dressed", "confidence", "source",
            *[c for c in players.columns if c.startswith(("p_goals_", "p_ast_", "p_points_", "p_shots_", "p_blocks_", "p_saves_",
                                                         "exp_"))]]
    return {
        "game_id": game_id,
        "stamp": proj["stamp"][0],
        "players": rows(players.select([c for c in keep if c in players.columns])),
        "quotes": rows(probs.select("player_id", "prop_type", "line", "book", "price_over", "price_under", "p_book",
                                    "p_market", "books") if probs.height else None),
        "edges": rows(edges.sort("edge", descending=True) if edges is not None else None),
    }


def _totals(graded: pl.DataFrame) -> dict:
    if graded.is_empty():
        return {"bets": 0, "staked": 0.0, "pnl": 0.0, "roi": None, "mean_clv": None, "beat_close": None}
    staked = graded["stake_units"].sum()
    return {"bets": graded.height, "staked": staked, "pnl": graded["pnl_units"].sum(),
            "roi": graded["pnl_units"].sum() / staked if staked else None, "mean_clv": graded["clv"].mean(),
            "beat_close": (graded["clv"] > 0).mean()}


@router.get("/props/bets")
def get_props_bets(data: SiteData = Depends(get_data)) -> dict:
    """Every props ledger bet (newest first), with totals and a breakdown by stat and side of graded,
    non-void bets."""
    ledger = data.props_ledger()
    if ledger is None or ledger.is_empty():
        return {"bets": [], "totals": _totals(pl.DataFrame()), "breakdown": []}
    teams = data.games().select("game_id", "home_abbr", "away_abbr")
    ledger = ledger.join(teams, on="game_id", how="left").sort("placed_at", descending=True)
    graded = ledger.filter(pl.col("graded_at").is_not_null() & (pl.col("result") != "void"))
    breakdown = graded.group_by("prop_type", "side").agg(
        pl.len().alias("bets"), pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
    ).with_columns((pl.col("pnl") / pl.col("staked")).alias("roi")).sort("prop_type", "side")
    return {"bets": rows(ledger), "totals": _totals(graded), "breakdown": rows(breakdown)}
