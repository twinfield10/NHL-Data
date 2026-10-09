"""``/api/edges`` and ``/api/bets``: live edges and the paper/real ledger."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
from fastapi import APIRouter, Depends, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import rows, starts_utc, with_selection
from nhl.betting import info
from nhl.props import ledger as props_ledger

router = APIRouter(prefix="/api", tags=["betting"])

#: A game-market bet: one per (game, market, side, line).
BET_KEY = ["game_id", "market", "side", "line"]


def _with_bets(edges: pl.DataFrame, data: SiteData) -> pl.DataFrame:
    """Add the paper bet already recorded for each row (``bet_price``, ``bet_book``,
    ``bet_stake``, ``placed_at``) and its CLV against this row's consensus (``bet_clv``)."""
    ledger = data.ledger()
    cols = {"bet_price": pl.Float64, "bet_book": pl.String, "bet_stake": pl.Float64, "placed_at": pl.Datetime("us", "UTC")}
    if ledger is None or ledger.is_empty():
        return edges.with_columns(*[pl.lit(None, dtype=t).alias(c) for c, t in cols.items()],
                                  pl.lit(None, dtype=pl.Float64).alias("bet_clv"))
    bets = ledger.filter(pl.col("kind") == "paper").select(
        *BET_KEY, pl.col("price").alias("bet_price"), pl.col("book").alias("bet_book"),
        pl.col("stake_units").alias("bet_stake"), "placed_at").unique(BET_KEY, keep="first")
    bets = bets.cast({c: edges.schema[c] for c in BET_KEY})
    dec = pl.when(pl.col("bet_price") < 0).then(1 + 100 / -pl.col("bet_price")).otherwise(1 + pl.col("bet_price") / 100)
    return edges.join(bets, on=BET_KEY, how="left", nulls_equal=True).with_columns(
        (pl.col("p_market_side") * dec - 1).alias("bet_clv"))


@router.get("/edges")
def get_edges(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """The latest edges snapshot for ``date``: best book per side, flagged bets first, each with
    the paper bet already placed on it (if any)."""
    edges = data.edges(day)
    if edges is not None and edges.height:
        edges = with_selection(_with_bets(edges, data)).sort(["flagged", "edge"], descending=[True, True])
    return {
        "date": day.isoformat(),
        "stamp": None if edges is None or edges.is_empty() else edges["stamp"][0],
        "edges": rows(edges),
    }


def _totals(graded: pl.DataFrame) -> dict:
    """Headline numbers for a set of graded bets."""
    if graded.is_empty():
        return {"bets": 0, "staked": 0.0, "pnl": 0.0, "roi": None, "mean_clv": None, "beat_close": None}
    staked = graded["stake_units"].sum()
    return {
        "bets": graded.height,
        "staked": staked,
        "pnl": graded["pnl_units"].sum(),
        "roi": graded["pnl_units"].sum() / staked if staked else None,
        "mean_clv": graded["clv"].mean(),
        "beat_close": (graded["clv"] > 0).mean(),
    }


def _open_totals(bets: pl.DataFrame) -> dict:
    """Ungraded bets judged against the market now (see :func:`nhl.props.ledger.live_view`)."""
    open_ = bets.filter(pl.col("status") != "graded") if bets.height else bets
    counts = dict(open_.group_by("status").len().iter_rows()) if open_.height else {}
    clv = open_["clv_now"].drop_nulls() if open_.height else pl.Series([], dtype=pl.Float64)
    return {"bets": open_.height, "staked": float(open_["stake_units"].sum()) if open_.height else 0.0,
            "mean_clv": clv.mean() if clv.len() else None, "beating": (clv > 0).mean() if clv.len() else None,
            **{s: counts.get(s, 0) for s in ("value", "faded", "gone", "closed")}}


@router.get("/bets")
def get_bets(
    kind: str | None = Query(None, pattern="^(paper|real)$", description="paper or real; default both"),
    data: SiteData = Depends(get_data),
) -> dict:
    """Every ledger bet (newest first) beside the market now (best price, live CLV, still a play?),
    with totals, a market breakdown and a breakdown by placement window and information grade
    (graded bets)."""
    ledger = data.ledger()
    if ledger is None or ledger.is_empty():
        return {"bets": [], "totals": _totals(pl.DataFrame()), "open": _open_totals(pl.DataFrame()), "breakdown": [],
                "info": []}
    ledger = ledger.cast({"side": pl.Int64})
    if kind:
        ledger = ledger.filter(pl.col("kind") == kind)
    games = data.games()
    open_days = ledger.filter(pl.col("graded_at").is_null())["game_date"].unique().to_list()
    frames = [e for d in open_days if (e := data.edges(d)) is not None and e.height]
    edges = pl.concat(frames, how="diagonal_relaxed") if frames else None
    ledger = props_ledger.live_view(ledger, edges, starts_utc(games), datetime.now(timezone.utc), key=BET_KEY)
    teams = games.select("game_id", "home_abbr", "away_abbr")
    ledger = with_selection(ledger.join(teams, on="game_id", how="left")).sort("placed_at", descending=True)

    graded = ledger.filter(pl.col("graded_at").is_not_null())
    breakdown = graded.group_by("kind", "market").agg(
        pl.len().alias("bets"), pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
    ).with_columns((pl.col("pnl") / pl.col("staked")).alias("roi")).sort("kind", "market")
    return {"bets": rows(ledger), "totals": _totals(graded), "open": _open_totals(ledger), "breakdown": rows(breakdown),
            "info": rows(info.breakdown(ledger))}
