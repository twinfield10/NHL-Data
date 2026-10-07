"""``/api/edges`` and ``/api/bets``: live edges and the paper/real ledger."""

from __future__ import annotations

from datetime import date

import polars as pl
from fastapi import APIRouter, Depends, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import rows, with_selection

router = APIRouter(prefix="/api", tags=["betting"])


@router.get("/edges")
def get_edges(day: date = Depends(game_day), data: SiteData = Depends(get_data)) -> dict:
    """The latest edges snapshot for ``date``: best book per side, flagged bets first."""
    edges = data.edges(day)
    if edges is not None and edges.height:
        edges = with_selection(edges).sort(["flagged", "edge"], descending=[True, True])
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


@router.get("/bets")
def get_bets(
    kind: str | None = Query(None, pattern="^(paper|real)$", description="paper or real; default both"),
    data: SiteData = Depends(get_data),
) -> dict:
    """Every ledger bet (newest first), with totals and a market breakdown of graded bets."""
    ledger = data.ledger()
    if ledger is None or ledger.is_empty():
        return {"bets": [], "totals": _totals(pl.DataFrame()), "breakdown": []}
    if kind:
        ledger = ledger.filter(pl.col("kind") == kind)
    teams = data.games().select("game_id", "home_abbr", "away_abbr")
    ledger = with_selection(ledger.join(teams, on="game_id", how="left")).sort("placed_at", descending=True)

    graded = ledger.filter(pl.col("graded_at").is_not_null())
    breakdown = graded.group_by("kind", "market").agg(
        pl.len().alias("bets"), pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
    ).with_columns((pl.col("pnl") / pl.col("staked")).alias("roi")).sort("kind", "market")
    return {"bets": rows(ledger), "totals": _totals(graded), "breakdown": rows(breakdown)}
