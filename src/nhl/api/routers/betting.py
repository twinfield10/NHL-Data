"""``/api/edges`` and ``/api/bets``: live edges and the paper/real ledger."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
from fastapi import APIRouter, Depends, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import rows, starts_utc, with_selection
from nhl.betting import info
from nhl.betting.ledger import price_clv
from nhl.props import ledger as props_ledger

router = APIRouter(prefix="/api", tags=["betting"])

#: A game-market bet: one per (game, market, side, line).
BET_KEY = ["game_id", "market", "side", "line"]


def _season(game_id: int) -> int:
    year = int(str(game_id)[:4])
    return year * 10000 + year + 1


def _same_book(rows: pl.DataFrame, data: SiteData, book: str, out: str) -> pl.DataFrame:
    """Add ``out``: the price at ``book`` (a column) on each row's side and line, now for games
    not started and at the close for started ones."""
    empty = rows.with_columns(pl.lit(None, dtype=pl.Float64).alias(out))
    if rows.is_empty():
        return empty
    starts = starts_utc(data.games().filter(pl.col("game_id").is_in(rows["game_id"].unique().implode())))
    started = set(starts.filter(pl.col("start_utc") <= datetime.now(timezone.utc))["game_id"].to_list())
    ids = rows["game_id"].unique().to_list()
    frames = []
    for point, games in (("close", [g for g in ids if g in started]), ("last", [g for g in ids if g not in started])):
        if games and (p := data.book_prices(_season(games[0]), games, point)) is not None:
            frames.append(p.filter(pl.col("game_id").is_in(games)))
    if not frames:
        return empty
    prices = pl.concat(frames).rename({"book": book}).unique(["game_id", "market", book, "line"], keep="last")
    j = rows.with_row_index("_i").join(prices.cast({"line": rows.schema["line"]}), on=["game_id", "market", book, "line"],
                                       how="left", nulls_equal=True)
    return j.with_columns(pl.when(pl.col("side") == 1).then("price_1").otherwise("price_2").alias(out)).drop(
        "price_1", "price_2").sort("_i").drop("_i")


def _with_bets(edges: pl.DataFrame, data: SiteData) -> pl.DataFrame:
    """Add the paper bet already recorded for each row (``bet_price``, ``bet_book``,
    ``bet_stake``, ``placed_at``) and its CLV against this row's consensus (``bet_clv``)."""
    ledger = data.ledger()
    cols = {"bet_price": pl.Float64, "bet_book": pl.String, "bet_stake": pl.Float64, "placed_at": pl.Datetime("us", "UTC")}
    if ledger is None or ledger.is_empty():
        return edges.with_columns(*[pl.lit(None, dtype=t).alias(c) for c, t in cols.items()],
                                  *[pl.lit(None, dtype=pl.Float64).alias(c) for c in ("bet_clv", "bet_book_now", "bet_price_clv")])
    bets = ledger.filter(pl.col("kind") == "paper").select(
        *BET_KEY, pl.col("price").alias("bet_price"), pl.col("book").alias("bet_book"),
        pl.col("stake_units").alias("bet_stake"), "placed_at").unique(BET_KEY, keep="first")
    bets = bets.cast({c: edges.schema[c] for c in BET_KEY})
    dec = pl.when(pl.col("bet_price") < 0).then(1 + 100 / -pl.col("bet_price")).otherwise(1 + pl.col("bet_price") / 100)
    out = edges.join(bets, on=BET_KEY, how="left", nulls_equal=True).with_columns(
        (pl.col("p_market_side") * dec - 1).alias("bet_clv"))
    out = _same_book(out, data, "bet_book", "bet_book_now")
    return out.with_columns(price_clv(pl.col("bet_price"), pl.col("bet_book_now")).alias("bet_price_clv"))


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
        return {"bets": 0, "staked": 0.0, "pnl": 0.0, "roi": None, "mean_clv": None, "beat_close": None,
                "mean_price_clv": None}
    staked = graded["stake_units"].sum()
    return {
        "bets": graded.height,
        "staked": staked,
        "pnl": graded["pnl_units"].sum(),
        "roi": graded["pnl_units"].sum() / staked if staked else None,
        "mean_clv": graded["clv"].mean(), "mean_price_clv": graded["price_clv"].mean(),
        "beat_close": (graded["clv"] > 0).mean(),
    }


def _open_totals(bets: pl.DataFrame) -> dict:
    """Ungraded bets judged against the market now (see :func:`nhl.props.ledger.live_view`)."""
    open_ = bets.filter(pl.col("status") != "graded") if bets.height else bets
    counts = dict(open_.group_by("status").len().iter_rows()) if open_.height else {}
    clv = open_["clv_now"].drop_nulls() if open_.height else pl.Series([], dtype=pl.Float64)
    return {"bets": open_.height, "staked": float(open_["stake_units"].sum()) if open_.height else 0.0,
            "mean_clv": clv.mean() if clv.len() else None, "beating": (clv > 0).mean() if clv.len() else None,
            "mean_price_clv": open_["price_clv"].mean() if open_.height and "price_clv" in open_.columns else None,
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
    # Price CLV: the same book's price later, the close once graded, else now.
    open_ = _same_book(ledger.filter(pl.col("graded_at").is_null()), data, "book", "book_now")
    ledger = pl.concat([ledger.filter(pl.col("graded_at").is_not_null()).with_columns(pl.lit(None, dtype=pl.Float64).alias("book_now")),
                        open_], how="diagonal_relaxed")
    ledger = ledger.with_columns(price_clv(pl.col("price"), pl.coalesce("close_price", "book_now")).alias("price_clv"))
    teams = games.select("game_id", "home_abbr", "away_abbr")
    ledger = with_selection(ledger.join(teams, on="game_id", how="left")).sort("placed_at", descending=True)

    graded = ledger.filter(pl.col("graded_at").is_not_null())
    breakdown = graded.group_by("kind", "market").agg(
        pl.len().alias("bets"), pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
        pl.col("price_clv").mean().alias("mean_price_clv"),
    ).with_columns((pl.col("pnl") / pl.col("staked")).alias("roi")).sort("kind", "market")
    return {"bets": rows(ledger), "totals": _totals(graded), "open": _open_totals(ledger), "breakdown": rows(breakdown),
            "info": rows(info.breakdown(ledger))}
