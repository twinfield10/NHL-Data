"""``/api/props``: player-prop edges, one game's prop board, and the props ledger (M9 phase E)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query

from nhl.api.data import SiteData
from nhl.api.deps import game_day, get_data
from nhl.api.serialize import EASTERN, json_view, rows, starts_utc
from nhl.betting import info
from nhl.betting.ledger import price_clv
from nhl.props import ledger as props_ledger
from nhl.props import live
from nhl.site import views

router = APIRouter(prefix="/api", tags=["props"])

#: Edge columns the site uses (the snapshot carries more).
EDGE_COLS = ["game_id", "start_utc", "away_abbr", "home_abbr", "player_id", "player_name", "team", "position", "slot",
             "pp_unit", "prop_type", "line", "side", "book", "price", "books", "two_way_books", "p_model_side", "p_market_side", "p",
             "edge", "flagged", "stake_units", "stamp"]
SERIES = ["book", "game_id", "player_id", "prop_type", "line", "side"]


BET_KEY = ["game_id", "player_id", "prop_type", "line", "side"]
#: ``/api/props`` default floor on edge (the prebuilt view uses it).
DEFAULT_MIN_EDGE = -0.02


def _same_book(rows: pl.DataFrame, data: SiteData, book: str, out: str) -> pl.DataFrame:
    """Add ``out``: the latest price at ``book`` (a column) for each row's player, stat, line and
    side, as of now or puck drop, whichever is first."""
    if rows.is_empty() or rows[book].null_count() == rows.height:
        return rows.with_columns(pl.lit(None, dtype=pl.Float64).alias(out))
    ids = rows["game_id"].unique().to_list()
    quotes = data.props_quotes(_season(int(ids[0]))).filter(pl.col("game_id").is_in(ids))
    now = datetime.now(timezone.utc)
    cut = starts_utc(data.games().filter(pl.col("game_id").is_in(ids))).with_columns(
        pl.min_horizontal(pl.col("start_utc"), pl.lit(now)).alias("_cut")).select("game_id", "_cut")
    latest = live.drop_pulled(quotes.join(cut, on="game_id").filter(pl.col("captured_at") <= pl.col("_cut")).sort("captured_at")
                              .group_by(SERIES).last(), data.props_seen(_season(int(ids[0]))))
    latest = latest.select(*SERIES, pl.col("price").cast(pl.Float64).alias(out)).rename({"book": book})
    keys_ = [book, *[c for c in SERIES if c != "book"]]
    return rows.join(latest.cast({c: rows.schema[c] for c in keys_ if c in rows.schema}), on=keys_, how="left")


def _with_bets(edges: pl.DataFrame, data: SiteData) -> pl.DataFrame:
    """Add the paper bet already recorded for each row (``bet_price``, ``bet_book``,
    ``bet_stake``, ``placed_at``) and its CLV against this row's consensus (``bet_clv``): later
    runs resize stakes as the day's cap fills, but the ledger keeps the bet as first placed."""
    ledger = data.props_ledger()
    if ledger is None or ledger.is_empty():
        return edges.with_columns(pl.lit(None, dtype=pl.Float64).alias("bet_price"), pl.lit(None, dtype=pl.String).alias("bet_book"),
                                  pl.lit(None, dtype=pl.Float64).alias("bet_stake"),
                                  pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("placed_at"),
                                  *[pl.lit(None, dtype=pl.Float64).alias(c) for c in ("bet_clv", "bet_book_now", "bet_price_clv")])
    bets = ledger.filter(pl.col("kind") == "paper").select(
        *BET_KEY, pl.col("price").alias("bet_price"), pl.col("book").alias("bet_book"),
        pl.col("stake_units").alias("bet_stake"), "placed_at")
    dec = pl.when(pl.col("bet_price") < 0).then(1 + 100 / -pl.col("bet_price")).otherwise(1 + pl.col("bet_price") / 100)
    out = edges.join(bets, on=BET_KEY, how="left").with_columns((pl.col("p_market_side") * dec - 1).alias("bet_clv"))
    out = _same_book(out, data, "bet_book", "bet_book_now")
    return out.with_columns(price_clv(pl.col("bet_price"), pl.col("bet_book_now")).alias("bet_price_clv"))


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
def get_props(day: date = Depends(game_day), min_edge: float = Query(DEFAULT_MIN_EDGE, description="lowest edge returned"),
              data: SiteData = Depends(get_data)) -> dict:
    """The day's best prop quote per (game, player, stat, line, side), flagged first, with each
    quote's opening price at the same book. Started games keep their last pregame view."""
    if min_edge == DEFAULT_MIN_EDGE and (raw := data.view(views.props_key(day))) is not None:
        return json_view(raw)
    return build_props(data, day, min_edge)


def build_props(data: SiteData, day: date, min_edge: float = DEFAULT_MIN_EDGE) -> dict:
    """The ``/api/props`` payload for ``day`` (also prebuilt by :mod:`nhl.site.publish`)."""
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
    if (raw := data.view(views.game_key(game_id, "props"))) is not None:
        return json_view(raw)
    game = data.game(game_id)
    if game is None:
        raise HTTPException(status_code=404, detail=f"unknown game {game_id}")
    return build_game_props(data, game)


def build_game_props(data: SiteData, game: dict) -> dict:
    """The game page's props tab (also prebuilt by :mod:`nhl.site.publish`)."""
    game_id, day = game["game_id"], game["game_date"]
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
    latest = live.drop_pulled(quotes.filter(pl.col("captured_at") <= cutoff).sort("captured_at").group_by(SERIES).last(),
                              data.props_seen(_season(game_id)))
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
                                    "p_market", "books").sort("player_id", "prop_type", "line", "book") if probs.height else None),
        "edges": rows(edges.sort("edge", descending=True) if edges is not None else None),
    }


def _totals(graded: pl.DataFrame) -> dict:
    if graded.is_empty():
        return {"bets": 0, "staked": 0.0, "pnl": 0.0, "roi": None, "mean_clv": None, "beat_close": None,
                "mean_price_clv": None}
    staked = graded["stake_units"].sum()
    return {"bets": graded.height, "staked": staked, "pnl": graded["pnl_units"].sum(),
            "roi": graded["pnl_units"].sum() / staked if staked else None, "mean_clv": graded["clv"].mean(), "mean_price_clv": graded["price_clv"].mean(),
            "beat_close": (graded["clv"] > 0).mean()}


def _open_totals(bets: pl.DataFrame) -> dict:
    """Ungraded bets judged against the market now (see :func:`nhl.props.ledger.live_view`)."""
    open_ = bets.filter(pl.col("status") != "graded")
    counts = dict(open_.group_by("status").len().iter_rows()) if open_.height else {}
    clv = open_["clv_now"].drop_nulls() if open_.height else pl.Series([], dtype=pl.Float64)
    return {"bets": open_.height, "staked": float(open_["stake_units"].sum()) if open_.height else 0.0,
            "mean_clv": clv.mean() if clv.len() else None, "beating": (clv > 0).mean() if clv.len() else None,
            "mean_price_clv": open_["price_clv"].mean() if open_.height and "price_clv" in open_.columns else None,
            **{s: counts.get(s, 0) for s in ("value", "faded", "gone", "closed")}}


@router.get("/props/bets")
def get_props_bets(data: SiteData = Depends(get_data)) -> dict:
    """Every props ledger bet (newest first) beside the market now (best price, live CLV, still
    available with value?), with totals and breakdowns by stat and side and by how long before puck
    drop the bet was placed (graded, non-void bets)."""
    ledger = data.props_ledger()
    if ledger is None or ledger.is_empty():
        return {"bets": [], "totals": _totals(pl.DataFrame()), "open": _open_totals(pl.DataFrame({"status": []})),
                "breakdown": [], "timing": [], "info": []}
    games = data.games()
    open_days = ledger.filter(pl.col("graded_at").is_null())["game_date"].unique().to_list()
    frames = [e for d in open_days if (e := data.props_edges(d)) is not None and e.height]
    edges = pl.concat(frames, how="diagonal_relaxed") if frames else None
    ledger = props_ledger.live_view(ledger, edges, starts_utc(games), datetime.now(timezone.utc))
    # Price CLV: the same book's price later, the close once graded, else the latest quote.
    graded_ = pl.col("graded_at").is_not_null()
    ledger = pl.concat([ledger.filter(graded_).with_columns(pl.lit(None, dtype=pl.Float64).alias("book_now")),
                        _same_book(ledger.filter(~graded_), data, "book", "book_now")], how="diagonal_relaxed")
    ledger = ledger.with_columns(price_clv(pl.col("price"), pl.coalesce("close_price", "book_now")).alias("price_clv"))
    teams = games.select("game_id", "home_abbr", "away_abbr")
    ledger = ledger.join(teams, on="game_id", how="left").sort("placed_at", descending=True)
    graded = ledger.filter(pl.col("graded_at").is_not_null() & (pl.col("result") != "void"))
    breakdown = graded.group_by("prop_type", "side").agg(
        pl.len().alias("bets"), pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
        pl.col("price_clv").mean().alias("mean_price_clv"),
    ).with_columns((pl.col("pnl") / pl.col("staked")).alias("roi")).sort("prop_type", "side")
    return {"bets": rows(ledger), "totals": _totals(graded), "open": _open_totals(ledger), "breakdown": rows(breakdown),
            "timing": rows(props_ledger.timing(ledger)), "info": rows(info.breakdown(ledger))}
