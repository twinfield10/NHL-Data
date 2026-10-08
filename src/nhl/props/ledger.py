"""The player-prop paper ledger (M9 phase D), graded on closing-line value and result.

``bets/props_ledger.parquet``, one row per bet:

* identity: ``bet_id``, ``kind`` (``paper``), ``placed_at``, ``game_id``, ``game_date``,
  ``player_id``, ``player_name``, ``team``, ``prop_type`` (goals | assists | points), ``line``,
  ``side`` (over | under), ``book``, ``price`` (American), ``stake_units``;
* the view when placed: ``p_model``, ``p_market``, ``p_blend``, ``edge``, ``books``,
  ``pregame_stamp``;
* grading (:func:`grade`, once the game is final and its game logs are in):
  - ``close_price``: the same book's last price before puck drop;
  - ``p_close``: the devigged consensus for the side at the bet's line, at the close;
  - ``clv`` = p_close × decimal(price) − 1;
  - ``stat``: the player's goals / assists / points;
  - ``result``: win | loss | void, where void means he didn't play;
  - ``pnl_units``, ``graded_at``.

A paper bet is recorded the first time a (game, player, stat, line, side) is flagged and never
re-added, so the ledger keeps the price available when the model first liked it.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timezone

import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

SCHEMA = {
    "bet_id": pl.String, "kind": pl.String, "placed_at": pl.Datetime("us", "UTC"), "game_id": pl.Int64,
    "game_date": pl.Date, "player_id": pl.Int64, "player_name": pl.String, "team": pl.String, "prop_type": pl.String,
    "line": pl.Float64, "side": pl.String, "book": pl.String, "price": pl.Float64, "stake_units": pl.Float64,
    "p_model": pl.Float64, "p_market": pl.Float64, "p_blend": pl.Float64, "edge": pl.Float64, "books": pl.Int64,
    "pregame_stamp": pl.String,
    "close_price": pl.Float64, "p_close": pl.Float64, "clv": pl.Float64, "stat": pl.Float64, "result": pl.String,
    "pnl_units": pl.Float64, "graded_at": pl.Datetime("us", "UTC"),
}
BET_KEY = ["game_id", "player_id", "prop_type", "line", "side"]


def load(store: Store) -> pl.DataFrame:
    led = store.get_parquet(keys.PROPS_LEDGER)
    return pl.DataFrame(schema=SCHEMA) if led is None else led.cast(SCHEMA)


def day_stakes(store: Store, day: date, exclude: pl.DataFrame | None = None) -> float:
    """Units of paper prop bets already recorded for ``day`` (not counting ``exclude``'s bets)."""
    led = load(store).filter((pl.col("kind") == "paper") & (pl.col("game_date") == day))
    if exclude is not None and exclude.height:
        led = led.join(exclude.select(BET_KEY).cast({"player_id": pl.Int64}), on=BET_KEY, how="anti")
    return float(led["stake_units"].sum())


def add_paper(store: Store, edges: pl.DataFrame) -> int:
    """Append paper bets for flagged edges not in the ledger yet. Returns bets added."""
    if edges.is_empty():
        return 0
    led = load(store)
    new = edges.select(
        "game_id", "game_date", "player_id", "player_name", "team", "prop_type", "line", "side", "book", "price",
        "stake_units", pl.col("p_model_side").alias("p_model"), pl.col("p_market_side").alias("p_market"),
        pl.col("p").alias("p_blend"), "edge", pl.col("books").cast(pl.Int64), "pregame_stamp",
        pl.col("as_of").alias("placed_at"),
    ).join(led.filter(pl.col("kind") == "paper").select(BET_KEY), on=BET_KEY, how="anti")
    if new.is_empty():
        return 0
    new = new.with_columns(
        pl.lit("paper").alias("kind"),
        pl.concat_str([pl.lit("paper"), *[pl.col(c).cast(pl.String) for c in BET_KEY]], separator="|").map_elements(
            lambda s: hashlib.sha1(s.encode()).hexdigest()[:16], return_dtype=pl.String).alias("bet_id"))
    store.put_parquet(keys.PROPS_LEDGER, pl.concat([led, new], how="diagonal_relaxed").cast(SCHEMA).select(list(SCHEMA)))
    return new.height


def outcomes(store: Store, game_ids: list[int]) -> pl.DataFrame:
    """``game_id, player_id`` and the player's goals, assists and points for games with logs."""
    seasons = sorted({int(str(g)[:4]) for g in game_ids})
    frames = [f for y in seasons if (f := store.get_parquet(keys.player_game_logs(int(f"{y}{y + 1}")))) is not None]
    if not frames:
        return pl.DataFrame(schema={"game_id": pl.Int64, "player_id": pl.Int64})
    logs = pl.concat(frames, how="diagonal_relaxed").filter((pl.col("strength") == "all") & pl.col("game_id").is_in(game_ids))
    return logs.select("game_id", pl.col("player_id").cast(pl.Int64), pl.col("goals").cast(pl.Float64).alias("goals"),
                       (pl.col("a1") + pl.col("a2")).cast(pl.Float64).alias("assists")).with_columns(
        (pl.col("goals") + pl.col("assists")).alias("points"))


def grade(store: Store) -> int:
    """Grade every ungraded prop bet whose game is final and logged. Returns bets graded."""
    from nhl.betting.edges import _games
    from nhl.props.live import latest_quotes, market_probs

    led = load(store)
    todo = led.filter(pl.col("graded_at").is_null())
    if todo.is_empty():
        return 0
    games = _games(store, todo["game_id"].unique().to_list()).filter(pl.col("is_final"))
    res = outcomes(store, games["game_id"].to_list())
    logged = res["game_id"].unique().to_list()
    todo = todo.filter(pl.col("game_id").is_in(logged))
    if todo.is_empty():
        return 0
    starts = dict(games.select("game_id", "start_utc").iter_rows())
    season = int(games["season"][0])
    quotes = latest_quotes(store, season, todo["game_id"].unique().to_list(), {g: starts[g] for g in logged})
    probs = market_probs(quotes)
    close = probs.select("book", "game_id", "player_id", "prop_type", "line", "price_over", "price_under", "p_market")
    consensus = close.group_by("game_id", "player_id", "prop_type", "line").agg(pl.col("p_market").first().alias("_p_close"))
    long = res.unpivot(index=["game_id", "player_id"], on=["goals", "assists", "points"], variable_name="prop_type",
                       value_name="stat")
    g = (todo.drop("close_price", "p_close", "clv", "stat", "result", "pnl_units", "graded_at")
         .join(close.drop("p_market"), on=["book", "game_id", "player_id", "prop_type", "line"], how="left")
         .join(consensus, on=["game_id", "player_id", "prop_type", "line"], how="left")
         .join(long, on=["game_id", "player_id", "prop_type"], how="left"))
    dec = pl.when(pl.col("price") < 0).then(1 + 100 / -pl.col("price")).otherwise(1 + pl.col("price") / 100)
    over = pl.col("side") == "over"
    won = pl.when(over).then(pl.col("stat") > pl.col("line")).otherwise(pl.col("stat") < pl.col("line"))
    p_close = pl.when(over).then(pl.col("_p_close")).otherwise(1 - pl.col("_p_close"))
    g = g.with_columns(
        pl.when(over).then(pl.col("price_over")).otherwise(pl.col("price_under")).alias("close_price"),
        p_close.alias("p_close"), (p_close * dec - 1).alias("clv"),
        pl.when(pl.col("stat").is_null()).then(pl.lit("void")).when(won).then(pl.lit("win")).otherwise(pl.lit("loss"))
        .alias("result"),
        pl.when(pl.col("stat").is_null()).then(0.0).when(won).then(pl.col("stake_units") * (dec - 1))
        .otherwise(-pl.col("stake_units")).alias("pnl_units"),
        pl.lit(datetime.now(timezone.utc)).alias("graded_at"),
    ).cast(SCHEMA, strict=False).select(list(SCHEMA))
    out = pl.concat([led.join(g.select("bet_id"), on="bet_id", how="anti"), g]).sort("placed_at")
    store.put_parquet(keys.PROPS_LEDGER, out)
    return g.height


def summary(store: Store, by: tuple[str, ...] = ("prop_type", "side")) -> pl.DataFrame:
    """Graded prop bets by ``by``: count, mean CLV, share beating the close, units won, ROI."""
    led = load(store).filter(pl.col("graded_at").is_not_null() & (pl.col("result") != "void"))
    if led.is_empty():
        return led
    return led.group_by(*by).agg(
        pl.len().alias("bets"), pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
        pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        (pl.col("pnl_units").sum() / pl.col("stake_units").sum()).alias("roi"),
    ).sort(*by)


__all__ = ["SCHEMA", "add_paper", "day_stakes", "grade", "load", "outcomes", "summary"]
