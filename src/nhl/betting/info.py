"""What the model knew when a paper bet was placed, shared by the game and props ledgers.

Every paper bet carries ``pregame_stamp``, the pregame run its price came from; that run's slate
row is exactly the information the model priced with. :func:`attach` copies the parts that
measure how settled the game was:

* ``home_goalie`` / ``away_goalie``: DailyFaceoff starter status (``Confirmed`` | ``Likely``),
  ``Model`` when DailyFaceoff had nothing, and ``home_goalie_p`` / ``away_goalie_p``, the
  model's probability that the named starter starts;
* ``home_dfo_share`` / ``away_dfo_share``: share of each skater lineup taken from DailyFaceoff
  (the rest filled from usage and the last game);
* ``lineup_flags``: lineup issues plus game-time decisions across both teams;
* ``stale_inputs``: sources past their freshness limit;
* ``info_grade``: ``A`` (both starters confirmed, both lineups projected, nothing flagged or
  stale), ``C`` (a starter or a lineup from the model alone, or a stale input), else ``B``;
* ``window``: ``open`` (placed in the day's first edges run), ``post`` (within
  :data:`POST_MINUTES` of puck drop), else ``pre``.

Slates and edge snapshots are immutable, so :func:`attach` gives the same answer for a bet placed
today and for one backfilled later (:func:`backfill`).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date

import polars as pl

from nhl.sources.common import stamp as stamp_of
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: Minutes before puck drop that count as "post" (lineups out, closing prices forming).
POST_MINUTES = 30
#: A lineup counts as projected when at least this share of it came from DailyFaceoff.
PROJECTED_SHARE = 0.5

SCHEMA = {
    "home_goalie": pl.String, "away_goalie": pl.String, "home_goalie_p": pl.Float64, "away_goalie_p": pl.Float64,
    "home_dfo_share": pl.Float64, "away_dfo_share": pl.Float64, "lineup_flags": pl.Int64, "stale_inputs": pl.String,
    "info_grade": pl.String, "window": pl.String,
}


def _goalie(col: str) -> pl.Expr:
    return pl.when(pl.col(col).is_in(["Confirmed", "Likely"])).then(pl.col(col)).otherwise(pl.lit("Model"))


def _flags(side: str) -> pl.Expr:
    """Distinct lineup issues (``" | "``-joined in the slate) plus game-time decisions for one team."""
    issues = pl.col(f"{side}_lineup_issues").fill_null("")
    n_issues = pl.when(issues == "").then(0).otherwise(issues.str.count_matches(r" \| ") + 1)
    return n_issues + pl.col(f"{side}_game_time_decisions").fill_null(0).cast(pl.Int64)


def grade_expr() -> pl.Expr:
    """``info_grade`` from the snapshot columns (see the module docstring)."""
    goalies = [pl.col("home_goalie"), pl.col("away_goalie")]
    shares = [pl.col("home_dfo_share").fill_null(0.0), pl.col("away_dfo_share").fill_null(0.0)]
    stale = pl.col("stale_inputs").fill_null("") != ""
    weak = (goalies[0] == "Model") | (goalies[1] == "Model") | (shares[0] < PROJECTED_SHARE) | (shares[1] < PROJECTED_SHARE) | stale
    strong = (goalies[0] == "Confirmed") & (goalies[1] == "Confirmed") & (pl.col("lineup_flags") == 0)
    return pl.when(weak).then(pl.lit("C")).when(strong).then(pl.lit("A")).otherwise(pl.lit("B"))


def _slates(store: Store, pairs: list[tuple[date, str]]) -> pl.DataFrame:
    """The slate rows of each (game date, pregame stamp), with the snapshot columns."""
    frames = []
    for day, st in pairs:
        s = store.get_parquet(keys.pregame_slate(day, st))
        if s is None or s.is_empty():
            logger.warning("no pregame slate for %s %s", day, st)
            continue
        frames.append(s.select(
            "game_id", pl.lit(st).alias("pregame_stamp"), pl.col("start_time").cast(pl.Datetime("us", "UTC")).alias("_start"),
            _goalie("home_starter_dfo").alias("home_goalie"), _goalie("away_starter_dfo").alias("away_goalie"),
            pl.col("home_starter_p").alias("home_goalie_p"), pl.col("away_starter_p").alias("away_goalie_p"),
            "home_dfo_share", "away_dfo_share", (_flags("home") + _flags("away")).alias("lineup_flags"),
            pl.col("stale_inputs").cast(pl.String),
        ))
    if not frames:
        return pl.DataFrame(schema={"game_id": pl.Int64, "pregame_stamp": pl.String, "_start": pl.Datetime("us", "UTC")})
    return pl.concat(frames, how="diagonal_relaxed").cast({"game_id": pl.Int64})


def _first_runs(store: Store, days: list[date], prefix: Callable[[date], str]) -> pl.DataFrame:
    """``game_date, _first``: the stamp of each day's first edges snapshot."""
    out = []
    for d in days:
        stamps = sorted(k.rsplit("/", 1)[-1].removesuffix(".parquet") for k in store.list_keys(prefix(d)))
        out.append({"game_date": d, "_first": stamps[0] if stamps else None})
    return pl.DataFrame(out, schema={"game_date": pl.Date, "_first": pl.String})


def attach(store: Store, bets: pl.DataFrame, prefix: Callable[[date], str]) -> pl.DataFrame:
    """``bets`` (with ``game_id``, ``game_date``, ``pregame_stamp``, ``placed_at``) plus the
    snapshot columns in :data:`SCHEMA`.

    Args:
        store: Where the slates and edges snapshots live.
        bets: Ledger rows; existing snapshot columns are replaced.
        prefix: The edges snapshot prefix for a game date (game markets or props), used to tell
            the day's first run.
    """
    bets = bets.drop([c for c in SCHEMA if c in bets.columns])
    if bets.is_empty():
        return bets.with_columns(*[pl.lit(None, dtype=t).alias(c) for c, t in SCHEMA.items()])
    pairs = bets.select("game_date", "pregame_stamp").drop_nulls().unique().iter_rows()
    slates = _slates(store, list(pairs))
    firsts = _first_runs(store, bets["game_date"].unique().to_list(), prefix)
    out = (bets.join(slates, on=["game_id", "pregame_stamp"], how="left")
           .join(firsts, on="game_date", how="left"))
    for c, t in SCHEMA.items():
        if c not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=t).alias(c))
    placed_stamp = pl.col("placed_at").map_elements(stamp_of, return_dtype=pl.String)
    lead = (pl.col("_start") - pl.col("placed_at")).dt.total_minutes()
    has_snapshot = pl.col("home_goalie").is_not_null()
    return out.with_columns(
        pl.when(has_snapshot).then(grade_expr()).alias("info_grade"),
        pl.when(lead.is_not_null() & (lead < POST_MINUTES)).then(pl.lit("post"))
        .when(pl.col("_first").is_not_null() & (placed_stamp <= pl.col("_first"))).then(pl.lit("open"))
        .when(lead.is_not_null() | pl.col("_first").is_not_null()).then(pl.lit("pre"))
        .alias("window"),
    ).drop("_start", "_first").cast(SCHEMA)


def game_prefix(day: date) -> str:
    """Game-market edges snapshots for ``day``."""
    return keys.betting_edges(day, "x").rsplit("/", 1)[0] + "/"


def props_prefix(day: date) -> str:
    """Player-prop edges snapshots for ``day``."""
    return keys.props_edges(day, "x").rsplit("/", 1)[0] + "/"


def backfill(store: Store) -> dict[str, int]:
    """Fill the snapshot columns on every paper bet in both ledgers that lacks them. Returns
    bets filled per ledger."""
    from nhl.betting import ledger
    from nhl.props import ledger as props_ledger

    done = {}
    for name, mod, key, prefix in (("game", ledger, keys.BETS_LEDGER, game_prefix),
                                   ("props", props_ledger, keys.PROPS_LEDGER, props_prefix)):
        led = mod.load(store)
        todo = led.filter((pl.col("kind") == "paper") & pl.col("info_grade").is_null() & pl.col("pregame_stamp").is_not_null())
        if todo.is_empty():
            done[name] = 0
            continue
        filled = attach(store, todo, prefix).select(list(mod.SCHEMA))
        out = pl.concat([led.join(filled.select("bet_id"), on="bet_id", how="anti"), filled]).sort("placed_at")
        store.put_parquet(key, out.cast(mod.SCHEMA).select(list(mod.SCHEMA)))
        done[name] = filled.filter(pl.col("info_grade").is_not_null()).height
    return done


def breakdown(bets: pl.DataFrame) -> pl.DataFrame:
    """Graded, non-void bets by ``window`` and ``info_grade`` (nulls kept as their own group):
    count, units staked and won, ROI, mean CLV and share beating the close."""
    g = bets.filter(pl.col("graded_at").is_not_null() & ~pl.col("result").is_in(["void", "push"])) if bets.height else bets
    if g.is_empty():
        return pl.DataFrame(schema={"window": pl.String, "info_grade": pl.String, "bets": pl.UInt32, "staked": pl.Float64,
                                    "pnl": pl.Float64, "roi": pl.Float64, "mean_clv": pl.Float64, "beat_close": pl.Float64})
    order = {"open": 0, "pre": 1, "post": 2}
    return g.group_by("window", "info_grade").agg(
        pl.len().alias("bets"), pl.col("stake_units").sum().alias("staked"), pl.col("pnl_units").sum().alias("pnl"),
        pl.col("clv").mean().alias("mean_clv"), (pl.col("clv") > 0).mean().alias("beat_close"),
    ).with_columns((pl.col("pnl") / pl.col("staked")).alias("roi")).sort(
        pl.col("window").replace_strict(order, default=9, return_dtype=pl.Int8), pl.col("info_grade"), nulls_last=True)


__all__ = ["POST_MINUTES", "PROJECTED_SHARE", "SCHEMA", "attach", "backfill", "breakdown", "game_prefix", "grade_expr",
           "props_prefix"]
