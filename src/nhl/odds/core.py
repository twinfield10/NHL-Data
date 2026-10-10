"""The odds table: one schema for every book, stored as line transitions.

Row grain is one price on one side of one market from one book at one moment::

    book, game_id, captured_at, market_uid, side, line, price

* ``market``: ``moneyline`` | ``moneyline_3way`` | ``puckline`` | ``total`` | ``team_total``.
* ``moneyline_3way`` is the regulation-time three-way line (``period="reg"``, or the period
  it settles on, e.g. ``p1``): sides ``home`` | ``away`` | ``draw``. Books often publish only
  the two team prices, so a 3-way pair's implied probabilities sum well below 1 and must
  never be compared with a two-way moneyline.
* ``period``: ``game`` (full game incl. OT/SO) | ``reg`` (60 minutes) | ``p1`` | ``p2`` | ``p3``.
* ``side``: ``home`` | ``away`` for moneyline/puckline; ``over`` | ``under`` for totals;
  team totals use ``subject`` = ``home``/``away`` with ``over``/``under`` sides.
* ``line``: the handicap **from that side's perspective** (home -1.5 / away +1.5), the
  total for totals, null for moneylines.
* ``price``: American odds, as quoted (vig included).
* ``is_alternate``: True for every rung other than the book's main line (alternate puck
  lines, totals and team totals); the rung's line is then part of ``market_uid``.
* ``price_point``: ``live`` for our own polls; ``open`` / ``close`` / ``last`` for
  historical rows backfilled from a source that does not timestamp its prices.

``market_uid = period|market|subject|alt_line`` keeps alternates and team totals apart,
as in rebirtha-nfl (ADR 0005 there). ``alt_line`` is ``main`` for the main line; for an
alternate it is the rung's line, taken from the **home** side for puck lines so both sides
of one rung share a ``market_uid`` (home -2.5 / away +2.5 -> ``game|puckline|game|-2.5``).
"""

from __future__ import annotations

from datetime import timedelta

import polars as pl

from nhl.teams import resolve_team

MAIN_LINE = "main"
MARKETS = ("moneyline", "moneyline_3way", "puckline", "total", "team_total")
PERIODS = ("game", "reg", "p1", "p2", "p3")

#: Identity of one price series over time.
MARKET_KEY = ("book", "game_id", "market_uid", "side")
#: Values whose change makes a poll worth storing.
TRANSITION_VALUES = ("line", "price")

#: Books whose closing prices may serve as the CLV benchmark. The exchanges count: the live
#: close stops at the scheduled start, so an in-play exchange price never becomes a close.
REFERENCE_BOOKS = ("4Casters", "Novig", "LowVig", "DraftKings", "FanDuel", "Caesars", "BetMGM", "ESPN BET")

ODDS_SCHEMA: dict[str, pl.DataType] = {
    "book": pl.Utf8,
    "game_id": pl.Int64,
    "captured_at": pl.Datetime("us", "UTC"),
    "start_time": pl.Datetime("us", "UTC"),
    "away_team": pl.Utf8,
    "home_team": pl.Utf8,
    "period": pl.Utf8,
    "market": pl.Utf8,
    "subject": pl.Utf8,
    "side": pl.Utf8,
    "line": pl.Float64,
    "price": pl.Float64,
    "is_alternate": pl.Boolean,
    "depth": pl.Float64,
    "price_point": pl.Utf8,
    "source_event_id": pl.Utf8,
    "market_uid": pl.Utf8,
}


def odds_frame(rows: list[dict]) -> pl.DataFrame:
    """Build a correctly typed odds frame from row dicts and add ``market_uid``.

    Rows need at least book, captured_at, start_time, away_team, home_team, market, side,
    line, price. Team labels may be any spelling; they are resolved to tricodes here.
    Missing optional columns are filled with defaults (period ``game``, subject ``game``).
    """
    if not rows:
        return pl.DataFrame(schema=ODDS_SCHEMA)
    df = pl.DataFrame(rows, infer_schema_length=None, strict=False)
    for col, dtype in ODDS_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype).alias(col))
    for col in ("captured_at", "start_time"):
        if df.schema[col] == pl.Utf8:
            df = df.with_columns(pl.col(col).str.to_datetime(time_zone="UTC", strict=False))
        elif isinstance(df.schema[col], pl.Datetime) and df.schema[col].time_zone is None:
            df = df.with_columns(pl.col(col).dt.replace_time_zone("UTC"))
        else:
            df = df.with_columns(pl.col(col).dt.convert_time_zone("UTC"))
    df = df.with_columns(
        pl.col("period").fill_null("game"),
        pl.col("subject").fill_null("game"),
        pl.col("is_alternate").fill_null(False),
        pl.col("price_point").fill_null("live"),
    )
    # Resolve labels in the season the game was played, so "Phoenix"/"Coyotes" in 2012-13
    # becomes PHX (the games table's code then), "Utah" in 2024-25 becomes team 59, etc.
    df = df.with_columns(
        pl.struct(side, "start_time")
        .map_elements(lambda r, side=side: resolve_team(r[side], r["start_time"]), return_dtype=pl.Utf8)
        .alias(side)
        for side in ("away_team", "home_team")
    )
    df = df.with_columns(market_uid_expr())
    return df.select([pl.col(c).cast(t) for c, t in ODDS_SCHEMA.items()])


def market_uid_expr() -> pl.Expr:
    """``period|market|subject|alt_line`` (``main`` for the main line) as an expression.

    Re-apply it after relabelling ``period``/``market``/``is_alternate`` on a typed frame.
    """
    line = pl.col("line").cast(pl.Float64)
    home_line = pl.when((pl.col("market") == "puckline") & (pl.col("side") == "away")).then(-line).otherwise(line)
    rung = pl.when(home_line == 0).then(pl.lit(0.0)).otherwise(home_line)  # no "-0.0"
    return pl.concat_str(
        pl.col("period"), pl.col("market"), pl.col("subject"),
        pl.when(pl.col("is_alternate")).then(rung.cast(pl.Utf8)).otherwise(pl.lit(MAIN_LINE)),
        separator="|",
    ).alias("market_uid")


def attach_game_ids(odds: pl.DataFrame, games: pl.DataFrame, tolerance_days: int = 1) -> pl.DataFrame:
    """Fill ``game_id`` by matching (away, home) tricodes and start date to the games table.

    The book's start time is UTC; NHL ``game_date`` is the local date. A 7pm ET game is
    23:00 UTC the same day, a 10pm PT game is the next day in UTC, so a one-day tolerance
    is enough. Rows that still don't match keep a null ``game_id`` for the caller to log.

    Args:
        odds: Output of :func:`odds_frame`.
        games: ``processed/games.parquet``.
        tolerance_days: Max gap between UTC start date and local game date.
    """
    if odds.is_empty():
        return odds
    known = odds.filter(pl.col("game_id").is_not_null())
    todo = odds.filter(pl.col("game_id").is_null()).drop("game_id").with_row_index("_r")
    cand = (
        todo.select("_r", "away_team", "home_team", "start_time")
        .join(
            games.select("game_id", "game_date", pl.col("away_abbr").alias("away_team"), pl.col("home_abbr").alias("home_team")),
            on=["away_team", "home_team"],
            how="inner",
        )
        .with_columns((pl.col("start_time").dt.date() - pl.col("game_date")).abs().alias("_gap"))
        .filter(pl.col("_gap") <= timedelta(days=tolerance_days))
        .sort("_gap")
        .unique("_r", keep="first")
        .select("_r", "game_id")
    )
    matched = todo.join(cand, on="_r", how="left").drop("_r").select(odds.columns)
    return pl.concat([known, matched])


def implied_probability(american: float) -> float:
    """Raw (vigged) implied probability of an American price."""
    return 100.0 / (american + 100.0) if american > 0 else -american / (-american + 100.0)


def american_price(probability: float) -> float:
    """American price for a probability (inverse of :func:`implied_probability`)."""
    if probability >= 1.0:
        return -99999.0
    if probability <= 0.0:
        return 99999.0
    if probability > 0.5:
        return -(probability / (1.0 - probability)) * 100.0
    return (1.0 - probability) / probability * 100.0
