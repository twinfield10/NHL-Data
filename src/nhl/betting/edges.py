"""Live edges and stakes (M6 phase E): the latest pregame prices against the latest odds.

For every game on a date that hasn't started, every captured book and every main market
(moneyline, puck line, total):

1. **Model:** the probability at the book's exact line from the latest pregame snapshot's
   score matrix (a whole-number total as P(over | no push)).
2. **Market:** the devigged consensus of all books at the line most of them hang; a book at a
   different line uses its own devigged price.
3. **Blend:** :mod:`nhl.betting.blend` (per market and season segment).
4. **Edge** = (1 − P(push)) × (p × decimal − 1) for each side; the best book per side wins.
   A book more than :data:`OUTLIER` from the consensus is treated as a bad quote and skipped.
5. **Stake** (owner, 2026-10-06): ¼ Kelly on a bankroll of :data:`BANKROLL_UNITS` units,
   at most 2 u per bet and 3 u per game (moneyline and puck line on a game are correlated),
   counting bets already in the ledger. No daily cap while paper bets are being tracked
   (owner, 2026-10-10). **Laddered** (owner, 2026-10-10; docs/plans/bet-timing.md phase B):
   a position's ceiling is a share of the per-bet cap set by the time to puck drop
   (:data:`LADDER`: 25% more than 24 h out, 50% to 6 h, 100% after). Each tier fills at most
   once, at its first run where the side still flags, up to ``min(¼ Kelly now, ceiling)``; a
   top-up under :data:`MIN_TOP_UP` is skipped and a position never shrinks. Every fill is its
   own ledger row (``fill``, ``ladder``), at its own price and line.
   A side whose market already has the other side in the ledger is ``blocked`` (no stake, no
   paper bet): the official ledger never hedges itself when the model flips (owner, 2026-10-10).
6. **Flag** when the edge clears :data:`MIN_EDGE`.

**Tiers** say how much history stands behind a bet (all are staked the same, by the owner's
choice; the ledger measures each tier's CLV separately):

* ``validated``: moneylines in November-February (CLV at the open +1.0% ± 0.4%, 2016-26);
* ``unvalidated``: moneylines in other months, puck lines, totals (totals CLV was −1% in
  both eras; staked anyway by the owner's choice, 2026-10-07).

Outputs ``pregame/edges/{date}/{stamp}.parquet`` and appends newly flagged bets to the paper
ledger (:mod:`nhl.betting.ledger`).
"""

from __future__ import annotations

import fcntl
import json
import logging
from contextlib import contextmanager
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from nhl.betting import blend, devig, evaluate, ledger
from nhl.betting import lines as lines_mod
from nhl.sources.common import stamp
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

EASTERN = ZoneInfo("America/New_York")
BANKROLL_UNITS = 100.0
KELLY_FRACTION = 0.25
MAX_BET_UNITS = 2.0
MAX_GAME_UNITS = 3.0
#: (hours before puck drop at least, share of :data:`MAX_BET_UNITS`, tier) — first match wins.
LADDER = ((24.0, 0.25, "early"), (6.0, 0.5, "day"), (0.0, 1.0, "late"))
#: Smallest top-up worth a fill (a first fill is recorded at any size, even 0 when the game is full).
MIN_TOP_UP = 0.1
MIN_EDGE = {"moneyline": 0.02, "puckline": 0.03, "total": 0.03}
OUTLIER = 0.03
LOCK_PATH = "/tmp/nhl_data_edges.lock"


def tier(market: str, segment: str) -> str:
    return "validated" if market == "moneyline" and segment == "nov_feb" else "unvalidated"


@contextmanager
def _lock():
    with open(LOCK_PATH, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def latest_prices(store: Store, day: date) -> tuple[pl.DataFrame, str] | None:
    """The newest pregame prices snapshot for ``day`` and its stamp."""
    raw = store.get_bytes(keys.pregame_latest(day))
    if raw is None:
        return None
    st = json.loads(raw)["stamp"]
    prices = store.get_parquet(keys.pregame_prices(day, st))
    return (prices, st) if prices is not None else None


#: Judge each book's price against the consensus at its own line (every book quoting that line,
#: alternate rungs included) rather than at the market's primary line only.
BY_LINE = True


def book_rows(store: Store, season: int, game_ids: list[int], point: str = "last",
              by_line: bool | None = None) -> pl.DataFrame:
    """Price per book, game and market at ``point`` (``last`` = now, ``close`` = the final
    capture before puck drop), cleaned, with the market consensus at the same point.

    Rows are each book's main line. With ``by_line`` (default :data:`BY_LINE`) the consensus a
    row is compared with is the one at the row's own line, pooled over every book's main line
    and alternate rungs there; otherwise it is the primary line's, and a book hanging another
    line is compared with itself only.
    """
    by_line = BY_LINE if by_line is None else by_line
    live = lines_mod.clean(lines_mod.live_lines(store, season)).filter(
        (pl.col("point") == point) & pl.col("game_id").is_in(game_ids)
    )
    if live.is_empty():
        return live
    if by_line:
        pool = lines_mod.clean(lines_mod.live_lines(store, season, alternates=True)).filter(
            (pl.col("point") == point) & pl.col("game_id").is_in(game_ids))
        at_line = pl.concat([devig.consensus_by_line(pool.filter(pl.col("market") == m), method, (point,))
                             for m, method in evaluate.METHOD.items()])
        cons = live.select("game_id", "market", "line").unique().join(at_line, on=["game_id", "market", "line"], how="left",
                                                                       nulls_equal=True)
        cons = cons.select("game_id", "market", "line", pl.col("line").alias("cons_line"), pl.col("p_fair").alias("p_cons"),
                           pl.col("books").alias("cons_books"))
        key = ["game_id", "market", "line"]
    else:
        cons = pl.concat([
            devig.consensus(live.filter(pl.col("market") == m), method, (point,)) for m, method in evaluate.METHOD.items()
        ]).select("game_id", "market", pl.col("line").alias("cons_line"), pl.col("p_fair").alias("p_cons"), pl.col("books").alias("cons_books"))
        key = ["game_id", "market"]
    parts = []
    for (market,), part in live.partition_by("market", as_dict=True).items():
        parts.append(part.with_columns(pl.Series("p_book", devig.fair(part["price_1"].to_numpy(), part["price_2"].to_numpy(),
                                                                       evaluate.METHOD[market]))))
    rows = pl.concat(parts).join(cons, on=key, how="left", nulls_equal=True)
    same = (pl.col("line") == pl.col("cons_line")) | (pl.col("line").is_null() & pl.col("cons_line").is_null())
    return rows.with_columns(
        pl.when(same).then(pl.col("p_cons")).otherwise(pl.col("p_book")).alias("p_market"),
        (same & ((pl.col("p_book") - pl.col("p_cons")).abs() > OUTLIER)).alias("outlier"),
    )


def _games(store: Store, game_ids: list[int]) -> pl.DataFrame:
    """Catalog rows for ``game_ids`` with the start time in UTC."""
    return store.read_parquet_required(keys.GAMES).filter(pl.col("game_id").is_in(game_ids)).with_columns(
        pl.col("start_time_et").str.to_datetime().dt.replace_time_zone("America/New_York").dt.convert_time_zone("UTC").alias("start_utc")
    )


def _price(store: Store, rows: pl.DataFrame, prices: pl.DataFrame, games: pl.DataFrame, day: date,
           now: datetime) -> pl.DataFrame:
    """Model x market x blend for book ``rows``; the best book per (game, market, side, line),
    flagged against :data:`MIN_EDGE` (no stakes). ``prices`` carries each game's pregame ``stamp``."""
    rows = evaluate.attach_model(rows, prices)
    model = blend.load(store)
    seg = blend.segment_of(day)
    parts = []
    for (market,), part in rows.partition_by("market", as_dict=True).items():
        parts.append(part.with_columns(pl.Series("p_blend", blend.apply(model, market, seg, part["p_market"].to_numpy(),
                                                                         part["p_model"].to_numpy()))))
    rows = pl.concat(parts)
    sides = []
    for side, col, flip in ((1, "price_1", False), (2, "price_2", True)):
        p = (1 - pl.col("p_blend")) if flip else pl.col("p_blend")
        dec = pl.when(pl.col(col) < 0).then(1 + 100 / -pl.col(col)).otherwise(1 + pl.col(col) / 100)
        sides.append(rows.with_columns(
            pl.lit(side).alias("side"), pl.col(col).alias("price"), dec.alias("decimal"), p.alias("p"),
            (((1 - pl.col("p_model")) if flip else pl.col("p_model"))).alias("p_model_side"),
            (((1 - pl.col("p_market")) if flip else pl.col("p_market"))).alias("p_market_side"),
        ))
    out = pl.concat(sides).with_columns(
        ((1 - pl.col("p_push")) * (pl.col("p") * pl.col("decimal") - 1)).alias("edge"),
        (KELLY_FRACTION * (pl.col("p") * pl.col("decimal") - 1) / (pl.col("decimal") - 1)).clip(0, None).alias("kelly"),
    ).filter(~pl.col("outlier"))
    best = out.sort("edge", descending=True).group_by("game_id", "market", "side", "line", maintain_order=True).first()
    best = best.join(games.select("game_id", "game_date", "start_utc", "home_abbr", "away_abbr"), on="game_id").join(
        prices.select("game_id", pl.col("stamp").alias("pregame_stamp")), on="game_id", how="left",
    ).with_columns(
        pl.col("market").map_elements(lambda m: tier(m, seg), return_dtype=pl.String).alias("tier"),
        pl.lit(seg).alias("segment"), pl.lit(now).alias("as_of"),
    )
    return best.with_columns(
        (pl.col("edge") >= pl.col("market").replace_strict(MIN_EDGE, return_dtype=pl.Float64)).alias("flagged"),
    )


def compute(store: Store, day: date | None = None, now: datetime | None = None) -> pl.DataFrame:
    """Best edge per (game, market, side, line) for ``day``'s games not yet started."""
    now = now or datetime.now(timezone.utc)
    day = day or now.astimezone(EASTERN).date()
    got = latest_prices(store, day)
    if got is None:
        logger.info("edges %s: no pregame snapshot", day)
        return pl.DataFrame()
    prices, pregame_stamp = got
    games = _games(store, prices["game_id"].to_list()).filter(pl.col("start_utc") > now)
    if games.is_empty():
        return pl.DataFrame()
    rows = book_rows(store, int(games["season"][0]), games["game_id"].to_list())
    if rows.is_empty():
        logger.info("edges %s: no live odds", day)
        return pl.DataFrame()
    prices = prices.with_columns(pl.lit(pregame_stamp).alias("stamp"))
    return _stakes(_price(store, rows, prices, games, day, now), store, day)


def last_pregame_prices(store: Store, day: date) -> pl.DataFrame | None:
    """Each game's prices (with score matrix and ``stamp``) from the last pregame run that priced it."""
    keys_ = sorted(k for k in store.list_keys(f"pregame/prices/{day.isoformat()}/") if k.endswith(".parquet"))
    frames = [df.with_columns(pl.lit(k.rsplit("/", 1)[-1].removesuffix(".parquet")).alias("stamp"))
              for k in keys_ if (df := store.get_parquet(k)) is not None]
    if not frames:
        return None
    prices = pl.concat(frames, how="diagonal_relaxed")
    return prices.filter(pl.col("stamp") == pl.col("stamp").max().over("game_id"))


def closing(store: Store, day: date, now: datetime | None = None) -> pl.DataFrame:
    """The edge view at the close for ``day``'s games that have started: every book's last
    price before puck drop against the model's final pregame price. Same columns as
    :func:`compute` with ``stake_units`` 0 (nothing can be bet any more)."""
    now = now or datetime.now(timezone.utc)
    prices = last_pregame_prices(store, day)
    if prices is None:
        return pl.DataFrame()
    games = _games(store, prices["game_id"].to_list()).filter(pl.col("start_utc") <= now)
    if games.is_empty():
        return pl.DataFrame()
    rows = book_rows(store, int(games["season"][0]), games["game_id"].to_list(), point="close")
    if rows.is_empty():
        return pl.DataFrame()
    return _price(store, rows, prices, games, day, now).with_columns(pl.lit(0.0).alias("stake_units"))


def ladder_tier(start: pl.Expr, as_of: pl.Expr) -> tuple[pl.Expr, pl.Expr]:
    """``(tier, share of the per-bet cap)`` for a run at ``as_of`` before a ``start``."""
    hours = (start - as_of).dt.total_seconds() / 3600
    tier, share = pl.lit(LADDER[-1][2]), pl.lit(LADDER[-1][1])
    for h, frac, name in reversed(LADDER[:-1]):
        tier = pl.when(hours > h).then(pl.lit(name)).otherwise(tier)
        share = pl.when(hours > h).then(pl.lit(frac)).otherwise(share)
    return tier, share


def _stakes(edges: pl.DataFrame, store: Store, day: date) -> pl.DataFrame:
    """Fills per flagged side under the ladder and the per-bet and per-game caps.

    Adds ``ladder`` (this run's tier), ``blocked``, ``fill_units`` (this run's new units on the
    side), ``fill_new`` (the row gets a ledger fill) and ``stake_units`` (the position after
    this run). A side already in the paper ledger keeps what it has; it tops up once per
    tier, only on its best row this run (the highest edge across books and lines). A flagged
    side is ``blocked`` when the other side of its (game, market) is in the ledger, or flags
    on the same run with a bigger edge. Game room counts everything placed on the game.
    """
    key = ["game_id", "market", "side"]
    placed = ledger.load(store).filter((pl.col("kind") == "paper") & (pl.col("game_date") == day)).with_columns(
        pl.col("side").cast(pl.Int64))
    pos = placed.group_by(key).agg(pl.col("stake_units").sum().alias("_placed"), pl.col("ladder").alias("_filled"))
    taken = placed.group_by("game_id", "market").agg(pl.col("side").first().alias("_taken"))
    tier, share = ladder_tier(pl.col("start_utc"), pl.col("as_of"))
    e = (edges.with_columns(pl.col("side").cast(pl.Int64)).join(pos, on=key, how="left")
         .join(taken, on=["game_id", "market"], how="left").with_columns(tier.alias("ladder"), share.alias("_share")))
    flagged = pl.col("flagged")
    best = pl.when(flagged).then(pl.col("edge")).max().over(*key)
    rival = pl.when(flagged).then(pl.col("edge")).max().over("game_id", "market")
    e = e.with_columns((flagged & pl.when(pl.col("_taken").is_null()).then(best < rival)
                        .otherwise(pl.col("_taken") != pl.col("side"))).alias("blocked"))
    open_tier = pl.col("_filled").is_null() | ~pl.col("_filled").list.contains(pl.col("ladder"))
    can = flagged & ~pl.col("blocked") & open_tier
    eligible = can & (pl.when(can).then(pl.col("edge")).rank("ordinal", descending=True).over(*key) == 1).fill_null(False)
    target = (pl.col("kelly") * BANKROLL_UNITS).clip(0, None).clip(None, MAX_BET_UNITS * pl.col("_share"))
    e = e.with_columns(eligible.alias("_eligible"), pl.when(eligible).then(
        (target - pl.col("_placed").fill_null(0.0)).clip(0, None)).otherwise(0.0).alias("_new"))
    game_used = placed.group_by("game_id").agg(pl.col("stake_units").sum().alias("_used"))
    e = e.join(game_used, on="game_id", how="left").with_columns(
        (MAX_GAME_UNITS - pl.col("_used").fill_null(0.0)).clip(0, None).alias("_room"))
    new_game = pl.col("_new").sum().over("game_id")
    e = e.with_columns(pl.when(new_game > pl.col("_room")).then(pl.col("_new") * pl.col("_room") / new_game)
                       .otherwise(pl.col("_new")).round(2).alias("_new"))
    first_fill = pl.col("_placed").is_null()
    e = e.with_columns(
        (pl.col("_eligible") & (first_fill | (pl.col("_new") >= MIN_TOP_UP))).alias("fill_new"))
    return e.with_columns(
        pl.when(pl.col("fill_new")).then(pl.col("_new")).otherwise(0.0).alias("fill_units"),
    ).with_columns((pl.col("_placed").fill_null(0.0) + pl.col("fill_units")).round(2).alias("stake_units")).drop(
        "_placed", "_filled", "_taken", "_share", "_eligible", "_new", "_used", "_room")


def run(store: Store, day: date | None = None, write: bool = True) -> pl.DataFrame:
    """Compute edges, snapshot them, and add new flagged bets to the paper ledger."""
    with _lock():
        now = datetime.now(timezone.utc)
        day = day or now.astimezone(EASTERN).date()
        e = compute(store, day, now)
        if e.is_empty() or not write:
            return e
        st = stamp(now)
        store.put_parquet(keys.betting_edges(day, st), e.drop("score_matrix", strict=False).with_columns(pl.lit(st).alias("stamp")))
        new = ledger.add_paper(store, e.filter(pl.col("fill_new")))
        logger.info("edges %s: %d flagged, %d new fills (%.2f u)", day, e.filter(pl.col("flagged")).height, new,
                    float(e["fill_units"].sum()))
        return e


def render(e: pl.DataFrame) -> str:
    """Flagged bets, then the best unflagged edge per game, for the terminal."""
    if e.is_empty():
        return "no edges (no pregame snapshot, no odds, or all games started)"
    def line(r: dict) -> str:
        team = r["home_abbr"] if r["side"] == 1 else r["away_abbr"]
        if r["market"] == "moneyline":
            side = team
        elif r["market"] == "puckline":
            side = f"{team} {(r['line'] if r['side'] == 1 else -r['line']):+.1f}"
        else:
            side = f"{'over' if r['side'] == 1 else 'under'} {r['line']}"
        start = r["start_utc"].astimezone(EASTERN).strftime("%H:%M")
        return (f"  {start} {r['away_abbr']}@{r['home_abbr']}  {r['market']:9s} {side:12s} {int(r['price']):+5d} {r['book']:<12s} "
                f"edge {100 * r['edge']:+5.1f}%  model {100 * r['p_model_side']:4.1f}% mkt {100 * r['p_market_side']:4.1f}% "
                f"blend {100 * r['p']:4.1f}%  {r['stake_units']:.2f}u  [{r['tier']}]")
    flagged = e.filter(pl.col("flagged")).sort("start_utc", "edge", descending=[False, True])
    out = [f"FLAGGED ({flagged.height}, {flagged['stake_units'].sum():.2f} u):"] + [line(r) for r in flagged.iter_rows(named=True)]
    if flagged.is_empty():
        out.append("  none")
    return "\n".join(out)
