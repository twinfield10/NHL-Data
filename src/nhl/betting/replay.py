"""Rebuild the official paper ledgers from the edges snapshots, under the current staking rules.

Every ``nhl edges`` / ``nhl props-edges`` run snapshots each quote it evaluated, with its
``flagged`` call and Kelly fraction. Replaying a day's snapshots in time order through the
current stakers (:func:`nhl.betting.edges._stakes`, :func:`nhl.props.live._stakes`) and
``add_paper`` gives the ledger those rules would have written live: same flags, same prices,
same placement times, new stakes. Use it after a staking rule changes (e.g. the daily caps
were dropped on 2026-10-10).

Real bets and paper bets on days that aren't replayed are kept as they are. A rebuilt bet that
matches an existing one (same id, price, placement time and stake) keeps its grading; any
other is left ungraded for ``nhl grade-bets``.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from nhl.betting import edges as game_edges
from nhl.betting import info
from nhl.betting import ledger as game_ledger
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: Columns filled by grading (each ledger has a subset).
GRADING = ["close_line", "close_price", "p_close", "clv", "stat", "result", "pnl_units", "graded_at"]


class _Overlay:
    """A store whose ledger key lives in memory; every other read goes to the real store."""

    def __init__(self, store: Store, key: str, ledger: pl.DataFrame) -> None:
        self._store, self._key, self.ledger = store, key, ledger

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        return self.ledger if key == self._key else self._store.get_parquet(key)

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        if key != self._key:
            raise ValueError(f"replay only writes the ledger, not {key}")
        self.ledger = df
        return key

    def __getattr__(self, name: str):
        return getattr(self._store, name)


def _spec(kind: str):
    """(ledger module, ledger key, snapshot prefix, stakes function, which rows to add, bet key)."""
    if kind == "game":
        return (game_ledger, keys.BETS_LEDGER, info.game_prefix, game_edges._stakes,
                pl.col("flagged") & ~pl.col("blocked"), ["game_id", "market", "side"])
    if kind == "props":
        from nhl.props import ledger as props_ledger
        from nhl.props import live

        return (props_ledger, keys.PROPS_LEDGER, info.props_prefix, live._stakes,
                pl.col("flagged") & ~pl.col("blocked") & (pl.col("stake_units") > 0), props_ledger.BET_KEY)
    raise ValueError(f"unknown ledger {kind!r}")


def snapshot_days(store: Store, kind: str) -> list[date]:
    """Game dates with at least one edges snapshot."""
    prefix = _spec(kind)[2](date(2000, 1, 1)).rsplit("/", 2)[0] + "/"
    return sorted({date.fromisoformat(k.split("/")[2]) for k in store.list_keys(prefix)})


def rebuild(store: Store, kind: str, days: list[date] | None = None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The rebuilt ledger and the current one (nothing is written).

    Args:
        store: The real store (read only here).
        kind: ``game`` or ``props``.
        days: Game dates to replay (default: every date with a snapshot).
    """
    mod, key, prefix, stakes, addable, bet_key = _spec(kind)
    days = days or snapshot_days(store, kind)
    current = mod.load(store)
    replayed = (pl.col("kind") == "paper") & pl.col("game_date").is_in(days)
    mem = _Overlay(store, key, current.filter(~replayed))
    # A bet placed live on a run counts as flagged on it, whatever the snapshot says: early runs
    # paper-traded track-only totals without flagging them.
    match = list(dict.fromkeys([*bet_key, "line", "book", "price", "as_of"]))
    live = current.filter(replayed).rename({"placed_at": "as_of"}).select(*match, pl.lit(True).alias("_live"))
    for day in days:
        for snap_key in sorted(store.list_keys(prefix(day))):
            snap = store.get_parquet(snap_key)
            if snap is None or snap.is_empty() or "flagged" not in snap.columns:
                continue
            snap = snap.drop("stake_units", "blocked", strict=False)
            snap = snap.join(live.cast({c: snap.schema[c] for c in match}), on=match, how="left", nulls_equal=True)
            snap = snap.with_columns((pl.col("flagged").fill_null(False) | pl.col("_live").fill_null(False)).alias("flagged")).drop("_live")
            e = stakes(snap, mem, day)
            mod.add_paper(mem, e.filter(addable))
    rebuilt = mem.ledger
    # Keep the grading of bets that didn't change.
    same = ["bet_id", "price", "placed_at", "stake_units"]
    grading = [c for c in GRADING if c in mod.SCHEMA]
    graded = current.filter(pl.col("graded_at").is_not_null()).select(*same, *grading)
    rebuilt = rebuilt.drop(grading).join(graded, on=same, how="left", nulls_equal=True)
    return rebuilt.cast(mod.SCHEMA).select(list(mod.SCHEMA)), current


def diff(rebuilt: pl.DataFrame, current: pl.DataFrame) -> pl.DataFrame:
    """Per game date: paper bets and units before and after."""
    def per_day(df: pl.DataFrame, tag: str) -> pl.DataFrame:
        return df.filter(pl.col("kind") == "paper").group_by("game_date").agg(
            pl.len().alias(f"bets_{tag}"), pl.col("stake_units").sum().round(2).alias(f"units_{tag}"),
            (pl.col("stake_units") == 0).sum().alias(f"zero_{tag}"))
    return per_day(current, "before").join(per_day(rebuilt, "after"), on="game_date", how="full", coalesce=True).sort("game_date")


def apply(store: Store, kind: str, days: list[date] | None = None) -> pl.DataFrame:
    """Rebuild and write the ledger under the live edges lock, so no poll writes it meanwhile."""
    key = _spec(kind)[1]
    if kind == "game":
        lock = game_edges._lock()
    else:
        from nhl.props import live

        lock = live._lock()
    with lock:
        rebuilt, current = rebuild(store, kind, days)
        store.put_parquet(key, rebuilt)
    logger.info("replay %s: %d -> %d bets", kind, current.height, rebuilt.height)
    return diff(rebuilt, current)
