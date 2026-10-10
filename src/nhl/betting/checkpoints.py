"""Checkpoint ledgers (docs/plans/bet-timing.md phase C): every side at fixed moments, graded.

The official ledgers record what we bet (laddered game lines, props by their own rule). These
backend-only ledgers record what **betting at one moment only** would have done, so timings
can be compared with each other and with the official ledger. They are rebuilt from the edges
snapshots once a day is final; nothing is placed live.

**Checkpoints.** The quote at a checkpoint is the game's latest edges snapshot at or before
that moment (snapshots are written only when something changed, so that is exactly what we
would have seen):

* ``open``: the game's first snapshot (the first with both a model price and a market);
* ``first_flag``: for each side, the first snapshot where it flags;
* ``goalies_confirmed``: the first snapshot whose pregame slate has both starters ``Confirmed``;
* ``T-24h``, ``T-6h``, ``T-3h``, ``T-60m``: the last snapshot at or before puck drop minus 24 h,
  6 h, 3 h, 60 min;
* ``close``: the last snapshot before puck drop (CLV ≈ 0 by construction; its value is the
  result: does the model beat the closing line on outcomes?).

A game with no snapshot by a checkpoint's moment has no rows for it.

**Rows.** One per (checkpoint, side): the best quote per side (game lines: the highest edge
across books and lines; props: per line, as in the props ledger), flagged or not. Unflagged
rows have stake 0 but are graded, so the edge threshold itself can be studied. Props keep
unflagged rows only with a positive edge, which bounds the table.

**Stakes.** Each checkpoint is its own all-in strategy, sized as if it were the only bet:
game lines ¼ Kelly up to 2 u per bet and 3 u per game (no ladder); props by the props rules at
that moment (a tenth of ¼ Kelly, 2× at least 4 h out, per-bet and per-player caps). When both
sides of a market flag at one checkpoint, the bigger edge is staked. Never sum checkpoints.

Stored at ``bets/checkpoints.parquet`` and ``bets/props_checkpoints.parquet``.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable

import polars as pl

from nhl.betting import info
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

CHECKPOINTS = ("open", "first_flag", "goalies_confirmed", "T-24h", "T-6h", "T-3h", "T-60m", "close")
#: Hours before puck drop of the fixed-offset checkpoints.
OFFSETS = {"T-24h": 24.0, "T-6h": 6.0, "T-3h": 3.0, "T-60m": 1.0}
#: Unflagged prop quotes are kept only above this edge.
PROPS_MIN_EDGE = 0.0
#: Columns the live stakers add to a snapshot; recomputed here.
_STAKE_COLS = ("stake_units", "blocked", "ladder", "fill_units", "fill_new", "score_matrix")


@dataclass(frozen=True)
class Kind:
    """One checkpoint ledger: where its snapshots and rows live, and how a side is keyed."""

    name: str
    key: str
    prefix: Callable[[date], str]
    side_key: tuple[str, ...]
    columns: tuple[str, ...]


GAME = Kind("game", keys.BETS_CHECKPOINTS, info.game_prefix, ("game_id", "market", "side"), (
    "game_id", "game_date", "market", "side", "line", "book", "price", "edge", "kelly", "flagged", "tier",
    "p_model_side", "p_market_side", "p", "pregame_stamp", "start_utc"))
PROPS = Kind("props", keys.PROPS_CHECKPOINTS, info.props_prefix, ("game_id", "player_id", "prop_type", "line", "side"), (
    "game_id", "game_date", "player_id", "player_name", "team", "prop_type", "line", "side", "book", "price", "edge",
    "kelly", "flagged", "books", "p_model_side", "p_market_side", "p", "pregame_stamp", "start_utc"))
KINDS = {"game": GAME, "props": PROPS}


def snapshots(store: Store, kind: Kind, day: date) -> pl.DataFrame:
    """Every edges snapshot of ``day``, stacked, with ``stamp`` and ``as_of``."""
    frames = []
    for k in sorted(store.list_keys(kind.prefix(day))):
        f = store.get_parquet(k)
        if f is None or f.is_empty() or "flagged" not in f.columns:
            continue
        st = k.rsplit("/", 1)[-1].removesuffix(".parquet")
        frames.append(f.drop([c for c in _STAKE_COLS if c in f.columns]).with_columns(pl.lit(st).alias("stamp")))
    if not frames:
        return pl.DataFrame()
    out = pl.concat(frames, how="diagonal_relaxed")
    return out.with_columns(pl.col("start_utc").cast(pl.Datetime("us", "UTC")), pl.col("as_of").cast(pl.Datetime("us", "UTC")))


def best_quotes(snaps: pl.DataFrame, kind: Kind) -> pl.DataFrame:
    """The best quote per side in each snapshot (props: unflagged ones only with a positive edge)."""
    if kind is PROPS:
        snaps = snaps.filter(pl.col("flagged") | (pl.col("edge") > PROPS_MIN_EDGE))
    return snaps.sort("edge", descending=True, nulls_last=True).group_by("stamp", *kind.side_key, maintain_order=True).first()


def confirmed_stamps(store: Store, best: pl.DataFrame) -> pl.DataFrame:
    """``game_id, stamp`` of the snapshots whose pregame slate has both starters ``Confirmed``."""
    pairs = best.select("game_date", "pregame_stamp").drop_nulls().unique()
    slates = info._slates(store, list(pairs.iter_rows()))
    if slates.is_empty() or "home_goalie" not in slates.columns:
        return pl.DataFrame(schema={"game_id": pl.Int64, "stamp": pl.String})
    both = slates.filter((pl.col("home_goalie") == "Confirmed") & (pl.col("away_goalie") == "Confirmed"))
    return best.select("game_id", "pregame_stamp", "stamp").unique().join(
        both.select("game_id", "pregame_stamp"), on=["game_id", "pregame_stamp"]).select("game_id", "stamp").unique()


def pick(best: pl.DataFrame, kind: Kind, confirmed: pl.DataFrame) -> pl.DataFrame:
    """``best`` rows at each checkpoint, with ``checkpoint``."""
    runs = best.select("game_id", "stamp", "as_of", "start_utc").unique()
    moments = [
        runs.group_by("game_id").agg(pl.col("stamp").min()).with_columns(pl.lit("open").alias("checkpoint")),
        runs.group_by("game_id").agg(pl.col("stamp").max()).with_columns(pl.lit("close").alias("checkpoint")),
        confirmed.group_by("game_id").agg(pl.col("stamp").min()).with_columns(pl.lit("goalies_confirmed").alias("checkpoint")),
    ]
    for name, hours in OFFSETS.items():
        moments.append(runs.filter(pl.col("as_of") <= pl.col("start_utc") - timedelta(hours=hours))
                       .group_by("game_id").agg(pl.col("stamp").max()).with_columns(pl.lit(name).alias("checkpoint")))
    at = pl.concat([m.select("game_id", "stamp", "checkpoint") for m in moments])
    fixed = best.join(at, on=["game_id", "stamp"])
    first = (best.filter(pl.col("flagged")).sort("stamp").group_by(*kind.side_key, maintain_order=True).first()
             .with_columns(pl.lit("first_flag").alias("checkpoint")))
    return pl.concat([fixed, first.select(fixed.columns)])


def stake_game(rows: pl.DataFrame) -> pl.DataFrame:
    """All-in game-line stakes per checkpoint: ¼ Kelly, 2 u per bet, 3 u per game."""
    from nhl.betting.edges import BANKROLL_UNITS, MAX_BET_UNITS, MAX_GAME_UNITS

    rival = pl.when(pl.col("flagged")).then(pl.col("edge")).max().over("checkpoint", "game_id", "market")
    keep = pl.col("flagged") & (pl.col("edge") >= rival)
    rows = rows.with_columns(pl.when(keep).then((pl.col("kelly") * BANKROLL_UNITS).clip(0, MAX_BET_UNITS))
                             .otherwise(0.0).alias("stake_units"))
    total = pl.col("stake_units").sum().over("checkpoint", "game_id")
    return rows.with_columns(pl.when(total > MAX_GAME_UNITS).then(pl.col("stake_units") * MAX_GAME_UNITS / total)
                             .otherwise(pl.col("stake_units")).round(2).alias("stake_units"))


def stake_props(rows: pl.DataFrame) -> pl.DataFrame:
    """All-in prop stakes per checkpoint, by the props rules at the checkpoint's moment."""
    from nhl.props.live import (BANKROLL_UNITS, EARLY_HOURS, EARLY_MULTIPLIER, MAX_BET_UNITS, MAX_PLAYER_UNITS,
                                UNIT_SCALE)

    stat = ["checkpoint", "game_id", "player_id", "prop_type"]
    mult = pl.when(pl.col("start_utc") - pl.col("as_of") >= timedelta(hours=EARLY_HOURS)).then(EARLY_MULTIPLIER).otherwise(1.0)
    best = pl.when(pl.col("flagged")).then(pl.col("edge")).max().over(*stat, "side")
    rival = pl.when(pl.col("flagged")).then(pl.col("edge")).max().over(stat)
    keep = pl.col("flagged") & (best >= rival)
    rows = rows.with_columns(mult.alias("_mult")).with_columns(
        pl.when(keep).then((pl.col("kelly") * BANKROLL_UNITS * UNIT_SCALE * pl.col("_mult"))
                           .clip(0, None).clip(None, MAX_BET_UNITS * pl.col("_mult"))).otherwise(0.0).alias("stake_units"))
    player = ["checkpoint", "game_id", "player_id"]
    total, cap = pl.col("stake_units").sum().over(player), (MAX_PLAYER_UNITS * pl.col("_mult")).max().over(player)
    return rows.with_columns(pl.when(total > cap).then(pl.col("stake_units") * cap / total)
                             .otherwise(pl.col("stake_units")).round(3).alias("stake_units")).drop("_mult")


def _ids(rows: pl.DataFrame, kind: Kind) -> pl.Expr:
    parts = [pl.col("checkpoint"), *[pl.col(c).cast(pl.String).fill_null("") for c in kind.side_key]]
    return pl.concat_str(parts, separator="|").map_elements(lambda s: hashlib.sha1(s.encode()).hexdigest()[:16],
                                                           return_dtype=pl.String)


def build_day(store: Store, kind: Kind, day: date) -> pl.DataFrame:
    """``day``'s checkpoint rows, staked and graded (rows on games not final stay ungraded)."""
    snaps = snapshots(store, kind, day)
    if snaps.is_empty():
        return pl.DataFrame()
    best = best_quotes(snaps, kind)
    rows = pick(best, kind, confirmed_stamps(store, best))
    rows = stake_game(rows) if kind is GAME else stake_props(rows)
    rows = rows.select(
        pl.col("checkpoint"), pl.col("stamp").alias("snapshot_stamp"), pl.col("as_of").alias("placed_at"),
        *[pl.col(c) for c in kind.columns if c in rows.columns], "stake_units",
    ).rename({"p_model_side": "p_model", "p_market_side": "p_market", "p": "p_blend"})
    if kind is GAME:
        rows = rows.with_columns(pl.col("side").cast(pl.Int8))
    rows = rows.with_columns(_ids(rows, kind).alias("bet_id"))
    if kind is GAME:
        from nhl.betting.ledger import grade_frame
    else:
        from nhl.props.ledger import grade_frame
    graded = grade_frame(store, rows)
    return pl.concat([rows.join(graded.select("bet_id"), on="bet_id", how="anti"), graded], how="diagonal_relaxed")


def snapshot_days(store: Store, kind: Kind) -> list[date]:
    """Game dates with at least one edges snapshot."""
    prefix = kind.prefix(date(2000, 1, 1)).rsplit("/", 2)[0] + "/"
    return sorted({date.fromisoformat(k.split("/")[2]) for k in store.list_keys(prefix)})


def build(store: Store, kind: Kind, days: list[date], write: bool = True) -> pl.DataFrame:
    """Rebuild ``days`` in ``kind``'s checkpoint ledger (other days are kept). Returns the new rows."""
    frames = [f for d in days if not (f := build_day(store, kind, d)).is_empty()]
    new = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    if write and not new.is_empty():
        old = store.get_parquet(kind.key)
        keep = old.filter(~pl.col("game_date").is_in(days)) if old is not None and old.height else None
        out = pl.concat([keep, new], how="diagonal_relaxed") if keep is not None else new
        store.put_parquet(kind.key, out.sort("game_date", "game_id", "checkpoint"))
    logger.info("checkpoints %s: %d rows over %d day(s)", kind.name, new.height, len(days))
    return new


def load(store: Store, kind: Kind) -> pl.DataFrame:
    return store.get_parquet(kind.key) if store.get_parquet(kind.key) is not None else pl.DataFrame()


# --------------------------------------------------------------------------- report

def _metrics(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """Graded, staked bets by ``by``: count, units, mean edge, mean CLV ± SE, share beating the
    close, ROI."""
    g = df.filter(pl.col("graded_at").is_not_null() & (pl.col("stake_units") > 0) & ~pl.col("result").is_in(["void"]))
    if g.is_empty():
        return pl.DataFrame()
    return g.group_by(by).agg(
        pl.len().alias("bets"), pl.col("stake_units").sum().round(2).alias("units"),
        pl.col("edge").mean().round(4).alias("mean_edge"), pl.col("clv").mean().round(4).alias("mean_clv"),
        (pl.col("clv").std() / pl.len().sqrt()).round(4).alias("se_clv"), (pl.col("clv") > 0).mean().round(3).alias("beat_close"),
        (pl.col("pnl_units").sum() / pl.col("stake_units").sum()).round(3).alias("roi"),
    ).sort(by)


def report(store: Store, kind: Kind) -> dict[str, pl.DataFrame]:
    """Strategy comparison (official ledger vs all-in at each checkpoint), edge decay of the
    official positions across checkpoints, and (game lines) CLV by ladder tier."""
    cp = load(store, kind)
    group = "market" if kind is GAME else "prop_type"
    if kind is GAME:
        from nhl.betting import ledger as led_mod
    else:
        from nhl.props import ledger as led_mod
    official = led_mod.load(store).filter(pl.col("kind") == "paper").with_columns(pl.lit("official").alias("strategy"))
    frames = [official.select("strategy", group, "stake_units", "edge", "clv", "result", "pnl_units", "graded_at")]
    if not cp.is_empty():
        frames.append(cp.select(pl.col("checkpoint").alias("strategy"), group, "stake_units", "edge", "clv", "result",
                                "pnl_units", "graded_at"))
    order = {s: i for i, s in enumerate(("official", *CHECKPOINTS))}
    out = {}
    for by, name in (([], "strategies"), ([group], f"strategies_by_{group}")):
        m = _metrics(pl.concat(frames, how="diagonal_relaxed"), ["strategy", *by])
        out[name] = m.sort(pl.col("strategy").replace_strict(order, default=99), *by) if m.height else m
    if not cp.is_empty():
        held = official.select(*kind.side_key).unique()
        decay = cp.join(held, on=list(kind.side_key)).group_by("checkpoint").agg(
            pl.len().alias("sides"), pl.col("edge").mean().round(4).alias("mean_edge"),
            pl.col("flagged").mean().round(3).alias("still_flagged"))
        out["edge_decay"] = decay.sort(pl.col("checkpoint").replace_strict(order, default=99))
    if kind is GAME and "ladder" in official.columns:
        out["ladder"] = _metrics(official.with_columns(pl.col("ladder").fill_null("pre-ladder")), ["ladder"])
    return out
