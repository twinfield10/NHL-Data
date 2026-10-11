"""Lineup and goalie change ledger: what moved in each game between pregame runs, and what it did
to the price.

Every ``nhl pregame`` run snapshots its projected lineups, starter probabilities and prices
(:mod:`nhl.pregame.slate`). This module diffs consecutive snapshots of one game and writes one
row per change to ``pregame/changes/{date}.parquet`` (rebuilt from the snapshots, so a backfill
is the same call):

* ``kind``: ``in`` / ``out`` (a skater joined or left the dressed lineup; a game-time decision
  counts as dressed when ``p_dressed`` ≥ 0.5), ``line`` (a dressed skater's even-strength slot
  changed), ``pp`` (power-play unit changed), ``gtd`` (``p_dressed`` moved by
  :data:`MIN_P_MOVE`), ``starter`` (the likeliest starter changed), ``goalie_status``
  (DailyFaceoff's status for the likeliest starter changed, e.g. Likely → Confirmed),
  ``goalie_p`` (the likeliest starter's probability moved by :data:`MIN_P_MOVE`) and ``price``
  (the price moved by :data:`MIN_PRICE_MOVE` with no lineup or goalie change: new ratings,
  coaches, referees or the rest of the model's inputs).
* ``before`` / ``after``: the change in words (slot, unit, status, probability).
* **Value**: every row carries its *step's* price move (``dp_home_win``, ``d_home_goals``,
  ``d_away_goals``) and ``step_changes``, the number of changes in that step. The simulator's
  seed is fixed, so with unchanged inputs a rerun gives the same price: a step with one change
  is that change's value; a step with several shares one move between them. Per-change values
  inside a shared step would need counterfactual repricing (or player values) and are not
  attempted here.

Runs that priced a different set of games are fine: a game is diffed only between runs that
both priced it.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: Smallest move in a probability (starter, dressed) recorded as a change.
MIN_P_MOVE = 0.05
#: Smallest home-win move recorded as an unexplained ``price`` change.
MIN_PRICE_MOVE = 0.002
#: Order of kinds within one step, most important first.
KIND_ORDER = ["starter", "goalie_status", "goalie_p", "out", "in", "gtd", "line", "pp", "price"]

SCHEMA = {
    "game_id": pl.Int64, "game_date": pl.Date, "stamp": pl.String, "prev_stamp": pl.String,
    "as_of": pl.Datetime("us", "UTC"), "team_id": pl.Int64, "team": pl.String, "kind": pl.String,
    "player_id": pl.Int64, "player_name": pl.String, "before": pl.String, "after": pl.String,
    "p_home_win_before": pl.Float64, "p_home_win": pl.Float64, "dp_home_win": pl.Float64,
    "d_home_goals": pl.Float64, "d_away_goals": pl.Float64, "step_changes": pl.Int64,
}
PRICE_COLS = ["p_home_win", "mean_home_goals", "mean_away_goals"]


def _stamps(store: Store, day: date) -> list[str]:
    """Every pregame run stamp for ``day`` with a prices snapshot, oldest first."""
    prefix = f"pregame/prices/{day.isoformat()}/"
    return sorted(k.rsplit("/", 1)[-1].removesuffix(".parquet") for k in store.list_keys(prefix) if k.endswith(".parquet"))


def _slot_text(slot: str | None) -> str:
    return slot.upper() if slot else "extra"


def _pct(p: float | None) -> str:
    return "–" if p is None else f"{100 * p:.0f}%"


def lineup_changes(prev: pl.DataFrame, cur: pl.DataFrame) -> list[dict]:
    """Skater changes for one game between two lineup snapshots (``team_id, kind, player_id,
    before, after``)."""
    out: list[dict] = []
    cols = ["team_id", "player_id", "slot", "pp_unit", "p_dressed"]

    def frame(df: pl.DataFrame) -> dict[tuple[int, int], dict]:
        df = df.filter(pl.col("player_id").is_not_null())
        df = df.with_columns(pl.col("p_dressed").fill_null(1.0)) if "p_dressed" in df.columns else df.with_columns(p_dressed=pl.lit(1.0))
        return {(r["team_id"], r["player_id"]): r for r in df.select(cols).iter_rows(named=True)}

    a, b = frame(prev), frame(cur)
    for key in sorted(a.keys() | b.keys(), key=lambda k: (k[0], k[1])):
        team_id, pid = key
        ra, rb = a.get(key), b.get(key)
        da = ra is not None and ra["p_dressed"] >= 0.5
        db = rb is not None and rb["p_dressed"] >= 0.5
        row = {"team_id": team_id, "player_id": pid}
        if da and not db:
            out.append({**row, "kind": "out", "before": _slot_text(ra["slot"]),
                        "after": "not dressed" if rb is None else f"{_pct(rb['p_dressed'])} to dress"})
            continue
        if db and not da:
            out.append({**row, "kind": "in", "before": "not dressed" if ra is None else f"{_pct(ra['p_dressed'])} to dress",
                        "after": _slot_text(rb["slot"])})
            continue
        if not (da and db):
            continue
        if ra["slot"] != rb["slot"]:
            out.append({**row, "kind": "line", "before": _slot_text(ra["slot"]), "after": _slot_text(rb["slot"])})
        if (ra["pp_unit"] or 0) != (rb["pp_unit"] or 0):
            unit = lambda u: f"PP{u}" if u else "no PP"  # noqa: E731
            out.append({**row, "kind": "pp", "before": unit(ra["pp_unit"]), "after": unit(rb["pp_unit"])})
        if abs(ra["p_dressed"] - rb["p_dressed"]) >= MIN_P_MOVE:
            out.append({**row, "kind": "gtd", "before": f"{_pct(ra['p_dressed'])} to dress",
                        "after": f"{_pct(rb['p_dressed'])} to dress"})
    return out


def goalie_changes(prev: pl.DataFrame, cur: pl.DataFrame) -> list[dict]:
    """Starter changes for one game between two goalie snapshots."""
    out: list[dict] = []
    for team_id in sorted(set(prev["team_id"].to_list()) | set(cur["team_id"].to_list())):
        def top(df: pl.DataFrame) -> dict | None:
            t = df.filter(pl.col("team_id") == team_id).sort("p_start", descending=True)
            return t.row(0, named=True) if t.height else None

        def p_of(df: pl.DataFrame, pid: int | None) -> float | None:
            t = df.filter((pl.col("team_id") == team_id) & (pl.col("player_id") == pid))
            return t["p_start"][0] if t.height else None

        a, b = top(prev), top(cur)
        if a is None or b is None:
            continue
        row = {"team_id": int(team_id), "player_id": b["player_id"]}
        status = lambda r: r["dfo_status"] or "model"  # noqa: E731
        if a["player_id"] != b["player_id"]:
            out.append({**row, "kind": "starter", "before_id": a["player_id"],
                        "before": f"{_pct(a['p_start'])} ({status(a)})", "after": f"{_pct(b['p_start'])} ({status(b)})"})
            continue
        if (a["dfo_status"] or None) != (b["dfo_status"] or None):
            out.append({**row, "kind": "goalie_status", "before": status(a), "after": status(b)})
        elif (pa := p_of(prev, b["player_id"])) is not None and abs(pa - b["p_start"]) >= MIN_P_MOVE:
            out.append({**row, "kind": "goalie_p", "before": _pct(pa), "after": _pct(b["p_start"])})
    return out


def _read(store: Store, key: str) -> pl.DataFrame | None:
    try:
        return store.get_parquet(key)
    except Exception:  # noqa: BLE001 - one unreadable snapshot skips a step, not the ledger
        logger.exception("change ledger: unreadable snapshot %s", key)
        return None


def build(store: Store, day: date, game_ids: list[int] | None = None, stamps: list[str] | None = None) -> pl.DataFrame:
    """The change ledger for ``day`` (optionally only ``game_ids``, or only the steps between
    consecutive ``stamps``), oldest step first."""
    stamps = _stamps(store, day) if stamps is None else stamps
    teams = store.read_parquet_required(keys.TEAMS).select(pl.col("team_id").cast(pl.Int64), pl.col("team_abbr").alias("team"))
    players = store.read_parquet_required(keys.PLAYERS).select("player_id", "player_name")
    rows: list[dict] = []
    # Last snapshot seen per game: (stamp, lineups, goalies, price row).
    last: dict[int, tuple[str, pl.DataFrame, pl.DataFrame, dict]] = {}
    for st in stamps:
        prices = _read(store, keys.pregame_prices(day, st))
        lineups = _read(store, keys.pregame_lineups(day, st))
        goalies = _read(store, keys.pregame_goalies(day, st))
        if prices is None or lineups is None or goalies is None:
            continue
        for p in prices.drop("score_matrix", strict=False).iter_rows(named=True):
            gid = p["game_id"]
            if game_ids is not None and gid not in game_ids:
                continue
            lg = lineups.filter(pl.col("game_id") == gid)
            gg = goalies.filter(pl.col("game_id") == gid).with_columns(pl.col("team_id").cast(pl.Int64))
            if gid in last:
                prev_st, pl_prev, pg_prev, pp = last[gid]
                found = goalie_changes(pg_prev, gg) + lineup_changes(pl_prev, lg)
                dp = p["p_home_win"] - pp["p_home_win"]
                if not found and abs(dp) >= MIN_PRICE_MOVE:
                    found = [{"team_id": None, "player_id": None, "kind": "price", "before": None, "after": None}]
                step = {
                    "game_id": gid, "game_date": day, "stamp": st, "prev_stamp": prev_st, "as_of": p["as_of"],
                    "p_home_win_before": pp["p_home_win"], "p_home_win": p["p_home_win"], "dp_home_win": dp,
                    "d_home_goals": p["mean_home_goals"] - pp["mean_home_goals"],
                    "d_away_goals": p["mean_away_goals"] - pp["mean_away_goals"],
                    "step_changes": sum(c["kind"] != "price" for c in found),
                }
                rows += [{"before_id": None, **step, **c} for c in found]
            last[gid] = (st, lg, gg, p)
    if not rows:
        return pl.DataFrame(schema=SCHEMA)
    df = pl.DataFrame(rows, infer_schema_length=None)
    names = players.rename({"player_id": "before_id", "player_name": "before_name"})
    df = (
        df.with_columns(pl.col("team_id").cast(pl.Int64), pl.col("player_id").cast(pl.Int64), pl.col("before_id").cast(pl.Int64))
        .join(teams, on="team_id", how="left")
        .join(players, on="player_id", how="left")
        .join(names, on="before_id", how="left")
        # "Skinner 98% (Confirmed)" -> "Pickard 60% (model) → Skinner 98% (Confirmed)" for a new starter.
        .with_columns(pl.when(pl.col("kind") == "starter")
                      .then(pl.concat_str(pl.col("before_name").fill_null("Other"), pl.lit(" "), pl.col("before")))
                      .otherwise(pl.col("before")).alias("before"),
                      pl.when(pl.col("kind") == "starter")
                      .then(pl.concat_str(pl.col("player_name").fill_null("Other"), pl.lit(" "), pl.col("after")))
                      .otherwise(pl.col("after")).alias("after"))
        .with_columns(pl.col("kind").replace_strict({k: i for i, k in enumerate(KIND_ORDER)}, return_dtype=pl.Int32).alias("_k"))
        .sort("game_id", "stamp", "team_id", "_k", "player_name", nulls_last=True)
    )
    return df.select(list(SCHEMA)).cast(SCHEMA)


def write(store: Store, day: date) -> pl.DataFrame:
    """Rebuild and store ``day``'s ledger (``pregame/changes/{date}.parquet``) from every snapshot."""
    df = build(store, day)
    store.put_parquet(keys.pregame_changes(day), df)
    logger.info("change ledger %s: %d changes over %d games", day, df.height, df["game_id"].n_unique())
    return df


def update(store: Store, day: date, stamp: str) -> pl.DataFrame:
    """Add the step into run ``stamp`` to ``day``'s ledger (after each pregame run).

    Reads only that run and the one before it, so it stays cheap late in a day with dozens of
    runs. A game the previous run didn't price is picked up the next time two runs share it; a
    missing ledger is rebuilt in full.
    """
    existing = store.get_parquet(keys.pregame_changes(day))
    stamps = _stamps(store, day)
    if existing is None or stamp not in stamps:
        return write(store, day)
    i = stamps.index(stamp)
    step = build(store, day, stamps=stamps[max(0, i - 1):i + 1])
    df = pl.concat([existing.filter(pl.col("stamp") != stamp), step]).sort("game_id", "stamp", maintain_order=True)
    store.put_parquet(keys.pregame_changes(day), df)
    return df


def render(df: pl.DataFrame) -> str:
    """The ledger as text, one block per game and one line per change."""
    from zoneinfo import ZoneInfo

    et = ZoneInfo("America/New_York")
    lines: list[str] = []
    for (gid,), g in df.partition_by("game_id", as_dict=True, maintain_order=True).items():
        lines.append(f"== game {gid}")
        for (st,), s in g.partition_by("stamp", as_dict=True, maintain_order=True).items():
            r0 = s.row(0, named=True)
            when = r0["as_of"].astimezone(et).strftime("%m-%d %H:%M ET")
            lines.append(f"  {when}  home win {100 * r0['p_home_win_before']:.1f}% -> {100 * r0['p_home_win']:.1f}%"
                         f" ({100 * r0['dp_home_win']:+.1f})  goals {r0['d_away_goals']:+.2f} away / {r0['d_home_goals']:+.2f} home")
            for r in s.iter_rows(named=True):
                who = f"{r['team'] or ''} {r['player_name'] or ''}".strip()
                change = f"{r['before']} -> {r['after']}" if r["before"] or r["after"] else "(inputs other than lineups)"
                lines.append(f"      {r['kind']:13s} {who:28s} {change}")
    return "\n".join(lines) or "no changes"
