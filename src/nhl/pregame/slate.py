"""Pregame slate summary and data freshness (M5).

**Data contract (for the site).** Every ``nhl pregame`` run has one ``stamp`` (UTC, e.g.
``20261006T185854Z``) and writes, for its game date:

* ``pregame/slate/{date}/{stamp}.parquet``: **one row per game** (grain: date → ``game_id``):
  teams, start time, model prices, market fair prices, likely starters, lineup flags;
* ``pregame/lineups|goalies|prices/{date}/{stamp}.parquet``: the detail behind each row,
  joined on ``game_id`` (and ``team_id``);
* ``pregame/freshness/{date}/{stamp}.parquet``: how old each input was;
* ``pregame/latest/{date}.json``: ``{"stamp": ...}``, the newest run for the date (the only
  file ever overwritten; every snapshot stays).

A page for a date reads ``latest`` → the slate; a game page filters the same stamp's detail
files by ``game_id``. History (how a game's price moved through the day) is every stamp for
the date.

**Freshness.** No scheduler runs the pollers yet, so a pregame run can silently use stale
inputs. :func:`freshness` reports each source's last capture, its age and whether it exceeds
its limit (:data:`MAX_AGE_HOURS`); stale sources are logged and listed on every slate row.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl

from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)
EASTERN = ZoneInfo("America/New_York")

#: Maximum age (hours) before a source counts as stale for a pregame run.
MAX_AGE_HOURS = {
    "dailyfaceoff_goalies": 6.0,
    "dailyfaceoff_lines": 24.0,
    "espn_injuries": 12.0,
    "transactions": 24.0,
    "ref_assignments": 24.0,
    "odds": 6.0,
    "ratings": 36.0,     # nightly `nhl update` writes a snapshot dated today (games before today)
    "game_state": 36.0,  # last final game present in player logs
}


def _latest_capture(store: Store, key: str, column: str = "captured_at") -> datetime | None:
    frame = store.get_parquet(key)
    if frame is None or frame.is_empty() or column not in frame.columns:
        return None
    return frame[column].max()


def freshness(store: Store, season: int, as_of: datetime) -> pl.DataFrame:
    """``source, last_update, age_hours, max_age_hours, stale, note`` for every pregame input."""
    rows: list[dict] = []

    def add(source: str, last: datetime | None, note: str = "") -> None:
        if last is not None and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        age = (as_of - last).total_seconds() / 3600 if last is not None else None
        limit = MAX_AGE_HOURS[source]
        rows.append({"source": source, "last_update": last, "age_hours": age, "max_age_hours": limit,
                     "stale": age is None or age > limit, "note": note or ("never captured" if last is None else "")})

    add("dailyfaceoff_goalies", _latest_capture(store, keys.dailyfaceoff_goalies(season)))
    add("dailyfaceoff_lines", _latest_capture(store, keys.dailyfaceoff_lines(season)))
    add("espn_injuries", _latest_capture(store, keys.injuries(season)))
    add("transactions", _latest_capture(store, keys.transactions(season)))
    add("ref_assignments", _latest_capture(store, keys.ref_assignments(season)))
    odds = [k for k in store.list_keys(keys.odds_live_prefix(season)) if k.endswith(".parquet")]
    lasts = [t for k in odds if (t := _latest_capture(store, k)) is not None]
    add("odds", max(lasts) if lasts else None, f"{len(odds)} books files")

    snaps = sorted({k.split("/")[1] for k in store.list_keys("ratings/") if k.count("/") == 2 and k[8:9].isdigit()})
    snap = date.fromisoformat(snaps[-1]) if snaps else None
    # A snapshot dated D uses games before D and is written early on D by the nightly job.
    add("ratings", datetime.combine(snap, datetime.min.time(), timezone.utc) if snap else None, f"snapshot {snap}")

    games = store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final"))
    logs = store.get_parquet(keys.player_game_logs(season))
    done = logs["game_date"].max() if logs is not None and logs.height else None
    missing = games.filter(pl.col("game_date") > done).height if done is not None else games.height
    last_final = games["game_date"].max() if games.height else None
    state_time = datetime.combine(done + timedelta(days=1), datetime.min.time(), timezone.utc) if done else None
    add("game_state", state_time, f"logs through {done}; {missing} final games not built (last final {last_final})")
    out = pl.DataFrame(rows, schema={"source": pl.String, "last_update": pl.Datetime("us", "UTC"), "age_hours": pl.Float64,
                                     "max_age_hours": pl.Float64, "stale": pl.Boolean, "note": pl.String})
    for r in out.filter(pl.col("stale")).iter_rows(named=True):
        age = "never" if r["age_hours"] is None else f"{r['age_hours']:.1f} h old"
        logger.warning("pregame input stale: %s (%s, limit %.0f h) %s", r["source"], age, r["max_age_hours"], r["note"])
    return out


def market_fair(store: Store, season: int, game_ids: list[int]) -> pl.DataFrame:
    """Latest devigged market consensus per game from our live captures:
    ``game_id, mkt_p_home_win, mkt_total_line, mkt_p_over, mkt_books, mkt_captured_at``."""
    from nhl.betting import devig, lines

    try:
        live = lines.clean(lines.live_lines(store, season)).filter(pl.col("game_id").is_in(game_ids))
    except Exception:  # noqa: BLE001 - the market is context for the slate, never a blocker
        logger.exception("market prices unavailable")
        live = pl.DataFrame()
    schema = {"game_id": pl.Int64, "mkt_p_home_win": pl.Float64, "mkt_total_line": pl.Float64, "mkt_p_over": pl.Float64,
              "mkt_books": pl.UInt32, "mkt_captured_at": pl.Datetime("us", "UTC")}
    if live.is_empty():
        return pl.DataFrame(schema=schema)
    ml = devig.consensus(live.filter(pl.col("market") == "moneyline"), "power", ("last",)).select(
        "game_id", pl.col("p_fair").alias("mkt_p_home_win"), pl.col("books").alias("mkt_books"))
    tot = devig.consensus(live.filter(pl.col("market") == "total"), "shin", ("last",)).select(
        "game_id", pl.col("line").alias("mkt_total_line"), pl.col("p_fair").alias("mkt_p_over"))
    when = live.filter(pl.col("point") == "last").group_by("game_id").agg(pl.col("captured_at").max().alias("mkt_captured_at"))
    return ml.join(tot, on="game_id", how="full", coalesce=True).join(when, on="game_id", how="left").select(list(schema)).cast(schema)


def build(store: Store, season: int, games: pl.DataFrame, dep: pl.DataFrame, probs: pl.DataFrame,
          prices: pl.DataFrame, fresh: pl.DataFrame, as_of: datetime, stamp: str) -> pl.DataFrame:
    """One row per game (see the module docstring)."""
    teams = store.read_parquet_required(keys.TEAMS).select(pl.col("team_id").cast(pl.Int64), pl.col("team_abbr"))
    players = store.read_parquet_required(keys.PLAYERS).select("player_id", "player_name")
    top = (
        probs.join(players, on="player_id", how="left")
        .with_columns(pl.col("player_name").fill_null("other"))
        .sort("p_start", descending=True)
        .group_by("game_id", "team_id", maintain_order=True)
        .agg(pl.col("player_id").first().alias("starter_id"), pl.col("player_name").first().alias("starter"),
             pl.col("p_start").first().alias("starter_p"), pl.col("dfo_status").first().alias("starter_dfo"),
             pl.col("player_name").slice(1, 1).first().alias("alt_starter"),
             pl.col("p_start").slice(1, 1).first().alias("alt_starter_p"))
    )
    flags = dep.group_by("game_id", "team_id").agg(
        (pl.col("source") == "dfo").mean().alias("dfo_share"),
        (pl.col("source") == "fill").sum().alias("fills"),
        (pl.col("source") == "return").sum().alias("returns"),
        (pl.col("source") == "gtd_backup").sum().alias("game_time_decisions"),
        pl.col("issues").filter(pl.col("issues").is_not_null() & (pl.col("issues") != "")).unique().str.join(" | ").alias("lineup_issues"),
    )
    side_cols = ["starter_id", "starter", "starter_p", "starter_dfo", "alt_starter", "alt_starter_p",
                 "dfo_share", "fills", "returns", "game_time_decisions", "lineup_issues", "team_abbr"]
    out = games.select(
        "game_id", "game_date", pl.col("start_utc").alias("start_time"),
        pl.col("home_team_id").cast(pl.Int64), pl.col("away_team_id").cast(pl.Int64),
    ).join(prices.drop("game_date", "home_team_id", "away_team_id", "as_of", "stamp", "score_matrix", strict=False),
           on="game_id", how="left")
    for side in ("home", "away"):
        per_team = top.join(flags, on=["game_id", "team_id"], how="full", coalesce=True).join(teams, on="team_id", how="left")
        out = out.join(
            per_team.select("game_id", pl.col("team_id").alias(f"{side}_team_id"), *[pl.col(c).alias(f"{side}_{c}") for c in side_cols]),
            on=["game_id", f"{side}_team_id"], how="left",
        )
    out = out.join(market_fair(store, season, out["game_id"].to_list()), on="game_id", how="left")
    stale = ", ".join(fresh.filter(pl.col("stale"))["source"].to_list())
    return out.with_columns(
        (pl.col("p_home_win") - pl.col("mkt_p_home_win")).alias("edge_home_win"),
        pl.lit(stale or None, dtype=pl.String).alias("stale_inputs"),
        pl.lit(as_of).alias("as_of"), pl.lit(stamp).alias("stamp"),
    ).sort("start_time", "game_id")


def write(store: Store, day: date, stamp: str, slate_rows: pl.DataFrame, fresh: pl.DataFrame) -> None:
    """Slate and freshness snapshots, then the ``latest`` pointer."""
    store.put_parquet(keys.pregame_slate(day, stamp), slate_rows)
    store.put_parquet(keys.pregame_freshness(day, stamp), fresh.with_columns(pl.lit(stamp).alias("stamp")))
    store.put_bytes(keys.pregame_latest(day), json.dumps({"stamp": stamp, "games": slate_rows.height}).encode(),
                    content_type="application/json")


def render(slate_rows: pl.DataFrame, fresh: pl.DataFrame) -> str:
    """A plain-text view of the slate for the terminal."""
    def pct(v: float | None) -> str:
        return "  -  " if v is None else f"{100 * v:4.1f}%"

    lines = []
    stale = fresh.filter(pl.col("stale"))
    if stale.height:
        ages = [(r["source"], "never" if r["age_hours"] is None else f"{r['age_hours']:.0f} h") for r in stale.iter_rows(named=True)]
        lines.append("STALE INPUTS: " + "; ".join(f"{source} ({age})" for source, age in ages))
        lines.append("")
    for r in slate_rows.iter_rows(named=True):
        start = r["start_time"].astimezone(EASTERN).strftime("%H:%M") if r["start_time"] else "?"
        edge = "" if r["edge_home_win"] is None else f" ({100 * r['edge_home_win']:+.1f})"
        lines.append(f"{start} ET  {r['away_team_abbr']} @ {r['home_team_abbr']}  [{r['game_id']}]")
        lines.append(f"    home win  model {pct(r['p_home_win'])}  market {pct(r['mkt_p_home_win'])}{edge}"
                     f"   | over {r['mkt_total_line'] or 5.5}: market {pct(r['mkt_p_over'])}"
                     f"   | model goals {r['mean_home_goals']:.2f}-{r['mean_away_goals']:.2f}" if r["p_home_win"] is not None else "    not priced")
        for side in ("away", "home"):
            g = f"{r[f'{side}_starter']} {pct(r[f'{side}_starter_p'])}" + (f" ({r[f'{side}_starter_dfo']})" if r[f"{side}_starter_dfo"] else "")
            if r[f"{side}_alt_starter"]:
                g += f" / {r[f'{side}_alt_starter']} {pct(r[f'{side}_alt_starter_p'])}"
            flags = [f"{r[f'{side}_{k}']} {k.replace('_', ' ')}" for k in ("fills", "returns", "game_time_decisions") if r[f"{side}_{k}"]]
            dfo = r[f"{side}_dfo_share"]
            lines.append(f"    {r[f'{side}_team_abbr']}: G {g} | lines {'DFO' if dfo and dfo > 0.5 else 'last game'}"
                         + (f" | {', '.join(flags)}" if flags else ""))
        lines.append("")
    return "\n".join(lines)
