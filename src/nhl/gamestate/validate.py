"""Validation of the M2 tables against official sources (``nhl validate-game-state``).

Full-season checks (no network):

* **Personnel:** share of all ice time with a plausible on-ice count (``valid_personnel``:
  3-6 skaters each, at most 5 with a goalie in net), and, for information, the share of
  games where at least 99% of time is plausible.
* **Goals:** stint goals equal the official final score, minus the shootout winner.
* **xG:** stint xG sums to the predictions table.
* **Faceoff context:** ``last_faceoff_s <= start_s`` always.

Sampled checks against NHL boxscores (``/gamecenter/{id}/boxscore``, cached under
``raw/boxscore/``):

* player TOI within 30 s, goals and assists exact;
* the starting goalie matches the boxscore ``starter`` flag.
"""

from __future__ import annotations

import logging
import random
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from nhl import config
from nhl.ingest.http import WEB_API, NHLClient
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

TOI_TOLERANCE_S = 30


def boxscore_key(season: int, game_id: int) -> str:
    """Cache key for one game's boxscore."""
    return f"raw/boxscore/{season}/{game_id}.json.gz"


def _boxscore(store: Store, client: NHLClient, season: int, game_id: int) -> dict[str, Any]:
    cached = store.get_json_gz(boxscore_key(season, game_id))
    if cached is not None:
        return cached
    payload = client.get_json(f"{WEB_API}/gamecenter/{game_id}/boxscore")
    store.put_json_gz(boxscore_key(season, game_id), payload)
    return payload


def boxscore_players(payload: dict[str, Any], game_id: int) -> pl.DataFrame:
    """Player rows from a boxscore: ``game_id, player_id, toi_s, goals, assists, starter``."""
    rows = []
    for side in ("homeTeam", "awayTeam"):
        groups = (payload.get("playerByGameStats") or {}).get(side) or {}
        for group in ("forwards", "defense", "goalies"):
            for p in groups.get(group) or []:
                m, s = (p.get("toi") or "0:00").split(":")
                rows.append({
                    "game_id": game_id, "player_id": p["playerId"], "is_goalie": group == "goalies",
                    "box_toi_s": int(m) * 60 + int(s), "box_goals": p.get("goals"), "box_assists": p.get("assists"),
                    "box_starter": p.get("starter"),
                })
    return pl.DataFrame(rows, schema={
        "game_id": pl.Int64, "player_id": pl.Int64, "is_goalie": pl.Boolean, "box_toi_s": pl.Int32,
        "box_goals": pl.Int32, "box_assists": pl.Int32, "box_starter": pl.Boolean,
    })


def season_checks(store: Store, season: int) -> dict[str, float]:
    """Full-season checks that need no network."""
    stints = store.read_parquet_required(keys.stints(season))
    games = store.read_parquet_required(keys.GAMES).filter((pl.col("season") == season) & pl.col("is_final"))
    xg = store.get_parquet(keys.xg_predictions(season))

    ok = pl.col("valid_personnel")
    per_game = stints.group_by("game_id").agg(
        (pl.col("duration_s").filter(ok).sum() / pl.col("duration_s").sum()).alias("valid_share"),
        (pl.col("home_gf") + pl.col("away_gf")).sum().alias("goals"),
        (pl.col("home_xgf") + pl.col("away_xgf")).sum().alias("xg"),
    )
    official = games.select(
        "game_id",
        # Regular-season games ending in period 5 went to a shootout (the winner gets +1);
        # playoff games ending in period 5 are double overtime.
        (
            pl.col("home_score") + pl.col("away_score")
            - ((pl.col("season_type") == "R") & (pl.col("last_period") == 5)).cast(pl.Int16)
        ).alias("official"),
    )
    goals = per_game.join(official, on="game_id", how="inner")
    out = {
        "games_final": float(games.height),
        "games_with_stints": float(per_game.height),
        "personnel_ok_99": float((per_game["valid_share"] >= 0.99).mean()),
        "valid_time_share": float(stints.filter(ok)["duration_s"].sum() / stints["duration_s"].sum()),
        "goals_match": float((goals["goals"] == goals["official"]).mean()),
        "faceoff_order_ok": float((stints["last_faceoff_s"] <= stints["start_s"]).fill_null(True).mean()),
    }
    if xg is not None:
        xg_game = xg.group_by("game_id").agg(pl.col("xg").sum().alias("pred"))
        joined = per_game.join(xg_game, on="game_id", how="inner")
        out["xg_match"] = float(((joined["xg"] - joined["pred"]).abs() < 1e-3).mean())
    return out


def sample_checks(store: Store, client: NHLClient, season: int, n_games: int, seed: int = 7) -> dict[str, float]:
    """Boxscore comparisons on a random sample of the season's games."""
    logs = store.read_parquet_required(keys.player_game_logs(season)).filter(pl.col("strength") == "all")
    starts = store.read_parquet_required(keys.goalie_starts(season))
    game_ids = sorted(logs["game_id"].unique().to_list())
    sample = random.Random(seed + season).sample(game_ids, min(n_games, len(game_ids)))
    box = pl.concat([boxscore_players(_boxscore(store, client, season, g), g) for g in sample])

    skaters = box.filter(~pl.col("is_goalie") & (pl.col("box_toi_s") > 0)).join(
        logs.select("game_id", "player_id", "toi_s", "goals", (pl.col("a1") + pl.col("a2")).alias("assists")),
        on=["game_id", "player_id"], how="left",
    )
    starters = box.filter(pl.col("box_starter").fill_null(False)).select("game_id", pl.col("player_id").alias("box_starter_id"))
    ours = starts.filter(pl.col("game_id").is_in(sample)).select("game_id", "starter")
    starter_cmp = starters.join(ours, left_on=["game_id", "box_starter_id"], right_on=["game_id", "starter"], how="left", coalesce=False)
    return {
        "sample_games": float(len(sample)),
        "player_games": float(skaters.height),
        "toi_within_30s": float(((skaters["toi_s"] - skaters["box_toi_s"]).abs() <= TOI_TOLERANCE_S).fill_null(False).mean()),
        "goals_exact": float((skaters["goals"] == skaters["box_goals"]).fill_null(False).mean()),
        "assists_exact": float((skaters["assists"] == skaters["box_assists"]).fill_null(False).mean()),
        "starter_match": float(starter_cmp["starter"].is_not_null().mean()) if starter_cmp.height else float("nan"),
    }


BARS = {
    "valid_time_share": 0.995,
    "goals_match": 0.995,
    "xg_match": 1.0,
    "faceoff_order_ok": 1.0,
    "toi_within_30s": 0.98,
    "goals_exact": 1.0,
    "assists_exact": 1.0,
    "starter_match": 1.0,
}


def write_report(results: dict[int, dict[str, float]], path: Path) -> str:
    """Render the validation results as Markdown and write them to ``path``."""
    cols = list(BARS)
    lines = [
        "# M2 validation report",
        "",
        f"Generated {date.today().isoformat()} by `nhl validate-game-state`. Bars from "
        "[the M2 plan](../plans/m2-game-state.md); a cell is marked ✗ below its bar.",
        "",
        "| Season | Games | games ≥99% valid | " + " | ".join(cols) + " |",
        "|---|---|---|" + "---|" * len(cols),
    ]
    for season, r in sorted(results.items()):
        cells = []
        for c in cols:
            v = r.get(c)
            cells.append("–" if v is None else f"{v:.2%}{'' if v >= BARS[c] else ' ✗'}")
        lines.append(
            f"| {season} | {int(r.get('games_with_stints', 0))} | {r['personnel_ok_99']:.2%} | " + " | ".join(cells) + " |"
        )
    df_rows = {s: r for s, r in results.items() if "dailyfaceoff_team_games" in r}
    lines += ["", "## Inferred units vs DailyFaceoff", ""]
    if df_rows:
        lines += [
            "Last DailyFaceoff version published before puck drop. Bar: forward trios ≥ 85% exact.",
            "",
            "| Season | Team-games | F trios | D pairs | PP units | PK units |",
            "|---|---|---|---|---|---|",
        ]
        for season, r in sorted(df_rows.items()):
            cells = [f"{r[f'dailyfaceoff_{u}_exact']:.1%}" for u in ("f", "d", "pp", "pk")]
            lines.append(f"| {season} | {int(r['dailyfaceoff_team_games'])} | " + " | ".join(cells) + " |")
        lines += [
            "",
            "PK units are a weak test: a team kills only a few penalties a game, and its PK forward and "
            "D pairs rotate independently, so exact four-man sets from one game rarely repeat. Treat "
            "fewer than ~100 team-games as anecdotal.",
        ]
    else:
        lines.append("Pending: DailyFaceoff capture started 2026-10-05, and no game played since then has been ingested yet.")
    lines += [
        "",
        "Bars: " + ", ".join(f"`{c}` ≥ {b:.1%}" for c, b in BARS.items()) + ".",
        "",
        "`valid_time_share` is the share of ice time with a plausible on-ice count; the rest are "
        "NHL shift-chart errors (a late exit briefly puts 6-8 skaters on), flagged per stint by "
        "`valid_personnel` for M3 to drop. *games ≥99% valid* is informational: the plan's original "
        "bar (99.5% of games) is stricter than the source allows in 2019-21, when the charts were "
        "worst. Boxscore columns use a random sample of games per season.",
        "",
    ]
    text = "\n".join(lines)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return text


def validate(store: Store, start_years: list[int], sample_games: int = 20, client: NHLClient | None = None) -> dict[int, dict[str, float]]:
    """Run every check for the given seasons."""
    client = client or NHLClient()
    results = {}
    for year in start_years:
        season = config.season_id(year)
        r = season_checks(store, season)
        if sample_games:
            r.update(sample_checks(store, client, season, sample_games))
        df = compare_dailyfaceoff(store, season)
        if df is not None:
            r.update({f"dailyfaceoff_{k}": v for k, v in df.items()})
        logger.info("%s: %s", season, {k: round(v, 4) for k, v in r.items()})
        results[season] = r
    return results


#: DailyFaceoff group -> our lineup unit.
_DF_UNITS = {**{f"f{i}": ("F", i) for i in range(1, 5)}, **{f"d{i}": ("D", i) for i in range(1, 4)},
             "pp1": ("PP", 1), "pp2": ("PP", 2), "pk1": ("PK", 1), "pk2": ("PK", 2)}


def compare_dailyfaceoff(store: Store, season: int) -> dict[str, float] | None:
    """Agreement of inferred units with DailyFaceoff's last published lines before puck drop.

    A DailyFaceoff unit counts as recovered when the same set of players appears as *any*
    inferred unit of that type for the team-game (rank is not required to match).

    Returns:
        ``team_games``, and per unit type the exact-match share (``f_exact``, ``d_exact``,
        ``pp_exact``, ``pk_exact``); None when no game has a prior DailyFaceoff capture.
    """
    lines = store.get_parquet(keys.dailyfaceoff_lines(season))
    lineups = store.get_parquet(keys.lineups(season))
    if lines is None or lineups is None:
        return None
    teams = store.read_parquet_required(keys.TEAMS).select(pl.col("team_id").cast(pl.Int32), pl.col("team_abbr").alias("team"))
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("season") == season).select(
        "game_id",
        pl.col("start_time_et").str.to_datetime().dt.replace_time_zone("America/New_York").dt.convert_time_zone("UTC").alias("puck_drop"),
    )
    played = lineups.select("game_id", "team_id").unique().join(games, on="game_id").join(teams, on="team_id")
    versions = lines.filter(pl.col("group").is_in(list(_DF_UNITS))).select("team", "updated_at").unique().sort("updated_at")
    chosen = (
        played.sort("puck_drop")
        .join_asof(versions, left_on="puck_drop", right_on="updated_at", by="team", strategy="backward")
        .filter(pl.col("updated_at").is_not_null())
    )
    if chosen.is_empty():
        return None
    df_units = (
        lines.join(chosen.select("game_id", "team_id", "team", "updated_at"), on=["team", "updated_at"])
        .filter(pl.col("group").is_in(list(_DF_UNITS)) & pl.col("player_id").is_not_null())
        .group_by("game_id", "team_id", "group")
        .agg(pl.col("player_id").sort().alias("members"))
        .with_columns(pl.col("group").replace_strict({k: v[0] for k, v in _DF_UNITS.items()}).alias("unit_type"))
    )
    ours = lineups.select("game_id", "team_id", "unit_type", pl.col("members").list.sort(), pl.lit(True).alias("found"))
    matched = df_units.join(ours, on=["game_id", "team_id", "unit_type", "members"], how="left").with_columns(
        pl.col("found").fill_null(False)
    )
    out = {"team_games": float(chosen.height)}
    for unit in ("F", "D", "PP", "PK"):
        part = matched.filter(pl.col("unit_type") == unit)
        out[f"{unit.lower()}_exact"] = float(part["found"].mean()) if part.height else float("nan")
    return out
