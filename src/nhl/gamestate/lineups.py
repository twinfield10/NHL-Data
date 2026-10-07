"""Actual lines, D pairs and special-teams units per team-game, inferred from stints.

* **Forward lines:** from the pairwise shared 5v5 TOI matrix among a team's forwards,
  repeatedly take the trio with the highest *minimum* pairwise shared time, up to 4.
* **D pairs:** the same with pairs, up to 3.
* **PP / PK units:** the exact skater sets with the most power-play (penalty-kill) time.
  The second unit is the best set sharing at most two players with the first, so a
  one-player substitution on PP1 isn't reported as PP2.

Shapes are reported, never forced: ``lineup_shape`` is the dressed count (``12F6D``), and
``shape_regular`` is true for 12F/6D and 11F/7D. ``confidence`` is the unit's time
together ÷ the smallest member's TOI in that state: 1.0 means the members only ever
played as this unit.
"""

from __future__ import annotations

from itertools import combinations

import numpy as np
import polars as pl

REGULAR_SHAPES = {(12, 6), (11, 7)}
MAX_LINES, MAX_PAIRS = 4, 3

_SCHEMA = {
    "game_id": pl.Int64,
    "team_id": pl.Int32,
    "unit_type": pl.Utf8,
    "unit_rank": pl.Int8,
    "members": pl.List(pl.Int64),
    "shared_toi_s": pl.Int32,
    "unit_toi_share": pl.Float64,
    "confidence": pl.Float64,
}


def team_stints(stints: pl.DataFrame) -> pl.DataFrame:
    """Each stint twice, once from each team's point of view.

    Returns:
        ``game_id, team_id, skaters, own_n, opp_n, goalies_in, duration_s, state`` where
        ``state`` is ``5v5`` / ``PP`` / ``PK`` / ``other`` for that team.
    """
    sides = []
    for own, opp in (("home", "away"), ("away", "home")):
        sides.append(
            stints.select(
                "game_id",
                pl.col(f"{own}_team_id").alias("team_id"),
                pl.col(f"{own}_skaters").alias("skaters"),
                pl.col(f"{own}_n").alias("own_n"),
                pl.col(f"{opp}_n").alias("opp_n"),
                (pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()).alias("goalies_in"),
                "duration_s",
            )
        )
    own, opp, gi = pl.col("own_n"), pl.col("opp_n"), pl.col("goalies_in")
    return pl.concat(sides).with_columns(
        pl.when(gi & (own == 5) & (opp == 5)).then(pl.lit("5v5"))
        .when(gi & (own > opp)).then(pl.lit("PP"))
        .when(gi & (own < opp)).then(pl.lit("PK"))
        .otherwise(pl.lit("other"))
        .alias("state")
    )


def _incidence(rows: pl.DataFrame) -> tuple[list[int], np.ndarray, np.ndarray]:
    """Players, a players x stints 0/1 matrix, and stint durations."""
    players = sorted({p for s in rows["skaters"].to_list() for p in s})
    index = {p: i for i, p in enumerate(players)}
    m = np.zeros((len(players), rows.height), dtype=np.float64)
    for j, skaters in enumerate(rows["skaters"].to_list()):
        for p in skaters:
            m[index[p], j] = 1.0
    return players, m, rows["duration_s"].to_numpy().astype(np.float64)


def _greedy_units(
    candidates: list[int], players: list[int], m: np.ndarray, d: np.ndarray, size: int, limit: int
) -> list[tuple[list[int], float, float]]:
    """Greedy units of ``size`` by max-min pairwise shared time.

    Returns:
        ``(members, shared_toi, confidence)`` per unit, best first.
    """
    index = {p: i for i, p in enumerate(players)}
    pool = [p for p in candidates if p in index]
    pair = (m * d) @ m.T
    units = []
    while len(units) < limit and len(pool) >= size:
        best, best_score = None, 0.0
        for combo in combinations(pool, size):
            idx = [index[p] for p in combo]
            score = min(pair[a, b] for a, b in combinations(idx, 2))
            if score > best_score:
                best, best_score = combo, score
        if best is None:
            break
        idx = [index[p] for p in best]
        together = float(np.prod(m[idx], axis=0) @ d)
        solo = float(min(pair[i, i] for i in idx))
        units.append((sorted(best), together, together / solo if solo else 0.0))
        pool = [p for p in pool if p not in best]
    return units


def _special_units(rows: pl.DataFrame) -> list[tuple[list[int], float, float]]:
    """Top two exact skater sets by time, the second sharing at most two players with the first."""
    if rows.is_empty():
        return []
    sets = (
        rows.with_columns(pl.col("skaters").list.sort())
        .group_by("skaters")
        .agg(pl.col("duration_s").sum())
        .filter(pl.col("skaters").list.len() >= 3)
        .sort("duration_s", descending=True)
    )
    if sets.is_empty():
        return []
    players, m, d = _incidence(rows)
    toi = dict(zip(players, (m @ d).tolist()))
    chosen: list[list[int]] = []
    for members in sets["skaters"].to_list():
        if not chosen or len(set(members) & set(chosen[0])) <= 2:
            chosen.append(members)
        if len(chosen) == 2:
            break
    out = []
    times = dict(zip(map(tuple, sets["skaters"].to_list()), sets["duration_s"].to_list()))
    for members in chosen:
        shared = float(times[tuple(members)])
        solo = min(toi[p] for p in members)
        out.append((members, shared, shared / solo if solo else 0.0))
    return out


def _team_game_units(group: pl.DataFrame, forwards: list[int], defense: list[int]) -> list[dict]:
    """All units for one team-game."""
    game_id, team_id = group["game_id"][0], group["team_id"][0]
    rows: list[dict] = []

    def emit(unit_type: str, units: list[tuple[list[int], float, float]], state_total: float) -> None:
        for rank, (members, shared, conf) in enumerate(units, start=1):
            rows.append(
                {
                    "game_id": game_id, "team_id": team_id, "unit_type": unit_type, "unit_rank": rank,
                    "members": members, "shared_toi_s": int(round(shared)),
                    "unit_toi_share": shared / state_total if state_total else None, "confidence": conf,
                }
            )

    ev = group.filter(pl.col("state") == "5v5")
    if not ev.is_empty():
        players, m, d = _incidence(ev)
        emit("F", _greedy_units(forwards, players, m, d, 3, MAX_LINES), float(d.sum()))
        emit("D", _greedy_units(defense, players, m, d, 2, MAX_PAIRS), float(d.sum()))
    for state in ("PP", "PK"):
        part = group.filter(pl.col("state") == state)
        emit(state, _special_units(part), float(part["duration_s"].sum()))
    return rows


def build_lineups(stints: pl.DataFrame, rosters: pl.DataFrame) -> pl.DataFrame:
    """Infer every team-game's lines, pairs and PP/PK units.

    Args:
        stints: Output of :func:`nhl.gamestate.stints.build_stints`.
        rosters: Output of :func:`nhl.gamestate.rosters.season_rosters`.

    Returns:
        One row per unit (see module docstring), with the team-game's ``lineup_shape``,
        ``n_forwards``, ``n_defense`` and ``shape_regular`` on every row.
    """
    if stints.is_empty():
        return pl.DataFrame(schema=_SCHEMA)
    positions = {
        (g, t): (fw, d)
        for g, t, fw, d in rosters.group_by("game_id", "team_id")
        .agg(
            pl.col("player_id").filter(pl.col("is_forward")).alias("fw"),
            pl.col("player_id").filter(pl.col("is_defense")).alias("d"),
        )
        .iter_rows()
    }
    rows: list[dict] = []
    for group in team_stints(stints).partition_by("game_id", "team_id", maintain_order=False):
        fw, d = positions.get((group["game_id"][0], group["team_id"][0]), ([], []))
        rows.extend(_team_game_units(group, fw, d))
    shape = (
        rosters.group_by("game_id", "team_id")
        .agg(
            pl.col("is_forward").sum().cast(pl.Int8).alias("n_forwards"),
            pl.col("is_defense").sum().cast(pl.Int8).alias("n_defense"),
        )
        .with_columns(
            pl.concat_str(pl.col("n_forwards"), pl.lit("F"), pl.col("n_defense"), pl.lit("D")).alias("lineup_shape"),
            pl.any_horizontal(
                *[(pl.col("n_forwards") == f) & (pl.col("n_defense") == d) for f, d in REGULAR_SHAPES]
            ).alias("shape_regular"),
        )
    )
    units = pl.DataFrame(rows, schema=_SCHEMA)
    return units.join(shape, on=["game_id", "team_id"], how="left").sort("game_id", "team_id", "unit_type", "unit_rank")
