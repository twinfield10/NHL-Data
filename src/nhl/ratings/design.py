"""Design matrices for the stint regressions (M3 phases C and D, Magnus 9 style).

Every stint becomes **one row per attacking team**: the response is that team's xG per 60,
the weight is the stint's duration, and the columns are:

* ``O:{player}`` for each attacking skater and ``D:{player}`` for each defending skater;
* ``zone:{type}:{s}``: the share of the stint spent ``s`` seconds (0-34) after the last
  faceoff, by start type from the attacking team's view: ``oz`` / ``nz`` / ``dz`` while the
  attackers on the ice are those who took the faceoff, ``otf`` once they have changed.
  Beyond 34 s is the reference;
* ``score:{lead}:p{period}`` (lead −3…+3 for the attackers, periods 1-3; tied in the
  1st is the reference) and ``home`` (attackers at home);
* rest: ``rest:att_b2b``, ``rest:att_rested``, ``rest:def_b2b``, ``rest:def_rested``
  (normal = 2 days is the reference);
* ``coach:O:{coach}`` (attacking coach) and ``coach:D:{coach}`` (defending coach);
* ``post_penalty``.

EV uses valid 5v5 stints. ST uses valid stints where the attackers have more skaters and
both goalies are in (5v4, 5v3, 4v3); only the power-play side attacks.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl
from scipy import sparse

ZONE_SECONDS = 35
ZONE_TYPES = ("oz", "nz", "dz", "otf")
LEADS = tuple(range(-3, 4))
_FLIP = {"O": "dz", "D": "oz", "N": "nz"}
_SAME = {"O": "oz", "D": "dz", "N": "nz"}


@dataclass
class Design:
    """Sparse design, response and weights, with column names and groups."""

    x: sparse.csr_matrix
    y: np.ndarray
    w: np.ndarray
    columns: list[str]
    rows: pl.DataFrame  # game_id, stint_id, attack (home/away), team ids, offence/defence lists

    def group(self, prefix: str) -> np.ndarray:
        """Indices of columns whose name starts with ``prefix``."""
        return np.array([i for i, c in enumerate(self.columns) if c.startswith(prefix)], dtype=int)


def _rest_category(days: pl.Expr) -> pl.Expr:
    return pl.when(days == 1).then(pl.lit("b2b")).when(days == 2).then(pl.lit("normal")).otherwise(pl.lit("rested"))


def attack_rows(
    stints: pl.DataFrame, rest: pl.DataFrame, coaches: pl.DataFrame, state: str = "EV"
) -> pl.DataFrame:
    """One row per stint and attacking team, with every attribute the columns need.

    Args:
        stints: ``processed/stints`` for one or more seasons.
        rest: ``schedule_context`` rows (``game_id, is_home, days_rest``).
        coaches: ``processed/coaches`` rows (``game_id, is_home, coach_id``).
        state: ``EV`` (5v5) or ``ST`` (power play attacking).
    """
    rest = rest.select("game_id", "is_home", _rest_category(pl.col("days_rest")).alias("rest"))
    coach = coaches.select("game_id", "is_home", "coach_id")
    base = stints.filter(
        pl.col("valid_personnel") & pl.col("home_goalie").is_not_null() & pl.col("away_goalie").is_not_null()
        & (pl.col("duration_s") > 0) & (pl.col("period") <= 3)
    )
    sides = []
    for att, dfd in (("home", "away"), ("away", "home")):
        is_home = att == "home"
        part = base.select(
            "game_id", "season", "stint_id", "period", "start_s", "end_s", "duration_s", "last_faceoff_s",
            "post_penalty_5v5",
            pl.lit(is_home).alias("att_home"),
            pl.col(f"{att}_team_id").alias("att_team"), pl.col(f"{dfd}_team_id").alias("def_team"),
            pl.col(f"{att}_skaters").alias("offence"), pl.col(f"{dfd}_skaters").alias("defence"),
            pl.col(f"{att}_n").alias("att_n"), pl.col(f"{dfd}_n").alias("def_n"),
            (pl.col(f"{att}_score").cast(pl.Int16) - pl.col(f"{dfd}_score").cast(pl.Int16)).clip(-3, 3).alias("lead"),
            pl.col(f"{att}_xgf").alias("xgf"),
            pl.col(f"{att}_gf").alias("gf"),
            pl.col(f"{att}_changed_since_faceoff").alias("att_changed"),
            (pl.col("last_faceoff_zone").replace_strict(_SAME if is_home else _FLIP, default=None)).alias("fo_zone"),
        )
        if state == "EV":
            part = part.filter((pl.col("att_n") == 5) & (pl.col("def_n") == 5))
        else:
            part = part.filter(pl.col("att_n") > pl.col("def_n"))
        part = (
            part.join(rest.rename({"is_home": "att_home", "rest": "att_rest"}), on=["game_id", "att_home"], how="left")
            .join(rest.with_columns(~pl.col("is_home")).rename({"is_home": "att_home", "rest": "def_rest"}), on=["game_id", "att_home"], how="left")
            .join(coach.rename({"is_home": "att_home", "coach_id": "att_coach"}), on=["game_id", "att_home"], how="left")
            .join(coach.with_columns(~pl.col("is_home")).rename({"is_home": "att_home", "coach_id": "def_coach"}), on=["game_id", "att_home"], how="left")
        )
        sides.append(part)
    return pl.concat(sides).with_columns(
        pl.when(pl.col("att_changed")).then(pl.lit("otf")).otherwise(pl.col("fo_zone")).alias("zone_type"),
        (pl.col("xgf") * 3600 / pl.col("duration_s")).alias("y"),
    ).sort("game_id", "stint_id", "att_home")


def _zone_shares(rows: pl.DataFrame) -> np.ndarray:
    """Fraction of each row's duration in seconds 0..34 after its last faceoff."""
    a0 = (rows["start_s"] - rows["last_faceoff_s"]).to_numpy().astype(float)
    a1 = a0 + rows["duration_s"].to_numpy().astype(float)
    a0 = np.nan_to_num(a0, nan=1e9)
    a1 = np.nan_to_num(a1, nan=1e9)
    edges = np.arange(ZONE_SECONDS)
    overlap = np.clip(np.minimum(a1[:, None], edges + 1) - np.maximum(a0[:, None], edges), 0, None)
    return overlap / rows["duration_s"].to_numpy().astype(float)[:, None]


def build(rows: pl.DataFrame, players: list[int] | None = None) -> Design:
    """Assemble the sparse design for :func:`attack_rows` output.

    Args:
        rows: Attack rows.
        players: Player ids to give columns (default: everyone in ``rows``).
    """
    players = players or sorted({p for col in ("offence", "defence") for lst in rows[col].to_list() for p in lst})
    coaches = sorted({c for c in rows["att_coach"].drop_nulls().to_list()} | {c for c in rows["def_coach"].drop_nulls().to_list()})
    columns = (
        [f"O:{p}" for p in players] + [f"D:{p}" for p in players]
        + [f"zone:{t}:{s}" for t in ZONE_TYPES for s in range(ZONE_SECONDS)]
        + [f"score:{lead}:p{per}" for lead in LEADS for per in (1, 2, 3) if not (lead == 0 and per == 1)]
        + ["home", "rest:att_b2b", "rest:att_rested", "rest:def_b2b", "rest:def_rested", "post_penalty"]
        + [f"coach:O:{c}" for c in coaches] + [f"coach:D:{c}" for c in coaches]
    )
    index = {c: i for i, c in enumerate(columns)}
    n = rows.height
    r_idx: list[np.ndarray] = []
    c_idx: list[np.ndarray] = []
    vals: list[np.ndarray] = []

    def add(row_ids: np.ndarray, col_ids: np.ndarray, v: np.ndarray | float = 1.0) -> None:
        r_idx.append(row_ids)
        c_idx.append(col_ids)
        vals.append(np.broadcast_to(np.asarray(v, dtype=float), row_ids.shape).copy())

    for side, prefix in (("offence", "O"), ("defence", "D")):
        exploded = rows.select(pl.int_range(pl.len()).alias("r"), side).explode(side).drop_nulls()
        keep = [index.get(f"{prefix}:{p}", -1) for p in exploded[side].to_list()]
        keep = np.array(keep)
        mask = keep >= 0
        add(exploded["r"].to_numpy()[mask], keep[mask])

    shares = _zone_shares(rows)
    ztype = rows["zone_type"].to_list()
    for t in ZONE_TYPES:
        sel = np.array([z == t for z in ztype])
        if not sel.any():
            continue
        rr, ss = np.nonzero(shares[sel])
        add(np.nonzero(sel)[0][rr], np.array([index[f"zone:{t}:{s}"] for s in ss]), shares[sel][rr, ss])

    lead, period = rows["lead"].to_numpy(), rows["period"].to_numpy()
    score_cols = np.array([index.get(f"score:{lv}:p{pv}", -1) for lv, pv in zip(lead, period)])
    mask = score_cols >= 0
    add(np.arange(n)[mask], score_cols[mask])

    flags = {
        "home": rows["att_home"].to_numpy(),
        "rest:att_b2b": (rows["att_rest"] == "b2b").fill_null(False).to_numpy(),
        "rest:att_rested": (rows["att_rest"] == "rested").fill_null(True).to_numpy(),
        "rest:def_b2b": (rows["def_rest"] == "b2b").fill_null(False).to_numpy(),
        "rest:def_rested": (rows["def_rest"] == "rested").fill_null(True).to_numpy(),
        "post_penalty": rows["post_penalty_5v5"].to_numpy(),
    }
    for name, flag in flags.items():
        ids = np.nonzero(flag)[0]
        add(ids, np.full(ids.shape, index[name]))
    for col, prefix in (("att_coach", "coach:O"), ("def_coach", "coach:D")):
        c = rows[col].to_list()
        ids = np.array([i for i, v in enumerate(c) if v is not None], dtype=int)
        add(ids, np.array([index[f"{prefix}:{c[i]}"] for i in ids], dtype=int))

    x = sparse.csr_matrix(
        (np.concatenate(vals), (np.concatenate(r_idx), np.concatenate(c_idx))), shape=(n, len(columns))
    )
    return Design(
        x=x, y=rows["y"].to_numpy().astype(float), w=rows["duration_s"].to_numpy().astype(float),
        columns=columns,
        rows=rows.select("game_id", "season", "stint_id", "att_home", "att_team", "def_team", "offence", "defence", "duration_s", "xgf", "gf"),
    )
