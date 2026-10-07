"""On-ice decomposition per skater-game (usage plan phase B): ``processed/onice_context/{season}``.

The M3 EV model is linear: for every 5v5 stint and attacking team,

    xGF/60 = intercept + Σ O(attackers) + Σ D(defenders) + zone + score/home/rest/coach/post-penalty

So a skater's on-ice xGF/60 and xGA/60 split **exactly** into parts, using the rating
snapshot in effect on the game's date (the latest dated on or before it, as the simulator
does; :func:`nhl.sim.inputs._latest_before`):

| part | xGF (his team attacking) | xGA (defending) |
|---|---|---|
| ``own`` | his O term | his D term |
| ``mates`` | the other 4 skaters' O terms | their D terms |
| ``comp`` | the 5 opponents' D terms | their O terms |
| ``zone`` | zone-start terms (attackers' view) | same, for the stint's attackers |
| ``ctx`` | score, home, rest, coaches, post-penalty | same |
| ``league`` | the intercept | same |
| ``resid`` | actual − all of the above | same |

All parts are xG per 60, TOI-weighted over the skater's 5v5 stints in the game (the
EV design's rows: valid personnel, both goalies in, regulation). Defence terms are
"allowed", so for xGA lower is better; xGD = xGF − xGA for each part.

**Quality of teammates / competition** (per skater, TOI-weighted):
``qot_o`` / ``qot_d`` = his 4 teammates' mean O / D terms, ``qoc_o`` / ``qoc_d`` the 5
opponents'. Net = O − D (higher = better teammates / tougher opponents). The classic
**TOI-based** versions, ``qot_toi`` / ``qoc_toi``, are the mean 5v5 TOI share
(``processed/usage``) of his teammates / opponents in that game.

Players missing from a snapshot get the simulator's replacement-level terms
(:func:`nhl.sim.inputs.replacement_levels`); context terms missing from it (a coach hired
after it) count 0. Seasons start in 2015-16, the first with snapshots.
"""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import polars as pl

from nhl.ratings import rapm
from nhl.sim.inputs import _latest_before, replacement_levels, snapshot_dates
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

PARTS = ("own", "mates", "comp", "zone", "ctx", "league")
_KEYS = ["game_id", "team_id", "player_id"]


def _snapshot_terms(store: Store, snap: date) -> tuple[dict[str, float], dict[str, float]]:
    """Every term of one EV snapshot by design column name, and the replacement levels."""
    base = f"ratings/{snap.isoformat()}/"
    ev = store.read_parquet_required(base + "ev.parquet")
    ctx = store.read_parquet_required(base + "context_ev.parquet")
    terms = dict(zip(ctx["term"], ctx["mean"]))
    terms.update({f"{s}:{p}": m for p, s, m in ev.select("player_id", "side", "mean").iter_rows()})
    repl = replacement_levels(ev, ev.clear(), store.read_parquet_required(base + "finishing.parquet"))
    return terms, {"O": repl["o_ev"], "D": repl["d_ev"]}


def row_sums(design, terms: dict[str, float], repl: dict[str, float], mask: np.ndarray) -> dict[str, np.ndarray]:
    """Per attack row in ``mask``: ``o_sum``, ``d_sum``, ``zone``, ``ctx`` and ``league``.

    Also returns ``missing`` (player columns not in the snapshot, counted once each).
    """
    cols = design.columns
    beta = np.zeros(len(cols))
    group = np.empty(len(cols), dtype="<U4")
    missing = 0
    for i, c in enumerate(cols):
        head = c.split(":", 1)[0]
        if head in ("O", "D"):
            group[i] = head
            if c in terms:
                beta[i] = terms[c]
            else:
                beta[i] = repl[head]
                missing += 1
        else:
            group[i] = "zone" if head == "zone" else "ctx"
            beta[i] = terms.get(c, 0.0)
    x = design.x[mask]
    out = {name: x @ np.where(group == g, beta, 0.0) for name, g in (("o_sum", "O"), ("d_sum", "D"), ("zone", "zone"), ("ctx", "ctx"))}
    out["league"] = np.full(x.shape[0], terms.get("intercept", 0.0))
    out["missing"] = np.array([missing])
    out["player_terms"] = {c: b for c, b, g in zip(cols, beta, group) if g in ("O", "D")}
    return out


def _views(rows: pl.DataFrame, player_terms: dict[str, float], share: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Attack (xGF) and defence (xGA) views: one row per stint-row and on-ice skater.

    ``share`` is ``game_id, player_id, share_5v5`` from usage, for the TOI-based QoT.
    """
    o = pl.DataFrame({"player_id": [int(c[2:]) for c in player_terms if c[0] == "O"],
                      "o": [v for c, v in player_terms.items() if c[0] == "O"]})
    d = pl.DataFrame({"player_id": [int(c[2:]) for c in player_terms if c[0] == "D"],
                      "d": [v for c, v in player_terms.items() if c[0] == "D"]})
    common = ["game_id", "duration_s", "y", "o_sum", "d_sum", "zone", "ctx", "league", "opp_toi", "own_toi_sum"]
    att = rows.select(*common, pl.col("att_team").alias("team_id"), pl.col("offence").alias("player_id")).explode("player_id", empty_as_null=True)
    att = att.join(o, on="player_id", how="left").join(share, on=["game_id", "player_id"], how="left").with_columns(
        pl.col("o").alias("own"), (pl.col("o_sum") - pl.col("o")).alias("mates"), pl.col("d_sum").alias("comp"),
        ((pl.col("o_sum") - pl.col("o")) / 4).alias("qot_o"), (pl.col("d_sum") / 5).alias("qoc_d"),
        ((pl.col("own_toi_sum") - pl.col("share_5v5")) / 4).alias("qot_toi"), pl.col("opp_toi").alias("qoc_toi"),
    )
    dfd = rows.select(*common, pl.col("def_team").alias("team_id"), pl.col("defence").alias("player_id")).explode("player_id", empty_as_null=True)
    dfd = dfd.join(d, on="player_id", how="left").with_columns(
        pl.col("d").alias("own"), (pl.col("d_sum") - pl.col("d")).alias("mates"), pl.col("o_sum").alias("comp"),
        ((pl.col("d_sum") - pl.col("d")) / 4).alias("qot_d"), (pl.col("o_sum") / 5).alias("qoc_o"),
    )
    return att, dfd


def _aggregate(view: pl.DataFrame, suffix: str, extra: list[str]) -> pl.DataFrame:
    """TOI-weighted per skater-game parts for one view (``suffix`` f = xGF, a = xGA)."""
    w = pl.col("duration_s")
    wmean = lambda c: ((pl.col(c) * w).sum() / w.sum())  # noqa: E731
    return view.group_by(_KEYS).agg(
        w.sum().alias("toi_s"),
        wmean("y").alias(f"actual_{suffix}"),
        *[wmean(p).alias(f"{p}_{suffix}") for p in PARTS],
        *[wmean(c).alias(c) for c in extra],
    ).with_columns(
        (pl.col(f"actual_{suffix}") - pl.sum_horizontal(*[f"{p}_{suffix}" for p in PARTS])).alias(f"resid_{suffix}")
    )


def _toi_shares(rows: pl.DataFrame, usage: pl.DataFrame) -> pl.DataFrame:
    """Adds ``opp_toi`` (defenders' mean 5v5 TOI share) and ``own_toi_sum`` (attackers' sum)."""
    share = usage.select("game_id", "player_id", "share_5v5")
    rows = rows.with_row_index("_r")
    per = []
    for side, name, agg in (("defence", "opp_toi", pl.col("share_5v5").mean()), ("offence", "own_toi_sum", pl.col("share_5v5").sum())):
        per.append(rows.select("_r", "game_id", pl.col(side).alias("player_id")).explode("player_id", empty_as_null=True)
                   .join(share, on=["game_id", "player_id"], how="left").group_by("_r").agg(agg.alias(name)))
    return rows.join(per[0], on="_r", how="left").join(per[1], on="_r", how="left").drop("_r")


def snapshot_parts(store: Store, design, rows: pl.DataFrame, days: list[date]):
    """Yield ``(snapshot, rows, player_terms)`` for each snapshot used by ``rows``.

    ``rows`` are the design's rows with ``_i`` (the design row index) and ``game_date``; each
    yielded frame adds ``snapshot, o_sum, d_sum, zone, ctx, league`` (see :func:`row_sums`).
    """
    rows = rows.with_columns(
        pl.col("game_date").map_elements(lambda d: _latest_before(days, d), return_dtype=pl.Date).alias("snapshot")
    )
    for (snap,), part in rows.filter(pl.col("snapshot").is_not_null()).partition_by("snapshot", as_dict=True).items():
        terms, repl = _snapshot_terms(store, snap)
        mask = np.zeros(design.x.shape[0], dtype=bool)
        mask[part["_i"].to_numpy()] = True
        part = part.sort("_i")
        sums = row_sums(design, terms, repl, mask)
        logger.debug("snapshot %s: %d rows, %d players at replacement", snap, part.height, int(sums["missing"][0]))
        yield snap, part.with_columns(*[pl.Series(k, sums[k]) for k in ("o_sum", "d_sum", "zone", "ctx", "league")]), sums["player_terms"]


def stint_predictions(store: Store, season: int, design=None, snapshots: list[date] | None = None) -> pl.DataFrame:
    """Every EV attack row of ``season`` with the point-in-time prediction split into
    ``o_sum, d_sum, zone, ctx, league`` and ``resid`` = ``y`` − their sum (xG per 60)."""
    design = design or rapm.season_design(store, season, "EV")
    days = snapshots if snapshots is not None else snapshot_dates(store)
    rows = design.rows.with_columns(pl.Series("y", design.y)).with_row_index("_i")
    parts = [part for _, part, _ in snapshot_parts(store, design, rows, days)]
    if not parts:
        return pl.DataFrame()
    return pl.concat(parts).with_columns(
        (pl.col("y") - pl.sum_horizontal("o_sum", "d_sum", "zone", "ctx", "league")).alias("resid")
    ).sort("_i")


def build_onice(store: Store, season: int, design=None, usage: pl.DataFrame | None = None,
                snapshots: list[date] | None = None) -> pl.DataFrame:
    """The decomposition for every skater-game of ``season`` that has a snapshot (see module docstring).

    Returns:
        ``game_id, team_id, player_id, snapshot, toi_s``; for ``f`` (xGF) and ``a`` (xGA):
        ``actual_*``, ``own_*``, ``mates_*``, ``comp_*``, ``zone_*``, ``ctx_*``, ``league_*``,
        ``resid_*``; ``qot_o, qot_d, qoc_o, qoc_d, qot_toi, qoc_toi``.
    """
    design = design or rapm.season_design(store, season, "EV")
    usage = usage if usage is not None else store.read_parquet_required(keys.usage(season))
    days = snapshots if snapshots is not None else snapshot_dates(store)
    rows = _toi_shares(design.rows.with_columns(pl.Series("y", design.y)).with_row_index("_i"), usage)
    share = usage.select("game_id", "player_id", "share_5v5")
    out = []
    for snap, part, player_terms in snapshot_parts(store, design, rows, days):
        att, dfd = _views(part, player_terms, share)
        f = _aggregate(att, "f", ["qot_o", "qoc_d", "qot_toi", "qoc_toi"])
        a = _aggregate(dfd, "a", ["qot_d", "qoc_o"]).drop("toi_s")
        out.append(f.join(a, on=_KEYS, how="left").with_columns(pl.lit(snap).alias("snapshot")))
    if not out:
        return pl.DataFrame()
    return pl.concat(out).sort(*_KEYS)


def _snapshot_player_terms(store: Store, snaps: list[date]) -> pl.DataFrame:
    """``snapshot, player_id, o, d`` for every player in each snapshot, plus a ``player_id``-null
    row per snapshot holding its replacement levels."""
    out = []
    for snap in snaps:
        base = f"ratings/{snap.isoformat()}/"
        ev = store.read_parquet_required(base + "ev.parquet")
        _, repl = _snapshot_terms(store, snap)
        wide = ev.pivot(on="side", index="player_id", values="mean").rename({"O": "o", "D": "d"})
        out.append(pl.concat([
            wide.select(pl.lit(snap).alias("snapshot"), "player_id", "o", "d"),
            pl.DataFrame({"snapshot": [snap], "player_id": [None], "o": [repl["O"]], "d": [repl["D"]]},
                         schema={"snapshot": pl.Date, "player_id": pl.Int64, "o": pl.Float64, "d": pl.Float64}),
        ]))
    return pl.concat(out)


def unit_context(store: Store, preds: pl.DataFrame, rosters: pl.DataFrame, usage: pl.DataFrame) -> pl.DataFrame:
    """The same decomposition for every forward line (exactly 3 forwards) and D pair (exactly 2
    defencemen), over the 5v5 stints the whole unit was on the ice together.

    For a unit, ``own`` is the sum of its members' terms, ``mates`` the other skaters' (the pair
    for a line, the line for a pair) and ``comp`` the five opponents'.

    Args:
        preds: :func:`stint_predictions` output.
        rosters: ``game_id, player_id, position``.
        usage: ``processed/usage`` (for the unit's tier).

    Returns:
        ``team_id, kind`` (F / D), ``player_ids`` (sorted), ``toi_s``, ``tier`` (the members'
        most common tier, by time together), and ``{part}_{f|a}`` for
        actual / own / mates / comp / zone / ctx / league / resid.
    """
    pos = rosters.select("game_id", "player_id", (pl.col("position") == "D").alias("is_d"))
    terms = _snapshot_player_terms(store, sorted(preds["snapshot"].unique().to_list()))
    repl = terms.filter(pl.col("player_id").is_null()).select("snapshot", pl.col("o").alias("ro"), pl.col("d").alias("rd"))
    named = terms.drop_nulls("player_id")
    base = preds.select("_i", "game_id", "snapshot", "att_team", "def_team", "duration_s", "y",
                        "o_sum", "d_sum", "zone", "ctx", "league", "resid", "offence", "defence")
    groups = []
    for side, team, term in (("offence", "att_team", "o"), ("defence", "def_team", "d")):
        ex = (base.select("_i", "game_id", "snapshot", pl.col(side).alias("player_id")).explode("player_id", empty_as_null=True)
              .join(pos, on=["game_id", "player_id"], how="inner")
              .join(named, on=["snapshot", "player_id"], how="left").join(repl, on="snapshot", how="left")
              .with_columns(pl.coalesce(term, "r" + term).alias("t")))
        groups.append(ex.group_by("_i").agg(
            pl.col("player_id").filter(~pl.col("is_d")).sort().alias(f"{side}_F"),
            pl.col("player_id").filter(pl.col("is_d")).sort().alias(f"{side}_D"),
            pl.col("t").filter(~pl.col("is_d")).sum().alias(f"{side}_F_t"),
            pl.col("t").filter(pl.col("is_d")).sum().alias(f"{side}_D_t"),
        ))
    rows = base.join(groups[0], on="_i").join(groups[1], on="_i")
    tiers = usage.select("game_id", "player_id", "tier")
    out = []
    for kind, size in (("F", 3), ("D", 2)):
        views = []
        for s, side, team, other_sum, comp in (("f", "offence", "att_team", "o_sum", "d_sum"), ("a", "defence", "def_team", "d_sum", "o_sum")):
            v = rows.filter(pl.col(f"{side}_{kind}").list.len() == size).select(
                "_i", "game_id", pl.col(team).cast(pl.Int64).alias("team_id"), pl.col(f"{side}_{kind}").alias("player_ids"),
                "duration_s", pl.col("y").alias("actual"), pl.col(f"{side}_{kind}_t").alias("own"),
                (pl.col(other_sum) - pl.col(f"{side}_{kind}_t")).alias("mates"), pl.col(comp).alias("comp"),
                "zone", "ctx", "league", "resid",
            )
            w = pl.col("duration_s")
            agg = v.group_by("team_id", "player_ids").agg(
                w.sum().alias(f"toi_{s}"),
                *[((pl.col(c) * w).sum() / w.sum()).alias(f"{c}_{s}") for c in ("actual", *PARTS, "resid")],
            )
            views.append((v, agg))
        both = views[0][1].join(views[1][1], on=["team_id", "player_ids"], how="inner")
        # Tier: the members' most common tier, weighted by seconds together.
        together = (views[0][0].group_by("team_id", "player_ids", "game_id").agg(pl.col("duration_s").sum())
                    .with_columns(pl.col("player_ids").list.first().alias("player_id"))
                    .join(tiers, on=["game_id", "player_id"], how="left")
                    .group_by("team_id", "player_ids", "tier").agg(pl.col("duration_s").sum())
                    .sort("duration_s", descending=True).group_by("team_id", "player_ids", maintain_order=True)
                    .agg(pl.col("tier").first()))
        out.append(both.join(together, on=["team_id", "player_ids"], how="left").with_columns(
            pl.lit(kind).alias("kind"), pl.col("toi_f").alias("toi_s")).drop("toi_f", "toi_a"))
    return pl.concat(out)


def linemates(stints: pl.DataFrame) -> pl.DataFrame:
    """Season 5v5 time together per (team, player, teammate): ``team_id, player_id, mate_id, shared_s, games``."""
    from nhl.usage.projection import shared_toi

    return shared_toi(stints).group_by("team_id", "player_id", "mate_id").agg(
        pl.col("shared_s").sum(), pl.col("game_id").n_unique().alias("games"))


#: Minimum 5v5 minutes for a skater to get QoT / QoC / part percentiles in the summary.
PCTL_MIN_TOI_S = 200 * 60


def summarize(onice: pl.DataFrame, usage: pl.DataFrame) -> pl.DataFrame:
    """Per (season, player, team): TOI-weighted parts, xGD parts and percentiles.

    ``*_xgd`` = xGF part − xGA part (higher is better), ``qot_net`` / ``qoc_net`` = O − D.
    Percentiles (``*_pct``, 0-1) are within position group (F / D) among skaters with at
    least :data:`PCTL_MIN_TOI_S` of 5v5 time with that team; others get null.
    """
    meta = usage.select("game_id", "player_id", "season", pl.col("position").is_in(["C", "L", "R"]).alias("is_f"))
    j = onice.join(meta, on=["game_id", "player_id"], how="left")
    w = pl.col("toi_s")
    wm = lambda e: (e * w).sum() / w.sum()  # noqa: E731
    cols = [c for c in onice.columns if c.endswith(("_f", "_a")) or c.startswith(("qot_", "qoc_"))]
    s = j.group_by("season", "player_id", "team_id").agg(
        pl.col("is_f").first(), pl.len().alias("games"), w.sum().alias("toi_s"), *[wm(pl.col(c)).alias(c) for c in cols],
    ).with_columns(
        *[(pl.col(f"{p}_f") - pl.col(f"{p}_a")).alias(f"{p}_xgd") for p in (*PARTS, "resid", "actual")],
        (pl.col("qot_o") - pl.col("qot_d")).alias("qot_net"), (pl.col("qoc_o") - pl.col("qoc_d")).alias("qoc_net"),
    )
    big = pl.col("toi_s") >= PCTL_MIN_TOI_S
    # Rank with nulls outside the qualifying group (nulls aren't ranked or counted).
    out = s.with_columns(*[pl.when(big).then(pl.col(c)).alias(f"_{c}") for c in ("qot_net", "qoc_net", "own_xgd", "mates_xgd", "comp_xgd")])
    out = out.with_columns(*[
        (pl.col(f"_{c}").rank("average").over("season", "is_f") / pl.col(f"_{c}").count().over("season", "is_f")).alias(f"{c}_pct")
        for c in ("qot_net", "qoc_net", "own_xgd", "mates_xgd", "comp_xgd")
    ]).drop([f"_{c}" for c in ("qot_net", "qoc_net", "own_xgd", "mates_xgd", "comp_xgd")])
    return out.with_columns(pl.when(pl.col("is_f")).then(pl.lit("F")).otherwise(pl.lit("D")).alias("group")).drop("is_f").sort("season", "team_id", "player_id")


def validate(onice: pl.DataFrame) -> dict[str, float]:
    """Sanity numbers: TOI-weighted mean residual (xGF, xGA) and the largest parts-sum error."""
    w = pl.col("toi_s")
    return {
        "player_games": float(onice.height),
        "resid_f": float(onice.select((pl.col("resid_f") * w).sum() / w.sum()).item()),
        "resid_a": float(onice.select((pl.col("resid_a") * w).sum() / w.sum()).item()),
        "parts_sum_err": float(onice.select(
            (pl.col("actual_f") - pl.sum_horizontal(*[f"{p}_f" for p in (*PARTS, "resid")])).abs().max()).item()),
    }


def build_season(store: Store, season: int) -> dict[str, float]:
    """Build and store ``processed/onice_context/{season}``, its season summary, the unit
    decomposition (``processed/unit_context``) and season linemates (``processed/linemates``)."""
    usage = store.read_parquet_required(keys.usage(season))
    onice = build_onice(store, season, usage=usage)
    if onice.is_empty():
        logger.info("onice %s: no rating snapshots", season)
        return {}
    store.put_parquet(keys.onice_context(season), onice)
    store.put_parquet(keys.onice_context_summary(season), summarize(onice, usage))
    stints = store.read_parquet_required(keys.stints(season))
    rosters = store.read_parquet_required(keys.rosters(season))
    store.put_parquet(keys.unit_context(season), unit_context(store, stint_predictions(store, season), rosters, usage))
    store.put_parquet(keys.linemates(season), linemates(stints))
    checks = validate(onice)
    logger.info("onice %s: %s", season, ", ".join(f"{k} {v:.4f}" for k, v in checks.items()))
    return checks
