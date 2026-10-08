"""Style axes for every skater and soft archetypes for forwards (archetypes plan phase B).

Exploration (docs/plans/archetypes.md, Phase A results) found that forwards have real but
limited cluster structure and defence has none that is stable, so (owner, 2026-10-07):

* **Axes, everyone.** Each axis is a fixed, named composite: the mean of a few signed style
  features after a variance-stabilising transform (``sqrt`` for rates, ``logit`` for shares)
  and standardisation *within season and position group*, which removes league-wide drift in
  scorer recording. Axes are then scaled to mean 0, sd 1 over the fit pool. Fixed composites
  beat factor analysis here: FA lost the centre (faceoffs) and defensive-role axes.
* **Archetypes, forwards only.** A full-covariance Gaussian mixture on the forward axes,
  :data:`K_FORWARD` components, fitted once on pooled 500+-minute player-seasons
  (:data:`FIT_SEASONS`). Components are named by rule from their centroids, so a refit keeps
  the same names. Defence gets axes and comps only.
* **Comps:** nearest player-seasons in axis space (same group, other players, any season
  from 2010-11), style only. Ratings stay separate.

Tables: ``models/archetypes/{version}/model.json.gz`` and ``processed/archetypes/{season}``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import polars as pl
from sklearn.mixture import GaussianMixture

from nhl import config
from nhl.archetypes.features import FEATURES
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: axis -> {feature: sign}. Names read from the positive end.
AXES: dict[str, dict[str, dict[str, int]]] = {
    "F": {
        "perimeter": {"shot_dist": 1, "point_share": 1, "net_front_share": -1, "slot_share": -1},
        "shooter": {"shots60": 1},
        "release": {"wristsnap_share": 1, "tip_share": -1, "backhand_share": -1},
        "physical": {"hits60": 1, "hits_taken60": 1, "pen_taken60": 1, "pen_drawn60": 1},
        "size": {"height_in": 1, "weight_lb": 1},
        "defensive": {"blocks60": 1, "block_share": 1, "dz_start_share": 1, "pk_share": 1},
        "centre": {"faceoffs60": 1},
    },
    "D": {
        "wrister": {"wristsnap_share": 1, "slap_share": -1},
        "activation": {"slot_share": 1, "net_front_share": 1, "point_share": -1, "shot_dist": -1},
        "shooter": {"shots60": 1},
        "physical": {"hits60": 1, "hits_taken60": 1, "pen_taken60": 1, "pen_drawn60": 1},
        "size": {"height_in": 1, "weight_lb": 1},
        "shot_blocker": {"blocks60": 1, "block_share": 1},
        "defensive": {"dz_start_share": 1, "pk_share": 1},
    },
}
AXIS_LABELS = {
    "perimeter": "shoots from distance (vs net-front / slot)",
    "shooter": "shot volume",
    "release": "wrist/snap release (vs tips and backhands)",
    "physical": "hits, gets hit, takes and draws penalties",
    "size": "height and weight",
    "defensive": "blocks, DZ starts, PK time",
    "centre": "faceoffs taken",
    "wrister": "wrist/snap shots (vs slap shots)",
    "activation": "shoots from the slot / net-front (vs the point)",
    "shot_blocker": "blocks shots",
}

FIT_SEASONS = tuple(config.season_id(y) for y in range(2015, 2026))
FIT_MIN_MINUTES = 500
NORM_MIN_SKATERS = 100
K_FORWARD = 5
N_COMPS = 5
MODEL_PREFIX = "models/archetypes/"

_KIND = {f.name: f.kind for f in FEATURES}


def _features(group: str) -> list[str]:
    return sorted({f for axis in AXES[group].values() for f in axis})


def transform(df: pl.DataFrame, feats: list[str]) -> pl.DataFrame:
    """Variance-stabilised features: ``sqrt`` for rates, clipped ``logit`` for shares."""
    out = []
    for f in feats:
        c = pl.col(f)
        if _KIND[f] == "rate":
            c = c.clip(0).sqrt()
        elif _KIND[f] == "share":
            p = c.clip(1e-3, 1 - 1e-3)
            c = (p / (1 - p)).log()
        out.append(c.cast(pl.Float64).alias(f))
    return df.with_columns(out)


def season_norms(style: pl.DataFrame, group: str, prev: dict[str, tuple[float, float]] | None = None
                 ) -> dict[str, tuple[float, float]]:
    """Per-feature (mean, sd) of transformed features among the season's 500+-minute skaters.

    Falls back to 200+ minutes, then to ``prev`` (the previous season's norms), when the season
    is too young to have :data:`NORM_MIN_SKATERS` such skaters.
    """
    feats = _features(group)
    base = style.filter((pl.col("window") == "season") & (pl.col("group") == group))
    for minutes in (FIT_MIN_MINUTES, 200):
        pop = base.filter(pl.col("toi_5v5_min") >= minutes)
        if pop.height >= NORM_MIN_SKATERS:
            t = transform(pop, feats)
            return {f: (float(t[f].mean()), float(t[f].std())) for f in feats}
    if prev is None:
        raise ValueError(f"no norms for group {group}: too few skaters and no previous season")
    return prev


def axis_raw(df: pl.DataFrame, group: str, norms: dict[str, tuple[float, float]]) -> np.ndarray:
    """Unscaled axis composites (rows of ``df`` × axes of ``group``).

    A missing feature (e.g. no height/weight yet for a debutant) scores as the league norm
    (z = 0) rather than poisoning the whole row with NaN.
    """
    feats = _features(group)
    t = transform(df, feats)
    z = {f: np.nan_to_num((t[f].to_numpy() - norms[f][0]) / norms[f][1], nan=0.0) for f in feats}
    return np.column_stack([sum(s * z[f] for f, s in axis.items()) / len(axis) for axis in AXES[group].values()])


# --- naming -----------------------------------------------------------------------------------


def name_forward_components(means: np.ndarray) -> list[str]:
    """Rule-based names for the forward mixture components from their axis centroids.

    Centres (``centre`` > 0) split by ``defensive``: higher = two-way centre, other =
    offensive centre. Wingers: the most ``physical`` + ``size`` = power forward; of the rest,
    the most ``shooter`` + ``perimeter`` + ``release`` = skill winger; anything left = balanced.
    Duplicates get a numeric suffix.
    """
    ax = list(AXES["F"])
    col = {a: means[:, i] for i, a in enumerate(ax)}
    names = [""] * len(means)
    centres = [i for i in range(len(means)) if col["centre"][i] > 0]
    wings = [i for i in range(len(means)) if i not in centres]
    if centres:
        two_way = max(centres, key=lambda i: col["defensive"][i])
        for i in centres:
            names[i] = "two-way centre" if i == two_way and len(centres) > 1 else "offensive centre"
        if len(centres) == 1:
            names[centres[0]] = "centre"
    if wings:
        power = max(wings, key=lambda i: col["physical"][i] + col["size"][i])
        names[power] = "power forward"
        rest = [i for i in wings if i != power]
        if rest:
            skill = max(rest, key=lambda i: col["shooter"][i] + col["perimeter"][i] + col["release"][i])
            names[skill] = "skill winger"
            for i in rest:
                if i != skill:
                    names[i] = "balanced winger"
    seen: dict[str, int] = {}
    for i, n in enumerate(names):
        seen[n] = seen.get(n, 0) + 1
        if seen[n] > 1:
            names[i] = f"{n} {seen[n]}"
    return names


# --- fit / save / load ---------------------------------------------------------------------------


def _all_norms(store: Store, seasons: list[int]) -> dict[tuple[int, str], dict[str, tuple[float, float]]]:
    norms: dict[tuple[int, str], dict[str, tuple[float, float]]] = {}
    for season in sorted(seasons):
        style = store.read_parquet_required(keys.style(season))
        prev_id = config.season_id(config.season_start_year(season) - 1)
        for g in AXES:
            norms[(season, g)] = season_norms(style, g, norms.get((prev_id, g)))
    return norms


def fit(store: Store, seasons: tuple[int, ...] = FIT_SEASONS, seed: int = 0) -> dict:
    """Fit axis scaling and the forward mixture on pooled 500+-minute player-seasons."""
    model: dict = {"fit_seasons": list(seasons), "axes": AXES, "groups": {}, "k_forward": K_FORWARD,
                   "fitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    norms = _all_norms(store, list(seasons))
    for g in AXES:
        parts = []
        for season in seasons:
            st = store.read_parquet_required(keys.style(season)).filter(
                (pl.col("window") == "season") & (pl.col("group") == g) & (pl.col("toi_5v5_min") >= FIT_MIN_MINUTES))
            parts.append(axis_raw(st, g, norms[(season, g)]))
        raw = np.vstack(parts)
        mu, sd = raw.mean(0), raw.std(0)
        entry = {"axis_mean": mu.tolist(), "axis_sd": sd.tolist(), "n_fit": int(len(raw))}
        if g == "F":
            gm = GaussianMixture(K_FORWARD, covariance_type="full", n_init=8, random_state=seed).fit((raw - mu) / sd)
            names = name_forward_components(gm.means_)
            order = np.argsort(names)
            entry["mixture"] = {"weights": gm.weights_[order].tolist(), "means": gm.means_[order].tolist(),
                                "covariances": gm.covariances_[order].tolist(), "names": [names[i] for i in order]}
        model["groups"][g] = entry
    return model


def version_key(version: str) -> str:
    """Key of one stored model version."""
    return f"{MODEL_PREFIX}{version}/model.json.gz"


def save(store: Store, model: dict, version: str | None = None) -> str:
    """Store ``model`` as a new version and point ``LATEST`` at it; return the version."""
    version = version or "v" + datetime.now(timezone.utc).strftime("%Y%m%d")
    model["version"] = version
    store.put_json_gz(version_key(version), model)
    store.put_json_gz(f"{MODEL_PREFIX}LATEST.json.gz", {"version": version})
    return version


def load(store: Store, version: str | None = None) -> dict:
    """Load a model version (default: LATEST)."""
    if version is None:
        latest = store.get_json_gz(f"{MODEL_PREFIX}LATEST.json.gz")
        if latest is None:
            raise FileNotFoundError("no archetype model: run `nhl archetypes --fit` first")
        version = latest["version"]
    model = store.get_json_gz(version_key(version))
    if model is None:
        raise FileNotFoundError(version_key(version))
    return model


def _mixture(entry: dict) -> GaussianMixture:
    mix = entry["mixture"]
    gm = GaussianMixture(len(mix["weights"]), covariance_type="full")
    gm.weights_ = np.asarray(mix["weights"])
    gm.means_ = np.asarray(mix["means"])
    gm.covariances_ = np.asarray(mix["covariances"])
    gm.precisions_cholesky_ = np.linalg.cholesky(np.linalg.inv(gm.covariances_))
    return gm


# --- assign ---------------------------------------------------------------------------------------


def scores(style: pl.DataFrame, model: dict, norms: dict[str, dict[str, tuple[float, float]]]) -> pl.DataFrame:
    """Axis scores and (forwards) archetype probabilities for every row of a ``style`` table.

    Returns ``season, window, player_id, group, toi_5v5_min, {axis}`` per axis of the group,
    and for forwards ``p_{archetype}``, ``archetype`` (top) and ``confidence`` (its probability).
    """
    out = []
    for g, entry in model["groups"].items():
        df = style.filter(pl.col("group") == g)
        if df.is_empty():
            continue
        z = (axis_raw(df, g, norms[g]) - np.asarray(entry["axis_mean"])) / np.asarray(entry["axis_sd"])
        part = df.select("season", "window", "player_id", "group", "toi_5v5_min").with_columns(
            pl.Series(a, z[:, i]) for i, a in enumerate(model["axes"][g]))
        if "mixture" in entry:
            names = entry["mixture"]["names"]
            p = _mixture(entry).predict_proba(z)
            part = part.with_columns(
                *[pl.Series(f"p_{n.replace(' ', '_').replace('-', '_')}", p[:, i]) for i, n in enumerate(names)],
                pl.Series("archetype", [names[i] for i in p.argmax(1)]),
                pl.Series("confidence", p.max(1)),
            )
        out.append(part)
    return pl.concat(out, how="diagonal_relaxed")


def percentiles(sc: pl.DataFrame, model: dict) -> pl.DataFrame:
    """Add ``{axis}_pct``: percentile within group, window and season among 500+-minute skaters."""
    out = []
    for g in model["groups"]:
        df = sc.filter(pl.col("group") == g)
        for (window,), part in df.group_by(["window"]):
            ref = part.filter(pl.col("toi_5v5_min") >= FIT_MIN_MINUTES)
            ref = part if ref.height < NORM_MIN_SKATERS else ref
            cols = []
            for a in model["axes"][g]:
                sorted_ref = np.sort(ref[a].to_numpy())
                pct = np.searchsorted(sorted_ref, part[a].to_numpy(), side="right") / max(len(sorted_ref), 1)
                cols.append(pl.Series(f"{a}_pct", 100 * pct))
            out.append(part.with_columns(cols))
    return pl.concat(out, how="diagonal_relaxed")


def comps(sc: pl.DataFrame, pool: pl.DataFrame, model: dict, n: int = N_COMPS) -> pl.DataFrame:
    """``season, window, player_id, comps`` (list of {player_id, season, distance}), nearest first.

    ``pool`` is every 500+-minute ``season``-window player-season with axis scores; the
    player's own seasons are excluded.
    """
    out = []
    for g in model["groups"]:
        axes = list(model["axes"][g])
        q = sc.filter(pl.col("group") == g)
        p = pool.filter(pl.col("group") == g)
        if q.is_empty() or p.is_empty():
            continue
        Q, P = q.select(axes).to_numpy(), p.select(axes).to_numpy()
        pid, pseason = p["player_id"].to_numpy(), p["season"].to_numpy()
        qid = q["player_id"].to_numpy()
        rows = []
        for i in range(len(Q)):
            d = np.sqrt(((P - Q[i]) ** 2).sum(1))
            d[pid == qid[i]] = np.inf
            # One entry per comp player: his closest season.
            best, seen = [], set()
            for j in np.argsort(d):
                if not np.isfinite(d[j]):
                    break
                if pid[j] not in seen:
                    seen.add(pid[j])
                    best.append(j)
                    if len(best) == n:
                        break
            rows.append([{"player_id": int(pid[j]), "season": int(pseason[j]), "distance": float(d[j])} for j in best])
        out.append(q.select("season", "window", "player_id").with_columns(pl.Series("comps", rows)))
    return pl.concat(out)


def comp_pool(store: Store, model: dict) -> pl.DataFrame:
    """Axis scores of every stored 500+-minute ``season``-window player-season (the comps pool)."""
    seasons = sorted(int(k.rsplit("/", 1)[1].split(".")[0]) for k in store.list_keys("processed/style/"))
    norms = _all_norms(store, seasons)
    parts = []
    for season in seasons:
        st = store.read_parquet_required(keys.style(season)).filter(
            (pl.col("window") == "season") & (pl.col("toi_5v5_min") >= FIT_MIN_MINUTES))
        if st.height:
            parts.append(scores(st, model, {g: norms[(season, g)] for g in AXES}))
    return pl.concat(parts, how="diagonal_relaxed")


def build_season(store: Store, season: int, model: dict | None = None, pool: pl.DataFrame | None = None
                 ) -> dict[str, float]:
    """Build ``processed/archetypes/{season}`` (both windows) and return checks."""
    model = model or load(store)
    style = store.read_parquet_required(keys.style(season))
    prev_id = config.season_id(config.season_start_year(season) - 1)
    prev_style = store.get_parquet(keys.style(prev_id))
    norms = {}
    for g in AXES:
        prev = season_norms(prev_style, g) if prev_style is not None else None
        norms[g] = season_norms(style, g, prev)
    sc = percentiles(scores(style, model, norms), model)
    pool = comp_pool(store, model) if pool is None else pool
    out = sc.join(comps(sc, pool, model), on=["season", "window", "player_id"], how="left").with_columns(
        pl.lit(model["version"]).alias("model_version"))
    store.put_parquet(keys.archetypes(season), out.sort("window", "group", "player_id"))
    f = out.filter((pl.col("window") == "season") & (pl.col("group") == "F") & (pl.col("toi_5v5_min") >= FIT_MIN_MINUTES))
    checks = {"skaters": float(out.filter(pl.col("window") == "season").height)}
    if f.height:
        checks["f_confidence"] = float(f["confidence"].mean())
        for name, share in f["archetype"].value_counts(normalize=True).iter_rows():
            checks[f"share_{name.replace(' ', '_')}"] = float(share)
    logger.info("archetypes %s: %s", season, ", ".join(f"{k} {v:.3f}" for k, v in checks.items()))
    return checks


def persistence(store: Store, seasons: list[int]) -> dict[str, float]:
    """Year-to-year stability among 500+-minute skaters (``season`` window): axis correlations
    and the share of forwards keeping the same top archetype."""
    tabs = [store.get_parquet(keys.archetypes(s)) for s in seasons]
    df = pl.concat([t for t in tabs if t is not None], how="diagonal_relaxed").filter(
        (pl.col("window") == "season") & (pl.col("toi_5v5_min") >= FIT_MIN_MINUTES))
    nxt = df.with_columns(pl.col("season") - 10001)
    j = df.join(nxt, on=["player_id", "season", "group"], suffix="_next")
    res: dict[str, float] = {}
    for g in AXES:
        jg = j.filter(pl.col("group") == g)
        for a in AXES[g]:
            res[f"{g}_{a}_yoy"] = float(np.corrcoef(jg[a], jg[f"{a}_next"])[0, 1])
    jf = j.filter(pl.col("group") == "F")
    res["F_same_archetype"] = float((jf["archetype"] == jf["archetype_next"]).mean())
    return res
