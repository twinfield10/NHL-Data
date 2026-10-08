"""Do archetypes earn a place in the models? (archetypes plan phase C).

Three tests, each scored on held-out seasons, each using only style known *before* the
season: the previous season's ``2yr`` window from ``processed/archetypes`` (forward archetype
probabilities and axes; defence axes only). Skaters without one (rookies, call-ups) get the
neutral point (axes 0, mean probabilities) and are flagged.

1. **Point attribution** (:func:`attribution_test`): for every skater on the ice for a goal
   for (5v5, and 5-on-4 PP separately), was he the scorer, the primary assister, the secondary
   assister, or none? Each skater's probabilities are his own history (previous three seasons
   at weights 1, ½, ¼ plus the season to date) shrunk toward a prior from a multinomial
   logistic model. Baseline prior: position (F/D) only. Candidate prior: position plus style.
   The prior weight ``k`` is tuned per model on the training seasons. Score: held-out log loss
   per skater-goal, leave one season out.
2. **Line chemistry** (:func:`chemistry_test`): see that function.
3. **Aging** (:func:`aging_test`): see that function.
"""

from __future__ import annotations

import logging

import numpy as np
import polars as pl
from sklearn.linear_model import LogisticRegression

from nhl import config
from nhl.archetypes.model import AXES
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

HISTORY_WEIGHTS = (1.0, 0.5, 0.25)
OUTCOMES = ("G", "A1", "A2", "none")
K_GRID = (5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 320.0, 640.0, 1280.0)
#: History sizes (weighted on-ice goals before the game) for the by-sample breakdown.
HISTORY_BUCKETS = (0, 10, 40, 100)


# --- style known before the season -------------------------------------------------------------


def style_inputs(store: Store, season: int) -> pl.DataFrame:
    """Previous season's ``2yr`` style per player: ``player_id, has_style`` plus ``F_{axis}``,
    ``D_{axis}`` (0 for the other group) and the forward ``p_*`` columns (0 for defence)."""
    prev = config.season_id(config.season_start_year(season) - 1)
    a = store.get_parquet(keys.archetypes(prev))
    if a is None:
        return pl.DataFrame(schema={"player_id": pl.Int64, "has_style": pl.Boolean})
    a = a.filter(pl.col("window") == "2yr")
    p_cols = sorted(c for c in a.columns if c.startswith("p_"))
    cols = []
    for g, axes in AXES.items():
        for ax in axes:
            cols.append(pl.when(pl.col("group") == g).then(pl.col(ax)).otherwise(0.0).fill_null(0.0).alias(f"{g}_{ax}"))
    return a.select("player_id", pl.lit(True).alias("has_style"), *cols,
                    *[pl.col(c).fill_null(0.0) for c in p_cols])


def style_columns(style: pl.DataFrame) -> list[str]:
    """Style feature columns of a :func:`style_inputs` table."""
    return [c for c in style.columns if c not in ("player_id", "has_style")]


def attach_style(df: pl.DataFrame, style: pl.DataFrame, on: str = "player_id") -> pl.DataFrame:
    """Left-join style; missing players get 0 axes and the mean forward probabilities."""
    cols = style_columns(style)
    out = df.join(style.rename({"player_id": on}) if on != "player_id" else style, on=on, how="left")
    fills = []
    for c in cols:
        fill = float(style[c].mean()) if c.startswith("p_") and style.height else 0.0
        fills.append(pl.col(c).fill_null(fill))
    return out.with_columns(*fills, pl.col("has_style").fill_null(False))


# --- 1. point attribution --------------------------------------------------------------------


def goal_rows(events: pl.DataFrame) -> pl.DataFrame:
    """One row per (regular-season goal, skater of the scoring team on the ice).

    Returns ``season, game_id, event_idx, game_date, state`` (``5v5`` or ``PP`` = scoring team
    5 on 4), ``player_id, outcome`` (G / A1 / A2 / none).
    """
    g = events.filter((pl.col("event_type") == "GOAL") & (pl.col("season_type") == "R"))
    own_n = pl.when(pl.col("is_home_event")).then(pl.col("home_skaters_on")).otherwise(pl.col("away_skaters_on"))
    opp_n = pl.when(pl.col("is_home_event")).then(pl.col("away_skaters_on")).otherwise(pl.col("home_skaters_on"))
    state = (pl.when((own_n == 5) & (opp_n == 5)).then(pl.lit("5v5"))
             .when((own_n == 5) & (opp_n == 4)).then(pl.lit("PP")))
    own = pl.when(pl.col("is_home_event")).then(pl.col("home_skater_ids")).otherwise(pl.col("away_skater_ids"))
    rows = (
        g.with_columns(state.alias("state"), own.alias("player_id"))
        .filter(pl.col("state").is_not_null() & ~pl.col("home_net_empty") & ~pl.col("away_net_empty"))
        .select("season", "game_id", "event_idx", "game_date", "state", "player_id",
                "player_1_id", "player_2_id", "player_3_id")
        .explode("player_id")
        .drop_nulls("player_id")
    )
    outcome = (pl.when(pl.col("player_id") == pl.col("player_1_id")).then(pl.lit("G"))
               .when(pl.col("player_id") == pl.col("player_2_id")).then(pl.lit("A1"))
               .when(pl.col("player_id") == pl.col("player_3_id")).then(pl.lit("A2"))
               .otherwise(pl.lit("none")))
    return rows.with_columns(outcome.alias("outcome")).drop("player_1_id", "player_2_id", "player_3_id")


def history_counts(rows: pl.DataFrame, history: pl.DataFrame) -> pl.DataFrame:
    """Add each skater's weighted outcome counts before the goal: ``h_G, h_A1, h_A2, h_none, h_n``.

    ``history`` holds earlier seasons' rows with a ``w`` column (the season weight); the current
    season's earlier goals count at weight 1 (strictly before this game, so same-game goals
    don't leak).
    """
    onehot = [(pl.col("outcome") == o).cast(pl.Float64).alias(f"h_{o}") for o in OUTCOMES]
    past = (history.group_by("player_id", "state")
            .agg(*[((pl.col("outcome") == o) * pl.col("w")).sum().alias(f"h_{o}") for o in OUTCOMES]))
    per_game = (rows.with_columns(onehot)
                .group_by("player_id", "state", "game_id", "game_date")
                .agg(*[pl.col(f"h_{o}").sum() for o in OUTCOMES])
                .sort("game_date", "game_id")
                .with_columns(*[(pl.col(f"h_{o}").cum_sum() - pl.col(f"h_{o}")).over("player_id", "state").alias(f"s_{o}")
                                for o in OUTCOMES])
                .select("player_id", "state", "game_id", *[f"s_{o}" for o in OUTCOMES]))
    out = rows.join(per_game, on=["player_id", "state", "game_id"], how="left").join(
        past, on=["player_id", "state"], how="left")
    return out.with_columns(
        *[(pl.col(f"h_{o}").fill_null(0) + pl.col(f"s_{o}").fill_null(0)).alias(f"h_{o}") for o in OUTCOMES]
    ).drop([f"s_{o}" for o in OUTCOMES]).with_columns(pl.sum_horizontal([f"h_{o}" for o in OUTCOMES]).alias("h_n"))


def attribution_rows(store: Store, seasons: list[int]) -> pl.DataFrame:
    """Every evaluable skater-goal of ``seasons`` with history counts, position and prior style."""
    years = sorted({config.season_start_year(s) for s in seasons})
    need = sorted({y - k for y in years for k in range(len(HISTORY_WEIGHTS) + 1)})
    by_season = {}
    for y in need:
        ev = store.get_parquet(keys.events(config.season_id(y)))
        if ev is not None:
            by_season[y] = goal_rows(ev)
    players = store.read_parquet_required(keys.PLAYERS).select(
        "player_id", (pl.col("position") == "D").alias("is_d"))
    out = []
    for y in years:
        if y not in by_season:
            continue
        hist = [by_season[y - 1 - i].with_columns(pl.lit(w).alias("w"))
                for i, w in enumerate(HISTORY_WEIGHTS) if (y - 1 - i) in by_season]
        history = pl.concat(hist) if hist else by_season[y].head(0).with_columns(pl.lit(0.0).alias("w"))
        rows = history_counts(by_season[y], history).join(players, on="player_id", how="left")
        rows = attach_style(rows.with_columns(pl.col("is_d").fill_null(False)), style_inputs(store, config.season_id(y)))
        out.append(rows)
        logger.info("attribution rows %s: %d", config.season_id(y), rows.height)
    return pl.concat(out, how="diagonal_relaxed")


#: Prior models: ``position`` (F/D only), ``shooter`` (+ the shot-volume axes only, i.e. what
#: any props model would use), ``archetype`` (+ forward archetype probabilities), ``axes``
#: (+ every F and D axis), ``style`` (+ both).
PRIOR_MODELS = ("position", "shooter", "archetype", "axes", "style")


def _prior_features(df: pl.DataFrame, model: str, style_cols: list[str]) -> np.ndarray:
    cols = [df["is_d"].cast(pl.Float64).to_numpy()]
    if model == "shooter":
        use = ("F_shooter", "D_shooter")
    else:
        use = {"position": (), "archetype": ("p_",), "axes": ("F_", "D_"), "style": ("p_", "F_", "D_")}[model]
    cols += [df[c].to_numpy().astype(float) for c in style_cols if c.startswith(use)]
    return np.column_stack(cols)


def _posterior(df: pl.DataFrame, prior: np.ndarray, k: float) -> np.ndarray:
    counts = np.column_stack([df[f"h_{o}"].to_numpy() for o in OUTCOMES])
    n = df["h_n"].to_numpy()[:, None]
    return (counts + k * prior) / (n + k)


def _log_loss(p: np.ndarray, y: np.ndarray) -> np.ndarray:
    return -np.log(np.clip(p[np.arange(len(y)), y], 1e-12, 1.0))


def attribution_test(rows: pl.DataFrame, eval_seasons: list[int]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Leave-one-season-out log loss for the ``position`` and ``style`` priors, by state.

    Returns ``(by_season, by_history)``: per (state, season) the log loss of each
    :data:`PRIOR_MODELS` prior, its tuned ``k`` and its gain over ``position``; and the pooled
    gains by history-size bucket.
    """
    style_cols = [c for c in rows.columns if c.startswith(("F_", "D_", "p_"))]
    y_all = rows["outcome"].replace_strict({o: i for i, o in enumerate(OUTCOMES)}, return_dtype=pl.Int64).to_numpy()
    rows = rows.with_columns(pl.Series("_y", y_all))
    res, scored = [], []
    for state in ("5v5", "PP"):
        st = rows.filter(pl.col("state") == state)
        for season in eval_seasons:
            train, test = st.filter(pl.col("season") != season), st.filter(pl.col("season") == season)
            if test.is_empty():
                continue
            out = {"state": state, "season": season, "n": test.height}
            for model in PRIOR_MODELS:
                lr = LogisticRegression(max_iter=500, C=1.0)
                lr.fit(_prior_features(train, model, style_cols), train["_y"].to_numpy())
                p_train = lr.predict_proba(_prior_features(train, model, style_cols))
                ll = {k: _log_loss(_posterior(train, p_train, k), train["_y"].to_numpy()).mean() for k in K_GRID}
                k = min(ll, key=ll.get)
                p_test = _posterior(test, lr.predict_proba(_prior_features(test, model, style_cols)), k)
                loss = _log_loss(p_test, test["_y"].to_numpy())
                out[f"ll_{model}"], out[f"k_{model}"] = float(loss.mean()), k
                test = test.with_columns(pl.Series(f"loss_{model}", loss))
            for model in PRIOR_MODELS[1:]:
                out[f"gain_{model}"] = 1 - out[f"ll_{model}"] / out["ll_position"]
            res.append(out)
            scored.append(test.select("state", "season", "h_n", "has_style", *[f"loss_{m}" for m in PRIOR_MODELS]))
            logger.info("attribution %s %s: %s", state, season, out)
    sc = pl.concat(scored).with_columns(
        pl.col("h_n").cut(list(HISTORY_BUCKETS[1:]), left_closed=True).alias("history"))
    by_hist = (sc.group_by("state", "history", "has_style")
               .agg(pl.len().alias("n"), *[pl.col(f"loss_{m}").mean() for m in PRIOR_MODELS])
               .with_columns(*[(1 - pl.col(f"loss_{m}") / pl.col("loss_position")).alias(f"gain_{m}")
                               for m in PRIOR_MODELS[1:]])
               .sort("state", "has_style", "history"))
    return pl.DataFrame(res), by_hist


# --- 2. line chemistry ------------------------------------------------------------------------
#
# Each 5v5 attack row (one stint seen from one team; :func:`nhl.usage.onice.stint_predictions`)
# has ``resid`` = actual xG/60 − the point-in-time additive RAPM prediction. Feature sets, for
# the attacking unit (``att``) and the defending unit (``def``):
#
# * ``main``: intercept, the additive sums ``o_sum`` / ``d_sum``, forward counts;
# * ``style``: + each unit's summed forward archetype probabilities and summed F / D axes
#   (does the additive rating miss style altogether?);
# * ``chemistry``: + pairwise archetype co-occurrence within each unit's forwards (15 pairs:
#   Σ over forward pairs of p_i[a]·p_j[b]) and the within-unit spread (sd) of every F axis
#   among forwards and D axis among defencemen (are mixed or uniform lines better than
#   their parts?).
#
# **Gate:** ``chemistry`` beats ``style`` on held-out weighted MSE in most seasons by more
# than noise. Then archetype mix would go into lineup-level projections.

CHEM_SETS = ("main", "style", "chemistry")


def _unit_features(rows: pl.DataFrame, side: str, style: pl.DataFrame, positions: pl.DataFrame) -> pl.DataFrame:
    """Per attack row ``_i``: unit features for the ``offence`` or ``defence`` list."""
    tag = "att" if side == "offence" else "def"
    cols = style_columns(style)
    p_cols = [c for c in cols if c.startswith("p_")]
    f_axes = [f"F_{a}" for a in AXES["F"]]
    d_axes = [f"D_{a}" for a in AXES["D"]]
    sk = (rows.select("_i", pl.col(side).alias("player_id")).explode("player_id").drop_nulls("player_id")
          .join(positions, on="player_id", how="left").with_columns(pl.col("is_d").fill_null(False)))
    sk = attach_style(sk, style)
    fwd, dmen = ~pl.col("is_d"), pl.col("is_d")
    pairs = [(a, b) for i, a in enumerate(p_cols) for b in p_cols[i:]]
    agg = [
        fwd.sum().cast(pl.Float64).alias(f"{tag}_nf"),
        *[pl.col(c).filter(fwd).sum().alias(f"{tag}_S_{c}") for c in p_cols],
        *[(pl.col(a) * pl.col(b)).filter(fwd).sum().alias(f"{tag}_Q_{a}_{b}") for a, b in pairs],
        *[pl.col(c).filter(fwd).sum().alias(f"{tag}_sum_{c}") for c in f_axes],
        *[pl.col(c).filter(dmen).sum().alias(f"{tag}_sum_{c}") for c in d_axes],
        *[pl.col(c).filter(fwd).std(ddof=0).fill_null(0).alias(f"{tag}_sd_{c}") for c in f_axes],
        *[pl.col(c).filter(dmen).std(ddof=0).fill_null(0).alias(f"{tag}_sd_{c}") for c in d_axes],
    ]
    u = sk.group_by("_i").agg(agg)
    # Σ_{i<j} p_i[a] p_j[b] (both orders for a ≠ b) = S_a S_b − Q_ab; for a = b it's (S_a² − Q_aa) / 2.
    pair_exprs = []
    for a, b in pairs:
        prod = pl.col(f"{tag}_S_{a}") * pl.col(f"{tag}_S_{b}") - pl.col(f"{tag}_Q_{a}_{b}")
        pair_exprs.append((prod / 2 if a == b else prod).alias(f"{tag}_pair_{a}_{b}"))
    return u.with_columns(pair_exprs).drop([c for c in u.columns if c.startswith((f"{tag}_Q_",))])


def chemistry_frame(preds: pl.DataFrame, style: pl.DataFrame, positions: pl.DataFrame) -> pl.DataFrame:
    """``preds`` (with ``_i``, ``offence``, ``defence``, ``o_sum``, ``d_sum``, ``resid``,
    ``duration_s``) plus both units' features."""
    base = preds.select("_i", "duration_s", "resid", "o_sum", "d_sum", "offence", "defence")
    out = base
    for side in ("offence", "defence"):
        out = out.join(_unit_features(base, side, style, positions), on="_i", how="left")
    return out.drop("offence", "defence").fill_null(0.0)


def chem_columns(frame: pl.DataFrame, which: str) -> list[str]:
    """Feature columns of a :func:`chemistry_frame` for one feature set (intercept added later).

    One archetype is the reference in the summed probabilities, and the pair terms drop that
    archetype's pairs, so the sets stay full rank.
    """
    cols = ["o_sum", "d_sum", "att_nf", "def_nf"]
    if which in ("style", "chemistry"):
        s_cols = sorted(c for c in frame.columns if c.startswith(("att_S_", "def_S_")))
        ref = s_cols[0].split("_S_", 1)[1]
        cols += [c for c in s_cols if not c.endswith(ref)]
        cols += [c for c in frame.columns if c.startswith(("att_sum_", "def_sum_"))]
    if which == "chemistry":
        cols += [c for c in frame.columns if c.startswith(("att_pair_", "def_pair_")) and ref not in c]
        cols += [c for c in frame.columns if c.startswith(("att_sd_", "def_sd_"))]
    return cols


def chem_normals(frame: pl.DataFrame):
    """Normal equations per feature set (and ``zero``) for one season's chemistry frame."""
    from nhl.usage.matchups import Normal

    w = frame["duration_s"].to_numpy().astype(float)
    y = frame["resid"].to_numpy()
    out = {}
    for which in CHEM_SETS:
        x = np.column_stack([np.ones(frame.height)] + [frame[c].to_numpy().astype(float) for c in chem_columns(frame, which)])
        xw = x * w[:, None]
        out[which] = Normal(xw.T @ x, xw.T @ y, float(np.sum(w * y * y)), float(w.sum()))
    out["zero"] = Normal(np.zeros((1, 1)), np.zeros(1), float(np.sum(w * y * y)), float(w.sum()))
    return out


def chemistry_test(normals: dict, ridge: float = 1e3) -> pl.DataFrame:
    """Leave-one-season-out held-out weighted MSE per feature set, with gains:
    ``gain_style`` (style vs main) and ``gain_chemistry`` (chemistry vs style)."""
    from nhl.usage.matchups import Normal

    out = []
    for season in sorted(normals):
        row = {"season": season, "weight_h": normals[season]["zero"].w / 3600}
        for which in CHEM_SETS:
            test = normals[season][which]
            train = sum((normals[s][which] for s in normals if s != season),
                        start=Normal(np.zeros_like(test.xtx), np.zeros_like(test.xty), 0.0, 0.0))
            row[f"mse_{which}"] = test.sse(train.solve(ridge)) / test.w
        out.append(row)
    return pl.DataFrame(out).with_columns(
        (1 - pl.col("mse_style") / pl.col("mse_main")).alias("gain_style"),
        (1 - pl.col("mse_chemistry") / pl.col("mse_style")).alias("gain_chemistry"),
    )


def chemistry_season(store: Store, season: int, positions: pl.DataFrame):
    """Normal equations for one season (None when it has no rating snapshots)."""
    from nhl.usage.onice import stint_predictions

    preds = stint_predictions(store, season)
    if preds.is_empty():
        return None
    return chem_normals(chemistry_frame(preds, style_inputs(store, season), positions))


def positions_table(store: Store) -> pl.DataFrame:
    """``player_id, is_d`` from the player table."""
    return store.read_parquet_required(keys.PLAYERS).select("player_id", (pl.col("position") == "D").alias("is_d"))


# --- 3. aging ---------------------------------------------------------------------------------
#
# The production EV ratings age each player's prior with one league curve per side
# (``ratings/ev/age_curve.json``). The test adds a style-dependent shift
# ``Σ_j θ_j f_ij`` to the prior mean of every rated player-side entering a season, where the
# features ``f`` are side-specific and come in sets (each with an ``× (age − 27) / 5`` slope):
#
# * ``pooled``: 1, a, a² (re-tunes the league curve; the bar the others must beat);
# * ``position``: + is_d;
# * ``archetype``: + forward archetype probabilities;
# * ``axes``: + F and D axes.
#
# Scoring is M3's chain test (fit to Dec 31 with the aged prior, score the rest of the season
# by weighted squared xG/60 error). The fit's coefficients are linear in the prior means, so
# the late-season SSE is quadratic in θ: SSE(θ) = SSE₀ − 2θᵀc + θᵀAθ, with
# M = H⁻¹ diag(prior precision) F, A = MᵀG_late M and c = Mᵀ(r_late − G_late b₀). Each season
# contributes (A, c); θ is fitted on the other seasons in closed form, so leave one season out
# is exact and needs one early-season solve per season.

AGING_SETS = ("pooled", "position", "archetype", "axes")


def _aging_features(players: pl.DataFrame, which: str, p_cols: list[str]) -> tuple[np.ndarray, list[str]]:
    """Features per prior row (``player_id, side, age, is_d`` + style), side-specific columns."""
    a = ((players["age"].to_numpy() - 27.0) / 5.0)
    base: dict[str, np.ndarray] = {"one": np.ones(len(a)), "a": a, "a2": a * a}
    if which != "pooled":
        base["is_d"] = players["is_d"].cast(pl.Float64).to_numpy()
    if which == "archetype":
        base.update({c: players[c].to_numpy() for c in p_cols[1:]})  # first archetype is the reference
    if which == "axes":
        base.update({c: players[c].to_numpy() for c in players.columns if c.startswith(("F_", "D_"))})
    cols, names = [], []
    side = players["side"].to_numpy()
    for s in ("O", "D"):
        on = (side == s).astype(float)
        for n, v in base.items():
            cols.append(on * v)
            names.append(f"{s}:{n}")
            if n not in ("one", "a", "a2"):
                cols.append(on * v * a)
                names.append(f"{s}:{n}*a")
    return np.column_stack(cols), names


def aging_season(store: Store, season: int) -> dict | None:
    """(A, c, SSE₀, weight) per feature set for one season, plus the no-aging SSE."""
    from datetime import date

    from nhl.ratings import rapm

    prior = store.get_parquet(rapm.prior_key("EV", season))
    if prior is None:
        return None
    start = config.season_start_year(season)
    curve = rapm.load_curve(store, "EV")
    ages = rapm._ages(store, start)
    design = rapm.season_design(store, season, "EV")
    mask = (design.rows["game_date"] < date(start + 1, 1, 1)).to_numpy()
    early, late = rapm.normal_equations(design, mask), rapm.normal_equations(design, ~mask)
    hyper, defence = rapm.Hyper(), rapm.defence_ids(store)
    k = len(early.columns)

    def solve(pr: pl.DataFrame):
        prec, mean, big = rapm._penalties(early, pr, hyper, defence)
        h = early.gram.copy()
        h[:k, :k] += np.diag(prec) + big
        h[k, k] += 1e-6
        rhs = early.rhs.copy()
        rhs[:k] += prec * mean
        return h, prec, np.linalg.solve(h, rhs)

    def sse(b: np.ndarray) -> float:
        return float(late.yy - 2 * b @ late.rhs + b @ late.gram @ b)

    aged = rapm.age_prior(prior, ages, curve)
    h, prec, b0 = solve(aged)
    _, _, b_none = solve(prior)
    # Prior rows in design-column order (players without a prior row are newcomers: no shift).
    col_index = {c: i for i, c in enumerate(early.columns)}
    style = style_inputs(store, season)
    p_cols = sorted(c for c in style.columns if c.startswith("p_"))
    rows = (aged.select("player_id", "side")
            .with_columns(pl.concat_str(pl.col("side"), pl.lit(":"), pl.col("player_id").cast(pl.Utf8)).alias("term"))
            .filter(pl.col("term").is_in(list(col_index)))
            .join(ages, on="player_id", how="left").with_columns(pl.col("age").fill_null(27.0))
            .join(positions_table(store), on="player_id", how="left").with_columns(pl.col("is_d").fill_null(False)))
    rows = attach_style(rows, style)
    idx = np.array([col_index[t] for t in rows["term"]])
    resid = late.rhs - late.gram @ b0
    out = {"season": season, "sse0": sse(b0), "sse_none": sse(b_none), "weight": late.weight,
           "early_share": early.weight / (early.weight + late.weight), "sets": {}}
    for which in AGING_SETS:
        f, names = _aging_features(rows, which, p_cols)
        full = np.zeros((k + 1, f.shape[1]))
        full[idx] = f * prec[idx][:, None]
        m = np.linalg.solve(h, full)
        out["sets"][which] = {"A": m.T @ late.gram @ m, "c": m.T @ resid, "names": names}
    logger.info("aging %s: sse0 %.1f none %.1f", season, out["sse0"], out["sse_none"])
    return out


#: Seasons whose pre-Jan-1 span holds less than this share of the season's 5v5 time are
#: dropped: 2012-13 and 2020-21 started in January, so their "early" fit is the prior alone
#: and the level of every prior mean dominates θ.
MIN_EARLY_SHARE = 1 / 3


def aging_test(seasons: list[dict], ridge: float = 1e-3) -> tuple[pl.DataFrame, dict[str, list[tuple[str, float]]]]:
    """Leave-one-season-out late-season MSE for each aging set; ``gain_*`` is relative to the
    stored league curve. Also returns the all-season θ per set (for the report)."""
    seasons = [s for s in seasons if s.get("early_share", 1.0) >= MIN_EARLY_SHARE]
    out = []
    for i, s in enumerate(seasons):
        row = {"season": s["season"], "mse_curve": s["sse0"] / s["weight"], "mse_none": s["sse_none"] / s["weight"]}
        for which in AGING_SETS:
            a = sum(t["sets"][which]["A"] for j, t in enumerate(seasons) if j != i)
            c = sum(t["sets"][which]["c"] for j, t in enumerate(seasons) if j != i)
            lam = ridge * np.trace(a) / len(c)
            theta = np.linalg.solve(a + lam * np.eye(len(c)), c)
            test = s["sets"][which]
            row[f"mse_{which}"] = (s["sse0"] - 2 * theta @ test["c"] + theta @ test["A"] @ theta) / s["weight"]
        out.append(row)
    df = pl.DataFrame(out).with_columns(
        *[(1 - pl.col(f"mse_{w}") / pl.col("mse_curve")).alias(f"gain_{w}") for w in AGING_SETS],
        (1 - pl.col("mse_curve") / pl.col("mse_none")).alias("gain_curve_vs_none"),
    )
    thetas = {}
    for which in AGING_SETS:
        a = sum(t["sets"][which]["A"] for t in seasons)
        c = sum(t["sets"][which]["c"] for t in seasons)
        theta = np.linalg.solve(a + ridge * np.trace(a) / len(c) * np.eye(len(c)), c)
        thetas[which] = list(zip(seasons[0]["sets"][which]["names"], theta.tolist()))
    return df, thetas


# --- driver and report ------------------------------------------------------------------------


def run(store: Store, seasons: list[int]) -> dict[str, pl.DataFrame]:
    """All three tests over ``seasons`` (each test uses the seasons it has inputs for)."""
    rows = attribution_rows(store, seasons)
    attr, attr_hist = attribution_test(rows, seasons)

    positions = positions_table(store)
    normals = {s: n for s in seasons if config.season_start_year(s) >= 2015
               and (n := chemistry_season(store, s, positions)) is not None}
    chem = chemistry_test(normals, ridge=1e4)

    aging_rows = [r for s in seasons if (r := aging_season(store, s)) is not None]
    aging, thetas = aging_test(aging_rows, ridge=1e-2)
    theta = pl.DataFrame([{"set": w, "term": n, "theta": v} for w, t in thetas.items() for n, v in t])
    return {"attribution": attr, "attribution_history": attr_hist, "chemistry": chem,
            "aging": aging, "aging_theta": theta}


def _md(df: pl.DataFrame, digits: int = 4) -> str:
    from nhl.usage.matchups import _md as md

    return md(df, digits)


def write_report(res: dict[str, pl.DataFrame], path) -> str:
    """Numbers section of ``docs/reports/archetypes.md`` (the narrative is written by hand)."""
    from pathlib import Path

    a = res["attribution"]
    gains = [c for c in a.columns if c.startswith("gain_")]
    attr_sum = a.group_by("state").agg(
        pl.len().alias("seasons"),
        *[(pl.col(g).mean() * 100).alias(f"{g}_pct") for g in gains],
        *[(pl.col(g) > 0).sum().alias(f"{g}_pos") for g in gains],
    ).sort("state")
    beyond = a.with_columns((1 - pl.col("ll_axes") / pl.col("ll_shooter")).alias("v")).group_by("state").agg(
        (pl.col("v").mean() * 100).alias("axes_vs_shooter_pct"), (pl.col("v") > 0).sum().alias("seasons_pos")).sort("state")
    c = res["chemistry"]
    g = res["aging"]
    aging_rel = g.select(
        "season",
        *[(pl.col(f"gain_{w}") * 1e4).alias(f"{w}_vs_curve_bp") for w in AGING_SETS],
        ((1 - pl.col("mse_archetype") / pl.col("mse_position")) * 1e4).alias("archetype_vs_position_bp"),
        ((1 - pl.col("mse_axes") / pl.col("mse_position")) * 1e4).alias("axes_vs_position_bp"),
    )
    pos_theta = res["aging_theta"].filter(pl.col("set") == "position").select("term", "theta")
    text = "\n\n".join([
        "# Archetypes phase C: numbers",
        "Generated by `nhl archetypes-tests`. Gains are relative improvements (positive = better).",
        "## 1. Point attribution (log loss per skater-goal, leave one season out)",
        _md(attr_sum),
        "Axes prior vs shot-volume-only prior:",
        _md(beyond),
        "By weighted history size (on-ice goals for before the game):",
        _md(res["attribution_history"].select("state", pl.col("history").cast(pl.Utf8), "has_style", "n",
                                               *[c for c in res["attribution_history"].columns if c.startswith("gain_")])),
        "Per season:",
        _md(a.select("state", "season", "n", *[c for c in a.columns if c.startswith(("ll_", "k_"))])),
        "## 2. Line chemistry (held-out weighted MSE of the stint residual, xG/60)",
        _md(c.select("season", "weight_h", "mse_main", (pl.col("gain_style") * 1e4).alias("style_vs_main_bp"),
                     (pl.col("gain_chemistry") * 1e4).alias("chemistry_vs_style_bp"))),
        f"Mean: style vs main {c['gain_style'].mean() * 1e4:.3f} bp ({(c['gain_style'] > 0).sum()} of {c.height} seasons "
        f"positive); chemistry vs style {c['gain_chemistry'].mean() * 1e4:.3f} bp "
        f"({(c['gain_chemistry'] > 0).sum()} of {c.height}).",
        "## 3. Aging (late-season weighted MSE after an early-season fit; bp = 0.01%)",
        _md(aging_rel, 3),
        "Means (bp): " + ", ".join(f"{col} {aging_rel[col].mean():.3f} ({(aging_rel[col] > 0).sum()}/{aging_rel.height})"
                                    for col in aging_rel.columns if col != "season"),
        "Position set, all seasons (θ in xG/60; `O:` offence priors, `D:` defence priors, a = (age − 27)/5):",
        _md(pos_theta),
    ]) + "\n"
    Path(path).write_text(text)
    return text
