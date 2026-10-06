"""Model vs market on history (M6 phase D).

1. **Information test.** Logistic regression of the result on logit(market fair probability)
   and logit(model probability), per market. A positive, stable model coefficient means the
   model knows something the market price doesn't.
2. **Blend.** p = σ(c + a·logit(market) + b·logit(model)), fitted per market on earlier
   seasons only and applied to the next (rolling).
3. **Betting simulation.** At the opening price, bet a side when its blended probability
   beats the price by the minimum edge; stake ¼ Kelly capped at 2% of bankroll. Measured by
   **CLV** (closing fair probability × decimal price taken − 1, only when the close is at the
   same line) and by ROI on results.

Model prices come from the simulator's market columns (``p_home_win``,
``p_home_minus_1_5``, ``p_away_minus_1_5``, ``p_over_{4.5..7.5}``), or, where the history
has score matrices (M6 phase C), at the exact book line: a whole-number total is priced as
P(over | no push), which is what a devigged book price means.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
from scipy.optimize import minimize

from nhl.betting import devig
from nhl.betting import lines as lines_mod
from nhl.sim import markets
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

#: Devig method per market, chosen on closing log loss 2015-2026 (differences ≤ 0.00015).
METHOD = {"moneyline": "power", "puckline": "multiplicative", "total": "shin"}
MIN_EDGE = {"moneyline": 0.02, "puckline": 0.03, "total": 0.03}
KELLY_FRACTION = 0.25
MAX_BET = 0.02
TOTAL_LINES = (4.5, 5.5, 6.5, 7.5)
#: A book's opening price counts toward "best available" only within this of the consensus.
OUTLIER = 0.03


def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-4, 1 - 1e-4)
    return np.log(p / (1 - p))


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1 / (1 + np.exp(-x))


def model_probs(prices: pl.DataFrame) -> pl.DataFrame:
    """Long table of model probabilities of side 1 (home / over) at each priced line:
    ``game_id, market, line, p_model``."""
    base = prices.select("game_id", "p_home_win", "p_home_minus_1_5", "p_away_minus_1_5",
                         *[f"p_over_{x}" for x in TOTAL_LINES])
    parts = [
        base.select("game_id", pl.lit("moneyline").alias("market"), pl.lit(None, dtype=pl.Float64).alias("line"), pl.col("p_home_win").alias("p_model")),
        base.select("game_id", pl.lit("puckline").alias("market"), pl.lit(-1.5).alias("line"), pl.col("p_home_minus_1_5").alias("p_model")),
        base.select("game_id", pl.lit("puckline").alias("market"), pl.lit(1.5).alias("line"), (1 - pl.col("p_away_minus_1_5")).alias("p_model")),
        *[base.select("game_id", pl.lit("total").alias("market"), pl.lit(x).alias("line"), pl.col(f"p_over_{x}").alias("p_model")) for x in TOTAL_LINES],
    ]
    return pl.concat(parts)


def market_probs(lines: pl.DataFrame, points: tuple[str, ...]) -> pl.DataFrame:
    """Consensus fair probability per (game, market) with each market's devig method."""
    return pl.concat([
        devig.consensus(lines.filter(pl.col("market") == m), method, points) for m, method in METHOD.items()
    ])


def attach_model(rows: pl.DataFrame, prices: pl.DataFrame) -> pl.DataFrame:
    """``rows`` (``game_id, market, line``) with ``p_model`` (side 1, given no push) and
    ``p_push``. With score matrices every line is priced exactly (a 6.0 total included);
    otherwise only the simulator's fixed lines (:func:`model_probs`) match."""
    if "score_matrix" not in prices.columns:
        return rows.join(model_probs(prices), on=["game_id", "market", "line"], how="inner", nulls_equal=True).with_columns(
            pl.lit(0.0).alias("p_push"))
    with_m = prices.filter(pl.col("score_matrix").is_not_null())
    without = prices.filter(pl.col("score_matrix").is_null())
    out = []
    if without.height:
        out.append(attach_model(rows.filter(pl.col("game_id").is_in(without["game_id"].implode())), without.drop("score_matrix")))
    mats = with_m.select("game_id", "score_matrix")
    for (market,), part in rows.join(mats, on="game_id", how="inner").partition_by("market", as_dict=True).items():
        m = part["score_matrix"].to_numpy()
        lines_ = None if market == "moneyline" else part["line"].to_numpy()
        win, push = markets.line_probs(m, market, lines_)
        p = np.where(push < 1, win / np.maximum(1 - push, 1e-9), 0.5)
        out.append(part.drop("score_matrix").with_columns(pl.Series("p_model", p), pl.Series("p_push", push)))
    return pl.concat(out) if out else rows.with_columns(pl.lit(None, dtype=pl.Float64).alias("p_model"), pl.lit(None, dtype=pl.Float64).alias("p_push"))


def dataset(lines: pl.DataFrame, prices: pl.DataFrame, scores: pl.DataFrame) -> pl.DataFrame:
    """Closing consensus joined to the model at the same line, graded: ``game_id, season,
    market, line, p_market, p_model, y`` (pushes and unpriced lines dropped, counted)."""
    close = market_probs(lines, ("close", "last")).filter(pl.col("game_id").is_in(prices["game_id"].implode()))
    j = attach_model(close, prices)
    missing = close.height - j.height
    if missing:
        logger.info("closing lines without a model price (whole-number totals without score matrices): %d", missing)
    graded = lines_mod.grade(j, scores)
    return graded.filter(pl.col("y").is_not_null()).rename({"p_fair": "p_market"})


@dataclass
class Logit:
    coef: np.ndarray
    se: np.ndarray


def fit_logit(x: np.ndarray, y: np.ndarray) -> Logit:
    """Unpenalised logistic regression with an intercept; standard errors from the Hessian."""
    X = np.c_[np.ones(len(y)), x]

    def nll(b: np.ndarray) -> tuple[float, np.ndarray]:
        z = X @ b
        p = _sigmoid(z)
        return float(-np.sum(y * z - np.logaddexp(0, z))), X.T @ (p - y)

    res = minimize(nll, np.zeros(X.shape[1]), jac=True, method="BFGS")
    p = _sigmoid(X @ res.x)
    hess = (X * (p * (1 - p))[:, None]).T @ X
    return Logit(res.x, np.sqrt(np.diag(np.linalg.inv(hess))))


def information_test(data: pl.DataFrame) -> pl.DataFrame:
    """Per market and season (and pooled): coefficients on logit(market) and logit(model)."""
    rows = []
    for (market,), part in data.partition_by("market", as_dict=True).items():
        for season in [None, *sorted(part["season"].unique().to_list())]:
            d = part if season is None else part.filter(pl.col("season") == season)
            y = d["y"].to_numpy()
            fit = fit_logit(np.c_[_logit(d["p_market"].to_numpy()), _logit(d["p_model"].to_numpy())], y)
            ll = lambda p: float(-np.mean(y * np.log(np.clip(p, 1e-4, 1)) + (1 - y) * np.log(np.clip(1 - p, 1e-4, 1))))
            rows.append({
                "market": market, "season": "all" if season is None else str(season), "n": d.height,
                "b_market": fit.coef[1], "se_market": fit.se[1], "b_model": fit.coef[2], "se_model": fit.se[2],
                "ll_market": ll(d["p_market"].to_numpy()), "ll_model": ll(d["p_model"].to_numpy()),
            })
    return pl.DataFrame(rows).sort("market", "season")


def rolling_blend(data: pl.DataFrame, min_train_seasons: int = 1) -> pl.DataFrame:
    """``data`` with ``p_blend``: per market, a blend fitted on all earlier seasons
    (seasons without enough history are dropped)."""
    out = []
    for (market,), part in data.partition_by("market", as_dict=True).items():
        seasons = sorted(part["season"].unique().to_list())
        for i, season in enumerate(seasons):
            if i < min_train_seasons:
                continue
            tr = part.filter(pl.col("season") < season)
            fit = fit_logit(np.c_[_logit(tr["p_market"].to_numpy()), _logit(tr["p_model"].to_numpy())], tr["y"].to_numpy())
            te = part.filter(pl.col("season") == season)
            z = fit.coef[0] + fit.coef[1] * _logit(te["p_market"].to_numpy()) + fit.coef[2] * _logit(te["p_model"].to_numpy())
            out.append(te.with_columns(pl.Series("p_blend", _sigmoid(z)), pl.lit(fit.coef[2]).alias("b_model")))
    return pl.concat(out) if out else data.head(0)


def blend_fit(data: pl.DataFrame, market: str, before: int) -> Logit | None:
    """Blend coefficients for ``market`` from seasons before ``before``."""
    tr = data.filter((pl.col("market") == market) & (pl.col("season") < before))
    if tr.height < 500:
        return None
    return fit_logit(np.c_[_logit(tr["p_market"].to_numpy()), _logit(tr["p_model"].to_numpy())], tr["y"].to_numpy())


def simulate_bets(lines: pl.DataFrame, prices: pl.DataFrame, scores: pl.DataFrame, data: pl.DataFrame,
                  min_edge: dict[str, float] | None = None) -> pl.DataFrame:
    """Bets at the opening price, one per (game, market, side) at most, with CLV and result.

    The blend for each season uses the opening consensus as its market input and is fitted on
    earlier seasons of ``data`` (closing consensus vs model), so nothing from the bet's own
    season is used. The bet takes the best opening price across books at the modal line.
    """
    min_edge = min_edge or MIN_EDGE
    opens = lines.filter(pl.col("point") == "open")
    if opens.is_empty():
        return pl.DataFrame()
    open_cons = market_probs(opens, ("open",)).rename({"p_fair": "p_open"})
    close_cons = market_probs(lines, ("close", "last")).select("game_id", "market", pl.col("line").alias("close_line"), pl.col("p_fair").alias("p_close"))
    # Best price across books, ignoring stale or bad quotes: a book whose fair probability is
    # more than OUTLIER from the opening consensus at that line doesn't count.
    per_book = opens.with_columns(pl.Series("p_book", devig.fair(opens["price_1"].to_numpy(), opens["price_2"].to_numpy())))
    best = (
        per_book.join(open_cons.select("game_id", "market", "line", "p_open"), on=["game_id", "market", "line"], nulls_equal=True)
        .filter((pl.col("p_book") - pl.col("p_open")).abs() <= OUTLIER)
        .group_by("game_id", "market", "line").agg(pl.col("price_1").max(), pl.col("price_2").max(), pl.len().alias("open_books"))
    )
    cand = (
        attach_model(open_cons, prices)
        .join(best, on=["game_id", "market", "line"], how="inner", nulls_equal=True)
        .join(close_cons, on=["game_id", "market"], how="left")
    )
    rows = []
    for (market, season), part in cand.partition_by("market", "season", as_dict=True).items():
        fit = blend_fit(data, market, season)
        if fit is None:
            continue
        z = fit.coef[0] + fit.coef[1] * _logit(part["p_open"].to_numpy()) + fit.coef[2] * _logit(part["p_model"].to_numpy())
        p1 = _sigmoid(z)
        for side, p, price in ((1, p1, part["price_1"].to_numpy()), (2, 1 - p1, part["price_2"].to_numpy())):
            d = devig.decimal(price)
            no_push = 1 - part["p_push"].to_numpy()
            ev = no_push * (p * d - 1)  # a push returns the stake
            take = ev >= min_edge[market]
            if not take.any():
                continue
            kelly = np.clip(KELLY_FRACTION * (p * d - 1) / (d - 1), 0, MAX_BET)
            sel = part.filter(pl.Series(take))
            same_line = (sel["close_line"].is_null() & sel["line"].is_null()) | (sel["close_line"] == sel["line"])
            p_close = sel["p_close"].to_numpy() if side == 1 else 1 - sel["p_close"].to_numpy()
            rows.append(sel.select("game_id", "season", "market", "line").with_columns(
                pl.lit(side).alias("side"), pl.Series("price", price[take]), pl.Series("open_books", part["open_books"].to_numpy()[take]), pl.Series("p_blend", p[take]),
                pl.Series("p_model", (part["p_model"].to_numpy() if side == 1 else 1 - part["p_model"].to_numpy())[take]),
                pl.Series("edge", ev[take]), pl.Series("stake", kelly[take]),
                pl.Series("clv", np.where(same_line.fill_null(False).to_numpy(), p_close * d[take] - 1, np.nan)),
            ))
    if not rows:
        return pl.DataFrame()
    bets = pl.concat(rows)
    graded = lines_mod.grade(bets, scores)
    win = pl.when(pl.col("side") == 1).then(pl.col("y")).otherwise(1 - pl.col("y"))
    dec = pl.when(pl.col("price") < 0).then(1 + 100 / -pl.col("price")).otherwise(1 + pl.col("price") / 100)
    return graded.with_columns(
        pl.when(pl.col("y").is_null()).then(0.0).when(win == 1).then(dec - 1).otherwise(-1.0).alias("return"),
    )


def bet_summary(bets: pl.DataFrame) -> pl.DataFrame:
    """Per market (and all): bets, mean edge, CLV (mean, share positive), flat and staked ROI."""
    if bets.is_empty():
        return pl.DataFrame()
    def agg(df: pl.DataFrame, label: str) -> dict:
        c = df.filter(pl.col("clv").is_not_nan())
        return {
            "market": label, "bets": df.height, "mean_edge": df["edge"].mean(),
            "clv_bets": c.height, "mean_clv": c["clv"].mean() if c.height else None,
            "clv_positive": (c["clv"] > 0).mean() if c.height else None,
            "roi_flat": df["return"].mean(), "roi_staked": (df["return"] * df["stake"]).sum() / df["stake"].sum(),
        }
    return pl.DataFrame([agg(bets, "all")] + [agg(p, m) for (m,), p in bets.partition_by("market", as_dict=True).items()])


def blend_out_of_sample(data: pl.DataFrame) -> pl.DataFrame:
    """Per market: closing-market vs rolling-blend log loss on seasons after the first."""
    b = rolling_blend(data)
    rows = []
    for (market,), part in b.partition_by("market", as_dict=True).items():
        y = part["y"].to_numpy()

        def ll(p: np.ndarray) -> float:
            p = np.clip(p, 1e-4, 1 - 1e-4)
            return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))
        rows.append({"market": market, "n": len(y), "ll_market": ll(part["p_market"].to_numpy()),
                     "ll_blend": ll(part["p_blend"].to_numpy())})
    return pl.DataFrame(rows).with_columns((pl.col("ll_market") - pl.col("ll_blend")).alias("gain")).sort("market")


def load_prices(store: Store, seasons: list[int]) -> tuple[pl.DataFrame, dict[int, str]]:
    """Model prices per season: the M5 ``pregame`` variant where it exists, else the M4
    actual-lineup backtest (labelled, since it knows the lineup and starter)."""
    frames, used = [], {}
    for season in seasons:
        hist = store.get_parquet(keys.pregame_history(season))
        if hist is not None:
            frames.append(hist)
            used[season] = "pregame + score matrix"
            continue
        pg = store.get_parquet(keys.pregame_backtest(season))
        if pg is not None:
            frames.append(pg.filter(pl.col("variant") == "pregame"))
            used[season] = "pregame"
            continue
        sim = store.get_parquet(keys.sim_backtest(season))
        if sim is not None:
            frames.append(sim)
            used[season] = "actual (M4)"
    return pl.concat(frames, how="diagonal_relaxed"), used


#: Seasonal stretches evaluated separately (2026-10-06: the model trails the close in April
#: and the playoffs, and matches or beats it November-February).
SEGMENTS = {"all games": None, "Nov-Feb": (11, 12, 1, 2)}


def evaluate_segment(lines: pl.DataFrame, prices: pl.DataFrame, scores: pl.DataFrame) -> dict[str, pl.DataFrame]:
    data = dataset(lines, prices, scores)
    return {"info": information_test(data), "oos": blend_out_of_sample(data),
            "bets": simulate_bets(lines, prices, scores, data)}


def write_report(path: Path, used: dict[int, str], segments: dict[str, dict[str, pl.DataFrame]]) -> str:
    """Markdown report: per segment, the information test, the out-of-sample blend and the
    bet simulation."""
    def table(df: pl.DataFrame) -> list[str]:
        cols = df.columns
        fmt = lambda v: f"{v:.4f}" if isinstance(v, float) else str(v)  # noqa: E731
        return ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols),
                *["| " + " | ".join(fmt(v) for v in r) + " |" for r in df.iter_rows()]]
    lines = [
        "# M6 model vs market",
        "",
        "Model prices by season: " + ", ".join(f"{s}: {v}" for s, v in sorted(used.items())) + ". "
        "Only pregame prices are honest; `actual (M4)` knows the lineup and starter, which the "
        "opening line doesn't. With score matrices every closing line is priced exactly "
        "(whole-number totals as P(over | no push)).",
        "",
    ]
    for label, seg in segments.items():
        bets = seg["bets"]
        lines += [
            f"## {label}", "",
            "### Information test (closing consensus vs model, pooled)", "",
            *table(seg["info"].filter(pl.col("season") == "all")), "",
            "### Out of sample: rolling blend vs the closing market", "",
            "Positive `gain` means the blend beats the close on later seasons.", "",
            *table(seg["oos"]), "",
            "### Betting at the open (¼ Kelly, 2% cap, best non-outlier price)", "",
            *(table(bet_summary(bets)) if not bets.is_empty() else ["No bets."]), "",
        ]
    text = "\n".join(lines)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return text


def run(store: Store, seasons: list[int], report: Path) -> str:
    """Build lines, evaluate model vs market for ``seasons`` per :data:`SEGMENTS`, write the report."""
    lines = pl.concat([lines_mod.build(store, s) for s in seasons])
    prices, used = load_prices(store, seasons)
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("is_final"))
    scores = games.select("game_id", "home_score", "away_score")
    segments = {}
    for label, months in SEGMENTS.items():
        keep = games if months is None else games.filter(pl.col("game_date").dt.month().is_in(list(months)))
        seg_lines = lines.filter(pl.col("game_id").is_in(keep["game_id"].implode()))
        segments[label] = evaluate_segment(seg_lines, prices, scores)
    return write_report(report, used, segments)
