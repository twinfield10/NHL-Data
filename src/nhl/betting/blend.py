"""Blend of model and market (M6 phase E): how much to trust the model against the price.

p = σ(c + a·logit(market fair) + b·logit(model)), one fit per market and **segment**
(November-February vs the rest of the season, where the model is weaker), on every season of
honest pregame history against the closing consensus. Stored at :func:`keys.blend_model` and
refit with ``nhl fit-blend`` (once a season, after ``nhl pregame-history``).

The same fit stores an **overtime** calibration on regular-season games, P(overtime) =
σ(c + b·|logit(P(home win))|): the closer the game, the likelier a tie after 60 minutes. The
simulator has too few overtimes (21.1% against 22.5%, 2016-2026), and its own game-to-game
P(overtime) adds nothing once the mismatch is known (leave-one-season-out log loss: mismatch
0.53255, simulator P(overtime) recalibrated 0.53285, both 0.53264, league rate 0.53294). The
regulation three-way uses it with the blended moneyline (:func:`three_way`).
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import numpy as np
import polars as pl

from nhl.betting import evaluate
from nhl.betting import lines as lines_mod
from nhl.sim import markets as sim_markets
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

NOV_FEB = (11, 12, 1, 2)
SEGMENTS = ("nov_feb", "other")


def segment_of(day: date) -> str:
    """``nov_feb`` or ``other`` for a game date."""
    return "nov_feb" if day.month in NOV_FEB else "other"


def fit(store: Store, seasons: list[int]) -> dict:
    """Fit and store the blend; returns the stored dict."""
    lines = pl.concat([lines_mod.build(store, s) for s in seasons])
    prices, used = evaluate.load_prices(store, seasons)
    games = store.read_parquet_required(keys.GAMES).filter(pl.col("is_final"))
    out_ot = fit_overtime(prices, games)
    data = evaluate.dataset(lines, prices, games.select("game_id", "home_score", "away_score")).join(
        games.select("game_id", "game_date"), on="game_id"
    )
    out: dict = {"fitted_at": datetime.now(timezone.utc).isoformat(), "seasons": seasons,
                 "price_sources": {str(k): v for k, v in used.items()}, "markets": {}}
    for market in evaluate.METHOD:
        out["markets"][market] = {}
        for seg in SEGMENTS:
            months = list(NOV_FEB)
            part = data.filter((pl.col("market") == market)
                               & (pl.col("game_date").dt.month().is_in(months) if seg == "nov_feb"
                                  else ~pl.col("game_date").dt.month().is_in(months)))
            f = evaluate.fit_logit(np.c_[evaluate._logit(part["p_market"].to_numpy()), evaluate._logit(part["p_model"].to_numpy())],
                                   part["y"].to_numpy())
            out["markets"][market][seg] = {"coef": f.coef.tolist(), "se": f.se.tolist(), "n": part.height}
            logger.info("blend %s %s: market %.3f model %.3f (n=%d)", market, seg, f.coef[1], f.coef[2], part.height)
    out["overtime"] = out_ot
    logger.info("overtime: c %.3f b %.3f (n=%d)", *out_ot["coef"], out_ot["n"])
    store.put_bytes(keys.blend_model(), json.dumps(out, indent=1).encode(), content_type="application/json")
    return out


def load(store: Store) -> dict:
    raw = store.get_bytes(keys.blend_model())
    if raw is None:
        raise FileNotFoundError("no blend model; run `nhl fit-blend` first")
    return json.loads(raw)


def apply(model: dict, market: str, segment: str, p_market: np.ndarray, p_model: np.ndarray) -> np.ndarray:
    """Blended probability of side 1."""
    c, a, b = model["markets"][market][segment]["coef"]
    z = c + a * evaluate._logit(np.asarray(p_market)) + b * evaluate._logit(np.asarray(p_model))
    return 1 / (1 + np.exp(-z))


def fit_overtime(prices: pl.DataFrame, games: pl.DataFrame) -> dict:
    """Fit P(overtime) on the model's mismatch, regular-season games (``coef`` = [c, b])."""
    data = prices.select("game_id", "p_home_win").drop_nulls().join(
        games.filter(pl.col("season_type") == "R").select("game_id", (pl.col("last_period") > 3).cast(pl.Float64).alias("y")),
        on="game_id")
    f = evaluate.fit_logit(np.abs(evaluate._logit(data["p_home_win"].to_numpy()))[:, None], data["y"].to_numpy())
    return {"coef": f.coef.tolist(), "se": f.se.tolist(), "n": data.height, "feature": "abs_logit_p_home_win"}


def overtime(model: dict | None, p_home_win: np.ndarray, p_overtime: np.ndarray) -> np.ndarray:
    """Calibrated P(overtime) for a game with home win probability ``p_home_win``; the
    simulator's ``p_overtime`` when the stored blend predates the calibration."""
    cal = (model or {}).get("overtime")
    if cal is None or cal.get("feature") != "abs_logit_p_home_win":
        return np.asarray(p_overtime, dtype=np.float64)
    c, b = cal["coef"]
    return 1 / (1 + np.exp(-(c + b * np.abs(evaluate._logit(np.asarray(p_home_win, dtype=np.float64))))))


def three_way(model: dict | None, segment: str, matrix: np.ndarray, p_market_ml: float | None) -> dict[str, dict[str, float] | None]:
    """The regulation three-way for one score matrix: ``model`` (the simulator's moneyline) and
    ``blend`` (the moneyline blended with the market's ``p_market_ml``, None without a market
    price or a stored blend), each split by its own calibrated overtime rate and the
    simulator's home share of overtime wins."""
    m = np.asarray(matrix, dtype=np.float64)[None]
    p_win = sim_markets.line_probs(m, "moneyline")[0]
    p_ot, p_home_ot = sim_markets.overtime_split(m)

    def split(p: np.ndarray) -> dict[str, float]:
        h, d, a = (float(x[0]) for x in sim_markets.split_three_way(p, overtime(model, p, p_ot), p_home_ot))
        return {"home": h, "draw": d, "away": a}

    blended = None
    if model is not None and p_market_ml is not None:
        blended = split(apply(model, "moneyline", segment, np.array([p_market_ml]), p_win))
    return {"model": split(p_win), "blend": blended}
