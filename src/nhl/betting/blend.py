"""Blend of model and market (M6 phase E): how much to trust the model against the price.

p = σ(c + a·logit(market fair) + b·logit(model)), one fit per market and **segment**
(November-February vs the rest of the season, where the model is weaker), on every season of
honest pregame history against the closing consensus. Stored at :func:`keys.blend_model` and
refit with ``nhl fit-blend`` (once a season, after ``nhl pregame-history``).
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

import numpy as np
import polars as pl

from nhl.betting import evaluate
from nhl.betting import lines as lines_mod
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
