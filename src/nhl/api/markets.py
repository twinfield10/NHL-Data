"""Per-game market views for the site: every book's current line, the consensus through the
day, the model at the market's line, and the regulation three-way moneyline.

Works from the raw live odds transitions (:func:`nhl.odds.store.load_live_odds`), pregame only
(nothing captured at or after puck drop), so a started game shows its closing view.

* Markets: ``moneyline``, ``puckline``, ``total`` (full game, main line) and ``moneyline_3way``
  (regulation; a game that reaches overtime is the draw).
* Sides: ``home`` / ``away`` (plus ``draw`` for the three-way) or ``over`` / ``under``.
* ``line``: the **home** handicap for puck lines, the total for totals, null otherwise.
* **Consensus** at a moment: each book's latest quote is devigged (the per-market method of
  :data:`nhl.betting.evaluate.METHOD`; multiplicative for the three-way) and averaged over the
  books hanging the most common line. A book quoting only some sides of a market is skipped.
"""

from __future__ import annotations

from datetime import datetime

import numpy as np
import polars as pl

from nhl.betting import blend, devig
from nhl.betting.evaluate import METHOD
from nhl.betting.lines import canonical_book
from nhl.sim import markets as sim_markets

SIDES: dict[str, tuple[str, ...]] = {
    "moneyline": ("home", "away"),
    "moneyline_3way": ("home", "draw", "away"),
    "puckline": ("home", "away"),
    "total": ("over", "under"),
}
FULL_GAME = ("moneyline", "puckline", "total")


def start_utc(game: dict) -> datetime | None:
    """Puck drop in UTC from the catalog's Eastern start time."""
    et = game.get("start_time_et")
    if not et:
        return None
    return pl.Series([et]).str.to_datetime().dt.replace_time_zone("America/New_York").dt.convert_time_zone("UTC")[0]


def game_quotes(odds: pl.DataFrame, game_id: int, start: datetime | None) -> pl.DataFrame:
    """One game's pregame quotes: ``book, market, side, line, price, captured_at``, oldest first."""
    if odds.is_empty():
        return pl.DataFrame(schema={"book": pl.String, "market": pl.String, "side": pl.String, "line": pl.Float64,
                                    "price": pl.Float64, "captured_at": pl.Datetime("us", "UTC")})
    main = ~pl.col("is_alternate").fill_null(False)
    which = ((pl.col("period") == "game") & pl.col("market").is_in(FULL_GAME)) | (
        (pl.col("period") == "reg") & (pl.col("market") == "moneyline_3way"))
    df = odds.filter((pl.col("game_id") == game_id) & main & which & pl.col("price").is_not_null())
    if start is not None:
        df = df.filter(pl.col("captured_at") < start)
    return df.select(canonical_book(pl.col("book")).alias("book"), "market", "side", "line", "price", "captured_at").sort("captured_at")


def _decimal(price: float) -> float:
    return 1 + 100 / -price if price < 0 else 1 + price / 100


def _book_view(market: str, quote: dict[str, tuple[float | None, float]]) -> dict | None:
    """A book's complete quote for one market (``line``, ``prices``, devigged ``fair``), or None."""
    sides = SIDES[market]
    if any(s not in quote for s in sides):
        return None
    prices = {s: quote[s][1] for s in sides}
    if market == "puckline":
        line = quote["home"][0]
        if line is None or quote["away"][0] is None or line != -quote["away"][0]:
            return None
    elif market == "total":
        line = quote["over"][0]
        if line is None or quote["under"][0] != line:
            return None
    else:
        line = None
    if market == "moneyline_3way":
        q = {s: 1 / _decimal(p) for s, p in prices.items()}
        total = sum(q.values())
        if not 1.0 < total < 1.3:  # books that publish only the two team prices sum well below 1
            return None
        fair = {s: v / total for s, v in q.items()}
    else:
        p1 = float(devig.fair(np.array([prices[sides[0]]]), np.array([prices[sides[1]]]), METHOD[market])[0])
        if np.isnan(p1):
            return None
        fair = {sides[0]: p1, sides[1]: 1 - p1}
    return {"line": line, "prices": prices, "fair": fair}


def _consensus(market: str, books: dict[str, dict]) -> dict | None:
    """Average fair probabilities and best price per side over the books at the most common line."""
    if not books:
        return None
    counts: dict[float | None, int] = {}
    for v in books.values():
        counts[v["line"]] = counts.get(v["line"], 0) + 1
    # Most books first; on a tie, the line closest to a coin flip is the main one.
    def balance(line):
        ps = [v["fair"][SIDES[market][0]] for v in books.values() if v["line"] == line]
        return abs(sum(ps) / len(ps) - 0.5)
    line = min(counts, key=lambda ln: (-counts[ln], balance(ln)))
    at = {b: v for b, v in books.items() if v["line"] == line}
    sides = SIDES[market]
    fair = {s: float(np.mean([v["fair"][s] for v in at.values()])) for s in sides}
    best = {}
    for s in sides:
        book, v = max(at.items(), key=lambda kv: _decimal(kv[1]["prices"][s]))
        best[s] = {"price": v["prices"][s], "book": book}
    return {"line": line, "books": len(at), "fair": fair, "best": best}


def replay(quotes: pl.DataFrame) -> tuple[list[dict], dict[str, dict[str, dict]]]:
    """Walk the quotes in time order.

    Returns:
        ``history``: one row per market each time its consensus changes (``t, market, line, books,
        fair, best``), and ``state``: each book's final complete quote per market
        (``{market: {book: {line, prices, fair, captured_at}}}``).
    """
    raw: dict[tuple[str, str], dict[str, tuple[float | None, float]]] = {}
    stamps: dict[tuple[str, str], datetime] = {}
    history: list[dict] = []
    last: dict[str, tuple] = {}
    for (t,), part in quotes.partition_by("captured_at", as_dict=True, maintain_order=True).items():
        touched = set()
        for r in part.iter_rows(named=True):
            key = (r["market"], r["book"])
            raw.setdefault(key, {})[r["side"]] = (r["line"], r["price"])
            stamps[key] = t
            touched.add(r["market"])
        for market in sorted(touched):
            books = {b: v for (m, b), q in raw.items() if m == market and (v := _book_view(m, q)) is not None}
            cons = _consensus(market, books)
            if cons is None:
                continue
            sig = (cons["line"], tuple(round(cons["fair"][s], 4) for s in SIDES[market]),
                   tuple(cons["best"][s]["price"] for s in SIDES[market]))
            if last.get(market) == sig:
                continue
            last[market] = sig
            history.append({"t": t, "market": market, **cons})
    state: dict[str, dict[str, dict]] = {}
    for (market, book), q in raw.items():
        v = _book_view(market, q)
        if v is not None:
            state.setdefault(market, {})[book] = {**v, "captured_at": stamps[(market, book)]}
    return history, state


def current(history: list[dict]) -> dict[str, dict]:
    """The latest consensus per market from :func:`replay`'s history."""
    out: dict[str, dict] = {}
    for row in history:
        out[row["market"]] = row
    return out


def model_probs(matrix: np.ndarray, market: str, line: float | None, blend_model: dict | None = None) -> dict[str, float] | None:
    """The model's probability of each side of ``market`` at ``line`` from one score matrix (a
    push on a whole-number line is taken out, as books refund it). The three-way uses the
    overtime calibration stored with ``blend_model`` (the raw simulator split without one)."""
    if market == "moneyline_3way":
        return blend.three_way(blend_model, "other", matrix, None)["model"]
    m = np.asarray(matrix, dtype=np.float64)[None]
    if market != "moneyline" and line is None:
        return None
    p, push = (float(x[0]) for x in sim_markets.line_probs(m, market, line))
    p = p / (1 - push) if push < 1 else p
    s1, s2 = SIDES[market]
    return {s1: p, s2: 1 - p}


def model_history(prices: pl.DataFrame, lines: dict[str, float | None], blend_model: dict | None = None) -> list[dict]:
    """The model's side probabilities in every pregame run (``t, market, line, p``), each market at
    its current consensus ``lines`` (a market without one is skipped, except the moneylines)."""
    out = []
    for r in prices.sort("as_of").iter_rows(named=True):
        for market in SIDES:
            if market in ("puckline", "total") and lines.get(market) is None:
                continue
            p = model_probs(np.asarray(r["score_matrix"]), market, lines.get(market), blend_model)
            if p is not None:
                out.append({"t": r["as_of"], "market": market, "line": lines.get(market), "p": p})
    return out


def three_way_card(matrix: np.ndarray | None, cons: dict | None, ml_cons: dict | None = None,
                   blend_model: dict | None = None, segment: str = "other") -> dict | None:
    """The regulation three-way for a game card: model, blend and market probabilities, best
    price and edge per side.

    The blend is the moneyline blend (against ``ml_cons``, the moneyline consensus) split by the
    calibrated overtime rate, so the three-way agrees with the two-way; the edge uses it, or the
    model where there is no moneyline market. It is still never flagged as a play: the split
    has no closing-line record of its own.
    """
    if matrix is None and cons is None:
        return None
    tw = (blend.three_way(blend_model, segment, matrix, ml_cons["fair"]["home"] if ml_cons else None)
          if matrix is not None else {"model": None, "blend": None})
    sides = []
    for s in SIDES["moneyline_3way"]:
        best = cons["best"][s] if cons else None
        p_model = tw["model"][s] if tw["model"] else None
        p_blend = tw["blend"][s] if tw["blend"] else None
        p = p_blend if p_blend is not None else p_model
        sides.append({
            "side": s, "p_model": p_model, "p_blend": p_blend, "p_market": cons["fair"][s] if cons else None,
            "price": best["price"] if best else None, "book": best["book"] if best else None,
            "edge": p * _decimal(best["price"]) - 1 if best and p is not None else None,
        })
    return {"sides": sides, "books": cons["books"] if cons else 0}
