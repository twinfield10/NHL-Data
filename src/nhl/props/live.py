"""Live player-prop projections and edges (M9 phase D).

**Projections** (:func:`project_day`): for each of the day's games, its latest pregame snapshot
(the score matrix and that run's projected lineup) plus each skater's rates from every game
logged before today (:func:`nhl.props.rates.rates_for_day`), through
:mod:`nhl.props.project`, then the backtest's logit calibration (:data:`nhl.props.project.CALIBRATION`). Stored per pregame run at ``pregame/props/{date}/{stamp}.parquet``
with the inputs (slot, PP unit, P(dressed), source), so a moved edge can be explained later.

**Edges** (:func:`compute`), for goals, assists, points, shots on goal, blocks and saves
over/unders and N+ ladders from every captured book (DraftKings via ESPN, FanDuel, LowVig,
4Casters). Shots, blocks and saves come from the team volume model (:mod:`nhl.props.volume`):

1. **Book probability** (P(over) at the book's line). A two-way market is devigged
   multiplicatively. A one-sided ladder rung is divided by 1 + that book's margin on the same
   stat, measured from its own two-way markets that day (:data:`DEFAULT_MARGIN` without
   any). A two-way pair whose implied probabilities sum below 1 is a broken quote (sides from
   different moments or markets) and is skipped.
2. **Market** = the median book probability for that player, stat and line, over the books
   quoting both sides when at least :data:`MIN_TWO_WAY_BOOKS` do (else over every book). A book
   more than :data:`OUTLIER` from it is treated as a bad quote and skipped.
3. **Blend** = logit average of model and market with weight :data:`MODEL_WEIGHT`. This is a
   placeholder until phase F fits it on graded props.
4. **Edge** = p × decimal − 1 per side; the best book per (game, player, stat, line, side).
5. **Flag** when the edge clears :data:`MIN_EDGE` and the bet is one the backtest can stand
   behind: price no longer than :data:`MAX_PRICE`, at least :data:`MIN_BOOKS` books in the
   market and :data:`MIN_TWO_WAY_BOOKS` of them quoting both sides, the skater projected to dress for certain (no game-time decision) with high lineup
   confidence, or the goalie at least :data:`MIN_P_START` likely to start.
6. **Stake**: ¼ Kelly on the 100-unit bankroll, at most :data:`MAX_BET_UNITS` per bet,
   and :data:`MAX_PLAYER_UNITS` per player-game, counting bets already in the props ledger.
   No daily cap while paper bets are being tracked (owner, 2026-10-10).

Every evaluated quote is snapshotted at ``pregame/props_edges/{date}/{stamp}.parquet``, and
newly flagged bets go to the props paper ledger (:mod:`nhl.props.ledger`).
"""

from __future__ import annotations

import fcntl
import logging
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl

from nhl.betting.edges import _games, last_pregame_prices
from nhl.odds.props import SEEN_KEY as PROP_SEEN_KEY
from nhl.odds.props import load_props
from nhl.odds.store import in_latest_poll, load_seen, still_listed
from nhl.props import project, rates, volume
from nhl.sources.common import stamp
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

EASTERN = ZoneInfo("America/New_York")
#: Book prop_type -> projection stat.
STATS = {"goals": "goals", "assists": "ast", "points": "points", "shots": "shots", "blocks": "blocks", "saves": "saves"}
#: Thresholds projected per stat (P(stat >= k)); a line above the top one isn't priced.
THRESHOLDS = {"goals": (1, 2, 3), "ast": (1, 2, 3), "points": (1, 2, 3, 4), "shots": tuple(range(1, 9)),
              "blocks": tuple(range(1, 6)), "saves": tuple(range(12, 46))}
#: Goalies projected to start at least this likely get a saves projection; at least
#: :data:`MIN_P_START` one that can be flagged (the prop is void if he doesn't start).
MIN_P_START_PROJECTED = 0.05
#: Bumped whenever the projection's contents change, so cached projections are rebuilt.
PROJECTION_VERSION = 3
MIN_P_START = 0.9
DEFAULT_MARGIN = 0.07
OUTLIER = 0.10
MODEL_WEIGHT = 0.5
MIN_EDGE = 0.05
MAX_PRICE = 400.0
MIN_BOOKS = 2
#: Books quoting both sides needed to flag a play; with this many, the consensus uses only them.
MIN_TWO_WAY_BOOKS = 2
BANKROLL_UNITS = 100.0
KELLY_FRACTION = 0.25
MAX_BET_UNITS = 0.5
MAX_PLAYER_UNITS = 1.0
#: Reporting floor for the edges snapshot: every best quote at or above it is kept.
SNAPSHOT_MIN_EDGE = -1.0
LOCK_PATH = "/tmp/nhl_data_props_edges.lock"


@contextmanager
def _lock():
    with open(LOCK_PATH, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _decimal(price: pl.Expr) -> pl.Expr:
    return pl.when(price < 0).then(1 + 100 / -price).otherwise(1 + price / 100)


def _implied(price: pl.Expr) -> pl.Expr:
    return 1 / _decimal(price)


# ------------------------------------------------------------------------- projections --
def _snapshot_inputs(store: Store, day: date) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame] | None:
    """Each game's last pregame prices, and the projected lineup and starting-goalie
    probabilities from the same run."""
    prices = last_pregame_prices(store, day)
    if prices is None or prices.is_empty():
        return None
    deps, goalies = [], []
    for (st,), part in prices.partition_by("stamp", as_dict=True).items():
        ids = part["game_id"].implode()
        dep = store.get_parquet(keys.pregame_lineups(day, st))
        if dep is not None:
            deps.append(dep.filter(pl.col("game_id").is_in(ids)).drop("as_of", "stamp", strict=False)
                        .with_columns(pl.lit(st).alias("pregame_stamp")))
        g = store.get_parquet(keys.pregame_goalies(day, st))
        if g is not None:
            goalies.append(g.filter(pl.col("game_id").is_in(ids)).select("game_id", pl.col("team_id").cast(pl.Int64), "player_id",
                                                                       "p_start").with_columns(pl.lit(st).alias("pregame_stamp")))
    if not deps:
        return None
    gdf = pl.concat(goalies, how="diagonal_relaxed") if goalies else pl.DataFrame(
        schema={"game_id": pl.Int64, "team_id": pl.Int64, "player_id": pl.Int64, "p_start": pl.Float64, "pregame_stamp": pl.String})
    return prices, pl.concat(deps, how="diagonal_relaxed"), gdf


def _team_volume(store: Store, season: int, day: date, prices: pl.DataFrame) -> pl.DataFrame:
    """Expected team shots on goal and blocks for ``day``'s games (``game_id, team_id,
    opp_team_id, mu_sog, mu_blk``), from every game logged before it (:mod:`nhl.props.volume`)."""
    cur, last = volume.team_logs(store, season), volume.team_logs(store, season - 10001)
    today = pl.concat([
        prices.select("game_id", pl.lit(day).alias("game_date"), pl.col(f"{a}_team_id").cast(pl.Int64).alias("team_id"),
                      pl.col(f"{b}_team_id").cast(pl.Int64).alias("opp_team_id"), pl.lit(a == "home").alias("is_home"))
        for a, b in (("home", "away"), ("away", "home"))
    ]).with_columns(*[pl.lit(0.0).alias(c) for c in ("sf", "sa", "cf", "blocks")])
    if not cur.is_empty():
        today = today.select(cur.columns).cast(cur.schema)
        cur = pl.concat([cur.filter(pl.col("game_date") < day), today])
    else:
        cur = today
    feat = volume.features(volume.team_rates(cur, last), prices.select("game_id", "home_team_id", "p_home_win")).filter(
        pl.col("game_id").is_in(prices["game_id"].implode()))
    return feat.select("game_id", "team_id", "opp_team_id", volume.expected(feat, "sog").alias("mu_sog"),
                       volume.expected(feat, "blk").alias("mu_blk"))


def project_day(store: Store, day: date, write: bool = True) -> pl.DataFrame:
    """Projections for every projected skater (goals, assists, points, shots, blocks) and every
    probable starting goalie (saves, conditional on starting) on ``day``'s games.

    Cached per set of pregame runs: the key's stamp is the newest pregame stamp involved, so a
    new pregame run (a lineup change, a new starter) makes a new projection.
    """
    got = _snapshot_inputs(store, day)
    if got is None:
        return pl.DataFrame()
    prices, dep, goalies = got
    st = str(prices["stamp"].max())
    # The version makes a code change (new markets, a model fix) rebuild the day's cache.
    key = keys.props_projections(day, f"{st}-v{PROJECTION_VERSION}")
    if (cached := store.get_parquet(key)) is not None:
        return cached
    season = int(store.read_parquet_required(keys.GAMES).filter(pl.col("game_id") == prices["game_id"][0])["season"][0])
    logs = {s: rates.bucket_logs(store, s) for s in (season, season - 10001, season - 20002)}
    pit, mix, team_f = rates.rates_for_day(store, season, day, dep, logs=logs)
    dists = project.team_goal_dists(prices.select("game_id", "home_team_id", "away_team_id", "score_matrix"))
    proj = project.calibrate(project.project_players(project.shares(dep, pit, mix, team_f=team_f, dep_power=project.DEP_POWER),
                                                     dists, thresholds=THRESHOLDS),
                             THRESHOLDS)
    # Shots and blocks: team volume x each skater's share (phase C).
    vol = _team_volume(store, season, day, prices)
    for stat, key_ in (("shots", "sog"), ("blocks", "blk")):
        sh = volume.player_shares(dep, pit, volume.stat_mix(logs[season - 10001], key_), key_, volume.DEP_POWER[key_])
        p = volume.player_props(sh, vol.select("game_id", "team_id", pl.col(f"mu_{key_}").alias("mu")), key_, THRESHOLDS[stat])
        p = p.rename({f"p_{key_}_{k}": f"p_{stat}_{k}" for k in THRESHOLDS[stat]} | {f"exp_{key_}": f"exp_{stat}"})
        proj = proj.join(p.filter(pl.col("player_id").is_not_null()).unique(["game_id", "player_id"])
                         .select("game_id", "player_id", f"exp_{stat}", *[f"p_{stat}_{k}" for k in THRESHOLDS[stat]]),
                         on=["game_id", "player_id"], how="left")
    info = dep.select("game_id", "player_id", "slot", "pp_unit", "pk_unit", "source", "confidence", "pregame_stamp")
    proj = proj.filter(pl.col("player_id").is_not_null()).join(info, on=["game_id", "player_id"], how="left")
    # Saves: every probable starter, conditional on his starting (a prop on a goalie who sits is void).
    starters = goalies.filter(pl.col("player_id").is_not_null() & (pl.col("p_start") >= MIN_P_START_PROJECTED)).join(
        vol.select("game_id", "team_id", "opp_team_id"), on=["game_id", "team_id"], how="inner").join(
        vol.select("game_id", pl.col("team_id").alias("opp_team_id"), pl.col("mu_sog").alias("mu_against")),
        on=["game_id", "opp_team_id"], how="inner").join(
        dists.select("game_id", pl.col("team_id").alias("opp_team_id"), pl.col("mean_goals").alias("goals_against")),
        on=["game_id", "opp_team_id"], how="inner")
    if starters.height:
        g = volume.saves_props(starters, THRESHOLDS["saves"]).with_columns(
            pl.lit("G").alias("position"), pl.col("p_start").alias("p_dressed"),
            pl.when(pl.col("p_start") >= MIN_P_START).then(pl.lit("high")).otherwise(pl.lit("low")).alias("confidence"),
            pl.lit("goalie").alias("source"))
        proj = pl.concat([proj, g.drop("opp_team_id", "mu_against", "goals_against")], how="diagonal_relaxed")
    proj = proj.with_columns(pl.lit(st).alias("stamp"))
    if write:
        store.put_parquet(key, proj)
    return proj


# ----------------------------------------------------------------------- market probs --
def market_probs(quotes: pl.DataFrame) -> pl.DataFrame:
    """P(over) per book quote, and the consensus per (game, player, stat, line).

    Args:
        quotes: Latest prop prices (``book, game_id, player_id, prop_type, line, side, price``),
            over/under sides only.

    Returns:
        One row per (book, game, player, stat, line) with ``price_over``, ``price_under``
        (either may be null), ``p_book`` (devigged P(over)), ``two_way``, ``p_market``,
        ``books``, ``two_way_books`` and ``outlier``.

    ``p_market`` is the median ``p_book`` of the books quoting both sides when at least
    :data:`MIN_TWO_WAY_BOOKS` do, else of every book. A one-sided quote is devigged with an assumed
    margin, so it can pull the consensus toward itself (an over-only +100 beside two-way +170 /
    −235 reads as a big overlay at +170); two-way pairs carry their own margin.
    """
    key = ["book", "game_id", "player_id", "prop_type", "line"]
    if quotes.is_empty():
        return pl.DataFrame(schema={"book": pl.String, "game_id": pl.Int64, "player_id": pl.Int64, "prop_type": pl.String,
                                    "line": pl.Float64, "price_over": pl.Float64, "price_under": pl.Float64,
                                    "two_way": pl.Boolean, "margin": pl.Float64, "p_book": pl.Float64, "p_market": pl.Float64,
                                    "books": pl.Int64, "two_way_books": pl.Int64, "outlier": pl.Boolean})
    quotes = quotes.with_columns(pl.col("price").cast(pl.Float64), pl.col("line").cast(pl.Float64))
    wide = quotes.pivot(on="side", index=key, values="price", aggregate_function="last")
    for side in ("over", "under"):
        if side not in wide.columns:
            wide = wide.with_columns(pl.lit(None, dtype=pl.Float64).alias(side))
    wide = wide.rename({"over": "price_over", "under": "price_under"}).with_columns(
        (pl.col("price_over").is_not_null() & pl.col("price_under").is_not_null()).alias("two_way"))
    io, iu = _implied(pl.col("price_over")), _implied(pl.col("price_under"))
    margins = wide.filter(pl.col("two_way")).group_by("book", "prop_type").agg((io + iu - 1).median().alias("margin"))
    wide = wide.join(margins, on=["book", "prop_type"], how="left").with_columns(
        pl.when(pl.col("two_way")).then(io / (io + iu))
        .when(pl.col("price_over").is_not_null()).then(io / (1 + pl.col("margin").fill_null(DEFAULT_MARGIN)))
        .otherwise(1 - iu / (1 + pl.col("margin").fill_null(DEFAULT_MARGIN)))
        .clip(0.001, 0.999).alias("p_book"))
    # A two-way pair whose sides sum below 1 is no real market (sides from different moments or
    # markets, e.g. a stale under beside a fresh over): it's left out of the consensus and never bet.
    broken = pl.col("two_way") & (io + iu < 1.0)
    wide = wide.with_columns(broken.fill_null(False).alias("_broken"))
    group = ["game_id", "player_id", "prop_type", "line"]
    good = wide.filter(~pl.col("_broken"))
    cons = good.group_by(group).agg(
        pl.col("p_book").median().alias("_p_all"), pl.col("p_book").filter(pl.col("two_way")).median().alias("_p_two"),
        pl.col("book").n_unique().alias("books"), pl.col("book").filter(pl.col("two_way")).n_unique().alias("two_way_books"),
    ).with_columns(
        pl.when(pl.col("two_way_books") >= MIN_TWO_WAY_BOOKS).then("_p_two").otherwise("_p_all").alias("p_market"),
    ).drop("_p_all", "_p_two")
    return wide.join(cons, on=group, how="left").with_columns(
        (pl.col("_broken") | ((pl.col("p_book") - pl.col("p_market")).abs() > OUTLIER)).fill_null(True).alias("outlier"),
        pl.col("books").fill_null(0).cast(pl.Int64), pl.col("two_way_books").fill_null(0).cast(pl.Int64)).drop("_broken")


def latest_quotes(store: Store, season: int, game_ids: list[int], cutoff: datetime | dict[int, datetime]) -> pl.DataFrame:
    """Each book's last over/under price per (game, player, stat, line, side) captured by ``cutoff``
    (one moment, or one per game such as its start time)."""
    props = load_props(store, season).filter(
        pl.col("game_id").is_in(game_ids) & pl.col("prop_type").is_in(list(STATS)) & pl.col("side").is_in(["over", "under"])
        & pl.col("player_id").is_not_null() & pl.col("line").is_not_null())
    if isinstance(cutoff, dict):
        limits = pl.DataFrame({"game_id": list(cutoff), "_cut": list(cutoff.values())},
                              schema={"game_id": pl.Int64, "_cut": pl.Datetime("us", "UTC")})
        props = props.join(limits, on="game_id").filter(pl.col("captured_at") <= pl.col("_cut")).drop("_cut")
    else:
        props = props.filter(pl.col("captured_at") <= cutoff)
    return props.sort("captured_at").group_by("book", "game_id", "player_id", "prop_type", "line", "side").last()


#: A book's closing prop price counts only if it listed the prop this close to the start.
#: FanDuel, DraftKings (via ESPN), 4Casters and Novig props poll every 5 minutes in the last
#: 90; LowVig's headless walk every 5-10, so 20 minutes allows one missed LowVig poll.
PROP_CLOSE_MAX_GAP = timedelta(minutes=20)


def drop_pulled(quotes: pl.DataFrame, seen: pl.DataFrame | None) -> pl.DataFrame:
    """Prop quotes whose book still listed them at its latest poll of the game
    (:func:`nhl.odds.store.in_latest_poll`): a prop taken down keeps its last stored price in
    the transitions, which must never read as a current price or a close."""
    return in_latest_poll(quotes, seen, PROP_SEEN_KEY)


def load_props_seen(store: Store, season: int) -> pl.DataFrame | None:
    """Every prop poller's seen table for ``season``."""
    return load_seen(store, keys.props_seen_prefix(season), PROP_SEEN_KEY)


def current_quotes(store: Store, season: int, game_ids: list[int], now: datetime) -> pl.DataFrame:
    """Each book's latest price per prop side as of ``now``, props it has taken down left out."""
    return drop_pulled(latest_quotes(store, season, game_ids, now), load_props_seen(store, season))


def closing_quotes(store: Store, season: int, starts: dict[int, datetime]) -> pl.DataFrame:
    """Each book's closing price per prop side: the last before each game's ``starts`` time, kept
    only if the book listed that prop at its last pregame poll of the game and that poll was
    within :data:`PROP_CLOSE_MAX_GAP` of the start (:func:`nhl.odds.store.still_listed`); a
    prop pulled earlier has no close."""
    quotes = latest_quotes(store, season, list(starts), starts)
    if quotes.is_empty():
        return quotes
    limits = pl.DataFrame({"game_id": list(starts), "start_time": list(starts.values())},
                          schema={"game_id": pl.Int64, "start_time": pl.Datetime("us", "UTC")})
    quotes = quotes.drop("start_time", strict=False).join(limits, on="game_id")
    seen = load_props_seen(store, season)
    return still_listed(drop_pulled(quotes, seen), seen, PROP_SEEN_KEY, PROP_CLOSE_MAX_GAP)


def _model_over(proj: pl.DataFrame) -> pl.DataFrame:
    """Long ``game_id, player_id, prop_type, line, p_model`` (P(over line)) from projections."""
    rows = []
    for prop_type, stat in STATS.items():
        for k in THRESHOLDS[stat]:
            if f"p_{stat}_{k}" not in proj.columns:  # an older cached projection
                continue
            rows.append(proj.select("game_id", "player_id", pl.lit(prop_type).alias("prop_type"),
                                    pl.lit(k - 0.5).alias("line"), pl.col(f"p_{stat}_{k}").alias("p_model")))
    # Skaters have no saves line and goalies no skater lines.
    return pl.concat(rows).drop_nulls("p_model")


def _logit(x: pl.Expr) -> pl.Expr:
    x = x.clip(1e-6, 1 - 1e-6)
    return (x / (1 - x)).log()


def price_quotes(probs: pl.DataFrame, proj: pl.DataFrame) -> pl.DataFrame:
    """Model × market × blend for every quote side; the best book per (game, player, stat, line, side)."""
    base = probs.filter(~pl.col("outlier")).join(_model_over(proj), on=["game_id", "player_id", "prop_type", "line"],
                                                 how="inner")
    z = MODEL_WEIGHT * _logit(pl.col("p_model")) + (1 - MODEL_WEIGHT) * _logit(pl.col("p_market"))
    base = base.with_columns((1 / (1 + (-z).exp())).alias("p_blend"))
    sides = []
    for side, col in (("over", "price_over"), ("under", "price_under")):
        flip = side == "under"
        p = (1 - pl.col("p_blend")) if flip else pl.col("p_blend")
        sides.append(base.filter(pl.col(col).is_not_null()).with_columns(
            pl.lit(side).alias("side"), pl.col(col).alias("price"), _decimal(pl.col(col)).alias("decimal"), p.alias("p"),
            ((1 - pl.col("p_model")) if flip else pl.col("p_model")).alias("p_model_side"),
            ((1 - pl.col("p_market")) if flip else pl.col("p_market")).alias("p_market_side")))
    out = pl.concat(sides).with_columns(
        (pl.col("p") * pl.col("decimal") - 1).alias("edge"),
        (KELLY_FRACTION * (pl.col("p") * pl.col("decimal") - 1) / (pl.col("decimal") - 1)).clip(0, None).alias("kelly"))
    return out.sort("edge", descending=True).group_by("game_id", "player_id", "prop_type", "line", "side",
                                                      maintain_order=True).first()


def _stakes(edges: pl.DataFrame, store: Store, day: date) -> pl.DataFrame:
    """Units per flagged quote. A bet already in the ledger keeps its placed stake and uses up
    room; only new bets are sized, within what's left of the per-player cap."""
    from nhl.props import ledger

    key = ledger.BET_KEY
    placed = ledger.load(store).filter((pl.col("kind") == "paper") & (pl.col("game_date") == day))
    e = edges.join(placed.select(*key, pl.col("stake_units").alias("_placed")), on=key, how="left").with_columns(
        pl.when(pl.col("flagged") & pl.col("_placed").is_null())
        .then((pl.col("kelly") * BANKROLL_UNITS).clip(0, MAX_BET_UNITS)).otherwise(0.0).alias("_new"))
    player_used = placed.group_by("game_id", "player_id").agg(pl.col("stake_units").sum().alias("_used"))
    e = e.join(player_used, on=["game_id", "player_id"], how="left").with_columns(
        (MAX_PLAYER_UNITS - pl.col("_used").fill_null(0.0)).clip(0, None).alias("_room"))
    new_total = pl.col("_new").sum().over("game_id", "player_id")
    e = e.with_columns(pl.when(new_total > pl.col("_room")).then(pl.col("_new") * pl.col("_room") / new_total)
                       .otherwise(pl.col("_new")).alias("_new"))
    return e.with_columns(pl.coalesce("_placed", pl.col("_new").round(2)).alias("stake_units")).drop(
        "_placed", "_new", "_used", "_room")


def compute(store: Store, day: date | None = None, now: datetime | None = None, write: bool = True) -> pl.DataFrame:
    """Best edge per (game, player, stat, line, side) for ``day``'s games not yet started.
    ``write`` lets the day's projections be cached (nothing else is written here)."""
    now = now or datetime.now(timezone.utc)
    day = day or now.astimezone(EASTERN).date()
    proj = project_day(store, day, write=write)
    if proj.is_empty():
        logger.info("props edges %s: no pregame snapshot", day)
        return pl.DataFrame()
    games = _games(store, proj["game_id"].unique().to_list()).filter(pl.col("start_utc") > now)
    if games.is_empty():
        return pl.DataFrame()
    quotes = current_quotes(store, int(games["season"][0]), games["game_id"].to_list(), now)
    if quotes.is_empty():
        logger.info("props edges %s: no prop prices", day)
        return pl.DataFrame()
    best = price_quotes(market_probs(quotes), proj)
    names = quotes.group_by("player_id").agg(pl.col("player_name").first(), pl.col("team").drop_nulls().first())
    info = proj.select("game_id", "player_id", "team_id", "position", "p_dressed", "confidence", "source", "slot", "pp_unit",
                       "pregame_stamp")
    best = best.join(info, on=["game_id", "player_id"], how="left").join(names, on="player_id", how="left").join(
        games.select("game_id", "game_date", "start_utc", "home_abbr", "away_abbr"), on="game_id").with_columns(
        pl.lit(now).alias("as_of"))
    flagged = ((pl.col("edge") >= MIN_EDGE) & (pl.col("price") <= MAX_PRICE) & (pl.col("books") >= MIN_BOOKS)
               & (pl.col("two_way_books") >= MIN_TWO_WAY_BOOKS)
               & pl.when(pl.col("position") == "G").then(pl.col("p_dressed") >= MIN_P_START)
                 .otherwise(pl.col("p_dressed").fill_null(1.0) >= 0.999)
               & (pl.col("confidence") == "high"))
    return _stakes(best.with_columns(flagged.fill_null(False).alias("flagged")), store, day)


def run(store: Store, day: date | None = None, write: bool = True) -> pl.DataFrame:
    """Compute prop edges, snapshot them, and add new flagged bets to the props paper ledger."""
    from nhl.props import ledger

    with _lock():
        now = datetime.now(timezone.utc)
        day = day or now.astimezone(EASTERN).date()
        e = compute(store, day, now, write=write)
        if e.is_empty() or not write:
            return e
        st = stamp(now)
        store.put_parquet(keys.props_edges(day, st), e.filter(pl.col("edge") >= SNAPSHOT_MIN_EDGE).with_columns(
            pl.lit(st).alias("stamp")))
        new = ledger.add_paper(store, e.filter(pl.col("flagged") & (pl.col("stake_units") > 0)))
        logger.info("props edges %s: %d quotes, %d flagged (%.2f u), %d new paper bets", day, e.height,
                    e.filter(pl.col("flagged")).height, float(e["stake_units"].sum()), new)
        return e


def render(e: pl.DataFrame, top: int = 15) -> str:
    """Flagged props, then the best unflagged edges, for the terminal."""
    if e.is_empty():
        return "no prop edges (no pregame snapshot, no prop prices, or all games started)"

    def line(r: dict) -> str:
        start = r["start_utc"].astimezone(EASTERN).strftime("%H:%M")
        what = f"{r['prop_type']} {r['side']} {r['line']}"
        return (f"  {start} {r['away_abbr']}@{r['home_abbr']}  {str(r['player_name'])[:22]:22s} {what:18s} "
                f"{int(r['price']):+5d} {r['book']:<10s} edge {100 * r['edge']:+5.1f}%  model {100 * r['p_model_side']:4.1f}% "
                f"mkt {100 * r['p_market_side']:4.1f}% ({r['books']} bk)  {r['stake_units']:.2f}u")

    flagged = e.filter(pl.col("flagged")).sort("edge", descending=True)
    out = [f"FLAGGED PROPS ({flagged.height}, {flagged['stake_units'].sum():.2f} u):"]
    out += [line(r) for r in flagged.iter_rows(named=True)] or ["  none"]
    rest = e.filter(~pl.col("flagged")).sort("edge", descending=True).head(top)
    out += [f"BEST UNFLAGGED (top {top}):"] + [line(r) for r in rest.iter_rows(named=True)]
    return "\n".join(out)


__all__ = ["MIN_EDGE", "MODEL_WEIGHT", "STATS", "compute", "latest_quotes", "market_probs", "price_quotes",
           "project_day", "render", "run"]
