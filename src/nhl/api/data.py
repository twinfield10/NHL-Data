"""Read-side data access for the site: the pregame/betting contract in S3, cached in-process.

Everything the API serves comes from objects the pipeline already writes (see
:mod:`nhl.pregame.slate` for the data contract). Snapshots under a ``stamp`` never change,
so they are cached for the life of the process; the two mutable objects (the ``latest``
pointer and the bets ledger) are re-read after :data:`MUTABLE_TTL_SECONDS`. The one
exception is rankings, which compose team ratings from a fresh lineup projection
(:mod:`nhl.ratings.rankings`) and are cached for :data:`RANKINGS_TTL_SECONDS`.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone

import polars as pl

from nhl.betting import edges as edges_mod
from nhl.pregame import lineups
from nhl.ratings import rankings
from nhl.sim import constants as sim_constants
from nhl.sim.inputs import snapshot_dates
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

MUTABLE_TTL_SECONDS = 30.0
LIST_TTL_SECONDS = 60.0
GAMES_TTL_SECONDS = 300.0
CLOSING_TTL_SECONDS = 120.0
RANKINGS_TTL_SECONDS = 900.0
DAY_SECONDS = 86_400.0
READ_WORKERS = 16


class SiteData:
    """Cached reads of slate, lineups, goalies, prices, edges and the ledger.

    Args:
        store: Bucket access (defaults to the project's bucket and local cache).
    """

    def __init__(self, store: Store | None = None) -> None:
        self.store = store or Store()
        self._lock = threading.Lock()
        self._immutable: dict[str, pl.DataFrame | None] = {}
        self._timed: dict[str, tuple[float, object]] = {}
        self._players: dict[int, str] | None = None

    # ------------------------------------------------------------------ cache helpers
    def _parquet(self, key: str) -> pl.DataFrame | None:
        """A stamped (immutable) parquet object, read once per process."""
        with self._lock:
            if key in self._immutable:
                return self._immutable[key]
        df = self.store.get_parquet(key)
        with self._lock:
            self._immutable[key] = df
        return df

    def _all_stamps(self, kind: str, day: date) -> list[pl.DataFrame]:
        """Every ``pregame/{kind}/{day}/`` snapshot, oldest first (uncached ones read in parallel)."""
        prefix = f"pregame/{kind}/{day.isoformat()}/"
        keys_ = [f"{prefix}{st}.parquet" for st in self.stamps(prefix)]
        with ThreadPoolExecutor(READ_WORKERS) as pool:
            return [df for df in pool.map(self._parquet, keys_) if df is not None]

    def _timed_get(self, key: str, ttl: float, load):
        """``load()`` cached under ``key`` for ``ttl`` seconds."""
        now = time.monotonic()
        with self._lock:
            hit = self._timed.get(key)
            if hit and now - hit[0] < ttl:
                return hit[1]
        value = load()
        with self._lock:
            self._timed[key] = (now, value)
        return value

    # ------------------------------------------------------------------ lookups
    def player_names(self) -> dict[int, str]:
        """``player_id -> player_name`` for every player in the catalog."""
        if self._players is None:
            players = self.store.read_parquet_required(keys.PLAYERS)
            self._players = dict(zip(players["player_id"].to_list(), players["player_name"].to_list()))
        return self._players

    def games(self) -> pl.DataFrame:
        """The game catalog (dates, teams, final scores), re-read every few minutes."""
        return self._timed_get("games", GAMES_TTL_SECONDS, lambda: self.store.read_parquet_required(keys.GAMES))

    def venues(self) -> pl.DataFrame | None:
        """Arena name and location per game (from the play-by-play), re-read every few minutes."""
        return self._timed_get("venues", GAMES_TTL_SECONDS, lambda: self.store.get_parquet(keys.GAME_VENUES))

    def game(self, game_id: int) -> dict | None:
        """One catalog row, or None for an unknown game."""
        hit = self.games().filter(pl.col("game_id") == game_id)
        return hit.row(0, named=True) if hit.height else None

    def latest_stamp(self, day: date) -> str | None:
        """The newest pregame run's stamp for ``day``, or None if nothing ran."""
        def load() -> str | None:
            raw = self.store.get_bytes(keys.pregame_latest(day))
            return json.loads(raw)["stamp"] if raw else None
        return self._timed_get(f"latest/{day}", MUTABLE_TTL_SECONDS, load)

    def stamps(self, prefix: str) -> list[str]:
        """Every stamp under a ``pregame/{kind}/{date}/`` prefix, oldest first."""
        def load() -> list[str]:
            return sorted(k.rsplit("/", 1)[-1].removesuffix(".parquet") for k in self.store.list_keys(prefix))
        return self._timed_get(f"list/{prefix}", LIST_TTL_SECONDS, load)

    # ------------------------------------------------------------------ pregame
    def slate(self, day: date, stamp: str | None = None) -> pl.DataFrame | None:
        """One row per game for ``day`` (latest run unless ``stamp`` is given)."""
        stamp = stamp or self.latest_stamp(day)
        return self._parquet(keys.pregame_slate(day, stamp)) if stamp else None

    def day_slate(self, day: date) -> pl.DataFrame | None:
        """Each game's row from the last run that priced it on ``day``.

        A run only prices games that haven't started, so the latest run alone drops the early
        games; this keeps their final pregame price instead.
        """
        frames = self._all_stamps("slate", day)
        return _last_run_per_game(pl.concat(frames, how="diagonal_relaxed")) if frames else None

    def freshness(self, day: date) -> pl.DataFrame | None:
        """Input ages for the latest run on ``day``."""
        stamp = self.latest_stamp(day)
        return self._parquet(keys.pregame_freshness(day, stamp)) if stamp else None

    def lineups(self, day: date, game_id: int, stamp: str | None = None) -> pl.DataFrame | None:
        """Projected lineup rows for one game (latest run unless ``stamp`` is given)."""
        stamp = stamp or self.latest_stamp(day)
        df = self._parquet(keys.pregame_lineups(day, stamp)) if stamp else None
        return None if df is None else df.filter(pl.col("game_id") == game_id)

    def goalies(self, day: date, game_id: int, stamp: str | None = None) -> pl.DataFrame | None:
        """Starter probabilities for one game (latest run unless ``stamp`` is given)."""
        stamp = stamp or self.latest_stamp(day)
        df = self._parquet(keys.pregame_goalies(day, stamp)) if stamp else None
        return None if df is None else df.filter(pl.col("game_id") == game_id)

    def price_history(self, day: date, game_id: int) -> pl.DataFrame:
        """Model and market prices for one game across every pregame run on ``day``."""
        cols = ["stamp", "as_of", "p_home_win", "mkt_p_home_win", "mean_home_goals", "mean_away_goals",
                "home_starter", "away_starter"]
        frames = [df.filter(pl.col("game_id") == game_id).select(cols) for df in self._all_stamps("slate", day)]
        return pl.concat(frames) if frames else pl.DataFrame()

    # ------------------------------------------------------------------ betting
    def edges(self, day: date) -> pl.DataFrame | None:
        """The latest edges snapshot for ``day``."""
        stamps = self.stamps(f"pregame/edges/{day.isoformat()}/")
        return self._parquet(keys.betting_edges(day, stamps[-1])) if stamps else None

    def day_edges(self, day: date) -> pl.DataFrame | None:
        """Each game's edges from the last snapshot that covered it on ``day``.

        Edges are computed only for games that haven't started, so this keeps the final
        pregame view of the early games (like :meth:`day_slate`).
        """
        frames = self._all_stamps("edges", day)
        return _last_run_per_game(pl.concat(frames, how="diagonal_relaxed")) if frames else None

    def closing_edges(self, day: date) -> pl.DataFrame | None:
        """The edge view at the close for ``day``'s started games (see :func:`nhl.betting.edges.closing`)."""
        def load() -> pl.DataFrame | None:
            try:
                df = edges_mod.closing(self.store, day)
            except Exception:  # a bad odds file must not take the page down
                logger.exception("closing edges failed for %s", day)
                return None
            return df if df.height else None
        return self._timed_get(f"closing/{day}", CLOSING_TTL_SECONDS, load)

    def card_edges(self, day: date) -> pl.DataFrame | None:
        """Closing prices for games that have started, the latest live snapshot for the rest;
        ``point`` says which (``close`` | ``live``)."""
        live, close = self.day_edges(day), self.closing_edges(day)
        parts = []
        if close is not None:
            parts.append(close.with_columns(pl.lit("close").alias("point")))
        if live is not None:
            started = [] if close is None else close["game_id"].unique().to_list()
            parts.append(live.filter(~pl.col("game_id").is_in(started)).with_columns(pl.lit("live").alias("point")))
        if not parts:
            return None
        return pl.concat(parts, how="diagonal_relaxed").drop("score_matrix", "kelly", strict=False)

    def market_lines(self, day: date, game_ids: list[int], season: int) -> pl.DataFrame | None:
        """Best current price per side across books (no model) for ``game_ids``, for games the
        model hasn't priced yet. Same side/line conventions as edges, plus the devigged market
        probability of side 1 and the consensus line."""
        if not game_ids:
            return None

        def load() -> pl.DataFrame | None:
            try:
                rows = edges_mod.book_rows(self.store, season, game_ids)
            except Exception:  # a bad odds file must not take the page down
                logger.exception("market lines failed for %s", day)
                return None
            if rows.is_empty():
                return None
            rows = rows.filter(~pl.col("outlier"))
            sides = []
            for side, col in ((1, "price_1"), (2, "price_2")):
                dec = pl.when(pl.col(col) < 0).then(1 + 100 / -pl.col(col)).otherwise(1 + pl.col(col) / 100)
                p1 = pl.col("p_market")
                sides.append(rows.select(
                    "game_id", "market", "line", "book", "cons_line", "captured_at", pl.lit(side).alias("side"),
                    pl.col(col).alias("price"), dec.alias("decimal"),
                    (p1 if side == 1 else 1 - p1).alias("p_market_side"),
                ))
            best = pl.concat(sides).sort("decimal", descending=True).group_by(
                "game_id", "market", "side", "line", maintain_order=True).first()
            return best.drop("decimal")

        return self._timed_get(f"lines/{day}/{sorted(game_ids)}", LIST_TTL_SECONDS, load)

    # ------------------------------------------------------------------ ratings
    def rating_snapshot(self, day: date) -> date | None:
        """The latest rating snapshot dated on or before ``day`` (the one ``day``'s prices use)."""
        def load() -> list[date]:
            return snapshot_dates(self.store)
        days = [d for d in self._timed_get("rating-days", LIST_TTL_SECONDS, load) if d <= day]
        return days[-1] if days else None

    def rankings(self, day: date) -> dict | None:
        """Player, goalie, team and line boards from ``day``'s rating snapshot, or None without one.

        Team boards project every team's lineup as of now, so the whole thing is cached for
        :data:`RANKINGS_TTL_SECONDS` (injury news moves it within a day).
        """
        snap = self.rating_snapshot(day)
        if snap is None:
            return None

        def load() -> dict:
            season = int(self.games().filter(pl.col("game_date") <= day)["season"].max())
            span = [season - 10001, season]
            games = self.games()
            tables = rankings.snapshot_tables(self.store, snap)
            players = self.store.read_parquet_required(keys.PLAYERS)
            teams = rankings.current_teams(self.store, span, games)
            team_ids = sorted(set(games.filter(pl.col("season") == season)["home_team_id"].cast(pl.Int64).to_list()))
            as_of = datetime.now(timezone.utc)
            dep = lineups.project(self.store, rankings.lineup_targets(team_ids, day), span, as_of=as_of)
            starts = pl.concat([s for y in span if (s := self.store.get_parquet(keys.goalie_starts(y))) is not None],
                               how="vertical_relaxed")
            goalies = rankings.goalie_weights(starts, games, teams)
            constants = sim_constants.estimate(self.store, season)
            board = rankings.team_board(tables, dep, goalies, constants)
            lines = rankings.line_board(tables, dep)
            return {
                "snapshot": snap, "as_of": as_of, "season": season,
                "league": {"xg60_5v5": constants["xg60_5v5"], "xg60_pp": constants["xg60_pp"]},
                "players": rankings.player_board(tables, players, teams, day),
                "goalies": rankings.goalie_board(tables, players, teams, day),
                "teams": board, "goalie_weights": goalies, "lineups": dep,
                "lines": lines, "tables": tables,
            }
        return self._timed_get(f"rankings/{day}", RANKINGS_TTL_SECONDS, load)

    def units(self, season: int, current: bool) -> pl.DataFrame | None:
        """Every forward line and D pair iced at 5v5 in ``season`` (:func:`nhl.ratings.rankings.observed_units`).
        A finished season is cached for a day, the current one for :data:`RANKINGS_TTL_SECONDS`."""
        def load() -> pl.DataFrame | None:
            stints, rosters = self.store.get_parquet(keys.stints(season)), self.store.get_parquet(keys.rosters(season))
            if stints is None or rosters is None:
                return None
            return rankings.observed_units(stints, rosters)
        return self._timed_get(f"units/{season}", RANKINGS_TTL_SECONDS if current else DAY_SECONDS, load)

    def processed(self, key: str, current: bool) -> pl.DataFrame | None:
        """A season table under ``processed/`` (usage, on-ice context, matchups, linemates): the
        current season re-read every :data:`RANKINGS_TTL_SECONDS` (the nightly job rewrites it),
        finished seasons once a day."""
        return self._timed_get(f"processed/{key}", RANKINGS_TTL_SECONDS if current else DAY_SECONDS,
                               lambda: self.store.get_parquet(key))

    def ledger(self) -> pl.DataFrame | None:
        """The paper/real bet ledger (re-read every :data:`MUTABLE_TTL_SECONDS`)."""
        return self._timed_get("ledger", MUTABLE_TTL_SECONDS, lambda: self.store.get_parquet(keys.BETS_LEDGER))


def _last_run_per_game(df: pl.DataFrame) -> pl.DataFrame:
    """Keep each game's rows from the newest ``stamp`` that includes it."""
    return df.filter(pl.col("stamp") == pl.col("stamp").max().over("game_id"))
