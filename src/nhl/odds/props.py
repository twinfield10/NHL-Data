"""The player-prop table: one schema for every book, stored as price transitions.

Row grain is one price on one side of one player prop from one book at one moment::

    book, game_id, captured_at, player_name, prop_type, line, side, price

* ``prop_type`` (normalized across books, see :data:`PROP_TYPES`): ``goals``, ``assists``,
  ``points``, ``shots`` (on goal), ``saves``, ``blocks``, ``pp_points``, and the yes-only
  scorer markets ``first_goal``, ``last_goal``, ``first_team_goal``.
* ``line`` / ``side``: an over/under at ``line``. Ladder ("milestone") markets are folded
  into the same vocabulary: "3+ shots" is ``shots`` over 2.5, "anytime goalscorer" is
  ``goals`` over 0.5, "to score 2+ goals" is ``goals`` over 1.5, so the same bet from two
  books lands on the same key. Scorer markets with no line use side ``yes`` (``no`` when a
  book quotes it) and a null line.
* ``price``: American odds as quoted (an exchange's VWAP for 4Casters, with ``depth`` the
  dollars resting on that side).
* ``player_id``: NHL id resolved with :class:`nhl.sources.dailyfaceoff.PlayerResolver`;
  ``team`` is the player's tricode when the book says it or the roster match infers it.
* ``away_team`` / ``home_team`` / ``start_time`` are kept to match the NHL ``game_id``.

Each poller writes its own file (:func:`props_key`), like the odds tables, so pollers
never read-modify-write the same object. Each poll also refreshes the poller's seen table
(:data:`SEEN_KEY`, :func:`nhl.odds.store.record_seen`): when each book last listed each prop
before puck drop, so a prop taken down hours early has no close.
"""

from __future__ import annotations

import logging
from typing import Any

import polars as pl

from nhl.odds.core import attach_game_ids
from nhl.odds.store import record_seen, season_of_game
from nhl.sources.common import append_transitions
from nhl.sources.dailyfaceoff import NAMESAKES, PlayerResolver, norm_name, split_jersey
from nhl.storage import keys
from nhl.storage.s3 import Store
from nhl.teams import resolve_team

logger = logging.getLogger(__name__)

PROP_TYPES = ("goals", "assists", "points", "shots", "saves", "blocks", "pp_points",
              "first_goal", "last_goal", "first_team_goal")
SIDES = ("over", "under", "yes", "no")

#: Identity of one prop price series over time.
PROP_KEY = ("book", "game_id", "player_name", "prop_type", "line", "side")
#: Values whose change makes a poll worth storing.
PROP_VALUES = ("price",)
#: Grain of the props seen table: one side of one line of one player's prop at one book (per
#: side, as an exchange often drops one side too thin to fill while the other stays).
SEEN_KEY = ("book", "game_id", "player_name", "prop_type", "line", "side")

PROPS_SCHEMA: dict[str, pl.DataType] = {
    "book": pl.Utf8,
    "game_id": pl.Int64,
    "captured_at": pl.Datetime("us", "UTC"),
    "start_time": pl.Datetime("us", "UTC"),
    "away_team": pl.Utf8,
    "home_team": pl.Utf8,
    "player_name": pl.Utf8,
    "player_id": pl.Int64,
    "team": pl.Utf8,
    "prop_type": pl.Utf8,
    "line": pl.Float64,
    "side": pl.Utf8,
    "price": pl.Float64,
    "depth": pl.Float64,
    "price_point": pl.Utf8,
    "source_event_id": pl.Utf8,
}


def props_key(season: int, source: str) -> str:
    """One poller's prop table for a season (``external/odds/props/{season}/{source}.parquet``)."""
    return keys.odds_props(season).replace(".parquet", f"/{source}.parquet")


def milestone_line(threshold: Any) -> float | None:
    """The over/under line of an "N+" ladder rung ("3+" -> 2.5), or None if unreadable."""
    text = str(threshold or "").strip().rstrip("+")
    try:
        return float(text) - 0.5
    except ValueError:
        return None


def props_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Build a correctly typed props frame from row dicts.

    Rows need at least book, captured_at, start_time, away_team, home_team, player_name,
    prop_type, side, price. Team labels may be any spelling; they are resolved to tricodes.
    Rows whose ``prop_type``/``side`` fall outside the vocabulary are dropped (and logged),
    as are duplicates on :data:`PROP_KEY` + event + ``captured_at`` (the first row wins, so a
    source should list its two-way market before an equivalent ladder rung).
    """
    if not rows:
        return pl.DataFrame(schema=PROPS_SCHEMA)
    df = pl.DataFrame(rows, infer_schema_length=None, strict=False)
    for col, dtype in PROPS_SCHEMA.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype).alias(col))
    for col in ("captured_at", "start_time"):
        if df.schema[col] == pl.Utf8:
            df = df.with_columns(pl.col(col).str.to_datetime(time_zone="UTC", strict=False))
        elif isinstance(df.schema[col], pl.Datetime) and df.schema[col].time_zone is None:
            df = df.with_columns(pl.col(col).dt.replace_time_zone("UTC"))
        else:
            df = df.with_columns(pl.col(col).dt.convert_time_zone("UTC"))
    df = df.with_columns(
        pl.col("price_point").fill_null("live"),
        *(pl.col(c).map_elements(resolve_team, return_dtype=pl.Utf8) for c in ("away_team", "home_team", "team")),
    ).select([pl.col(c).cast(t) for c, t in PROPS_SCHEMA.items()])
    bad = ~pl.col("prop_type").is_in(PROP_TYPES) | ~pl.col("side").is_in(SIDES)
    if (n_bad := df.filter(bad).height):
        logger.warning("props: %d row(s) outside the prop_type/side vocabulary dropped", n_bad)
    # game_id is still null here, so the event id keeps two games of one player apart.
    return df.filter(~bad).unique(subset=[*PROP_KEY, "source_event_id", "captured_at"], keep="first",
                                  maintain_order=True)


def _roster_match(resolver: PlayerResolver, team: str, key: str, jersey: int | None = None) -> int | None:
    """``player_id`` on one team's roster by full name (namesakes told apart by ``jersey``, or
    not at all), else unique last name + initial."""
    roster = resolver.roster(team)
    if key in NAMESAKES and jersey is None:
        return NAMESAKES[key] if any(p["player_id"] == NAMESAKES[key] for p in roster) else None
    exact = [p for p in roster if p["first"] + p["last"] == key]
    if len(exact) > 1:
        hit = [p["player_id"] for p in exact if jersey is not None and p["number"] == jersey]
        return hit[0] if len(hit) == 1 else None
    if exact:
        return exact[0]["player_id"]
    same_last = [p["player_id"] for p in roster if p["last"] and key.endswith(p["last"]) and p["first"][:1] == key[:1]]
    return same_last[0] if len(same_last) == 1 else None


def resolve_player(
    resolver: PlayerResolver, name: str | None, team: str | None, home: str | None, away: str | None
) -> tuple[int | None, str | None]:
    """Resolve one prop's player to ``(player_id, team)``.

    With a known team the resolver's own order applies. Without one (4Casters lists only
    the matchup) both teams' rosters are tried by name, which also tells us the team; the
    league-wide players table is the last resort and leaves the team unknown.

    Args:
        resolver: Shared resolver (rosters are cached on it).
        name: Player name as the book publishes it.
        team: Player's tricode if the book gives it.
        home: Home tricode of the game.
        away: Away tricode of the game.
    """
    if team:
        return resolver.resolve(team, name), team
    bare, jersey = split_jersey(name)
    key = norm_name(bare)
    if not key:
        return None, None
    for candidate in (home, away):
        if candidate and (pid := _roster_match(resolver, candidate, key, jersey)) is not None:
            resolver.stats["resolved"] += 1
            return pid, candidate
    return resolver.resolve(None, name), None


def resolve_player_ids(props: pl.DataFrame, resolver: PlayerResolver) -> pl.DataFrame:
    """Fill ``player_id`` (and ``team`` where it was unknown) on a props frame.

    Returns:
        The frame with ``player_id`` and ``team`` filled where resolution succeeded.
    """
    if props.is_empty():
        return props
    people = props.select("player_name", "team", "home_team", "away_team").unique()
    resolved = [
        {"player_name": n, "team": t, "home_team": h, "away_team": a, "_pid": pid, "_team": team}
        for n, t, h, a in people.iter_rows()
        for pid, team in [resolve_player(resolver, n, t, h, a)]
    ]
    lookup = pl.DataFrame(resolved, schema={"player_name": pl.Utf8, "team": pl.Utf8, "home_team": pl.Utf8,
                                            "away_team": pl.Utf8, "_pid": pl.Int64, "_team": pl.Utf8})
    return (
        props.join(lookup, on=["player_name", "team", "home_team", "away_team"], how="left", nulls_equal=True)
        .with_columns(pl.coalesce("player_id", "_pid").alias("player_id"), pl.coalesce("team", "_team").alias("team"))
        .drop("_pid", "_team")
        .select(props.columns)
    )


def store_props(
    store: Store, props: pl.DataFrame, games: pl.DataFrame, source: str, resolver: PlayerResolver | None = None
) -> int:
    """Attach game ids, resolve player ids and append prop transitions by season.

    Args:
        store: S3 store.
        props: Output of :func:`props_frame`.
        games: ``processed/games.parquet``.
        source: Poller name; selects the file (:func:`props_key`).
        resolver: Shared player resolver; built from the store when None and needed.

    Returns:
        Rows written across all seasons.
    """
    if props.is_empty():
        logger.info("[%s] props: none offered", source)
        return 0
    props = attach_game_ids(props, games)
    unmatched = props.filter(pl.col("game_id").is_null())
    if unmatched.height:
        pairs = sorted(unmatched.select("away_team", "home_team").unique().rows(), key=str)
        logger.warning("[%s] props: %d row(s) across %d game(s) matched no scheduled game and were dropped: %s",
                       source, unmatched.height, len(pairs), pairs)
    props = props.filter(pl.col("game_id").is_not_null())
    if props.is_empty():
        return 0
    resolver = resolver or PlayerResolver.from_store(store)
    props = resolve_player_ids(props, resolver)
    players = props.select("player_name", "team").unique()
    hit = props.filter(pl.col("player_id").is_not_null()).select("player_name", "team").unique().height
    logger.info("[%s] props: %d row(s), %d player(s), %d resolved to an NHL id (%.0f%%)",
                source, props.height, players.height, hit, 100 * hit / max(players.height, 1))
    written = 0
    for (season,), part in props.with_columns(season_of_game(pl.col("game_id")).alias("_season")).partition_by(
        "_season", as_dict=True
    ).items():
        written += append_transitions(store, props_key(int(season), source), part.drop("_season"),
                                      PROP_KEY, PROP_VALUES)
        record_seen(store, keys.props_seen(int(season), source), part, SEEN_KEY)
    logger.info("[%s] props: %d row(s) written", source, written)
    return written


def load_props(store: Store, season: int) -> pl.DataFrame:
    """Every polled prop price for a season, all sources combined."""
    prefix = keys.odds_props(season).replace(".parquet", "/")
    frames = [store.get_parquet(k) for k in store.list_keys(prefix) if k.endswith(".parquet")]
    frames = [f for f in frames if f is not None]
    if not frames:
        return pl.DataFrame(schema=PROPS_SCHEMA)
    return pl.concat(frames, how="diagonal_relaxed").sort("game_id", "captured_at")


__all__ = ["PROPS_SCHEMA", "PROP_KEY", "PROP_TYPES", "PROP_VALUES", "SEEN_KEY", "SIDES", "load_props", "milestone_line",
           "props_frame", "props_key", "resolve_player", "resolve_player_ids", "store_props"]
