"""NHL EDGE puck/player-tracking aggregates (2021-22 onward), stored as season-to-date snapshots.

``api-web.nhle.com/v1/edge/...`` serves one JSON document per (entity, season, game type),
``/{endpoint}/{id}/{season}/{gameType}`` (``/now`` 307-redirects to the current season).
A season/game type with no tracking data for the entity answers 404. Every ``*-detail``
payload carries ``seasonsWithEdgeStats`` (all seasons x game types the entity has data
for), so the detail call doubles as the availability check before the other endpoints.

Endpoints fetched per entity (the first one is the detail/availability call):

* skater: ``skater-detail`` (identity only; its metric blocks duplicate the others),
  ``skater-skating-speed-detail`` (max speed, bursts 18-20/20-22/22+ mph),
  ``skater-shot-speed-detail`` (top/avg shot speed, attempts by speed band),
  ``skater-skating-distance-detail`` (total, per 60, max game/period by strength),
  ``skater-zone-time`` (OZ/NZ/DZ time % by strength, zone starts),
  ``skater-shot-location-detail`` (SOG/goals/sh% by 17 areas and high/mid/long totals).
* goalie: ``goalie-detail`` (GAA, games above .900, goal differential/60, goal support,
  point %), ``goalie-save-percentage-detail``, ``goalie-5v5-detail``,
  ``goalie-shot-location-detail`` (saves/GA/sv% by area and danger band).
* team: ``team-detail`` (identity), ``team-skating-speed-detail``,
  ``team-shot-speed-detail``, ``team-skating-distance-detail``, ``team-zone-time-details``,
  ``team-shot-location-detail`` (team lists are split by position F/D and strength).

Not stored (derivable or per-game): the ``*-landing`` league pages, ``*-top-10`` leaderboards,
``*-comparison`` (one-call subset without percentiles or strength splits), and every
per-game list (``topSkatingSpeeds``, ``hardestShots``, ``*Last10``). ``leagueAvg`` values and
metric-unit duplicates are dropped: one row is one entity, so they are constant per season.

Normalized tables (:func:`keys.edge`): one wide row per (id, season, game_type, captured_at)
with flat Float64 metric columns named ``{block}_{label...}_{metric}``, e.g.
``speed_bursts_over_22``, ``distance_es_distance_per_60_percentile``,
``zone_pp_offensive_zone_pctg``, ``area_low_slot_sog``, ``loc_high_d_goals_rank`` (team).
Rows are appended only when a value changed since the last snapshot of that series
(:func:`nhl.sources.common.append_transitions`); use :func:`latest_edge` for the current view.

Raw bundles (all endpoints for one entity-season) are cached under
``raw/external/edge/{kind}/{season}/{id}.json.gz`` for completed seasons (fetched once)
and ``raw/external/edge/{kind}/{season}/{YYYY-MM-DD}/{id}.json.gz`` for the current season.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

import polars as pl
import requests

from nhl import config
from nhl.ingest.http import WEB_API, NHLClient
from nhl.sources.common import append_transitions, utcnow
from nhl.sources.dailyfaceoff import EASTERN, parse_time, season_for_date
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

EDGE_API = f"{WEB_API}/edge"
FIRST_EDGE_SEASON = 2021
KINDS: tuple[str, ...] = ("skater", "goalie", "team")
GAME_TYPES: tuple[int, ...] = (2, 3)
#: Requests per second to api-web from this module (the game ingest uses its own budget).
EDGE_RPS = 3.0

ENDPOINTS: dict[str, tuple[str, ...]] = {
    "skater": (
        "skater-detail",
        "skater-skating-speed-detail",
        "skater-shot-speed-detail",
        "skater-skating-distance-detail",
        "skater-zone-time",
        "skater-shot-location-detail",
    ),
    "goalie": (
        "goalie-detail",
        "goalie-save-percentage-detail",
        "goalie-5v5-detail",
        "goalie-shot-location-detail",
    ),
    "team": (
        "team-detail",
        "team-skating-speed-detail",
        "team-shot-speed-detail",
        "team-skating-distance-detail",
        "team-zone-time-details",
        "team-shot-location-detail",
    ),
}

#: Top-level blocks of the detail payloads that are flattened (the rest duplicate the
#: sub-detail endpoints).
DETAIL_BLOCKS: dict[str, tuple[str, ...]] = {"skater": (), "goalie": ("stats",), "team": ()}

#: Column prefix for each flattened top-level block.
BLOCK_PREFIX: dict[str, str] = {
    "skatingSpeedDetails": "speed",
    "shotSpeedDetails": "shot_speed",
    "skatingDistanceDetails": "distance",
    "zoneTimeDetails": "zone",
    "zoneStarts": "zone_starts",
    "shotLocationDetails": "area",
    "shotLocationTotals": "loc",
    "savePctgDetails": "svpct",
    "savePctg5v5Details": "sv5v5",
    "stats": "goalie",
    "shotDifferential": "shot_diff",
}

#: Fields of list items that label the item (become part of the column name).
LABEL_FIELDS: tuple[str, ...] = ("strengthCode", "positionCode", "position", "locationCode", "area")
#: Leaf names dropped from a column path (the unit/value wrapper adds nothing).
_TRANSPARENT = {"imperial", "value"}
_SKIP_FIELDS = {"metric", "leagueAvg", "overlay"}

ID_COLUMN = {"skater": "player_id", "goalie": "player_id", "team": "team_id"}
IDENTITY_SCHEMA: dict[str, dict[str, pl.DataType]] = {
    "skater": {
        "player_id": pl.Int64(), "season": pl.Int32(), "game_type": pl.Int8(), "team": pl.String(),
        "position": pl.String(), "games_played": pl.Int32(), "goals": pl.Int32(), "assists": pl.Int32(),
        "points": pl.Int32(),
    },
    "goalie": {
        "player_id": pl.Int64(), "season": pl.Int32(), "game_type": pl.Int8(), "team": pl.String(),
        "games_played": pl.Int32(), "wins": pl.Int32(), "losses": pl.Int32(), "ot_losses": pl.Int32(),
        "gaa": pl.Float64(), "save_pctg": pl.Float64(),
    },
    "team": {
        "team_id": pl.Int64(), "season": pl.Int32(), "game_type": pl.Int8(), "team": pl.String(),
        "games_played": pl.Int32(), "wins": pl.Int32(), "losses": pl.Int32(), "ot_losses": pl.Int32(),
        "points": pl.Int32(),
    },
}


# --------------------------------------------------------------------------- naming
def snake(text: str) -> str:
    """camelCase / free-text label -> snake_case (``burstsOver22`` -> ``bursts_over_22``,
    ``"L Net Side"`` -> ``l_net_side``, ``savePctg5v5`` -> ``save_pctg_5v5``)."""
    text = re.sub(r"([a-z])([A-Z0-9])", r"\1_\2", text)
    text = re.sub(r"([0-9])([A-Za-z])", r"\1_\2", text)
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()
    return text.replace("5_v_5", "5v5")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _flatten(node: Any, path: list[str], out: dict[str, float]) -> None:
    """Collect numeric leaves of ``node`` into ``out`` under snake-cased path names."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key in _SKIP_FIELDS or key.endswith("LeagueAvg") or key in LABEL_FIELDS:
                continue
            _flatten(value, path if key in _TRANSPARENT else [*path, snake(key)], out)
    elif isinstance(node, list):
        for item in node:
            if not isinstance(item, dict):
                continue
            labels = [snake(str(item[f])) for f in item if f in LABEL_FIELDS and item[f] is not None]
            _flatten(item, [*path[:1], *labels, *path[1:]], out)
    elif (_is_number(node) or node is None) and path:
        name = "_".join(path)
        if node is not None or name not in out:
            out[name] = None if node is None else float(node)


def flatten_payload(payload: dict[str, Any], blocks: Iterable[str] | None = None) -> dict[str, float]:
    """Flatten the metric blocks of one EDGE payload into ``{column: value}``.

    Per-game lists, identity blocks, ``leagueAvg`` and metric-unit duplicates are skipped.
    List items are labelled by their ``strengthCode``/``positionCode``/``position``/
    ``locationCode``/``area`` fields, so ``zoneTimeDetails[strengthCode=pp].offensiveZonePctg``
    becomes ``zone_pp_offensive_zone_pctg``.

    Args:
        payload: One endpoint's JSON.
        blocks: Top-level blocks to flatten (default: every block in :data:`BLOCK_PREFIX`).
    """
    wanted = set(BLOCK_PREFIX) if blocks is None else set(blocks)
    out: dict[str, float] = {}
    for block, node in payload.items():
        if block not in wanted or block not in BLOCK_PREFIX:
            continue
        _flatten(node, [BLOCK_PREFIX[block]], out)
    return out


# --------------------------------------------------------------------------- normalize
def available_game_types(detail: dict[str, Any] | None, season: int) -> list[int]:
    """Game types with EDGE data for ``season`` according to ``seasonsWithEdgeStats``."""
    for entry in (detail or {}).get("seasonsWithEdgeStats") or []:
        if entry.get("id") == season:
            return [int(g) for g in entry.get("gameTypes") or [] if int(g) in GAME_TYPES]
    return []


def _identity(kind: str, detail: dict[str, Any], entity_id: int, season: int, game_type: int) -> dict[str, Any]:
    """Identity/box-score columns of one row from the detail payload."""
    if kind == "team":
        t = detail.get("team") or {}
        return {
            "team_id": entity_id, "season": season, "game_type": game_type, "team": t.get("abbrev"),
            "games_played": t.get("gamesPlayed"), "wins": t.get("wins"), "losses": t.get("losses"),
            "ot_losses": t.get("otLosses"), "points": t.get("points"),
        }
    p = detail.get("player") or {}
    row = {
        "player_id": entity_id, "season": season, "game_type": game_type,
        "team": (p.get("team") or {}).get("abbrev"), "games_played": p.get("gamesPlayed"),
    }
    if kind == "skater":
        row |= {"position": p.get("position"), "goals": p.get("goals"), "assists": p.get("assists"),
                "points": p.get("points")}
    else:
        row |= {"wins": p.get("wins"), "losses": p.get("losses"), "ot_losses": p.get("overtimeLosses"),
                "gaa": p.get("goalsAgainstAvg"), "save_pctg": p.get("savePctg")}
    return row


def normalize_bundle(kind: str, bundle: dict[str, Any]) -> pl.DataFrame:
    """One wide row per game type in a raw bundle (see :func:`fetch_bundle`).

    Args:
        kind: ``"skater"``, ``"goalie"`` or ``"team"``.
        bundle: ``{"id", "season", "captured_at", "game_types": {"2": {endpoint: payload}}}``.

    Returns:
        Identity columns (:data:`IDENTITY_SCHEMA`), Float64 metric columns and
        ``captured_at``; empty if the bundle holds no data.
    """
    detail_name = ENDPOINTS[kind][0]
    captured_at = parse_time(bundle["captured_at"])
    rows: list[dict[str, Any]] = []
    for gt, payloads in sorted((bundle.get("game_types") or {}).items()):
        detail = (payloads or {}).get(detail_name)
        if not detail:
            continue
        row = _identity(kind, detail, int(bundle["id"]), int(bundle["season"]), int(gt))
        metrics: dict[str, float] = flatten_payload(detail, DETAIL_BLOCKS[kind])
        for name in ENDPOINTS[kind][1:]:
            payload = payloads.get(name)
            if payload:
                for col, value in flatten_payload(payload).items():
                    if value is not None or col not in metrics:
                        metrics[col] = value
        rows.append({**row, **{f"m:{c}": v for c, v in metrics.items()}})
    return _frame(kind, rows, captured_at)


def _frame(kind: str, rows: list[dict[str, Any]], captured_at: datetime | None) -> pl.DataFrame:
    """Build a typed frame from identity + ``m:``-prefixed metric dicts."""
    ident = IDENTITY_SCHEMA[kind]
    metric_cols = list(dict.fromkeys(c for r in rows for c in r if c.startswith("m:")))
    schema = {**ident, **{c[2:]: pl.Float64() for c in metric_cols}, "captured_at": pl.Datetime("us", "UTC")}
    data = [
        {**{c: r.get(c) for c in ident}, **{c[2:]: r.get(c) for c in metric_cols}, "captured_at": captured_at}
        for r in rows
    ]
    return pl.DataFrame(data, schema=schema)


def metric_columns(frame: pl.DataFrame, kind: str) -> list[str]:
    """Metric (tracked-value) columns of an EDGE frame: everything but identity and stamp."""
    fixed = set(IDENTITY_SCHEMA[kind]) | {"captured_at"}
    return [c for c in frame.columns if c not in fixed]


# --------------------------------------------------------------------------- fetch
def _get(client: NHLClient, endpoint: str, entity_id: int, season: int, game_type: int) -> dict[str, Any] | None:
    """GET one EDGE document; None when the API has no data (404)."""
    try:
        return client.get_json(f"{EDGE_API}/{endpoint}/{entity_id}/{season}/{game_type}")
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise


def fetch_bundle(client: NHLClient, kind: str, entity_id: int, season: int, captured_at: datetime) -> dict[str, Any]:
    """Fetch every endpoint of ``kind`` for one entity-season (regular season + playoffs).

    The regular-season detail payload says which game types exist; if it is missing
    (404), the playoff detail is tried before giving up.

    Returns:
        Raw bundle ``{"kind", "id", "season", "captured_at", "game_types": {gt: {endpoint: payload|None}}}``.
    """
    names = ENDPOINTS[kind]
    game_types: dict[str, dict[str, Any]] = {}
    detail = _get(client, names[0], entity_id, season, 2)
    if detail is not None:
        wanted = available_game_types(detail, season) or [2]
        prefetched = {2: detail}
    else:
        playoff = _get(client, names[0], entity_id, season, 3)
        wanted = [3] if playoff is not None else []
        prefetched = {3: playoff} if playoff is not None else {}
    for gt in wanted:
        first = prefetched.get(gt) or _get(client, names[0], entity_id, season, gt)
        if first is None:
            continue
        payloads = {names[0]: first}
        for name in names[1:]:
            payloads[name] = _get(client, name, entity_id, season, gt)
        game_types[str(gt)] = payloads
    return {"kind": kind, "id": entity_id, "season": season, "captured_at": captured_at.isoformat(),
            "game_types": game_types}


def raw_prefix(kind: str, season: int, capture_day: str | None = None) -> str:
    """Raw bundle prefix; ``capture_day`` (YYYY-MM-DD) only for the in-progress season."""
    base = f"raw/external/edge/{kind}/{season}/"
    return base + (f"{capture_day}/" if capture_day else "")


def _id_from_key(key: str) -> int:
    return int(key.rsplit("/", 1)[-1].split(".", 1)[0])


# --------------------------------------------------------------------------- enumerate
def entity_ids(store: Store, kind: str, season: int) -> list[int]:
    """Players (skaters or goalies) or teams to snapshot for a season.

    Players come from the season's stats-API bios (``raw/players/{kind}_bios_{season}``),
    which list everyone who played that season; without them, from ``processed/players``
    by position and ``last_season >= season`` (a superset; misses answer 404 and cost one
    request). Teams come from the games table (home team ids that season).
    """
    if kind == "team":
        games = store.get_parquet(keys.GAMES)
        if games is not None:
            ids = games.filter(pl.col("season") == season).get_column("home_team_id").unique().sort()
            if not ids.is_empty():
                return [int(i) for i in ids]
        teams = store.read_parquet_required(keys.TEAMS)
        from nhl.teams import (
            TEAMS as ACTIVE,  # local import: only needed on the fallback path
        )

        return sorted(int(i) for i in teams.filter(pl.col("team_abbr").is_in(list(ACTIVE)))["team_id"])
    bios = store.get_json_gz(keys.raw_player_bios(kind, season))
    if bios:
        return sorted({int(b["playerId"]) for b in bios})
    players = store.read_parquet_required(keys.PLAYERS)
    is_goalie = pl.col("position") == "G"
    return sorted(int(i) for i in players.filter(
        (is_goalie if kind == "goalie" else ~is_goalie) & (pl.col("last_season") >= season)
    )["player_id"])


# --------------------------------------------------------------------------- snapshot
def edge_keys(kind: str) -> tuple[str, str, str]:
    """Series key of an EDGE table (captured_at distinguishes snapshots)."""
    return (ID_COLUMN[kind], "season", "game_type")


def write_rows(store: Store, kind: str, season: int, frames: Sequence[pl.DataFrame]) -> int:
    """Append changed rows of ``frames`` to :func:`keys.edge` for one kind/season."""
    frames = [f for f in frames if not f.is_empty()]
    if not frames:
        return 0
    rows = pl.concat(frames, how="diagonal_relaxed")
    return append_transitions(store, keys.edge(kind, season), rows, edge_keys(kind), metric_columns(rows, kind))


def snapshot_season(
    store: Store,
    kind: str,
    season: int,
    client: NHLClient,
    current: bool,
    ids: Sequence[int] | None = None,
    flush_every: int = 100,
) -> int:
    """Snapshot one kind/season. Resumable: cached bundles are not refetched.

    Bundles already cached but absent from the table (a crash between the raw write and
    the table flush) are re-normalized from the cache.

    Args:
        store: S3 store.
        kind: ``"skater"``, ``"goalie"`` or ``"team"``.
        season: 8-digit season id.
        client: api-web client (rate-limited).
        current: True for the in-progress season (one cache per capture day).
        ids: Restrict to these entity ids (default: :func:`entity_ids`).
        flush_every: Write the table every N entities.

    Returns:
        Rows written to :func:`keys.edge`.
    """
    captured_at = utcnow()
    day = captured_at.astimezone(EASTERN).date().isoformat() if current else None
    prefix = raw_prefix(kind, season, day)
    cached = {_id_from_key(k) for k in store.list_keys(prefix) if k.endswith(".json.gz") and k.count("/") == prefix.count("/")}
    table = store.get_parquet(keys.edge(kind, season))
    id_col = ID_COLUMN[kind]
    stored: set[int] = set()
    if table is not None and not table.is_empty():
        recent = table
        if current:
            recent = table.filter(pl.col("captured_at").dt.convert_time_zone(str(EASTERN)).dt.date().cast(pl.String) == day)
        stored = set(recent.get_column(id_col).to_list())
    todo = list(ids) if ids is not None else entity_ids(store, kind, season)
    logger.info("edge %s %s: %d ids, %d cached", kind, season, len(todo), len(cached & set(todo)))
    pending: list[pl.DataFrame] = []
    written = fetched = 0
    started = time.monotonic()
    for i, entity_id in enumerate(todo, 1):
        key = f"{prefix}{entity_id}.json.gz"
        if entity_id in cached:
            if entity_id in stored:
                continue
            bundle = store.get_json_gz(key)
        else:
            try:
                bundle = fetch_bundle(client, kind, entity_id, season, captured_at)
            except Exception as exc:  # noqa: BLE001 - one entity must not stop the backfill
                logger.warning("edge %s %s %s failed: %r", kind, season, entity_id, exc)
                continue
            store.put_json_gz(key, bundle)
            fetched += 1
        if bundle:
            pending.append(normalize_bundle(kind, bundle))
        if len(pending) >= flush_every:
            written += write_rows(store, kind, season, pending)
            pending = []
            rate = fetched / max(time.monotonic() - started, 1e-9)
            logger.info("edge %s %s: %d/%d ids (%.2f fetched/s)", kind, season, i, len(todo), rate)
    written += write_rows(store, kind, season, pending)
    logger.info("edge %s %s: fetched %d bundles, wrote %d rows", kind, season, fetched, written)
    return written


def snapshot_edge(
    store: Store,
    start_year: int = FIRST_EDGE_SEASON,
    kinds: Sequence[str] = KINDS,
    end_year: int | None = None,
    client: NHLClient | None = None,
    ids: dict[str, Sequence[int]] | None = None,
) -> dict[str, int]:
    """Snapshot EDGE aggregates for every kind and season ``start_year..end_year``.

    Completed seasons are fetched once (raw cache keyed by id); the in-progress season is
    re-fetched once per capture day and only changed rows are appended.

    Args:
        store: S3 store.
        start_year: First season start year (EDGE starts with 2021-22).
        kinds: Subset of :data:`KINDS`.
        end_year: Last season start year (default: the current season).
        client: api-web client; default is limited to :data:`EDGE_RPS`.
        ids: Optional ``{kind: ids}`` restriction (tests, spot refreshes).

    Returns:
        ``{kind: rows written}``.
    """
    client = client or NHLClient(rps=EDGE_RPS)
    current = season_for_date(utcnow().astimezone(EASTERN).date())
    last = config.season_start_year(current) if end_year is None else end_year
    out: dict[str, int] = {}
    for kind in kinds:
        if kind not in ENDPOINTS:
            raise ValueError(f"unknown EDGE kind {kind!r}; expected one of {KINDS}")
        out[kind] = 0
        for year in range(max(start_year, FIRST_EDGE_SEASON), last + 1):
            season = config.season_id(year)
            out[kind] += snapshot_season(
                store, kind, season, client, current=season >= current,
                ids=(ids or {}).get(kind),
            )
    return out


def latest_edge(frame: pl.DataFrame, kind: str) -> pl.DataFrame:
    """Latest snapshot per (id, season, game_type) of an EDGE table.

    Args:
        frame: Contents of :func:`keys.edge` (one or several seasons concatenated).
        kind: ``"skater"``, ``"goalie"`` or ``"team"``.
    """
    return frame.sort("captured_at").group_by(list(edge_keys(kind)), maintain_order=True).last()


def load_latest_edge(store: Store, kind: str, season: int) -> pl.DataFrame | None:
    """Read :func:`keys.edge` for one season and reduce it with :func:`latest_edge`."""
    frame = store.get_parquet(keys.edge(kind, season))
    return None if frame is None else latest_edge(frame, kind)
