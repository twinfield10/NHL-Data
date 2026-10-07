"""Shared plumbing for third-party sources: raw archiving and change-only storage.

Every poll archives its raw payload verbatim (so normalizers can be fixed and replayed),
and normalized tables store only *transitions*: a row is written when a tracked value
differs from the last stored observation of the same series. Ported from the
rebirtha-nfl odds pipeline.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

import polars as pl

from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    """Current UTC time truncated to whole seconds (stamps must survive a replay)."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def stamp(moment: datetime) -> str:
    """Filesystem-safe UTC stamp, e.g. ``20261005T174210Z``."""
    return moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def archive_raw(store: Store, source: str, captured_at: datetime, name: str, payload: Any) -> str:
    """Write one raw payload to ``raw/external/{source}/{date}/{stamp}-{name}.json.gz``.

    Args:
        store: S3 store.
        source: Source name, e.g. ``"lowvig"``.
        captured_at: Poll time (UTC).
        name: Payload label within the poll, e.g. ``"offering"``.
        payload: JSON-serializable payload.

    Returns:
        The key written.
    """
    key = f"raw/external/{source}/{captured_at:%Y-%m-%d}/{stamp(captured_at)}-{name}.json.gz"
    store.put_json_gz(key, payload)
    return key


def _differs(left: pl.Expr, right: pl.Expr) -> pl.Expr:
    """Null-safe inequality (a value appearing or disappearing counts as a change)."""
    return (left != right).fill_null(False) | (left.is_null() != right.is_null())


def keep_transitions(frame: pl.DataFrame, keys: Sequence[str], values: Sequence[str]) -> pl.DataFrame:
    """Reduce observations to the points where a tracked value changed. Idempotent.

    Args:
        frame: Observations with ``captured_at``.
        keys: Columns identifying one series over time.
        values: Columns whose change makes an observation worth storing.
    """
    if frame.is_empty():
        return frame
    keys = list(keys)
    tracked = [c for c in values if c in frame.columns]
    ordered = frame.unique(subset=[*keys, "captured_at"], keep="last").sort([*keys, "captured_at"])
    first = pl.int_range(pl.len()).over(keys) == 0
    if not tracked:
        return ordered.filter(first)
    changed = pl.any_horizontal(*(_differs(pl.col(c), pl.col(c).shift(1).over(keys)) for c in tracked))
    return ordered.filter(first | changed)


def new_transitions(
    existing: pl.DataFrame, incoming: pl.DataFrame, keys: Sequence[str], values: Sequence[str]
) -> pl.DataFrame:
    """Rows of ``incoming`` that differ from the latest stored observation of their series.

    Use this for live polls: the write is purely additive, so a poll where nothing moved
    writes nothing.
    """
    incoming = keep_transitions(incoming, keys, values)
    if incoming.is_empty() or existing.is_empty():
        return incoming
    keys = list(keys)
    tracked = [c for c in values if c in incoming.columns and c in existing.columns]
    prev = {c: f"_prev_{c}" for c in tracked}
    latest = (
        existing.sort("captured_at")
        .group_by(keys, maintain_order=True)
        .agg(pl.lit(True).alias("_seen"), *(pl.col(c).last().alias(prev[c]) for c in tracked))
    )
    joined = incoming.join(latest, on=keys, how="left", nulls_equal=True)
    unseen = pl.col("_seen").is_null()
    changed = pl.any_horizontal(*(_differs(pl.col(c), pl.col(prev[c])) for c in tracked)) if tracked else pl.lit(False)
    return joined.filter(unseen | changed).drop("_seen", *prev.values())


def append_transitions(
    store: Store, key: str, incoming: pl.DataFrame, keys: Sequence[str], values: Sequence[str]
) -> int:
    """Append only the changed rows of ``incoming`` to the parquet table at ``key``.

    Returns:
        Number of rows written.
    """
    if incoming.is_empty():
        return 0
    existing = store.get_parquet(key)
    if existing is None:
        existing = incoming.clear()
    fresh = new_transitions(existing, incoming, keys, values)
    if fresh.is_empty():
        logger.info("no changes for %s", key)
        return 0
    store.put_parquet(key, pl.concat([existing, fresh], how="diagonal_relaxed"))
    logger.info("%d new row(s) -> %s", fresh.height, key)
    return fresh.height
