"""Source tweets behind DailyFaceoff reports, fetched through X's public oEmbed endpoint.

DailyFaceoff cites a tweet for almost every goalie confirmation and line update. The
tweet text (and when it was posted) is the primary evidence, so each one is fetched once
via ``publish.twitter.com/oembed`` and cached forever: tweets are immutable, so a raw
payload at :func:`nhl.storage.keys.tweet_raw` is never refetched. Deleted or protected
tweets are recorded as ``available = False`` and never retried.

The posting time comes from the snowflake id itself, not from the oEmbed html (which only
carries a date).
"""

from __future__ import annotations

import html
import logging
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import polars as pl

from nhl.ingest.http import WebClient
from nhl.sources.common import utcnow
from nhl.storage import keys
from nhl.storage.s3 import Store

logger = logging.getLogger(__name__)

OEMBED_URL = "https://publish.twitter.com/oembed"
#: Milliseconds between the Unix epoch and the Twitter snowflake epoch (2010-11-04).
TWITTER_EPOCH_MS = 1288834974657

_STATUS_RE = re.compile(r"^https?://(?:www\.|mobile\.)?(?:x|twitter)\.com/([^/?#]+)/status(?:es)?/(\d+)", re.I)
_P_RE = re.compile(r"<p\b[^>]*>(.*?)</p>", re.S | re.I)
_BR_RE = re.compile(r"<br\s*/?>", re.I)
_TAG_RE = re.compile(r"<[^>]+>")

#: HTTP statuses meaning the tweet itself is gone or hidden (deleted, protected, suspended).
_UNAVAILABLE = (401, 403, 404, 410)

SCHEMA: dict[str, pl.DataType] = {
    "tweet_id": pl.String(),
    "url": pl.String(),
    "author_name": pl.String(),
    "author_handle": pl.String(),
    "created_at": pl.Datetime("us", "UTC"),
    "text": pl.String(),
    "available": pl.Boolean(),
    "fetched_at": pl.Datetime("us", "UTC"),
}


def tweet_id_from_url(url: str | None) -> str | None:
    """Extract the status id from an x.com / twitter.com status URL.

    Args:
        url: Any URL (non-tweet URLs return None).

    Returns:
        The numeric id as a string, or None.
    """
    if not url:
        return None
    match = _STATUS_RE.match(url.strip())
    return match.group(2) if match else None


def canonical_tweet_url(url: str | None) -> str | None:
    """Drop tracking query strings (``?s=20``) from tweet URLs; other URLs pass through.

    Args:
        url: Source URL as published.

    Returns:
        ``https://x.com/{handle}/status/{id}`` for tweets, the stripped input otherwise.
    """
    if not url:
        return None
    match = _STATUS_RE.match(url.strip())
    if not match:
        return url.strip()
    return f"https://x.com/{match.group(1)}/status/{match.group(2)}"


def snowflake_time(tweet_id: str | int) -> datetime:
    """Posting time encoded in a tweet id: ``ms = (id >> 22) + 1288834974657``.

    Args:
        tweet_id: Snowflake id.

    Returns:
        Aware UTC datetime (millisecond precision).
    """
    ms = (int(tweet_id) >> 22) + TWITTER_EPOCH_MS
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def html_to_text(embed_html: str | None) -> str | None:
    """Plain tweet text from the oEmbed blockquote, keeping line breaks.

    Only the ``<p>`` body is used, so the trailing "— Author (@handle) date" byline is
    dropped. ``<br>`` becomes a newline, tags are stripped and entities unescaped.

    Args:
        embed_html: The oEmbed ``html`` field.
    """
    if not embed_html:
        return None
    match = _P_RE.search(embed_html)
    body = match.group(1) if match else embed_html
    body = _BR_RE.sub("\n", body)
    text = html.unescape(_TAG_RE.sub("", body))
    lines = [line.strip() for line in text.split("\n")]
    return "\n".join(lines).strip()


def _handle(author_url: str | None) -> str | None:
    """``https://x.com/Gabby_Shirley_`` -> ``Gabby_Shirley_``."""
    if not author_url:
        return None
    path = urlparse(author_url).path.strip("/")
    return path.split("/")[0] or None


def parse_oembed(tweet_id: str, url: str, raw: dict[str, Any], fetched_at: datetime) -> dict[str, Any]:
    """Turn one cached raw record (see :func:`_fetch_raw`) into a table row.

    Args:
        tweet_id: Snowflake id.
        url: Canonical tweet URL.
        raw: ``{"status": int, "fetched_at": iso, "payload": oembed-json | None}``.
        fetched_at: Fallback fetch time when the raw record lacks one.
    """
    payload = raw.get("payload") or {}
    available = raw.get("status") == 200 and bool(payload.get("html"))
    stamp = raw.get("fetched_at")
    url_match = _STATUS_RE.match(url or "")
    handle = _handle(payload.get("author_url")) or (url_match.group(1) if url_match else None)
    return {
        "tweet_id": str(tweet_id),
        "url": url,
        "author_name": payload.get("author_name"),
        "author_handle": handle,
        "created_at": snowflake_time(tweet_id),
        "text": html_to_text(payload.get("html")) if available else None,
        "available": available,
        "fetched_at": datetime.fromisoformat(stamp) if stamp else fetched_at,
    }


class RateLimited(RuntimeError):
    """oEmbed throttled us or failed transiently; stop this batch and try next poll."""


def _fetch_raw(client: WebClient, url: str) -> dict[str, Any]:
    """GET the oEmbed record for one tweet (following the publish.x.com redirect).

    Returns:
        ``{"status", "fetched_at", "payload"}``; ``payload`` is None for unavailable tweets.

    Raises:
        RateLimited: On 429 or a 5xx, so the tweet is not recorded as unavailable.
    """
    client.limiter.wait()
    params = {"url": url, "omit_script": "true", "dnt": "true"}
    resp = client.session.get(OEMBED_URL, params=params, timeout=client.timeout, allow_redirects=True)
    fetched_at = utcnow().isoformat()
    if resp.status_code == 200:
        return {"status": 200, "fetched_at": fetched_at, "payload": resp.json()}
    if resp.status_code in _UNAVAILABLE:
        return {"status": resp.status_code, "fetched_at": fetched_at, "payload": None}
    raise RateLimited(f"oembed {url}: HTTP {resp.status_code}")


def fetch_tweets(store: Store, urls: Iterable[str | None], client: WebClient | None = None) -> int:
    """Fetch and store every not-yet-stored tweet among ``urls``.

    Ids already in :data:`keys.TWEETS` are skipped without a request; a raw payload
    already cached at :func:`keys.tweet_raw` is reused instead of refetched. Non-tweet
    URLs are ignored. Requests are spaced at <= 1 per second.

    Args:
        store: S3 store.
        urls: Source URLs (any mix of tweet and non-tweet links, duplicates allowed).
        client: Optional client (tests / shared limiter).

    Returns:
        Number of rows added to the tweets table.
    """
    wanted: dict[str, str] = {}
    for url in urls:
        tid = tweet_id_from_url(url)
        if tid and tid not in wanted:
            wanted[tid] = canonical_tweet_url(url)  # type: ignore[assignment]
    if not wanted:
        return 0
    existing = store.get_parquet(keys.TWEETS)
    known = set(existing.get_column("tweet_id").to_list()) if existing is not None else set()
    todo = {tid: url for tid, url in wanted.items() if tid not in known}
    if not todo:
        return 0

    client = client or WebClient("oembed", rps=1.0, headers={"Accept": "application/json"})
    rows: list[dict[str, Any]] = []
    now = utcnow()
    for tid, url in todo.items():
        raw_key = keys.tweet_raw(tid)
        raw = store.get_json_gz(raw_key)
        if raw is None:
            try:
                raw = _fetch_raw(client, url)
            except (RateLimited, OSError, ValueError) as exc:
                logger.warning("tweets: stopping batch at %s (%r); remaining retried next poll", tid, exc)
                break
            store.put_json_gz(raw_key, raw)
        row = parse_oembed(tid, url, raw, now)
        if not row["available"]:
            logger.info("tweet %s unavailable (HTTP %s)", tid, raw.get("status"))
        rows.append(row)

    if not rows:
        return 0
    fresh = pl.DataFrame(rows, schema=SCHEMA)
    table = fresh if existing is None else pl.concat([existing, fresh], how="diagonal_relaxed")
    store.put_parquet(keys.TWEETS, table.unique(subset=["tweet_id"], keep="first", maintain_order=True))
    logger.info("tweets: %d new (%d unavailable) -> %s", fresh.height, int((~fresh["available"]).sum()), keys.TWEETS)
    return fresh.height
