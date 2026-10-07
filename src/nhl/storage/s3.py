"""S3 is the system of record; a local directory acts as a read-through cache.

Every object is written to S3 and mirrored locally. Reads use the local copy when its
ETag still matches the bucket (one HEAD request), so rebuilding processed tables from
tens of thousands of raw game files does not re-download them. Objects under
``raw/`` are immutable once a game is final, so their cached copy is trusted without
the HEAD check.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
from pathlib import Path
from typing import Any

import boto3
import polars as pl
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from nhl import config

logger = logging.getLogger(__name__)

_MISSING = {"NoSuchKey", "404", "NotFound"}
_IMMUTABLE_PREFIXES = ("raw/pbp/", "raw/shifts/")


class S3Error(RuntimeError):
    """An S3 operation failed for a reason other than a missing object."""


class Store:
    """Bucket access with a local ETag-validated cache.

    Args:
        bucket: Bucket name. Defaults to ``config.S3_BUCKET``.
        cache_dir: Local cache root. Defaults to ``config.CACHE_DIR``.
        region: Bucket region.
    """

    def __init__(
        self,
        bucket: str | None = None,
        cache_dir: Path | None = None,
        region: str | None = None,
    ) -> None:
        self.bucket = bucket or config.S3_BUCKET
        self.cache_dir = Path(cache_dir or config.CACHE_DIR)
        self.client = boto3.client(
            "s3",
            region_name=region or config.S3_REGION,
            config=Config(
                connect_timeout=20,
                read_timeout=300,
                retries={"max_attempts": 8, "mode": "adaptive"},
                max_pool_connections=32,
            ),
        )

    # ------------------------------------------------------------------ cache
    def _cache_path(self, key: str) -> Path:
        return self.cache_dir / key

    def _etag_path(self, key: str) -> Path:
        return self.cache_dir / ".etags" / (key + ".etag")

    def _write_cache(self, key: str, data: bytes, etag: str) -> None:
        path = self._cache_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        etag_path = self._etag_path(key)
        etag_path.parent.mkdir(parents=True, exist_ok=True)
        etag_path.write_text(etag)

    def uri(self, key: str) -> str:
        """Return ``s3://bucket/key`` for logs."""
        return f"s3://{self.bucket}/{key}"

    # ------------------------------------------------------------------ bytes
    def put_bytes(self, key: str, data: bytes, content_type: str | None = None) -> str:
        """Upload bytes and mirror them into the cache.

        Args:
            key: Object key.
            data: Payload.
            content_type: Optional MIME type.

        Returns:
            The stored object's ETag.

        Raises:
            S3Error: If the upload fails.
        """
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Key": key, "Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        try:
            resp = self.client.put_object(**kwargs)
        except (ClientError, BotoCoreError) as exc:
            raise S3Error(f"Failed to write {self.uri(key)}: {exc}") from exc
        etag = resp.get("ETag", "").strip('"')
        self._write_cache(key, data, etag)
        logger.debug("put %d bytes -> %s", len(data), self.uri(key))
        return etag

    def get_bytes(self, key: str) -> bytes | None:
        """Read an object, preferring a valid cached copy.

        Args:
            key: Object key.

        Returns:
            The payload, or None if the object does not exist.

        Raises:
            S3Error: On any failure other than a missing object.
        """
        cached = self._cache_path(key)
        if cached.exists() and key.startswith(_IMMUTABLE_PREFIXES):
            return cached.read_bytes()

        if cached.exists() and self._etag_path(key).exists():
            remote = self.head_etag(key)
            if remote is None:
                return None
            if remote == self._etag_path(key).read_text():
                return cached.read_bytes()

        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in _MISSING:
                return None
            raise S3Error(f"Failed to read {self.uri(key)}: {exc}") from exc
        except BotoCoreError as exc:
            raise S3Error(f"Failed to read {self.uri(key)}: {exc}") from exc
        data = resp["Body"].read()
        self._write_cache(key, data, resp.get("ETag", "").strip('"'))
        return data

    def head_etag(self, key: str) -> str | None:
        """Return the object's ETag, or None if it does not exist."""
        try:
            resp = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in _MISSING:
                return None
            raise S3Error(f"Failed to stat {self.uri(key)}: {exc}") from exc
        return resp.get("ETag", "").strip('"')

    def list_keys(self, prefix: str) -> list[str]:
        """List every key under a prefix (paginated)."""
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        except (ClientError, BotoCoreError) as exc:
            raise S3Error(f"Failed to list {self.uri(prefix)}: {exc}") from exc
        return keys

    # ------------------------------------------------------------- typed I/O
    def put_json_gz(self, key: str, obj: Any) -> str:
        """Serialize ``obj`` as gzipped JSON."""
        payload = gzip.compress(json.dumps(obj, separators=(",", ":")).encode(), mtime=0)
        return self.put_bytes(key, payload, "application/gzip")

    def get_json_gz(self, key: str) -> Any | None:
        """Read gzipped JSON, or None if absent."""
        data = self.get_bytes(key)
        return None if data is None else json.loads(gzip.decompress(data))

    def put_parquet(self, key: str, df: pl.DataFrame) -> str:
        """Write a DataFrame as zstd parquet."""
        buf = io.BytesIO()
        df.write_parquet(buf, compression="zstd")
        etag = self.put_bytes(key, buf.getvalue(), "application/vnd.apache.parquet")
        logger.info("wrote %s rows -> %s", f"{df.height:,}", self.uri(key))
        return etag

    def get_parquet(self, key: str) -> pl.DataFrame | None:
        """Read a parquet object, or None if absent."""
        data = self.get_bytes(key)
        return None if data is None else pl.read_parquet(io.BytesIO(data))

    def read_parquet_required(self, key: str) -> pl.DataFrame:
        """Read a parquet object that must exist.

        Raises:
            FileNotFoundError: If the object is missing.
        """
        df = self.get_parquet(key)
        if df is None:
            raise FileNotFoundError(f"{self.uri(key)} does not exist; run the step that builds it first")
        return df
