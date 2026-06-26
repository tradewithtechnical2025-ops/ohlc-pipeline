"""
manifest_utils.py
==================
Shared utility for TradeWithTech pipelines to write data files to R2
alongside a small sibling ".manifest.json" file containing a content
hash, schema version, and timestamp.

Drop this into any pipeline (pipeline.py, pipeline_rrg.py, pipeline_insider.py,
pipeline_news.py, bse_classification.py, bse_ohlc.py, bse_pattern.py, etc.)
and replace direct put_object() calls for frontend-consumed JSON datasets
with write_with_manifest() / write_json_with_manifest().

No central manifest, no coordination between pipelines needed --
each dataset's manifest lives next to its own data file, so pipelines
running at completely different times never touch each other's state.

Usage
-----
    from manifest_utils import write_json_with_manifest

    # BEFORE:
    # r2_client.put_object(Bucket=BUCKET, Key="bands.json", Body=json.dumps(bands).encode())

    # AFTER:
    write_json_with_manifest(r2_client, BUCKET, "bands.json", bands, schema_v=1)

If you're writing pre-serialized bytes (e.g. already have data_bytes from
a CSV->JSON step), use write_with_manifest() directly instead.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def _compute_hash(data_bytes: bytes) -> str:
    """Short, stable content hash. md5 is fine here -- this is only used
    for change-detection, not security."""
    return hashlib.md5(data_bytes).hexdigest()[:10]


def _manifest_key_for(key: str) -> str:
    """screener_feed.json   -> screener_feed.manifest.json
       ohlc/chunk_1.json    -> ohlc/chunk_1.manifest.json"""
    if key.endswith(".json"):
        return key[: -len(".json")] + ".manifest.json"
    return key + ".manifest.json"


def write_with_manifest(
    r2_client,
    bucket: str,
    key: str,
    data_bytes: bytes,
    schema_v: int = 1,
    content_type: str = "application/json",
    extra_meta: Optional[dict] = None,
) -> dict:
    """
    Push a data file to R2, then push its sibling manifest file.

    Args:
        r2_client:    boto3 S3 client configured for the R2 endpoint
        bucket:       R2 bucket name
        key:          object key for the data file, e.g. "screener_feed.json"
        data_bytes:   exact bytes being uploaded for the data file
        schema_v:     bump this whenever the JSON *structure* changes
                      (added/removed/renamed fields) -- forces the frontend
                      to refetch even if the content hash happens to match
        content_type: content-type for the data object
        extra_meta:   optional dict merged into the manifest
                      (e.g. {"row_count": 1485})

    Returns:
        The manifest dict that was written (handy for logging/tests).
    """
    r2_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data_bytes,
        ContentType=content_type,
    )

    manifest = {
        "hash": _compute_hash(data_bytes),
        "schema_v": schema_v,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "size_bytes": len(data_bytes),
    }
    if extra_meta:
        manifest.update(extra_meta)

    manifest_key = _manifest_key_for(key)
    manifest_bytes = json.dumps(manifest, separators=(",", ":")).encode("utf-8")

    r2_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json",
    )

    logger.info(
        "[manifest] %s -> hash=%s schema_v=%s size=%dB",
        key, manifest["hash"], schema_v, len(data_bytes),
    )

    return manifest


def write_json_with_manifest(
    r2_client,
    bucket: str,
    key: str,
    obj,
    schema_v: int = 1,
    extra_meta: Optional[dict] = None,
) -> dict:
    """
    Convenience wrapper: serialize a Python object to JSON and write it
    + its manifest in one call.

    Example:
        write_json_with_manifest(r2_client, "twt-data", "bands.json", bands_list, schema_v=1)
    """
    data_bytes = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return write_with_manifest(
        r2_client, bucket, key, data_bytes, schema_v=schema_v, extra_meta=extra_meta
    )


def read_manifest(r2_client, bucket: str, key: str) -> Optional[dict]:
    """
    Read back a manifest for a given data key. Returns None if no manifest
    exists yet (first run / new dataset).
    """
    manifest_key = _manifest_key_for(key)
    try:
        resp = r2_client.get_object(Bucket=bucket, Key=manifest_key)
        return json.loads(resp["Body"].read())
    except r2_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning("[manifest] could not read %s: %s", manifest_key, e)
        return None


def has_changed(r2_client, bucket: str, key: str, new_data_bytes: bytes) -> bool:
    """
    Check whether new_data_bytes differs from what's currently stored on
    R2, WITHOUT uploading anything. Useful inside a pipeline to skip an
    expensive downstream step (e.g. recompute breadth score, push insider
    notifications) when the underlying dataset hasn't actually changed.

    Example:
        new_bytes = json.dumps(bands_list).encode()
        if has_changed(r2_client, BUCKET, "bands.json", new_bytes):
            recompute_dependent_stuff()
        write_with_manifest(r2_client, BUCKET, "bands.json", new_bytes)
    """
    existing = read_manifest(r2_client, bucket, key)
    if existing is None:
        return True
    return existing.get("hash") != _compute_hash(new_data_bytes)
