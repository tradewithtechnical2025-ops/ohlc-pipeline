"""
r2_manifest.py
==============
Generic manifest helper for TradeWithTech pipelines.

Unlike the earlier boto3-based draft, this version is transport-agnostic:
it doesn't care HOW your pipeline uploads to R2 (Worker HTTP API, boto3,
whatever) -- it just wraps whatever upload function you already have.

This matches the pattern seen in fetch scripts like the Finedge peers
pipeline, where uploads go through a Cloudflare Worker:

    async def r2_upload(client, filename, data):
        url = f"{WORKER_URL}?file={filename}"
        r = await client.post(url, headers=WORKER_HEADERS,
                               content=json.dumps(data).encode(), timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"{filename} upload failed")

Usage
-----
    from r2_manifest import upload_with_manifest

    # BEFORE:
    # await r2_upload(client, "peers.json", output)

    # AFTER:
    manifest = await upload_with_manifest(
        client, r2_upload, "peers.json", output,
        schema_v=1, extra_meta={"symbol_count": len(output)}
    )
    print(f"✅ peers.json uploaded (hash={manifest['hash']})")

If a pipeline instead uses a sync boto3 client, write a tiny async wrapper
around put_object and pass that as upload_fn -- the manifest logic itself
doesn't change.
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional


def compute_hash(data: Any) -> str:
    """
    IMPORTANT: this must mirror the EXACT serialization your upload_fn uses,
    otherwise the hash won't reflect what's actually stored on R2.
    TradeWithTech's r2_upload() does `json.dumps(data).encode()` (default
    separators, no sort_keys) -- this matches that exactly.
    """
    return hashlib.md5(json.dumps(data).encode()).hexdigest()[:10]


def manifest_filename_for(filename: str) -> str:
    """peers.json -> peers.manifest.json"""
    if filename.endswith(".json"):
        return filename[: -len(".json")] + ".manifest.json"
    return filename + ".manifest.json"


def build_manifest(data: Any, schema_v: int = 1, extra_meta: Optional[dict] = None) -> dict:
    manifest = {
        "hash": compute_hash(data),
        "schema_v": schema_v,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra_meta:
        manifest.update(extra_meta)
    return manifest


async def upload_with_manifest(
    client,
    upload_fn: Callable[..., Awaitable[None]],
    filename: str,
    data: Any,
    schema_v: int = 1,
    extra_meta: Optional[dict] = None,
) -> dict:
    """
    Uploads `data` via the pipeline's existing upload_fn (same signature as
    your current r2_upload: upload_fn(client, filename, data)), then uploads
    a sibling manifest file the same way.

    Returns the manifest dict (useful for logging, e.g. hash in print statement).
    """
    manifest = build_manifest(data, schema_v=schema_v, extra_meta=extra_meta)

    await upload_fn(client, filename, data)
    await upload_fn(client, manifest_filename_for(filename), manifest)

    return manifest
