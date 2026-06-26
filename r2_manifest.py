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


def compute_hash(data: Any, ensure_ascii: bool = True) -> str:
    """
    IMPORTANT: this must mirror the EXACT serialization your upload_fn uses,
    otherwise the hash won't reflect what's actually stored on R2.
    TradeWithTech's r2_upload() does `json.dumps(data).encode()` (default
    separators, no sort_keys, ensure_ascii=True) -- this matches that by default.
    Pass ensure_ascii=False if your pipeline's upload_fn serializes that way
    (e.g. json.dumps(data, ensure_ascii=False)) -- otherwise the hash would be
    computed over different bytes than what's actually uploaded.
    """
    return hashlib.md5(json.dumps(data, ensure_ascii=ensure_ascii).encode()).hexdigest()[:10]


def manifest_filename_for(filename: str) -> str:
    """peers.json -> peers.manifest.json"""
    if filename.endswith(".json"):
        return filename[: -len(".json")] + ".manifest.json"
    return filename + ".manifest.json"


def build_manifest(data: Any, schema_v: int = 1, extra_meta: Optional[dict] = None, ensure_ascii: bool = True) -> dict:
    manifest = {
        "hash": compute_hash(data, ensure_ascii=ensure_ascii),
        "schema_v": schema_v,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra_meta:
        manifest.update(extra_meta)
    return manifest


async def upload_str_with_manifest(
    client,
    upload_fn: Callable[..., Awaitable[None]],
    filename: str,
    json_str: str,
    schema_v: int = 1,
    extra_meta: Optional[dict] = None,
) -> dict:
    """
    Variant of upload_with_manifest() for pipelines that already serialize
    to a JSON string BEFORE calling their upload_fn -- e.g. pipeline.py's
    pattern of r2_upload(client, filename, json.dumps(...)), as opposed to
    passing a raw dict/list and letting upload_fn serialize internally.

    Hash is computed directly from json_str, so it always reflects exactly
    what gets uploaded -- no risk of a second, possibly-different serialization.

    Example (inside an asyncio.gather(...) batch, same as existing calls):

        from r2_manifest import upload_str_with_manifest

        await asyncio.gather(
            ...,
            upload_str_with_manifest(
                client, r2_upload, "screener_feed.json", json.dumps(screener_feed),
                schema_v=1, extra_meta={"stock_count": len(screener_feed)}
            ),
        )
    """
    manifest = {
        "hash": hashlib.md5(json_str.encode()).hexdigest()[:10],
        "schema_v": schema_v,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    if extra_meta:
        manifest.update(extra_meta)

    await upload_fn(client, filename, json_str)
    await upload_fn(client, manifest_filename_for(filename), json.dumps(manifest, separators=(",", ":")))

    return manifest


async def upload_with_manifest(
    client,
    upload_fn: Callable[..., Awaitable[None]],
    filename: str,
    data: Any,
    schema_v: int = 1,
    extra_meta: Optional[dict] = None,
    ensure_ascii: bool = True,
) -> dict:
    """
    Uploads `data` via the pipeline's existing upload_fn (same signature as
    your current r2_upload: upload_fn(client, filename, data)), then uploads
    a sibling manifest file the same way.

    Pass ensure_ascii=False if your upload_fn serializes with
    json.dumps(data, ensure_ascii=False) -- otherwise the manifest hash
    won't match the actually-uploaded bytes.

    Returns the manifest dict (useful for logging, e.g. hash in print statement).
    """
    manifest = build_manifest(data, schema_v=schema_v, extra_meta=extra_meta, ensure_ascii=ensure_ascii)

    await upload_fn(client, filename, data)
    await upload_fn(client, manifest_filename_for(filename), manifest)

    return manifest
