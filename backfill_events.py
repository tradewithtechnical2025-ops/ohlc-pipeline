#!/usr/bin/env python3
"""
One-time backfill script.
Adds missing `tag` and `bse_code` fields to existing events.json entries.

Run once:
    WORKER_URL=... WORKER_TOKEN=... python3 backfill_events.py
"""

import asyncio
import json
import os

import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}


async def r2_download(client, filename):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=120)
    if r.status_code != 200:
        print(f"⚠️  {filename} not found (status {r.status_code})")
        return None
    try:
        return r.json()
    except Exception:
        return None


async def r2_upload(client, filename, data):
    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers=UP_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed: {r.status_code}")
    print(f"✅ Uploaded {filename}")


async def main():
    async with httpx.AsyncClient() as client:

        print("📥 Loading data from R2...")
        events  = await r2_download(client, "reports/events.json")  or []
        ipo_raw = await r2_download(client, "reports/ipo.json")      or []
        bse_snap= await r2_download(client, "snapshots/upstox_bse.json") or {}

        print(f"   events.json     : {len(events)} entries")
        print(f"   ipo.json        : {len(ipo_raw)} entries")
        print(f"   upstox_bse.json : {len(bse_snap)} tokens")

        # Build lookup maps
        ipo_by_isin    = {x["isin"]: x for x in ipo_raw if x.get("isin")}
        bse_by_isin    = {v["isin"]: v["exchange_token"] for v in bse_snap.values() if v.get("isin")}

        tag_added      = 0
        bse_code_added = 0

        for ev in events:
            event_type = ev.get("event", "")
            isin       = ev.get("isin")

            # ── Add tag to NEW_BSE_LISTING / NEW_NSE_LISTING ──────────
            if event_type in ("NEW_BSE_LISTING", "NEW_NSE_LISTING") and "tag" not in ev:
                ev["tag"] = "IPO" if (isin and isin in ipo_by_isin) else "OTHER"
                tag_added += 1

            # ── Add bse_code where missing ────────────────────────────
            # BSE_TO_NSE already has it from pipeline; fill for NEW_NSE_LISTING
            if "bse_code" not in ev and isin and isin in bse_by_isin:
                ev["bse_code"] = bse_by_isin[isin]
                bse_code_added += 1

        print(f"\n🔧 tag added      : {tag_added}")
        print(f"🔧 bse_code added : {bse_code_added}")

        if tag_added == 0 and bse_code_added == 0:
            print("✅ Nothing to update — all entries already have tag + bse_code")
            return

        await r2_upload(client, "reports/events.json", events)
        print("\n🎉 Backfill complete.")


if __name__ == "__main__":
    asyncio.run(main())
