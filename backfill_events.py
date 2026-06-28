#!/usr/bin/env python3
"""
One-time backfill script.
1. Fixes tag: "OTHER" → "IPO" where ISIN matches ipo_data.json
2. Adds missing NEW_BSE_LISTING for stocks that have bse_code but no BSE event
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
        events   = await r2_download(client, "reports/events.json")    or []
        ipo_resp = await r2_download(client, "ipo_data.json")          or {}
        bse_snap = await r2_download(client, "snapshots/upstox_bse.json") or {}

        ipo_raw = ipo_resp.get("ipos", ipo_resp) if isinstance(ipo_resp, dict) else ipo_resp

        print(f"   events.json     : {len(events)} entries")
        print(f"   ipo_data.json   : {len(ipo_raw)} IPOs")
        print(f"   upstox_bse.json : {len(bse_snap)} tokens")

        ipo_by_isin = {x["isin"]: x for x in ipo_raw if x.get("isin")}
        bse_by_isin = {v["isin"]: v for v in bse_snap.values() if v.get("isin")}

        tag_fixed       = 0
        bse_added       = 0
        existing_isins  = {e.get("isin") for e in events if e.get("event") == "NEW_BSE_LISTING"}

        new_bse_events = []

        for ev in events:
            event_type = ev.get("event", "")
            isin       = ev.get("isin")

            # Fix tag: OTHER → IPO where ISIN matches ipo_data
            if event_type in ("NEW_BSE_LISTING", "NEW_NSE_LISTING"):
                if isin and isin in ipo_by_isin and ev.get("tag") != "IPO":
                    ev["tag"] = "IPO"
                    tag_fixed += 1

            # Add missing NEW_BSE_LISTING for NEW_NSE_LISTING with bse_code
            if event_type == "NEW_NSE_LISTING" and isin and ev.get("bse_code"):
                if isin not in existing_isins:
                    bse = bse_by_isin.get(isin)
                    if bse:
                        new_bse_events.append({
                            "event":    "NEW_BSE_LISTING",
                            "date":     ev["date"],
                            "symbol":   bse["trading_symbol"],
                            "name":     bse["name"],
                            "isin":     isin,
                            "bse_code": bse["exchange_token"],
                            "segment":  bse["segment"],
                            "tag":      "IPO" if isin in ipo_by_isin else "OTHER",
                            "source":   "backfill",
                        })
                        existing_isins.add(isin)
                        bse_added += 1

        events.extend(new_bse_events)

        print(f"\n🔧 tag fixed (OTHER→IPO) : {tag_fixed}")
        print(f"🔧 NEW_BSE_LISTING added : {bse_added}")
        if new_bse_events:
            for e in new_bse_events:
                print(f"   + {e['symbol']} ({e['date']}) bse_code={e['bse_code']}")

        if tag_fixed == 0 and bse_added == 0:
            print("✅ Nothing to update")
            return

        await r2_upload(client, "reports/events.json", events)
        print("\n🎉 Backfill complete.")


if __name__ == "__main__":
    asyncio.run(main())
