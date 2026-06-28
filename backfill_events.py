#!/usr/bin/env python3
"""
Backfill script — runs as needed.
1. Fixes tag: OTHER/IPO → IPO_MAINBOARD or IPO_SME
2. Adds missing NEW_BSE_LISTING for stocks in ipo_data.json with BSE exchange
3. Adds missing NEW_NSE_LISTING for stocks in ipo_data.json with NSE exchange
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
RETENTION_DAYS = 180

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


def cutoff():
    return (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")


async def main():
    async with httpx.AsyncClient() as client:

        print("📥 Loading data from R2...")
        events   = await r2_download(client, "reports/events.json")    or []
        ipo_resp = await r2_download(client, "ipo_data.json")          or {}
        bse_snap = await r2_download(client, "snapshots/upstox_bse.json") or {}
        nse_snap = await r2_download(client, "snapshots/upstox_nse.json") or {}

        ipo_raw = ipo_resp.get("ipos", ipo_resp) if isinstance(ipo_resp, dict) else ipo_resp

        print(f"   events.json     : {len(events)} entries")
        print(f"   ipo_data.json   : {len(ipo_raw)} IPOs")
        print(f"   upstox_bse.json : {len(bse_snap)} tokens")
        print(f"   upstox_nse.json : {len(nse_snap)} ISINs")

        ipo_by_isin   = {x["isin"]: x for x in ipo_raw if x.get("isin")}
        bse_by_isin   = {v["isin"]: v for v in bse_snap.values() if v.get("isin")}
        nse_by_isin   = nse_snap  # already keyed by isin

        def ipo_tag(isin):
            return "IPO_SME" if ipo_by_isin.get(isin, {}).get("issue_type") == "sme" else "IPO_MAINBOARD"

        cut = cutoff()
        tag_fixed       = 0
        bse_added       = 0
        nse_added       = 0

        # Build sets of already-recorded ISINs per event type
        bse_recorded = {e.get("isin") for e in events if e.get("event") == "NEW_BSE_LISTING" and e.get("isin")}
        nse_recorded = {e.get("isin") for e in events if e.get("event") in ("NEW_NSE_LISTING", "BSE_TO_NSE", "SME_TO_NSE", "SME_TO_MAINBOARD_NSE") and e.get("isin")}

        new_events = []

        for ev in events:
            event_type = ev.get("event", "")
            isin       = ev.get("isin")

            # Fix tag: OTHER/IPO → correct tag
            if event_type in ("NEW_BSE_LISTING", "NEW_NSE_LISTING") and isin and isin in ipo_by_isin:
                correct = ipo_tag(isin)
                if ev.get("tag") != correct:
                    ev["tag"] = correct
                    tag_fixed += 1

        # Add missing NEW_BSE_LISTING from ipo_data
        for ipo in ipo_raw:
            isin         = ipo.get("isin")
            listing_date = ipo.get("listing_date") or ""
            exchanges    = (ipo.get("listing_exchange") or "").upper()

            if not isin or not listing_date or not exchanges:
                continue
            if listing_date < cut:
                continue

            on_bse = "BSE" in exchanges
            on_nse = "NSE" in exchanges

            if on_bse and isin not in bse_recorded:
                bse = bse_by_isin.get(isin)
                if bse:
                    new_events.append({
                        "event":    "NEW_BSE_LISTING",
                        "date":     listing_date,
                        "symbol":   bse["trading_symbol"],
                        "name":     bse["name"],
                        "isin":     isin,
                        "bse_code": bse["exchange_token"],
                        "segment":  bse["segment"],
                        "tag":      ipo_tag(isin),
                        "source":   "backfill",
                    })
                    bse_recorded.add(isin)
                    bse_added += 1

            if on_nse and isin not in nse_recorded:
                nse = nse_by_isin.get(isin)
                if nse:
                    bse = bse_by_isin.get(isin)
                    ev_nse = {
                        "event":   "NEW_NSE_LISTING",
                        "date":    listing_date,
                        "symbol":  nse["symbol"],
                        "name":    nse["name"],
                        "isin":    isin,
                        "segment": nse["segment"],
                        "tag":     ipo_tag(isin),
                        "source":  "backfill",
                    }
                    if bse:
                        ev_nse["bse_code"] = bse["exchange_token"]
                    new_events.append(ev_nse)
                    nse_recorded.add(isin)
                    nse_added += 1

        events.extend(new_events)

        print(f"\n🔧 tag fixed            : {tag_fixed}")
        print(f"🔧 NEW_BSE_LISTING added : {bse_added}")
        print(f"🔧 NEW_NSE_LISTING added : {nse_added}")

        if tag_fixed == 0 and bse_added == 0 and nse_added == 0:
            print("✅ Nothing to update")
            return

        if new_events:
            for e in new_events[:10]:
                print(f"   + {e['event']} {e['symbol']} ({e['date']}) [{e.get('tag','')}]")
            if len(new_events) > 10:
                print(f"   ... and {len(new_events)-10} more")

        await r2_upload(client, "reports/events.json", events)
        print("\n🎉 Backfill complete.")


if __name__ == "__main__":
    asyncio.run(main())
