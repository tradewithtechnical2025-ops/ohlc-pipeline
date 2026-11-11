"""
backfill_vwap.py — one-time backfill of vwap_daily.json from NSE bhavcopies.

pipeline_master.py saves each day's VWAP from Upstox (average_price) from now on.
Upstox has no VWAP history, so this script fills the past days from NSE's
sec_bhavdata_full_DDMMYYYY.csv (AVG_PRICE = the day's VWAP, TTL_TRD_QNTY = volume).

- NSE stocks only (BSE-only stocks are not in the NSE file).
- Only stocks in master.json are kept, so the file stays the site's universe.
- Days already in vwap_daily.json are left as they are (never overwritten).
- Same format as pipeline_master.py: {"days": {"YYYY-MM-DD": {SYMBOL: [vwap, volume]}}}

Run once (needs WORKER_URL, WORKER_TOKEN; run from the folder with pipeline_nse.py):
    BACKFILL_DAYS=60 python backfill_vwap.py
"""

import asyncio
import os
from datetime import date, datetime, timedelta, timezone

import httpx

from pipeline_nse import BHAV_BASE, fetch_csv_url, fmt, is_trading_day, make_nse_session, prev_trading_day

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
VWAP_FILE     = "vwap_daily.json"
MASTER_FILE   = "master.json"
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "60"))      # trading days to fill
KEEP_DAYS     = int(os.environ.get("VWAP_KEEP_DAYS", "60"))     # same cap as pipeline_master.py
IST           = timezone(timedelta(hours=5, minutes=30))


def parse_bhav_vwap(rows: list[dict], universe: set[str]) -> dict:
    """{SYMBOL: [avg_price, volume]} for EQ/BE rows of one sec_bhavdata_full file.
    NSE's header has a leading space (' AVG_PRICE'), so keys and values are stripped."""
    out = {}
    for row in rows:
        r = {str(k).strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        if r.get("SERIES") not in ("EQ", "BE"):
            continue
        sym = (r.get("SYMBOL") or "").upper()
        if not sym or (universe and sym not in universe):
            continue
        try:
            avg = float(str(r.get("AVG_PRICE") or "0").replace(",", ""))
            vol = int(float(str(r.get("TTL_TRD_QNTY") or "0").replace(",", "")))
        except ValueError:
            continue
        if avg > 0 and vol > 0:
            out[sym] = [round(avg, 2), vol]
    return out


async def r2_get(client, name):
    r = await client.get(f"{WORKER_URL}/{name}", headers={"X-Secret-Token": WORKER_TOKEN}, timeout=120)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


async def r2_put(client, name, data):
    import json
    r = await client.post(f"{WORKER_URL}?file={name}",
                          headers={"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"},
                          content=json.dumps(data).encode(), timeout=180)
    r.raise_for_status()


async def main():
    async with httpx.AsyncClient() as client:
        master = await r2_get(client, MASTER_FILE) or []
        universe = {str(s.get("symbol") or "").upper() for s in master if s.get("symbol")}
        print(f"Universe from {MASTER_FILE}: {len(universe)} NSE symbols")
        hist = await r2_get(client, VWAP_FILE)
        days = (hist or {}).get("days") if isinstance(hist, dict) else None
        days = days if isinstance(days, dict) else {}
        print(f"{VWAP_FILE}: {len(days)} day(s) already saved {sorted(days)[-3:]}")

        nse = make_nse_session()
        today = datetime.now(IST).date()
        # Today's file is published ~8 pm IST; start from today only after that.
        cur = today if (is_trading_day(today) and datetime.now(IST).hour >= 21) else prev_trading_day(today)
        added = skipped = missing = 0
        tried = 0
        while added + skipped < BACKFILL_DAYS and tried < BACKFILL_DAYS + 15:
            tried += 1
            key = cur.isoformat()
            if key in days:
                skipped += 1
            else:
                try:
                    rows = fetch_csv_url(nse, f"{BHAV_BASE}/sec_bhavdata_full_{fmt(cur)}.csv", retries=3)
                except Exception as e:
                    rows = None
                    print(f"  ⚠ {key}: {e}")
                if rows:
                    data = parse_bhav_vwap(rows, universe)
                    if data:
                        days[key] = data
                        added += 1
                        print(f"  ✓ {key}: {len(data)} stocks")
                    else:
                        missing += 1
                else:
                    missing += 1
                    print(f"  · {key}: no file (holiday?)")
                await asyncio.sleep(0.5)
            cur = prev_trading_day(cur)
        nse.close()

        for old in sorted(days)[:-KEEP_DAYS]:
            days.pop(old, None)
        await r2_put(client, VWAP_FILE, {"updated": datetime.now(IST).isoformat(timespec="seconds"),
                                         "fields": ["vwap", "volume"], "days": days})
        print(f"✅ {VWAP_FILE}: +{added} day(s) from NSE bhavcopy, {skipped} already present, "
              f"{missing} without a file | now {len(days)} days ({min(days)} → {max(days)})")


if __name__ == "__main__":
    asyncio.run(main())
