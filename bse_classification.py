#!/usr/bin/env python3
"""
BSE Classification — sector/industry from Finedge company-profile API.
Profile jo deta hai wahi store hota hai (koi manual mapping nahi).

Input : bse.json   (R2)  — BSE universe (price>20, mcap>=100cr)
Output: bse_classification.json (R2)

Re-run safe: pehle se done stocks skip, har batch pe checkpoint upload.
"""

import asyncio
import json
import os

import httpx

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

BSE_FILE = "bse.json"
OUT_FILE = "bse_classification.json"

CONCURRENCY = 4
RATE_DELAY  = 0.20
RETRY       = 3
BATCH       = 40

DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}


async def finedge_get(client, sem, path):
    url = f"{FINEDGE_BASE}/{path}"
    params = {"token": FINEDGE_TOKEN}
    async with sem:
        for attempt in range(RETRY):
            await asyncio.sleep(RATE_DELAY)
            try:
                r = await client.get(url, params=params, timeout=60)
            except Exception:
                await asyncio.sleep(2 ** attempt)
                continue
            if r.status_code == 429:
                await asyncio.sleep(15)
                continue
            if r.status_code != 200:
                return None
            try:
                return r.json()
            except Exception:
                return None
    return None


async def r2_download(client, filename):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=120)
    if r.status_code != 200:
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


async def fetch_profile(client, sem, stock):
    sym = stock["symbol"]
    d = await finedge_get(client, sem, f"company-profile/{sym}")
    if not d:
        return sym, None
    # Profile jo deta hai wahi — no manual modification
    return sym, {
        "symbol":        sym,
        "bse_code":      stock.get("bse_code"),
        "exchange":      "BSE",
        "name":          d.get("name") or stock.get("name", ""),
        "sector":        d.get("sector", ""),
        "industry":      d.get("industry", ""),
        "sub_industry":  d.get("sub_industry", ""),
        "macro_sector":  d.get("macro_sector", ""),
        "market_cap_cr": d.get("market_cap") if d.get("market_cap") is not None
                         else stock.get("market_cap_cr"),
    }


async def main():
    async with httpx.AsyncClient() as client:

        bse = await r2_download(client, BSE_FILE)
        if not isinstance(bse, list) or not bse:
            raise RuntimeError("bse.json not found / empty in R2")
        print(f"📋 BSE universe: {len(bse)} stocks")

        # Re-run safe: existing classification load karo
        existing = await r2_download(client, OUT_FILE)
        out  = list(existing) if isinstance(existing, list) else []
        done = {x.get("symbol") for x in out}
        todo = [s for s in bse if s.get("symbol") not in done]
        print(f"Already done: {len(done)}  Remaining: {len(todo)}")

        if not todo:
            print("✅ Sab already done!")
            return

        sem = asyncio.Semaphore(CONCURRENCY)
        ok = fail = 0

        for i in range(0, len(todo), BATCH):
            batch   = todo[i:i + BATCH]
            results = await asyncio.gather(*[fetch_profile(client, sem, s) for s in batch])
            for sym, rec in results:
                if rec:
                    out.append(rec); ok += 1
                else:
                    fail += 1; print(f"  ✗ {sym}: no profile")
            done_n = min(i + BATCH, len(todo))
            print(f"  {done_n}/{len(todo)}  ✓{ok}  ✗{fail}")
            # checkpoint upload (crash-safe)
            await r2_upload(client, OUT_FILE, out)

        # final upload
        await r2_upload(client, OUT_FILE, out)
        print(f"🎉 BSE classification: ✓{ok}  ✗{fail}  → {OUT_FILE} ({len(out)} total)")


if __name__ == "__main__":
    asyncio.run(main())
