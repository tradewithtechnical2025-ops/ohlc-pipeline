#!/usr/bin/env python3
"""
BSE Classification — sector/industry from Finedge company-profile API.
Profile jo deta hai wahi store hota hai (koi manual mapping nahi).

- Saare BSE stocks INCLUDE hote hain (jinka profile nahi, woh blank sector +
  profile_found=False ke saath, naam bse.json se).
- Re-run safe: jinka profile mil chuka (profile_found=True) woh skip; baaki
  (naye + failed) dobara attempt — transient recover + future fill.

Input : bse.json   (R2)
Output: bse_classification.json (R2)
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
    """Hamesha ek record return karta hai. Profile na mile to blank sector."""
    sym = stock["symbol"]

    base = {
        "symbol":        sym,
        "bse_code":      stock.get("bse_code"),
        "trading_symbol":  stock.get("trading_symbol"),
        "exchange":      "BSE",
        "name":          stock.get("name", ""),     # naam universe se (profile se nahi)
        "sector":        "",
        "industry":      "",
        "sub_industry":  "",
        "macro_sector":  "",
        "market_cap_cr": stock.get("market_cap_cr"),
        "profile_found": False,
    }

    d = await finedge_get(client, sem, f"company-profile/{sym}")
    if not d:
        return sym, base                            # no profile -> include blank

    # Profile jo deta hai wahi — no manual modification
    base.update({
        "name":          d.get("name") or base["name"],
        "sector":        d.get("sector", ""),
        "industry":      d.get("industry", ""),
        "sub_industry":  d.get("sub_industry", ""),
        "macro_sector":  d.get("macro_sector", ""),
        "market_cap_cr": d.get("market_cap") if d.get("market_cap") is not None
                         else base["market_cap_cr"],
        "profile_found": True,
    })
    return sym, base


async def main():
    async with httpx.AsyncClient() as client:

        bse = await r2_download(client, BSE_FILE)
        if not isinstance(bse, list) or not bse:
            raise RuntimeError("bse.json not found / empty in R2")
        print(f"📋 BSE universe: {len(bse)} stocks")

        # Existing load (dict by symbol)
        existing = await r2_download(client, OUT_FILE)
        by_sym = {}
        if isinstance(existing, list):
            by_sym = {x.get("symbol"): x for x in existing if x.get("symbol")}

        # Profile mil chuka = skip. Naye + blank (profile_found False) = dobara attempt.
        done_ok = {s for s, r in by_sym.items() if r.get("profile_found")}
        todo = [s for s in bse if s.get("symbol") not in done_ok]
        print(f"With profile (skip): {len(done_ok)}  To attempt: {len(todo)}")

        if not todo:
            print("✅ Sab ke profile already mil chuke!")
            return

        sem = asyncio.Semaphore(CONCURRENCY)
        got = blank = 0

        for i in range(0, len(todo), BATCH):
            batch   = todo[i:i + BATCH]
            results = await asyncio.gather(*[fetch_profile(client, sem, s) for s in batch])
            for sym, rec in results:
                by_sym[sym] = rec                   # include hamesha (update bhi)
                if rec["profile_found"]:
                    got += 1
                else:
                    blank += 1
            done_n = min(i + BATCH, len(todo))
            print(f"  {done_n}/{len(todo)}  profile✓{got}  blank✗{blank}")
            await r2_upload(client, OUT_FILE, list(by_sym.values()))   # checkpoint

        await r2_upload(client, OUT_FILE, list(by_sym.values()))
        total       = len(by_sym)
        with_prof   = sum(1 for r in by_sym.values() if r.get("profile_found"))
        print(f"🎉 BSE classification → {OUT_FILE}")
        print(f"   Total: {total}  |  with profile: {with_prof}  |  blank: {total - with_prof}")


if __name__ == "__main__":
    asyncio.run(main())
