#!/usr/bin/env python3

import asyncio
import json
import os
from datetime import datetime

import httpx

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

CONCURRENCY = 4
RATE_DELAY  = 0.25
RETRY       = 3

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# R2 Helpers
# ─────────────────────────────────────────────

async def r2_download(client, filename):

    url = f"{WORKER_URL}/{filename}"

    r = await client.get(
        url,
        headers=WORKER_HEADERS,
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} download failed")

    return r.json()


async def r2_upload(client, filename, data):

    url = f"{WORKER_URL}?file={filename}"

    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed")


# ─────────────────────────────────────────────
# Finedge Helper
# ─────────────────────────────────────────────

async def finedge_get(client, sem, path, params):

    params["token"] = FINEDGE_TOKEN

    url = f"{FINEDGE_BASE}/{path}"

    async with sem:

        for attempt in range(RETRY):

            await asyncio.sleep(RATE_DELAY)

            try:

                r = await client.get(
                    url,
                    params=params,
                    timeout=30,
                )

            except Exception as e:

                print(f"Network Error: {e}")

                await asyncio.sleep(2 ** attempt)

                continue

            if r.status_code == 429:

                print("429 Rate Limit")

                await asyncio.sleep(15)

                continue

            if r.status_code != 200:
                return None

            try:
                return r.json()

            except Exception:
                return None

    return None


# ─────────────────────────────────────────────
# Shareholding Parser
# ─────────────────────────────────────────────

def parse_shareholding(symbol, data):

    rows = data.get("rows") or []
    cols = (data.get("columns") or [])[:8]

    if not rows or not cols:

        return {
            "updated"  : datetime.now().strftime("%Y-%m-%d"),
            "quarters" : [],
            "promoter" : [],
            "fii"      : [],
            "dii"      : [],
            "public"   : [],
        }

    # ─────────────────────────────────────────

    def clean_text(v):
        return str(v).lower().strip()

    # ─────────────────────────────────────────

    def find_row(targets):

        for row in rows:

            name = clean_text(row.get("name"))
            cat  = clean_text(row.get("catagory"))

            text = f"{name} {cat}"

            for t in targets:

                t = clean_text(t)

                # exact match first
                if name == t or cat == t:

                    d = row.get("data") or {}

                    return [d.get(q) for q in cols]

                # contains fallback
                if t in text:

                    d = row.get("data") or {}

                    return [d.get(q) for q in cols]

        return []

    # ─────────────────────────────────────────
    # Strict Matching
    # ─────────────────────────────────────────

    promoter = find_row([
        "promoter indian",
        "promoter"
    ])

    fii = find_row([
        "institutions foreign",
        "foreign institutions",
        "foreign",
        "fii"
    ])

    dii = find_row([
        "institutions domestic",
        "domestic institutions",
        "domestic",
        "dii"
    ])

    public = find_row([
        "non institutions",
        "non-institutions",
        "public"
    ])

    # ─────────────────────────────────────────
    # Validation
    # ─────────────────────────────────────────

    try:

        total = (
            (promoter[0] if promoter else 0) +
            (fii[0] if fii else 0) +
            (dii[0] if dii else 0) +
            (public[0] if public else 0)
        )

        if total > 130:

            print(
                f"⚠ BAD DATA {symbol} | "
                f"P={promoter[:1]} "
                f"F={fii[:1]} "
                f"D={dii[:1]} "
                f"PUB={public[:1]}"
            )

    except Exception:
        pass

    # ─────────────────────────────────────────

    return {
        "updated"  : datetime.now().strftime("%Y-%m-%d"),
        "quarters" : cols,
        "promoter" : promoter,
        "fii"      : fii,
        "dii"      : dii,
        "public"   : public,
    }


# ─────────────────────────────────────────────
# Fetch One
# ─────────────────────────────────────────────

async def fetch_one(client, sem, symbol):

    data = await finedge_get(
        client,
        sem,
        f"shareholdings/pattern/{symbol}",
        {"period": "quarterly"}
    )

    if not data:
        return symbol, None

    try:

        parsed = parse_shareholding(symbol, data)

        return symbol, parsed

    except Exception as e:

        print(f"Parse Error {symbol}: {e}")

        return symbol, None


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        master = await r2_download(
            client,
            "master.json"
        )

        symbols = [
            x["symbol"]
            for x in master
            if x.get("exchange") == "NSE"
        ]

        sem = asyncio.Semaphore(CONCURRENCY)

        output = {}

        total = len(symbols)

        for i in range(0, total, 25):

            batch = symbols[i:i+25]

            tasks = [
                fetch_one(client, sem, s)
                for s in batch
            ]

            results = await asyncio.gather(*tasks)

            for sym, data in results:

                if data:

                    output[sym] = data

                    print(f"✓ {sym}")

                else:

                    print(f"✗ {sym}")

            print(f"{min(i+25, total)}/{total}")

        await r2_upload(
            client,
            "shareholding.json",
            output
        )

        print("✅ shareholding.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
