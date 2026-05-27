import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

import httpx

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

CONCURRENCY  = 4
RATE_DELAY   = 0.25
RETRY        = 3

HERE = Path(__file__).parent

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

async def r2_download(client, filename):
    url = f"{WORKER_URL}/{filename}"
    r = await client.get(url, headers=WORKER_HEADERS)

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


async def finedge_get(client, sem, path, params):
    params["token"] = FINEDGE_TOKEN

    url = f"{FINEDGE_BASE}/{path}"

    async with sem:
        for attempt in range(RETRY):

            await asyncio.sleep(RATE_DELAY)

            try:
                r = await client.get(url, params=params, timeout=30)

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


# ─────────────────────────────────────────────
# Shareholding Parser
# ─────────────────────────────────────────────

def parse_shareholding(data):

    rows = data.get("rows", [])
    cols = data.get("columns", [])[:8]

    def find_row(keywords):

        for row in rows:

            text = (
                f"{row.get('name','')} "
                f"{row.get('catagory','')}"
            ).lower()

            if any(k in text for k in keywords):

                d = row.get("data", {})

                return [d.get(q) for q in cols]

        return []

    promoter = find_row(["promoter"])
    fii       = find_row(["foreign", "fii"])
    dii       = find_row(["domestic", "dii"])
    public    = find_row(["public", "non-institutions"])

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

    parsed = parse_shareholding(data)

    return symbol, parsed


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        master = await r2_download(client, "master.json")

        symbols = [
            x["symbol"]
            for x in master
            if x.get("exchange") == "NSE"
        ]

        sem = asyncio.Semaphore(CONCURRENCY)

        output = {}

        for i in range(0, len(symbols), 25):

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

            print(f"{min(i+25, len(symbols))}/{len(symbols)}")

        await r2_upload(
            client,
            "shareholding.json",
            output
        )

        print("✅ shareholding.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
