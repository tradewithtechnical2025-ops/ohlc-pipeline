#!/usr/bin/env python3

import asyncio
import json
import os

import httpx

# =========================================================
# CONFIG
# =========================================================

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

MASTER_URL = f"{WORKER_URL}/master.json"

OUTPUT_FILE = "classification.json"

CONCURRENCY = 10
RATE_DELAY = 0.15
RETRY = 3

MIN_MARKET_CAP_CR = 50

# =========================================================
# HEADERS
# =========================================================

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================================================
# PROFILE API
# =========================================================

PROFILE_URL = (
    "https://data.finedgeapi.com/api/v1/stock/profile"
)

# =========================================================
# DOWNLOAD MASTER
# =========================================================

async def load_master(client):

    print("Downloading master.json...")

    r = await client.get(
        MASTER_URL,
        timeout=120
    )

    r.raise_for_status()

    data = r.json()

    print(f"Loaded {len(data)} stocks")

    return data

# =========================================================
# PROFILE FETCH
# =========================================================

async def fetch_profile(client, symbol, semaphore):

    params = {
        "token": FINEDGE_TOKEN,
        "symbol": symbol
    }

    async with semaphore:

        for attempt in range(RETRY):

            await asyncio.sleep(RATE_DELAY)

            try:

                r = await client.get(
                    PROFILE_URL,
                    params=params,
                    timeout=60
                )

            except Exception:

                await asyncio.sleep(2 ** attempt)
                continue

            if r.status_code == 429:

                print(f"{symbol} -> 429")

                await asyncio.sleep(10)
                continue

            if r.status_code != 200:

                print(f"{symbol} -> HTTP {r.status_code}")
                return None

            try:

                return r.json()

            except Exception:
                return None

    return None

# =========================================================
# PROCESS STOCK
# =========================================================

async def process_stock(client, stock, semaphore):

    symbol = stock["symbol"]

    profile = await fetch_profile(
        client,
        symbol,
        semaphore
    )

    if not profile:
        return None

    market_cap = (
        profile.get("market_cap")
        or 0
    )

    try:
        market_cap = float(market_cap)
    except:
        market_cap = 0

    # Skip tiny illiquid companies
    if market_cap < MIN_MARKET_CAP_CR:
        return None

    return {

        "symbol": symbol,

        "name": stock.get("name"),

        "exchange": stock.get("exchange"),

        "market_cap_cr": market_cap,

        "sector": profile.get("sector"),

        "industry": profile.get("industry"),

        "sub_industry": profile.get(
            "sub_industry"
        ),

        "consolidated_ind": stock.get(
            "consolidated_ind",
            False
        )
    }

# =========================================================
# WORKER UPLOAD
# =========================================================

async def r2_upload(client, filename, data):

    url = f"{WORKER_URL}?file={filename}"

    payload = json.dumps(data)

    r = await client.post(
        url,
        headers={
            "X-Secret-Token": WORKER_TOKEN,
            "Content-Type": "application/json",
        },
        content=payload.encode(),
        timeout=120,
    )

    if r.status_code != 200:

        raise RuntimeError(
            f"{filename} upload failed"
        )

    print(f"Uploaded {filename}")

# =========================================================
# MAIN
# =========================================================

async def main():

    semaphore = asyncio.Semaphore(
        CONCURRENCY
    )

    async with httpx.AsyncClient(
        headers=HEADERS
    ) as client:

        master = await load_master(client)

        tasks = [

            process_stock(
                client,
                stock,
                semaphore
            )

            for stock in master
        ]

        results = await asyncio.gather(
            *tasks
        )

        classification = [
            x for x in results
            if x
        ]

        classification.sort(
            key=lambda x: x["market_cap_cr"],
            reverse=True
        )

        print()
        print("=== SUMMARY ===")
        print(
            f"Final Stocks: {len(classification)}"
        )

        await r2_upload(
            client,
            OUTPUT_FILE,
            classification
        )

        print()
        print("classification.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())

