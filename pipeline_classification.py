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

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

OUTPUT_FILE = "classification.json"

CONCURRENCY = 5
BATCH_SIZE  = 50

RATE_DELAY = 0.15
RETRY = 3

MIN_MARKET_CAP_CR = 50

# =========================================================
# HEADERS
# =========================================================

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}

# =========================================================
# PROFILE API
# =========================================================

PROFILE_URL = (
    "https://data.finedgeapi.com/api/v1/stock/profile"
)

# =========================================================
# R2 DOWNLOAD
# =========================================================

async def r2_download(client, filename):

    url = f"{WORKER_URL}/{filename}"

    r = await client.get(
        url,
        headers=WORKER_HEADERS,
        timeout=120,
    )

    if r.status_code != 200:

        raise RuntimeError(
            f"{filename} download failed"
        )

    return r.json()

# =========================================================
# R2 UPLOAD
# =========================================================

async def r2_upload(client, filename, data):

    url = f"{WORKER_URL}?file={filename}"

    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )

    if r.status_code != 200:

        raise RuntimeError(
            f"{filename} upload failed"
        )

    print(f"✅ Uploaded {filename}")

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

            except Exception as e:

                print(f"{symbol} Network Error: {e}")

                await asyncio.sleep(2 ** attempt)

                continue

            if r.status_code == 429:

                print(f"{symbol} -> 429")

                await asyncio.sleep(10)

                continue

            if r.status_code == 503:

                print(f"{symbol} -> 503")

                await asyncio.sleep(5)

                continue

            if r.status_code != 200:

                print(
                    f"{symbol} -> HTTP {r.status_code}"
                )

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

        print(f"✗ {symbol} | profile fail")

        return None

    market_cap = (
        profile.get("market_cap")
        or 0
    )

    try:

        market_cap = float(market_cap)

    except:

        market_cap = 0

    # Skip tiny companies

    if market_cap < MIN_MARKET_CAP_CR:

        print(
            f"✗ {symbol} | market cap < 50cr"
        )

        return None

    print(
        f"✓ {symbol} | {market_cap:.0f}cr"
    )

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
# MAIN
# =========================================================

async def main():

    semaphore = asyncio.Semaphore(
        CONCURRENCY
    )

    async with httpx.AsyncClient(
        headers=HEADERS
    ) as client:

        print("Downloading master.json...")

        master = await r2_download(
            client,
            "master.json"
        )

        print(
            f"Loaded {len(master)} stocks"
        )

        results = []

        total = len(master)

        for i in range(0, total, BATCH_SIZE):

            batch = master[i:i+BATCH_SIZE]

            tasks = [

                process_stock(
                    client,
                    stock,
                    semaphore
                )

                for stock in batch
            ]

            batch_results = await asyncio.gather(
                *tasks
            )

            results.extend(batch_results)

            print()
            print(
                f"Processed "
                f"{min(i+BATCH_SIZE, total)}"
                f"/{total}"
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
            f"✓ Final Stocks : "
            f"{len(classification)}"
        )

        print(
            f"✗ Removed      : "
            f"{len(master) - len(classification)}"
        )

        await r2_upload(
            client,
            OUTPUT_FILE,
            classification
        )

        print()
        print("🎉 classification.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())

