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

OUTPUT_FILE = "master.json"

RATE_DELAY = 0.20
RETRY = 3
MIN_MARKET_CAP_CR = 50
MIN_PRICE = 10
MIN_TURNOVER_CR = 1
QUOTE_CONCURRENCY = 20

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================================================
# FILTERS
# =========================================================

BAD_KEYWORDS = [

    "ETF",
    "BEES",

    "LIQUID",

    "NIFTY",
    "SENSEX",

    "GOLD",
    "SILVER",

    "INDEX",

    "NEXT50",
    "MIDCAP",
    "SMALLCAP",
]

# =========================================================
# HELPERS
# =========================================================

def is_valid_stock(stock):

    symbol = str(
        stock.get("symbol", "")
    ).upper()

    name = str(
        stock.get("name", "")
    ).upper()

    if not symbol:
        return False

    if not name:
        return False

    for keyword in BAD_KEYWORDS:

        if keyword in symbol:
            return False

        if keyword in name:
            return False

    return True


# =========================================================
# FINEDGE GET
# =========================================================

async def finedge_get(client, path):

    url = f"{FINEDGE_BASE}/{path}"

    params = {
        "token": FINEDGE_TOKEN
    }

    for attempt in range(RETRY):

        await asyncio.sleep(RATE_DELAY)

        try:

            r = await client.get(
                url,
                params=params,
                timeout=60,
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

            print(f"HTTP {r.status_code}")

            return None

        try:

            return r.json()

        except Exception:

            return None

    return None
# =========================================================
# QUOTE
# =========================================================

async def fetch_quote(client, symbol):

    return await finedge_get(
        client,
        f"quote?symbol={symbol}"
    )

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
            f"{filename} upload failed: {r.status_code}"
        )

    print(f"✅ Uploaded {filename}")


# =========================================================
# FETCH STOCK SYMBOLS
# =========================================================

async def fetch_symbols(client):

    print("Fetching stock universe...")

    data = await finedge_get(
        client,
        "stock-symbols"
    )

    if not data:

        raise RuntimeError(
            "stock-symbols fetch failed"
        )

    print(f"Fetched {len(data)} symbols")

    return data
async def process_stock(client, stock, semaphore):

    symbol = str(
        stock.get("symbol", "")
    ).strip().upper()

    if not is_valid_stock(stock):
        return None

    async with semaphore:

        quote = await fetch_quote(
            client,
            symbol
        )

    if not quote:
        return None

    q = quote.get(symbol)

    if not q:
        return None

    price = float(
        q.get("current_price") or 0
    )

    volume = float(
        q.get("volume") or 0
    )

    market_cap = float(
        q.get("market_cap") or 0
    )

    turnover_cr = (
        price * volume
    ) / 1e7

    if market_cap < MIN_MARKET_CAP_CR:
        return None

    if price < MIN_PRICE:
        return None

    if turnover_cr < MIN_TURNOVER_CR:
        return None

    nse_code = stock.get("nse_code")
    bse_code = stock.get("bse_code")

    return {
        "symbol": symbol,
        "name": stock.get("name"),
        "exchange": "NSE" if nse_code else "BSE",
        "bse_code": bse_code,
        "nse_code": nse_code,
        "consolidated_ind": stock.get(
            "consolidated_ind",
            False
        ),
        "market_cap_cr": market_cap,
        "price": price,
        "volume": volume,
        "turnover_cr": round(turnover_cr, 2)
    }

# =========================================================
# BUILD MASTER
# =========================================================

async def build_master(client, data):

    print()
    print("=== Building Master Universe ===")

    semaphore = asyncio.Semaphore(
        QUOTE_CONCURRENCY
    )

    tasks = [
        process_stock(
            client,
            stock,
            semaphore
        )
        for stock in data
    ]

    results = await asyncio.gather(
        *tasks
    )

    master = [
        x for x in results
        if x
    ]

    master.sort(
        key=lambda x: x["market_cap_cr"],
        reverse=True
    )

    print()
    print("=== Summary ===")
    print(f"✓ Final Stocks : {len(master)}")

    return master


# =========================================================
# MAIN
# =========================================================

async def main():

    async with httpx.AsyncClient(
        headers=HEADERS
    ) as client:

        data = await fetch_symbols(
            client
        )

        master = await build_master(
            client,
            data
        )

        await r2_upload(
            client,
            OUTPUT_FILE,
            master
        )

        print()
        print("🎉 master.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
