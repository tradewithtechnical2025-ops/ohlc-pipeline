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


# =========================================================
# BUILD MASTER
# =========================================================

async def build_master(client, data):

    print()
    print("=== Building Master Universe ===")

    master = []
    filtered_mcap = 0
    filtered_price = 0
    filtered_turnover = 0
    batch_size = 75

    for i in range(0, len(data), batch_size):
        print(
            f"\n📦 Batch {i//batch_size + 1}"
        )
        batch = data[i:i + batch_size]

        symbols = []

        stock_map = {}

        for stock in batch:

            if not is_valid_stock(stock):
                continue

            symbol = str(
                stock.get("symbol", "")
            ).strip().upper()

            if not symbol:
                continue

            symbols.append(symbol)

            stock_map[symbol] = stock

        if not symbols:
            continue

        path = "quote?" + "&".join(
            f"symbol={s}"
            for s in symbols
        )

        quotes = await finedge_get(
            client,
            path
        )

        if not quotes:
            continue

        for symbol, q in quotes.items():

            try:

                price = float(
                    q.get("current_price") or 0
                )

                volume = float(
                    q.get("volume") or 0
                )

                market_cap = float(
                    q.get("market_cap") or 0
                )

            except Exception:
                continue

            turnover_cr = (
                price * volume
            ) / 1e7

            if market_cap < MIN_MARKET_CAP_CR:
                filtered_mcap += 1
                continue

            if price < MIN_PRICE:
                filtered_price += 1
                continue

            if turnover_cr < MIN_TURNOVER_CR:
                filtered_turnover += 1
                continue

            stock = stock_map.get(symbol)

            if not stock:
                continue

            nse_code = stock.get("nse_code")
            bse_code = stock.get("bse_code")

            master.append({

                "symbol": symbol,

                "name": stock.get("name"),

                "exchange": "NSE"
                if nse_code else "BSE",

                "bse_code": bse_code,

                "nse_code": nse_code,

                "consolidated_ind": stock.get(
                    "consolidated_ind",
                    False
                ),

                "market_cap_cr": market_cap,

                "price": price,

                "volume": volume,

                "turnover_cr": round(
                    turnover_cr,
                    2
                )
            })

        print(
            f"Processed {min(i + batch_size, len(data))}/{len(data)}"
        )

    master.sort(
        key=lambda x: x["market_cap_cr"],
        reverse=True
    )

    print()
    print("=== Summary ===")
    print(f"✓ Final Stocks     : {len(master)}")
    print(f"✗ MCAP Rejected   : {filtered_mcap}")
    print(f"✗ Price Rejected  : {filtered_price}")
    print(f"✗ Turn Rejected   : {filtered_turnover}")

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
