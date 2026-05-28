# Final `pipeline_master.py`

```python
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

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
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
# WORKER UPLOAD
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

def build_master(data):

    print()
    print("=== Building Master Universe ===")

    seen = set()

    master = []

    skipped = 0

    total = len(data)

    for idx, stock in enumerate(data, start=1):

        if not is_valid_stock(stock):
            skipped += 1
            continue

        symbol = str(
            stock.get("symbol")
        ).strip().upper()

        if symbol in seen:
            continue

        seen.add(symbol)

        nse_code = stock.get("nse_code")
        bse_code = stock.get("bse_code")

        exchange = "NSE" if nse_code else "BSE"

        item = {

            "symbol": symbol,

            "name": stock.get("name"),

            "exchange": exchange,

            "bse_code": bse_code,

            "nse_code": nse_code,

            "consolidated_ind": stock.get(
                "consolidated_ind",
                False
            )
        }

        master.append(item)

        print(
            f"[{idx}/{total}] ✓ {symbol}"
        )

    print()
    print("=== Summary ===")
    print(f"✓ Final Stocks : {len(master)}")
    print(f"✗ Skipped      : {skipped}")

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

        master = build_master(
            data
        )

        await r2_upload(
            client,
            OUTPUT_FILE,
            master
        )

        print()
        print("✅ master.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
```
