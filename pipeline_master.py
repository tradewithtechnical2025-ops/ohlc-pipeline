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

RATE_DELAY = 0.30
RETRY = 3
MIN_MARKET_CAP_CR = 10
MIN_PRICE = 10
MIN_TURNOVER_CR = 1

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# Symbols to watch closely in debug logs
DEBUG_WATCH = {"AAVAS", "HDFCBANK", "RELIANCE", "INFY"}

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

            print(f"  ⚠️  Network Error: {e}")

            await asyncio.sleep(2 ** attempt)

            continue

        if r.status_code == 429:

            print("  ⏳ 429 Rate Limit — waiting 15s...")

            await asyncio.sleep(15)

            continue

        if r.status_code != 200:

            print(f"  ❌ HTTP {r.status_code} for path: {path[:80]}")

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

    print("📡 Fetching stock universe...")

    data = await finedge_get(
        client,
        "stock-symbols"
    )

    if not data:

        raise RuntimeError(
            "stock-symbols fetch failed"
        )

    print(f"✅ Fetched {len(data)} symbols")
    print(f"   Sample entry format: {data[0]}")

    # Check watch list presence in stock-symbols
    print()
    print("🔍 Watch List — stock-symbols check:")
    for entry in data:
        sym  = str(entry.get("symbol", "")).upper()
        name = str(entry.get("name",   "")).upper()
        for w in DEBUG_WATCH:
            if w in sym or w in name:
                print(f"   FOUND  {w:15s} → {entry}")

    return data


# =========================================================
# BUILD MASTER
# =========================================================

async def build_master(client, data):

    print()
    print("=" * 50)
    print("     Building Master Universe")
    print("=" * 50)

    master = []
    filtered_mcap     = 0
    filtered_price    = 0
    filtered_turnover = 0
    filtered_keyword  = 0
    api_empty         = 0
    batch_size        = 25
    total_batches     = (len(data) + batch_size - 1) // batch_size

    for i in range(0, len(data), batch_size):

        batch_num = i // batch_size + 1

        print()
        print(f"📦 Batch {batch_num}/{total_batches}  (symbols {i+1}–{min(i+batch_size, len(data))})")

        batch = data[i:i + batch_size]

        symbols   = []
        stock_map = {}

        for stock in batch:

            if not is_valid_stock(stock):
                sym = str(stock.get("symbol", "")).upper()
                if sym in DEBUG_WATCH:
                    print(f"  🔍 {sym}: ✗ REJECTED at keyword filter — entry={stock}")
                filtered_keyword += 1
                continue

            symbol = str(
                stock.get("symbol", "")
            ).strip().upper()

            if not symbol:
                continue

            symbols.append(symbol)
            stock_map[symbol] = stock

        if not symbols:
            print("  ⏭️  All symbols filtered by keyword — skipping")
            continue

        path = "quote?" + "&".join(
            f"symbol={s}"
            for s in symbols
        )

        quotes = await finedge_get(client, path)

        if not quotes:
            print(f"  ⚠️  Empty response for batch {batch_num} — skipping")
            api_empty += len(symbols)
            continue

        batch_added    = 0
        batch_rejected = 0

        for symbol, q in quotes.items():

            is_watched = symbol in DEBUG_WATCH

            if symbol not in stock_map:
                if is_watched:
                    print(f"  🔍 {symbol}: in quotes but NOT in stock_map (keyword filtered or missing)")
                continue

            try:

                price      = float(q.get("current_price") or 0)
                volume     = float(q.get("volume")        or 0)
                market_cap = float(q.get("market_cap")    or 0)

            except Exception:
                if is_watched:
                    print(f"  🔍 {symbol}: failed to parse fields — raw={q}")
                batch_rejected += 1
                continue

            turnover_cr = (price * volume) / 1e7

            if is_watched:
                print(
                    f"  🔍 {symbol}: price={price} vol={volume} "
                    f"mcap={market_cap} turnover={turnover_cr:.2f}Cr"
                )

            if market_cap < MIN_MARKET_CAP_CR:
                if is_watched:
                    print(f"  🔍 {symbol}: ✗ REJECTED — mcap {market_cap} < {MIN_MARKET_CAP_CR}")
                filtered_mcap += 1
                batch_rejected += 1
                continue

            if price < MIN_PRICE:
                if is_watched:
                    print(f"  🔍 {symbol}: ✗ REJECTED — price {price} < {MIN_PRICE}")
                filtered_price += 1
                batch_rejected += 1
                continue

            if turnover_cr < MIN_TURNOVER_CR:
                if is_watched:
                    print(f"  🔍 {symbol}: ✗ REJECTED — turnover {turnover_cr:.2f} < {MIN_TURNOVER_CR}")
                filtered_turnover += 1
                batch_rejected += 1
                continue

            if is_watched:
                print(f"  🔍 {symbol}: ✓ ADDED to master")

            stock = stock_map[symbol]

            nse_code = stock.get("nse_code")
            bse_code = stock.get("bse_code")

            master.append({
                "symbol":           symbol,
                "name":             stock.get("name"),
                "exchange":         "NSE" if nse_code else "BSE",
                "bse_code":         bse_code,
                "nse_code":         nse_code,
                "consolidated_ind": stock.get("consolidated_ind", False),
                "market_cap_cr":    market_cap,
                "price":            price,
                "volume":           volume,
                "turnover_cr":      round(turnover_cr, 2),
            })

            batch_added += 1

        print(
            f"  ✓ Added: {batch_added:>4}   "
            f"✗ Rejected: {batch_rejected:>4}   "
            f"Running total: {len(master)}"
        )

    master.sort(
        key=lambda x: x["market_cap_cr"],
        reverse=True
    )

    print()
    print("=" * 50)
    print("               Summary")
    print("=" * 50)
    print(f"  ✓ Final Stocks         : {len(master)}")
    print(f"  ✗ Keyword Filtered     : {filtered_keyword}")
    print(f"  ✗ MCAP Rejected        : {filtered_mcap}")
    print(f"  ✗ Price Rejected       : {filtered_price}")
    print(f"  ✗ Turnover Rejected    : {filtered_turnover}")
    print(f"  ✗ API Empty/Skipped    : {api_empty}")
    print("=" * 50)

    return master


# =========================================================
# MAIN
# =========================================================

async def main():

    async with httpx.AsyncClient(headers=HEADERS) as client:

        data = await fetch_symbols(client)

        master = await build_master(client, data)

        await r2_upload(client, OUTPUT_FILE, master)

        print()
        print("🎉 Done — master.json uploaded successfully")


if __name__ == "__main__":
    asyncio.run(main())
