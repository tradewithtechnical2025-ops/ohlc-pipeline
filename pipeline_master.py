#!/usr/bin/env python3

import asyncio
import json
import os

import re

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
MIN_MARKET_CAP_CR = 10
MIN_PRICE = 5
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

    symbol = str(stock.get("symbol", "")).upper()
    name   = str(stock.get("name",   "")).upper()

    if not symbol:
        return False
    if not name:
        return False

    for keyword in BAD_KEYWORDS:
        # Exact word match — SKYGOLD passes, "GOLD ETF" fails
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, symbol):
            return False
        if re.search(pattern, name):
            return False

    return True


# =========================================================
# FINEDGE GET
# =========================================================

async def finedge_get(client, path):

    url    = f"{FINEDGE_BASE}/{path}"
    params = {"token": FINEDGE_TOKEN}

    for attempt in range(RETRY):

        await asyncio.sleep(RATE_DELAY)

        try:
            r = await client.get(url, params=params, timeout=60)

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

    url     = f"{WORKER_URL}?file={filename}"
    payload = json.dumps(data)

    r = await client.post(
        url,
        headers={
            "X-Secret-Token": WORKER_TOKEN,
            "Content-Type":   "application/json",
        },
        content=payload.encode(),
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed: {r.status_code}")

    print(f"✅ Uploaded {filename}")


# =========================================================
# FETCH STOCK SYMBOLS
# =========================================================

async def fetch_symbols(client):

    print("📡 Fetching stock universe...")

    data = await finedge_get(client, "stock-symbols")

    if not data:
        raise RuntimeError("stock-symbols fetch failed")

    print(f"✅ Fetched {len(data)} raw symbols")

    return data


# =========================================================
# BUILD MASTER
# =========================================================

async def build_master(client, data):

    print()
    print("=" * 50)
    print("     Building Master Universe")
    print("=" * 50)

    # Build global stock_map — enrichment only, no filtering here
    stock_map = {}

    for stock in data:
        if not is_valid_stock(stock):
            continue
        sym = str(stock.get("symbol", "")).strip().upper()
        if sym:
            stock_map[sym] = stock

    print(f"  📋 stock_map built from stock-symbols : {len(stock_map)}")
    print()

    # ── Single API call — response contains full universe ─────────────────
    # Use first symbol just to trigger the response
    first_sym = next(iter(stock_map))
    print(f"📡 Fetching quotes (single call, trigger symbol: {first_sym})...")

    quotes = await finedge_get(client, f"quote?symbol={first_sym}")

    if not quotes:
        raise RuntimeError("quote fetch failed")

    print(f"✅ Got {len(quotes)} quotes from API")
    print()

    # ── Process all returned quotes ────────────────────────────────────────
    master            = []
    filtered_mcap     = 0
    filtered_price    = 0
    filtered_turnover = 0
    filtered_keyword  = 0

    for symbol, q in quotes.items():

        # Enrich from stock_map if available, else use quote data directly
        stock    = stock_map.get(symbol)
        nse_code = stock.get("nse_code") if stock else None
        bse_code = stock.get("bse_code") if stock else (
            symbol if symbol.isdigit() else None
        )
        name     = stock.get("name") if stock else q.get("name") or symbol
        exchange = "NSE" if nse_code else "BSE"

        # Skip BSE-only stocks
        # If nse_code exists → NSE ✓
        # If symbol is purely numeric → BSE code, skip
        # If symbol is alphabetic but not in stock_map → assume NSE (e.g. FIRSTCRY)
        is_numeric_bse = symbol.isdigit()
        if is_numeric_bse:
            continue
        if not nse_code:
            nse_code = symbol  # assume NSE listing

        # Apply keyword filter using resolved name + symbol
        fake_entry = {"symbol": symbol, "name": name}
        if not is_valid_stock(fake_entry):
            filtered_keyword += 1
            continue

        try:
            price      = float(q.get("current_price") or 0)
            volume     = float(q.get("volume")        or 0)
            market_cap = float(q.get("market_cap")    or 0)

        except Exception:
            continue

        turnover_cr = (price * volume) / 1e7

        if market_cap < MIN_MARKET_CAP_CR:
            filtered_mcap += 1
            continue

        if price < MIN_PRICE:
            filtered_price += 1
            continue

        if turnover_cr < MIN_TURNOVER_CR:
            filtered_turnover += 1
            continue

        master.append({
            "symbol":           symbol,
            "name":             name,
            "exchange":         exchange,
            "bse_code":         bse_code,
            "nse_code":         nse_code,
            "consolidated_ind": stock.get("consolidated_ind", False) if stock else False,
            "market_cap_cr":    market_cap,
            "price":            price,
            "volume":           volume,
            "turnover_cr":      round(turnover_cr, 2),
        })

    # Symbols in stock_map that API never returned
    quoted_symbols  = set(quotes.keys())
    never_quoted    = set(stock_map.keys()) - quoted_symbols

    master.sort(key=lambda x: x["market_cap_cr"], reverse=True)

    print("=" * 50)
    print("               Summary")
    print("=" * 50)
    enriched   = sum(1 for s in master if s["nse_code"] or s["bse_code"])
    unenriched = len(master) - enriched

    print(f"  ✓ Final Stocks         : {len(master)}")
    print(f"    — Enriched (in stock-symbols) : {enriched}")
    print(f"    — Quote-only (not in symbols) : {unenriched}")
    print(f"  ✗ Keyword Filtered     : {filtered_keyword}")
    print(f"  ✗ MCAP Rejected        : {filtered_mcap}")
    print(f"  ✗ Price Rejected       : {filtered_price}")
    print(f"  ✗ Turnover Rejected    : {filtered_turnover}")
    print(f"  ✗ Never Quoted by API  : {len(never_quoted)}")
    print("=" * 50)

    # Top 20 extra symbols by market cap not in stock-symbols
    extras = []
    for symbol, q in quotes.items():
        if symbol not in stock_map:
            try:
                mc    = float(q.get("market_cap")    or 0)
                price = float(q.get("current_price") or 0)
                extras.append((symbol, price, mc))
            except Exception:
                pass

    extras.sort(key=lambda x: x[2], reverse=True)
    print()
    print("🔎 Top 20 extras (in quotes but NOT in stock-symbols):")
    for sym, price, mc in extras[:20]:
        print(f"  {sym:20s}  price={price:<10}  mcap={mc:.0f} Cr")

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
