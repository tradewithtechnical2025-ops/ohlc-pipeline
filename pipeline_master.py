#!/usr/bin/env python3

import asyncio
import json
import os
import re
import gzip

import httpx

# =========================================================
# CONFIG
# =========================================================

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"
UPSTOX_BSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
UPSTOX_NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
OUTPUT_FILE     = "master.json"
BSE_OUTPUT_FILE = "bse.json"        # full BSE universe

RATE_DELAY = 0.20
RETRY = 3
MIN_MARKET_CAP_CR = 10
MIN_PRICE = 10
MIN_TURNOVER_CR = 1
MIN_BSE_PRICE = 20          # BSE master: sirf price > 20 waale
MIN_BSE_MCAP_CR = 100       # BSE master: market cap >= 100 cr (tune as needed)

# Agar sirf BSE-only stocks chahiye (jo NSE pe nahi), to True kar do.
# False = har BSE-listed stock (dual-listed bhi) -> "full BSE universe"
BSE_ONLY_EXCLUSIVE = True

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================================================
# FILTERS
# =========================================================

BAD_KEYWORDS = [
    "ETF", "BEES", "LIQUID", "NIFTY", "SENSEX",
    "GOLD", "SILVER", "INDEX", "NEXT50", "MIDCAP", "SMALLCAP",
]

def is_bad_symbol(symbol, name):
    """Return True if symbol should be excluded."""

    # Purely numeric = BSE-only code
    if symbol.isdigit():
        return True

    # Rights entitlement
    if symbol.endswith("-RE"):
        return True

    # Keyword filter — exact word boundary match
    for keyword in BAD_KEYWORDS:
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, symbol):
            return True
        if name and re.search(pattern, name):
            return True

    return False


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


async def fetch_upstox_master(client, url, label):

    print(f"📡 Fetching Upstox {label} master...")

    r = await client.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Referer": "https://upstox.com/"
        },
        follow_redirects=True,
        timeout=120
    )
    r.raise_for_status()

    data = json.loads(gzip.decompress(r.content))

    print(f"✅ Loaded {len(data)} {label} instruments")

    return data

# =========================================================
# BUILD MASTER  (NSE-centric, filtered)
# =========================================================

async def build_master(client, data, quotes, nse_name_map):

    print()
    print("=" * 50)
    print("     Building Master Universe")
    print("=" * 50)

    # Build stock_map — NO filtering here, just enrichment lookup
    stock_map = {}
    for stock in data:
        sym = str(stock.get("symbol", "")).strip().upper()
        if sym:
            stock_map[sym] = stock

    print(f"  📋 stock_map built : {len(stock_map)} symbols")
    print()

    # Process all returned quotes
    master            = []
    filtered_bad      = 0
    filtered_mcap     = 0
    filtered_price    = 0
    filtered_turnover = 0
    upstox_named      = 0

    for symbol, q in quotes.items():

        # Enrich from stock_map if available
        stock    = stock_map.get(symbol)

        # Name fallback chain:
        # Finedge stock-symbols -> Finedge quote -> Upstox NSE master -> symbol
        name = (stock.get("name") if stock else None) or q.get("name")
        if not name:
            name = nse_name_map.get(symbol)
            if name:
                upstox_named += 1
        if not name:
            name = symbol

        nse_code = stock.get("nse_code") if stock else symbol
        bse_code = stock.get("bse_code") if stock else None
        exchange = "NSE" if nse_code else "BSE"

        # Apply filters
        if is_bad_symbol(symbol, name):
            filtered_bad += 1
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

    # Stats
    never_quoted = set(stock_map.keys()) - set(quotes.keys())
    enriched     = sum(1 for s in master if stock_map.get(s["symbol"]))
    quote_only   = len(master) - enriched

    master.sort(key=lambda x: x["market_cap_cr"], reverse=True)

    print("=" * 50)
    print("               Summary (master)")
    print("=" * 50)
    print(f"  ✓ Final Stocks         : {len(master)}")
    print(f"    — Enriched           : {enriched}")
    print(f"    — Quote-only         : {quote_only}")
    print(f"    — Upstox-named       : {upstox_named}")
    print(f"  ✗ Bad Symbol Filtered  : {filtered_bad}")
    print(f"  ✗ MCAP Rejected        : {filtered_mcap}")
    print(f"  ✗ Price Rejected       : {filtered_price}")
    print(f"  ✗ Turnover Rejected    : {filtered_turnover}")
    print(f"  ✗ Never Quoted by API  : {len(never_quoted)}")
    print("=" * 50)

    return master


# =========================================================
# BUILD BSE MASTER  (full BSE universe, NO liquidity filter)
# =========================================================

def build_bse_master(data, quotes, upstox_map, only_exclusive=False):
    """
    Full BSE universe from stock-symbols data.
    Sirf price > MIN_BSE_PRICE waale. No-quote stocks (price=None) bhi skip.
    Numeric BSE codes rakhe jaate hain (mcap/turnover filter nahi).

    only_exclusive=True  -> sirf BSE-only (jo NSE pe listed nahi)
    only_exclusive=False -> har BSE-listed stock (dual-listed bhi)
    """
    print()
    print("=" * 50)
    print("     Building BSE Universe")
    print("=" * 50)

    out         = []
    no_quote    = 0
    below_price = 0
    below_mcap  = 0

    for stock in data:

        bse_code = str(stock.get("bse_code") or "").strip()
        if not bse_code:
            continue                                  # BSE pe listed hi nahi
        info = upstox_map.get(bse_code)

        if not info:
            continue
        # Mainboard only
        if info.get("segment") != "BSE_EQ":
            continue

        nse_code = str(stock.get("nse_code") or "").strip()
        if only_exclusive and nse_code:
            continue                                  # dual-listed skip

        sym  = str(stock.get("symbol") or "").strip().upper()
        name = stock.get("name") or ""

        # Quote dhoondho — alpha symbol ya bse_code, dono try (jo mile)
        q = quotes.get(sym) or quotes.get(bse_code) or {}
        try:
            price = float(q.get("current_price") or 0) or None
            mcap  = float(q.get("market_cap")    or 0) or None
            vol   = float(q.get("volume")        or 0) or None
        except Exception:
            price = mcap = vol = None

        if price is None:
            no_quote += 1
            continue                                  # quote nahi -> skip
        if price <= MIN_BSE_PRICE:
            below_price += 1
            continue                                  # price <= 20 -> skip
        if mcap is None or mcap < MIN_BSE_MCAP_CR:
            below_mcap += 1
            continue                                  # mcap < threshold -> skip

        out.append({
            "symbol":        sym or bse_code,
            "trading_symbol": info.get("trading_symbol"),
            "name":          name,
            "exchange":      "BSE",
            "bse_code":      bse_code,
            "nse_code":      nse_code or None,
            "dual_listed":   bool(nse_code),
            "consolidated_ind": stock.get("consolidated_ind", False),
            "market_cap_cr": mcap,
            "price":         price,
            "volume":        vol,
        })

    out.sort(key=lambda x: (x["market_cap_cr"] or 0), reverse=True)

    print(f"  ✓ BSE stocks (final)         : {len(out)}")
    print(f"  ✗ No quote / price = 0       : {no_quote}")
    print(f"  ✗ Price <= {MIN_BSE_PRICE}             : {below_price}")
    print(f"  ✗ MCAP < {MIN_BSE_MCAP_CR} cr           : {below_mcap}")
    print(f"    mode                       : {'BSE-only' if only_exclusive else 'all BSE-listed'}")
    print("=" * 50)

    return out


# =========================================================
# MAIN
# =========================================================

async def main():

    async with httpx.AsyncClient(headers=HEADERS) as client:

        data = await fetch_symbols(client)

        upstox_data = await fetch_upstox_master(client, UPSTOX_BSE_URL, "BSE")
        upstox_map = {
            str(x["exchange_token"]): {
                "segment": x.get("segment"),
                "trading_symbol": x.get("trading_symbol")
            }
            for x in upstox_data
        }

        # Upstox NSE master — naye listings (e.g. CMRGREEN) ke names ke liye
        upstox_nse = await fetch_upstox_master(client, UPSTOX_NSE_URL, "NSE")
        nse_name_map = {
            str(x.get("trading_symbol", "")).strip().upper(): x.get("name")
            for x in upstox_nse
            if x.get("segment") == "NSE_EQ" and x.get("instrument_type") == "EQ"
        }
        print(f"  📋 NSE name map: {len(nse_name_map)} symbols")

        # Single quote call — dono builders ke liye reuse
        print()
        print("📡 Fetching quotes (single call)...")
        quotes = await finedge_get(client, "quote?symbol=RELIANCE")
        if not quotes:
            raise RuntimeError("quote fetch failed")
        print(f"✅ Got {len(quotes)} quotes from API")

        # NSE-centric filtered master
        master = await build_master(client, data, quotes, nse_name_map)
        await r2_upload(client, OUTPUT_FILE, master)

        # Full BSE universe
        bse = build_bse_master(data, quotes, upstox_map, only_exclusive=BSE_ONLY_EXCLUSIVE)
        await r2_upload(client, BSE_OUTPUT_FILE, bse)

        print()
        print(f"🎉 Done — {OUTPUT_FILE} ({len(master)}) + {BSE_OUTPUT_FILE} ({len(bse)}) uploaded")


if __name__ == "__main__":
    asyncio.run(main())
