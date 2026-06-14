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

UPSTOX_ACCESS_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "")

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"
UPSTOX_BSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
UPSTOX_NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
UPSTOX_OHLC_URL = "https://api.upstox.com/v3/market-quote/ohlc"

UPSTOX_TOKEN = os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip()
OUTPUT_FILE     = "master.json"
BSE_OUTPUT_FILE = "bse.json"

RATE_DELAY = 0.20
RETRY = 3
MIN_MARKET_CAP_CR = 10
MIN_PRICE = 10
MIN_TURNOVER_CR = 1
MIN_BSE_PRICE = 20
MIN_BSE_MCAP_CR = 100

BSE_ONLY_EXCLUSIVE = True

DEBUG_SYMBOLS = ["CMRGREEN"]

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
    if symbol.isdigit():
        return True
    if symbol.endswith("-RE"):
        return True
    for keyword in BAD_KEYWORDS:
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, symbol):
            return True
        if name and re.search(pattern, name):
            return True
    return False


def debug_trace_upstox(upstox_nse, quotes):
    for ds in DEBUG_SYMBOLS:
        key = ds.upper().replace(" ", "")
        prefix = key[:5]
        matches = [
            x for x in upstox_nse
            if prefix in str(x.get("trading_symbol") or "").upper().replace(" ", "")
            or prefix in str(x.get("name") or "").upper().replace(" ", "")
        ]
        print(f"  🔍 DEBUG {ds}: {len(matches)} Upstox NSE entries matched")
        for m in matches[:8]:
            print(
                f"      tsym={m.get('trading_symbol')!r} | seg={m.get('segment')} | "
                f"type={m.get('instrument_type')} | name={m.get('name')!r} | "
                f"ikey={m.get('instrument_key')} | isin={m.get('isin')!r}"
            )
        print(f"      in Finedge quotes : {key in quotes}")


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
# UPSTOX QUOTES
# =========================================================

async def fetch_upstox_quotes(client, instrument_keys):
    out = {}
    for i in range(0, len(instrument_keys), 500):
        batch = instrument_keys[i:i + 500]
        try:
            r = await client.get(
                "https://api.upstox.com/v2/market-quote/quotes",
                params={"instrument_key": ",".join(batch)},
                headers={
                    "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
                    "Accept": "application/json",
                },
                timeout=60,
            )
        except Exception as e:
            print(f"  ⚠️  Upstox quote network error: {e}")
            continue
        if r.status_code != 200:
            print(f"  ❌ Upstox quotes HTTP {r.status_code}: {r.text[:120]}")
            continue
        payload = r.json().get("data", {}) or {}
        for k, v in payload.items():
            sym = str(v.get("symbol") or k.split(":")[-1]).strip().upper()
            if sym:
                out[sym] = v
        await asyncio.sleep(0.3)
    return out


# =========================================================
# BUILD MASTER  (NSE-centric, filtered)
# =========================================================

async def build_master(client, data, quotes, nse_name_map, nse_isin_map):

    print()
    print("=" * 50)
    print("     Building Master Universe")
    print("=" * 50)

    stock_map = {}
    for stock in data:
        sym = str(stock.get("symbol", "")).strip().upper()
        if sym:
            stock_map[sym] = stock

    print(f"  📋 stock_map built : {len(stock_map)} symbols")
    print()

    master            = []
    filtered_bad      = 0
    filtered_mcap     = 0
    filtered_price    = 0
    filtered_turnover = 0
    upstox_named      = 0
    new_listings      = 0

    for symbol, q in quotes.items():

        stock = stock_map.get(symbol)

        name = (stock.get("name") if stock else None) or q.get("name") or ""
        name = str(name).strip()

        if not name or name.upper() == symbol.upper():
            upstox_name = nse_name_map.get(symbol)
            if upstox_name:
                name = upstox_name
                upstox_named += 1

        if not name:
            name = symbol

        nse_code = stock.get("nse_code") if stock else symbol
        bse_code = stock.get("bse_code") if stock else None
        exchange = "NSE" if nse_code else "BSE"

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

        is_upstox_src = bool(q.get("_source") == "upstox")

        if not is_upstox_src and market_cap < MIN_MARKET_CAP_CR:
            filtered_mcap += 1
            continue

        if price < MIN_PRICE:
            filtered_price += 1
            continue

        if turnover_cr < MIN_TURNOVER_CR:
            filtered_turnover += 1
            continue

        if is_upstox_src:
            new_listings += 1

        master.append({
            "symbol":           symbol,
            "name":             name,
            "exchange":         exchange,
            "bse_code":         bse_code,
            "nse_code":         nse_code,
            "isin":             nse_isin_map.get(symbol, ""),   # ← NEW
            "consolidated_ind": stock.get("consolidated_ind", False) if stock else False,
            "market_cap_cr":    market_cap if not is_upstox_src else None,
            "price":            price,
            "volume":           volume,
            "turnover_cr":      round(turnover_cr, 2),
            "new_listing":      is_upstox_src,
        })

    never_quoted = set(stock_map.keys()) - set(quotes.keys())
    enriched     = sum(1 for s in master if stock_map.get(s["symbol"]))
    quote_only   = len(master) - enriched

    master.sort(key=lambda x: (x["market_cap_cr"] or 0), reverse=True)

    print("=" * 50)
    print("               Summary (master)")
    print("=" * 50)
    print(f"  ✓ Final Stocks         : {len(master)}")
    print(f"    — Enriched           : {enriched}")
    print(f"    — Quote-only         : {quote_only}")
    print(f"    — Upstox-named       : {upstox_named}")
    print(f"    — New listings (Upstox): {new_listings}")
    print(f"  ✗ Bad Symbol Filtered  : {filtered_bad}")
    print(f"  ✗ MCAP Rejected        : {filtered_mcap}")
    print(f"  ✗ Price Rejected       : {filtered_price}")
    print(f"  ✗ Turnover Rejected    : {filtered_turnover}")
    print(f"  ✗ Never Quoted by API  : {len(never_quoted)}")
    print("=" * 50)

    return master


# =========================================================
# BUILD BSE MASTER
# =========================================================

def build_bse_master(data, quotes, upstox_map, only_exclusive=False):
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
            continue
        info = upstox_map.get(bse_code)
        if not info:
            continue
        if info.get("segment") != "BSE_EQ":
            continue

        nse_code = str(stock.get("nse_code") or "").strip()
        if only_exclusive and nse_code:
            continue

        sym  = str(stock.get("symbol") or "").strip().upper()
        name = stock.get("name") or ""

        q = quotes.get(sym) or quotes.get(bse_code) or {}
        try:
            price = float(q.get("current_price") or 0) or None
            mcap  = float(q.get("market_cap")    or 0) or None
            vol   = float(q.get("volume")        or 0) or None
        except Exception:
            price = mcap = vol = None

        if price is None:
            no_quote += 1
            continue
        if price <= MIN_BSE_PRICE:
            below_price += 1
            continue
        if mcap is None or mcap < MIN_BSE_MCAP_CR:
            below_mcap += 1
            continue

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
# UPSTOX INJECTION
# =========================================================

async def fetch_upstox_ohlc(client, instrument_keys):
    out = {}
    for i in range(0, len(instrument_keys), 500):
        batch = instrument_keys[i:i + 500]
        r = await client.get(
            UPSTOX_OHLC_URL,
            params={"instrument_key": ",".join(batch), "interval": "1d"},
            headers={
                "Authorization": f"Bearer {UPSTOX_TOKEN}",
                "Accept": "application/json",
            },
            timeout=60,
        )
        if r.status_code != 200:
            print(f"  ⚠️  Upstox OHLC HTTP {r.status_code} (batch {i // 500 + 1})")
            continue
        out.update(r.json().get("data") or {})
    return out


def find_missing_nse(upstox_nse, quotes):
    missing = []
    for x in upstox_nse:
        if x.get("segment") != "NSE_EQ":
            continue
        if x.get("instrument_type") != "EQ":
            continue
        tsym = str(x.get("trading_symbol") or "").strip().upper()
        if not tsym or tsym in quotes:
            continue
        if "-" in tsym:
            continue
        if is_bad_symbol(tsym, x.get("name")):
            continue
        missing.append(x)
    return missing


async def inject_missing_from_upstox(client, master, upstox_nse, quotes, nse_isin_map):

    print()
    print("=" * 50)
    print("     Upstox Injection (missing NSE)")
    print("=" * 50)

    if not UPSTOX_TOKEN:
        print("  ⚠️  UPSTOX_ACCESS_TOKEN not set — injection skipped")
        print("=" * 50)
        return 0

    existing = {s["symbol"] for s in master}
    missing  = [x for x in find_missing_nse(upstox_nse, quotes)
                if str(x.get("trading_symbol") or "").strip().upper() not in existing]

    print(f"  📋 Missing from Finedge : {len(missing)}")

    if not missing:
        print("=" * 50)
        return 0

    key_map = {}
    for x in missing:
        ikey = x.get("instrument_key")
        tsym = str(x.get("trading_symbol") or "").strip().upper()
        isin = str(x.get("isin") or "").strip()
        if ikey and tsym:
            key_map[tsym] = {
                "ikey": ikey,
                "name": str(x.get("name") or "").strip(),
                "isin": isin,
            }

    ohlc = await fetch_upstox_ohlc(client, [v["ikey"] for v in key_map.values()])
    print(f"  📡 Upstox OHLC received : {len(ohlc)}")

    ohlc_by_sym = {}
    for k, v in ohlc.items():
        sym = k.split(":")[-1].strip().upper()
        ohlc_by_sym[sym] = v

    for ds in DEBUG_SYMBOLS:
        key = ds.upper().replace(" ", "")
        print(
            f"  🔍 DEBUG {ds}: in key_map={key in key_map} | "
            f"ohlc mila={key in ohlc_by_sym}"
        )
        if key in ohlc_by_sym:
            print(f"      ohlc data: {json.dumps(ohlc_by_sym[key])[:300]}")

    injected       = 0
    no_data        = 0
    below_price    = 0
    below_turnover = 0

    for tsym, info in key_map.items():

        is_debug = tsym in {d.upper().replace(" ", "") for d in DEBUG_SYMBOLS}

        d = ohlc_by_sym.get(tsym)
        if not d:
            if is_debug:
                print(f"  🔍 DEBUG {tsym}: REJECTED — no OHLC data")
            no_data += 1
            continue

        candle = d.get("live_ohlc") or d.get("prev_ohlc") or {}

        try:
            price = float(d.get("last_price") or candle.get("close") or 0)
            vol   = float(candle.get("volume") or 0)
        except Exception:
            if is_debug:
                print(f"  🔍 DEBUG {tsym}: REJECTED — price/vol parse fail")
            no_data += 1
            continue

        if price <= 0:
            if is_debug:
                print(f"  🔍 DEBUG {tsym}: REJECTED — price <= 0")
            no_data += 1
            continue

        if price < MIN_PRICE:
            if is_debug:
                print(f"  🔍 DEBUG {tsym}: REJECTED — price {price} < {MIN_PRICE}")
            below_price += 1
            continue

        turnover_cr = (price * vol) / 1e7

        if turnover_cr < MIN_TURNOVER_CR:
            if is_debug:
                print(f"  🔍 DEBUG {tsym}: REJECTED — turnover {turnover_cr:.2f} < {MIN_TURNOVER_CR}")
            below_turnover += 1
            continue

        if is_debug:
            print(f"  🔍 DEBUG {tsym}: ✅ INJECTED @ {price} (turnover {turnover_cr:.2f} cr)")

        master.append({
            "symbol":           tsym,
            "name":             info["name"] or tsym,
            "exchange":         "NSE",
            "bse_code":         None,
            "nse_code":         tsym,
            "isin":             info["isin"],                    # ← NEW
            "consolidated_ind": False,
            "market_cap_cr":    0,
            "price":            price,
            "volume":           vol,
            "turnover_cr":      round(turnover_cr, 2),
            "source":           "upstox",
        })
        injected += 1

    master.sort(key=lambda x: x["market_cap_cr"], reverse=True)

    print(f"  ✓ Injected              : {injected}")
    print(f"  ✗ No OHLC data          : {no_data}")
    print(f"  ✗ Price Rejected        : {below_price}")
    print(f"  ✗ Turnover Rejected     : {below_turnover}")
    print("=" * 50)

    return injected


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

        upstox_nse = await fetch_upstox_master(client, UPSTOX_NSE_URL, "NSE")

        # ── NSE name map + ISIN map (dono ek hi loop mein) ──────────────
        nse_name_map = {}
        nse_isin_map = {}
        for x in upstox_nse:
            if x.get("segment") != "NSE_EQ":
                continue
            tsym = str(x.get("trading_symbol") or "").strip().upper()
            nm   = str(x.get("name") or "").strip()
            isin = str(x.get("isin") or "").strip()
            if tsym and nm and tsym not in nse_name_map:
                nse_name_map[tsym] = nm
            if tsym and isin and tsym not in nse_isin_map:
                nse_isin_map[tsym] = isin
        print(f"  📋 NSE name map : {len(nse_name_map)} symbols")
        print(f"  📋 NSE ISIN map : {len(nse_isin_map)} symbols")

        print()
        print("📡 Fetching quotes (single call)...")
        quotes = await finedge_get(client, "quote?symbol=RELIANCE")
        if not quotes:
            raise RuntimeError("quote fetch failed")
        print(f"✅ Got {len(quotes)} quotes from API")

        debug_trace_upstox(upstox_nse, quotes)

        # Pass nse_isin_map to builders
        master = await build_master(client, data, quotes, nse_name_map, nse_isin_map)

        await inject_missing_from_upstox(client, master, upstox_nse, quotes, nse_isin_map)

        await r2_upload(client, OUTPUT_FILE, master)

        bse = build_bse_master(data, quotes, upstox_map, only_exclusive=BSE_ONLY_EXCLUSIVE)
        await r2_upload(client, BSE_OUTPUT_FILE, bse)

        print()
        print(f"🎉 Done — {OUTPUT_FILE} ({len(master)}) + {BSE_OUTPUT_FILE} ({len(bse)}) uploaded")


if __name__ == "__main__":
    asyncio.run(main())
