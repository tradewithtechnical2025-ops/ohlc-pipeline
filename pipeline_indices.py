#!/usr/bin/env python3

import asyncio
import json
import os
from datetime import datetime, timedelta

import httpx
from r2_manifest import upload_with_manifest

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def normalize_index_symbol(v):
    v = str(v).upper().strip()
    REPLACE = {
        "NIF50": "NIFTY50",
        "NIFTY 50": "NIFTY50",
        "NIFTY BANK": "NIFTYBANK",
        "NIFTY IT": "NIFTYIT",
        "NIFTY AUTO": "NIFTYAUTO",
        "NIFTY FMCG": "NIFTYFMCG",
        "NIFTY PHARMA": "NIFTYPHARMA",
        "NIFTY REALTY": "NIFTYREALTY",
        "NIFTY METAL": "NIFTYMETAL",
    }
    return REPLACE.get(v, v.replace(" ", ""))


BAD_KEYWORDS = [
    "2X", "1X", "INV", "LEV", "TRI", "EQW", "EQUAL",
    "LOWVOL", "ALPHA", "QUALITY", "MOM", "MOMENTUM",
    "ESG", "VOL", "MULT", "QUA", "VALUE", "SHODUR",
    "ENH", "30T", "50T", "LIQ", "VAR", "BETA",
    "SHA", "TMC", "FPI", "EMERGE", "RURAL", "WAVES",
]

BAD_TYPES = {"Strategy", "Strategy Indices", "Volatility"}

MANUAL_BLACKLIST = {
    "NIFFINSER255", "NIFFINSEREXB", "NIFHEA2", "NIFINDCORGRO4",
    "NIFMIDFINSER", "NIFMIDHEA", "NIFMIDITTEL", "SNSXBBGEFS",
    "SNSXBSE30", "SNSXIND150", "SNSXINSLDR",
}


# ─────────────────────────────────────────────
# Index Map — Frontend ke liye (categorized)
# ─────────────────────────────────────────────

INDEX_CATEGORIES = {
    "NIFTY50"    : ("major",    "Nifty 50"),
    "NIFNEX50"   : ("major",    "Nifty Next 50"),
    "NIF100"     : ("major",    "Nifty 100"),
    "NIF200"     : ("major",    "Nifty 200"),
    "NIF500"     : ("major",    "Nifty 500"),
    "NIFMID50"   : ("major",    "Nifty Midcap 50"),
    "NIFMID100"  : ("major",    "Nifty Midcap 100"),
    "NIFMID150"  : ("major",    "Nifty Midcap 150"),
    "NIFMID400"  : ("major",    "Nifty MidSmallcap 400"),
    "NIFLAR250"  : ("major",    "Nifty LargeMidcap 250"),
    "NIFSMA50"   : ("major",    "Nifty Smallcap 50"),
    "NIFSMA100"  : ("major",    "Nifty Smallcap 100"),
    "NIFSMA250"  : ("major",    "Nifty Smallcap 250"),
    "NIFMIC250"  : ("major",    "Nifty Microcap 250"),
    "NIFTOTMAR"  : ("major",    "Nifty Total Market"),
    "NIFIPO"     : ("major",    "Nifty IPO"),
    "NIFMIDSEL2" : ("major",    "Nifty Midcap Select"),
    "NIFBAN"     : ("sectoral", "Nifty Bank"),
    "NIFPRIBAN"  : ("sectoral", "Nifty Private Bank"),
    "NIFPSUBAN"  : ("sectoral", "Nifty PSU Bank"),
    "NIFIT"      : ("sectoral", "Nifty IT"),
    "NIFAUT"     : ("sectoral", "Nifty Auto"),
    "NIFPHA"     : ("sectoral", "Nifty Pharma"),
    "NIFHEAIND"  : ("sectoral", "Nifty Healthcare"),
    "NIFFMC"     : ("sectoral", "Nifty FMCG"),
    "NIFMET"     : ("sectoral", "Nifty Metal"),
    "NIFREA"     : ("sectoral", "Nifty Realty"),
    "NIFMED"     : ("sectoral", "Nifty Media"),
    "NIFFINSER"  : ("sectoral", "Nifty Financial Services"),
    "NIFCONDUR"  : ("sectoral", "Nifty Consumer Durables"),
    "NIFCHE"     : ("sectoral", "Nifty Chemicals"),
    "NIFOILGAS"  : ("sectoral", "Nifty Oil & Gas"),
    "NIFENE"     : ("sectoral", "Nifty Energy"),
    "NIFCOM"     : ("sectoral", "Nifty Commodities"),
    "NIFINF"     : ("sectoral", "Nifty Infrastructure"),
    "NIFSERSEC"  : ("sectoral", "Nifty Services"),
    "NIFPSE"     : ("sectoral", "Nifty PSE"),
    "NIFCPS"     : ("sectoral", "Nifty CPSE"),
    "NIFMNC"     : ("sectoral", "Nifty MNC"),
    "NIFCAPMAR"  : ("sectoral", "Nifty Capital Markets"),
    "NIFTRALOG"  : ("sectoral", "Nifty Transport & Logistics"),
    "NIFMOB"     : ("sectoral", "Nifty Mobility"),
    "NIFCORHOU"  : ("sectoral", "Nifty Core Housing"),
    "NIFHOU"     : ("sectoral", "Nifty Housing"),
    "NIFINDDEF"    : ("thematic", "Nifty India Defence"),
    "NIFEVNEWAGEA" : ("thematic", "Nifty EV & New Age Auto"),
    "NIFINDDIG2"   : ("thematic", "Nifty India Digital"),
    "NIFINDINT"    : ("thematic", "Nifty India Internet"),
    "NIFINDMAN"    : ("thematic", "Nifty India Manufacturing"),
    "NIFINDCON"    : ("thematic", "Nifty India Consumption"),
    "NIFINDNEWAGE" : ("thematic", "Nifty New Age Consumption"),
    "NIFINDTOU"    : ("thematic", "Nifty India Tourism"),
    "NIFNONCYCCON" : ("thematic", "Nifty Non-Cyclical Consumer"),
    "NIFINDINFLOG" : ("thematic", "Nifty Infra & Logistics"),
    "NIFINDSEL5CO" : ("thematic", "Nifty Select 5 Corp Groups"),
    "NIFMIDINDCON" : ("thematic", "Nifty MidSmall Consumption"),
}


def build_index_map(master_parsed):
    index_map = {}
    counts = {"major": 0, "sectoral": 0, "thematic": 0}
    for symbol, meta in master_parsed.items():
        if symbol not in INDEX_CATEGORIES:
            continue
        category, label = INDEX_CATEGORIES[symbol]
        stocks = [
            c if isinstance(c, str) else c.get("symbol", "")
            for c in meta.get("constituents", [])
        ]
        stocks = [s.upper() for s in stocks if s]
        if stocks:
            index_map[symbol] = {
                "label"   : label,
                "category": category,
                "count"   : len(stocks),
                "stocks"  : stocks,
            }
            counts[category] += 1
    print(f"✓ index_map: {len(index_map)} indices "
          f"(major:{counts['major']} "
          f"sectoral:{counts['sectoral']} "
          f"thematic:{counts['thematic']})")
    return index_map


def is_bad_index(symbol, index_name):
    symbol = str(symbol).upper()
    index_name = str(index_name).upper()
    if symbol in MANUAL_BLACKLIST:
        return True
    return any(k in symbol or k in index_name for k in BAD_KEYWORDS)


# ─────────────────────────────────────────────
# R2 Upload
# ─────────────────────────────────────────────

async def r2_upload(client, filename, data):
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=300,
    )
    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed")


# ─────────────────────────────────────────────
# Index Master
# ─────────────────────────────────────────────

async def fetch_index_master(client):
    url = f"{FINEDGE_BASE}/index/master"
    params = {"token": FINEDGE_TOKEN}
    r = await client.get(url, params=params, timeout=300)
    r.raise_for_status()
    return r.json()


def parse_index_master(rows):
    output = {}
    skipped = 0
    for row in rows:
        raw_symbol = str(row.get("index_symbol", "")).strip()
        symbol = normalize_index_symbol(raw_symbol)
        if not symbol:
            skipped += 1; continue
        index_name     = str(row.get("index_name", ""))
        index_sub_type = str(row.get("index_sub_type", ""))
        constituents   = row.get("constituents") or []
        if is_bad_index(symbol, index_name):
            skipped += 1; continue
        if index_sub_type in BAD_TYPES:
            skipped += 1; continue
        if not constituents or len(constituents) < 5:
            skipped += 1; continue
        output[symbol] = {
            "api_symbol" : raw_symbol,
            "name"       : row.get("index_name"),
            "type"       : index_sub_type,
            "index_type" : row.get("index_type"),
            "exchange"   : row.get("exchange"),
            "description": row.get("description"),
            "constituents": constituents,
        }
    print(f"✓ Clean indices: {len(output)}")
    print(f"✓ Removed noisy indices: {skipped}")
    return output


# ─────────────────────────────────────────────
# Daily Feed
# ─────────────────────────────────────────────

async def fetch_index_daily(client):
    url = f"{FINEDGE_BASE}/index/market-price/daily-feed"
    params = {"token": FINEDGE_TOKEN}
    r = await client.get(url, params=params, timeout=300)
    r.raise_for_status()
    return r.json()


def parse_index_daily(rows, valid_symbols):
    output = {}
    skipped = 0
    for row in rows:
        symbol = normalize_index_symbol(row.get("index_symbol"))
        if symbol not in valid_symbols:
            skipped += 1; continue
        output[symbol] = {
            "name"         : row.get("index_name"),
            "close"        : row.get("close_price"),
            "open"         : row.get("open_price"),
            "high"         : row.get("high_price"),
            "low"          : row.get("low_price"),
            "change_pct"   : row.get("change_pct"),
            "points_change": row.get("points_change"),
            "volume"       : row.get("volume"),
            "turnover"     : row.get("turnover"),
            "market_cap"   : row.get("market_cap"),
            "pe"           : row.get("pe"),
            "pb"           : row.get("pb"),
            "div_yield"    : row.get("div_yield"),
        }
    print(f"✓ Daily feed indices: {len(output)}")
    print(f"✓ Skipped noisy daily feed: {skipped}")
    return output


# ─────────────────────────────────────────────
# Index Returns
# ─────────────────────────────────────────────

async def fetch_index_returns(client):
    url = f"{FINEDGE_BASE}/index/price-returns"
    params = {"token": FINEDGE_TOKEN}
    r = await client.get(url, params=params, timeout=300)
    r.raise_for_status()
    return r.json()


def parse_index_returns(rows, valid_symbols):
    """
    Finedge ne fix kar diya sign issue.
    3Y/5Y/7Y/10Y = CAGR → absolute convert karo.
    Structure: { "1M": {"v": 1.04, "d": "2026-05-13"}, ... }
    """
    CAGR = {"3Y": 3, "5Y": 5, "7Y": 7, "10Y": 10}
    ASIS = {"1M", "3M", "6M", "1Y"}
    ALL  = list(ASIS) + list(CAGR)
    output = {}
    skipped = 0
    for row in rows:
        symbol = normalize_index_symbol(row.get("index_symbol"))
        if symbol not in valid_symbols:
            skipped += 1; continue
        dates = row.get("dates") or {}
        ret = {}
        for p in ALL:
            raw = row.get(p)
            if raw is None:
                continue
            if p in CAGR:
                v = round((pow(1 + raw / 100, CAGR[p]) - 1) * 100, 2)
            else:
                v = round(raw, 2)
            ret[p] = {"v": v, "d": dates.get(p) or None}
        ret["last_date"] = dates.get("last_date") or None
        output[symbol] = ret
    print(f"✓ Returns indices: {len(output)}")
    print(f"✓ Skipped noisy returns: {skipped}")
    return output


# ─────────────────────────────────────────────
# Historical
# ─────────────────────────────────────────────

async def fetch_index_history_one(client, api_symbol):
    today     = datetime.now().date()
    from_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    url = f"{FINEDGE_BASE}/index/market-price/historical"
    params = {
        "index_symbol": api_symbol,
        "from_date"   : from_date,
        "to_date"     : to_date,
        "token"       : FINEDGE_TOKEN,
    }
    try:
        r = await client.get(url, params=params, timeout=300)
        if r.status_code != 200:
            return []
        return r.json().get("rows") or []
    except:
        return []


def parse_index_history(rows):
    if not rows:
        return []
    return [{
        "date"         : r.get("quote_date"),
        "open"         : r.get("open_price"),
        "high"         : r.get("high_price"),
        "low"          : r.get("low_price"),
        "close"        : r.get("close_price"),
        "change_pct"   : r.get("change_pct"),
        "points_change": r.get("points_change"),
        "volume"       : r.get("volume"),
        "turnover"     : r.get("turnover"),
    } for r in rows]


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():
    async with httpx.AsyncClient() as client:

        print("\n=================================")
        print(" INDEX PIPELINE STARTED")
        print("=================================\n")

        print("=== INDEX MASTER ===")
        master_rows = await fetch_index_master(client)
        if isinstance(master_rows, dict):
            master_rows = master_rows.get("data", [])
        master_parsed = parse_index_master(master_rows)
        valid_symbols = set(master_parsed.keys())
        await upload_with_manifest(client, r2_upload, "index_master.json", master_parsed,
                                    schema_v=1, extra_meta={"index_count": len(master_parsed)})
        print("✅ index_master.json uploaded\n")

        index_map = build_index_map(master_parsed)
        await upload_with_manifest(client, r2_upload, "index_map.json", index_map,
                                    schema_v=1, extra_meta={"index_count": len(index_map)})
        print(f"✅ index_map.json uploaded ({len(index_map)} indices)\n")

        print("=== INDEX DAILY FEED ===")
        daily_rows   = await fetch_index_daily(client)
        daily_parsed = parse_index_daily(daily_rows, valid_symbols)
        await upload_with_manifest(client, r2_upload, "index_daily.json", daily_parsed,
                                    schema_v=1, extra_meta={"index_count": len(daily_parsed)})
        print("✅ index_daily.json uploaded\n")

        print("=== INDEX RETURNS ===")
        returns_rows   = await fetch_index_returns(client)
        returns_parsed = parse_index_returns(returns_rows, valid_symbols)
        await upload_with_manifest(client, r2_upload, "index_returns.json", returns_parsed,
                                    schema_v=1, extra_meta={"index_count": len(returns_parsed)})
        print("✅ index_returns.json uploaded\n")

        print("=== INDEX HISTORICAL ===")
        symbols = sorted(master_parsed.items())
        total = len(symbols)
        success = failed = 0

        for i, (symbol, meta) in enumerate(symbols, 1):
            rows   = await fetch_index_history_one(client, meta["api_symbol"])
            parsed = parse_index_history(rows)
            if not parsed:
                failed += 1
                print(f"[{i}/{total}] ✗ {symbol} | no data")
                continue
            await upload_with_manifest(client, r2_upload, f"index_history/{symbol}.json", parsed,
                                        schema_v=1, extra_meta={"candle_count": len(parsed)})
            success += 1
            print(f"[{i}/{total}] ✓ {symbol} | {len(parsed)} candles")

        print("\n=================================")
        print(" INDEX PIPELINE COMPLETED")
        print("=================================")
        print(f"\n✅ Success: {success}")
        print(f"❌ Failed : {failed}")
        print(f"📦 Total  : {total}\n")


if __name__ == "__main__":
    asyncio.run(main())
