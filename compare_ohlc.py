#!/usr/bin/env python3
"""
Upstox (live API) vs Finedge (R2 chunks) — OHLC Comparison
============================================================

Usage:
  python compare_ohlc.py                         # all NSE symbols
  python compare_ohlc.py --days 30               # lookback window (default: 30)
  python compare_ohlc.py --limit 50              # cap symbols (testing)
  python compare_ohlc.py --sym RELIANCE TCS      # specific symbols

Env vars required:
  UPSTOX_TOKEN   — Upstox Bearer token
  WORKER_URL     — Cloudflare Worker base URL
  WORKER_TOKEN   — Worker secret token

Output:
  ohlc_comparison.csv   — full row-level OHLC diff per date
  ohlc_summary.csv      — per-symbol summary
"""

import asyncio
import csv
import json
import os
import sys
from datetime import date, timedelta

import httpx

# ── Config ────────────────────────────────────────────────────────────────────

UPSTOX_TOKEN = os.environ.get("UPSTOX_TOKEN", "")
WORKER_URL   = os.environ.get("WORKER_URL", "").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

UPSTOX_BASE       = "https://api.upstox.com/v2"
UPSTOX_SEARCH_URL = f"{UPSTOX_BASE}/instruments/search"

R2_CHUNKS     = 8
CONCURRENCY   = 5      # OHLC fetch concurrency
RATE_DELAY    = 0.4    # seconds between OHLC requests per slot
RETRY         = 3

# Instrument search — keep lower, it's a different quota
SEARCH_CONC   = 3
SEARCH_DELAY  = 0.3

MISMATCH_PCT  = 0.05   # flag if OHLC diff > 0.05%

# ── Date helpers ──────────────────────────────────────────────────────────────

def today_str():
    return date.today().isoformat()

def date_n_ago(n):
    return (date.today() - timedelta(days=n)).isoformat()

# ── R2 ────────────────────────────────────────────────────────────────────────

async def r2_download(client, filename):
    url = f"{WORKER_URL}/{filename}"
    r = await client.get(url, headers={"X-Secret-Token": WORKER_TOKEN}, timeout=90)
    if r.status_code == 404: return None
    if r.status_code != 200:
        print(f"  [R2] {filename} — HTTP {r.status_code}"); return None
    return r.json()

async def load_classification(client):
    data = await r2_download(client, "classification.json")
    if not data or not isinstance(data, list):
        print("[ERROR] classification.json missing or invalid"); sys.exit(1)
    return data

async def load_r2_ohlc(client):
    tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"  [R2] ohlc_{i+1}.json error: {res}")
        elif res and "stocks" in res:
            all_data.update(res["stocks"])
    print(f"  [R2] {len(all_data)} stocks from {R2_CHUNKS} chunks")
    return all_data

# ── Upstox instrument search → ISIN + instrument_key ─────────────────────────

async def search_instrument(client, sem, symbol):
    """
    GET /v2/instruments/search?query=SYMBOL&exchange=NSE&instrument_type=EQ
    Returns (symbol, isin, instrument_key) or (symbol, None, None)
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
    }
    params = {
        "query"          : symbol,
        "exchange"       : "NSE",
        "instrument_type": "EQ",
    }
    async with sem:
        await asyncio.sleep(SEARCH_DELAY)
        try:
            r = await client.get(UPSTOX_SEARCH_URL, headers=headers,
                                 params=params, timeout=15)
        except httpx.RequestError as e:
            print(f"  [Search] {symbol}: network error — {e}")
            return symbol, None, None

    if r.status_code == 401:
        print("  [Search] ❌ 401 — check UPSTOX_TOKEN"); return symbol, None, None
    if r.status_code != 200:
        return symbol, None, None

    try:
        data = r.json()
    except Exception:
        return symbol, None, None

    items = data.get("data") or []
    # Find exact tradingsymbol match in NSE EQ segment
    for item in items:
        seg  = item.get("segment", "")
        ts   = item.get("trading_symbol", "") or item.get("tradingsymbol", "")
        isin = item.get("isin", "")
        ikey = item.get("instrument_key", "")
        if seg == "NSE_EQ" and ts.upper() == symbol and isin:
            return symbol, isin, ikey

    # Fallback — first NSE_EQ result
    for item in items:
        if item.get("segment", "") == "NSE_EQ":
            isin = item.get("isin", "")
            ikey = item.get("instrument_key", "")
            if isin:
                return symbol, isin, ikey

    return symbol, None, None


async def build_isin_map(client, symbols):
    """
    Batch search all symbols via /instruments/search.
    Returns {symbol: (isin, instrument_key)}
    """
    print(f"  Searching instruments for {len(symbols)} symbols "
          f"(concurrency={SEARCH_CONC})…")
    sem = asyncio.Semaphore(SEARCH_CONC)
    results = {}
    done = 0
    BATCH = 100

    for batch_start in range(0, len(symbols), BATCH):
        batch = symbols[batch_start:batch_start+BATCH]
        tasks = [search_instrument(client, sem, sym) for sym in batch]
        batch_results = await asyncio.gather(*tasks)
        for sym, isin, ikey in batch_results:
            if isin:
                results[sym] = (isin, ikey)
            done += 1
        print(f"    [{done}/{len(symbols)}]  found: {len(results)}")

    print(f"  ISIN map: {len(results)} / {len(symbols)} resolved")
    missing = [s for s in symbols if s not in results]
    if missing[:5]:
        print(f"  Unresolved sample: {missing[:5]}")
    return results

# ── Upstox OHLC fetch ─────────────────────────────────────────────────────────

async def fetch_upstox(client, sem, symbol, instrument_key, from_date, to_date):
    """
    GET /v2/historical-candle/{instrument_key}/day/{to_date}/{from_date}
    Returns (symbol, {date: {o,h,l,c,v}})
    """
    url = (f"{UPSTOX_BASE}/historical-candle/"
           f"{instrument_key}/day/{to_date}/{from_date}")
    headers = {
        "Accept"       : "application/json",
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
    }
    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            try:
                r = await client.get(url, headers=headers, timeout=30)
            except httpx.RequestError as e:
                await asyncio.sleep(2 ** attempt); continue

        if r.status_code == 401:
            print("  [Upstox] ❌ 401 — check UPSTOX_TOKEN"); return symbol, {}
        if r.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"  [Upstox] {symbol}: 429 — waiting {wait}s")
            await asyncio.sleep(wait); continue
        if r.status_code in (502, 503, 504):
            await asyncio.sleep(2 ** attempt); continue
        if r.status_code != 200:
            return symbol, {}

        try: payload = r.json()
        except: return symbol, {}

        candles = (payload.get("data") or {}).get("candles") or []
        result = {}
        for row in candles:
            d = str(row[0])[:10]  # "2024-01-15T00:00:00+05:30" → "2024-01-15"
            if from_date <= d <= to_date:
                result[d] = {
                    "o": row[1], "h": row[2],
                    "l": row[3], "c": row[4], "v": row[5],
                }
        return symbol, result

    return symbol, {}

# ── R2 stock → {date: ohlcv} ─────────────────────────────────────────────────

def r2_to_date_map(stock, from_date, to_date):
    result = {}
    dates = stock.get("d", [])
    vols  = stock.get("v") or [0] * len(dates)
    for i, d in enumerate(dates):
        if from_date <= d <= to_date:
            result[d] = {
                "o": stock["o"][i], "h": stock["h"][i],
                "l": stock["l"][i], "c": stock["c"][i],
                "v": vols[i] if i < len(vols) else 0,
            }
    return result

# ── Compare ───────────────────────────────────────────────────────────────────

def compare(symbol, up_data, r2_data):
    rows = []
    for d in sorted(set(up_data) & set(r2_data)):
        u = up_data[d]; f = r2_data[d]
        diffs = {}; mismatch = False
        for field in ("o", "h", "l", "c"):
            uv = u.get(field); fv = f.get(field)
            if uv and fv and uv > 0:
                pct = abs(uv - fv) / uv * 100
                diffs[field] = round(pct, 4)
                if pct > MISMATCH_PCT: mismatch = True
            else:
                diffs[field] = None
        uv_v = u.get("v") or 0; fv_v = f.get("v") or 0
        vol_diff = round(abs(uv_v - fv_v) / uv_v * 100, 2) if uv_v else None
        rows.append({
            "symbol"    : symbol,
            "date"      : d,
            "up_o"      : u.get("o"), "r2_o": f.get("o"), "diff_o_pct": diffs.get("o"),
            "up_h"      : u.get("h"), "r2_h": f.get("h"), "diff_h_pct": diffs.get("h"),
            "up_l"      : u.get("l"), "r2_l": f.get("l"), "diff_l_pct": diffs.get("l"),
            "up_c"      : u.get("c"), "r2_c": f.get("c"), "diff_c_pct": diffs.get("c"),
            "up_v"      : uv_v,       "r2_v": fv_v,       "diff_v_pct": vol_diff,
            "mismatch"  : mismatch,
        })
    return rows

def make_summary(symbol, rows, up_data, r2_data):
    total = len(rows); mm = sum(1 for r in rows if r["mismatch"])
    max_c = max((r["diff_c_pct"] or 0) for r in rows) if rows else 0
    avg_c = sum((r["diff_c_pct"] or 0) for r in rows) / total if total else 0
    return {
        "symbol"        : symbol,
        "common_dates"  : total,
        "only_upstox"   : len(set(up_data) - set(r2_data)),
        "only_r2"       : len(set(r2_data) - set(up_data)),
        "mismatches"    : mm,
        "mismatch_pct"  : round(mm / total * 100, 2) if total else 0,
        "max_close_diff": round(max_c, 4),
        "avg_close_diff": round(avg_c, 4),
    }

# ── CSV export ────────────────────────────────────────────────────────────────

def export_csv(rows, path):
    if not rows: print(f"  (no data — {path} skipped)"); return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  ✅ {path}  ({len(rows)} rows)")

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    args = sys.argv[1:]

    days = 30
    if "--days" in args:
        i = args.index("--days"); days = int(args[i+1]); args = args[:i]+args[i+2:]

    limit = None
    if "--limit" in args:
        i = args.index("--limit"); limit = int(args[i+1]); args = args[:i]+args[i+2:]

    specific = []
    if "--sym" in args:
        i = args.index("--sym"); specific = [s.upper() for s in args[i+1:]]; args = args[:i]

    from_date = date_n_ago(days + 15)   # +15 buffer for weekends/holidays
    to_date   = today_str()

    print(f"\n{'═'*65}")
    print(f"  Upstox (API) vs Finedge (R2) — OHLC Comparison")
    print(f"  Range     : {from_date} → {to_date}  (~{days} trading days)")
    print(f"  Threshold : {MISMATCH_PCT}%")
    print(f"{'═'*65}\n")

    missing_env = [v for v in ("UPSTOX_TOKEN","WORKER_URL","WORKER_TOKEN")
                   if not os.environ.get(v)]
    if missing_env:
        print(f"[ERROR] Missing env vars: {missing_env}"); sys.exit(1)

    async with httpx.AsyncClient() as client:

        # 1 — symbol list
        print("[1/4] classification.json from R2…")
        classification = await load_classification(client)
        nse_symbols = sorted({
            str(s.get("symbol","")).strip().upper()
            for s in classification
            if str(s.get("exchange","")).strip() == "NSE" and s.get("symbol")
        })
        print(f"  NSE symbols: {len(nse_symbols)}")

        if specific:
            nse_symbols = [s for s in nse_symbols if s in specific]
            print(f"  Filtered to --sym: {len(nse_symbols)}")
        if limit:
            nse_symbols = nse_symbols[:limit]
            print(f"  Capped at --limit: {len(nse_symbols)}")

        # 2 — ISIN map via search API
        print("\n[2/4] Resolving instrument keys via /instruments/search…")
        isin_map  = await build_isin_map(client, nse_symbols)
        fetchable = [s for s in nse_symbols if s in isin_map]
        no_isin   = [s for s in nse_symbols if s not in isin_map]
        print(f"  Fetchable : {len(fetchable)}  |  Unresolved: {len(no_isin)}")

        # 3 — R2 OHLC chunks
        print("\n[3/4] Loading R2 OHLC chunks…")
        r2_all = await load_r2_ohlc(client)

        # 4 — Upstox OHLC fetch + compare
        print(f"\n[4/4] Fetching Upstox OHLC…  "
              f"(concurrency={CONCURRENCY}, delay={RATE_DELAY}s/slot)")

        sem = asyncio.Semaphore(CONCURRENCY)
        detail_rows  = []
        summary_rows = []
        done = 0
        BATCH = 50

        for batch_start in range(0, len(fetchable), BATCH):
            batch = fetchable[batch_start:batch_start+BATCH]
            tasks = [
                fetch_upstox(client, sem, sym, isin_map[sym][1], from_date, to_date)
                for sym in batch
            ]
            results = await asyncio.gather(*tasks)

            for sym, up_data in results:
                done += 1
                r2_stock = r2_all.get(sym)
                if not r2_stock:
                    summary_rows.append({
                        "symbol": sym, "common_dates": 0,
                        "only_upstox": len(up_data), "only_r2": 0,
                        "mismatches": 0, "mismatch_pct": 0,
                        "max_close_diff": 0, "avg_close_diff": 0,
                    })
                    continue
                r2_data = r2_to_date_map(r2_stock, from_date, to_date)
                rows    = compare(sym, up_data, r2_data)
                detail_rows.extend(rows)
                summary_rows.append(make_summary(sym, rows, up_data, r2_data))

            pct = done / len(fetchable) * 100
            mm  = sum(r["mismatches"] for r in summary_rows)
            print(f"  [{done:>4}/{len(fetchable)}  {pct:5.1f}%]  mismatches so far: {mm}")

    # Export
    print("\n  Exporting…")
    export_csv(detail_rows,  "ohlc_comparison.csv")
    export_csv(summary_rows, "ohlc_summary.csv")

    # Stats
    total = len(detail_rows)
    mm    = sum(1 for r in detail_rows if r["mismatch"])
    syms_mm = sum(1 for r in summary_rows if r["mismatches"] > 0)
    no_r2   = sum(1 for r in summary_rows if r["common_dates"] == 0)

    print(f"\n{'═'*65}")
    print(f"  Date×symbol pairs       : {total}")
    if total:
        print(f"  OHLC mismatches         : {mm}  ({mm/total*100:.2f}%)")
    print(f"  Symbols with mismatch   : {syms_mm}")
    print(f"  Symbols missing in R2   : {no_r2}")
    print(f"{'═'*65}\n")

if __name__ == "__main__":
    asyncio.run(main())
