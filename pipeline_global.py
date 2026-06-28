import os
import json
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
UPSTOX_TOKEN  = os.environ["UPSTOX_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"]
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

HEADERS = {
    "Accept":        "application/json",
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
}

# GLOBAL_INDEX instruments (supported by v2 full quotes)
INDEX_INSTRUMENTS = [
    {"key": "GLOBAL_INDEX|SGX%20NIFTY",  "name": "GIFT NIFTY",  "country": "India"},
    {"key": "GLOBAL_INDEX|^GSPC",         "name": "S&P 500",     "country": "America"},
    {"key": "GLOBAL_INDEX|^DJI",          "name": "DOW JONES",   "country": "America"},
    {"key": "GLOBAL_INDEX|IXIX",          "name": "NASDAQ 100",  "country": "America"},
    {"key": "GLOBAL_INDEX|^GDAXI",        "name": "DAX",         "country": "Germany"},
    {"key": "GLOBAL_INDEX|^FTSE",         "name": "FTSE 100",    "country": "UK"},
    {"key": "GLOBAL_INDEX|^FCHI",         "name": "CAC 40",      "country": "France"},
    {"key": "GLOBAL_INDEX|^HSI",          "name": "HANG SENG",   "country": "Hong Kong"},
    {"key": "GLOBAL_INDEX|^N225",         "name": "NIKKEI 225",  "country": "Japan"},
    {"key": "NSE_INDEX|India%20VIX",      "name": "India VIX",   "country": "India"},
]

# GLOBAL_INDICATOR — try LTP V3 separately
INDICATOR_INSTRUMENTS = [
    {"key": "GLOBAL_INDICATOR|USDINR",    "name": "USD/INR",     "country": ""},
    {"key": "GLOBAL_INDICATOR|BZUSD",     "name": "Brent Oil",   "country": ""},
    {"key": "GLOBAL_INDICATOR|CLUSD",     "name": "WTI Oil",     "country": ""},
]

# ── Fetch full quotes (indices) ────────────────────────────────────────────────
def fetch_full_quotes(instruments):
    results = {}
    for instr in instruments:
        url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instr['key']}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            results.update(r.json().get("data", {}))
            print(f"  OK  {instr['name']}")
        else:
            print(f"  ERR {instr['name']} → {r.status_code}: {r.text[:100]}")
    return results

# ── Fetch LTP V3 (indicators) ─────────────────────────────────────────────────
def fetch_ltp_v3(instruments):
    results = {}
    for instr in instruments:
        url = f"https://api.upstox.com/v3/market-quote/ltp?instrument_key={instr['key']}"
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            results.update(r.json().get("data", {}))
            print(f"  OK  {instr['name']}")
        else:
            print(f"  ERR {instr['name']} → {r.status_code}: {r.text[:100]}")
    return results

# ── Upload to R2 ───────────────────────────────────────────────────────────────
def upload_r2(filename, payload):
    data = json.dumps(payload, separators=(",", ":")).encode()
    r = requests.post(
        f"{WORKER_URL}?file={filename}",
        headers={"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"},
        data=data,
        timeout=60,
    )
    r.raise_for_status()
    print(f"  Uploaded {filename} ({len(data)/1024:.1f} KB) → {r.status_code}")

# ── Build result entry ─────────────────────────────────────────────────────────
def build_entry(instr, raw):
    lookup = instr["key"].replace("%20", " ")
    colon_key = lookup.replace("|", ":")
    quote = raw.get(lookup) or raw.get(colon_key) or {}

    if not quote:
        print(f"  MISSING: {instr['name']}")

    ohlc    = quote.get("ohlc", {})
    ltp     = quote.get("last_price")
    prev    = ohlc.get("close")
    chg_pct = round((ltp - prev) / prev * 100, 2) if ltp and prev and prev != 0 else None

    return {
        "key":        lookup,
        "name":       instr["name"],
        "country":    instr["country"],
        "ltp":        ltp,
        "change":     quote.get("net_change"),
        "change_pct": chg_pct,
        "open":       ohlc.get("open"),
        "high":       ohlc.get("high"),
        "low":        ohlc.get("low"),
        "close":      prev,
        "volume":     quote.get("volume"),
        "ts":         quote.get("last_trade_time"),
    }

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=== pipeline_global.py ===")

    print(f"Fetching {len(INDEX_INSTRUMENTS)} indices (v2 full quotes)...")
    index_raw = fetch_full_quotes(INDEX_INSTRUMENTS)

    all_raw = index_raw
    all_instruments = INDEX_INSTRUMENTS

    results = [build_entry(i, all_raw) for i in all_instruments]

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data":       results,
    }

    print("Uploading to R2...")
    upload_r2("global_markets.json", output)
    print(f"Done. {len(results)} instruments written.")

if __name__ == "__main__":
    main()
