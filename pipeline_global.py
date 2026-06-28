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

# All instruments — spaces encoded as %20, | and ^ kept raw
INSTRUMENTS = [
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
    {"key": "GLOBAL_INDICATOR|USDINR",    "name": "USD/INR",     "country": ""},
    {"key": "GLOBAL_INDICATOR|BZUSD",     "name": "Brent Oil",   "country": ""},
    {"key": "GLOBAL_INDICATOR|CLUSD",     "name": "WTI Oil",     "country": ""},
]

# ── Fetch V3 OHLC ─────────────────────────────────────────────────────────────
def fetch_quotes(instruments):
    results = {}
    for instr in instruments:
        url = f"https://api.upstox.com/v3/market-quote/ohlc?instrument_key={instr['key']}&interval=1d"
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            results.update(r.json().get("data", {}))
            print(f"  OK  {instr['name']}")
        else:
            print(f"  ERR {instr['name']} → {r.status_code}: {r.text[:120]}")
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
    lookup    = instr["key"].replace("%20", " ")
    colon_key = lookup.replace("|", ":")
    quote     = raw.get(lookup) or raw.get(colon_key) or {}

    if not quote:
        print(f"  MISSING: {instr['name']}")

    live   = quote.get("live_ohlc") or {}
    prev_o = quote.get("prev_ohlc") or {}
    ltp    = live.get("close")       # live_ohlc.close = current LTP
    prev_c = prev_o.get("close")     # prev_ohlc.close = previous day close
    chg    = round(ltp - prev_c, 4) if ltp is not None and prev_c is not None else None
    chg_pct = round(chg / prev_c * 100, 2) if chg is not None and prev_c and prev_c != 0 else None

    return {
        "key":        lookup,
        "name":       instr["name"],
        "country":    instr["country"],
        "ltp":        ltp,
        "change":     chg,
        "change_pct": chg_pct,
        "open":       live.get("open"),
        "high":       live.get("high"),
        "low":        live.get("low"),
        "close":      prev_c,
        "volume":     quote.get("volume"),
        "ts":         quote.get("ts"),
    }

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=== pipeline_global.py ===")
    print(f"Fetching {len(INSTRUMENTS)} instruments (v3 OHLC)...")

    raw     = fetch_quotes(INSTRUMENTS)
    results = [build_entry(i, raw) for i in INSTRUMENTS]

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data":       results,
    }

    print("Uploading to R2...")
    upload_r2("global_markets.json", output)
    print(f"Done. {len(results)} instruments written.")

if __name__ == "__main__":
    main()
