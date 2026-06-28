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

# ── Fetch V3 LTP (ltp + cp + volume) ──────────────────────────────────────────
def fetch_ltp(instruments):
    # Also fetch OHLC from v3 for open/high/low
    ltp_data  = {}
    ohlc_data = {}

    for instr in instruments:
        key = instr["key"]

        # V3 LTP — ltp, cp (prev close), volume
        r = requests.get(
            f"https://api.upstox.com/v3/market-quote/ltp?instrument_key={key}",
            headers=HEADERS, timeout=15,
        )
        if r.status_code == 200:
            ltp_data.update(r.json().get("data", {}))
            print(f"  LTP OK  {instr['name']}")
        else:
            print(f"  LTP ERR {instr['name']} → {r.status_code}: {r.text[:100]}")

        # V3 OHLC (1d) — open/high/low/live_ohlc
        r2 = requests.get(
            f"https://api.upstox.com/v3/market-quote/ohlc?instrument_key={key}&interval=1d",
            headers=HEADERS, timeout=15,
        )
        if r2.status_code == 200:
            ohlc_data.update(r2.json().get("data", {}))

    return ltp_data, ohlc_data

# ── Upload to R2 ───────────────────────────────────────────────────────────────
def upload_r2(filename, payload):
    data = json.dumps(payload, separators=(",", ":")).encode()
    r = requests.post(
        f"{WORKER_URL}?file={filename}",
        headers={"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"},
        data=data, timeout=60,
    )
    r.raise_for_status()
    print(f"  Uploaded {filename} ({len(data)/1024:.1f} KB) → {r.status_code}")

# ── Build result entry ─────────────────────────────────────────────────────────
def build_entry(instr, ltp_data, ohlc_data):
    lookup    = instr["key"].replace("%20", " ")
    colon_key = lookup.replace("|", ":")

    lq = ltp_data.get(lookup)  or ltp_data.get(colon_key)  or {}
    oq = ohlc_data.get(lookup) or ohlc_data.get(colon_key) or {}

    ltp    = lq.get("last_price")
    prev_c = lq.get("cp")           # V3 LTP: cp = previous day close
    volume = lq.get("volume")

    live   = oq.get("live_ohlc") or {}
    chg    = round(ltp - prev_c, 4) if ltp is not None and prev_c is not None else None
    chg_pct = round(chg / prev_c * 100, 2) if chg and prev_c else None

    if not lq:
        print(f"  MISSING: {instr['name']}")

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
        "volume":     volume,
        "ts":         lq.get("last_trade_time"),
    }

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=== pipeline_global.py ===")
    print(f"Fetching {len(INSTRUMENTS)} instruments (v3 LTP + OHLC)...")

    ltp_data, ohlc_data = fetch_ltp(INSTRUMENTS)
    results = [build_entry(i, ltp_data, ohlc_data) for i in INSTRUMENTS]

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data":       results,
    }

    print("Uploading to R2...")
    upload_r2("global_markets.json", output)
    print(f"Done. {len(results)} instruments written.")

if __name__ == "__main__":
    main()
