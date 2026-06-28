import os
import json
import time
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
UPSTOX_TOKEN   = os.environ["UPSTOX_TOKEN"]
R2_WORKER_URL  = os.environ["R2_WORKER_URL"]          # e.g. https://your-worker.workers.dev
R2_SECRET      = os.environ["R2_SECRET_TOKEN"]

QUOTE_URL      = "https://api.upstox.com/v2/market-quote/quotes"
HEADERS        = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept":        "application/json",
}

# Instrument keys from Global Instruments file
INSTRUMENTS = [
    {"key": "GLOBAL_INDEX|SGX NIFTY",   "name": "GIFT NIFTY",  "country": "India"},
    {"key": "GLOBAL_INDEX|^GSPC",        "name": "S&P 500",     "country": "America"},
    {"key": "GLOBAL_INDEX|^DJI",         "name": "DOW JONES",   "country": "America"},
    {"key": "GLOBAL_INDEX|IXIX",         "name": "NASDAQ 100",  "country": "America"},
    {"key": "GLOBAL_INDEX|^GDAXI",       "name": "DAX",         "country": "Germany"},
    {"key": "GLOBAL_INDEX|^FTSE",        "name": "FTSE 100",    "country": "UK"},
    {"key": "GLOBAL_INDEX|^FCHI",        "name": "CAC 40",      "country": "France"},
    {"key": "GLOBAL_INDEX|^HSI",         "name": "HANG SENG",   "country": "Hong Kong"},
    {"key": "GLOBAL_INDEX|^N225",        "name": "NIKKEI 225",  "country": "Japan"},
    {"key": "GLOBAL_INDICATOR|USDINR",   "name": "USD/INR",     "country": ""},
    {"key": "GLOBAL_INDICATOR|BZUSD",    "name": "Brent Oil",   "country": ""},
    {"key": "GLOBAL_INDICATOR|CLUSD",    "name": "WTI Oil",     "country": ""},
    {"key": "NSE_INDEX|India VIX",       "name": "India VIX",   "country": "India"},
]

# ── Fetch quotes ───────────────────────────────────────────────────────────────
def fetch_quotes(keys: list[str]) -> dict:
    """Fetch quotes for a batch of instrument keys (max 500)."""
    params = {"instrument_key": ",".join(keys)}
    r = requests.get(QUOTE_URL, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise ValueError(f"Upstox error: {data}")
    return data.get("data", {})

# ── Upload to R2 ───────────────────────────────────────────────────────────────
def upload_r2(filename: str, payload: dict):
    r = requests.put(
        f"{R2_WORKER_URL}/{filename}",
        headers={
            "X-Secret-Token": R2_SECRET,
            "Content-Type":   "application/json",
        },
        data=json.dumps(payload),
        timeout=30,
    )
    r.raise_for_status()
    print(f"  Uploaded {filename} → {r.status_code}")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=== pipeline_global.py ===")

    all_keys = [i["key"] for i in INSTRUMENTS]

    print(f"Fetching {len(all_keys)} instruments...")
    raw = fetch_quotes(all_keys)

    results = []
    for instr in INSTRUMENTS:
        key  = instr["key"]
        # Upstox returns data keyed by instrument_key (colons replaced with pipes)
        quote = raw.get(key) or raw.get(key.replace("|", ":")) or {}

        ohlc    = quote.get("ohlc", {})
        depth   = quote.get("depth", {})

        results.append({
            "key":          key,
            "name":         instr["name"],
            "country":      instr["country"],
            "ltp":          quote.get("last_price"),
            "change":       quote.get("net_change"),
            "change_pct":   quote.get("net_change_percent"),  # may be None; compute below
            "open":         ohlc.get("open"),
            "high":         ohlc.get("high"),
            "low":          ohlc.get("low"),
            "close":        ohlc.get("close"),       # prev close
            "volume":       quote.get("volume"),
            "ts":           quote.get("last_trade_time"),
        })

    # Compute change_pct if not provided by API
    for r in results:
        if r["change_pct"] is None and r["ltp"] and r["close"] and r["close"] != 0:
            r["change_pct"] = round((r["ltp"] - r["close"]) / r["close"] * 100, 2)

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data":       results,
    }

    upload_r2("global_markets.json", output)
    print(f"Done. {len(results)} instruments written.")

if __name__ == "__main__":
    main()
