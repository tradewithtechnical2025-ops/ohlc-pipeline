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

# Instrument keys — spaces encoded as %20, | and ^ kept raw
# India VIX excluded: not supported in global quotes v2
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
    {"key": "GLOBAL_INDICATOR|USDINR",    "name": "USD/INR",     "country": ""},
    {"key": "GLOBAL_INDICATOR|BZUSD",     "name": "Brent Oil",   "country": ""},
    {"key": "GLOBAL_INDICATOR|CLUSD",     "name": "WTI Oil",     "country": ""},
    {"key": "NSE_INDEX|India%20VIX",      "name": "India VIX",   "country": "India"},
]

# ── Fetch quotes ───────────────────────────────────────────────────────────────
def fetch_quotes(instruments):
    """Fetch in small batches; skip invalid keys gracefully."""
    results = {}
    # Try all at once first; fall back to one-by-one on 400
    key_str = ",".join(i["key"] for i in instruments)
    url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={key_str}"
    r = requests.get(url, headers=HEADERS, timeout=15)

    if r.status_code == 400:
        print("Batch failed — fetching one by one...")
        for instr in instruments:
            u = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instr['key']}"
            resp = requests.get(u, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                d = resp.json().get("data", {})
                results.update(d)
                print(f"  OK  {instr['name']}")
            else:
                print(f"  ERR {instr['name']} → {resp.status_code}: {resp.text[:120]}")
        return results

    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise ValueError(f"Upstox error: {data}")
    return data.get("data", {})

# ── Upload to R2 ───────────────────────────────────────────────────────────────
def upload_r2(filename, payload):
    r = requests.put(
        f"{WORKER_URL}/{filename}",
        headers={"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30,
    )
    r.raise_for_status()
    print(f"  Uploaded {filename} → {r.status_code}")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("=== pipeline_global.py ===")
    print(f"Fetching {len(INSTRUMENTS)} instruments...")

    raw = fetch_quotes(INSTRUMENTS)
    print(f"Got {len(raw)} quotes from API")
    print("Keys returned:", list(raw.keys())[:5])

    results = []
    for instr in INSTRUMENTS:
        # API returns keys with : instead of |
        lookup = instr["key"].replace("%20", " ")
        colon_key = lookup.replace("|", ":")
        quote = raw.get(lookup) or raw.get(colon_key) or {}

        if not quote:
            print(f"  MISSING: {instr['name']} ({lookup})")

        ohlc = quote.get("ohlc", {})
        ltp  = quote.get("last_price")
        prev = ohlc.get("close")

        chg_pct = None
        if ltp and prev and prev != 0:
            chg_pct = round((ltp - prev) / prev * 100, 2)

        results.append({
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
        })

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data":       results,
    }

    upload_r2("global_markets.json", output)
    print(f"Done. {len(results)} instruments written.")

if __name__ == "__main__":
    main()
