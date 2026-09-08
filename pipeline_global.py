import os
import json
import time
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

# Batch size for Upstox LTP/OHLC calls. Keeps the query string well under any
# URL-length limit and reduces blast radius if one batch errors out — a
# failed batch only drops those instruments, not the whole list. 50 is
# comfortably safe for index keys (well below Upstox's own instrument-count
# ceiling per request).
BATCH_SIZE = 50

# Indian indices — batch-safe keys (spaces as literal space, encoded by requests)
# These use v3 LTP batch.
#
# India VIX moved here from the old global list: it's an NSE_INDEX key, so it
# batches with the rest and no longer needs the one-request-per-instrument
# path that existed only to handle %20-encoded GLOBAL_INDEX keys.
#
# Keys verified against Upstox's Instrument Search API (v2/instruments/search,
# segments=INDEX) — don't guess key formats, they don't always match the
# display name exactly (e.g. "NIFTY MID SELECT", "Nifty Pvt Bank").
INDIAN_INSTRUMENTS = [
    # ── Benchmark ─────────────────────────────────────────────────────────
    {"key": "NSE_INDEX|Nifty 50",          "name": "NIFTY 50",        "country": "India"},
    {"key": "NSE_INDEX|Nifty Bank",        "name": "NIFTY BANK",      "country": "India"},
    {"key": "NSE_INDEX|India VIX",         "name": "India VIX",       "country": "India"},
    {"key": "BSE_INDEX|SENSEX",            "name": "SENSEX",          "country": "India"},

    # ── Broad market ──────────────────────────────────────────────────────
    {"key": "NSE_INDEX|Nifty Midcap 50",   "name": "NIFTY MIDCAP 50", "country": "India"},
    {"key": "NSE_INDEX|NIFTY MID SELECT",  "name": "NIFTY MID SEL",   "country": "India"},
    {"key": "NSE_INDEX|Nifty Smallcap 50", "name": "NIFTY SMLCAP 50", "country": "India"},
    {"key": "NSE_INDEX|Nifty Next 50",     "name": "NIFTY NEXT 50",   "country": "India"},
    {"key": "NSE_INDEX|Nifty 100",         "name": "NIFTY 100",       "country": "India"},
    {"key": "NSE_INDEX|Nifty 200",         "name": "NIFTY 200",       "country": "India"},
    {"key": "NSE_INDEX|Nifty 500",         "name": "NIFTY 500",       "country": "India"},
    {"key": "NSE_INDEX|NIFTY MIDCAP 100",  "name": "NIFTY MIDCAP 100","country": "India"},
    {"key": "NSE_INDEX|NIFTY MIDCAP 150",  "name": "NIFTY MIDCAP 150","country": "India"},
    {"key": "NSE_INDEX|NIFTY SMLCAP 100",  "name": "NIFTY SMLCAP 100","country": "India"},
    {"key": "NSE_INDEX|NIFTY SMLCAP 250",  "name": "NIFTY SMLCAP 250","country": "India"},
    {"key": "NSE_INDEX|NIFTY MIDSML 400",  "name": "NIFTY MIDSML 400","country": "India"},

    # ── Sectoral ──────────────────────────────────────────────────────────
    {"key": "NSE_INDEX|Nifty Fin Service", "name": "NIFTY FIN SVC",   "country": "India"},
    {"key": "NSE_INDEX|Nifty IT",          "name": "NIFTY IT",        "country": "India"},
    {"key": "NSE_INDEX|Nifty Auto",        "name": "NIFTY AUTO",      "country": "India"},
    {"key": "NSE_INDEX|Nifty FMCG",        "name": "NIFTY FMCG",      "country": "India"},
    {"key": "NSE_INDEX|Nifty Pharma",      "name": "NIFTY PHARMA",    "country": "India"},
    {"key": "NSE_INDEX|Nifty Realty",      "name": "NIFTY REALTY",    "country": "India"},
    {"key": "NSE_INDEX|Nifty Metal",       "name": "NIFTY METAL",     "country": "India"},
    {"key": "NSE_INDEX|Nifty Energy",      "name": "NIFTY ENERGY",    "country": "India"},
    {"key": "NSE_INDEX|Nifty Media",       "name": "NIFTY MEDIA",     "country": "India"},
    {"key": "NSE_INDEX|Nifty PSU Bank",    "name": "NIFTY PSU BANK",  "country": "India"},
    {"key": "BSE_INDEX|BANKEX",            "name": "BANKEX",          "country": "India"},
    {"key": "NSE_INDEX|Nifty Pvt Bank",    "name": "NIFTY PVT BANK",   "country": "India"},
    {"key": "NSE_INDEX|NIFTY HEALTHCARE",  "name": "NIFTY HEALTHCARE", "country": "India"},
    {"key": "NSE_INDEX|NIFTY CONSR DURBL", "name": "NIFTY CONSR DURBL","country": "India"},
    {"key": "NSE_INDEX|NIFTY OIL AND GAS", "name": "NIFTY OIL & GAS",  "country": "India"},
    {"key": "NSE_INDEX|Nifty Chemicals",   "name": "NIFTY CHEMICALS",  "country": "India"},

    # ── Thematic ──────────────────────────────────────────────────────────
    {"key": "NSE_INDEX|Nifty Infra",       "name": "NIFTY INFRA",       "country": "India"},
    {"key": "NSE_INDEX|Nifty PSE",         "name": "NIFTY PSE",         "country": "India"},
    {"key": "NSE_INDEX|Nifty CPSE",        "name": "NIFTY CPSE",        "country": "India"},
    {"key": "NSE_INDEX|Nifty Commodities", "name": "NIFTY COMMODITIES", "country": "India"},
    {"key": "NSE_INDEX|Nifty MNC",         "name": "NIFTY MNC",         "country": "India"},
    {"key": "NSE_INDEX|Nifty Serv Sector", "name": "NIFTY SERV SECTOR", "country": "India"},
    {"key": "NSE_INDEX|Nifty EV",          "name": "NIFTY EV",          "country": "India"},
    {"key": "NSE_INDEX|Nifty Ind Defence", "name": "NIFTY IND DEFENCE", "country": "India"},
    {"key": "NSE_INDEX|NIFTY IND DIGITAL", "name": "NIFTY IND DIGITAL", "country": "India"},
    {"key": "NSE_INDEX|Nifty Ind Tourism", "name": "NIFTY IND TOURISM", "country": "India"},
    {"key": "NSE_INDEX|NIFTY INDIA MFG",   "name": "NIFTY INDIA MFG",   "country": "India"},
    {"key": "NSE_INDEX|Nifty InfraLog",    "name": "NIFTY INFRALOG",    "country": "India"},
    {"key": "NSE_INDEX|Nifty Internet",    "name": "NIFTY INTERNET",    "country": "India"},
    {"key": "NSE_INDEX|Nifty IPO",         "name": "NIFTY IPO",         "country": "India"},
    {"key": "NSE_INDEX|Nifty Mobility",    "name": "NIFTY MOBILITY",    "country": "India"},
    {"key": "NSE_INDEX|Nifty Multi Infra", "name": "NIFTY MULTI INFRA", "country": "India"},
    {"key": "NSE_INDEX|Nifty Multi Mfg",   "name": "NIFTY MULTI MFG",   "country": "India"},
    {"key": "NSE_INDEX|Nifty New Consump", "name": "NIFTY NEW CONSUMP", "country": "India"},
    {"key": "NSE_INDEX|Nifty NonCyc Cons", "name": "NIFTY NONCYC CONS", "country": "India"},
    {"key": "NSE_INDEX|Nifty RailwaysPSU", "name": "NIFTY RAILWAYSPSU", "country": "India"},
    {"key": "NSE_INDEX|Nifty REITs Realty","name": "NIFTY REITS REALTY","country": "India"},
    {"key": "NSE_INDEX|Nifty Rural",       "name": "NIFTY RURAL",       "country": "India"},
    {"key": "NSE_INDEX|Nifty Trans Logis", "name": "NIFTY TRANS LOGIS", "country": "India"},
    {"key": "NSE_INDEX|Nifty Waves",       "name": "NIFTY WAVES",       "country": "India"},
    {"key": "NSE_INDEX|NiftyConglomerate", "name": "NIFTYCONGLOMERATE", "country": "India"},
    {"key": "NSE_INDEX|Nifty Consumption", "name": "NIFTY CONSUMPTION", "country": "India"},
    {"key": "NSE_INDEX|Nifty Housing",     "name": "NIFTY HOUSING",     "country": "India"},
    {"key": "NSE_INDEX|Nifty CoreHousing", "name": "NIFTY COREHOUSING", "country": "India"},
    {"key": "NSE_INDEX|Nifty GrowSect 15", "name": "NIFTY GROWSECT 15", "country": "India"},
    {"key": "NSE_INDEX|Nifty FPI 150",     "name": "NIFTYFPI",          "country": "India"},
]

# ── Fetch Indian indices in batches (requests handles space encoding) ──────────
def fetch_indian(instruments):
    ltp_data  = {}
    ohlc_data = {}

    total_batches = (len(instruments) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(instruments), BATCH_SIZE):
        batch     = instruments[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        key_str   = ",".join(ins["key"] for ins in batch)

        print(f"  Batch {batch_num}/{total_batches} — {len(batch)} indices…")

        r = requests.get(
            "https://api.upstox.com/v3/market-quote/ltp",
            params={"instrument_key": key_str},
            headers=HEADERS, timeout=30,
        )
        if r.status_code == 200:
            batch_ltp = r.json().get("data", {})
            ltp_data.update(batch_ltp)
            print(f"    LTP OK — {len(batch_ltp)} quotes")
        else:
            print(f"    LTP ERR → {r.status_code}: {r.text[:200]}")

        r2 = requests.get(
            "https://api.upstox.com/v3/market-quote/ohlc",
            params={"instrument_key": key_str, "interval": "1d"},
            headers=HEADERS, timeout=30,
        )
        if r2.status_code == 200:
            batch_ohlc = r2.json().get("data", {})
            ohlc_data.update(batch_ohlc)
            print(f"    OHLC OK — {len(batch_ohlc)} quotes")
        else:
            print(f"    OHLC ERR → {r2.status_code}: {r2.text[:200]}")

        # Small pause between batches so we don't hammer the rate limit.
        if i + BATCH_SIZE < len(instruments):
            time.sleep(0.5)

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
    lookup    = instr["key"]
    colon_key = lookup.replace("|", ":")

    lq = ltp_data.get(lookup)  or ltp_data.get(colon_key)  or {}
    oq = ohlc_data.get(lookup) or ohlc_data.get(colon_key) or {}

    if not lq:
        print(f"  MISSING: {instr['name']}")

    ltp    = lq.get("last_price")
    prev_c = lq.get("cp")
    volume = lq.get("volume")
    live   = oq.get("live_ohlc") or {}

    chg     = round(ltp - prev_c, 4) if ltp is not None and prev_c is not None else None
    chg_pct = round(chg / prev_c * 100, 2) if chg and prev_c else None

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
    print("=== pipeline_global.py (Indian indices only) ===")

    print(f"Fetching {len(INDIAN_INSTRUMENTS)} Indian indices (batch size {BATCH_SIZE})...")
    ltp, ohlc = fetch_indian(INDIAN_INSTRUMENTS)

    results = [build_entry(i, ltp, ohlc) for i in INDIAN_INSTRUMENTS]

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "data":       results,
    }

    print("Uploading to R2...")
    upload_r2("global_markets.json", output)
    print(f"Done. {len(results)} instruments written.")

if __name__ == "__main__":
    main()
