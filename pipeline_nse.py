"""
pipeline_nse.py
Fetches from NSE archives:
  - sec_list (today's circuit bands, EQ+BE only)
  - eq_band_changes (next trading day's band changes, EQ+BE only)
  - bulk.csv (latest bulk deals)
  - block.csv (latest block deals)

Saves to R2:
  - nse/bands.json         → all EQ+BE symbols with current band + next-day change if any
  - nse/bulk.json          → today's bulk deals
  - nse/block.json         → today's block deals
  - nse/bulk_history.json  → accumulated bulk deal history, keyed by symbol
  - nse/block_history.json → accumulated block deal history, keyed by symbol
"""

import os
import io
import json
import time
import csv
import boto3
from datetime import date, timedelta
from botocore.client import Config
import requests

# ── R2 config ────────────────────────────────────────────────────────────────
R2_ENDPOINT   = os.environ["R2_ENDPOINT"]
R2_BUCKET     = os.environ["R2_BUCKET"]
AWS_ACCESS_KEY= os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY= os.environ["AWS_SECRET_ACCESS_KEY"]

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

# ── NSE session ───────────────────────────────────────────────────────────────
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def nse_session():
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    s.get("https://www.nseindia.com", timeout=15)
    time.sleep(1)
    return s

# ── Date helpers ──────────────────────────────────────────────────────────────
NSE_HOLIDAYS_2026 = {
    date(2026, 1, 26),   # Republic Day
    date(2026, 3, 25),   # Holi
    date(2026, 4, 2),    # Ram Navami (tentative)
    date(2026, 4, 14),   # Dr. Ambedkar Jayanti
    date(2026, 5, 1),    # Maharashtra Day
    date(2026, 8, 15),   # Independence Day
    date(2026, 10, 2),   # Gandhi Jayanti
    date(2026, 11, 4),   # Diwali Laxmi Puja (tentative)
    date(2026, 12, 25),  # Christmas
}

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NSE_HOLIDAYS_2026

def next_trading_day(d: date) -> date:
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt

def fmt(d: date) -> str:
    return d.strftime("%d%m%Y")

# ── Fetch helpers ─────────────────────────────────────────────────────────────
def fetch_csv(session, url, retries=3) -> list[dict]:
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()
            text = r.content.decode("utf-8-sig").strip()
            reader = csv.DictReader(io.StringIO(text))
            return [row for row in reader]
        except Exception as e:
            print(f"  Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(3)
    raise RuntimeError(f"Failed to fetch {url}")

# ── R2 helpers ────────────────────────────────────────────────────────────────
def r2_get_json(key: str) -> dict | list | None:
    try:
        obj = s3.get_object(Bucket=R2_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"  R2 read error {key}: {e}")
        return None

def r2_put_json(key: str, data):
    body = json.dumps(data, ensure_ascii=False, indent=2)
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"  Saved → {key} ({len(body)} bytes)")

# ── Main logic ────────────────────────────────────────────────────────────────
def run():
    today     = date.today()
    next_day  = next_trading_day(today)
    today_str = fmt(today)
    next_str  = fmt(next_day)

    print(f"Today: {today}  |  Next trading day: {next_day}")

    session = nse_session()

    # 1. sec_list — today's bands (EQ + BE only)
    print("\n[1] Fetching sec_list...")
    sec_url = f"https://archives.nseindia.com/content/equities/sec_list_{today_str}.csv"
    sec_rows = fetch_csv(session, sec_url)

    bands = {}  # symbol → {series, circuit}
    for row in sec_rows:
        series = row.get("Series", "").strip()
        if series not in ("EQ", "BE"):
            continue
        symbol = row.get("Symbol", "").strip()
        band   = row.get("Band", "").strip()
        if symbol:
            bands[symbol] = {"series": series, "circuit": int(band) if band.isdigit() else band}

    print(f"  EQ+BE symbols: {len(bands)}")

    # 2. eq_band_changes — next trading day (EQ + BE only)
    print(f"\n[2] Fetching eq_band_changes for {next_day}...")
    chg_url = f"https://archives.nseindia.com/content/equities/eq_band_changes_{next_str}.csv"
    chg_rows = fetch_csv(session, chg_url)

    changes = {}  # symbol → {from, to}
    for row in chg_rows:
        series = row.get("Series", "").strip()
        if series not in ("EQ", "BE"):
            continue
        symbol = row.get("Symbol", "").strip()
        frm    = row.get("From", "").strip()
        to     = row.get("To", "").strip()
        if symbol:
            changes[symbol] = {
                "from": int(frm) if frm.isdigit() else frm,
                "to":   int(to)  if to.isdigit()  else to,
            }

    print(f"  Band changes (EQ+BE): {len(changes)}")

    # 3. Merge into bands.json
    for symbol, chg in changes.items():
        if symbol in bands:
            bands[symbol]["change"] = chg
        else:
            # symbol in changes but not in sec_list — add it
            bands[symbol] = {"series": "?", "circuit": chg["from"], "change": chg}

    # Convert to list for easier frontend consumption
    bands_list = [{"symbol": sym, **info} for sym, info in sorted(bands.items())]
    r2_put_json("nse/bands.json", {
        "date": today.isoformat(),
        "next_trading_day": next_day.isoformat(),
        "data": bands_list,
    })

    # 4. bulk.csv
    print("\n[3] Fetching bulk deals...")
    bulk_rows = fetch_csv(session, "https://archives.nseindia.com/content/equities/bulk.csv")
    # Filter out "NO RECORDS"
    bulk_rows = [r for r in bulk_rows if r.get("Symbol", "").strip() not in ("", "NO RECORDS")]
    bulk_clean = []
    for r in bulk_rows:
        bulk_clean.append({
            "date":     r.get("Date", "").strip(),
            "symbol":   r.get("Symbol", "").strip(),
            "name":     r.get("Security Name", "").strip(),
            "client":   r.get("Client Name", "").strip(),
            "side":     r.get("Buy/Sell", "").strip(),
            "qty":      r.get("Quantity Traded", "").strip(),
            "price":    r.get("Trade Price / Wght. Avg. Price", "").strip(),
        })
    r2_put_json("nse/bulk.json", bulk_clean)
    print(f"  Bulk deals: {len(bulk_clean)}")

    # 5. block.csv
    print("\n[4] Fetching block deals...")
    block_rows = fetch_csv(session, "https://archives.nseindia.com/content/equities/block.csv")
    block_rows = [r for r in block_rows if r.get("Symbol", "").strip() not in ("", "NO RECORDS")]
    block_clean = []
    for r in block_rows:
        block_clean.append({
            "date":   r.get("Date", "").strip(),
            "symbol": r.get("Symbol", "").strip(),
            "name":   r.get("Security Name", "").strip(),
            "client": r.get("Client Name", "").strip(),
            "side":   r.get("Buy/Sell", "").strip(),
            "qty":    r.get("Quantity Traded", "").strip(),
            "price":  r.get("Trade Price / Wght. Avg. Price", "").strip(),
        })
    r2_put_json("nse/block.json", block_clean)
    print(f"  Block deals: {len(block_clean)}")

    # 6. Bulk history — symbol-wise accumulated
    print("\n[5] Updating bulk history...")
    bulk_hist = r2_get_json("nse/bulk_history.json") or {}
    for deal in bulk_clean:
        sym = deal["symbol"]
        if sym not in bulk_hist:
            bulk_hist[sym] = []
        # Avoid duplicates (same date+client+side+qty)
        key = f"{deal['date']}|{deal['client']}|{deal['side']}|{deal['qty']}"
        existing_keys = {
            f"{d['date']}|{d['client']}|{d['side']}|{d['qty']}"
            for d in bulk_hist[sym]
        }
        if key not in existing_keys:
            bulk_hist[sym].append(deal)
    r2_put_json("nse/bulk_history.json", bulk_hist)

    # 7. Block history — symbol-wise accumulated
    print("\n[6] Updating block history...")
    block_hist = r2_get_json("nse/block_history.json") or {}
    for deal in block_clean:
        sym = deal["symbol"]
        if sym not in block_hist:
            block_hist[sym] = []
        key = f"{deal['date']}|{deal['client']}|{deal['side']}|{deal['qty']}"
        existing_keys = {
            f"{d['date']}|{d['client']}|{d['side']}|{d['qty']}"
            for d in block_hist[sym]
        }
        if key not in existing_keys:
            block_hist[sym].append(deal)
    r2_put_json("nse/block_history.json", block_hist)

    print("\n✅ pipeline_nse.py complete")

if __name__ == "__main__":
    run()
