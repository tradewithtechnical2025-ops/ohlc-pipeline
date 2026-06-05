"""
pipeline_nse.py
Fetches from NSE archives:
  - sec_list_DDMMYYYY.csv   → today's circuit bands (EQ+BE only)
  - eq_band_changes_DDMMYYYY.csv → next trading day's changes (EQ+BE only)
  - bulk.csv                → latest bulk deals
  - block.csv               → latest block deals

Saves to R2 via Worker:
  nse/bands.json         → EQ+BE symbols with current band + next-day change if any
  nse/bulk.json          → latest bulk deals
  nse/block.json         → latest block deals
  nse/bulk_history.json  → symbol-wise accumulated bulk history
  nse/block_history.json → symbol-wise accumulated block history
"""

import asyncio
import csv
import io
import json
import os
import sys
import time
from datetime import date, timedelta

import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}

NSE_BASE = "https://archives.nseindia.com/content/equities"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer":    "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── NSE Holidays 2025-2026 ────────────────────────────────────────────────────
NSE_HOLIDAYS = {
    date(2026, 1, 26),
    date(2026, 3, 25),
    date(2026, 4, 14),
    date(2026, 4, 18),
    date(2026, 5, 1),
    date(2026, 8, 15),
    date(2026, 10, 2),
    date(2026, 11, 4),
    date(2026, 12, 25),
}

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NSE_HOLIDAYS

def next_trading_day(d: date) -> date:
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt

def fmt(d: date) -> str:
    return d.strftime("%d%m%Y")


# ── NSE fetch (sync requests via httpx) ──────────────────────────────────────
def nse_get_csv(session: httpx.Client, url: str, retries=3) -> list[dict]:
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            text = r.content.decode("utf-8-sig").strip()
            if "NO RECORDS" in text.splitlines()[:3]:
                return []
            reader = csv.DictReader(io.StringIO(text))
            rows = [row for row in reader]
            print(f"  ✓ {url.split('/')[-1]} → {len(rows)} rows")
            return rows
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"  404: {url.split('/')[-1]}")
                raise  # let caller handle 404 fallback
            print(f"  Attempt {attempt+1} failed: {e}")
            time.sleep(3 * (attempt + 1))
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"Failed to fetch: {url}")

def prev_trading_day(d: date) -> date:
    prv = d - timedelta(days=1)
    while not is_trading_day(prv):
        prv -= timedelta(days=1)
    return prv

def nse_get_csv_with_fallback(session: httpx.Client, base_url_tpl: str, d: date, max_back=5) -> tuple[list[dict], date]:
    """Try date d, fallback to previous trading days if 404."""
    cur = d
    for _ in range(max_back):
        url = base_url_tpl.format(fmt(cur))
        try:
            rows = nse_get_csv(session, url)
            return rows, cur
        except (httpx.HTTPStatusError, RuntimeError):
            print(f"  Falling back from {cur}...")
            cur = prev_trading_day(cur)
    raise RuntimeError(f"Could not fetch {base_url_tpl} for last {max_back} trading days")

def make_nse_session() -> httpx.Client:
    client = httpx.Client(headers=NSE_HEADERS, follow_redirects=True, timeout=30)
    try:
        client.get("https://www.nseindia.com")
        time.sleep(1)
    except Exception:
        pass
    return client


# ── Worker R2 helpers ─────────────────────────────────────────────────────────
async def r2_get(client: httpx.AsyncClient, filename: str):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

async def r2_put(client: httpx.AsyncClient, filename: str, data):
    body = json.dumps(data, ensure_ascii=False).encode()
    r = await client.post(f"{WORKER_URL}?file={filename}", headers=UP_HEADERS,
                          content=body, timeout=120)
    r.raise_for_status()
    print(f"  ↑ {filename} ({len(body)/1024:.1f} KB)")


# ── Main ──────────────────────────────────────────────────────────────────────
async def run():
    today    = date.today()
    next_day = next_trading_day(today)
    print(f"Today: {today}  |  Next trading day: {next_day}")

    # ── 1. Fetch from NSE (sync) ──────────────────────────────────────────────
    nse = make_nse_session()

    print("\n[1] sec_list (today's bands)...")
    sec_rows = nse_get_csv(nse, f"{NSE_BASE}/sec_list_{fmt(today)}.csv")

    print(f"\n[2] eq_band_changes (next day: {next_day})...")
    chg_rows = nse_get_csv(nse, f"{NSE_BASE}/eq_band_changes_{fmt(next_day)}.csv")

    print("\n[3] bulk.csv...")
    bulk_rows = nse_get_csv(nse, f"{NSE_BASE}/bulk.csv")

    print("\n[4] block.csv...")
    block_rows = nse_get_csv(nse, f"{NSE_BASE}/block.csv")

    nse.close()

    # ── 2. Process bands ──────────────────────────────────────────────────────
    bands = {}
    for row in sec_rows:
        series = row.get("Series", "").strip()
        if series not in ("EQ", "BE"):
            continue
        sym  = row.get("Symbol", "").strip()
        band = row.get("Band", "").strip()
        if sym:
            bands[sym] = {
                "series":  series,
                "circuit": int(band) if band.isdigit() else band,
            }

    changes = {}
    for row in chg_rows:
        series = row.get("Series", "").strip()
        if series not in ("EQ", "BE"):
            continue
        sym = row.get("Symbol", "").strip()
        frm = row.get("From", "").strip()
        to  = row.get("To", "").strip()
        if sym:
            changes[sym] = {
                "from": int(frm) if frm.isdigit() else frm,
                "to":   int(to)  if to.isdigit()  else to,
            }

    for sym, chg in changes.items():
        if sym in bands:
            bands[sym]["change"] = chg
        else:
            bands[sym] = {"series": "?", "circuit": chg["from"], "change": chg}

    bands_out = {
        "date":             today.isoformat(),
        "next_trading_day": next_day.isoformat(),
        "data":             [{"symbol": s, **v} for s, v in sorted(bands.items())],
    }
    print(f"\n  Bands: {len(bands)} symbols  |  Changes: {len(changes)}")

    # ── 3. Process bulk/block ─────────────────────────────────────────────────
    def clean_deals(rows, has_remarks=False):
        out = []
        for r in rows:
            sym = r.get("Symbol", "").strip()
            if not sym or sym == "NO RECORDS":
                continue
            d = {
                "date":   r.get("Date", "").strip(),
                "symbol": sym,
                "name":   r.get("Security Name", "").strip(),
                "client": r.get("Client Name", "").strip(),
                "side":   r.get("Buy/Sell", "").strip(),
                "qty":    r.get("Quantity Traded", "").strip(),
                "price":  r.get("Trade Price / Wght. Avg. Price", "").strip(),
            }
            if has_remarks:
                d["remarks"] = r.get("Remarks", "").strip()
            out.append(d)
        return out

    bulk_clean  = clean_deals(bulk_rows, has_remarks=True)
    block_clean = clean_deals(block_rows)
    print(f"  Bulk: {len(bulk_clean)}  Block: {len(block_clean)}")

    # ── 4. Upload to R2 ───────────────────────────────────────────────────────
    print("\n[5] Uploading to R2...")
    async with httpx.AsyncClient() as client:

        # bands, bulk, block — straight upload
        await asyncio.gather(
            r2_put(client, "nse/bands.json",  bands_out),
            r2_put(client, "nse/bulk.json",   bulk_clean),
            r2_put(client, "nse/block.json",  block_clean),
        )

        # bulk history
        bulk_hist  = await r2_get(client, "nse/bulk_history.json")  or {}
        block_hist = await r2_get(client, "nse/block_history.json") or {}

        def merge_history(hist: dict, deals: list) -> int:
            added = 0
            for d in deals:
                sym = d["symbol"]
                if sym not in hist:
                    hist[sym] = []
                key = f"{d['date']}|{d['client']}|{d['side']}|{d['qty']}"
                existing = {f"{x['date']}|{x['client']}|{x['side']}|{x['qty']}" for x in hist[sym]}
                if key not in existing:
                    hist[sym].append(d)
                    added += 1
            return added

        b1 = merge_history(bulk_hist,  bulk_clean)
        b2 = merge_history(block_hist, block_clean)
        print(f"  History — bulk +{b1}  block +{b2}")

        await asyncio.gather(
            r2_put(client, "nse/bulk_history.json",  bulk_hist),
            r2_put(client, "nse/block_history.json", block_hist),
        )

    print("\n✅ pipeline_nse.py complete")


if __name__ == "__main__":
    asyncio.run(run())
