"""
pipeline_nse.py
Fetches from NSE archives:
  - sec_list_DDMMYYYY.csv        → today's circuit bands (EQ+BE only)
  - eq_band_changes_DDMMYYYY.csv → next trading day's changes (EQ+BE only)
  - bulk.csv                     → latest bulk deals
  - block.csv                    → latest block deals

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

# ── NSE Holidays 2026 ─────────────────────────────────────────────────────────
NSE_HOLIDAYS = {
    date(2026, 1, 26),
    date(2026, 3, 25),
    date(2026, 4, 14),
    date(2026, 4, 18),
    date(2026, 5,  1),
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

def prev_trading_day(d: date) -> date:
    prv = d - timedelta(days=1)
    while not is_trading_day(prv):
        prv -= timedelta(days=1)
    return prv

def fmt(d: date) -> str:
    return d.strftime("%d%m%Y")


# ── NSE fetch ─────────────────────────────────────────────────────────────────
def make_nse_session() -> httpx.Client:
    client = httpx.Client(headers=NSE_HEADERS, follow_redirects=True, timeout=30)
    try:
        client.get("https://www.nseindia.com")
        time.sleep(1)
    except Exception:
        pass
    return client

def fetch_csv_url(session: httpx.Client, url: str) -> list[dict] | None:
    """Returns rows or None on 404. Raises on other errors."""
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        text = r.content.decode("utf-8-sig").strip()
        if "NO RECORDS" in text.splitlines()[:3]:
            return []
        rows = list(csv.DictReader(io.StringIO(text)))
        print(f"  ✓ {url.split('/')[-1]} → {len(rows)} rows")
        return rows
    except httpx.HTTPStatusError:
        raise
    except Exception as e:
        raise RuntimeError(str(e))

def fetch_with_fallback(session: httpx.Client, tpl: str, start: date, direction: str = "prev", max_tries: int = 5) -> tuple[list[dict], date]:
    """
    tpl: url template with {} for date string e.g. f"{NSE_BASE}/sec_list_{{}}.csv"
    direction: "prev" = go back, "next" = go forward on 404
    """
    cur = start
    for i in range(max_tries):
        url = tpl.format(fmt(cur))
        rows = fetch_csv_url(session, url)
        if rows is not None:
            if i > 0:
                print(f"  ⚠ Fell back to {cur}")
            return rows, cur
        print(f"  404 for {cur}, trying {'previous' if direction=='prev' else 'next'} trading day...")
        cur = prev_trading_day(cur) if direction == "prev" else next_trading_day(cur)
    raise RuntimeError(f"Could not fetch after {max_tries} attempts: {tpl}")


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

    nse = make_nse_session()

    # 1. sec_list — fallback to prev trading day if today not uploaded yet
    print("\n[1] sec_list (today's bands)...")
    sec_rows, sec_date = fetch_with_fallback(
        nse, f"{NSE_BASE}/sec_list_{{}}.csv", today, direction="prev"
    )

    # 2. eq_band_changes — for next trading day, fallback to next+1 etc
    print(f"\n[2] eq_band_changes (next day: {next_day})...")
    chg_rows, chg_date = fetch_with_fallback(
        nse, f"{NSE_BASE}/eq_band_changes_{{}}.csv", next_day, direction="next"
    )

    # 3. bulk + block (static URLs, always latest)
    print("\n[3] bulk.csv...")
    bulk_rows = fetch_csv_url(nse, f"{NSE_BASE}/bulk.csv") or []

    print("\n[4] block.csv...")
    block_rows = fetch_csv_url(nse, f"{NSE_BASE}/block.csv") or []

    nse.close()

    # ── Process bands ─────────────────────────────────────────────────────────
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
        "date":             sec_date.isoformat(),
        "next_trading_day": chg_date.isoformat(),
        "data":             [{"symbol": s, **v} for s, v in sorted(bands.items())],
    }
    print(f"\n  Bands: {len(bands)} symbols  |  Changes: {len(changes)}")

    # ── Process bulk/block ────────────────────────────────────────────────────
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

    # ── Upload to R2 ──────────────────────────────────────────────────────────
    print("\n[5] Uploading to R2...")
    async with httpx.AsyncClient() as client:

        await asyncio.gather(
            r2_put(client, "nse/bands.json",  bands_out),
            r2_put(client, "nse/bulk.json",   bulk_clean),
            r2_put(client, "nse/block.json",  block_clean),
        )

        bulk_hist, block_hist = await asyncio.gather(
            r2_get(client, "nse/bulk_history.json"),
            r2_get(client, "nse/block_history.json"),
        )
        bulk_hist  = bulk_hist  or {}
        block_hist = block_hist or {}

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
