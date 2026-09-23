"""
pipeline_us.py
US Stocks OHLC pipeline (Tiingo) for tradewithtech.in

- Backfill mode: pulls last 3 years of daily OHLC for each symbol in US_SYMBOLS
- Daily mode: pulls only latest price and appends to existing R2 JSON
- Uploads one JSON file per symbol via the Worker: us_ohlc_<SYMBOL>.json

Follows the same Worker-proxy upload/download pattern as pipeline_nse.py
(WORKER_URL + WORKER_TOKEN, X-Secret-Token header, httpx async, retry w/
exponential backoff) instead of talking to R2/boto3 directly.

Env vars required (already exist as GitHub Actions secrets):
  TIINGO_API_KEY
  WORKER_URL
  WORKER_TOKEN

Run modes:
  python pipeline_us.py --mode backfill
  python pipeline_us.py --mode daily
"""

import os
import sys
import json
import asyncio
import argparse
import logging
from datetime import datetime, timedelta

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

TIINGO_API_KEY = os.environ["TIINGO_API_KEY"]
WORKER_URL     = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN   = os.environ["WORKER_TOKEN"]

WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

TIINGO_BASE = "https://api.tiingo.com/tiingo/daily"
BACKFILL_YEARS = 3
RETRY = 5
REQUEST_DELAY_SEC = 1.0  # be polite to Tiingo, avoid hourly limit issues on free plan
CONCURRENCY = 5          # parallel symbols in flight at once

# Starter list of 50 popular US stocks — edit as needed
US_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRKB", "AVGO", "JPM",
    "LLY", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ", "NFLX",
    "BAC", "ABBV", "CRM", "WMT", "KO", "AMD", "PEP", "MRK", "ADBE", "TMO",
    "CSCO", "ORCL", "ACN", "MCD", "LIN", "ABT", "DHR", "WFC", "TXN", "CAT",
    "PM", "INTU", "IBM", "GE", "QCOM", "AMGN", "NOW", "SPGI", "UBER", "BA",
]


def r2_filename(symbol: str) -> str:
    # '.' in tickers like BRK.B is fine in a filename, but keep it explicit/safe
    return f"us_ohlc_{symbol.replace('.', '_')}.json"


# ---------------------------------------------------------------------------
# Worker upload / download (same pattern as pipeline_nse.py)
# ---------------------------------------------------------------------------

async def r2_upload(client: httpx.AsyncClient, filename: str, data):
    if not isinstance(data, (bytes, str)):
        data = json.dumps(data)
    if isinstance(data, str):
        data = data.encode()
    url = f"{WORKER_URL}?file={filename}"
    last_err = None
    for attempt in range(RETRY):
        try:
            r = await client.post(url, headers={**WORKER_HEADERS, "Content-Type": "application/json"}, content=data, timeout=90)
        except httpx.RequestError as e:
            last_err = e
            log.warning(f"  Upload {filename}: {e}, retry {attempt + 1}")
            await asyncio.sleep(2 ** attempt)
            continue
        if r.status_code == 200:
            return
        last_err = RuntimeError(f"Upload failed: HTTP {r.status_code}")
        log.warning(f"  Upload {filename}: HTTP {r.status_code}, retry {attempt + 1}")
        await asyncio.sleep(2 ** attempt)
    raise last_err


async def r2_download(client: httpx.AsyncClient, filename: str):
    url = f"{WORKER_URL}/{filename}"
    last_err = None
    for attempt in range(RETRY):
        try:
            r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
        except httpx.RequestError as e:
            last_err = e
            log.warning(f"  Download {filename}: {e}, retry {attempt + 1}")
            await asyncio.sleep(2 ** attempt)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 200:
            return r.json()
        last_err = RuntimeError(f"Download failed: HTTP {r.status_code}")
        log.warning(f"  Download {filename}: HTTP {r.status_code}, retry {attempt + 1}")
        await asyncio.sleep(2 ** attempt)
    raise last_err


# ---------------------------------------------------------------------------
# Tiingo calls
# ---------------------------------------------------------------------------

async def fetch_meta(client: httpx.AsyncClient, symbol: str):
    url = f"{TIINGO_BASE}/{symbol}"
    params = {"token": TIINGO_API_KEY}
    r = await client.get(url, params=params, timeout=30)
    if r.status_code == 404:
        log.warning(f"  {symbol}: meta not found on Tiingo")
        return None
    r.raise_for_status()
    return r.json()


async def fetch_historical(client: httpx.AsyncClient, symbol: str, start_date: str, end_date: str):
    url = f"{TIINGO_BASE}/{symbol}/prices"
    params = {"startDate": start_date, "endDate": end_date, "token": TIINGO_API_KEY, "format": "json"}
    r = await client.get(url, params=params, timeout=30)
    if r.status_code == 404:
        log.warning(f"  {symbol}: not found on Tiingo, skipping")
        return None
    r.raise_for_status()
    return r.json()


async def fetch_latest(client: httpx.AsyncClient, symbol: str):
    url = f"{TIINGO_BASE}/{symbol}/prices"
    params = {"token": TIINGO_API_KEY, "format": "json"}
    r = await client.get(url, params=params, timeout=30)
    if r.status_code == 404:
        log.warning(f"  {symbol}: not found on Tiingo, skipping")
        return None
    r.raise_for_status()
    data = r.json()
    return data[-1] if data else None


def normalize_bar(bar):
    return {
        "date": bar["date"][:10],
        "o": bar.get("open"),
        "h": bar.get("high"),
        "l": bar.get("low"),
        "c": bar.get("close"),
        "v": bar.get("volume"),
    }


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

async def backfill_one(sem, tiingo_client, r2_client, symbol, start_str, end_str, stats):
    async with sem:
        try:
            raw = await fetch_historical(tiingo_client, symbol, start_str, end_str)
            if not raw:
                stats["failed"].append(symbol)
                return
            bars = [normalize_bar(b) for b in raw]

            meta = await fetch_meta(tiingo_client, symbol)
            company = {
                "name": meta.get("name") if meta else None,
                "exchangeCode": meta.get("exchangeCode") if meta else None,
                "description": meta.get("description") if meta else None,
                "startDate": meta.get("startDate") if meta else None,
            } if meta else {}

            payload = {
                "symbol": symbol,
                "updated": datetime.utcnow().isoformat() + "Z",
                "company": company,
                "bars": bars,
            }
            await r2_upload(r2_client, r2_filename(symbol), payload)
            log.info(f"  {symbol}: {len(bars)} bars uploaded (meta: {'ok' if meta else 'missing'})")
            stats["ok"] += 1
        except Exception as e:
            log.error(f"  {symbol}: {e}")
            stats["failed"].append(symbol)
        await asyncio.sleep(REQUEST_DELAY_SEC)


async def run_backfill():
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=365 * BACKFILL_YEARS)
    start_str, end_str = start_date.isoformat(), end_date.isoformat()

    log.info(f"Backfill: {start_str} to {end_str} for {len(US_SYMBOLS)} symbols")
    stats = {"ok": 0, "failed": []}
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as tiingo_client, httpx.AsyncClient() as r2_client:
        tasks = [backfill_one(sem, tiingo_client, r2_client, sym, start_str, end_str, stats) for sym in US_SYMBOLS]
        await asyncio.gather(*tasks)

    log.info(f"Backfill done: {stats['ok']} ok, {len(stats['failed'])} failed")
    if stats["failed"]:
        log.info("Failed symbols: " + ", ".join(stats["failed"]))


async def daily_one(sem, tiingo_client, r2_client, symbol, stats):
    async with sem:
        try:
            latest = await fetch_latest(tiingo_client, symbol)
            if not latest:
                stats["failed"].append(symbol)
                return
            new_bar = normalize_bar(latest)

            filename = r2_filename(symbol)
            existing = await r2_download(r2_client, filename)
            if existing is None:
                log.warning(f"  {symbol}: no existing file, run backfill first. Skipping.")
                stats["failed"].append(symbol)
                return

            bars = existing.get("bars", [])
            if bars and bars[-1]["date"] == new_bar["date"]:
                bars[-1] = new_bar
            else:
                bars.append(new_bar)

            existing["bars"] = bars
            existing["updated"] = datetime.utcnow().isoformat() + "Z"
            await r2_upload(r2_client, filename, existing)
            log.info(f"  {symbol}: appended {new_bar['date']}")
            stats["ok"] += 1
        except Exception as e:
            log.error(f"  {symbol}: {e}")
            stats["failed"].append(symbol)
        await asyncio.sleep(REQUEST_DELAY_SEC)


async def run_daily():
    log.info(f"Daily update for {len(US_SYMBOLS)} symbols")
    stats = {"ok": 0, "failed": []}
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as tiingo_client, httpx.AsyncClient() as r2_client:
        tasks = [daily_one(sem, tiingo_client, r2_client, sym, stats) for sym in US_SYMBOLS]
        await asyncio.gather(*tasks)

    log.info(f"Daily update done: {stats['ok']} ok, {len(stats['failed'])} failed")
    if stats["failed"]:
        log.info("Failed symbols: " + ", ".join(stats["failed"]))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    args = parser.parse_args()

    if args.mode == "backfill":
        asyncio.run(run_backfill())
    else:
        asyncio.run(run_daily())


if __name__ == "__main__":
    main()
