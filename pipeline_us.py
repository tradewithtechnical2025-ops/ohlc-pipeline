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
REQUEST_DELAY_SEC = 3.0  # be polite to Tiingo, avoid hourly limit issues on free plan
CONCURRENCY = 1          # sequential — free plan is only 50 requests/hour
TIINGO_429_WAIT_SEC = 90 # Tiingo free plan resets roughly on a rolling basis; back off and retry

# TEST list — 10 stocks only, to stay well under Tiingo free plan's 50 req/hour limit
# (10 symbols x 2 calls [meta+historical] = 20 calls per backfill run)
# Once ready to scale up, swap back to the full 50-symbol list.
US_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "JPM", "LLY",
]

# Full 50-symbol list — kept here for later use
# US_SYMBOLS = [
#     "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRKB", "AVGO", "JPM",
#     "LLY", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ", "NFLX",
#     "BAC", "ABBV", "CRM", "WMT", "KO", "AMD", "PEP", "MRK", "ADBE", "TMO",
#     "CSCO", "ORCL", "ACN", "MCD", "LIN", "ABT", "DHR", "WFC", "TXN", "CAT",
#     "PM", "INTU", "IBM", "GE", "QCOM", "AMGN", "NOW", "SPGI", "UBER", "BA",
# ]


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

async def _tiingo_get(client: httpx.AsyncClient, url: str, params: dict, label: str):
    """GET with 404->None and 429-aware backoff/retry (Tiingo hourly rate limit)."""
    for attempt in range(RETRY):
        r = await client.get(url, params=params, timeout=30)
        if r.status_code == 404:
            log.warning(f"  {label}: not found on Tiingo, skipping")
            return None
        if r.status_code == 429:
            log.warning(f"  {label}: 429 rate limited, waiting {TIINGO_429_WAIT_SEC}s (attempt {attempt + 1}/{RETRY})")
            await asyncio.sleep(TIINGO_429_WAIT_SEC)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"{label}: still rate limited after {RETRY} retries")


async def fetch_meta(client: httpx.AsyncClient, symbol: str):
    url = f"{TIINGO_BASE}/{symbol}"
    params = {"token": TIINGO_API_KEY}
    return await _tiingo_get(client, url, params, f"{symbol} meta")


async def fetch_historical(client: httpx.AsyncClient, symbol: str, start_date: str, end_date: str):
    url = f"{TIINGO_BASE}/{symbol}/prices"
    params = {"startDate": start_date, "endDate": end_date, "token": TIINGO_API_KEY, "format": "json"}
    return await _tiingo_get(client, url, params, symbol)


async def fetch_latest(client: httpx.AsyncClient, symbol: str):
    url = f"{TIINGO_BASE}/{symbol}/prices"
    params = {"token": TIINGO_API_KEY, "format": "json"}
    data = await _tiingo_get(client, url, params, symbol)
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

# ---------------------------------------------------------------------------
# Screener feed — lightweight, single-file summary for ALL symbols
# (price/change/volume + sector/industry/mcap), same idea as production's
# screener_feed.json. This is what us_stocks.html should fetch once,
# instead of looping N individual us_ohlc_<SYMBOL>.json fetches — the
# per-symbol files stay purely for the chart page (one fetch per click).
# ---------------------------------------------------------------------------
async def generate_screener_feed(r2_client, latest_bars):
    if not latest_bars:
        log.warning("  screener feed: no bars collected, skipping")
        return
    try:
        meta = await r2_download(r2_client, "us_company_meta.json")
    except Exception as e:
        log.warning(f"  screener feed: couldn't load us_company_meta.json ({e}), sector/industry/mcap will be blank")
        meta = None
    meta_stocks = (meta or {}).get("stocks", {})

    rows = []
    for symbol, info in latest_bars.items():
        last = info["last"]
        prev = info["prev"]
        price = last["c"] if last else None
        change_pct = None
        if last and prev and prev.get("c"):
            change_pct = ((last["c"] - prev["c"]) / prev["c"]) * 100

        m = meta_stocks.get(symbol, {})
        rows.append({
            "symbol": symbol,
            "name": m.get("name") or info.get("name") or symbol,
            "sector": m.get("sector"),
            "industry": m.get("industry"),
            "mcap": m.get("marketCap"),
            "ltp": price,
            "pct_ch": change_pct,
            "volume": last.get("v") if last else None,
        })

    payload = {
        "updated": datetime.utcnow().isoformat() + "Z",
        "count": len(rows),
        "stocks": rows,
    }
    await r2_upload(r2_client, "us_screener_feed.json", payload)
    log.info(f"  screener feed: uploaded us_screener_feed.json ({len(rows)} symbols)")


async def backfill_one(sem, tiingo_client, r2_client, symbol, start_str, end_str, stats, latest_bars):
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
            if len(bars) >= 1:
                latest_bars[symbol] = {
                    "name": company.get("name"),
                    "last": bars[-1],
                    "prev": bars[-2] if len(bars) >= 2 else None,
                }
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
    latest_bars = {}
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as tiingo_client, httpx.AsyncClient() as r2_client:
        tasks = [backfill_one(sem, tiingo_client, r2_client, sym, start_str, end_str, stats, latest_bars) for sym in US_SYMBOLS]
        await asyncio.gather(*tasks)
        await generate_screener_feed(r2_client, latest_bars)

    log.info(f"Backfill done: {stats['ok']} ok, {len(stats['failed'])} failed")
    if stats["failed"]:
        log.info("Failed symbols: " + ", ".join(stats["failed"]))


async def daily_one(sem, tiingo_client, r2_client, symbol, stats, latest_bars):
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
            if len(bars) >= 1:
                latest_bars[symbol] = {
                    "name": (existing.get("company") or {}).get("name"),
                    "last": bars[-1],
                    "prev": bars[-2] if len(bars) >= 2 else None,
                }
        except Exception as e:
            log.error(f"  {symbol}: {e}")
            stats["failed"].append(symbol)
        await asyncio.sleep(REQUEST_DELAY_SEC)


async def run_daily():
    log.info(f"Daily update for {len(US_SYMBOLS)} symbols")
    stats = {"ok": 0, "failed": []}
    latest_bars = {}
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as tiingo_client, httpx.AsyncClient() as r2_client:
        tasks = [daily_one(sem, tiingo_client, r2_client, sym, stats, latest_bars) for sym in US_SYMBOLS]
        await asyncio.gather(*tasks)
        await generate_screener_feed(r2_client, latest_bars)

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
