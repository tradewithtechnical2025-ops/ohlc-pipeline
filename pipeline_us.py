"""
pipeline_us.py
US Stocks OHLC pipeline (Yahoo Finance via yfinance) for tradewithtech.in

Drop-in replacement for the Tiingo version. Same R2 files, same JSON shape:
  us_ohlc_<SYMBOL>.json   -> {symbol, updated, company{...}, bars[{date,o,h,l,c,v}]}
  us_screener_feed.json   -> {updated, count, stocks[...]}
  us_company_meta.json    -> {stocks{SYM:{name,sector,industry,marketCap}}}  (merged, backfill/meta only)

Changes vs Tiingo version:
  - No API key, no 50 req/hour limit: all symbols come in ONE batched download.
  - Prices are split/dividend ADJUSTED, so charts don't show fake crashes on splits (e.g. NVDA 10:1).
  - Daily mode pulls the last 10 days and merges, so a missed run / holiday never leaves a permanent gap.
  - Failed symbols in daily mode keep their previous screener row instead of disappearing.

Env vars (GitHub Actions secrets): WORKER_URL, WORKER_TOKEN

Run modes:
  python pipeline_us.py --mode backfill   # 3y history + company meta, overwrites per-symbol files
  python pipeline_us.py --mode daily      # last 10 days merged into existing files
  python pipeline_us.py --mode meta       # only refresh us_company_meta.json
"""

import os
import sys
import json
import asyncio
import argparse
import logging
from datetime import datetime, timezone

import httpx
import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

WORKER_URL     = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN   = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

BACKFILL_PERIOD = "3y"
DAILY_PERIOD    = "10d"   # overlap window: heals gaps from missed runs
RETRY           = 5
UPLOAD_CONCURRENCY = 8    # Worker uploads in parallel (Yahoo is one batched call anyway)

# Yahoo format for class shares: BRK-B, BF-B (not BRKB / BRK.B)
US_SYMBOLS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "AVGO", "JPM",
    "LLY", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ", "NFLX",
    "BAC", "ABBV", "CRM", "WMT", "KO", "AMD", "PEP", "MRK", "ADBE", "TMO",
    "CSCO", "ORCL", "ACN", "MCD", "LIN", "ABT", "DHR", "WFC", "TXN", "CAT",
    "PM", "INTU", "IBM", "GE", "QCOM", "AMGN", "NOW", "SPGI", "UBER", "BA",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def r2_filename(symbol: str) -> str:
    return f"us_ohlc_{symbol.replace('.', '_')}.json"


# ---------------------------------------------------------------------------
# Worker upload / download (unchanged pattern from pipeline_nse.py)
# ---------------------------------------------------------------------------

async def r2_upload(client: httpx.AsyncClient, filename: str, data):
    if not isinstance(data, (bytes, str)):
        data = json.dumps(data, separators=(",", ":"))
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
# Yahoo
# ---------------------------------------------------------------------------

def _r(x, nd=4):
    return None if pd.isna(x) else round(float(x), nd)


def yahoo_bars(symbols, period) -> dict:
    """One batched download -> {symbol: [bar, ...]} (adjusted OHLC). Missing symbols are absent."""
    for attempt in range(3):
        data = yf.download(symbols, period=period, interval="1d", group_by="ticker",
                           auto_adjust=True, actions=False, threads=True, progress=False)
        if not data.empty:
            break
        log.warning(f"  Yahoo returned nothing, retry {attempt + 1}")
        import time; time.sleep(10 * (attempt + 1))
    out = {}
    for sym in symbols:
        try:
            df = data[sym] if isinstance(data.columns, pd.MultiIndex) else data
        except KeyError:
            continue
        df = df.dropna(subset=["Close"])
        if df.empty:
            continue
        out[sym] = [
            {"date": idx.strftime("%Y-%m-%d"), "o": _r(row.Open), "h": _r(row.High),
             "l": _r(row.Low), "c": _r(row.Close), "v": None if pd.isna(row.Volume) else int(row.Volume)}
            for idx, row in df.iterrows()
        ]
    return out


def yahoo_info(symbol) -> dict:
    """Company info; slow (one request per symbol) and occasionally flaky, so never fatal."""
    try:
        i = yf.Ticker(symbol).info or {}
    except Exception as e:
        log.warning(f"  {symbol} info: {e}")
        return {}
    return {
        "name": i.get("longName") or i.get("shortName"),
        "exchangeCode": i.get("exchange"),
        "description": i.get("longBusinessSummary"),
        "startDate": (datetime.fromtimestamp(i["firstTradeDateEpochUtc"], timezone.utc).date().isoformat()
                      if i.get("firstTradeDateEpochUtc") else None),
        "sector": i.get("sector"),
        "industry": i.get("industry"),
        "marketCap": i.get("marketCap"),
    }


def merge_bars(old, new):
    by_date = {b["date"]: b for b in old}
    by_date.update({b["date"]: b for b in new})   # newer data wins (also fixes revised bars)
    return [by_date[d] for d in sorted(by_date)]


# ---------------------------------------------------------------------------
# Meta + screener
# ---------------------------------------------------------------------------

async def refresh_meta(r2_client, symbols) -> dict:
    """Fetch info for each symbol and MERGE into us_company_meta.json (keeps entries for other symbols)."""
    existing = await r2_download(r2_client, "us_company_meta.json") or {}
    stocks = existing.get("stocks", {})
    infos = {}
    for sym in symbols:
        info = await asyncio.to_thread(yahoo_info, sym)
        if info.get("name"):
            infos[sym] = info
            stocks[sym] = {k: info[k] for k in ("name", "sector", "industry", "marketCap")}
    existing.update({"updated": now_iso(), "stocks": stocks})
    await r2_upload(r2_client, "us_company_meta.json", existing)
    log.info(f"  meta: {len(infos)}/{len(symbols)} symbols refreshed")
    return infos


async def generate_screener_feed(r2_client, latest_bars):
    try:
        meta = await r2_download(r2_client, "us_company_meta.json")
    except Exception as e:
        log.warning(f"  screener feed: couldn't load meta ({e}), sector/industry/mcap blank")
        meta = None
    meta_stocks = (meta or {}).get("stocks", {})

    # Keep previous rows for symbols that failed this run, so they don't vanish from the screener.
    try:
        prev_feed = await r2_download(r2_client, "us_screener_feed.json") or {}
    except Exception:
        prev_feed = {}
    prev_rows = {r["symbol"]: r for r in prev_feed.get("stocks", [])}

    rows = []
    for symbol in US_SYMBOLS:
        info = latest_bars.get(symbol)
        if not info:
            if symbol in prev_rows:
                rows.append(prev_rows[symbol])
            continue
        last, prev = info["last"], info["prev"]
        change_pct = ((last["c"] - prev["c"]) / prev["c"]) * 100 if prev and prev.get("c") else None
        m = meta_stocks.get(symbol, {})
        rows.append({
            "symbol": symbol,
            "name": m.get("name") or info.get("name") or symbol,
            "sector": m.get("sector"),
            "industry": m.get("industry"),
            "mcap": m.get("marketCap"),
            "ltp": last["c"],
            "pct_ch": None if change_pct is None else round(change_pct, 4),
            "volume": last.get("v"),
            "date": last["date"],
        })

    if not rows:
        log.warning("  screener feed: nothing to write, skipping")
        return
    await r2_upload(r2_client, "us_screener_feed.json", {"updated": now_iso(), "count": len(rows), "stocks": rows})
    log.info(f"  screener feed: uploaded ({len(rows)} symbols)")


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

async def _gather_limited(coros, limit):
    sem = asyncio.Semaphore(limit)
    async def run(c):
        async with sem:
            return await c
    return await asyncio.gather(*(run(c) for c in coros))


async def run_backfill():
    log.info(f"Backfill: {BACKFILL_PERIOD} for {len(US_SYMBOLS)} symbols (one Yahoo call)")
    all_bars = await asyncio.to_thread(yahoo_bars, US_SYMBOLS, BACKFILL_PERIOD)
    log.info(f"  Yahoo: got {len(all_bars)}/{len(US_SYMBOLS)} symbols")
    stats = {"ok": 0, "failed": [s for s in US_SYMBOLS if s not in all_bars]}
    latest_bars = {}

    async with httpx.AsyncClient() as r2_client:
        infos = await refresh_meta(r2_client, list(all_bars))

        async def upload(sym):
            bars = all_bars[sym]
            info = infos.get(sym, {})
            company = {k: info.get(k) for k in ("name", "exchangeCode", "description", "startDate")} if info else {}
            try:
                await r2_upload(r2_client, r2_filename(sym),
                                {"symbol": sym, "updated": now_iso(), "company": company, "bars": bars})
                log.info(f"  {sym}: {len(bars)} bars uploaded (meta: {'ok' if info else 'missing'})")
                stats["ok"] += 1
                latest_bars[sym] = {"name": company.get("name"), "last": bars[-1],
                                    "prev": bars[-2] if len(bars) > 1 else None}
            except Exception as e:
                log.error(f"  {sym}: {e}")
                stats["failed"].append(sym)

        await _gather_limited([upload(s) for s in all_bars], UPLOAD_CONCURRENCY)
        await generate_screener_feed(r2_client, latest_bars)
    return stats


async def run_daily():
    log.info(f"Daily update: last {DAILY_PERIOD} for {len(US_SYMBOLS)} symbols (one Yahoo call)")
    all_bars = await asyncio.to_thread(yahoo_bars, US_SYMBOLS, DAILY_PERIOD)
    log.info(f"  Yahoo: got {len(all_bars)}/{len(US_SYMBOLS)} symbols")
    stats = {"ok": 0, "failed": [s for s in US_SYMBOLS if s not in all_bars]}
    latest_bars = {}

    async with httpx.AsyncClient() as r2_client:
        async def update(sym):
            try:
                filename = r2_filename(sym)
                existing = await r2_download(r2_client, filename)
                if existing is None:
                    log.warning(f"  {sym}: no existing file, run backfill first. Skipping.")
                    stats["failed"].append(sym)
                    return
                bars = merge_bars(existing.get("bars", []), all_bars[sym])
                added = len(bars) - len(existing.get("bars", []))
                existing["bars"] = bars
                existing["updated"] = now_iso()
                await r2_upload(r2_client, filename, existing)
                log.info(f"  {sym}: last {bars[-1]['date']} (+{added} new)")
                stats["ok"] += 1
                latest_bars[sym] = {"name": (existing.get("company") or {}).get("name"),
                                    "last": bars[-1], "prev": bars[-2] if len(bars) > 1 else None}
            except Exception as e:
                log.error(f"  {sym}: {e}")
                stats["failed"].append(sym)

        await _gather_limited([update(s) for s in all_bars], UPLOAD_CONCURRENCY)
        await generate_screener_feed(r2_client, latest_bars)
    return stats


async def run_meta():
    async with httpx.AsyncClient() as r2_client:
        await refresh_meta(r2_client, US_SYMBOLS)
    return {"ok": len(US_SYMBOLS), "failed": []}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "daily", "meta"], required=True)
    args = parser.parse_args()

    stats = asyncio.run({"backfill": run_backfill, "daily": run_daily, "meta": run_meta}[args.mode]())
    log.info(f"{args.mode} done: {stats['ok']} ok, {len(stats['failed'])} failed")
    if stats["failed"]:
        log.info("Failed symbols: " + ", ".join(stats["failed"]))
    # Fail the GitHub Action (so you get an email) if most symbols failed
    if len(stats["failed"]) > len(US_SYMBOLS) // 2:
        sys.exit(1)


if __name__ == "__main__":
    main()
