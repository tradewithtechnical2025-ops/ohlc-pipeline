"""
pipeline_us_meta.py
1) Rebuilds us_universe.json (index constituents from Wikipedia — see us_universe.py)
2) Fetches company meta from Yahoo for every symbol -> us_company_meta.json on R2

us_company_meta.json:
  {
    "updated": "...Z",
    "count": N,
    "stocks": {
      "AAPL": {"name", "indices", "sector", "industry", "marketCap", "exchange", "exchangeName",
               "country", "website", "employees", "beta", "pe", "fwdPe", "divYield",
               "high52", "low52", "avgVolume", "sharesOut", "description", "startDate"},
      ...
    }
  }

Existing entries are MERGED field by field: a field Yahoo didn't return this time keeps its old value,
and fields this script doesn't know about are left untouched.

Env vars: WORKER_URL, WORKER_TOKEN
Run: python pipeline_us_meta.py
"""

import sys
import time
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx
import yfinance as yf

import pipeline_us as p
from pipeline_us import r2_upload, r2_download, now_iso, EXCHANGE_NAMES

log = logging.getLogger("meta")

META_FILE    = "us_company_meta.json"
WORKERS      = 4      # parallel Yahoo info requests
RETRY_PASSES = 2      # extra passes over symbols that failed
LOG_EVERY    = 100


def fetch_one(symbol: str) -> dict:
    """Return meta dict for one symbol, or {} on failure (never raises)."""
    try:
        i = yf.Ticker(symbol).info or {}
    except Exception:
        return {}
    name = i.get("longName") or i.get("shortName")
    if not name:
        return {}
    div = i.get("dividendYield")
    first = i.get("firstTradeDateEpochUtc")
    exch = i.get("exchange")
    return {
        "name":         name,
        "sector":       i.get("sector"),
        "industry":     i.get("industry"),
        "marketCap":    i.get("marketCap") or i.get("totalAssets"),   # ETFs report AUM as totalAssets
        "exchange":     exch,
        "exchangeName": EXCHANGE_NAMES.get(exch, exch),
        "country":      i.get("country"),
        "website":      i.get("website"),
        "employees":    i.get("fullTimeEmployees"),
        "beta":         i.get("beta"),
        "pe":           i.get("trailingPE"),
        "fwdPe":        i.get("forwardPE"),
        # yfinance has returned this both as a fraction (0.005) and as a percent (0.5); store percent
        "divYield":     None if div is None else (round(div * 100, 4) if div < 0.2 else round(div, 4)),
        "high52":       i.get("fiftyTwoWeekHigh"),
        "low52":        i.get("fiftyTwoWeekLow"),
        "avgVolume":    i.get("averageVolume"),
        "sharesOut":    i.get("sharesOutstanding"),
        "description":  i.get("longBusinessSummary"),
        "startDate":    datetime.fromtimestamp(first, timezone.utc).date().isoformat() if first else None,
    }


def fetch_all(symbols) -> dict:
    results, pending = {}, list(symbols)
    for attempt in range(1 + RETRY_PASSES):
        if attempt:
            log.info(f"  retry pass {attempt}: {len(pending)} symbols, waiting {20 * attempt}s")
            time.sleep(20 * attempt)
        with ThreadPoolExecutor(WORKERS) as ex:
            for n, (sym, m) in enumerate(zip(pending, ex.map(fetch_one, pending)), 1):
                if m:
                    results[sym] = m
                if n % LOG_EVERY == 0:
                    log.info(f"  info {n}/{len(pending)} (ok so far: {len(results)})")
        pending = [s for s in pending if s not in results]
        if not pending:
            break
    return results


async def main():
    async with httpx.AsyncClient() as client:
        await p.init_universe(client, rebuild=True)     # fresh index lists every run
        symbols = p.US_SYMBOLS

        # Read existing meta BEFORE the slow part; never overwrite a file we couldn't read
        try:
            existing = await r2_download(client, META_FILE) or {}
        except Exception as e:
            log.error(f"Couldn't read existing {META_FILE} ({e}); aborting to avoid clobbering it")
            sys.exit(1)

        log.info(f"Meta: fetching {len(symbols)} symbols from Yahoo ({WORKERS} parallel)")
        fresh = await asyncio.to_thread(fetch_all, symbols)
        failed = [s for s in symbols if s not in fresh]
        log.info(f"  Yahoo: got {len(fresh)}/{len(symbols)}")
        if not fresh:
            log.error("Nothing fetched; leaving existing meta file untouched")
            sys.exit(1)

        stocks = existing.get("stocks", {})
        for sym in symbols:
            merged = dict(stocks.get(sym, {}))
            merged.update({k: v for k, v in fresh.get(sym, {}).items() if v is not None})
            merged["indices"] = p.UNIVERSE[sym]["indices"]          # always current
            merged.setdefault("name", p.UNIVERSE[sym].get("name"))
            stocks[sym] = merged
        for sym in stocks:                      # dropped from every index: clear stale tags
            if sym not in p.UNIVERSE:
                stocks[sym]["indices"] = []
        existing.update({"updated": now_iso(), "count": len(stocks), "stocks": stocks})
        await r2_upload(client, META_FILE, existing)

    log.info(f"Meta done: {len(fresh)} updated, {len(failed)} failed, {len(stocks)} total in file")
    if failed:
        log.info("Failed symbols (kept previous values): " + ", ".join(failed))
    empty = [k for k, v in p.UNIVERSE_INDICES.items() if not v.get("count")]
    if empty:
        log.error(f"No constituents for: {', '.join(empty)} (Wikipedia + fallbacks failed) — check logs")
    if len(failed) > len(symbols) * 0.2 or empty:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
