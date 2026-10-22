"""
pipeline_us_meta.py
US company meta (sector / industry / market cap) from Yahoo Finance -> us_company_meta.json on R2

Replaces the Tiingo-based meta script. Output shape (read by the screener feed in pipeline_us.py):
  {
    "updated": "...Z",
    "count": N,
    "stocks": {
      "AAPL": {"name", "sector", "industry", "marketCap", "exchange", "country",
               "website", "employees", "beta", "pe", "fwdPe", "divYield",
               "high52", "low52", "avgVolume", "sharesOut"},
      ...
    }
  }

Existing entries are MERGED field by field: a field Yahoo didn't return this time keeps its old value,
and fields this script doesn't know about are left untouched.

Symbols and Worker helpers come from pipeline_us.py so there's one list to maintain.

Env vars: WORKER_URL, WORKER_TOKEN
Run: python pipeline_us_meta.py
"""

import sys
import time
import asyncio
import logging

import httpx
import yfinance as yf

from pipeline_us import US_SYMBOLS, r2_upload, r2_download, now_iso

log = logging.getLogger("meta")

META_FILE = "us_company_meta.json"
DELAY_SEC = 0.5     # between symbols, keeps Yahoo happy
RETRY_PASSES = 2    # extra passes over symbols that failed


def fetch_one(symbol: str) -> dict:
    """Return meta dict for one symbol, or {} on failure (never raises)."""
    try:
        i = yf.Ticker(symbol).info or {}
    except Exception as e:
        log.warning(f"  {symbol}: {e}")
        return {}
    name = i.get("longName") or i.get("shortName")
    if not name:
        return {}
    div = i.get("dividendYield")
    return {
        "name":      name,
        "sector":    i.get("sector"),
        "industry":  i.get("industry"),
        "marketCap": i.get("marketCap"),
        "exchange":  i.get("exchange"),
        "country":   i.get("country"),
        "website":   i.get("website"),
        "employees": i.get("fullTimeEmployees"),
        "beta":      i.get("beta"),
        "pe":        i.get("trailingPE"),
        "fwdPe":     i.get("forwardPE"),
        # yfinance has returned this both as a fraction (0.005) and as a percent (0.5); store percent
        "divYield":  None if div is None else (round(div * 100, 4) if div < 0.2 else round(div, 4)),
        "high52":    i.get("fiftyTwoWeekHigh"),
        "low52":     i.get("fiftyTwoWeekLow"),
        "avgVolume": i.get("averageVolume"),
        "sharesOut": i.get("sharesOutstanding"),
    }


def fetch_all(symbols) -> dict:
    results, pending = {}, list(symbols)
    for attempt in range(1 + RETRY_PASSES):
        if attempt:
            log.info(f"  retry pass {attempt}: {len(pending)} symbols, waiting {15 * attempt}s")
            time.sleep(15 * attempt)
        failed = []
        for sym in pending:
            m = fetch_one(sym)
            if m:
                results[sym] = m
            else:
                failed.append(sym)
            time.sleep(DELAY_SEC)
        pending = failed
        if not pending:
            break
    return results


async def main():
    log.info(f"Meta: {len(US_SYMBOLS)} symbols from Yahoo")
    fresh = await asyncio.to_thread(fetch_all, US_SYMBOLS)
    failed = [s for s in US_SYMBOLS if s not in fresh]
    log.info(f"  Yahoo: got {len(fresh)}/{len(US_SYMBOLS)}")

    if not fresh:
        log.error("Nothing fetched; leaving existing meta file untouched")
        sys.exit(1)

    async with httpx.AsyncClient() as client:
        existing = await client_safe_download(client)
        stocks = existing.get("stocks", {})
        for sym, m in fresh.items():
            merged = dict(stocks.get(sym, {}))
            merged.update({k: v for k, v in m.items() if v is not None})
            stocks[sym] = merged
        existing.update({"updated": now_iso(), "count": len(stocks), "stocks": stocks})
        await r2_upload(client, META_FILE, existing)

    log.info(f"Meta done: {len(fresh)} updated, {len(failed)} failed, {len(stocks)} total in file")
    if failed:
        log.info("Failed symbols (kept previous values): " + ", ".join(failed))
    if len(failed) > len(US_SYMBOLS) // 2:
        sys.exit(1)


async def client_safe_download(client) -> dict:
    try:
        return await r2_download(client, META_FILE) or {}
    except Exception as e:
        # Don't overwrite a file we couldn't read — we'd lose entries for symbols not in US_SYMBOLS
        log.error(f"Couldn't read existing {META_FILE} ({e}); aborting to avoid clobbering it")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
