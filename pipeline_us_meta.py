"""
pipeline_us_meta.py
US Stocks Sector/Industry/Market Cap pipeline (NASDAQ Screener API) for tradewithtech.in

Free, no API key, no rate limit — pulls the full NASDAQ+NYSE listed-stock
screener in one call and uploads a compact symbol -> {sector, industry,
marketCap, name, country} map to R2 via the Worker.

This is a SEPARATE, low-frequency pipeline from pipeline_us.py (Tiingo OHLC).
Sector/industry classification barely changes day to day, so this is meant
to run weekly/monthly, not daily — no need to burn Tiingo's rate limit or
combine with the OHLC pipeline.

Uploads: us_company_meta.json  (via WORKER_URL, same pattern as pipeline_nse.py)

Env vars required (already exist as GitHub Actions secrets):
  WORKER_URL
  WORKER_TOKEN

Run:
  python pipeline_us_meta.py
"""

import os
import json
import asyncio
import logging
from datetime import datetime

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

RETRY = 5

# NASDAQ's public screener API — covers NASDAQ + NYSE + AMEX listed common stocks.
# No API key needed, but it does need browser-like headers or it 403s.
NASDAQ_SCREENER_URL = "https://api.nasdaq.com/api/screener/stocks"
NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json",
}


async def fetch_screener(client: httpx.AsyncClient, limit: int = 10000):
    params = {"tableonly": "true", "limit": str(limit), "download": "true"}
    r = await client.get(NASDAQ_SCREENER_URL, params=params, headers=NASDAQ_HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def clean_name(raw_name):
    """NASDAQ screener returns names like 'Apple Inc. Common Stock' or
    'Meta Platforms Inc. Class A Common Stock' — strip the redundant
    trailing 'Common Stock', but keep meaningful suffixes like
    'Class A' (dual-class shares) or 'Warrant'/'Rights' (different
    instrument, not the underlying common stock)."""
    if not raw_name:
        return raw_name
    name = raw_name.strip()
    if name.endswith(" Common Stock"):
        name = name[: -len(" Common Stock")].strip()
    return name


def parse_market_cap(raw):
    try:
        val = float(raw)
        return val if val > 0 else None
    except (TypeError, ValueError):
        return None


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


async def main():
    async with httpx.AsyncClient() as client:
        log.info("Fetching NASDAQ screener (sector/industry/market cap)...")
        raw = await fetch_screener(client)

        rows = (raw.get("data") or {}).get("rows") or []
        log.info(f"Got {len(rows)} rows from NASDAQ screener")

        meta = {}
        skipped = 0
        for row in rows:
            symbol = (row.get("symbol") or "").strip()
            if not symbol:
                skipped += 1
                continue
            meta[symbol] = {
                "name": clean_name(row.get("name")),
                "sector": row.get("sector") or None,
                "industry": row.get("industry") or None,
                "marketCap": parse_market_cap(row.get("marketCap")),
                "country": row.get("country") or None,
                "ipoYear": row.get("ipoyear") or None,
            }

        payload = {
            "updated": datetime.utcnow().isoformat() + "Z",
            "count": len(meta),
            "stocks": meta,
        }

        await r2_upload(client, "us_company_meta.json", payload)
        log.info(f"Uploaded us_company_meta.json with {len(meta)} symbols ({skipped} skipped, no symbol)")


if __name__ == "__main__":
    asyncio.run(main())
