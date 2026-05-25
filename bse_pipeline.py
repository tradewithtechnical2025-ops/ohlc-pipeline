#!/usr/bin/env python3

import asyncio
import gzip
import io
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

log = logging.getLogger(__name__)

# =========================================================
# CONFIG
# =========================================================

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]

WORKER_URL = os.environ["WORKER_URL"].rstrip("/")

WORKER_TOKEN = os.environ["WORKER_TOKEN"]

BASE_URL = "https://api.upstox.com/v2/historical-candle"

R2_CHUNKS = 8

ROLLING_DAYS = 548

CONCURRENCY = 5

RETRY = 3

RATE_DELAY = 0.5

HERE = Path(__file__).parent

# =========================================================
# HEADERS
# =========================================================

UPSTOX_HEADERS = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept": "application/json",
}

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN
}

# =========================================================
# DATE UTILS
# =========================================================

def today_ist():

    return datetime.now(
        ZoneInfo("Asia/Kolkata")
    ).strftime("%Y-%m-%d")


def prev_trading_day(d):

    dt = date.fromisoformat(d)

    while True:

        dt -= timedelta(days=1)

        if dt.weekday() < 5:
            return dt.isoformat()


def rolling_cutoff(anchor):

    return (
        date.fromisoformat(anchor)
        - timedelta(days=ROLLING_DAYS)
    ).isoformat()

# =========================================================
# DOWNLOAD BSE-ONLY STOCK MASTER
# =========================================================

async def load_bse_isin_map():

    url = (
        "https://assets.upstox.com/"
        "market-quote/instruments/"
        "exchange/complete.json.gz"
    )

    async with httpx.AsyncClient(
        timeout=120
    ) as client:

        r = await client.get(url)

        r.raise_for_status()

        compressed = io.BytesIO(r.content)

        with gzip.GzipFile(
            fileobj=compressed
        ) as gz:

            data = json.loads(
                gz.read().decode()
            )

    # =====================================
    # NSE ISIN SET
    # =====================================

    NSE_ISINS = set()

    for row in data:

        if row.get("segment") != "NSE_EQ":
            continue

        isin = row.get("isin")

        if isin:
            NSE_ISINS.add(isin)

    log.info(
        f"NSE ISINs: {len(NSE_ISINS)}"
    )

    # =====================================
    # BSE ONLY
    # =====================================

    BSE_EQUITY = {
        "A",
        "B",
        "T",
        "XT",
        "X",
        "Z",
        "ZP",
        "E",
        "R",
        "P",
        "G"
    }

    bad_words = [
        "ETF",
        "MF",
        "FUND",
        "GOLD",
        "LIQUID",
        "GSEC",
        "GS",
        "BOND",
        "SDL",
        "NCD",
    ]

    mapping = {}

    for row in data:

        segment = str(
            row.get("segment", "")
        ).upper()

        instrument_type = str(
            row.get(
                "instrument_type",
                ""
            )
        ).upper()

        trading_symbol = str(
            row.get(
                "trading_symbol",
                ""
            )
        ).upper()

        short_name = str(
            row.get(
                "short_name",
                ""
            )
        ).upper()

        isin = row.get("isin")

        # =====================================
        # ONLY BSE CASH MARKET
        # =====================================

        if segment != "BSE_EQ":
            continue

        if instrument_type not in BSE_EQUITY:
            continue

        if not isin:
            continue

        # =====================================
        # ONLY BSE-ONLY STOCKS
        # =====================================

        if isin in NSE_ISINS:
            continue

        # =====================================
        # REMOVE ETF / MF / BONDS
        # =====================================

        name_check = (
            trading_symbol
            + " "
            + short_name
        )

        if any(
            x in name_check
            for x in bad_words
        ):
            continue

        mapping[trading_symbol] = isin

    log.info(
        f"Loaded "
        f"{len(mapping)} "
        f"BSE-only stocks"
    )

    return mapping

# =========================================================
# LIQUIDITY FILTER
# =========================================================

def passes_bse_liquidity_filter(
    candles,
    min_turnover=3_00_00_000,
    min_price=20,
    min_avg_volume=5000,
):

    if len(candles) < 20:
        return False

    recent = candles[-20:]

    avg_close = sum(
        c["c"] for c in recent
    ) / 20

    avg_vol = sum(
        c["v"] for c in recent
    ) / 20

    turnover = avg_close * avg_vol

    if avg_close < min_price:
        return False

    if avg_vol < min_avg_volume:
        return False

    if turnover < min_turnover:
        return False

    return True

# =========================================================
# FETCH OHLC
# =========================================================

async def fetch_bse_ohlc(
    client,
    sem,
    sym,
    isin,
    from_date,
    to_date,
):

    key = quote(
        f"BSE_EQ|{isin}",
        safe=""
    )

    url = (
        f"{BASE_URL}/{key}/day/"
        f"{to_date}/{from_date}"
    )

    for attempt in range(RETRY):

        async with sem:

            await asyncio.sleep(
                RATE_DELAY
            )

            try:

                r = await client.get(
                    url,
                    headers=UPSTOX_HEADERS,
                    timeout=20
                )

            except httpx.RequestError:

                await asyncio.sleep(
                    2 ** attempt
                )

                continue

        if r.status_code == 401:

            log.error(
                "TOKEN EXPIRED"
            )

            sys.exit(1)

        if r.status_code == 429:

            log.warning(
                f"{sym}: 429"
            )

            await asyncio.sleep(10)

            continue

        if r.status_code != 200:

            return sym, None

        payload = r.json()

        raw = payload.get(
            "data",
            {}
        ).get("candles", [])

        if not raw:

            return sym, None

        candles = sorted(
            [{
                "d": c[0][:10],
                "o": c[1],
                "h": c[2],
                "l": c[3],
                "c": c[4],
                "v": c[5],
                "oi": (
                    c[6]
                    if len(c) > 6
                    else 0
                )
            } for c in raw],
            key=lambda x: x["d"]
        )

        return sym, candles

    return sym, None

# =========================================================
# BUILD STOCK OBJECT
# =========================================================

def build_stock_obj(candles):

    return {
        k: [c[k] for c in candles]
        for k in (
            "d",
            "o",
            "h",
            "l",
            "c",
            "v",
            "oi"
        )
    }

# =========================================================
# APPLY ROLLING WINDOW
# =========================================================

def apply_rolling_window(
    all_data,
    cutoff
):

    for s in all_data.values():

        keep = [
            i for i, d in enumerate(
                s["d"]
            )
            if d >= cutoff
        ]

        for k in s:

            s[k] = [
                s[k][i]
                for i in keep
            ]

# =========================================================
# R2 UPLOAD
# =========================================================

async def r2_upload(
    client,
    filename,
    data
):

    if isinstance(data, str):

        data = data.encode()

    url = (
        f"{WORKER_URL}"
        f"?file={filename}"
    )

    r = await client.post(
        url,
        headers={
            **WORKER_HEADERS,
            "Content-Type":
            "application/json"
        },
        content=data,
        timeout=90
    )

    if r.status_code != 200:

        raise RuntimeError(
            f"Upload failed "
            f"{filename}"
        )

    log.info(
        f"Uploaded {filename}"
    )

# =========================================================
# UPLOAD CHUNKS
# =========================================================

async def upload_bse_chunks(
    client,
    all_data,
    today
):

    symbols = sorted(
        all_data.keys()
    )

    n = len(symbols)

    size = (
        n + R2_CHUNKS - 1
    ) // R2_CHUNKS

    tasks = []

    for i in range(R2_CHUNKS):

        chunk_syms = symbols[
            i * size:(i + 1) * size
        ]

        chunk = {
            s: all_data[s]
            for s in chunk_syms
        }

        payload = json.dumps({
            "updated": today,
            "chunk": i + 1,
            "total": R2_CHUNKS,
            "stocks": chunk
        })

        tasks.append(
            r2_upload(
                client,
                f"bse_ohlc_{i+1}.json",
                payload
            )
        )

    await asyncio.gather(*tasks)

# =========================================================
# MAIN PIPELINE
# =========================================================

async def run_bse_daily():

    today = today_ist()

    prev = prev_trading_day(today)

    cutoff = rolling_cutoff(today)

    log.info(
        f"BSE Daily "
        f"{prev} → {today}"
    )

    sem = asyncio.Semaphore(
        CONCURRENCY
    )

    BSE_ISIN_MAP = (
        await load_bse_isin_map()
    )

    symbols = list(
        BSE_ISIN_MAP.keys()
    )

    async with httpx.AsyncClient() as client:

        tasks = [
            fetch_bse_ohlc(
                client,
                sem,
                sym,
                BSE_ISIN_MAP[sym],
                prev,
                today
            )
            for sym in symbols
        ]

        results = await asyncio.gather(
            *tasks
        )

        fetched = {}

        for sym, candles in results:

            if not candles:
                continue

            if not passes_bse_liquidity_filter(
                candles
            ):
                continue

            fetched[sym] = candles

        log.info(
            f"Liquid stocks: "
            f"{len(fetched)}"
        )

        all_data = {}

        for sym, candles in fetched.items():

            all_data[sym] = (
                build_stock_obj(
                    candles
                )
            )

        apply_rolling_window(
            all_data,
            cutoff
        )

        delta = {}

        for sym, candles in fetched.items():

            today_c = next(
                (
                    c for c in candles
                    if c["d"] == today
                ),
                None
            )

            if today_c:
                delta[sym] = today_c

        await asyncio.gather(

            upload_bse_chunks(
                client,
                all_data,
                today
            ),

            r2_upload(
                client,
                "bse_ohlc_delta.json",
                json.dumps({
                    "date": today,
                    "stocks": delta
                })
            )
        )

    log.info(
        "BSE Pipeline Complete"
    )

# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":

    asyncio.run(run_bse_daily())
