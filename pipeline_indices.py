#!/usr/bin/env python3

import asyncio
import json
import os
from datetime import datetime, timedelta

import httpx

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def normalize_index_symbol(v):

    v = str(v).upper().strip()

    REPLACE = {
        "NIF50": "NIFTY50",
        "NIFTY 50": "NIFTY50",
        "NIFTY BANK": "NIFTYBANK",
        "NIFTY IT": "NIFTYIT",
        "NIFTY AUTO": "NIFTYAUTO",
    }

    return REPLACE.get(v, v.replace(" ", ""))


# ─────────────────────────────────────────────
# R2 Helpers
# ─────────────────────────────────────────────

async def r2_upload(client, filename, data):

    url = f"{WORKER_URL}?file={filename}"

    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=300,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed")


async def r2_download(client, filename):

    url = f"{WORKER_URL}/{filename}"

    r = await client.get(
        url,
        headers=WORKER_HEADERS,
        timeout=300,
    )

    if r.status_code != 200:
        return {}

    try:
        return r.json()
    except:
        return {}


# ─────────────────────────────────────────────
# Index Master
# ─────────────────────────────────────────────

async def fetch_index_master(client):

    url = f"{FINEDGE_BASE}/index/master"

    params = {
        "token": FINEDGE_TOKEN,
    }

    r = await client.get(
        url,
        params=params,
        timeout=300,
    )

    r.raise_for_status()

    return r.json()


def parse_index_master(rows):

    output = {}

    for row in rows:

        symbol = normalize_index_symbol(
            row.get("index_symbol")
        )

        if not symbol:
            continue

        output[symbol] = {

            "name": row.get(
                "index_name"
            ),

            "type": row.get(
                "index_sub_type"
            ),

            "index_type": row.get(
                "index_type"
            ),

            "exchange": row.get(
                "exchange"
            ),

            "description": row.get(
                "description"
            ),

            "constituents": row.get(
                "constituents", []
            ),
        }

    return output


# ─────────────────────────────────────────────
# Index Daily Feed
# ─────────────────────────────────────────────

async def fetch_index_daily(client):

    url = f"{FINEDGE_BASE}/index/market-price/daily-feed"

    params = {
        "token": FINEDGE_TOKEN,
    }

    r = await client.get(
        url,
        params=params,
        timeout=300,
    )

    r.raise_for_status()

    return r.json()


def parse_index_daily(rows):

    output = {}

    for row in rows:

        symbol = normalize_index_symbol(
            row.get("index_symbol")
        )

        if not symbol:
            continue

        output[symbol] = {

            "name": row.get(
                "index_name"
            ),

            "close": row.get(
                "close_price"
            ),

            "open": row.get(
                "open_price"
            ),

            "high": row.get(
                "high_price"
            ),

            "low": row.get(
                "low_price"
            ),

            "change_pct": row.get(
                "change_pct"
            ),

            "points_change": row.get(
                "points_change"
            ),

            "volume": row.get(
                "volume"
            ),

            "turnover": row.get(
                "turnover"
            ),

            "market_cap": row.get(
                "market_cap"
            ),

            "pe": row.get(
                "pe"
            ),

            "pb": row.get(
                "pb"
            ),

            "div_yield": row.get(
                "div_yield"
            ),
        }

    return output


# ─────────────────────────────────────────────
# Index Returns
# ─────────────────────────────────────────────

async def fetch_index_returns(client):

    url = f"{FINEDGE_BASE}/index/price-returns"

    params = {
        "token": FINEDGE_TOKEN,
    }

    r = await client.get(
        url,
        params=params,
        timeout=300,
    )

    r.raise_for_status()

    return r.json()


def parse_index_returns(rows):

    output = {}

    PERIODS = [
        "1M",
        "3M",
        "6M",
        "1Y",
        "3Y",
        "5Y",
        "7Y",
        "10Y",
    ]

    for row in rows:

        symbol = normalize_index_symbol(
            row.get("index_symbol")
        )

        if not symbol:
            continue

        item = {}

        for p in PERIODS:

            item[p] = row.get(p)

        output[symbol] = item

    return output


# ─────────────────────────────────────────────
# Index Historical
# ─────────────────────────────────────────────

async def fetch_index_history_one(
    client,
    symbol
):

    today = datetime.now().date()

    from_date = (
        today - timedelta(days=365)
    ).strftime("%Y-%m-%d")

    to_date = today.strftime(
        "%Y-%m-%d"
    )

    url = f"{FINEDGE_BASE}/index/market-price/historical"

    params = {
        "index_symbol": symbol,
        "from_date": from_date,
        "to_date": to_date,
        "token": FINEDGE_TOKEN,
    }

    try:

        r = await client.get(
            url,
            params=params,
            timeout=300,
        )

        if r.status_code != 200:
            return symbol, []

        data = r.json()

        return symbol, data.get(
            "rows", []
        )

    except:
        return symbol, []


def parse_index_history(rows):

    parsed = []

    for r in rows:

        parsed.append({

            "date": r.get(
                "quote_date"
            ),

            "open": r.get(
                "open_price"
            ),

            "high": r.get(
                "high_price"
            ),

            "low": r.get(
                "low_price"
            ),

            "close": r.get(
                "close_price"
            ),

            "change_pct": r.get(
                "change_pct"
            ),

            "points_change": r.get(
                "points_change"
            ),

            "volume": r.get(
                "volume"
            ),

            "turnover": r.get(
                "turnover"
            ),
        })

    return parsed


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        # ─────────────────────────
        # Index Master
        # ─────────────────────────

        print(
            "\n=== Index Master ==="
        )

        master_rows = await fetch_index_master(
            client
        )

        master_parsed = parse_index_master(
            master_rows
        )

        await r2_upload(
            client,
            "index_master.json",
            master_parsed
        )

        print(
            "✅ index_master.json uploaded"
        )

        # ─────────────────────────
        # Index Daily Feed
        # ─────────────────────────

        print(
            "\n=== Index Daily Feed ==="
        )

        daily_rows = await fetch_index_daily(
            client
        )

        daily_parsed = parse_index_daily(
            daily_rows
        )

        await r2_upload(
            client,
            "index_daily.json",
            daily_parsed
        )

        print(
            "✅ index_daily.json uploaded"
        )

        # ─────────────────────────
        # Index Returns
        # ─────────────────────────

        print(
            "\n=== Index Returns ==="
        )

        returns_rows = await fetch_index_returns(
            client
        )

        returns_parsed = parse_index_returns(
            returns_rows
        )

        await r2_upload(
            client,
            "index_returns.json",
            returns_parsed
        )

        print(
            "✅ index_returns.json uploaded"
        )

        # ─────────────────────────
        # Historical OHLC
        # ─────────────────────────

        print(
            "\n=== Index Historical ==="
        )

        symbols = list(
            master_parsed.keys()
        )

        for i, symbol in enumerate(symbols, 1):

            sym, rows = await fetch_index_history_one(
                client,
                symbol
            )

            parsed = parse_index_history(
                rows
            )

            filename = (
                f"index_history/{sym}.json"
            )

            await r2_upload(
                client,
                filename,
                parsed
            )

            print(
                f"✓ {sym} "
                f"({i}/{len(symbols)})"
            )

        print(
            "\n✅ All Index History Uploaded"
        )


if __name__ == "__main__":
    asyncio.run(main())
