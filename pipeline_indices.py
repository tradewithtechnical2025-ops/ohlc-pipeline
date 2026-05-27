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

        "NIFTY FMCG": "NIFTYFMCG",
        "NIFTY PHARMA": "NIFTYPHARMA",

        "NIFTY REALTY": "NIFTYREALTY",
        "NIFTY METAL": "NIFTYMETAL",
    }

    return REPLACE.get(
        v,
        v.replace(" ", "")
    )


BAD_KEYWORDS = [

    "2X",
    "1X",

    "INV",
    "LEV",

    "TRI",

    "EQW",
    "EQUAL",

    "LOWVOL",
    "ALPHA",
    "QUALITY",

    "MOM",
    "MOMENTUM",

    "ESG",

    "VOL",

    "MULT",
    "QUA",
    "VALUE",

    "SHODUR",
    "ENH",

    "30T",
    "50T",

    "LIQ",
    "VAR",

    "BETA",
]


BAD_TYPES = {

    "Strategy",
    "Strategy Indices",
    "Volatility",
}


def is_bad_index(symbol, index_name):

    symbol = str(symbol).upper()
    index_name = str(index_name).upper()

    return any(
        k in symbol or k in index_name
        for k in BAD_KEYWORDS
    )


# ─────────────────────────────────────────────
# R2 Upload
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

        raise RuntimeError(
            f"{filename} upload failed"
        )


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

    skipped = 0

    for row in rows:

        raw_symbol = str(
            row.get("index_symbol", "")
        ).strip()

        symbol = normalize_index_symbol(
            raw_symbol
        )

        if not symbol:
            skipped += 1
            continue

        index_name = str(
            row.get("index_name", "")
        )

        index_sub_type = str(
            row.get("index_sub_type", "")
        )

        constituents = row.get(
            "constituents"
        ) or []

        # Remove noisy indices

        if is_bad_index(
            symbol,
            index_name
        ):
            skipped += 1
            continue

        # Remove bad types

        if index_sub_type in BAD_TYPES:
            skipped += 1
            continue

        # Remove empty

        if not constituents:
            skipped += 1
            continue

        # Remove tiny/synthetic

        if len(constituents) < 5:
            skipped += 1
            continue

        output[symbol] = {

            "api_symbol": raw_symbol,

            "name": row.get(
                "index_name"
            ),

            "type": index_sub_type,

            "index_type": row.get(
                "index_type"
            ),

            "exchange": row.get(
                "exchange"
            ),

            "description": row.get(
                "description"
            ),

            "constituents": constituents,
        }

    print(
        f"✓ Clean indices: {len(output)}"
    )

    print(
        f"✓ Removed noisy indices: {skipped}"
    )

    return output


# ─────────────────────────────────────────────
# Daily Feed
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


def parse_index_daily(rows, valid_symbols):

    output = {}

    skipped = 0

    for row in rows:

        symbol = normalize_index_symbol(
            row.get("index_symbol")
        )

        if symbol not in valid_symbols:
            skipped += 1
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

            "pe": row.get("pe"),

            "pb": row.get("pb"),

            "div_yield": row.get(
                "div_yield"
            ),
        }

    print(
        f"✓ Daily feed indices: {len(output)}"
    )

    print(
        f"✓ Skipped noisy daily feed: {skipped}"
    )

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


def parse_index_returns(rows, valid_symbols):

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

    skipped = 0

    for row in rows:

        symbol = normalize_index_symbol(
            row.get("index_symbol")
        )

        if symbol not in valid_symbols:
            skipped += 1
            continue

        item = {}

        for p in PERIODS:

            item[p] = row.get(p)

        output[symbol] = item

    print(
        f"✓ Returns indices: {len(output)}"
    )

    print(
        f"✓ Skipped noisy returns: {skipped}"
    )

    return output


# ─────────────────────────────────────────────
# Historical
# ─────────────────────────────────────────────

async def fetch_index_history_one(
    client,
    api_symbol
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
        "index_symbol": api_symbol,
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

            return []

        data = r.json()

        return data.get(
            "rows"
        ) or []

    except:

        return []


def parse_index_history(rows):

    if not rows:
        return []

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

        print(
            "\n================================="
        )

        print(
            " INDEX PIPELINE STARTED"
        )

        print(
            "=================================\n"
        )

        # ─────────────────────────
        # Index Master
        # ─────────────────────────

        print(
            "=== INDEX MASTER ==="
        )

        master_rows = await fetch_index_master(
            client
        )

        if isinstance(master_rows, dict):

            master_rows = master_rows.get(
                "data",
                []
            )

        master_parsed = parse_index_master(
            master_rows
        )

        valid_symbols = set(
            master_parsed.keys()
        )

        await r2_upload(
            client,
            "index_master.json",
            master_parsed
        )

        print(
            "✅ index_master.json uploaded\n"
        )

        # ─────────────────────────
        # Daily Feed
        # ─────────────────────────

        print(
            "=== INDEX DAILY FEED ==="
        )

        daily_rows = await fetch_index_daily(
            client
        )

        daily_parsed = parse_index_daily(
            daily_rows,
            valid_symbols
        )

        await r2_upload(
            client,
            "index_daily.json",
            daily_parsed
        )

        print(
            "✅ index_daily.json uploaded\n"
        )

        # ─────────────────────────
        # Returns
        # ─────────────────────────

        print(
            "=== INDEX RETURNS ==="
        )

        returns_rows = await fetch_index_returns(
            client
        )

        returns_parsed = parse_index_returns(
            returns_rows,
            valid_symbols
        )

        await r2_upload(
            client,
            "index_returns.json",
            returns_parsed
        )

        print(
            "✅ index_returns.json uploaded\n"
        )

        # ─────────────────────────
        # Historical
        # ─────────────────────────

        print(
            "=== INDEX HISTORICAL ==="
        )

        symbols = sorted(
            master_parsed.items()
        )

        total = len(symbols)

        success = 0
        failed = 0

        for i, (symbol, meta) in enumerate(symbols, 1):

            rows = await fetch_index_history_one(
                client,
                meta["api_symbol"]
            )

            parsed = parse_index_history(
                rows
            )

            if not parsed:

                failed += 1

                print(
                    f"[{i}/{total}] "
                    f"✗ {symbol} | no data"
                )

                continue

            filename = (
                f"index_history/{symbol}.json"
            )

            await r2_upload(
                client,
                filename,
                parsed
            )

            success += 1

            print(
                f"[{i}/{total}] "
                f"✓ {symbol} "
                f"| {len(parsed)} candles"
            )

        print(
            "\n================================="
        )

        print(
            " INDEX PIPELINE COMPLETED"
        )

        print(
            "================================="
        )

        print(
            f"\n✅ Success: {success}"
        )

        print(
            f"❌ Failed : {failed}"
        )

        print(
            f"📦 Total  : {total}\n"
        )


if __name__ == "__main__":
    asyncio.run(main())
