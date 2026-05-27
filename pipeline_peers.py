#!/usr/bin/env python3

import asyncio
import json
import os

import httpx

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

CONCURRENCY = 5
RATE_DELAY  = 0.20
RETRY       = 3

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# R2 Helpers
# ─────────────────────────────────────────────

async def r2_download(client, filename):

    url = f"{WORKER_URL}/{filename}"

    r = await client.get(
        url,
        headers=WORKER_HEADERS,
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} download failed")

    return r.json()


async def r2_upload(client, filename, data):

    url = f"{WORKER_URL}?file={filename}"

    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed")


# ─────────────────────────────────────────────
# Finedge Helper
# ─────────────────────────────────────────────

async def finedge_get(client, sem, path, params):

    params["token"] = FINEDGE_TOKEN

    url = f"{FINEDGE_BASE}/{path}"

    async with sem:

        for attempt in range(RETRY):

            await asyncio.sleep(RATE_DELAY)

            try:

                r = await client.get(
                    url,
                    params=params,
                    timeout=30,
                )

            except Exception as e:

                print(f"Network Error: {e}")

                await asyncio.sleep(2 ** attempt)

                continue

            if r.status_code == 429:

                print("429 Rate Limit")

                await asyncio.sleep(15)

                continue

            if r.status_code != 200:

                return None

            try:

                return r.json()

            except Exception:

                return None

    return None


# ─────────────────────────────────────────────
# Parse Peers
# ─────────────────────────────────────────────

def parse_peers(symbol, data):

    peers_raw = data.get("peers") or []

    peers = []

    for sym in peers_raw:

        sym = str(sym).strip().upper()

        if not sym:
            continue

        if sym == symbol:
            continue

        peers.append(sym)

    peers = list(dict.fromkeys(peers))[:10]

    return {
        "group": "sub_industry",
        "peers": peers
    }


# ─────────────────────────────────────────────
# Fetch One
# ─────────────────────────────────────────────

async def fetch_one(client, sem, symbol):

    groups = [
        "sub_industry",
        "industry",
        "sector"
    ]

    for grp in groups:

        data = await finedge_get(
            client,
            sem,
            f"peers/{symbol}",
            {
                "group": grp
            }
        )

        if (
            not data
            or not isinstance(data, dict)
        ):
            continue

        peers = data.get("peers") or []

        if not peers:
            continue

        try:

            parsed = parse_peers(symbol, data)

            parsed["group"] = grp

            return symbol, parsed

        except Exception as e:

            print(f"Parse Error {symbol}: {e}")

            return symbol, {
                "group": grp,
                "peers": []
            }

    # Final fallback
    return symbol, {
        "group": "none",
        "peers": []
    }


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        master = await r2_download(
            client,
            "master.json"
        )

        symbols = [
            x["symbol"]
            for x in master
            if x.get("exchange") == "NSE"
        ]

        # Remove ETF / Index Symbols
        BAD_KEYWORDS = [
            "ETF",
            "LIQUID",
            "NIFTY",
            "GOLD",
            "SILVER",
            "NEXT50",
            "MIDCAP",
            "SMALLCAP",
        ]

        symbols = [
            s for s in symbols
            if not any(k in s for k in BAD_KEYWORDS)
        ]

        sem = asyncio.Semaphore(CONCURRENCY)

        output = {}

        total = len(symbols)

        for i in range(0, total, 25):

            batch = symbols[i:i+25]

            tasks = [
                fetch_one(client, sem, s)
                for s in batch
            ]

            results = await asyncio.gather(*tasks)

            for sym, data in results:

                if data:

                    output[sym] = data

                    print(f"✓ {sym}")

                else:

                    print(f"✗ {sym} | no peer data")

            print(f"{min(i+25, total)}/{total}")

        await r2_upload(
            client,
            "peers.json",
            output
        )

        print("✅ peers.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
