import asyncio
import json
import os
from collections import defaultdict

import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

async def r2_download(client, filename):

    url = f"{WORKER_URL}/{filename}"

    r = await client.get(url, headers=WORKER_HEADERS)

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
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        master = await r2_download(client, "master.json")

        industry_map = defaultdict(list)

        for stock in master:

            sym = stock.get("symbol")

            industry = (
                stock.get("industry")
                or stock.get("sector")
                or "Unknown"
            )

            industry_map[industry].append(sym)

        peers = {}

        for stock in master:

            sym = stock.get("symbol")

            industry = (
                stock.get("industry")
                or stock.get("sector")
                or "Unknown"
            )

            peer_list = [
                x for x in industry_map[industry]
                if x != sym
            ][:10]

            peers[sym] = {
                "sector"   : stock.get("sector", ""),
                "industry" : industry,
                "peers"    : peer_list,
            }

        await r2_upload(
            client,
            "peers.json",
            peers
        )

        print("✅ peers.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
