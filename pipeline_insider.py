# pipeline_insider.py

import asyncio
import json
import os
from datetime import datetime, timezone

import feedparser
import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

UP_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json"
}

RSS_URL = "https://nsearchives.nseindia.com/content/RSS/InsiderTrading.xml"


async def r2_put(client, filename, data):
    body = json.dumps(data, ensure_ascii=False).encode()

    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers=UP_HEADERS,
        content=body,
        timeout=120
    )

    r.raise_for_status()
    print(f"✓ Uploaded {filename}")


async def run():
    print("Fetching RSS...")

    feed = feedparser.parse(RSS_URL)

    items = []

    for entry in feed.entries:
        items.append({
            "title": entry.get("title", ""),
            "link": entry.get("link", ""),
            "published": entry.get("published", ""),
            "description": entry.get("description", "")
        })

    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items
    }

    print(f"RSS Items: {len(items)}")

    async with httpx.AsyncClient() as client:
        await r2_put(
            client,
            "nse_insider_trading.json",
            payload
        )

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
