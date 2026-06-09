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
RSS_URL = "https://archives.nseindia.com/content/RSS/InsiderTrading.xml"

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
    async with httpx.AsyncClient() as fetch_client:
        rss_resp = await fetch_client.get(
            RSS_URL,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"},
            timeout=30,
            follow_redirects=True
        )
        rss_resp.raise_for_status()
    feed = feedparser.parse(rss_resp.content)

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
