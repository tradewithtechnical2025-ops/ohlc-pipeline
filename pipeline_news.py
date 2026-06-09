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

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

# Feed definitions: (source_key, label, rss_url)
FEEDS = [
    # NSE Official
    ("nse_results",       "NSE Financial Results",  "https://nsearchives.nseindia.com/content/RSS/Financial_Results.xml"),
    ("nse_announcements", "NSE Announcements",       "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml"),
    ("nse_board",         "NSE Board Meetings",      "https://nsearchives.nseindia.com/content/RSS/Board_Meetings.xml"),
    ("nse_corp_actions",  "NSE Corporate Actions",   "https://nsearchives.nseindia.com/content/RSS/Corporate_action.xml"),
    # Market News
    ("et_markets",         "Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("mint_markets",      "LiveMint Markets",        "https://www.livemint.com/rss/markets"),
   
]

# R2 output files
OUTPUT_MAP = {
    "nse_results":       "nse_results_feed.json",
    "nse_announcements": "nse_announcements.json",
    "nse_board":         "nse_board_meetings.json",
    "nse_corp_actions":  "nse_corp_actions.json",
    "market_news":       "market_news.json",   # merged: mc + mint + cnbc
}


async def fetch_feed(client: httpx.AsyncClient, source_key: str, label: str, url: str) -> tuple[str, list[dict]]:
    try:
        r = await client.get(url, headers=BROWSER_HEADERS, timeout=20, follow_redirects=True)
        r.raise_for_status()
        feed = feedparser.parse(r.content)
        items = []
        for entry in feed.entries:
            items.append({
                "source":      label,
                "source_key":  source_key,
                "title":       entry.get("title", "").strip(),
                "link":        entry.get("link", ""),
                "published":   entry.get("published", ""),
                "summary":     entry.get("summary", entry.get("description", "")).strip()[:300],
            })
        print(f"  ✓ {label}: {len(items)} items")
        return source_key, items
    except Exception as e:
        print(f"  ✗ {label}: {e}")
        return source_key, []


async def r2_put(client: httpx.AsyncClient, filename: str, data: dict):
    body = json.dumps(data, ensure_ascii=False).encode()
    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers=UP_HEADERS,
        content=body,
        timeout=120
    )
    r.raise_for_status()
    print(f"✓ Uploaded {filename}")


def make_payload(items: list[dict]) -> dict:
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items
    }


async def run():
    now = datetime.now(timezone.utc).isoformat()
    print(f"Fetching all feeds... [{now}]")

    async with httpx.AsyncClient() as client:
        # Fetch all feeds concurrently
        tasks = [fetch_feed(client, sk, label, url) for sk, label, url in FEEDS]
        results = await asyncio.gather(*tasks)
        result_map = dict(results)

        uploads = []

        # NSE individual files
        for source_key, filename in [
            ("nse_results",       "nse_results_feed.json"),
            ("nse_announcements", "nse_announcements.json"),
            ("nse_board",         "nse_board_meetings.json"),
            ("nse_corp_actions",  "nse_corp_actions.json"),
        ]:
            items = result_map.get(source_key, [])
            uploads.append((filename, make_payload(items)))

        # Market news — merge mc + mint + cnbc, sort by source
        news_items = []
        for sk in ["mc_news", "mint_markets", "cnbctv18"]:
            news_items.extend(result_map.get(sk, []))
        uploads.append(("market_news.json", make_payload(news_items)))

        # Upload all concurrently
        print("\nUploading to R2...")
        upload_tasks = [r2_put(client, fname, payload) for fname, payload in uploads]
        await asyncio.gather(*upload_tasks)

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
