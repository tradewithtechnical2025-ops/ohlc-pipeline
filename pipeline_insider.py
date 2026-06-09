import asyncio
import json
import os
import xml.etree.ElementTree as ET
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

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/xml, text/xml, */*",
}

def parse_xml_trade(xml_text: str) -> dict:
    """Parse NSE insider trading XBRL XML and extract trade fields."""
    try:
        root = ET.fromstring(xml_text)
        # Strip namespace for easier tag matching
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        def g(tag):
            # Try with namespace first, then without
            el = root.find(f".//{ns}{tag}")
            if el is None:
                el = root.find(f".//{tag}")
            return el.text.strip() if el is not None and el.text else ""

        return {
            "insider_name":       g("nameOfThePerson") or g("NameOfThePerson") or g("name"),
            "insider_category":   g("categoryOfPerson") or g("CategoryOfPerson"),
            "designation":        g("typeOfSecurity") or "",   # sometimes designation is here
            "security_type":      g("typeOfSecurity") or g("TypeOfSecurity"),
            "transaction_type":   g("typeOfTransaction") or g("TypeOfTransaction"),  # Buy/Sell
            "trade_date":         g("dateOfAllotmentAcquisitionFromTo") or g("dateOfIntimationToCompany") or g("DateOfTransaction"),
            "qty":                g("numberOfSecuritiesTransacted") or g("NumberOfSecuritiesTransacted"),
            "price":              g("valueOfSecuritiesTransacted") or g("pricePerUnit") or "",
            "pre_holding_pct":    g("percentageOfShareholdingBeforeAcquisition") or g("PreShareholding"),
            "post_holding_pct":   g("percentageOfShareholdingAfterAcquisition") or g("PostShareholding"),
            "exchange":           g("exchange") or g("Exchange") or "NSE",
            "remarks":            g("remarks") or g("Remarks") or "",
        }
    except Exception as e:
        return {"parse_error": str(e)}


async def fetch_xml(client: httpx.AsyncClient, url: str) -> dict:
    try:
        r = await client.get(url, headers=BROWSER_HEADERS, timeout=20, follow_redirects=True)
        r.raise_for_status()
        return parse_xml_trade(r.text)
    except Exception as e:
        return {"fetch_error": str(e)}


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
            headers=BROWSER_HEADERS,
            timeout=30,
            follow_redirects=True
        )
        rss_resp.raise_for_status()
    feed = feedparser.parse(rss_resp.content)
    print(f"RSS Items: {len(feed.entries)}")

    # Parse description field: SYMBOL|Company|...|html_file|...
    def parse_desc(desc: str) -> dict:
        parts = [p.strip() for p in desc.split("|")]
        return {
            "symbol":     parts[0] if len(parts) > 0 else "",
            "regulation": parts[3] if len(parts) > 3 else "",
            "html_url":   f"https://nsearchives.nseindia.com/corporate/xbrl/{parts[5]}" if len(parts) > 5 else "",
        }

    # Build base items from RSS
    base_items = []
    for entry in feed.entries:
        desc_data = parse_desc(entry.get("description", ""))
        base_items.append({
            "company":    entry.get("title", ""),
            "symbol":     desc_data["symbol"],
            "regulation": desc_data["regulation"],
            "published":  entry.get("published", ""),
            "xml_url":    entry.get("link", ""),
            "html_url":   desc_data["html_url"],
        })

    # Fetch all XMLs concurrently
    print("Fetching XML details...")
    async with httpx.AsyncClient() as client:
        xml_tasks = [fetch_xml(client, item["xml_url"]) for item in base_items]
        xml_results = await asyncio.gather(*xml_tasks)

        # Merge
        items = []
        for base, trade in zip(base_items, xml_results):
            items.append({**base, **trade})

        ok  = sum(1 for t in xml_results if "fetch_error" not in t and "parse_error" not in t)
        err = len(xml_results) - ok
        print(f"XML parsed: {ok} ok, {err} errors")

        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(items),
            "items": items
        }

        await r2_put(client, "nse_insider_trading.json", payload)

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
