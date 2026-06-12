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

# NSE insider trading XBRL namespace
NS = "http://www.bseindia.com/xbrl/co/2017-09-15/in-bse-co"

def parse_xml_trade(xml_text: str) -> list[dict]:
    """
    Parse NSE/BSE insider trading XBRL XML.
    Returns a list of disclosures (one filing can have multiple trades).
    """
    try:
        root = ET.fromstring(xml_text)

        def g(tag, ctx_id=None):
            """Find tag value, optionally filtered by contextRef."""
            for el in root.iter(f"{{{NS}}}{tag}"):
                if ctx_id is None or el.get("contextRef") == ctx_id:
                    return el.text.strip() if el.text else ""
            return ""

        # MainI context — company-level fields
        company       = g("NameOfTheCompany", "MainI")
        symbol        = g("Symbol", "MainI")
        isin          = g("ISINCode", "MainI")
        signatory     = g("NameOfTheSignatory", "MainI")
        designation   = g("DesignationOfSignatory", "MainI")
        date_filing   = g("DateOfFiling", "MainI")
        regulation    = g("DisclosureUnderRegulation", "MainI")
        revised       = g("RevisedFilling", "MainI")

        # Collect all Disclosure contexts (Disclosure1, Disclosure2, ...)
        ctx_ids = []
        for ctx in root.iter("{http://www.xbrl.org/2003/instance}context"):
            cid = ctx.get("id", "")
            if cid.startswith("Disclosure"):
                ctx_ids.append(cid)

        disclosures = []
        for cid in ctx_ids:
            disclosures.append({
                "symbol":           symbol,
                "company":          company,
                "isin":             isin,
                "date_filing":      date_filing,
                "regulation":       regulation,
                "revised":          revised,
                "signatory":        signatory,
                "signatory_desig":  designation,
                # Trade-level fields
                "insider_name":     g("NameOfThePerson", cid),
                "insider_category": g("CategoryOfPerson", cid),
                "instrument":       g("TypeOfInstrument", cid),
                "transaction_type": g("SecuritiesAcquiredOrDisposedTransactionType", cid),  # Buy/Sell
                "mode":             g("ModeOfAcquisitionOrDisposal", cid),                  # Market Purchase etc
                "qty":              g("SecuritiesAcquiredOrDisposedNumberOfSecurity", cid),
                "value_inr":        g("SecuritiesAcquiredOrDisposedValueOfSecurity", cid),
                "trade_date_from":  g("DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyFromDate", cid),
                "trade_date_to":    g("DateOfAllotmentAdviceOrAcquisitionOfSharesOrSaleOfSharesSpecifyToDate", cid),
                "intimation_date":  g("DateOfIntimationToCompany", cid),
                "exchange":         g("ExchangeOnWhichTheTradeWasExecuted", cid),
                "pre_qty":          g("SecuritiesHeldPriorToAcquisitionOrDisposalNumberOfSecurity", cid),
                "pre_pct":          g("SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding", cid),
                "post_qty":         g("SecuritiesHeldPostAcquistionOrDisposalNumberOfSecurity", cid),
                "post_pct":         g("SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding", cid),
            })

        return disclosures if disclosures else [{"parse_error": "No Disclosure contexts found"}]

    except Exception as e:
        return [{"parse_error": str(e)}]


async def fetch_xml(client: httpx.AsyncClient, rss_item: dict) -> list[dict]:
    xml_url  = rss_item["xml_url"]
    html_url = rss_item["html_url"]
    published = rss_item["published"]
    try:
        r = await client.get(xml_url, headers=BROWSER_HEADERS, timeout=20, follow_redirects=True)
        r.raise_for_status()
        disclosures = parse_xml_trade(r.text)
        # Attach rss-level fields to each disclosure
        for d in disclosures:
            d["published"] = published
            d["xml_url"]   = xml_url
            d["html_url"]  = html_url
        return disclosures
    except Exception as e:
        return [{
            "published":   published,
            "xml_url":     xml_url,
            "html_url":    html_url,
            "fetch_error": str(e)
        }]


def trade_key(d: dict) -> tuple:
    """
    Unique key for a trade. Includes pre/post holdings so that two genuine
    same-day trades with identical qty/value still get distinct keys
    (second trade's pre_qty == first trade's post_qty).
    Duplicate re-filings have identical pre/post → deduped.
    """
    return (
        d.get("symbol", ""),
        d.get("insider_name", "").strip().upper(),
        d.get("trade_date_from", ""),
        d.get("trade_date_to", ""),
        d.get("transaction_type", ""),
        d.get("qty", ""),
        d.get("value_inr", ""),
        d.get("pre_qty", ""),
        d.get("post_qty", ""),
    )


def dedup_trades(items: list[dict]) -> list[dict]:
    """Keep first occurrence (RSS is latest-first, so latest published wins)."""
    seen = set()
    out = []
    for d in items:
        if "fetch_error" in d or "parse_error" in d:
            out.append(d)
            continue
        key = trade_key(d)
        if key in seen:
            continue
        seen.add(key)
        out.append(d)
    return out


def parse_desc(desc: str) -> dict:
    """Extract symbol, regulation, html_url from pipe-separated description."""
    parts = [p.strip() for p in desc.split("|")]
    html_file = parts[5] if len(parts) > 5 else ""
    return {
        "symbol":     parts[0] if len(parts) > 0 else "",
        "regulation": parts[3] if len(parts) > 3 else "",
        "html_url":   f"https://nsearchives.nseindia.com/corporate/xbrl/{html_file}" if html_file else "",
    }


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
            RSS_URL, headers=BROWSER_HEADERS, timeout=30, follow_redirects=True
        )
        rss_resp.raise_for_status()
    feed = feedparser.parse(rss_resp.content)
    print(f"RSS Items: {len(feed.entries)}")

    rss_items = []
    for entry in feed.entries:
        desc_data = parse_desc(entry.get("description", ""))
        rss_items.append({
            "published": entry.get("published", ""),
            "xml_url":   entry.get("link", ""),
            "html_url":  desc_data["html_url"],
        })

    print("Fetching & parsing XMLs...")
    async with httpx.AsyncClient() as client:
        tasks = [fetch_xml(client, item) for item in rss_items]
        results = await asyncio.gather(*tasks)

        # Flatten — each XML can have multiple disclosures
        all_items = [d for disclosures in results for d in disclosures]

        before = len(all_items)
        all_items = dedup_trades(all_items)
        print(f"Dedup: {before} → {len(all_items)}")

        ok  = sum(1 for d in all_items if "fetch_error" not in d and "parse_error" not in d)
        err = len(all_items) - ok
        print(f"Disclosures: {ok} ok, {err} errors")

        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "count": len(all_items),
            "items": all_items
        }

        await r2_put(client, "nse_insider_trading.json", payload)

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
