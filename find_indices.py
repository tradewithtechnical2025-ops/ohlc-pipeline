"""
find_indices.py
Lists ALL NSE indices available on Upstox, with their exact instrument_key.
Run this once locally with your UPSTOX_TOKEN to discover keys before adding
new indices to pipeline_global.py — don't guess the key format.

Usage:
    export UPSTOX_TOKEN=your_token_here
    python find_indices.py
"""

import os
import requests

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
}

# Instrument Search API — v2, segments/exchanges filter, free-text query.
# Docs: https://upstox.com/developer/api-documentation/instrument-search/
SEARCH_URL = "https://api.upstox.com/v2/instruments/search"


def search_indices(query="NIFTY"):
    """Search NSE+BSE INDEX segments for a keyword. Returns list of matches."""
    all_results = []
    page = 1
    while True:
        r = requests.get(
            SEARCH_URL,
            params={
                "query": query,
                "exchanges": "NSE,BSE",
                "segments": "INDEX",
                "page_number": page,
                "records": 30,  # max allowed
            },
            headers=HEADERS,
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  ERR {r.status_code}: {r.text[:200]}")
            break

        body = r.json()
        data = body.get("data", [])
        if not data:
            break

        all_results.extend(data)

        total_pages = body.get("meta_data", {}).get("page", {}).get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1

    return all_results


def main():
    # Search a few broad keywords to sweep most of the index universe.
    # NSE has 100+ indices; a single query="NIFTY" often misses BSE/other
    # spellings, so we sweep a handful of common prefixes.
    keywords = ["NIFTY", "SENSEX", "BANKEX", "INDIA VIX"]

    seen = {}
    for kw in keywords:
        print(f"Searching '{kw}'...")
        results = search_indices(kw)
        for item in results:
            key = item.get("instrument_key")
            name = item.get("trading_symbol") or item.get("name")
            if key and key not in seen:
                seen[key] = name

    print(f"\n=== Found {len(seen)} unique NSE index instruments ===\n")
    for key, name in sorted(seen.items(), key=lambda x: x[1] or ""):
        print(f'  {{"key": "{key}", "name": "{name}"}}')


if __name__ == "__main__":
    main()
