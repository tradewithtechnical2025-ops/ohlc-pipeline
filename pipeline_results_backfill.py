"""
pipeline_results_backfill.py — ONE-TIME script.

Re-fetches and re-parses every entry already in nse_results_detailed.json
using the current version of the XBRL parser + YoY-fundamentals logic in
nse_pipeline.py. Run this once whenever the parser gains new fields (e.g.
notes/yoy_caution, basis-verified fundamentals YoY) that older entries were
processed before, so existing entries pick up the improvements instead of
waiting for their next filing (which could be months away).

Safe to re-run: on any per-item fetch/parse failure it keeps that item's
existing data untouched rather than dropping it.

Usage: python pipeline_results_backfill.py
(same WORKER_URL / WORKER_TOKEN env vars as the other pipeline scripts)
"""

import asyncio
import httpx

from nse_pipeline import (
    r2_get,
    r2_put,
    fetch_xbrl_bytes,
    parse_financial_results_xbrl,
    _yoy_fundamentals,
    make_payload,
    FUNDAMENTALS_FILE,
)


async def run():
    async with httpx.AsyncClient() as client:
        existing = await r2_get(client, "nse_results_detailed.json")
        items = (existing or {}).get("items", [])
        if not items:
            print("No existing nse_results_detailed.json items — nothing to backfill")
            return
        print(f"Backfilling {len(items)} existing result filing(s)...")

        fundamentals = await r2_get(client, FUNDAMENTALS_FILE)
        fundamentals_stocks = (fundamentals or {}).get("stocks")
        if not fundamentals_stocks:
            print(f"⚠ {FUNDAMENTALS_FILE} unavailable — YoY-via-fundamentals will be skipped this run")

        sem = asyncio.Semaphore(5)  # be polite to nsearchives.nseindia.com
        stats = {"refreshed": 0, "kept_old": 0}

        async def reprocess(it):
            link = it.get("link")
            if not link:
                stats["kept_old"] += 1
                return it  # nothing to refetch from — keep as-is

            async with sem:
                try:
                    content = await fetch_xbrl_bytes(client, link)
                    if not content:
                        print(f"  ⚠ could not refetch {link.split('/')[-1]}, keeping existing data")
                        stats["kept_old"] += 1
                        return it

                    parsed = parse_financial_results_xbrl(content)
                    if not parsed.get("quarter") and not parsed.get("year"):
                        print(f"  ⚠ reparse gave no P&L data for {link.split('/')[-1]}, keeping existing data")
                        stats["kept_old"] += 1
                        return it

                    parsed["link"] = link
                    parsed["title"] = it.get("title", "")
                    parsed["published"] = it.get("published", "")
                    parsed["published_ts"] = it.get("published_ts", 0)

                    if "yoy_comparison" not in parsed and parsed.get("quarter", {}).get("period_end"):
                        symbol = parsed.get("meta", {}).get("symbol")
                        nature = parsed.get("meta", {}).get("standalone_consolidated")
                        yoy_fund = _yoy_fundamentals(
                            symbol, parsed["quarter"]["period_end"], parsed["quarter"],
                            nature, fundamentals_stocks
                        )
                        if yoy_fund:
                            parsed["yoy_fundamentals"] = yoy_fund

                    stats["refreshed"] += 1
                    return parsed
                except Exception as e:
                    print(f"  ⚠ error reprocessing {link.split('/')[-1]}: {e} — keeping existing data")
                    stats["kept_old"] += 1
                    return it

        updated = await asyncio.gather(*(reprocess(it) for it in items))
        updated.sort(key=lambda x: x.get("published_ts", 0), reverse=True)

        print(f"  Refreshed: {stats['refreshed']}   Kept old (fetch/parse failed): {stats['kept_old']}")

        payload = make_payload(updated)
        await r2_put(client, "nse_results_detailed.json", payload)
        print(f"✅ Backfill complete — {len(updated)} items re-uploaded")


if __name__ == "__main__":
    asyncio.run(run())
