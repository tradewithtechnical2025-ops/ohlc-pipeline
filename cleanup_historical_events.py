#!/usr/bin/env python3
"""
ONE-TIME CLEANUP — run manually, not part of the daily pipeline.

The old (pre-fix) migration_tracker.py compared only yesterday-vs-today
snapshots with no memory of history, so any stock that got suspended and
later resumed trading got wrongly tagged as a fresh listing/migration.
That bug is fixed going forward; this script removes the BAD entries
already sitting in reports/events.json from before the fix.

Covers exactly 4 categories, each verified against a DIFFERENT ground
truth — no per-symbol API calls anywhere (NSE's quote-equity API blocks
those), only bulk endpoints already proven reliable:

  1. New Listing in BSE        -> checked against the BSE bse_code itself
                                   (already known, no fetch needed). Codes
                                   below OLD_CODE_CUTOFF predate 2026.

  2. New Listing in NSE         -> checked against NSE's own bulk
     (incl. demergers)             "Recent Listing" feed
                                   (/api/new-listing-today?index=RecentListing).
                                   If the ISIN isn't in that feed despite
                                   the event date falling inside the
                                   feed's covered window, it didn't
                                   genuinely list recently.

  3. BSE -> NSE migration       -> same NSE recent-listings feed. If the
                                   ISIN shows up there as a recent NSE
                                   listing, the BSE_TO_NSE claim is
                                   confirmed (we already know it's on BSE
                                   from the event itself).

  4. SME -> NSE Mainboard       -> checked against NSE's official
     migration                     "Migration to Main Board" page (the
                                   full all-time list of ~160 companies
                                   with exact migration dates).

Plus one signal that applies everywhere for free: a symbol ending in
"-RE" is BSE's own re-admission marker — always a resumption.

Events whose date falls OUTSIDE the window any of these sources actually
cover are left untouched (no evidence either way — better to keep than
guess). This is printed clearly so you know what wasn't checked.

Flagged entries are written to reports/flagged_resumptions_<timestamp>.json
for review — nothing is silently lost — and removed from the live
reports/events.json only if run with --apply.

Usage:
    python cleanup_historical_events.py            # dry run, no changes
    python cleanup_historical_events.py --apply    # actually clean up R2
"""

import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone

import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
DL_HEADERS   = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS   = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}

# BSE codes below this predate 2026 — safe margin below the highest
# confirmed-old code seen (542851/544794) and well below the lowest
# confirmed genuine-2026 code seen (731895+).
OLD_CODE_CUTOFF = 600000

NSE_SME_MIGRATION_URL = "https://www.nseindia.com/static/companies-listing/raising-capital-public-issues-emerge-selecting-a-migration-to-main-board"

CHECK_EVENT_TYPES = {"NEW_BSE_LISTING", "NEW_NSE_LISTING", "BSE_TO_NSE", "SME_TO_NSE", "SME_TO_MAINBOARD_NSE"}

NSE_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


async def r2_download(client, filename):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=120)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


async def r2_upload(client, filename, data):
    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers=UP_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed: {r.status_code}")
    print(f"✅ Uploaded {filename}")


def is_readmission_symbol(symbol):
    return bool(symbol) and symbol.strip().upper().endswith("-RE")


def is_old_bse_code(code):
    if code is None:
        return False
    try:
        return int(str(code).strip()) < OLD_CODE_CUTOFF
    except ValueError:
        return False


# =========================================================
# BULK NSE SOURCES (no per-symbol calls)
# =========================================================

async def fetch_nse_recent_listing_isins(client):
    """Ground truth for categories 2 & 3 — NSE's own bulk 'Recent Listing'
    feed. Returns (isin_set, earliest_date, latest_date) so callers can
    tell whether a given event's date actually falls inside the window
    this feed covers. Never raises — empty results on any failure."""
    try:
        await client.get("https://www.nseindia.com/", headers=NSE_BROWSER_HEADERS, timeout=30)
        r = await client.get(
            "https://www.nseindia.com/api/new-listing-today?index=RecentListing",
            headers={
                **NSE_BROWSER_HEADERS,
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.nseindia.com/market-data/new-stock-exchange-listings-recent",
            },
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json().get("data", [])
    except Exception as e:
        print(f"⚠️  NSE recent-listing feed fetch failed (non-fatal): {e}")
        return set(), None, None

    isins = {row["isin"] for row in rows if row.get("isin")}
    parsed = []
    for row in rows:
        try:
            parsed.append(datetime.strptime(row["listing_date"], "%d-%b-%Y"))
        except (KeyError, ValueError):
            pass
    earliest = min(parsed) if parsed else None
    latest = max(parsed) if parsed else None
    print(f"📋 NSE recent-listing feed: {len(rows)} rows, {len(isins)} ISINs"
          + (f", covers {earliest:%Y-%m-%d} to {latest:%Y-%m-%d}" if parsed else ", no usable dates"))
    return isins, earliest, latest


async def fetch_nse_migration_symbols(client):
    """Ground truth for category 4 — NSE's official, all-time
    'Migration to Main Board' list (~160 companies, exact dates).
    Returns (symbol -> date_iso dict, latest_date). Never raises."""
    try:
        await client.get("https://www.nseindia.com/", headers=NSE_BROWSER_HEADERS, timeout=30)
        r = await client.get(
            NSE_SME_MIGRATION_URL,
            headers={**NSE_BROWSER_HEADERS, "Accept": "text/html,application/xhtml+xml",
                     "Referer": "https://www.nseindia.com/"},
            follow_redirects=True,
            timeout=60,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️  NSE migration list fetch failed (non-fatal): {e}")
        return {}, None

    html = r.text
    row_pattern    = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
    symbol_pattern = re.compile(r"/equity/([A-Z0-9&]+)/", re.I)
    date_pattern   = re.compile(r"(\d{1,2}-[A-Za-z]{3,9}-\d{4})")

    out = {}
    for row_html in row_pattern.findall(html):
        sym_m, date_m = symbol_pattern.search(row_html), date_pattern.search(row_html)
        if not sym_m or not date_m:
            continue
        try:
            date_iso = datetime.strptime(date_m.group(1), "%d-%b-%Y").strftime("%Y-%m-%d")
        except ValueError:
            continue
        out[sym_m.group(1).upper()] = date_iso

    latest = max(out.values()) if out else None
    print(f"📋 NSE official migration list: {len(out)} companies"
          + (f", most recent entry {latest}" if latest else ""))
    return out, latest


# =========================================================
# MAIN
# =========================================================

async def main():
    apply_changes = "--apply" in sys.argv
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async with httpx.AsyncClient() as client:
        events = await r2_download(client, "reports/events.json") or []
        print(f"📂 Loaded {len(events)} events\n")

        nse_recent_isins, recent_earliest, recent_latest = await fetch_nse_recent_listing_isins(client)
        nse_migrations, migration_latest = await fetch_nse_migration_symbols(client)
        print()

        kept, flagged, unverifiable = [], [], []

        for e in events:
            ev = e.get("event")
            if ev not in CHECK_EVENT_TYPES:
                kept.append(e)
                continue

            symbol = e.get("symbol") or ""
            if is_readmission_symbol(symbol):
                flagged.append({**e, "_flag_reason": "readmission_symbol(-RE)"})
                continue

            # --- Category 1: New Listing in BSE ---
            if ev == "NEW_BSE_LISTING":
                code = e.get("bse_code")
                if is_old_bse_code(code):
                    flagged.append({**e, "_flag_reason": f"old_bse_code({code})"})
                else:
                    kept.append(e)
                continue

            # --- Categories 2 & 3: New Listing in NSE / BSE->NSE migration ---
            if ev in ("NEW_NSE_LISTING", "BSE_TO_NSE"):
                date = e.get("date") or ""
                # Upper bound is TODAY, not the feed's own latest entry —
                # a gap (e.g. a weekend with zero genuine listings) means
                # nothing new happened, not that the feed is stale, since
                # we just fetched it live moments ago.
                if not (recent_earliest and date >= recent_earliest.strftime("%Y-%m-%d")
                        and date <= today_str):
                    unverifiable.append(e)  # before the feed's earliest entry — no evidence either way
                    continue
                isin = e.get("isin")
                if isin and isin in nse_recent_isins:
                    kept.append(e)  # confirmed: NSE's own feed shows this as a recent listing
                else:
                    flagged.append({**e, "_flag_reason": "not_in_nse_recent_listing_feed"})
                continue

            # --- Category 4: SME -> NSE Mainboard migration ---
            if ev in ("SME_TO_NSE", "SME_TO_MAINBOARD_NSE"):
                date = e.get("date") or ""
                if not (migration_latest and date <= migration_latest):
                    unverifiable.append(e)  # event is more recent than the list has caught up to
                    continue
                if symbol.upper() in nse_migrations:
                    kept.append(e)  # confirmed: in NSE's official all-time migration list
                else:
                    flagged.append({**e, "_flag_reason": "not_in_nse_migration_list"})
                continue

            kept.append(e)

        kept.extend(unverifiable)

        # --- Dedup pass: same real-world event, two tags ---
        # A genuine SME->Mainboard or BSE->NSE migration is, by definition,
        # ALSO "newly on NSE" — so a generic NEW_NSE_LISTING row for the
        # same ISIN often coexists with the more specific migration row
        # (e.g. QMSMEDI: confirmed via NSE's official migration list AND
        # separately backfilled as NEW_NSE_LISTING via the recent-listings
        # feed, since that backfill didn't know about the migration row).
        # The specific tag is strictly more informative — drop the generic
        # duplicate.
        SPECIFIC_NSE_TYPES = {"BSE_TO_NSE", "SME_TO_NSE", "SME_TO_MAINBOARD_NSE"}
        specific_isins = {e["isin"] for e in kept if e.get("event") in SPECIFIC_NSE_TYPES and e.get("isin")}
        specific_symbols = {e["symbol"].upper() for e in kept if e.get("event") in SPECIFIC_NSE_TYPES and e.get("symbol")}
        deduped = []
        for e in kept:
            if e.get("event") != "NEW_NSE_LISTING":
                deduped.append(e)
                continue
            isin_match = bool(e.get("isin")) and e["isin"] in specific_isins
            symbol_match = bool(e.get("symbol")) and e["symbol"].upper() in specific_symbols
            if isin_match or symbol_match:
                flagged.append({**e, "_flag_reason": "duplicate_of_specific_migration_tag"})
            else:
                deduped.append(e)
        dupes_removed = len(kept) - len(deduped)
        kept = deduped
        if dupes_removed:
            print(f"🔁 Dedup: removed {dupes_removed} generic NEW_NSE_LISTING row(s) duplicating a more specific migration tag")

        print(f"Total events checked  : {len(events)}")
        print(f"Flagged as resumption : {len(flagged)}")
        print(f"Kept                  : {len(kept) - len(unverifiable)}")
        print(f"Unverifiable (kept, outside source coverage): {len(unverifiable)}\n")

        by_type = {}
        for e in flagged:
            by_type[e["event"]] = by_type.get(e["event"], 0) + 1
        print("Flagged breakdown by event type:", json.dumps(by_type, indent=2))

        by_reason = {}
        for e in flagged:
            r = e.get("_flag_reason", "").split("(")[0]
            by_reason[r] = by_reason.get(r, 0) + 1
        print("Flagged breakdown by signal:", json.dumps(by_reason, indent=2))

        print("\nSample flagged entries (first 20):")
        for e in flagged[:20]:
            print(f"  {e.get('symbol'):15} | {e.get('event'):22} | {e.get('date')} | {e.get('_flag_reason')}")
        if len(flagged) > 20:
            print(f"  ... and {len(flagged) - 20} more")

        if unverifiable:
            print(f"\nSample unverifiable entries (kept, first 10) — outside what we could check:")
            for e in unverifiable[:10]:
                print(f"  {e.get('symbol'):15} | {e.get('event'):22} | {e.get('date')}")

        if not apply_changes:
            print("\n🔍 DRY RUN — no changes uploaded. Re-run with --apply to actually clean up R2.")
            return

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        await r2_upload(client, f"reports/flagged_resumptions_{ts}.json", flagged)
        await r2_upload(client, "reports/events.json", kept)
        print(f"\n🎉 Cleanup applied. {len(flagged)} entries removed, backed up to reports/flagged_resumptions_{ts}.json")


if __name__ == "__main__":
    asyncio.run(main())
