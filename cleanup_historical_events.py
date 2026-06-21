#!/usr/bin/env python3
"""
ONE-TIME CLEANUP — run manually, not part of the daily pipeline.

The old (pre-fix) migration_tracker.py compared only yesterday-vs-today
snapshots with no memory of history, so any stock that got suspended and
later resumed trading got wrongly tagged as a fresh listing/migration.
That bug is already fixed going forward (ever_seen_bse/ever_seen_nse
registries), but the BAD entries already sitting in reports/events.json
(from before the fix) are still there and will keep showing up in the
frontend for up to 180 days.

This script finds and removes those bad historical entries using FOUR
signals, applied in order (cheapest/most-certain first):

  1. Symbol ends with "-RE"  — BSE's own marker for a re-admitted /
     relisted entity (e.g. SUMEET-RE). Always a resumption, never new.

  2. BSE code is below OLD_CODE_CUTOFF — ONLY applied to NEW_BSE_LISTING
     events, since that's the only event type making a claim about BSE
     itself. Every confirmed resumption case found so far (RELINFRA
     500390, GENSOL 542851, EDUCOMP 532696, etc.) sits well under 600000,
     while every confirmed genuine-2026 entry (CPs 731895+, RAVINDRA
     ENERGY 751122) sits well above 730000. NOT applied to NEW_NSE_LISTING
     / BSE_TO_NSE / SME_TO_NSE — those claim something about NSE presence,
     and a company can legitimately have an old BSE code while being
     genuinely new to NSE (e.g. a real SME->Mainboard-NSE migration like
     DBEIL). Those defer to signals 3/4 instead.

  3. ISIN correlation (no network) — the SAME real-world event (e.g. a
     company resuming trading on both exchanges the same day) often gets
     recorded as multiple separate event rows by the old detection logic
     (one company -> NEW_BSE_LISTING + NEW_NSE_LISTING + BSE_TO_NSE, all
     same ISIN, same/nearby date). If one of those rows is already
     flagged by signal 1 or 2, every other row sharing that ISIN is the
     same event and gets flagged too — this is what catches RELINFRA's
     BSE_TO_NSE twin once its NEW_BSE_LISTING entry is flagged via the
     BSE code check, with no NSE API call needed.

  4. NSE's own quote-equity API (metadata.listingDate) — the definitive
     fallback for pure-NSE-only resumptions with no BSE cross-listing and
     no correlated twin row at all (e.g. QMSMEDI: listed 11-Oct-2022,
     suspended 25-Oct-2022, resumed 18-Jun-2026). Only queried for
     entries signals 1-3 couldn't resolve, to stay within NSE's rate
     limits and minimise exposure to bot-blocking.

Re-running after a partial cleanup: pass the previous run's flagged-file
name(s) as extra arguments so their ISINs are included in the signal-3
correlation set (catches twins of already-removed entries even though
the corroborating row is no longer in the live events.json):

    python cleanup_historical_events.py --apply reports/flagged_resumptions_20260621T171617Z.json

Flagged entries are written to reports/flagged_resumptions_<timestamp>.json
for review — nothing is silently lost — and removed from the live
reports/events.json only if run with --apply.

Usage:
    python cleanup_historical_events.py                              # dry run
    python cleanup_historical_events.py --apply                      # apply
    python cleanup_historical_events.py --apply reports/flagged_X.json  # apply + correlate against a prior flagged file
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
DL_HEADERS   = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS   = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}

# BSE codes below this predate 2026 — safe margin below the highest
# confirmed-old code seen (542851) and well below the lowest confirmed
# genuine-2026 code seen (731895). Adjust here if you find a real 2026
# listing flagged incorrectly, or a resumption that slips through.
OLD_CODE_CUTOFF = 600000

# If NSE's recorded listing date is more than this many days before the
# event's claimed date, it's a resumption, not a fresh listing. Generous
# buffer — genuine cases should match within a day or two.
LISTING_DATE_BUFFER_DAYS = 90

# Event types susceptible to the old snapshot-diff resumption bug.
# (SME_TO_NSE used the same buggy diff logic before the fix; SME_TO_MAINBOARD
# used an intersection check and was never affected, so it's excluded.)
CHECK_EVENT_TYPES = {"NEW_BSE_LISTING", "NEW_NSE_LISTING", "BSE_TO_NSE", "SME_TO_NSE"}

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


def classify_cheap(event):
    """Signal 1 (always applicable) + Signal 2 (ONLY for NEW_BSE_LISTING,
    where the BSE code age directly answers the claim being made).

    For NEW_NSE_LISTING / BSE_TO_NSE / SME_TO_NSE the claim is about NSE
    presence — a company can legitimately have an old BSE code while being
    genuinely new to NSE for the first time (e.g. a real SME->Mainboard-NSE
    migration). Using BSE code age there would wrongly flag real migrations
    like DBEIL (Deepak Builders, old BSE code, but possibly a first-ever
    NSE listing) — so those event types defer entirely to signal 3
    (NSE's own listingDate) instead."""
    symbol = event.get("symbol") or ""
    if is_readmission_symbol(symbol):
        return "readmission_symbol(-RE)"

    if event.get("event") == "NEW_BSE_LISTING":
        bse_code = event.get("bse_code")
        if is_old_bse_code(bse_code):
            return f"old_bse_code({bse_code})"

    return None


async def fetch_nse_listing_date(client, symbol, retries=1):
    """Signal 4 — NSE's own recorded original listing date for a symbol.
    Returns YYYY-MM-DD, or None if unavailable/blocked (never raises).
    Retries once on failure (transient blocks are common)."""
    for attempt in range(retries + 1):
        try:
            r = await client.get(
                f"https://www.nseindia.com/api/quote-equity?symbol={symbol}",
                headers={
                    **NSE_BROWSER_HEADERS,
                    "Accept": "application/json,text/plain,*/*",
                    "Referer": f"https://www.nseindia.com/get-quote/equity/{symbol}",
                },
                timeout=30,
            )
            if r.status_code != 200:
                if attempt < retries:
                    await asyncio.sleep(1.0)
                    continue
                return None
            raw = (r.json().get("metadata") or {}).get("listingDate")
            if not raw:
                return None
            return datetime.strptime(raw, "%d-%b-%Y").strftime("%Y-%m-%d")
        except Exception:
            if attempt < retries:
                await asyncio.sleep(1.0)
                continue
            return None
    return None


async def main():
    apply_changes = "--apply" in sys.argv
    reference_files = [a for a in sys.argv[1:] if a.startswith("reports/")]

    async with httpx.AsyncClient() as client:
        events = await r2_download(client, "reports/events.json") or []
        print(f"📂 Loaded {len(events)} events")

        # ISINs from prior cleanup runs (already removed from events.json,
        # but still useful to correlate against on a second pass).
        reference_bad_isins = set()
        for fname in reference_files:
            prior = await r2_download(client, fname) or []
            for e in prior:
                if e.get("isin"):
                    reference_bad_isins.add(e["isin"])
            print(f"📂 Loaded {len(prior)} entries from reference file {fname}")

        kept, flagged, unresolved = [], [], []
        for e in events:
            if e.get("event") not in CHECK_EVENT_TYPES:
                kept.append(e)
                continue
            reason = classify_cheap(e)
            if reason:
                flagged.append({**e, "_flag_reason": reason})
            else:
                unresolved.append(e)

        print(f"\nAfter signals 1+2 (no network): {len(flagged)} flagged, {len(unresolved)} remaining")

        # Signal 3 — ISIN correlation (no network). Same real-world event
        # often spans multiple rows (NEW_BSE_LISTING + NEW_NSE_LISTING +
        # BSE_TO_NSE, same ISIN). If one row is already flagged, every
        # other row sharing that ISIN is the same event.
        confirmed_bad_isins = reference_bad_isins | {e["isin"] for e in flagged if e.get("isin")}
        still_unresolved = []
        for e in unresolved:
            isin = e.get("isin")
            if isin and isin in confirmed_bad_isins:
                flagged.append({**e, "_flag_reason": "correlated_isin_resumption"})
            else:
                still_unresolved.append(e)
        unresolved = still_unresolved

        print(f"After signal 3 (ISIN correlation): {len(flagged)} flagged, {len(unresolved)} need NSE API check\n")

        # Signal 4 — NSE quote-equity API, rate-limited (NSE allows ~3 req/s).
        await client.get("https://www.nseindia.com/", headers=NSE_BROWSER_HEADERS, timeout=30)
        checked = api_resolved = api_failed = 0
        for e in unresolved:
            symbol = e.get("symbol")
            if not symbol:
                kept.append(e)
                continue

            if checked > 0 and checked % 30 == 0:
                # Refresh the session periodically — cookies/bot-detection
                # state can go stale over a long run.
                await client.get("https://www.nseindia.com/", headers=NSE_BROWSER_HEADERS, timeout=30)

            listing_date = await fetch_nse_listing_date(client, symbol)
            checked += 1
            if checked % 20 == 0:
                print(f"  ...checked {checked}/{len(unresolved)} via NSE API (resolved={api_resolved}, failed={api_failed})")
            await asyncio.sleep(0.5)

            if not listing_date:
                api_failed += 1
                kept.append({**e, "_check_status": "nse_api_failed_kept_by_default"})
                continue
            api_resolved += 1
            try:
                event_date = datetime.strptime(e["date"], "%Y-%m-%d")
                listed_dt = datetime.strptime(listing_date, "%Y-%m-%d")
            except (KeyError, ValueError):
                kept.append(e)
                continue

            if listed_dt < event_date - timedelta(days=LISTING_DATE_BUFFER_DAYS):
                flagged.append({**e, "_flag_reason": f"nse_listing_date({listing_date})"})
            else:
                kept.append(e)

        print(f"\nNSE API summary: {checked} checked, {api_resolved} resolved, {api_failed} failed/blocked")
        if api_failed:
            print(f"⚠️  {api_failed} entries kept by default because the NSE API call failed — "
                  f"they were NOT verified as genuine, just unresolved. Re-run later to re-check them "
                  f"(they're tagged \"_check_status\":\"nse_api_failed_kept_by_default\" in events.json).")

        print(f"\nTotal events checked  : {len(events)}")
        print(f"Flagged as resumption : {len(flagged)}")
        print(f"Kept                  : {len(kept)}\n")

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
            print(f"  {e.get('symbol'):15} | {e.get('event'):16} | {e.get('date')} | {e.get('_flag_reason')}")
        if len(flagged) > 20:
            print(f"  ... and {len(flagged) - 20} more")

        if not apply_changes:
            print("\n🔍 DRY RUN — no changes uploaded. Re-run with --apply to actually clean up R2.")
            return

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        await r2_upload(client, f"reports/flagged_resumptions_{ts}.json", flagged)
        await r2_upload(client, "reports/events.json", kept)
        print(f"\n🎉 Cleanup applied. {len(flagged)} entries removed, backed up to reports/flagged_resumptions_{ts}.json")


if __name__ == "__main__":
    asyncio.run(main())
