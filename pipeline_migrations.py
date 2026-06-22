#!/usr/bin/env python3

import asyncio
import gzip
import json
import os
import re
from datetime import datetime, timedelta, timezone

import httpx

# =========================================================
# CONFIG
# =========================================================

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

UPSTOX_BSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
UPSTOX_NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# NSE's own authoritative "Migration to Main Board" page — list of NSE
# Emerge (SME) companies that have moved to the NSE Mainboard, with dates.
# Used as a cross-check / safety net against our own snapshot-diff
# detection (covers any gaps from pipeline downtime, feed lag, etc.)
NSE_SME_MIGRATION_URL = "https://www.nseindia.com/static/companies-listing/raising-capital-public-issues-emerge-selecting-a-migration-to-main-board"

EVENTS_RETENTION_DAYS = 180

# Persistent registries — NEVER pruned. These remember every BSE token /
# NSE ISIN we have ever observed, so a stock that gets suspended and later
# resumes is correctly tagged as a RELISTING instead of a fresh NEW listing.
EVER_SEEN_BSE_FILE = "snapshots/bse_ever_seen.json"
EVER_SEEN_NSE_FILE = "snapshots/nse_ever_seen.json"

HEADERS    = {"User-Agent": "Mozilla/5.0"}
DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}

# Keywords that indicate bonds / NCDs / debentures in BSE_EQ
BOND_KEYWORDS = {"%", "NCD", "BOND", "DEBENTURE", "PVT", "SR-", "TRANCHE", "SERIES", "-RE"}

# Event types whose presence in the historical events log proves the
# instrument was once seen on that exchange (used to backfill ever-seen
# registries on first run after this fix, or if the registry file is lost).
BSE_PRESENCE_EVENTS = {"NEW_BSE_LISTING", "BSE_RELISTING", "DELISTED_BSE", "SME_TO_MAINBOARD", "SME_TO_NSE"}
NSE_PRESENCE_EVENTS = {"NEW_NSE_LISTING", "NSE_RELISTING", "DELISTED_NSE", "BSE_TO_NSE", "SME_TO_NSE"}

# =========================================================
# DATE HELPERS
# =========================================================

def today_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

# =========================================================
# R2 HELPERS
# =========================================================

async def r2_download(client, filename):
    r = await client.get(
        f"{WORKER_URL}/{filename}",
        headers=DL_HEADERS,
        timeout=120,
    )
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

# =========================================================
# FETCH UPSTOX
# =========================================================

async def fetch_upstox(client, url, label):
    print(f"📡 Fetching Upstox {label} master...")
    r = await client.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Referer": "https://upstox.com/",
        },
        follow_redirects=True,
        timeout=120,
    )
    r.raise_for_status()
    data = json.loads(gzip.decompress(r.content))
    print(f"✅ Loaded {len(data)} {label} raw instruments")
    return data

# =========================================================
# NSE OFFICIAL SME MIGRATION LIST  (cross-check / safety net)
# =========================================================

async def fetch_nse_sme_migrations(client):
    """
    Scrapes NSE's official "Migration to Main Board" page — the
    authoritative list of NSE Emerge (SME) companies that have moved to
    the NSE Mainboard, with migration dates. Used purely as a safety
    net: anything our own snapshot-diff detection might have missed
    (pipeline downtime, Upstox feed lag, etc.) gets caught here and
    added with source="nse_official_list".
    Never raises — a failure here should never break the main run.
    """
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        # NSE blocks plain requests without a prior session cookie — warm
        # up the same client's cookie jar with a homepage hit first.
        await client.get("https://www.nseindia.com/", headers=browser_headers, timeout=30)
        r = await client.get(
            NSE_SME_MIGRATION_URL,
            headers={**browser_headers, "Referer": "https://www.nseindia.com/"},
            follow_redirects=True,
            timeout=60,
        )
        r.raise_for_status()
    except Exception as e:
        print(f"⚠️  NSE SME migration list fetch failed (non-fatal): {e}")
        return []

    html = r.text
    out = []
    row_pattern    = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
    symbol_pattern = re.compile(r"/equity/([A-Z0-9&]+)/", re.I)
    date_pattern   = re.compile(r"(\d{1,2}-[A-Za-z]{3,9}-\d{4})")
    cell_pattern   = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)

    for row_html in row_pattern.findall(html):
        sym_m  = symbol_pattern.search(row_html)
        date_m = date_pattern.search(row_html)
        if not sym_m or not date_m:
            continue
        symbol = sym_m.group(1).upper()
        try:
            date_iso = datetime.strptime(date_m.group(1), "%d-%b-%Y").strftime("%Y-%m-%d")
        except ValueError:
            continue
        cells = cell_pattern.findall(row_html)
        name = re.sub(r"<[^>]+>", "", cells[0]).strip() if cells else None
        out.append({"symbol": symbol, "name": name, "date": date_iso})

    print(f"📋 NSE official SME→Mainboard list: {len(out)} companies parsed")
    if not out:
        print("⚠️  Parsed 0 rows — NSE may have changed their page structure or blocked the request; check manually.")
    return out


# Equity-like NSE series codes. Everything else (N0/N1/... = NCD/bond
# tranches, SF = mutual fund scheme units, GS/TB = G-Sec/T-Bill, etc.)
# is debt/non-equity and excluded from the recent-listings backfill.
# NOTE: BE = trade-to-trade settlement. EVERY new listing (not just
# demergers like the Vedanta spin-offs) spends its first few days/weeks
# in BE before NSE moves it to normal EQ rolling settlement — so BE is
# correctly treated as a genuine new listing here, not a special case.
NSE_EQUITY_SERIES = {"EQ", "BE", "BZ", "BT", "SM", "ST"}

NSE_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


async def fetch_nse_recent_listings(client):
    """
    NSE's own JSON API for the "Recent Listing" tab. Clean structured
    data with a 'series' field that reliably tells equity (EQ/BE/ST/...)
    apart from debt/NCD tranches (N0/N1/...) and mutual fund units (SF)
    — something Upstox's raw feed doesn't give us, which is why bonds
    used to slip through as fake NEW_NSE_LISTING events.
    Used purely as a safety net for genuinely-equity entries our own
    snapshot diff might have missed. Never raises.
    """
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
        print(f"⚠️  NSE recent-listings API fetch failed (non-fatal): {e}")
        return []

    print(f"📋 NSE official recent-listings feed: {len(rows)} rows ({sum(1 for x in rows if x.get('series') in NSE_EQUITY_SERIES)} equity-like)")
    return rows

# =========================================================
# BOND FILTER
# =========================================================

def is_bond(x):
    """
    Returns True for bonds / NCDs / debentures that sneak
    into BSE_EQ segment.
    - Symbol starts with digit  e.g. 775IRFC33
    - Name contains bond keywords e.g. IRFC-7.75%-15-4-33-PVT
    """
    sym  = (x.get("trading_symbol") or "")
    name = (x.get("name") or "").upper()
    if sym and sym[0].isdigit():
        return True
    if any(k in name for k in BOND_KEYWORDS):
        return True
    return False

# =========================================================
# SNAPSHOT BUILDERS
# =========================================================

def build_bse_snapshot(data):
    """
    Key    : exchange_token
    Filter : BSE_EQ / BSE_SME  +  INE ISIN  +  not a bond
    """
    out = {}
    for x in data:
        segment = str(x.get("segment") or "").upper()
        if segment not in ("BSE_EQ", "BSE_SME"):
            continue
        isin = str(x.get("isin") or "")
        if not isin.startswith("INE"):
            continue
        if is_bond(x):
            continue
        token = str(x.get("exchange_token") or "").strip()
        if not token:
            continue
        out[token] = {
            "exchange_token": token,
            "segment":        segment,
            "trading_symbol": str(x.get("trading_symbol") or "").strip().upper(),
            "isin":           isin,
            "name":           x.get("name"),
        }
    return out


def build_nse_snapshot(data):
    """
    Key    : isin  (for cross-matching with BSE)
    Filter : NSE_EQ or NSE_SME  +  INE ISIN  +  not a bond

    NOTE: NSE_SME is now tracked (previously only NSE_EQ was), so we can
    detect a company moving from NSE Emerge to the NSE Mainboard via a
    segment transition, the same way BSE_SME -> BSE_EQ is already
    detected on the BSE side. Also: bonds/NCDs were never filtered out
    on the NSE side before (only BSE had this) — fixed by reusing the
    same is_bond() check, so NCD tranches stop showing up as fake
    NEW_NSE_LISTING events.
    """
    out = {}
    for x in data:
        segment = str(x.get("segment") or "").upper()
        if segment not in ("NSE_EQ", "NSE_SME"):
            continue
        isin = str(x.get("isin") or "")
        if not isin.startswith("INE"):
            continue
        if is_bond(x):
            continue
        symbol = str(x.get("trading_symbol") or "").strip().upper()
        if not symbol:
            continue
        out[isin] = {
            "isin":    isin,
            "symbol":  symbol,
            "segment": segment,
            "name":    x.get("name"),
        }
    return out

# =========================================================
# EVER-SEEN REGISTRY HELPERS
# =========================================================

def update_ever_seen(ever_seen, keys, date):
    """Add any keys not already present. Never overwrites an existing
    first-seen date, never removes anything."""
    updated = dict(ever_seen)
    for k in keys:
        if k not in updated:
            updated[k] = date
    return updated


def backfill_ever_seen(ever_seen_bse, ever_seen_nse, old_bse, old_nse, existing_events, today):
    """
    Self-healing seed step. Covers two situations:
      1. This is the first run AFTER deploying this fix — the ever-seen
         registry files don't exist yet, so we rebuild as much history as
         we can from yesterday's snapshot + the existing 180-day events log.
      2. Normal runs — harmless no-op merge (everything already present).
    NOTE: instruments that have been suspended for longer than the
    180-day events retention AND were never in a prior snapshot will
    still show up as a one-time NEW_*/RELISTING misclassification the
    first time this fix sees them — there's no way to recover history
    that was never recorded. Every occurrence after that will be correct.
    """
    ever_seen_bse = update_ever_seen(ever_seen_bse, old_bse.keys(), today)
    ever_seen_nse = update_ever_seen(ever_seen_nse, old_nse.keys(), today)

    for e in existing_events:
        ev = e.get("event")
        if ev in BSE_PRESENCE_EVENTS and e.get("bse_code"):
            ever_seen_bse.setdefault(str(e["bse_code"]), e.get("date", today))
        if ev in NSE_PRESENCE_EVENTS and e.get("isin"):
            ever_seen_nse.setdefault(e["isin"], e.get("date", today))

    return ever_seen_bse, ever_seen_nse

# =========================================================
# DETECT EVENTS
# =========================================================

def detect_new_bse(old_bse, new_bse, ever_seen_bse, date):
    """
    NEW_BSE_LISTING   = token never seen before  -> genuine fresh listing
    BSE_RELISTING     = token seen before, missing yesterday, back today
                        -> suspension lifted / re-admitted to trading
    """
    out = []
    for token in sorted(set(new_bse) - set(old_bse)):
        s = new_bse[token]
        event_type = "BSE_RELISTING" if token in ever_seen_bse else "NEW_BSE_LISTING"
        out.append({
            "event":    event_type,
            "date":     date,
            "symbol":   s["trading_symbol"],
            "name":     s["name"],
            "isin":     s["isin"],
            "bse_code": token,
            "segment":  s["segment"],
        })
    return out


def detect_new_nse(old_nse, new_nse, ever_seen_nse, date):
    """
    NEW_NSE_LISTING   = ISIN never seen on NSE before -> genuine fresh listing
    NSE_RELISTING     = ISIN seen before, missing yesterday, back today
    """
    out = []
    for isin in sorted(set(new_nse) - set(old_nse)):
        s = new_nse[isin]
        event_type = "NSE_RELISTING" if isin in ever_seen_nse else "NEW_NSE_LISTING"
        out.append({
            "event":   event_type,
            "date":    date,
            "symbol":  s["symbol"],
            "name":    s["name"],
            "isin":    isin,
            "segment": s["segment"],
        })
    return out


def detect_delisted_bse(old_bse, new_bse, date):
    out = []
    for token in sorted(set(old_bse) - set(new_bse)):
        s = old_bse[token]
        out.append({
            "event":    "DELISTED_BSE",
            "date":     date,
            "symbol":   s["trading_symbol"],
            "name":     s["name"],
            "isin":     s["isin"],
            "bse_code": token,
        })
    return out


def detect_delisted_nse(old_nse, new_nse, date):
    out = []
    for isin in sorted(set(old_nse) - set(new_nse)):
        s = old_nse[isin]
        out.append({
            "event":  "DELISTED_NSE",
            "date":   date,
            "symbol": s["symbol"],
            "name":   s["name"],
            "isin":   isin,
        })
    return out


def detect_bse_to_nse(old_nse, new_nse, new_bse, ever_seen_nse, date):
    """
    Newly on NSE *Mainboard* today  AND  already on BSE  AND  genuinely
    never on NSE before (not a resumption — that's NSE_RELISTING's job)
    = BSE-only stock got an NSE Mainboard listing for the first time.
    Restricted to NSE_EQ — a fresh NSE_SME listing is just a normal
    NEW_NSE_LISTING (segment=NSE_SME), not a BSE->NSE migration.
    """
    bse_by_isin = {v["isin"]: v for v in new_bse.values()}
    out = []
    for isin in sorted(set(new_nse) - set(old_nse)):
        if isin in ever_seen_nse:
            continue  # resumption, already captured as NSE_RELISTING — not a fresh migration
        nse = new_nse[isin]
        if nse["segment"] != "NSE_EQ":
            continue  # SME listing, not a mainboard cross-listing
        bse = bse_by_isin.get(isin)
        if not bse:
            continue  # pure new NSE IPO, not BSE->NSE
        out.append({
            "event":       "BSE_TO_NSE",
            "date":        date,
            "symbol":      nse["symbol"],
            "name":        nse["name"],
            "isin":        isin,
            "bse_code":    bse["exchange_token"],
            "bse_segment": bse["segment"],
        })
    return out


def detect_sme_to_mainboard(old_bse, new_bse, date):
    out = []
    for token in sorted(set(old_bse) & set(new_bse)):
        if (
            old_bse[token]["segment"] == "BSE_SME"
            and new_bse[token]["segment"] == "BSE_EQ"
        ):
            s = new_bse[token]
            out.append({
                "event":    "SME_TO_MAINBOARD",
                "date":     date,
                "symbol":   s["trading_symbol"],
                "name":     s["name"],
                "isin":     s["isin"],
                "bse_code": token,
            })
    return out


def detect_nse_sme_to_mainboard(old_nse, new_nse, date):
    """
    NSE-side equivalent of detect_sme_to_mainboard — a company already
    on NSE Emerge (NSE_SME) moves to the NSE Mainboard (NSE_EQ).
    Uses an intersection (present in both snapshots, segment changed),
    same pattern as the BSE function — naturally immune to the
    suspension/resumption issue since it requires presence on both days.
    """
    out = []
    for isin in sorted(set(old_nse) & set(new_nse)):
        if (
            old_nse[isin]["segment"] == "NSE_SME"
            and new_nse[isin]["segment"] == "NSE_EQ"
        ):
            s = new_nse[isin]
            out.append({
                "event":  "SME_TO_MAINBOARD_NSE",
                "date":   date,
                "symbol": s["symbol"],
                "name":   s["name"],
                "isin":   isin,
            })
    return out


def detect_sme_to_nse(old_nse, new_nse, old_bse, ever_seen_nse, date):
    """
    Was BSE_SME yesterday + not on NSE Mainboard yesterday,
    but appears on NSE Mainboard today (and genuinely never on NSE
    before). Restricted to NSE_EQ — landing on NSE_SME instead is just
    a normal dual-SME listing, not a mainboard move.
    """
    old_bse_by_isin = {v["isin"]: v for v in old_bse.values()}
    out = []
    for isin in sorted(set(new_nse) - set(old_nse)):
        if isin in ever_seen_nse:
            continue  # resumption, not a fresh SME->NSE move
        nse = new_nse[isin]
        if nse["segment"] != "NSE_EQ":
            continue  # landed on NSE_SME, not mainboard
        bse = old_bse_by_isin.get(isin)
        if not bse or bse["segment"] != "BSE_SME":
            continue
        out.append({
            "event":    "SME_TO_NSE",
            "date":     date,
            "symbol":   nse["symbol"],
            "name":     nse["name"],
            "isin":     isin,
            "bse_code": bse["exchange_token"],
        })
    return out

# =========================================================
# ROLLING APPEND  (180 days)
# =========================================================

def cutoff_date():
    return (
        datetime.now(timezone.utc) - timedelta(days=EVENTS_RETENTION_DAYS)
    ).strftime("%Y-%m-%d")


def append_events(existing, new_events):
    cut = cutoff_date()
    filtered = [e for e in existing if e.get("date", "") >= cut]
    filtered.extend(new_events)
    return filtered


def append_summary(existing, new_entry):
    cut = cutoff_date()
    filtered = [e for e in existing if e.get("date", "") >= cut]
    filtered = [e for e in filtered if e.get("date") != new_entry["date"]]
    filtered.append(new_entry)
    filtered.sort(key=lambda x: x["date"])
    return filtered

# =========================================================
# MAIN
# =========================================================

async def main():

    today = today_str()

    print()
    print("=" * 60)
    print("      MIGRATION TRACKER")
    print("=" * 60)
    print(f"Date : {today}")
    print("=" * 60)

    async with httpx.AsyncClient(headers=HEADERS) as client:

        # --------------------------------------------------
        # Load yesterday snapshots + persistent ever-seen registries
        # --------------------------------------------------

        old_bse = await r2_download(client, "snapshots/upstox_bse.json") or {}
        old_nse = await r2_download(client, "snapshots/upstox_nse.json") or {}

        ever_seen_bse  = await r2_download(client, EVER_SEEN_BSE_FILE)  or {}
        ever_seen_nse  = await r2_download(client, EVER_SEEN_NSE_FILE)  or {}
        existing_events  = await r2_download(client, "reports/events.json")  or []
        existing_summary = await r2_download(client, "reports/summary.json") or []

        first_run = not old_bse

        if first_run:
            print("\n⚠️  First Run — saving baseline, no reports today.")

        # Self-heal the ever-seen registries from whatever history we do
        # have (yesterday's snapshot + the 180-day events log). No-op on
        # normal runs once the registry files exist and are complete.
        ever_seen_bse, ever_seen_nse = backfill_ever_seen(
            ever_seen_bse, ever_seen_nse, old_bse, old_nse, existing_events, today
        )

        # --------------------------------------------------
        # Fetch today from Upstox
        # --------------------------------------------------

        bse_raw = await fetch_upstox(client, UPSTOX_BSE_URL, "BSE")
        nse_raw = await fetch_upstox(client, UPSTOX_NSE_URL, "NSE")

        # Diagnostic: confirm what segment values Upstox actually uses for
        # NSE instruments — specifically whether NSE_SME (Emerge) shows up
        # at all. Cheap to leave in permanently; only prints when something
        # looks off (zero SME found) so it doesn't clutter normal runs.
        from collections import Counter
        nse_segment_counts = Counter(str(x.get("segment") or "").upper() for x in nse_raw)
        if nse_segment_counts.get("NSE_SME", 0) == 0:
            print(f"⚠️  Diagnostic: 0 raw instruments with segment=='NSE_SME' in Upstox's NSE feed. "
                  f"All segment values seen: {dict(nse_segment_counts)}")

        new_bse = build_bse_snapshot(bse_raw)
        new_nse = build_nse_snapshot(nse_raw)

        print(f"\n📊 BSE equity stocks : {len(new_bse)}")
        print(f"📊 NSE equity stocks : {len(new_nse)}")

        # --------------------------------------------------
        # Save today as new snapshot (overwrites yesterday)
        # --------------------------------------------------

        await r2_upload(client, "snapshots/upstox_bse.json", new_bse)
        await r2_upload(client, "snapshots/upstox_nse.json", new_nse)

        if first_run:
            # Baseline day — mark everything we see today as "ever seen"
            # from day 1, so genuine new listings are correctly detected
            # from day 2 onwards.
            ever_seen_bse = update_ever_seen(ever_seen_bse, new_bse.keys(), today)
            ever_seen_nse = update_ever_seen(ever_seen_nse, new_nse.keys(), today)
            await r2_upload(client, EVER_SEEN_BSE_FILE, ever_seen_bse)
            await r2_upload(client, EVER_SEEN_NSE_FILE, ever_seen_nse)
            print("\n✅ Baseline snapshots + ever-seen registries saved. Reports will start tomorrow.")
            return

        # --------------------------------------------------
        # Detect all events
        # --------------------------------------------------

        all_events = []
        all_events += detect_new_bse(old_bse, new_bse, ever_seen_bse, today)

        # Bootstrap guard: if yesterday's snapshot has zero NSE_SME entries,
        # this is the first run since NSE_SME tracking was added — every
        # currently-listed Emerge stock would otherwise look "new" today.
        # Suppress just the NEW_NSE_LISTING/NSE_RELISTING noise for
        # segment=NSE_SME this one time; they still get registered in
        # ever_seen_nse further below so detection is normal from here on.
        old_nse_has_sme = any(v.get("segment") == "NSE_SME" for v in old_nse.values())
        nse_new_events = detect_new_nse(old_nse, new_nse, ever_seen_nse, today)
        if not old_nse_has_sme:
            suppressed = [e for e in nse_new_events if e.get("segment") == "NSE_SME"]
            nse_new_events = [e for e in nse_new_events if e.get("segment") != "NSE_SME"]
            if suppressed:
                print(f"🧹 NSE_SME bootstrap: suppressed {len(suppressed)} pre-existing Emerge stocks (newly tracked, not new listings)")
        all_events += nse_new_events

        all_events += detect_delisted_bse(old_bse, new_bse, today)

        # Bootstrap guard: if yesterday's snapshot still has un-filtered
        # bonds in it (pre-dates the is_bond() fix on the NSE side), every
        # bond that's now correctly excluded from new_nse would otherwise
        # look like it "delisted" today. Suppress those specifically;
        # genuine delistings still fire normally.
        old_nse_had_bonds = any(
            is_bond({"trading_symbol": v["symbol"], "name": v["name"]})
            for v in old_nse.values()
        )
        delisted_nse_events = detect_delisted_nse(old_nse, new_nse, today)
        if old_nse_had_bonds:
            before = len(delisted_nse_events)
            delisted_nse_events = [
                e for e in delisted_nse_events
                if not is_bond({"trading_symbol": e["symbol"], "name": e["name"]})
            ]
            suppressed = before - len(delisted_nse_events)
            if suppressed:
                print(f"🧹 NSE bond-filter bootstrap: suppressed {suppressed} false DELISTED_NSE (bonds now excluded from tracking, not real delistings)")
        all_events += delisted_nse_events

        all_events += detect_bse_to_nse(old_nse, new_nse, new_bse, ever_seen_nse, today)
        all_events += detect_sme_to_mainboard(old_bse, new_bse, today)
        all_events += detect_nse_sme_to_mainboard(old_nse, new_nse, today)
        all_events += detect_sme_to_nse(old_nse, new_nse, old_bse, ever_seen_nse, today)

        # --------------------------------------------------
        # Safety net: cross-check against NSE's own official list
        # --------------------------------------------------

        nse_official = await fetch_nse_sme_migrations(client)
        cut = cutoff_date()
        already_recorded = {
            e["symbol"] for e in existing_events + all_events
            if e.get("event") in (
                "SME_TO_MAINBOARD", "SME_TO_MAINBOARD_NSE", "SME_TO_NSE",
                "NEW_NSE_LISTING", "NSE_RELISTING",
            )
        }
        backfilled = 0
        for m in nse_official:
            if m["date"] < cut or m["symbol"] in already_recorded:
                continue
            all_events.append({
                "event":  "SME_TO_MAINBOARD_NSE",
                "date":   m["date"],
                "symbol": m["symbol"],
                "name":   m["name"],
                "source": "nse_official_list",
            })
            already_recorded.add(m["symbol"])
            backfilled += 1
        if backfilled:
            print(f"🔁 Backfilled {backfilled} migration(s) from NSE's official list (not caught by snapshot diff)")

        # Safety net #2: NSE's "Recent Listing" feed for missed equity
        # NEW_NSE_LISTING entries — restricted to equity-like series so
        # we never accidentally backfill a bond/NCD/MF unit. Also skips
        # any isin already tagged with a more specific migration type
        # (e.g. SME_TO_MAINBOARD_NSE from the official list above) —
        # otherwise the same real event gets double-tagged: once
        # correctly as a migration, once redundantly as a generic new
        # listing (this is exactly what happened with QMSMEDI).
        nse_recent = await fetch_nse_recent_listings(client)
        already_recorded_isins = {
            e.get("isin") for e in existing_events + all_events
            if e.get("event") in ("NEW_NSE_LISTING", "NSE_RELISTING", "BSE_TO_NSE", "SME_TO_NSE", "SME_TO_MAINBOARD_NSE")
            and e.get("isin")
        }
        # SME_TO_MAINBOARD_NSE entries from the official-list scrape carry
        # no ISIN at all (that page only has Name/Symbol/Date) — fall back
        # to symbol matching for those, or this exclusion silently misses
        # them and creates the exact QMSMEDI-style duplicate.
        already_recorded_symbols = {
            e.get("symbol", "").upper() for e in existing_events + all_events
            if e.get("event") in ("BSE_TO_NSE", "SME_TO_NSE", "SME_TO_MAINBOARD_NSE")
            and e.get("symbol")
        }
        recent_backfilled = 0
        for row in nse_recent:
            if row.get("series") not in NSE_EQUITY_SERIES:
                continue
            isin = row.get("isin")
            if not isin or isin in already_recorded_isins:
                continue
            if (row.get("symbol") or "").upper() in already_recorded_symbols:
                continue
            try:
                date_iso = datetime.strptime(row["listing_date"], "%d-%b-%Y").strftime("%Y-%m-%d")
            except (KeyError, ValueError, TypeError):
                continue
            if date_iso < cut:
                continue
            all_events.append({
                "event":   "NEW_NSE_LISTING",
                "date":    date_iso,
                "symbol":  row.get("symbol"),
                "name":    row.get("name"),
                "isin":    isin,
                "segment": "NSE_SME" if row.get("instrument") == "SME" else "NSE_EQ",
                "source":  "nse_official_list",
            })
            already_recorded_isins.add(isin)
            recent_backfilled += 1
        if recent_backfilled:
            print(f"🔁 Backfilled {recent_backfilled} NEW_NSE_LISTING from NSE's recent-listings feed (not caught by snapshot diff)")

        summary_entry = {
            "date":                  today,
            "new_bse":               sum(1 for e in all_events if e["event"] == "NEW_BSE_LISTING"),
            "bse_relisting":         sum(1 for e in all_events if e["event"] == "BSE_RELISTING"),
            "new_nse":               sum(1 for e in all_events if e["event"] == "NEW_NSE_LISTING"),
            "nse_relisting":         sum(1 for e in all_events if e["event"] == "NSE_RELISTING"),
            "delisted_bse":          sum(1 for e in all_events if e["event"] == "DELISTED_BSE"),
            "delisted_nse":          sum(1 for e in all_events if e["event"] == "DELISTED_NSE"),
            "bse_to_nse":            sum(1 for e in all_events if e["event"] == "BSE_TO_NSE"),
            "sme_to_mainboard":      sum(1 for e in all_events if e["event"] == "SME_TO_MAINBOARD"),
            "sme_to_mainboard_nse":  sum(1 for e in all_events if e["event"] == "SME_TO_MAINBOARD_NSE"),
            "sme_to_nse":            sum(1 for e in all_events if e["event"] == "SME_TO_NSE"),
            "backfilled_official":   sum(1 for e in all_events if e.get("source") == "nse_official_list"),
            "total":                 len(all_events),
        }

        # --------------------------------------------------
        # Append + upload reports
        # --------------------------------------------------

        updated_events  = append_events(existing_events, all_events)
        updated_summary = append_summary(existing_summary, summary_entry)

        await r2_upload(client, "reports/events.json",  updated_events)
        await r2_upload(client, "reports/summary.json", updated_summary)

        # --------------------------------------------------
        # Extend + persist the ever-seen registries with today's
        # full universe (so today's genuinely-new instruments are
        # recognised correctly if they ever get suspended later)
        # --------------------------------------------------

        ever_seen_bse = update_ever_seen(ever_seen_bse, new_bse.keys(), today)
        ever_seen_nse = update_ever_seen(ever_seen_nse, new_nse.keys(), today)
        await r2_upload(client, EVER_SEEN_BSE_FILE, ever_seen_bse)
        await r2_upload(client, EVER_SEEN_NSE_FILE, ever_seen_nse)

        # --------------------------------------------------
        # Print summary
        # --------------------------------------------------

        print()
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(json.dumps(summary_entry, indent=2))
        print()
        print("🎉 Migration Tracker Done")


if __name__ == "__main__":
    asyncio.run(main())
