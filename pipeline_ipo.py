#!/usr/bin/env python3
"""
IPO Pipeline — GitHub Actions
Fetches all IPOs (upcoming, open, closed, listed) from Upstox API
with full details and uploads to R2 as ipo_data.json

Usage:
  python pipeline_ipo.py
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, timedelta
from zoneinfo import ZoneInfo

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

UPSTOX_BASE    = "https://api.upstox.com/v2"
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

STATUSES    = ["upcoming", "open", "closed", "listed"]
ISSUE_TYPES = ["regular", "sme"]

# How many days back to include "listed" IPOs
LISTED_LOOKBACK_DAYS = 90

# Max records per page (Upstox max = 30)
PAGE_SIZE = 30

# Concurrency for detail fetches
DETAIL_CONCURRENCY = 5
DETAIL_DELAY       = 0.3
RETRY              = 3


def _headers():
    return {
        "Accept"       : "application/json",
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
    }

def today_ist() -> str:
    return __import__("datetime").datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════
# R2 HELPERS
# ══════════════════════════════════════════════════════════════

async def r2_upload(client, filename, data):
    if isinstance(data, str): data = data.encode()
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(url, headers={**WORKER_HEADERS, "Content-Type": "application/json"},
                          content=data, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")


# ══════════════════════════════════════════════════════════════
# FETCH IPO LIST  (paginated)
# ══════════════════════════════════════════════════════════════

async def _fetch_ipo_list_page(client, status, issue_type, page):
    url = f"{UPSTOX_BASE}/ipos"
    params = {
        "status"     : status,
        "issue_type" : issue_type,
        "page_number": page,
        "records"    : PAGE_SIZE,
    }
    for attempt in range(RETRY):
        try:
            r = await client.get(url, headers=_headers(), params=params, timeout=20)
        except httpx.RequestError as e:
            log.warning(f"  List page error ({e}), retry {attempt+1}")
            await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 401: log.error("❌ UPSTOX_TOKEN invalid"); sys.exit(1)
        if r.status_code == 429: await asyncio.sleep(30); continue
        if r.status_code != 200: return []
        try:
            items = r.json().get("data") or []
            return items
        except: return []
    return []


async def fetch_all_ipo_ids(client) -> list[dict]:
    """
    Fetches IPO list across all statuses + issue_types with pagination.
    Returns list of {id, status, issue_type, name, ...basic fields}
    """
    all_ipos = {}  # id → item (dedup)
    today = today_ist()
    cutoff = (date.fromisoformat(today) - timedelta(days=LISTED_LOOKBACK_DAYS)).isoformat()

    for status in STATUSES:
        for issue_type in ISSUE_TYPES:
            page = 1
            while True:
                items = await _fetch_ipo_list_page(client, status, issue_type, page)
                if not items:
                    break
                for item in items:
                    ipo_id = item.get("id")
                    if not ipo_id:
                        continue
                    # For listed IPOs, skip if listing_date is too old
                    if status == "listed":
                        listing_date = item.get("listing_date") or item.get("timeline", {}).get("listing_date", "")
                        if listing_date and listing_date < cutoff:
                            continue
                    all_ipos[ipo_id] = item
                log.info(f"  List [{status}/{issue_type}] page {page}: {len(items)} items")
                if len(items) < PAGE_SIZE:
                    break
                page += 1
                await asyncio.sleep(0.2)

    log.info(f"Total unique IPOs from list: {len(all_ipos)}")
    return list(all_ipos.values())


# ══════════════════════════════════════════════════════════════
# FETCH IPO DETAILS
# ══════════════════════════════════════════════════════════════

async def _fetch_ipo_detail(client, sem, ipo_id):
    url = f"{UPSTOX_BASE}/ipos/{ipo_id}"
    async with sem:
        await asyncio.sleep(DETAIL_DELAY)
        for attempt in range(RETRY):
            try:
                r = await client.get(url, headers=_headers(), timeout=20)
            except httpx.RequestError as e:
                log.warning(f"  Detail {ipo_id} error ({e}), retry {attempt+1}")
                await asyncio.sleep(2 ** attempt); continue
            if r.status_code == 401: log.error("❌ UPSTOX_TOKEN invalid"); sys.exit(1)
            if r.status_code == 429: await asyncio.sleep(30); continue
            if r.status_code == 404: return ipo_id, None
            if r.status_code != 200: return ipo_id, None
            try:
                data = r.json().get("data")
                return ipo_id, data
            except: return ipo_id, None
    return ipo_id, None


# ══════════════════════════════════════════════════════════════
# BUILD NORMALIZED IPO OBJECT
# ══════════════════════════════════════════════════════════════

def _normalize(detail: dict) -> dict:
    """Flatten and normalize full IPO detail into clean object."""
    tl = detail.get("timeline") or {}
    reg = detail.get("registrar_info") or {}

    # Price band display
    min_p = detail.get("minimum_price") or 0
    max_p = detail.get("maximum_price") or 0
    if min_p and max_p and min_p != max_p:
        price_band = f"₹{min_p} – ₹{max_p}"
    elif max_p:
        price_band = f"₹{max_p}"
    else:
        price_band = "TBA"

    # Min investment
    lot_size = detail.get("lot_size") or 0
    min_qty  = detail.get("minimum_quantity") or lot_size
    min_inv  = round(max_p * min_qty) if max_p and min_qty else None

    # Subscription
    sub = detail.get("total_subscription")
    try: sub = round(float(sub), 2) if sub else None
    except: sub = None

    # Issue size display
    issue_size = detail.get("issue_size")

    return {
        # Identity
        "id"             : detail.get("id", ""),
        "symbol"         : detail.get("symbol", ""),
        "name"           : detail.get("name", ""),
        "status"         : detail.get("status", ""),
        "issue_type"     : detail.get("issue_type", ""),   # regular | sme
        "isin"           : detail.get("isin", ""),
        "industry"       : detail.get("industry", ""),

        # Pricing
        "price_band"     : price_band,
        "min_price"      : min_p or None,
        "max_price"      : max_p or None,
        "cut_off_price"  : detail.get("cut_off_price"),
        "face_value"     : detail.get("face_value"),
        "listing_price"  : detail.get("listing_price"),

        # Lot / size
        "lot_size"       : lot_size or None,
        "min_quantity"   : min_qty or None,
        "min_investment" : min_inv,
        "issue_size_cr"  : issue_size,

        # Exchange
        "listing_exchange": detail.get("listing_exchange", ""),

        # Dates (flat — easy to sort/filter)
        "bidding_start"  : detail.get("bidding_start_date", ""),
        "bidding_end"    : detail.get("bidding_end_date", ""),
        "allotment_date" : tl.get("allotment_date", ""),
        "listing_date"   : tl.get("listing_date", ""),
        "refund_date"    : tl.get("refund_initiation_date", ""),

        # Full timeline
        "timeline": {
            "pre_apply_start"  : tl.get("pre_apply_start_date", ""),
            "app_start"        : tl.get("application_start_date", ""),
            "app_end"          : tl.get("application_end_date", ""),
            "allotment_start"  : tl.get("allotment_start_date", ""),
            "allotment"        : tl.get("allotment_date", ""),
            "refund"           : tl.get("refund_initiation_date", ""),
            "listing"          : tl.get("listing_date", ""),
            "mandate_end"      : tl.get("mandate_end_date", ""),
        },

        # Documents
        "rhp_url"        : detail.get("rhp_url"),
        "drhp_url"       : detail.get("drhp_url"),

        # Subscription
        "subscription_x" : sub,

        # Registrar
        "registrar": {
            "name"   : reg.get("name", ""),
            "email"  : reg.get("email", ""),
            "contact": reg.get("contact_name", ""),
            "phone"  : reg.get("contact_number", ""),
            "website": reg.get("website", ""),
        },
    }


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

async def run_ipo_scan():
    today = today_ist()
    log.info(f"━━━ IPO Scan  {today} ━━━")

    async with httpx.AsyncClient() as client:
        # Step 1: get all IPO ids from list endpoint
        list_items = await fetch_all_ipo_ids(client)
        ipo_ids = [item["id"] for item in list_items if item.get("id")]
        log.info(f"Fetching details for {len(ipo_ids)} IPOs…")

        # Step 2: fetch full details concurrently
        sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
        results = await asyncio.gather(*[
            _fetch_ipo_detail(client, sem, ipo_id)
            for ipo_id in ipo_ids
        ])

        # Step 3: normalize
        normalized = []
        for ipo_id, detail in results:
            if detail:
                normalized.append(_normalize(detail))
            else:
                log.warning(f"  ✗ {ipo_id}: no detail")

        # Step 4: sort — active first, then by date
        STATUS_ORDER = {"open": 0, "upcoming": 1, "closed": 2, "listed": 3}
        normalized.sort(key=lambda x: (
            STATUS_ORDER.get(x["status"], 9),
            x.get("bidding_start") or x.get("listing_date") or "",
        ))

        # Step 5: summary
        by_status = {}
        by_type   = {}
        for ipo in normalized:
            s = ipo["status"];   by_status[s] = by_status.get(s, 0) + 1
            t = ipo["issue_type"]; by_type[t]  = by_type.get(t, 0) + 1

        log.info(f"Summary by status: {by_status}")
        log.info(f"Summary by type:   {by_type}")

        # Step 6: upload
        payload = json.dumps({
            "updated"  : today,
            "count"    : len(normalized),
            "by_status": by_status,
            "by_type"  : by_type,
            "ipos"     : normalized,
        }, separators=(",", ":"))

        await r2_upload(client, "ipo_data.json", payload)

    log.info(f"✅ {len(normalized)} IPOs uploaded")
    log.info("━━━ IPO Scan complete ━━━")


if __name__ == "__main__":
    asyncio.run(run_ipo_scan())
