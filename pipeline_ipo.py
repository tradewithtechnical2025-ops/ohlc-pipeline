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
from datetime import date, datetime, timedelta
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

# Listed IPOs: keep only last N days from listing_date
LISTED_LOOKBACK_DAYS = 365

# Max records per page (Upstox max = 30)
PAGE_SIZE = 30

DETAIL_CONCURRENCY = 5
DETAIL_DELAY       = 0.3
RETRY              = 3


def _headers():
    return {
        "Accept"       : "application/json",
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
    }

def today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════
# R2
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
# LIST  (paginated)
# ══════════════════════════════════════════════════════════════

async def _fetch_list_page(client, status, issue_type, page):
    url = f"{UPSTOX_BASE}/ipos"
    params = {"status": status, "issue_type": issue_type, "page_number": page, "records": PAGE_SIZE}
    for attempt in range(RETRY):
        try:
            r = await client.get(url, headers=_headers(), params=params, timeout=20)
        except httpx.RequestError as e:
            await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 401: log.error("❌ UPSTOX_TOKEN invalid"); sys.exit(1)
        if r.status_code == 429: await asyncio.sleep(30); continue
        if r.status_code != 200: return []
        try: return r.json().get("data") or []
        except: return []
    return []


async def fetch_all_ids(client) -> list[str]:
    """All IPO ids — listed ones limited to LISTED_LOOKBACK_DAYS via detail filter later."""
    all_ids = {}
    for status in STATUSES:
        for issue_type in ISSUE_TYPES:
            page = 1
            while True:
                items = await _fetch_list_page(client, status, issue_type, page)
                if not items: break
                for item in items:
                    ipo_id = item.get("id")
                    if ipo_id: all_ids[ipo_id] = status
                log.info(f"  [{status}/{issue_type}] page {page}: {len(items)}")
                if len(items) < PAGE_SIZE: break
                page += 1
                await asyncio.sleep(0.2)
    log.info(f"Total unique IDs: {len(all_ids)}")
    return list(all_ids.keys())


# ══════════════════════════════════════════════════════════════
# DETAIL
# ══════════════════════════════════════════════════════════════

async def _fetch_detail(client, sem, ipo_id):
    url = f"{UPSTOX_BASE}/ipos/{ipo_id}"
    async with sem:
        await asyncio.sleep(DETAIL_DELAY)
        for attempt in range(RETRY):
            try:
                r = await client.get(url, headers=_headers(), timeout=20)
            except httpx.RequestError as e:
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
# NORMALIZE
# ══════════════════════════════════════════════════════════════

def _normalize(detail: dict) -> dict:
    tl  = detail.get("timeline") or {}
    reg = detail.get("registrar_info") or {}

    min_p = detail.get("minimum_price") or 0
    max_p = detail.get("maximum_price") or 0
    if min_p and max_p and min_p != max_p:
        price_band = f"₹{int(min_p)} – ₹{int(max_p)}"
    elif max_p:
        price_band = f"₹{int(max_p)}"
    else:
        price_band = "TBA"

    lot_size = detail.get("lot_size") or 0
    min_qty  = detail.get("minimum_quantity") or lot_size
    min_inv  = round(max_p * min_qty) if max_p and min_qty else None

    sub = detail.get("total_subscription")
    try: sub = round(float(sub), 2) if sub else None
    except: sub = None

    listing_price = detail.get("listing_price")
    issue_price   = max_p or None
    listing_gain  = None
    if listing_price and issue_price:
        listing_gain = round((listing_price - issue_price) / issue_price * 100, 2)

    return {
        # Identity
        "id"              : detail.get("id", ""),
        "symbol"          : detail.get("symbol") or "",
        "name"            : detail.get("name", ""),
        "status"          : detail.get("status", ""),
        "issue_type"      : detail.get("issue_type", ""),
        "isin"            : detail.get("isin") or "",
        "industry"        : detail.get("industry") or "",

        # Pricing
        "price_band"      : price_band,
        "min_price"       : min_p or None,
        "max_price"       : max_p or None,
        "cut_off_price"   : detail.get("cut_off_price"),
        "face_value"      : detail.get("face_value"),
        "listing_price"   : listing_price,
        "listing_gain_pct": listing_gain,

        # Lot / size
        "lot_size"        : lot_size or None,
        "min_quantity"    : min_qty or None,
        "min_investment"  : min_inv,
        "issue_size_cr"   : detail.get("issue_size"),

        # Exchange
        "listing_exchange": detail.get("listing_exchange") or "",

        # Key dates (flat)
        "bidding_start"   : detail.get("bidding_start_date") or "",
        "bidding_end"     : detail.get("bidding_end_date") or "",
        "allotment_date"  : tl.get("allotment_date") or "",
        "listing_date"    : tl.get("listing_date") or "",
        "refund_date"     : tl.get("refund_initiation_date") or "",

        # Full timeline
        "timeline": {
            "pre_apply_start": tl.get("pre_apply_start_date") or "",
            "app_start"      : tl.get("application_start_date") or "",
            "app_end"        : tl.get("application_end_date") or "",
            "allotment_start": tl.get("allotment_start_date") or "",
            "allotment"      : tl.get("allotment_date") or "",
            "refund"         : tl.get("refund_initiation_date") or "",
            "listing"        : tl.get("listing_date") or "",
            "mandate_end"    : tl.get("mandate_end_date") or "",
        },

        # Documents
        "rhp_url"         : detail.get("rhp_url") or None,
        "drhp_url"        : detail.get("drhp_url") or None,

        # Subscription
        "subscription_x"  : sub,

        # Registrar
        "registrar": {
            "name"   : reg.get("name") or "",
            "email"  : reg.get("email") or "",
            "contact": reg.get("contact_name") or "",
            "phone"  : reg.get("contact_number") or "",
            "website": reg.get("website") or "",
        },
    }


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

async def run_ipo_scan():
    today = today_ist()
    cutoff = (date.fromisoformat(today) - timedelta(days=LISTED_LOOKBACK_DAYS)).isoformat()
    log.info(f"━━━ IPO Scan  {today}  listed_cutoff:{cutoff} ━━━")

    async with httpx.AsyncClient() as client:
        # Step 1: all IDs
        all_ids = await fetch_all_ids(client)
        log.info(f"Fetching details for {len(all_ids)} IPOs…")

        # Step 2: full details concurrently
        sem = asyncio.Semaphore(DETAIL_CONCURRENCY)
        results = await asyncio.gather(*[_fetch_detail(client, sem, ipo_id) for ipo_id in all_ids])

        # Step 3: normalize + filter
        normalized = []
        skipped_old = 0
        for ipo_id, detail in results:
            if not detail:
                log.warning(f"  ✗ {ipo_id}: no detail"); continue

            obj = _normalize(detail)

            # Filter old listed IPOs by listing_date
            if obj["status"] == "listed":
                ld = obj.get("listing_date") or obj["timeline"].get("listing") or ""
                if ld and ld < cutoff:
                    skipped_old += 1; continue

            normalized.append(obj)

        log.info(f"Skipped {skipped_old} old listed IPOs (before {cutoff})")

        # Step 4: sort — active first, then by bidding_start desc
        STATUS_ORDER = {"open": 0, "upcoming": 1, "closed": 2, "listed": 3}
        normalized.sort(key=lambda x: (
            STATUS_ORDER.get(x["status"], 9),
            -(x.get("bidding_start") or x.get("listing_date") or "").replace("-", "").ljust(8, "0").__hash__() * 0,
            x.get("bidding_start") or x.get("listing_date") or "",
        ))
        # Simpler sort: within each status, newest first
        normalized.sort(key=lambda x: (
            STATUS_ORDER.get(x["status"], 9),
            "~" if not (x.get("bidding_start") or x.get("listing_date")) else
            "z" + (x.get("bidding_start") or x.get("listing_date") or ""),
        ), reverse=False)
        # listed: newest listing first
        for grp_status in ["listed", "closed"]:
            grp = [i for i in normalized if i["status"] == grp_status]
            rest = [i for i in normalized if i["status"] != grp_status]
            grp.sort(key=lambda x: x.get("listing_date") or x.get("bidding_end") or "", reverse=True)
            normalized = [i for i in rest if STATUS_ORDER.get(i["status"], 9) < STATUS_ORDER[grp_status]] + \
                         grp + \
                         [i for i in rest if STATUS_ORDER.get(i["status"], 9) > STATUS_ORDER[grp_status]]

        # Step 5: summary
        by_status = {}; by_type = {}
        for ipo in normalized:
            s = ipo["status"];    by_status[s] = by_status.get(s, 0) + 1
            t = ipo["issue_type"]; by_type[t]  = by_type.get(t, 0) + 1

        log.info(f"Final: {len(normalized)} IPOs")
        log.info(f"  by_status: {by_status}")
        log.info(f"  by_type:   {by_type}")

        # Step 6: upload
        payload = json.dumps({
            "updated"  : today,
            "count"    : len(normalized),
            "by_status": by_status,
            "by_type"  : by_type,
            "ipos"     : normalized,
        }, separators=(",", ":"))

        await r2_upload(client, "ipo_data.json", payload)

    log.info(f"✅ {len(normalized)} IPOs → ipo_data.json")
    log.info("━━━ IPO Scan complete ━━━")


if __name__ == "__main__":
    asyncio.run(run_ipo_scan())
