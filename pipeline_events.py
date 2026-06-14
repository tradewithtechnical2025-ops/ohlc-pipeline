#!/usr/bin/env python3
"""
Corporate Actions Pipeline — GitHub Actions
Fetches corporate actions for all NSE stocks from Upstox per-ISIN endpoint
and uploads to R2 as corporate_actions.json

Usage:
  python pipeline_corporate_actions.py
"""

import asyncio
import json
import logging
import os
from datetime import datetime

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

CONCURRENCY  = 10
DELAY        = 0.5
RETRY        = 3

TYPE_MAP = {
    "dividend"        : "Dividend",
    "bonus"           : "Bonus",
    "bonus issue"     : "Bonus",
    "split"           : "Split",
    "stock split"     : "Split",
    "rights"          : "Rights",
    "rights issue"    : "Rights",
    "buyback"         : "Buyback",
    "merger"          : "Merger",
    "demerger"        : "Demerger",
}

VALID_TYPES = {
    "Dividend", "Bonus", "Split", "Rights", "Buyback", "Merger", "Demerger"
}


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _headers():
    return {
        "Accept"       : "application/json",
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
    }


def normalize_type(v):
    return TYPE_MAP.get(str(v).strip().lower(), str(v).strip().title())


def parse_date(v):
    """'14 Aug 2025' → '2025-08-14'  |  already ISO → passthrough  |  None → ''"""
    if not v:
        return ""
    v = str(v).strip()
    if not v:
        return ""
    # Already ISO
    if len(v) == 10 and v[4] == "-":
        return v
    # "14 Aug 2025"
    for fmt in ("%d %b %Y", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return v


def extract_event_detail(event_details, name):
    """event_details list mein se `name` key wali value nikalo."""
    for item in (event_details or []):
        if str(item.get("name", "")).strip().lower() == name.lower():
            return item.get("value")
    return None


def parse_action(raw):
    """Single raw action dict → normalized dict (or None if invalid)."""

    action_type = normalize_type(raw.get("name", ""))
    if action_type not in VALID_TYPES:
        return None

    details = raw.get("event_details") or []

    # Dates
    ex_date           = parse_date(
        extract_event_detail(details, "Ex dividend date")
        or extract_event_detail(details, "Ex date")
        or raw.get("expiry_date")
    )
    record_date       = parse_date(extract_event_detail(details, "Record date"))
    announcement_date = parse_date(extract_event_detail(details, "Announcement date"))

    # Sub-type (Final / Interim / Special)
    sub_type = extract_event_detail(details, "Dividend type") or ""

    # Amount
    amount = raw.get("amount")
    try:
        amount = float(amount) if amount not in (None, "") else None
    except (ValueError, TypeError):
        amount = None

    # Ratio (bonus / split)
    ratio = raw.get("ratio")
    if ratio == "":
        ratio = None

    # Dividend %
    div_pct = extract_event_detail(details, "Dividend %")
    try:
        div_pct = float(div_pct) if div_pct not in (None, "") else None
    except (ValueError, TypeError):
        div_pct = None

    # Detail text
    detail = extract_event_detail(details, "Details") or ""

    return {
        "type"             : action_type,
        "sub_type"         : sub_type,
        "announcement_date": announcement_date,
        "ex_date"          : ex_date,
        "record_date"      : record_date,
        "amount"           : amount,
        "div_pct"          : div_pct,
        "ratio"            : ratio,
        "detail"           : detail,
    }


# ──────────────────────────────────────────────
# R2
# ──────────────────────────────────────────────

async def r2_download(client, filename):
    r = await client.get(
        f"{WORKER_URL}/{filename}",
        headers=WORKER_HEADERS,
        timeout=120,
    )
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


async def r2_upload(client, filename, data):
    payload = json.dumps(data, separators=(",", ":")).encode()
    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers={**WORKER_HEADERS, "Content-Type": "application/json"},
        content=payload,
        timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(payload)/1024:.1f} KB)")


# ──────────────────────────────────────────────
# Fetch per ISIN
# ──────────────────────────────────────────────

async def fetch_ca_for_isin(client, sem, symbol, isin):
    url = f"{UPSTOX_BASE}/fundamentals/{isin}/corporate-actions"
    async with sem:
        await asyncio.sleep(DELAY)
        for attempt in range(RETRY):
            try:
                r = await client.get(url, headers=_headers(), timeout=20)
            except httpx.RequestError as e:
                await asyncio.sleep(2 ** attempt)
                continue
            if r.status_code == 401:
                log.error("❌ UPSTOX_TOKEN invalid")
                raise SystemExit(1)
            if r.status_code == 429:
                log.warning(f"  429 — waiting 30s ({symbol})")
                await asyncio.sleep(30)
                continue
            if r.status_code == 404:
                return symbol, []
            if r.status_code != 200:
                return symbol, []
            try:
                data = r.json()
                return symbol, data.get("data") or []
            except Exception:
                return symbol, []
    return symbol, []


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

async def run():

    log.info("━━━ Corporate Actions Pipeline ━━━")

    async with httpx.AsyncClient() as client:

        # Step 1: master.json se symbol → ISIN map
        log.info("Downloading master.json…")
        master = await r2_download(client, "master.json")
        if not master:
            raise RuntimeError("master.json download failed")

        isin_map = {
            s["symbol"]: s["isin"]
            for s in master
            if s.get("isin")
        }
        log.info(f"  {len(isin_map)} symbols with ISIN")

        # Step 2: concurrent fetch
        log.info(f"Fetching corporate actions ({CONCURRENCY} concurrent)…")
        sem     = asyncio.Semaphore(CONCURRENCY)
        tasks   = [
            fetch_ca_for_isin(client, sem, symbol, isin)
            for symbol, isin in isin_map.items()
        ]
        results = await asyncio.gather(*tasks)

        # Step 3: parse + build output
        output      = {}
        total_acts  = 0
        empty_syms  = 0
        skipped     = 0

        for symbol, raw_list in results:
            if not raw_list:
                empty_syms += 1
                continue

            parsed = []
            for raw in raw_list:
                item = parse_action(raw)
                if item is None:
                    skipped += 1
                    continue
                parsed.append(item)

            if parsed:
                # Sort by ex_date desc (latest first)
                parsed.sort(key=lambda x: x["ex_date"] or "", reverse=True)
                output[symbol] = parsed
                total_acts += len(parsed)

        log.info(f"  Symbols with actions : {len(output)}")
        log.info(f"  Total actions        : {total_acts}")
        log.info(f"  Empty / no data      : {empty_syms}")
        log.info(f"  Skipped (bad type)   : {skipped}")

        # Step 4: upload
        await r2_upload(client, "corporate_actions.json", output)

    log.info("✅ corporate_actions.json uploaded")
    log.info("━━━ Done ━━━")


if __name__ == "__main__":
    asyncio.run(run())
