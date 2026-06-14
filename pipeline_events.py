#!/usr/bin/env python3
"""
Corporate Actions Pipeline — GitHub Actions
Fetches corporate actions for all NSE stocks from Upstox per-ISIN endpoint
and uploads to R2 as corporate_actions.json
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

DELAY   = 0.5   # seconds between each request (sequential)
RETRY   = 2     # retries only on network error, NOT on 429

TYPE_MAP = {
    "dividend"     : "Dividend",
    "bonus"        : "Bonus",
    "bonus issue"  : "Bonus",
    "split"        : "Split",
    "stock split"  : "Split",
    "rights"       : "Rights",
    "rights issue" : "Rights",
    "buyback"      : "Buyback",
    "merger"       : "Merger",
    "demerger"     : "Demerger",
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
    if not v:
        return ""
    v = str(v).strip()
    if len(v) == 10 and v[4] == "-":
        return v
    for fmt in ("%d %b %Y", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return v


def extract_detail(event_details, name):
    for item in (event_details or []):
        if str(item.get("name", "")).strip().lower() == name.lower():
            return item.get("value")
    return None


def parse_action(raw):
    action_type = normalize_type(raw.get("name", ""))
    if action_type not in VALID_TYPES:
        return None

    details = raw.get("event_details") or []

    ex_date = parse_date(
        extract_detail(details, "Ex dividend date")
        or extract_detail(details, "Ex date")
        or raw.get("expiry_date")
    )
    record_date       = parse_date(extract_detail(details, "Record date"))
    announcement_date = parse_date(extract_detail(details, "Announcement date"))
    sub_type          = extract_detail(details, "Dividend type") or ""

    amount = raw.get("amount")
    try:
        amount = float(amount) if amount not in (None, "") else None
    except (ValueError, TypeError):
        amount = None

    ratio = raw.get("ratio") or None

    div_pct = extract_detail(details, "Dividend %")
    try:
        div_pct = float(div_pct) if div_pct not in (None, "") else None
    except (ValueError, TypeError):
        div_pct = None

    detail = extract_detail(details, "Details") or ""

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
# Fetch — sequential, no retry on 429
# ──────────────────────────────────────────────

async def fetch_all(client, isin_map):
    output    = {}
    skipped   = 0
    errors    = 0
    total     = len(isin_map)

    items = list(isin_map.items())

    for idx, (symbol, isin) in enumerate(items, 1):

        await asyncio.sleep(DELAY)

        url = f"{UPSTOX_BASE}/fundamentals/{isin}/corporate-actions"

        try:
            r = await client.get(url, headers=_headers(), timeout=20)
        except httpx.RequestError as e:
            log.warning(f"  Network error {symbol}: {e}")
            errors += 1
            continue

        if r.status_code == 401:
            log.error("❌ UPSTOX_TOKEN invalid")
            raise SystemExit(1)

        if r.status_code == 429:
            # Skip — do not retry, just move on
            log.warning(f"  429 skip {symbol} ({idx}/{total})")
            skipped += 1
            continue

        if r.status_code == 404:
            continue

        if r.status_code != 200:
            errors += 1
            continue

        try:
            raw_list = r.json().get("data") or []
        except Exception:
            errors += 1
            continue

        parsed = []
        for raw in raw_list:
            item = parse_action(raw)
            if item:
                parsed.append(item)

        if parsed:
            parsed.sort(key=lambda x: x["ex_date"] or "", reverse=True)
            output[symbol] = parsed

        if idx % 100 == 0:
            log.info(f"  Progress: {idx}/{total} | found={len(output)} skipped={skipped}")

    return output, skipped, errors


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

async def run():

    log.info("━━━ Corporate Actions Pipeline ━━━")

    async with httpx.AsyncClient() as client:

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

        log.info(f"Fetching corporate actions (sequential, {DELAY}s delay)…")
        output, skipped, errors = await fetch_all(client, isin_map)

        total_acts = sum(len(v) for v in output.values())
        log.info(f"  Symbols with actions : {len(output)}")
        log.info(f"  Total actions        : {total_acts}")
        log.info(f"  Skipped (429)        : {skipped}")
        log.info(f"  Errors               : {errors}")

        await r2_upload(client, "corporate_actions.json", output)

    log.info("✅ corporate_actions.json uploaded")
    log.info("━━━ Done ━━━")


if __name__ == "__main__":
    asyncio.run(run())
