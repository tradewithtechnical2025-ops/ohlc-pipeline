#!/usr/bin/env python3
"""
Corporate Actions Pipeline — GitHub Actions (FinEdge API version)

Migrated from the old per-ISIN Upstox pipeline. FinEdge's corporate-actions
endpoint returns current + future actions for ALL symbols in a single call
when no filters are passed — no more sequential per-symbol requests, no more
429 skip-logic needed.

Docs: GET https://data.finedgeapi.com/api/v1/corporate-actions/all
      "If no parameters are provided, current and future corporate actions
       are returned."

Output format is kept identical to the old Upstox-based pipeline
(corporate_actions.json = { SYMBOL: [ {type, sub_type, ex_date, ...}, ... ] })
so the frontend (hubRenderMovers / _corpActionsMap) needs no changes.
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

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_URL    = "https://data.finedgeapi.com/api/v1/corporate-actions/all"
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

RETRY = 3

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

import re

def normalize_type(v):
    v = str(v).strip().lower()
    if v in TYPE_MAP:
        return TYPE_MAP[v]
    # Some record shapes embed the type inside a longer description string,
    # e.g. "Bonus issue 1:2" — search for a known keyword instead of an
    # exact match in that case.
    for kw, mapped in TYPE_MAP.items():
        if kw in v:
            return mapped
    return v.title()


RATIO_RE = re.compile(r"(\d+\s*[:\-]\s*\d+)")

def extract_ratio(text):
    if not text:
        return None
    m = RATIO_RE.search(str(text))
    return m.group(1).replace(" ", "") if m else None


def parse_date(v):
    """FinEdge sample format is '04-Jun-2024' (%d-%b-%Y). Falls back to
    ISO (YYYY-MM-DD) and a couple other common formats just in case
    different action types return dates differently."""
    if not v:
        return ""
    v = str(v).strip()
    if len(v) == 10 and v[4] == "-":
        return v  # already ISO
    for fmt in ("%d-%b-%Y", "%d %b %Y", "%d-%B-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return v  # leave as-is if unrecognized — better than dropping the record


def to_float(v):
    try:
        return float(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None


def parse_action(raw):
    """Maps one FinEdge corporate-actions record to our normalized schema.
    Uses .get() everywhere and tries multiple possible field names, since
    the /all endpoint's dividend sample used ex_date/amount/subject, but
    the per-symbol split/bonus/rights endpoint (a different endpoint) uses
    date/action-as-description instead — /all *should* include all types
    per its own docs, but we don't yet have a confirmed sample of what a
    bonus/split/rights record looks like inside /all, so this stays
    tolerant of either shape until a real pipeline run confirms it."""
    action_raw = raw.get("action", "")
    action_type = normalize_type(action_raw)
    if action_type not in VALID_TYPES:
        return None

    ex_date = raw.get("ex_date") or raw.get("date")
    ratio   = raw.get("ratio") or extract_ratio(action_raw)

    return {
        "type"             : action_type,
        "sub_type"         : raw.get("dividend_type") or raw.get("type") or "",
        "announcement_date": parse_date(raw.get("announcement_date")),
        "ex_date"          : parse_date(ex_date),
        "record_date"      : parse_date(raw.get("record_date")),
        "amount"           : to_float(raw.get("amount")),
        "adj_amount"       : to_float(raw.get("adj_amount")),
        "div_pct"          : to_float(raw.get("div_pct") or raw.get("dividend_pct")),
        "ratio"            : ratio,
        "detail"           : raw.get("subject") or raw.get("detail") or action_raw or "",
    }


# ──────────────────────────────────────────────
# R2
# ──────────────────────────────────────────────

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
# Fetch — single call, current + future, all symbols
# ──────────────────────────────────────────────

async def fetch_all(client):
    for attempt in range(RETRY):
        try:
            r = await client.get(
                FINEDGE_URL,
                params={"token": FINEDGE_TOKEN},
                timeout=60,
            )
        except httpx.RequestError as e:
            log.warning(f"Network error (attempt {attempt+1}/{RETRY}): {e}")
            await asyncio.sleep(3 * (attempt + 1))
            continue

        if r.status_code == 401:
            log.error("❌ FINEDGE_TOKEN invalid")
            raise SystemExit(1)

        if r.status_code in (429, 502, 503, 504):
            log.warning(f"  {r.status_code} — retrying (attempt {attempt+1}/{RETRY})")
            await asyncio.sleep(5 * (attempt + 1))
            continue

        if r.status_code != 200:
            raise RuntimeError(f"FinEdge corporate-actions fetch failed: HTTP {r.status_code}")

        try:
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"Failed to parse FinEdge response: {e}")

        # Response is a flat list of action records, each carrying its own "symbol"
        return data if isinstance(data, list) else []

    raise RuntimeError("FinEdge corporate-actions fetch failed after retries")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

async def run():

    log.info("━━━ Corporate Actions Pipeline (FinEdge) ━━━")

    async with httpx.AsyncClient() as client:

        log.info("Fetching current + future corporate actions from FinEdge…")
        raw_records = await fetch_all(client)
        log.info(f"  {len(raw_records)} raw records received")

        output  = {}
        skipped = 0

        for raw in raw_records:
            symbol = (raw.get("symbol") or "").strip()
            if not symbol:
                skipped += 1
                continue

            item = parse_action(raw)
            if not item:
                skipped += 1
                continue

            output.setdefault(symbol, []).append(item)

        # Sort each symbol's actions by ex_date, most recent/soonest last-known first
        for symbol in output:
            output[symbol].sort(key=lambda x: x["ex_date"] or "", reverse=True)

        total_acts = sum(len(v) for v in output.values())
        log.info(f"  Symbols with actions : {len(output)}")
        log.info(f"  Total actions        : {total_acts}")
        log.info(f"  Skipped (bad type/no symbol) : {skipped}")

        await r2_upload(client, "corporate_actions.json", output)

    log.info("✅ corporate_actions.json uploaded")
    log.info("━━━ Done ━━━")


if __name__ == "__main__":
    asyncio.run(run())
