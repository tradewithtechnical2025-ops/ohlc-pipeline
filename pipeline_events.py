#!/usr/bin/env python3
"""
Corporate Actions + Announcements Pipeline — GitHub Actions (FinEdge API version)

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

ADDED (Aug 2026): Corporate Announcements section.
Docs: GET https://data.finedgeapi.com/api/v1/corp-announcements
      "Consolidated daily feed of corporate announcements published by
       companies listed on NSE and BSE... retrieve announcements for up to
       last 7 days from the current date... If symbol not provided,
       from_date/to_date cannot be more than 7 days apart."
This is the same "bulk daily feed, no per-symbol calls" shape as
corporate-actions/all, so it's handled with the same build-history-forward
approach: call with no params (defaults to from_date=today-1, to_date=today
per the docs), merge into a rolling archive, don't try to backfill deep
history in one shot since the API doesn't support that without a symbol.
Output format: corp_announcements.json / corp_announcements_history.json,
same { SYMBOL: [ {...}, ... ] } shape as the corporate actions files, for
frontend consistency.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone

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

FINEDGE_URL              = "https://data.finedgeapi.com/api/v1/corporate-actions/all"
FINEDGE_ANNOUNCEMENTS_URL = "https://data.finedgeapi.com/api/v1/corp-announcements"
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

RETRY = 3

# Announcements are far higher-volume than corporate actions (a company can
# file several a week vs a handful of dividends/bonuses a year), so this
# uses a shorter retention window than corp actions' 1-year one to keep the
# archive file size reasonable. Tune if a longer/shorter Documents-tab
# history is wanted later.
ANNOUNCEMENTS_RETENTION_DAYS = 180

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

    detail_text = raw.get("subject") or raw.get("detail") or action_raw or ""
    ex_date = raw.get("ex_date") or raw.get("date")
    # The ratio (e.g. "1:1") usually appears in the descriptive text
    # (subject/detail), not the short action keyword — try that first.
    ratio = raw.get("ratio") or extract_ratio(detail_text) or extract_ratio(action_raw)

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
        "detail"           : detail_text,
    }


def parse_announcement(raw, bse_map):
    """Maps one FinEdge corp-announcements record to our normalized schema.

    Symbol resolution: sample response carries both stock_symbol and
    nse_code (equal in the sample, "ITC") plus a numeric bse_code — prefer
    stock_symbol, fall back to nse_code, and as a last resort remap a
    numeric bse_code via classification.json (same bse_map already built
    for corporate actions below — reused here, not rebuilt).

    Timestamp handling: confirmed live that "ex_date" here is actually an
    IST-localized announcement/filing timestamp (NOT a dividend ex-date —
    unfortunate field-name reuse from a different FinEdge endpoint) and
    "timestamp_unix" is the same moment as a UTC epoch int. Verified they
    agree exactly (timestamp_unix converted to IST == the ex_date string).
    timestamp_unix is used as the authoritative sort/dedup/prune key since
    it's an unambiguous int; the ex_date string is kept as-is for display
    under the renamed field "announced_at" (renamed specifically to avoid
    confusion with corporate_actions.json's "ex_date", which means a
    completely different thing there).

    pdf_file_link_hist: the docs' sample value for this field is literally
    the placeholder string "string" (Swagger/OpenAPI example filler, not
    real data) — only kept if it actually looks like a URL.
    """
    symbol = (raw.get("stock_symbol") or raw.get("nse_code") or "").strip()
    if not symbol:
        bse_code = str(raw.get("bse_code") or "").strip()
        symbol = bse_map.get(bse_code, "")
    if not symbol:
        return None, None

    ts = raw.get("timestamp_unix")
    try:
        ts = int(ts) if ts not in (None, "") else None
    except (ValueError, TypeError):
        ts = None
    announced_at = str(raw.get("ex_date") or "")

    pdf_link = raw.get("pdf_file_link") or ""
    pdf_link_hist = raw.get("pdf_file_link_hist") or ""
    if not str(pdf_link_hist).startswith("http"):
        pdf_link_hist = ""

    item = {
        "category"     : raw.get("category") or "",
        "sub_category" : raw.get("sub_category") or "",
        "description"  : raw.get("description") or "",
        "announced_at" : announced_at,
        "pdf_link"     : pdf_link,
        "pdf_link_hist": pdf_link_hist,
        "timestamp_unix": ts,
    }
    return symbol, item


# ──────────────────────────────────────────────
# R2
# ──────────────────────────────────────────────

async def r2_download(client, filename):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=WORKER_HEADERS, timeout=120)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        return None


async def build_bse_to_nse_map(client):
    """classification.json (from the classification pipeline) carries both
    bse_code and symbol (NSE) per stock — use it to remap any numeric BSE
    scrip codes FinEdge returns instead of NSE trading symbols, since the
    frontend matches corporate actions against the NSE symbol only."""
    classification = await r2_download(client, "classification.json")
    if not classification:
        log.warning("  classification.json not found — BSE code remap skipped")
        return {}
    bse_map = {}
    for row in classification:
        bse_code = str(row.get("bse_code") or "").strip()
        symbol   = (row.get("symbol") or "").strip()
        if bse_code and symbol:
            bse_map[bse_code] = symbol
    log.info(f"  BSE→NSE map built: {len(bse_map)} entries")
    return bse_map


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
# Fetch — corporate actions: single call, current + future, all symbols
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
# Fetch — corporate announcements: daily bulk feed, all symbols
# ──────────────────────────────────────────────

async def fetch_announcements(client):
    """No symbol param → defaults apply (from_date=today-1, to_date=today
    per the docs), giving ~2 days of announcements across ALL companies in
    one call. Meant to be run daily and merged into a rolling history
    archive (see run()) rather than backfilled in one shot, since without a
    symbol the API caps from_date/to_date at 7 days apart anyway."""
    for attempt in range(RETRY):
        try:
            r = await client.get(
                FINEDGE_ANNOUNCEMENTS_URL,
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
            raise RuntimeError(f"FinEdge corp-announcements fetch failed: HTTP {r.status_code}")

        try:
            data = r.json()
        except Exception as e:
            raise RuntimeError(f"Failed to parse FinEdge announcements response: {e}")

        return data if isinstance(data, list) else []

    raise RuntimeError("FinEdge corp-announcements fetch failed after retries")


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

        # Some FinEdge records key by numeric BSE scrip code instead of the
        # NSE trading symbol — remap those so the frontend (which matches
        # on NSE symbol) can actually find them. bse_map is reused below
        # for corporate announcements too, so it's built once per run.
        bse_map = await build_bse_to_nse_map(client)
        remapped, unresolved = 0, 0
        for raw_symbol in list(output.keys()):
            if raw_symbol.isdigit():
                nse_symbol = bse_map.get(raw_symbol)
                if nse_symbol:
                    output.setdefault(nse_symbol, []).extend(output.pop(raw_symbol))
                    remapped += 1
                else:
                    unresolved += 1  # leave as-is — better than losing the data entirely
        if remapped or unresolved:
            log.info(f"  BSE codes remapped to NSE symbol : {remapped}")
            log.info(f"  BSE codes with no NSE match       : {unresolved}")

        # Re-sort after any merges from remapping
        for symbol in output:
            output[symbol].sort(key=lambda x: x["ex_date"] or "", reverse=True)

        # ── Cumulative history archive (for chart markers), rolling 1 year ──
        # We can't cheaply backfill deep past history (the no-symbol query
        # is capped at 30-day windows), so instead we build history forward:
        # every day's current+future fetch gets merged into a persistent
        # archive. As today's "future" action's ex_date passes, it becomes
        # part of the historical record automatically — no extra API calls
        # needed. To keep the file from growing forever, anything older
        # than 1 year (by ex_date) is pruned on every run.
        log.info("Merging into cumulative history archive (rolling 1 year)…")
        history = await r2_download(client, "corporate_actions_history.json") or {}

        cutoff = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                  .replace(year=datetime.now(timezone.utc).year - 1)).strftime("%Y-%m-%d")

        def _dedup_key(a):
            if not isinstance(a, dict):
                return None
            return (a.get("type", ""), a.get("ex_date", ""), a.get("amount"), a.get("ratio"), a.get("detail", ""))

        added, pruned, dropped_malformed = 0, 0, 0
        for symbol, acts in output.items():
            existing = history.setdefault(symbol, [])
            # Drop anything that isn't a well-formed record (e.g. leftover
            # from an older/different history file format on R2)
            clean_existing = [a for a in existing if isinstance(a, dict) and "ex_date" in a]
            dropped_malformed += len(existing) - len(clean_existing)
            existing = history[symbol] = clean_existing

            existing_keys = {_dedup_key(a) for a in existing}
            for a in acts:
                if _dedup_key(a) not in existing_keys:
                    existing.append(a)
                    existing_keys.add(_dedup_key(a))
                    added += 1

        # Prune anything older than the rolling cutoff, across all symbols
        # (not just ones touched this run) and drop symbols left with none.
        for symbol in list(history.keys()):
            before = len(history[symbol])
            history[symbol] = [a for a in history[symbol] if isinstance(a, dict) and (a.get("ex_date") or "") >= cutoff]
            pruned += before - len(history[symbol])
            history[symbol].sort(key=lambda x: x.get("ex_date") or "", reverse=True)
            if not history[symbol]:
                del history[symbol]

        log.info(f"  New records added to history      : {added}")
        log.info(f"  Old records pruned (> 1yr)        : {pruned}")
        log.info(f"  Malformed legacy records dropped  : {dropped_malformed}")
        log.info(f"  History now covers                : {len(history)} symbols")

        await r2_upload(client, "corporate_actions_history.json", history)

        total_acts = sum(len(v) for v in output.values())
        log.info(f"  Symbols with actions : {len(output)}")
        log.info(f"  Total actions        : {total_acts}")
        log.info(f"  Skipped (bad type/no symbol) : {skipped}")

        await r2_upload(client, "corporate_actions.json", output)

        log.info("✅ corporate_actions.json uploaded")

        # ══════════════════════════════════════════════════════════════
        # CORPORATE ANNOUNCEMENTS (added Aug 2026)
        # ══════════════════════════════════════════════════════════════
        log.info("━━━ Corporate Announcements Pipeline (FinEdge) ━━━")
        log.info("Fetching daily corporate announcements feed from FinEdge…")
        raw_announcements = await fetch_announcements(client)
        log.info(f"  {len(raw_announcements)} raw announcement records received")

        ann_output  = {}
        ann_skipped = 0
        for raw in raw_announcements:
            symbol, item = parse_announcement(raw, bse_map)
            if not symbol or not item:
                ann_skipped += 1
                continue
            ann_output.setdefault(symbol, []).append(item)

        for symbol in ann_output:
            ann_output[symbol].sort(key=lambda x: x.get("timestamp_unix") or 0, reverse=True)

        log.info(f"  Symbols with announcements : {len(ann_output)}")
        log.info(f"  Skipped (no resolvable symbol) : {ann_skipped}")

        await r2_upload(client, "corp_announcements.json", ann_output)

        # ── Cumulative history archive, rolling ANNOUNCEMENTS_RETENTION_DAYS ──
        # Same build-history-forward approach as corporate actions above —
        # today's daily-feed fetch gets merged into a persistent archive
        # rather than trying to backfill deep history (not possible without
        # a symbol param per the API's own 7-day window cap).
        log.info(f"Merging into announcements history archive (rolling {ANNOUNCEMENTS_RETENTION_DAYS} days)…")
        ann_history = await r2_download(client, "corp_announcements_history.json") or {}

        ann_cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=ANNOUNCEMENTS_RETENTION_DAYS)).timestamp())

        def _ann_dedup_key(a):
            if not isinstance(a, dict):
                return None
            return (a.get("timestamp_unix"), a.get("pdf_link"), a.get("description"))

        ann_added, ann_pruned, ann_dropped_malformed = 0, 0, 0
        for symbol, items in ann_output.items():
            existing = ann_history.setdefault(symbol, [])
            clean_existing = [a for a in existing if isinstance(a, dict) and "timestamp_unix" in a]
            ann_dropped_malformed += len(existing) - len(clean_existing)
            existing = ann_history[symbol] = clean_existing

            existing_keys = {_ann_dedup_key(a) for a in existing}
            for a in items:
                if _ann_dedup_key(a) not in existing_keys:
                    existing.append(a)
                    existing_keys.add(_ann_dedup_key(a))
                    ann_added += 1

        for symbol in list(ann_history.keys()):
            before = len(ann_history[symbol])
            ann_history[symbol] = [
                a for a in ann_history[symbol]
                if isinstance(a, dict) and (a.get("timestamp_unix") or 0) >= ann_cutoff_ts
            ]
            ann_pruned += before - len(ann_history[symbol])
            ann_history[symbol].sort(key=lambda x: x.get("timestamp_unix") or 0, reverse=True)
            if not ann_history[symbol]:
                del ann_history[symbol]

        log.info(f"  New records added to history      : {ann_added}")
        log.info(f"  Old records pruned (> {ANNOUNCEMENTS_RETENTION_DAYS}d)      : {ann_pruned}")
        log.info(f"  Malformed legacy records dropped  : {ann_dropped_malformed}")
        log.info(f"  History now covers                : {len(ann_history)} symbols")

        await r2_upload(client, "corp_announcements_history.json", ann_history)

        log.info("✅ corp_announcements.json uploaded")

    log.info("━━━ Done ━━━")


if __name__ == "__main__":
    asyncio.run(run())
