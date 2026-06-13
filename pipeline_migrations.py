#!/usr/bin/env python3

import asyncio
import gzip
import json
import os
from datetime import datetime, timedelta, timezone

import httpx

# =========================================================
# CONFIG
# =========================================================

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

UPSTOX_BSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
UPSTOX_NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

EVENTS_RETENTION_DAYS = 180

HEADERS    = {"User-Agent": "Mozilla/5.0"}
DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}

# Keywords that indicate bonds / NCDs / debentures in BSE_EQ
BOND_KEYWORDS = {"%", "NCD", "BOND", "DEBENTURE", "PVT", "SR-", "TRANCHE", "SERIES"}

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
    Filter : NSE_EQ  +  INE ISIN
    """
    out = {}
    for x in data:
        segment = str(x.get("segment") or "").upper()
        if segment != "NSE_EQ":
            continue
        isin = str(x.get("isin") or "")
        if not isin.startswith("INE"):
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
# DETECT EVENTS
# =========================================================

def detect_new_bse(old_bse, new_bse, date):
    out = []
    for token in sorted(set(new_bse) - set(old_bse)):
        s = new_bse[token]
        out.append({
            "event":    "NEW_BSE_LISTING",
            "date":     date,
            "symbol":   s["trading_symbol"],
            "name":     s["name"],
            "isin":     s["isin"],
            "bse_code": token,
            "segment":  s["segment"],
        })
    return out


def detect_new_nse(old_nse, new_nse, date):
    out = []
    for isin in sorted(set(new_nse) - set(old_nse)):
        s = new_nse[isin]
        out.append({
            "event":   "NEW_NSE_LISTING",
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


def detect_bse_to_nse(old_nse, new_nse, new_bse, date):
    """
    Newly on NSE today  AND  already on BSE
    = BSE-only stock got NSE listing
    """
    bse_by_isin = {v["isin"]: v for v in new_bse.values()}
    out = []
    for isin in sorted(set(new_nse) - set(old_nse)):
        bse = bse_by_isin.get(isin)
        if not bse:
            continue  # pure new NSE IPO, not BSE->NSE
        nse = new_nse[isin]
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


def detect_sme_to_nse(old_nse, new_nse, old_bse, date):
    """
    Was BSE_SME yesterday + not on NSE yesterday,
    but appears on NSE today
    """
    old_bse_by_isin = {v["isin"]: v for v in old_bse.values()}
    out = []
    for isin in sorted(set(new_nse) - set(old_nse)):
        bse = old_bse_by_isin.get(isin)
        if not bse or bse["segment"] != "BSE_SME":
            continue
        nse = new_nse[isin]
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
        # Load yesterday snapshots
        # --------------------------------------------------

        old_bse = await r2_download(client, "snapshots/upstox_bse.json") or {}
        old_nse = await r2_download(client, "snapshots/upstox_nse.json") or {}

        first_run = not old_bse

        if first_run:
            print("\n⚠️  First Run — saving baseline, no reports today.")

        # --------------------------------------------------
        # Fetch today from Upstox
        # --------------------------------------------------

        bse_raw = await fetch_upstox(client, UPSTOX_BSE_URL, "BSE")
        nse_raw = await fetch_upstox(client, UPSTOX_NSE_URL, "NSE")

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
            print("\n✅ Baseline snapshots saved. Reports will start tomorrow.")
            return

        # --------------------------------------------------
        # Detect all events
        # --------------------------------------------------

        all_events = []
        all_events += detect_new_bse(old_bse, new_bse, today)
        all_events += detect_new_nse(old_nse, new_nse, today)
        all_events += detect_delisted_bse(old_bse, new_bse, today)
        all_events += detect_delisted_nse(old_nse, new_nse, today)
        all_events += detect_bse_to_nse(old_nse, new_nse, new_bse, today)
        all_events += detect_sme_to_mainboard(old_bse, new_bse, today)
        all_events += detect_sme_to_nse(old_nse, new_nse, old_bse, today)

        summary_entry = {
            "date":             today,
            "new_bse":          sum(1 for e in all_events if e["event"] == "NEW_BSE_LISTING"),
            "new_nse":          sum(1 for e in all_events if e["event"] == "NEW_NSE_LISTING"),
            "delisted_bse":     sum(1 for e in all_events if e["event"] == "DELISTED_BSE"),
            "delisted_nse":     sum(1 for e in all_events if e["event"] == "DELISTED_NSE"),
            "bse_to_nse":       sum(1 for e in all_events if e["event"] == "BSE_TO_NSE"),
            "sme_to_mainboard": sum(1 for e in all_events if e["event"] == "SME_TO_MAINBOARD"),
            "sme_to_nse":       sum(1 for e in all_events if e["event"] == "SME_TO_NSE"),
            "total":            len(all_events),
        }

        # --------------------------------------------------
        # Load existing reports + append
        # --------------------------------------------------

        existing_events  = await r2_download(client, "reports/events.json")  or []
        existing_summary = await r2_download(client, "reports/summary.json") or []

        updated_events  = append_events(existing_events, all_events)
        updated_summary = append_summary(existing_summary, summary_entry)

        # --------------------------------------------------
        # Upload reports
        # --------------------------------------------------

        await r2_upload(client, "reports/events.json",  updated_events)
        await r2_upload(client, "reports/summary.json", updated_summary)

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
