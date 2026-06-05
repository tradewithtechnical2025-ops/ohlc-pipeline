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

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]

WORKER_URL = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

UPSTOX_BSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
UPSTOX_NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

RATE_DELAY = 0.20
RETRY = 3

RETENTION_DAYS = 365

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

DL_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN
}

UP_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json"
}

# =========================================================
# DATE HELPERS
# =========================================================

def today_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def yesterday_str():
    return (
        datetime.now(timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%d")


def history_file(prefix, day):
    return f"history/{prefix}_{day}.json"


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
        raise RuntimeError(
            f"{filename} upload failed: {r.status_code}"
        )

    print(f"✅ Uploaded {filename}")


# =========================================================
# FINEDGE
# =========================================================

async def finedge_get(client, path):

    url = f"{FINEDGE_BASE}/{path}"

    params = {
        "token": FINEDGE_TOKEN
    }

    for attempt in range(RETRY):

        await asyncio.sleep(RATE_DELAY)

        try:

            r = await client.get(
                url,
                params=params,
                timeout=60,
            )

        except Exception as e:

            print(f"⚠️ Network Error: {e}")

            await asyncio.sleep(2 ** attempt)
            continue

        if r.status_code == 429:

            print("⏳ 429 Rate Limit — waiting 15s")

            await asyncio.sleep(15)
            continue

        if r.status_code != 200:

            print(
                f"❌ HTTP {r.status_code} "
                f"for path {path[:100]}"
            )

            return None

        try:
            return r.json()
        except Exception:
            return None

    return None


async def fetch_symbols(client):

    print("📡 Fetching Finedge stock-symbols...")

    data = await finedge_get(
        client,
        "stock-symbols"
    )

    if not data:
        raise RuntimeError(
            "stock-symbols fetch failed"
        )

    print(
        f"✅ Loaded {len(data)} Finedge symbols"
    )

    return data


# =========================================================
# UPSTOX BSE
# =========================================================

async def fetch_upstox_bse(client):

    print("📡 Fetching Upstox BSE master...")

    r = await client.get(
        UPSTOX_BSE_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Referer": "https://upstox.com/"
        },
        follow_redirects=True,
        timeout=120,
    )

    r.raise_for_status()

    data = json.loads(
        gzip.decompress(r.content)
    )

    print(
        f"✅ Loaded {len(data)} BSE instruments"
    )

    return data


# =========================================================
# UPSTOX NSE
# =========================================================

async def fetch_upstox_nse(client):

    print("📡 Fetching Upstox NSE master...")

    r = await client.get(
        UPSTOX_NSE_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Referer": "https://upstox.com/"
        },
        follow_redirects=True,
        timeout=120,
    )

    r.raise_for_status()

    data = json.loads(
        gzip.decompress(r.content)
    )

    print(
        f"✅ Loaded {len(data)} NSE instruments"
    )

    return data


# =========================================================
# SNAPSHOT BUILDERS
# =========================================================

def build_finedge_snapshot(data):

    out = {}

    for stock in data:

        symbol = str(
            stock.get("symbol") or ""
        ).strip().upper()

        if not symbol:
            continue

        out[symbol] = {
            "symbol": symbol,
            "name": stock.get("name"),
            "bse_code": stock.get("bse_code"),
            "nse_code": stock.get("nse_code"),
        }

    return out


def build_bse_snapshot(data):

    out = {}

    for x in data:

        token = str(
            x.get("exchange_token") or ""
        ).strip()

        if not token:
            continue

        out[token] = {
            "exchange_token": token,
            "segment": x.get("segment"),
            "trading_symbol": x.get("trading_symbol"),
            "isin": x.get("isin"),
            "name": x.get("name"),
        }

    return out


def build_nse_snapshot(data):

    out = {}

    for x in data:

        symbol = str(
            x.get("trading_symbol") or ""
        ).strip().upper()

        if not symbol:
            continue

        out[symbol] = {
            "symbol": symbol,
            "segment": x.get("segment"),
            "isin": x.get("isin"),
            "name": x.get("name"),
        }

    return out
# =========================================================
# DETECT : NEW LISTINGS
# =========================================================

def detect_new_listings(
    old_finedge,
    new_finedge,
    old_nse,
    new_nse,
    old_bse,
    new_bse,
    report_date
):

    out = []

    # -----------------------------------------
    # NEW IN FINEDGE
    # -----------------------------------------

    old_symbols = set(old_finedge.keys())
    new_symbols = set(new_finedge.keys())

    for symbol in sorted(new_symbols - old_symbols):

        s = new_finedge[symbol]

        out.append({
            "event": "NEW_FINEDGE_LISTING",
            "date": report_date,

            "symbol": symbol,
            "name": s.get("name"),

            "bse_code": s.get("bse_code"),
            "nse_code": s.get("nse_code"),
        })

    # -----------------------------------------
    # NEW NSE LISTING
    # -----------------------------------------

    old_nse_symbols = set(old_nse.keys())
    new_nse_symbols = set(new_nse.keys())

    for symbol in sorted(new_nse_symbols - old_nse_symbols):

        s = new_nse[symbol]

        out.append({
            "event": "NEW_NSE_LISTING",
            "date": report_date,

            "symbol": symbol,
            "name": s.get("name"),

            "isin": s.get("isin"),
            "segment": s.get("segment"),
        })

    # -----------------------------------------
    # NEW BSE LISTING
    # -----------------------------------------

    old_tokens = set(old_bse.keys())
    new_tokens = set(new_bse.keys())

    for token in sorted(new_tokens - old_tokens):

        s = new_bse[token]

        out.append({
            "event": "NEW_BSE_LISTING",
            "date": report_date,

            "bse_code": token,

            "symbol": s.get("trading_symbol"),
            "name": s.get("name"),

            "isin": s.get("isin"),
            "segment": s.get("segment"),
        })

    return out


# =========================================================
# DETECT : DELISTED
# =========================================================

def detect_delisted(
    old_finedge,
    new_finedge,
    report_date
):

    out = []

    old_symbols = set(old_finedge.keys())
    new_symbols = set(new_finedge.keys())

    for symbol in sorted(old_symbols - new_symbols):

        s = old_finedge[symbol]

        out.append({
            "event": "DELISTED",
            "date": report_date,

            "symbol": symbol,
            "name": s.get("name"),

            "bse_code": s.get("bse_code"),
            "nse_code": s.get("nse_code"),
        })

    return out


# =========================================================
# DETECT : BSE -> NSE
# =========================================================

def detect_bse_to_nse(
    old_finedge,
    new_finedge,
    report_date
):

    out = []

    common = (
        set(old_finedge.keys())
        &
        set(new_finedge.keys())
    )

    for symbol in sorted(common):

        old = old_finedge[symbol]
        new = new_finedge[symbol]

        old_nse = (
            str(old.get("nse_code") or "")
            .strip()
        )

        new_nse = (
            str(new.get("nse_code") or "")
            .strip()
        )

        if (not old_nse) and new_nse:

            out.append({
                "event": "BSE_TO_NSE",
                "date": report_date,

                "symbol": symbol,
                "name": new.get("name"),

                "bse_code": new.get("bse_code"),

                "old_nse_code": None,
                "new_nse_code": new_nse,
            })

    return out


# =========================================================
# DETECT : SME -> MAINBOARD
# =========================================================

def detect_sme_to_mainboard(
    old_bse,
    new_bse,
    report_date
):

    out = []

    common = (
        set(old_bse.keys())
        &
        set(new_bse.keys())
    )

    for token in sorted(common):

        old = old_bse[token]
        new = new_bse[token]

        old_seg = str(
            old.get("segment") or ""
        ).upper()

        new_seg = str(
            new.get("segment") or ""
        ).upper()

        if (
            old_seg == "BSE_SME"
            and
            new_seg == "BSE_EQ"
        ):

            out.append({
                "event": "SME_TO_MAINBOARD",
                "date": report_date,

                "bse_code": token,

                "symbol":
                    new.get("trading_symbol"),

                "name":
                    new.get("name"),

                "isin":
                    new.get("isin"),

                "old_segment":
                    old_seg,

                "new_segment":
                    new_seg,
            })

    return out


# =========================================================
# DETECT : SME -> NSE
# =========================================================

def detect_sme_to_nse(
    old_finedge,
    new_finedge,
    old_bse,
    report_date
):

    out = []

    common = (
        set(old_finedge.keys())
        &
        set(new_finedge.keys())
    )

    for symbol in sorted(common):

        old = old_finedge[symbol]
        new = new_finedge[symbol]

        bse_code = str(
            new.get("bse_code") or ""
        )

        if not bse_code:
            continue

        bse_info = old_bse.get(bse_code)

        if not bse_info:
            continue

        old_segment = str(
            bse_info.get("segment") or ""
        ).upper()

        old_nse = str(
            old.get("nse_code") or ""
        ).strip()

        new_nse = str(
            new.get("nse_code") or ""
        ).strip()

        if (
            old_segment == "BSE_SME"
            and
            (not old_nse)
            and
            new_nse
        ):

            out.append({
                "event": "SME_TO_NSE",
                "date": report_date,

                "symbol": symbol,
                "name": new.get("name"),

                "bse_code": bse_code,

                "new_nse_code":
                    new_nse,
            })

    return out


# =========================================================
# SUMMARY
# =========================================================

def build_summary(
    report_date,
    new_listings,
    delisted,
    bse_to_nse,
    sme_to_mainboard,
    sme_to_nse
):

    return {
        "date": report_date,

        "new_listings":
            len(new_listings),

        "delisted":
            len(delisted),

        "bse_to_nse":
            len(bse_to_nse),

        "sme_to_mainboard":
            len(sme_to_mainboard),

        "sme_to_nse":
            len(sme_to_nse),
    }
# =========================================================
# CLEANUP
# =========================================================

async def cleanup_history(client):

    today = datetime.now(timezone.utc).date()

    for days_back in range(
        RETENTION_DAYS + 1,
        RETENTION_DAYS + 30
    ):

        old_date = (
            today - timedelta(days=days_back)
        ).strftime("%Y-%m-%d")

        files = [

            history_file(
                "finedge",
                old_date
            ),

            history_file(
                "upstox_bse",
                old_date
            ),

            history_file(
                "upstox_nse",
                old_date
            ),
        ]

        for f in files:

            try:

                r = await client.delete(
                    f"{WORKER_URL}/{f}",
                    headers=DL_HEADERS,
                    timeout=60
                )

                if r.status_code == 200:
                    print(
                        f"🗑 Deleted {f}"
                    )

            except Exception:
                pass


# =========================================================
# LOAD YESTERDAY SNAPSHOTS
# =========================================================

async def load_yesterday_snapshots(
    client,
    yday
):

    old_finedge = (
        await r2_download(
            client,
            history_file(
                "finedge",
                yday
            )
        )
        or {}
    )

    old_bse = (
        await r2_download(
            client,
            history_file(
                "upstox_bse",
                yday
            )
        )
        or {}
    )

    old_nse = (
        await r2_download(
            client,
            history_file(
                "upstox_nse",
                yday
            )
        )
        or {}
    )

    return (
        old_finedge,
        old_bse,
        old_nse
    )


# =========================================================
# SAVE TODAY SNAPSHOTS
# =========================================================

async def save_today_snapshots(
    client,
    today,
    finedge,
    bse,
    nse
):

    await r2_upload(
        client,
        history_file(
            "finedge",
            today
        ),
        finedge
    )

    await r2_upload(
        client,
        history_file(
            "upstox_bse",
            today
        ),
        bse
    )

    await r2_upload(
        client,
        history_file(
            "upstox_nse",
            today
        ),
        nse
    )


# =========================================================
# SAVE REPORTS
# =========================================================

async def save_reports(
    client,
    summary,
    new_listings,
    delisted,
    bse_to_nse,
    sme_to_mainboard,
    sme_to_nse
):

    await r2_upload(
        client,
        "reports/summary.json",
        summary
    )

    await r2_upload(
        client,
        "reports/new_listings.json",
        new_listings
    )

    await r2_upload(
        client,
        "reports/delisted.json",
        delisted
    )

    await r2_upload(
        client,
        "reports/bse_to_nse.json",
        bse_to_nse
    )

    await r2_upload(
        client,
        "reports/sme_to_mainboard.json",
        sme_to_mainboard
    )

    await r2_upload(
        client,
        "reports/sme_to_nse.json",
        sme_to_nse
    )


# =========================================================
# MAIN
# =========================================================

async def main():

    today = today_str()
    yday = yesterday_str()

    print()
    print("=" * 60)
    print("      MIGRATION TRACKER")
    print("=" * 60)

    async with httpx.AsyncClient(
        headers=HEADERS
    ) as client:

        # ----------------------------------
        # Load yesterday
        # ----------------------------------

        (
            old_finedge,
            old_bse,
            old_nse

        ) = await load_yesterday_snapshots(
            client,
            yday
        )

        first_run = (
            not old_finedge
        )

        if first_run:

            print()
            print(
                "⚠️ First Run Detected"
            )
            print(
                "No reports generated today."
            )

        # ----------------------------------
        # Fetch today
        # ----------------------------------

        finedge_raw = (
            await fetch_symbols(client)
        )

        upstox_bse_raw = (
            await fetch_upstox_bse(client)
        )

        upstox_nse_raw = (
            await fetch_upstox_nse(client)
        )

        # ----------------------------------
        # Build snapshots
        # ----------------------------------

        new_finedge = (
            build_finedge_snapshot(
                finedge_raw
            )
        )

        new_bse = (
            build_bse_snapshot(
                upstox_bse_raw
            )
        )

        new_nse = (
            build_nse_snapshot(
                upstox_nse_raw
            )
        )

        # ----------------------------------
        # First run
        # ----------------------------------

        if first_run:

            await save_today_snapshots(
                client,
                today,
                new_finedge,
                new_bse,
                new_nse
            )

            print()
            print(
                "✅ Baseline snapshots saved"
            )

            return

        # ----------------------------------
        # Reports
        # ----------------------------------

        new_listings = (
            detect_new_listings(
                old_finedge,
                new_finedge,
                old_nse,
                new_nse,
                old_bse,
                new_bse,
                today
            )
        )

        delisted = (
            detect_delisted(
                old_finedge,
                new_finedge,
                today
            )
        )

        bse_to_nse = (
            detect_bse_to_nse(
                old_finedge,
                new_finedge,
                today
            )
        )

        sme_to_mainboard = (
            detect_sme_to_mainboard(
                old_bse,
                new_bse,
                today
            )
        )

        sme_to_nse = (
            detect_sme_to_nse(
                old_finedge,
                new_finedge,
                old_bse,
                today
            )
        )

        summary = (
            build_summary(
                today,
                new_listings,
                delisted,
                bse_to_nse,
                sme_to_mainboard,
                sme_to_nse
            )
        )

        # ----------------------------------
        # Upload reports
        # ----------------------------------

        await save_reports(
            client,
            summary,
            new_listings,
            delisted,
            bse_to_nse,
            sme_to_mainboard,
            sme_to_nse
        )

        # ----------------------------------
        # Save snapshots
        # ----------------------------------

        await save_today_snapshots(
            client,
            today,
            new_finedge,
            new_bse,
            new_nse
        )

        # ----------------------------------
        # Cleanup
        # ----------------------------------

        await cleanup_history(
            client
        )

        print()
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)

        print(
            json.dumps(
                summary,
                indent=2
            )
        )

        print()
        print(
            "🎉 Migration Tracker Done"
        )


# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":

    asyncio.run(main())
