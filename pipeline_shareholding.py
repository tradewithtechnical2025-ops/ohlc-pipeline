#!/usr/bin/env python3

import asyncio
import json
import os
from datetime import datetime

import httpx
from r2_manifest import upload_with_manifest

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

CONCURRENCY = 4
RATE_DELAY  = 0.25
RETRY       = 3

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# R2 Helpers
# ─────────────────────────────────────────────

async def r2_download(client, filename):

    url = f"{WORKER_URL}/{filename}"

    r = await client.get(
        url,
        headers=WORKER_HEADERS,
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} download failed")

    return r.json()


async def r2_upload(client, filename, data):

    url = f"{WORKER_URL}?file={filename}"

    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed")


# ─────────────────────────────────────────────
# Finedge Helper
# ─────────────────────────────────────────────

async def finedge_get(client, sem, path, params):

    params["token"] = FINEDGE_TOKEN

    url = f"{FINEDGE_BASE}/{path}"

    async with sem:

        for attempt in range(RETRY):

            await asyncio.sleep(RATE_DELAY)

            try:

                r = await client.get(
                    url,
                    params=params,
                    timeout=30,
                )

            except Exception as e:

                print(f"  Network Error: {e}")

                await asyncio.sleep(2 ** attempt)

                continue

            if r.status_code == 429:

                print("  429 Rate Limit — waiting 15s")

                await asyncio.sleep(15)

                continue

            if r.status_code != 200:
                return None

            try:
                return r.json()

            except Exception:
                return None

    return None


# ─────────────────────────────────────────────
# Category → Field Mapping
#
#   catagory (API typo)  →  our field
#   ─────────────────────────────────────────
#   "Indian"             →  promoter  (Promoter Indian)
#   "Foreign"            →  promoter  (Promoter Foreign, += to total)
#   "InstitutionsForeign"→  fii
#   "InstitutionsDomestic"→  dii
#   "Goverments"         →  govt      (separate, not merged into DII)
#   "NonInstitutions"    →  public
# ─────────────────────────────────────────────

CATEGORY_MAP = {
    "indian":               "promoter",
    "foreign":              "promoter",
    "institutionsforeign":  "fii",
    "institutionsdomestic": "dii",
    "goverments":           "govt",
    "noninstitutions":      "public",
}

FIELDS = ["promoter", "fii", "dii", "govt", "public"]


def get_field(catagory, name):

    # Primary: catagory-based (most reliable)
    field = CATEGORY_MAP.get(catagory.lower().strip())

    if field:
        return field

    # Fallback: name-based (catches API variants)
    name_l = name.lower().strip()

    if "promoter" in name_l:
        return "promoter"

    if "foreign" in name_l and "institution" in name_l:
        return "fii"

    if "domestic" in name_l and "institution" in name_l:
        return "dii"

    if "government" in name_l or "govt" in name_l:
        return "govt"

    if "non-institution" in name_l or "noninstitution" in name_l:
        return "public"

    return None


# ─────────────────────────────────────────────
# Shareholding Parser
# ─────────────────────────────────────────────

def parse_shareholding(symbol, data):

    rows = data.get("rows") or []
    cols = data.get("columns") or []   # all quarters, no cap

    if not rows or not cols:
        return {
            "updated": datetime.now().strftime("%Y-%m-%d"),
            "data": []
        }

    # Per-quarter accumulator
    # Using += so Promoter Indian + Promoter Foreign both add correctly
    quarter_data = {
        q: {f: 0.0 for f in FIELDS}
        for q in cols
    }

    for row in rows:

        cat   = row.get("catagory", "")
        name  = row.get("name",     "")
        field = get_field(cat, name)

        if not field:
            continue

        d = row.get("data") or {}

        for q in cols:

            val = d.get(q)

            if val is None:
                continue

            try:
                quarter_data[q][field] += float(val)
            except Exception:
                pass

    # Build structured output
    structured = []

    for q in cols:

        entry = {"quarter": q}

        for f in FIELDS:
            entry[f] = round(quarter_data[q][f], 2)

        structured.append(entry)

    # Validation: total should be ~100%
    if structured:

        latest = structured[0]
        total  = sum(latest[f] for f in FIELDS)

        if abs(total - 100) > 5:
            print(
                f"  ⚠  BAD TOTAL {symbol} = {total:.1f}% | "
                f"P:{latest['promoter']} F:{latest['fii']} "
                f"D:{latest['dii']} G:{latest['govt']} Pub:{latest['public']}"
            )

    return {
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "data": structured
    }


# ─────────────────────────────────────────────
# Fetch One
# ─────────────────────────────────────────────

async def fetch_one(client, sem, symbol):

    data = await finedge_get(
        client,
        sem,
        f"shareholdings/pattern/{symbol}",
        {"period": "quarterly"}
    )

    if not data:
        return symbol, None   # None = API failed, preserve existing

    try:
        parsed = parse_shareholding(symbol, data)
        return symbol, parsed

    except Exception as e:
        print(f"  Parse Error {symbol}: {e}")
        return symbol, None   # None = parse failed, preserve existing


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        # ── Download master list ──────────────
        master = await r2_download(client, "master.json")

        symbols = [
            x["symbol"]
            for x in master
            if x.get("exchange") == "NSE"
        ]

        BAD_KEYWORDS = [
            "ETF", "LIQUID", "NIFTY", "GOLD",
            "SILVER", "NEXT50", "MIDCAP", "SMALLCAP",
        ]

        symbols = [
            s for s in symbols
            if not any(k in s for k in BAD_KEYWORDS)
        ]

        print(f"Total symbols: {len(symbols)}")

        # ── Download existing data (for preservation) ──
        try:
            existing = await r2_download(client, "shareholding.json")
            print(f"Existing data: {len(existing)} symbols")
        except Exception:
            existing = {}
            print("No existing shareholding.json found — fresh run")

        # ── Fetch all ────────────────────────
        sem    = asyncio.Semaphore(CONCURRENCY)
        output = {}
        total  = len(symbols)

        failed    = []   # symbols to retry
        preserved = 0
        updated   = 0
        empty     = 0

        for i in range(0, total, 25):

            batch = symbols[i:i + 25]

            tasks = [
                fetch_one(client, sem, s)
                for s in batch
            ]

            results = await asyncio.gather(*tasks)

            for sym, data in results:

                if data and data.get("data"):
                    # Fresh data successfully fetched
                    output[sym] = data
                    updated += 1
                    print(f"  ✓ {sym}")

                elif sym in existing and existing[sym].get("data"):
                    # API failed → preserve existing, schedule retry
                    output[sym] = existing[sym]
                    preserved += 1
                    failed.append(sym)
                    print(f"  ↺ {sym} | preserved — will retry")

                else:
                    # No data anywhere, schedule retry
                    output[sym] = {
                        "updated": datetime.now().strftime("%Y-%m-%d"),
                        "data": []
                    }
                    empty += 1
                    failed.append(sym)
                    print(f"  • {sym} | no data — will retry")

            print(f"── {min(i + 25, total)}/{total} done ──")

        # ── Retry Pass ───────────────────────
        retry_ok = 0

        if failed:

            print(f"\n🔄 Retrying {len(failed)} failed symbols...")
            await asyncio.sleep(5)

            for i in range(0, len(failed), 25):

                batch = failed[i:i + 25]

                tasks = [
                    fetch_one(client, sem, s)
                    for s in batch
                ]

                results = await asyncio.gather(*tasks)

                for sym, data in results:

                    if data and data.get("data"):
                        output[sym] = data
                        retry_ok += 1
                        print(f"  ✓ {sym} (retry ok)")
                    else:
                        print(f"  ✗ {sym} (retry failed — keeping existing)")

            print(f"  Retry recovered: {retry_ok}/{len(failed)}")

        # ── Upload ───────────────────────────
        manifest = await upload_with_manifest(
            client, r2_upload, "shareholding.json", output,
            schema_v=1, extra_meta={"symbol_count": len(output)}
        )

        print(
            f"\n✅ shareholding.json uploaded (hash={manifest['hash']})\n"
            f"   Updated:   {updated + retry_ok}\n"
            f"   Preserved: {preserved + empty - retry_ok}\n"
            f"   Empty:     {empty}\n"
            f"   Retry OK:  {retry_ok}/{len(failed)}\n"
            f"   Total:     {len(output)}"
        )


if __name__ == "__main__":
    asyncio.run(main())
