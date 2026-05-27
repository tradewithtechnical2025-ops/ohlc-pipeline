#!/usr/bin/env python3

import asyncio
import json
import os
from datetime import datetime, timedelta

import httpx

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

TYPE_MAP = {

    "dividend": "Dividend",
    "final dividend": "Final Dividend",
    "interim dividend": "Interim Dividend",
    "special dividend": "Special Dividend",

    "bonus": "Bonus",
    "bonus issue": "Bonus",

    "split": "Split",
    "stock split": "Split",

    "rights": "Rights",
    "rights issue": "Rights",

    "buyback": "Buyback",

    "merger": "Merger",
    "demerger": "Demerger",
}


VALID_TYPES = {
    "Dividend",
    "Final Dividend",
    "Interim Dividend",
    "Special Dividend",
    "Bonus",
    "Split",
    "Rights",
    "Buyback",
    "Merger",
    "Demerger",
}


KEYWORDS = [
    "presentation",
    "transcript",
    "concall",
    "conference call",
    "earnings call",
    "analyst meet",
    "investor meet",
]


def normalize_type(v):

    v = str(v).strip().lower()

    return TYPE_MAP.get(v, v.title())


def clean_float(v):

    try:
        return float(
            str(v).replace(",", "")
        )
    except:
        return 0


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
        return {}

    try:
        return r.json()
    except:
        return {}


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
# Corporate Actions
# ─────────────────────────────────────────────

async def fetch_corporate_actions(client):

    today = datetime.now().date()

    start = today - timedelta(days=30)
    end   = today + timedelta(days=90)

    all_rows = []

    current = start

    while current < end:

        chunk_end = min(
            current + timedelta(days=29),
            end
        )

        from_date = current.strftime(
            "%d-%b-%Y"
        )

        to_date = chunk_end.strftime(
            "%d-%b-%Y"
        )

        print(
            f"[CA] {from_date} -> {to_date}"
        )

        url = f"{FINEDGE_BASE}/corporate-actions/all"

        params = {
            "from_date": from_date,
            "to_date": to_date,
            "token": FINEDGE_TOKEN,
        }

        try:

            r = await client.get(
                url,
                params=params,
                timeout=120,
            )

            if r.status_code != 200:

                print(
                    f"[CA] Failed {from_date} -> {to_date}"
                )

                current = chunk_end + timedelta(days=1)
                continue

            data = r.json()

            if isinstance(data, list):

                all_rows.extend(data)

        except Exception as e:

            print(
                f"[CA] Error: {e}"
            )

        current = chunk_end + timedelta(days=1)

    return all_rows


def parse_corporate_actions(rows):

    output = {}

    for row in rows:

        symbol = str(
            row.get("symbol", "")
        ).strip().upper()

        if not symbol:
            continue

        if symbol.isdigit():
            continue

        action_type = normalize_type(
            row.get("action", "")
        )

        if action_type not in VALID_TYPES:
            continue

        item = {

            "date": row.get("ex_date"),

            "timestamp": row.get(
                "timestamp_unix"
            ),

            "type": action_type,

            "sub_type": normalize_type(
                row.get("dividend_type", "")
            ),

            "value": row.get("amount"),

            "text": row.get("subject"),
        }

        output.setdefault(symbol, [])
        output[symbol].append(item)

    for symbol in output:

        output[symbol].sort(
            key=lambda x: x["timestamp"]
        )

    return output


def merge_history(old, new):

    merged = old.copy()

    for symbol, actions in new.items():

        merged.setdefault(symbol, [])

        existing = {

            (
                x.get("date"),
                x.get("type"),
                x.get("text"),
            )

            for x in merged[symbol]
        }

        for item in actions:

            key = (
                item.get("date"),
                item.get("type"),
                item.get("text"),
            )

            if key not in existing:

                merged[symbol].append(item)

    return merged


# ─────────────────────────────────────────────
# Results Calendar
# ─────────────────────────────────────────────

async def fetch_results_calendar(client):

    today = datetime.now().date()

    from_date = (
        today - timedelta(days=7)
    ).strftime("%Y-%m-%d")

    to_date = (
        today + timedelta(days=30)
    ).strftime("%Y-%m-%d")

    url = f"{FINEDGE_BASE}/results-calendar"

    params = {
        "from_date": from_date,
        "to_date": to_date,
        "token": FINEDGE_TOKEN,
    }

    r = await client.get(
        url,
        params=params,
        timeout=120,
    )

    r.raise_for_status()

    return r.json()


def parse_results_calendar(rows):

    output = {}

    for row in rows:

        symbol = str(
            row.get("symbol", "")
        ).strip().upper()

        if not symbol:
            continue

        if symbol.isdigit():
            continue

        output[symbol] = {

            "name": row.get(
                "company_name"
            ),

            "date": row.get(
                "expected_result_date"
            )
        }

    return output


# ─────────────────────────────────────────────
# IPO Calendar
# ─────────────────────────────────────────────

async def fetch_ipo_calendar(client):

    today = datetime.now().date()

    from_date = (
        today - timedelta(days=7)
    ).strftime("%Y-%m-%d")

    to_date = (
        today + timedelta(days=30)
    ).strftime("%Y-%m-%d")

    url = f"{FINEDGE_BASE}/ipo-calendar"

    params = {
        "from_date": from_date,
        "to_date": to_date,
        "token": FINEDGE_TOKEN,
    }

    r = await client.get(
        url,
        params=params,
        timeout=120,
    )

    r.raise_for_status()

    data = r.json()

    return data.get("data", [])


def parse_ipo_calendar(rows):

    output = {}

    for row in rows:

        symbol = str(
            row.get("symbol", "")
        ).strip().upper()

        if not symbol:
            continue

        output[symbol] = {

            "name": row.get(
                "company_name"
            ),

            "status": row.get(
                "ipo_status"
            ),

            "start_date": row.get(
                "start_date"
            ),

            "end_date": row.get(
                "end_date"
            ),

            "price_range": row.get(
                "price_range"
            ),

            "issue_size": row.get(
                "issue_size"
            ),

            "subscription": clean_float(
                row.get("subscription")
            ),

            "security_type": row.get(
                "security_type"
            ),

            "exchange": row.get(
                "exchange"
            ),
        }

    return output


# ─────────────────────────────────────────────
# Filings
# ─────────────────────────────────────────────

async def fetch_filings_chunk(
    client,
    endpoint,
    from_date,
    to_date
):

    url = f"{FINEDGE_BASE}/{endpoint}"

    params = {
        "from_date": from_date,
        "to_date": to_date,
        "token": FINEDGE_TOKEN,
    }

    try:

        r = await client.get(
            url,
            params=params,
            timeout=120,
        )

        if r.status_code != 200:
            return []

        data = r.json()

        if isinstance(data, list):
            return data

        return []

    except:
        return []


async def fetch_filings(client):

    today = datetime.now().date()

    start = today - timedelta(days=30)
    end   = today

    all_rows = []

    current = start

    while current < end:

        chunk_end = min(
            current + timedelta(days=6),
            end
        )

        from_date = current.strftime(
            "%Y-%m-%d"
        )

        to_date = chunk_end.strftime(
            "%Y-%m-%d"
        )

        print(
            f"[FILINGS] {from_date} -> {to_date}"
        )

        pres = await fetch_filings_chunk(
            client,
            "investor-presentations",
            from_date,
            to_date
        )

        tran = await fetch_filings_chunk(
            client,
            "investor-call-transcripts",
            from_date,
            to_date
        )

        all_rows.extend(pres)
        all_rows.extend(tran)

        current = chunk_end + timedelta(days=1)

    return all_rows


def detect_filing_type(text):

    text = text.lower()

    if "presentation" in text:
        return "Presentation"

    if "transcript" in text:
        return "Transcript"

    if "concall" in text:
        return "Transcript"

    if "conference call" in text:
        return "Transcript"

    if "earnings call" in text:
        return "Transcript"

    return "Filing"


def parse_filings(rows):

    output = {}

    for row in rows:

        symbol = str(
            row.get("stock_symbol", "")
        ).strip().upper()

        if not symbol:
            continue

        if symbol.isdigit():
            continue

        text = str(
            row.get("description", "")
        )

        text_l = text.lower()

        if not any(
            k in text_l
            for k in KEYWORDS
        ):
            continue

        item = {

            "date": row.get("ex_date"),

            "timestamp": row.get(
                "timestamp_unix"
            ),

            "type": detect_filing_type(
                text
            ),

            "title": text,

            "pdf": row.get(
                "pdf_file_link"
            ),
        }

        output.setdefault(symbol, [])
        output[symbol].append(item)

    return output


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        # ─────────────────────────
        # Corporate Actions
        # ─────────────────────────

        print(
            "\n=== Corporate Actions ==="
        )

        ca_rows = await fetch_corporate_actions(
            client
        )

        ca_parsed = parse_corporate_actions(
            ca_rows
        )

        await r2_upload(
            client,
            "corporate_actions_upcoming.json",
            ca_parsed
        )

        history = await r2_download(
            client,
            "corporate_actions_history.json"
        )

        merged = merge_history(
            history,
            ca_parsed
        )

        await r2_upload(
            client,
            "corporate_actions_history.json",
            merged
        )

        print(
            "✅ Corporate Actions Uploaded"
        )

        # ─────────────────────────
        # Results Calendar
        # ─────────────────────────

        print(
            "\n=== Results Calendar ==="
        )

        rc_rows = await fetch_results_calendar(
            client
        )

        rc_parsed = parse_results_calendar(
            rc_rows
        )

        await r2_upload(
            client,
            "results_calendar.json",
            rc_parsed
        )

        print(
            "✅ Results Calendar Uploaded"
        )

        # ─────────────────────────
        # IPO Calendar
        # ─────────────────────────

        print(
            "\n=== IPO Calendar ==="
        )

        ipo_rows = await fetch_ipo_calendar(
            client
        )

        ipo_parsed = parse_ipo_calendar(
            ipo_rows
        )

        await r2_upload(
            client,
            "ipo_calendar.json",
            ipo_parsed
        )

        print(
            "✅ IPO Calendar Uploaded"
        )

        # ─────────────────────────
        # Filings
        # ─────────────────────────

        print(
            "\n=== Filings ==="
        )

        filings_rows = await fetch_filings(
            client
        )

        filings_parsed = parse_filings(
            filings_rows
        )

        await r2_upload(
            client,
            "filings_index.json",
            filings_parsed
        )

        print(
            "✅ Filings Uploaded"
        )


if __name__ == "__main__":
    asyncio.run(main())
