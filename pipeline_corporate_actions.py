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
    "Bonus",
    "Split",
    "Rights",
    "Buyback",
    "Merger",
    "Demerger",
}


def normalize_type(v):

    v = str(v).strip().lower()

    return TYPE_MAP.get(v, v.title())


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
# Fetch Actions
# ─────────────────────────────────────────────

async def fetch_actions(client):

    today = datetime.now().date()

    from_date = (
        today - timedelta(days=30)
    ).strftime("%d-%b-%Y")

    to_date = (
        today + timedelta(days=90)
    ).strftime("%d-%b-%Y")

    url = f"{FINEDGE_BASE}/corporate-actions/all"

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


# ─────────────────────────────────────────────
# Parse
# ─────────────────────────────────────────────

def parse_actions(rows):

    output = {}

    for row in rows:

        symbol = str(
            row.get("symbol", "")
        ).strip().upper()

        if not symbol:
            continue

        action_raw = row.get("action", "")

        action_type = normalize_type(
            action_raw
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

    return output


# ─────────────────────────────────────────────
# Merge History
# ─────────────────────────────────────────────

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
# Main
# ─────────────────────────────────────────────

async def main():

    async with httpx.AsyncClient() as client:

        print("Fetching corporate actions...")

        rows = await fetch_actions(client)

        parsed = parse_actions(rows)

        # Upcoming File
        await r2_upload(
            client,
            "corporate_actions_upcoming.json",
            parsed
        )

        print(
            "✅ corporate_actions_upcoming.json uploaded"
        )

        # Existing History
        history = await r2_download(
            client,
            "corporate_actions_history.json"
        )

        # Merge
        merged = merge_history(
            history,
            parsed
        )

        # Upload History
        await r2_upload(
            client,
            "corporate_actions_history.json",
            merged
        )

        print(
            "✅ corporate_actions_history.json uploaded"
        )


if __name__ == "__main__":
    asyncio.run(main())
