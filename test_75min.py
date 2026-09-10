#!/usr/bin/env python3
"""
test_75min.py — Standalone diagnostic for Upstox's V3 custom-interval API.

Checks whether `minutes/75` is accepted directly, and if so, whether the
returned bars are aligned to market open (9:15 IST) as expected for a
75-min timeframe (5 bars/day: 9:15-10:30, 10:30-11:45, 11:45-13:00,
13:00-14:15, 14:15-15:30).

Does NOT touch R2 or any of the main pipeline's state — pure read-only
diagnostic. Run it exactly like the main pipeline (same UPSTOX_TOKEN env
var / GitHub Actions secret), e.g. as an extra step in the same workflow,
or a one-off manual workflow_dispatch job.

Usage:
  python test_75min.py                 # today's intraday 75-min bars
  python test_75min.py historical      # a real trading day, via the
                                        # historical (not intraday) V3 endpoint
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, timedelta

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]

# RELIANCE — same instrument_key Upstox's own docs use as their example.
# Swap this for any NSE_EQ instrument_key you want to test with instead.
TEST_INSTRUMENT = "NSE_EQ|INE002A01018"
TEST_LABEL = "RELIANCE"


def _upstox_headers():
    return {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_TOKEN}"}


def _pretty_candles(candles, limit=10):
    """candles: list of [timestamp, open, high, low, close, volume, oi]"""
    lines = []
    for row in candles[:limit]:
        ts = row[0]
        lines.append(f"    {ts}   O:{row[1]:<10} H:{row[2]:<10} L:{row[3]:<10} C:{row[4]:<10} V:{row[5]}")
    if len(candles) > limit:
        lines.append(f"    … and {len(candles) - limit} more")
    return "\n".join(lines)


async def test_intraday_75m(client):
    """V3 intraday endpoint — today's session only."""
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{TEST_INSTRUMENT}/minutes/75"
    log.info(f"━━━ Testing INTRADAY 75-min: {TEST_LABEL} ━━━")
    log.info(f"GET {url}")
    r = await client.get(url, headers=_upstox_headers(), timeout=30)
    log.info(f"Status: {r.status_code}")
    if r.status_code != 200:
        log.error(f"❌ Failed. Response body:\n{r.text[:1000]}")
        return None
    payload = r.json()
    log.info(f"Response status field: {payload.get('status')}")
    candles = (payload.get("data") or {}).get("candles") or []
    log.info(f"✓ Got {len(candles)} candle(s) today")
    if candles:
        log.info("Candles (most recent first, as Upstox returns them):")
        log.info(_pretty_candles(candles))
        log.info("")
        log.info("⚠  CHECK: do the timestamps align to 9:15, 10:30, 11:45, 13:00, 14:15 IST?")
        log.info("   If they land on different minute-marks, the API is NOT honoring a")
        log.info("   9:15-anchored 75-min grid — you'd need to fetch a finer interval")
        log.info("   (e.g. 5-min) and resample yourself instead of trusting this directly.")
    return payload


async def test_historical_75m(client, days_back=35):
    """V3 historical endpoint — a completed past trading day (avoids any
    'today's session still in progress' ambiguity in the intraday check)."""
    to_date = date.today().isoformat()
    from_date = (date.today() - timedelta(days=days_back)).isoformat()
    url = f"https://api.upstox.com/v3/historical-candle/{TEST_INSTRUMENT}/minutes/75/{to_date}/{from_date}"
    log.info(f"━━━ Testing HISTORICAL 75-min: {TEST_LABEL}  ({from_date} → {to_date}) ━━━")
    log.info(f"GET {url}")
    r = await client.get(url, headers=_upstox_headers(), timeout=30)
    log.info(f"Status: {r.status_code}")
    if r.status_code != 200:
        log.error(f"❌ Failed. Response body:\n{r.text[:1000]}")
        return None
    payload = r.json()
    candles = (payload.get("data") or {}).get("candles") or []
    log.info(f"✓ Got {len(candles)} candle(s) over {days_back} calendar days")
    if candles:
        log.info("Candles (most recent first, as Upstox returns them):")
        log.info(_pretty_candles(candles, limit=15))
        # Rough sanity check: with 5 bars/trading day expected, days_back=5
        # calendar days should be ~3-4 trading days -> ~15-20 candles.
        # A count far off that suggests either a different interval was
        # silently substituted, or non-9:15-anchored bars are being cut
        # differently than expected.
        log.info("")
        log.info(f"Expected ballpark: ~5 bars/trading day. Got {len(candles)} over ~{days_back} calendar days.")
    return payload


async def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "intraday"
    async with httpx.AsyncClient() as client:
        if mode == "historical":
            await test_historical_75m(client)
        else:
            await test_intraday_75m(client)
    log.info("━━━ Done. Review the candle timestamps above manually. ━━━")


if __name__ == "__main__":
    asyncio.run(main())
