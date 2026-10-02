#!/usr/bin/env python3
"""
NSE OHLC + Fundamentals Pipeline — GitHub Actions
OHLC source: Upstox API (adjusted prices, TV-matching)
Fundamentals source: Finedge API

Usage:
  python pipeline.py daily
  python pipeline.py today
  python pipeline.py full
  python pipeline.py status
  python pipeline.py fund_daily
  python pipeline.py fund_full
  python pipeline.py fund_full_1..10
  python pipeline.py finedge_daily   # standalone Finedge OHLC snapshot → own R2 history
  python pipeline.py ep_scan
  python pipeline.py hlr_scan
  python pipeline.py pattern_scan
  python pipeline.py pattern_scan_force   # bypass trading-day gate (use after holiday-list fixes)
  python pipeline.py candle_scan          # candlestick patterns -> candle_patterns.json + candle_pattern_stats.json
  python pipeline.py candle_scan_force    # same, bypassing the trading-day gate
  python pipeline.py home_ticker          # home page ticker -> home_ticker.json (run after all scans)
  python pipeline.py vcp_scan
  python pipeline.py stage2_scan
  python pipeline.py minervini_scan   # full 8-point Trend Template (stage2 + RS Rating >= 70)
  python pipeline.py weinstein_scan   # original Weinstein 4-stage analysis (weekly SMA30)
  python pipeline.py weinstein_scan_dryrun   # same, but prints real symbol names/stages to log, no R2 writes
  python pipeline.py weinstein_debug SYMBOL  # week-by-week close/ema30/slope/stage for ONE symbol, no R2 writes
  python pipeline.py ath_backfill   # one-time All-Time-High full-history backfill (see ATH section)
  python pipeline.py ath_reset      # wipes ath_data.json -- use once to recover from pre-fix seeding, then re-run ath_backfill
"""

import asyncio
import bisect
import calendar
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from r2_manifest import upload_str_with_manifest

# ── Telegram notify ──
try:
    from telegram_notify import PipelineStatus
except ImportError:
    class PipelineStatus:
        def __init__(self, name): self.name = name
        def add(self, *a, **k): pass
        def set(self, *a, **k): pass
        def warn(self, msg): pass
        def success(self, *a, **k): pass
        def failure(self, exc, reraise=True, **k):
            if reraise: raise exc

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

UPSTOX_TOKEN  = os.environ["UPSTOX_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]
FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]

UPSTOX_BASE  = "https://api.upstox.com/v2"
FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

ROLLING_DAYS       = 1100
R2_CHUNKS          = 8
CONCURRENCY        = 5
RATE_DELAY         = 0.4
RETRY              = 5
FUND_CONCURRENCY   = 4
FINEDGE_DELAY      = 0.25

# ── Finedge daily-OHLC (standalone system — does NOT touch Upstox OHLC) ──
FINEDGE_QUOTE_URL      = "https://data.finedgeapi.com/api/v2/quote"
FINEDGE_OHLC_BATCH     = 100   # max symbols per call (non-premium limit)
FINEDGE_OHLC_CONCURRENCY = 3
FINEDGE_OHLC_DELAY     = 0.3
FINEDGE_OHLC_CHUNKS    = 4     # separate chunk count from Upstox R2_CHUNKS

HERE = Path(__file__).parent

with open(HERE / "nse_holidays.json") as f:
    NSE_HOLIDAYS: set[str] = set(json.load(f))

WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

def _upstox_headers():
    return {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_TOKEN}"}
