#!/usr/bin/env python3
"""
pipeline_patterns.py — Standalone chart-pattern scanner (GitHub Actions)

Detects classic technical chart patterns using pivot/zigzag analysis:
  - Head & Shoulders / Inverse Head & Shoulders
  - Double Top / Double Bottom

Reads the SAME daily OHLC chunks the main pipeline.py already maintains
(ohlc_1.json..ohlc_8.json) — this is a read-only consumer of that data,
never writes back to it. Writes its own output: pattern_scan_v2.json.

Design note: reuses the zigzag pivot-detection approach already proven
in pipeline.py's VCP scanner (_vcp_zigzag_close_pct) — a percentage-
threshold zigzag on CLOSING prices is far less noisy than using raw
intrabar highs/lows to decide *when* a swing reverses, then the actual
pivot price is the true extreme reached within that confirmed swing.
Pattern logic itself follows the classic point-naming (A/B/C/D/E) used
in most chart-pattern literature and open-source scanners researched
for this (e.g. BennyThadikaran/stock-pattern's documented approach).

Usage:
  python pipeline_patterns.py scan     # run the scan, upload results
  python pipeline_patterns.py status   # quick summary of last run
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from r2_manifest import upload_str_with_manifest

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

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

R2_CHUNKS = 8

MIN_LIQUIDITY_TURNOVER = 3_00_00_000  # ₹3 Cr avg daily turnover -- same
                                        # bar as the main pipeline's scanners


def today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════
# R2 HELPERS
# ══════════════════════════════════════════════════════════════

async def r2_upload(client, filename, data):
    if isinstance(data, str): data = data.encode()
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(url, headers={**WORKER_HEADERS, "Content-Type": "application/json"}, content=data, timeout=90)
    if r.status_code != 200: raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")

async def r2_download(client, filename):
    url = f"{WORKER_URL}/{filename}"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404: return None
    if r.status_code != 200: raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↓ {filename} ({len(r.content)/1024:.0f} KB)")
    return r.json()

async def download_all_ohlc(client) -> dict:
    tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception): log.warning(f"  ohlc_{i+1}.json error: {res}")
        elif res and "stocks" in res: all_data.update(res["stocks"])
    log.info(f"Loaded {len(all_data)} stocks from daily OHLC")
    return all_data


# ══════════════════════════════════════════════════════════════
# LIQUIDITY FILTER — same bar as the main pipeline's other scanners
# ══════════════════════════════════════════════════════════════

def _check_liquidity(volumes, closes, n, min_turnover=MIN_LIQUIDITY_TURNOVER):
    lookback = min(50, n)
    if lookback < 20: return True
    vols = [v for v in volumes[-lookback:] if v is not None]
    prices = [c for c in closes[-lookback:] if c is not None and c > 0]
    if len(vols) < 20 or len(prices) < 20: return False
    return (sum(vols) / len(vols) * sum(prices) / len(prices)) >= min_turnover


# ══════════════════════════════════════════════════════════════
# ZIGZAG PIVOTS — same approach as pipeline.py's VCP scanner
# (_vcp_zigzag_pct / _vcp_zigzag_close_pct), copied here so this
# pipeline stays fully standalone (no cross-file imports needed).
# ══════════════════════════════════════════════════════════════

def _zigzag_pct(highs, lows, pct_threshold=0.04):
    """Percentage-based ZigZag on raw highs/lows -- a pivot (H or L) only
    confirms once price has reversed by pct_threshold from the running
    extreme since the last confirmed pivot."""
    n = len(highs)
    if n < 2: return []
    piv = []
    ext_high = highs[0]; ext_high_idx = 0
    ext_low = lows[0]; ext_low_idx = 0
    direction = None
    for i in range(1, n):
        h, l = highs[i], lows[i]
        if h is None or l is None: continue
        if ext_high is None or h > ext_high: ext_high, ext_high_idx = h, i
        if ext_low is None or l < ext_low: ext_low, ext_low_idx = l, i
        if direction is None:
            if ext_high is not None and l <= ext_high * (1 - pct_threshold):
                piv.append((ext_high_idx, ext_high, "H", i)); direction = "down"; ext_low, ext_low_idx = l, i
            elif ext_low is not None and h >= ext_low * (1 + pct_threshold):
                piv.append((ext_low_idx, ext_low, "L", i)); direction = "up"; ext_high, ext_high_idx = h, i
        elif direction == "up":
            if l <= ext_high * (1 - pct_threshold):
                piv.append((ext_high_idx, ext_high, "H", i)); direction = "down"; ext_low, ext_low_idx = l, i
        else:
            if h >= ext_low * (1 + pct_threshold):
                piv.append((ext_low_idx, ext_low, "L", i)); direction = "up"; ext_high, ext_high_idx = h, i
    return piv

def _zigzag_close_pct(highs, lows, closes, pct_threshold=0.04):
    """Same idea, but uses CLOSING prices to decide WHEN a swing reverses
    (far less noisy than raw intrabar wicks) -- then reports the TRUE
    extreme (highest high / lowest low) reached within that confirmed
    swing's span as the pivot's actual price."""
    n = len(closes)
    if n < 2: return []
    close_piv = _zigzag_pct(closes, closes, pct_threshold)
    if not close_piv: return []
    piv = []
    span_start = 0
    for idx, _price, kind, confirm_idx in close_piv:
        scan_end = confirm_idx
        seg = highs[span_start:scan_end+1] if kind == "H" else lows[span_start:scan_end+1]
        vals = [(span_start + off, v) for off, v in enumerate(seg) if v is not None]
        if vals:
            true_idx, true_price = (max(vals, key=lambda x: x[1]) if kind == "H" else min(vals, key=lambda x: x[1]))
            piv.append((true_idx, true_price, kind))
            span_start = true_idx + 1
        else:
            span_start = idx + 1
    return piv


# ══════════════════════════════════════════════════════════════
# HEAD & SHOULDERS  /  INVERSE HEAD & SHOULDERS
#
# Classic point naming: A (shoulder1) - B (neckline pt 1) - C (head) -
# D (neckline pt 2) - E (shoulder2). Confirmed once a later close
# breaks the neckline (the line through B and D).
# ══════════════════════════════════════════════════════════════

def _detect_head_shoulders(hist, lookback=180, zigzag_pct=0.04,
                             shoulder_symmetry_pct=0.12, min_head_prominence_pct=0.03,
                             max_confirm_bars=15):
    """Looks for a completed H&S (bearish) or Inverse H&S (bullish) within
    the last `lookback` bars, confirmed by a neckline break within
    `max_confirm_bars` of the second shoulder. Returns at most one
    (the most recent) match, or None."""
    highs = hist.get("h") or []; lows = hist.get("l") or []
    closes = hist.get("c") or []; dates = hist.get("d") or []
    n = len(closes)
    if n < 60: return None

    lb = min(lookback, n)
    start = n - lb
    piv = _zigzag_close_pct(highs[start:], lows[start:], closes[start:], zigzag_pct)
    piv = [(i + start, p, k) for (i, p, k) in piv]
    if len(piv) < 5: return None

    # Walk backward through the pivot chain looking for a valid H-L-H-L-H
    # quintuple (bearish H&S) or L-H-L-H-L quintuple (inverse), most
    # recent first.
    for end in range(len(piv) - 1, 3, -1):
        window = piv[end-4:end+1]
        kinds = [p[2] for p in window]
        if kinds == ["H", "L", "H", "L", "H"]:
            (a_i, a_p, _), (b_i, b_p, _), (c_i, c_p, _), (d_i, d_p, _), (e_i, e_p, _) = window
            # Head must be the tallest of the three peaks
            if not (c_p > a_p and c_p > e_p): continue
            if (c_p - max(a_p, e_p)) / c_p < min_head_prominence_pct: continue
            # Shoulders roughly level with each other
            if abs(a_p - e_p) / max(a_p, e_p) > shoulder_symmetry_pct: continue
            neckline_avg = (b_p + d_p) / 2
            confirm_end = min(n, e_i + 1 + max_confirm_bars)
            confirm_idx = next((j for j in range(e_i + 1, confirm_end) if closes[j] is not None and closes[j] < neckline_avg), None)
            if confirm_idx is None: continue
            return {
                "pattern": "Head & Shoulders", "direction": "bearish",
                "left_shoulder": {"date": dates[a_i], "price": round(a_p, 2)},
                "head": {"date": dates[c_i], "price": round(c_p, 2)},
                "right_shoulder": {"date": dates[e_i], "price": round(e_p, 2)},
                "neckline": round(neckline_avg, 2),
                "confirmed_date": dates[confirm_idx], "confirmed_close": round(closes[confirm_idx], 2),
                "target": round(neckline_avg - (c_p - neckline_avg), 2),
            }
        if kinds == ["L", "H", "L", "H", "L"]:
            (a_i, a_p, _), (b_i, b_p, _), (c_i, c_p, _), (d_i, d_p, _), (e_i, e_p, _) = window
            if not (c_p < a_p and c_p < e_p): continue
            if (min(a_p, e_p) - c_p) / c_p < min_head_prominence_pct: continue
            if abs(a_p - e_p) / max(a_p, e_p) > shoulder_symmetry_pct: continue
            neckline_avg = (b_p + d_p) / 2
            confirm_end = min(n, e_i + 1 + max_confirm_bars)
            confirm_idx = next((j for j in range(e_i + 1, confirm_end) if closes[j] is not None and closes[j] > neckline_avg), None)
            if confirm_idx is None: continue
            return {
                "pattern": "Inverse Head & Shoulders", "direction": "bullish",
                "left_shoulder": {"date": dates[a_i], "price": round(a_p, 2)},
                "head": {"date": dates[c_i], "price": round(c_p, 2)},
                "right_shoulder": {"date": dates[e_i], "price": round(e_p, 2)},
                "neckline": round(neckline_avg, 2),
                "confirmed_date": dates[confirm_idx], "confirmed_close": round(closes[confirm_idx], 2),
                "target": round(neckline_avg + (neckline_avg - c_p), 2),
            }
    return None


# ══════════════════════════════════════════════════════════════
# DOUBLE TOP  /  DOUBLE BOTTOM
#
# Two peaks (or troughs) at a similar level, with a confirming trough
# (or peak) between them. Confirmed once a later close breaks that
# middle pivot.
# ══════════════════════════════════════════════════════════════

def _detect_double_top_bottom(hist, lookback=150, zigzag_pct=0.04,
                                peak_symmetry_pct=0.03, min_middle_retrace_pct=0.05,
                                max_confirm_bars=15):
    highs = hist.get("h") or []; lows = hist.get("l") or []
    closes = hist.get("c") or []; dates = hist.get("d") or []
    n = len(closes)
    if n < 40: return None

    lb = min(lookback, n)
    start = n - lb
    piv = _zigzag_close_pct(highs[start:], lows[start:], closes[start:], zigzag_pct)
    piv = [(i + start, p, k) for (i, p, k) in piv]
    if len(piv) < 3: return None

    for end in range(len(piv) - 1, 1, -1):
        window = piv[end-2:end+1]
        kinds = [p[2] for p in window]
        if kinds == ["H", "L", "H"]:
            (a_i, a_p, _), (b_i, b_p, _), (c_i, c_p, _) = window
            if abs(a_p - c_p) / max(a_p, c_p) > peak_symmetry_pct: continue
            if (min(a_p, c_p) - b_p) / min(a_p, c_p) < min_middle_retrace_pct: continue
            confirm_end = min(n, c_i + 1 + max_confirm_bars)
            confirm_idx = next((j for j in range(c_i + 1, confirm_end) if closes[j] is not None and closes[j] < b_p), None)
            if confirm_idx is None: continue
            return {
                "pattern": "Double Top", "direction": "bearish",
                "first_top": {"date": dates[a_i], "price": round(a_p, 2)},
                "trough": {"date": dates[b_i], "price": round(b_p, 2)},
                "second_top": {"date": dates[c_i], "price": round(c_p, 2)},
                "confirmed_date": dates[confirm_idx], "confirmed_close": round(closes[confirm_idx], 2),
                "target": round(b_p - (max(a_p, c_p) - b_p), 2),
            }
        if kinds == ["L", "H", "L"]:
            (a_i, a_p, _), (b_i, b_p, _), (c_i, c_p, _) = window
            if abs(a_p - c_p) / max(a_p, c_p) > peak_symmetry_pct: continue
            if (b_p - max(a_p, c_p)) / max(a_p, c_p) < min_middle_retrace_pct: continue
            confirm_end = min(n, c_i + 1 + max_confirm_bars)
            confirm_idx = next((j for j in range(c_i + 1, confirm_end) if closes[j] is not None and closes[j] > b_p), None)
            if confirm_idx is None: continue
            return {
                "pattern": "Double Bottom", "direction": "bullish",
                "first_bottom": {"date": dates[a_i], "price": round(a_p, 2)},
                "peak": {"date": dates[b_i], "price": round(b_p, 2)},
                "second_bottom": {"date": dates[c_i], "price": round(c_p, 2)},
                "confirmed_date": dates[confirm_idx], "confirmed_close": round(closes[confirm_idx], 2),
                "target": round(b_p + (b_p - min(a_p, c_p)), 2),
            }
    return None


# ══════════════════════════════════════════════════════════════
# PIPELINE MODES
# ══════════════════════════════════════════════════════════════

RESULT_KEEP_DAYS = 30  # how many days a confirmed pattern stays listed

async def run_scan() -> None:
    status = PipelineStatus("pattern_scan_v2")
    try:
        today = today_ist()
        log.info(f"━━━ Chart Pattern Scan  {today} ━━━")
        async with httpx.AsyncClient() as client:
            all_data = await download_all_ohlc(client)

            hs_signals = []
            dt_signals = []
            for sym, s in all_data.items():
                closes, volumes = s.get("c") or [], s.get("v") or []
                n = len(closes)
                if n < 60 or not _check_liquidity(volumes, closes, n):
                    continue
                hs = _detect_head_shoulders(s)
                if hs:
                    hs_signals.append({"symbol": sym, **hs})
                dt = _detect_double_top_bottom(s)
                if dt:
                    dt_signals.append({"symbol": sym, **dt})

            # Keep only patterns confirmed within the retention window --
            # older ones drop off automatically on the next run.
            cutoff = (date.fromisoformat(today) - timedelta(days=RESULT_KEEP_DAYS)).isoformat()
            hs_signals = [s for s in hs_signals if s["confirmed_date"] >= cutoff]
            dt_signals = [s for s in dt_signals if s["confirmed_date"] >= cutoff]

            hs_signals.sort(key=lambda x: x["confirmed_date"], reverse=True)
            dt_signals.sort(key=lambda x: x["confirmed_date"], reverse=True)

            log.info(f"Head & Shoulders / Inverse: {len(hs_signals)}")
            log.info(f"Double Top / Double Bottom: {len(dt_signals)}")

            payload = {
                "updated": today,
                "head_shoulders": hs_signals,
                "double_top_bottom": dt_signals,
            }
            await upload_str_with_manifest(
                client, r2_upload, "pattern_scan_v2.json", json.dumps(payload),
                schema_v=1, extra_meta={"hs_count": len(hs_signals), "dt_count": len(dt_signals)}
            )
        status.success()
        log.info("━━━ Chart Pattern Scan complete ━━━")
    except Exception as e:
        status.failure(e)


async def run_status() -> None:
    async with httpx.AsyncClient() as client:
        data = await r2_download(client, "pattern_scan_v2.json")
    if not data:
        print("\nNo pattern_scan_v2.json found — run 'scan' first.\n")
        return
    print(f"\nUpdated: {data.get('updated')}")
    print(f"Head & Shoulders / Inverse: {len(data.get('head_shoulders', []))}")
    for s in data.get("head_shoulders", [])[:10]:
        print(f"  {s['symbol']:<12} {s['pattern']:<24} confirmed {s['confirmed_date']}  target {s['target']}")
    print(f"\nDouble Top / Double Bottom: {len(data.get('double_top_bottom', []))}")
    for s in data.get("double_top_bottom", [])[:10]:
        print(f"  {s['symbol']:<12} {s['pattern']:<14} confirmed {s['confirmed_date']}  target {s['target']}")
    print()


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "scan":   asyncio.run(run_scan())
        case "status": asyncio.run(run_status())
        case _:
            print(__doc__)
            sys.exit(1)
