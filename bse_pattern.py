#!/usr/bin/env python3
"""
BSE Pattern + Metrics Scan — BSE OHLC pe.

- Liquidity filter HATA diya (universe pehle hi mcap/price se filtered hai).
- Har stock pe metrics: 52WH, distance from 52WH (%), ATR%(14d).
- Patterns: Inside Bar, Double Inside Bar, NR7, MCP, Weekly IB/Double IB/NR7/Tight Close.

Input : bse_ohlc_1..N.json (R2)
Output: bse_pattern_signals.json (R2)  — {signals:[...], stocks:{sym:{metrics+patterns}}}

Usage: python bse_pattern.py
"""

import asyncio
import json
import os
import sys
from collections import Counter
from datetime import date as dt

import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

CHUNK_PREFIX = "bse_ohlc"
BSE_CHUNKS   = 2
OUT_FILE     = "bse_pattern_signals.json"
MIN_BARS     = 10

DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}


# ── R2 ──
async def r2_download(client, filename):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=120)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    return r.json()

async def r2_upload(client, filename, data):
    r = await client.post(f"{WORKER_URL}?file={filename}", headers=UP_HEADERS,
                          content=json.dumps(data).encode(), timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    print(f"  ↑ {filename}")

async def download_chunks(client) -> dict:
    tasks = [r2_download(client, f"{CHUNK_PREFIX}_{i+1}.json") for i in range(BSE_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"  {CHUNK_PREFIX}_{i+1}.json error: {res}")
        elif res and "stocks" in res:
            all_data.update(res["stocks"])
    print(f"Loaded {len(all_data)} BSE stocks")
    return all_data


# ── per-stock metrics: 52WH, distance from 52WH, ATR%(14) ──
def _metrics(s):
    highs = s["h"]; lows = s["l"]; closes = s["c"]; n = len(closes)
    ltp = closes[-1] if closes else None

    w52    = [v for v in highs[-252:] if v is not None]
    high52 = max(w52) if w52 else None
    dist52 = round((ltp - high52) / high52 * 100, 2) if (high52 and ltp) else None

    trs = []
    for i in range(max(1, n - 14), n):
        h = highs[i]; l = lows[i]; pc = closes[i-1]
        if None in (h, l, pc):
            continue
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr14   = sum(trs) / len(trs) if trs else None
    atr_pct = round(atr14 / ltp * 100, 2) if (atr14 and ltp) else None

    return ltp, high52, dist52, atr_pct


# ── weekly aggregation ──
def _build_weekly(dates, opens, highs, lows, closes, volumes):
    weekly = {}
    for d, o, h, l, c, v in zip(dates, opens, highs, lows, closes, volumes):
        if h is None or l is None or c is None:
            continue
        key = dt.fromisoformat(d).isocalendar()[:2]
        if key not in weekly:
            weekly[key] = {"o": o, "h": h, "l": l, "c": c, "v": v or 0, "d": d}
        else:
            weekly[key]["h"] = max(weekly[key]["h"], h)
            weekly[key]["l"] = min(weekly[key]["l"], l)
            weekly[key]["c"] = c
            weekly[key]["v"] += v or 0
    return weekly


# ── pattern detection (NO liquidity / volume gate) ──
def _detect_patterns(all_data, coil_min_babies=3, tight_close_weeks=3, tight_close_pct=2.0):
    signals = []
    for sym, s in all_data.items():
        dates = s["d"]; opens = s["o"]; highs = s["h"]; lows = s["l"]
        closes = s["c"]; volumes = s["v"]; n = len(dates)
        if n < MIN_BARS:
            continue
        if any(v is None for v in [highs[-1], highs[-2], lows[-1], lows[-2], closes[-1]]):
            continue
        today_d = dates[-1]

        # Inside Bar
        if highs[-1] <= highs[-2] and lows[-1] >= lows[-2]:
            signals.append({"symbol": sym, "pattern": "Inside Bar", "date": today_d,
                "high": round(highs[-1], 2), "low": round(lows[-1], 2),
                "prev_high": round(highs[-2], 2), "prev_low": round(lows[-2], 2)})

        # Double Inside Bar
        if n >= 3 and highs[-3] is not None and lows[-3] is not None:
            if highs[-1] <= highs[-2] and lows[-1] >= lows[-2] and \
               highs[-2] <= highs[-3] and lows[-2] >= lows[-3]:
                signals.append({"symbol": sym, "pattern": "Double Inside Bar", "date": today_d,
                    "high": round(highs[-1], 2), "low": round(lows[-1], 2),
                    "mother_high": round(highs[-3], 2), "mother_low": round(lows[-3], 2)})

        # NR7
        if n >= 7:
            last7_h = [highs[-i] for i in range(1, 8)]; last7_l = [lows[-i] for i in range(1, 8)]
            if all(v is not None for v in last7_h + last7_l):
                today_range = last7_h[0] - last7_l[0]
                if today_range <= min(last7_h[i] - last7_l[i] for i in range(1, 7)):
                    signals.append({"symbol": sym, "pattern": "NR7", "date": today_d,
                        "range": round(today_range, 2),
                        "high": round(highs[-1], 2), "low": round(lows[-1], 2)})

        # MCP (n-bar coil)
        seen_mothers = set()
        for m_idx in range(n - coil_min_babies - 1, max(0, n - 60), -1):
            m_high = highs[m_idx]; m_low = lows[m_idx]
            if m_high is None or m_low is None:
                continue
            m_key = round(m_high * 200)
            if m_key in seen_mothers:
                continue
            baby_count = 0; coil_state = "Coiling"
            for b in range(m_idx + 1, n):
                if highs[b] is None or lows[b] is None:
                    continue
                if highs[b] > m_high:
                    coil_state = "Upper BO"; break
                elif lows[b] < m_low:
                    coil_state = "Lower BD"; break
                else:
                    baby_count += 1
            if baby_count >= coil_min_babies and coil_state == "Coiling":
                seen_mothers.add(m_key)
                signals.append({"symbol": sym,
                    "pattern": f"{baby_count} Bar MCP" if baby_count <= 6 else "Mini Coil",
                    "date": today_d, "mcp_high": round(m_high, 2), "mcp_low": round(m_low, 2),
                    "baby_count": baby_count, "coil_state": coil_state, "mother_date": dates[m_idx]})

        # Weekly patterns
        weekly = _build_weekly(dates, opens, highs, lows, closes, volumes)
        if not weekly:
            continue
        current_week = dt.fromisoformat(today_d).isocalendar()[:2]
        past_weeks = sorted(k for k in weekly if k < current_week)
        if len(past_weeks) < 2:
            continue
        lw = weekly[past_weeks[-1]]; lw2 = weekly[past_weeks[-2]]
        if lw["h"] <= lw2["h"] and lw["l"] >= lw2["l"]:
            signals.append({"symbol": sym, "pattern": "Weekly IB", "date": today_d,
                "w_high": round(lw["h"], 2), "w_low": round(lw["l"], 2), "w_close": round(lw["c"], 2),
                "prev_w_high": round(lw2["h"], 2), "prev_w_low": round(lw2["l"], 2)})
            if len(past_weeks) >= 3:
                lw3 = weekly[past_weeks[-3]]
                if lw2["h"] <= lw3["h"] and lw2["l"] >= lw3["l"]:
                    signals.append({"symbol": sym, "pattern": "Weekly Double IB", "date": today_d,
                        "w_high": round(lw["h"], 2), "w_low": round(lw["l"], 2),
                        "mother_w_high": round(lw3["h"], 2), "mother_w_low": round(lw3["l"], 2)})
        if len(past_weeks) >= 7:
            lw_range = lw["h"] - lw["l"]
            if lw_range <= min(weekly[past_weeks[-i]]["h"] - weekly[past_weeks[-i]]["l"] for i in range(2, 8)):
                signals.append({"symbol": sym, "pattern": "Weekly NR7", "date": today_d,
                    "w_range": round(lw_range, 2), "w_high": round(lw["h"], 2), "w_low": round(lw["l"], 2)})
        if len(past_weeks) >= tight_close_weeks:
            last_n = [weekly[past_weeks[-i]]["c"] for i in range(1, tight_close_weeks + 1)]
            if all(c is not None for c in last_n) and min(last_n) > 0:
                tc_range = (max(last_n) - min(last_n)) / min(last_n) * 100
                if tc_range <= tight_close_pct:
                    signals.append({"symbol": sym, "pattern": "Weekly Tight Close", "date": today_d,
                        "closes": [round(c, 2) for c in last_n], "range_pct": round(tc_range, 2)})
    return signals


def build_stock_feed(all_data, signals):
    """Per-stock: metrics (52WH, dist, ATR%) + pattern names."""
    pats_by_sym = {}
    for sig in signals:
        pats_by_sym.setdefault(sig["symbol"], []).append(sig["pattern"])

    stocks = {}
    for sym, s in all_data.items():
        if len(s.get("c", [])) < MIN_BARS:
            continue
        ltp, high52, dist52, atr_pct = _metrics(s)
        if ltp is None:
            continue
        stocks[sym] = {
            "ltp":           round(ltp, 2),
            "high52":        round(high52, 2) if high52 else None,
            "dist_52wh_pct": dist52,
            "atr_pct":       atr_pct,
            "patterns":      sorted(set(pats_by_sym.get(sym, []))),
        }
    return stocks


async def main():
    async with httpx.AsyncClient() as client:
        all_data = await download_chunks(client)
        if not all_data:
            print("❌ No BSE OHLC data — run bse_ohlc.py first"); sys.exit(1)

        signals = _detect_patterns(all_data)
        counts = Counter(s["pattern"] for s in signals)
        for pat, cnt in sorted(counts.items()):
            print(f"  {pat}: {cnt}")
        print(f"Pattern signals: {len(signals)}")

        stocks = build_stock_feed(all_data, signals)
        print(f"Stock feed: {len(stocks)} stocks (52WH / dist / ATR% included)")

        today = next(iter(all_data.values()))["d"][-1]
        await r2_upload(client, OUT_FILE, {
            "updated": today,
            "count":   len(signals),
            "summary": dict(counts),
            "signals": signals,
            "stocks":  stocks,
        })
        print("✅ BSE pattern + metrics scan complete")


if __name__ == "__main__":
    asyncio.run(main())
