#!/usr/bin/env python3
"""
BSE Pattern + Metrics Scan — BSE OHLC pe (rich feed).
- No liquidity filter (universe pehle hi mcap/price se filtered).
- Per-stock metrics: LTP, %chg, volume, RVol, returns(1/3/6/12M),
  52WH + distance%, 52WL, ATR%(14), EMA50/200 trend flags, RSI(14),
  RS percentile (BSE-relative), + candle/weekly patterns.
Input : bse_ohlc_1..N.json (R2)
Output: bse_pattern_signals.json (R2)  — {signals:[...], stocks:{sym:{...}}}
Usage : python bse_pattern.py
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

# ── indicators ──
def _ema(closes, period):
    n = len(closes)
    if n < period:
        return None
    k = 2/(period+1)
    seed = [v for v in closes[:period] if v]
    if not seed:
        return None
    e = sum(seed)/len(seed)
    for v in closes[period:]:
        e = v*k + e*(1-k) if v else e
    return e

def _rsi(closes, period=14):
    if len(closes) < period+1:
        return None
    gains = []; losses = []
    for i in range(1, len(closes)):
        d = closes[i]-closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[:period])/period; al = sum(losses[:period])/period
    for i in range(period, len(gains)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
    if al == 0:
        return 100.0
    return round(100 - 100/(1+ag/al), 1)

def _atr(highs, lows, closes, period=14):
    """Wilder's ATR(14) — seed = simple avg of first 14 TRs, then Wilder smooth."""
    n = len(closes)
    if n < period + 1:
        return None
    trs = []
    for i in range(1, n):
        hh = highs[i]; ll = lows[i]; pc = closes[i-1]
        if None in (hh, ll, pc):
            continue
        trs.append(max(hh - ll, abs(hh - pc), abs(ll - pc)))
    if len(trs) < period:
        return None
    # seed: simple avg of first 14 TRs
    atr = sum(trs[:period]) / period
    # Wilder smoothing for remaining
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr

def _compute(s):
    h = s["h"]; l = s["l"]; c = s["c"]; v = s["v"]; n = len(c)
    ltp  = c[-1] if c else None
    prev = c[-2] if n >= 2 else None
    pct_ch = round((ltp-prev)/prev*100, 2) if (ltp and prev) else None
    vol   = v[-1] if v else None
    avg20 = (sum(x for x in v[-21:-1] if x)/20) if n >= 21 else None
    rvol  = round(vol/avg20, 2) if (avg20 and vol) else None

    def ret(d):
        i = n-1-d
        if i < 0 or not c[i]:
            return None
        return round((ltp-c[i])/c[i]*100, 2)

    w52 = [x for x in h[-252:] if x is not None]; high52 = max(w52) if w52 else None
    lo52 = [x for x in l[-252:] if x is not None]; low52  = min(lo52) if lo52 else None
    dist = round((ltp-high52)/high52*100, 2) if (high52 and ltp) else None

    # ATR(14) — Wilder's smoothing
    atr     = _atr(h, l, c, period=14)
    atr_pct = round(atr/ltp*100, 2) if (atr and ltp) else None

    ema50  = _ema(c, 50)
    ema200 = _ema(c, 200)

    return {
        "ltp":           round(ltp, 2) if ltp else None,
        "pct_ch":        pct_ch,
        "volume":        vol,
        "rvol":          rvol,
        "ret1m":         ret(21),
        "ret3m":         ret(63),
        "ret6m":         ret(126),
        "ret12m":        ret(252),
        "high52":        round(high52, 2) if high52 else None,
        "dist_52wh_pct": dist,
        "low52":         round(low52, 2) if low52 else None,
        "atr_pct":       atr_pct,
        "above50":       bool(ema50 and ltp and ltp > ema50),
        "above200":      bool(ema200 and ltp and ltp > ema200),
        "ema_trend":     bool(ema50 and ema200 and ema50 > ema200),
        "rsi":           _rsi([x for x in c[-260:] if x is not None]),
    }

def _rs_percentile(all_data):
    """BSE-relative RS: weighted composite of 63/126/189/252-day returns -> percentile."""
    comp = {}
    for sym, s in all_data.items():
        c = s["c"]; n = len(c); idx = n-1
        if idx < 63:
            continue
        def ret(lb):
            j = idx-lb
            if j < 0 or not c[j]:
                return None
            return (c[idx]-c[j])/c[j]*100
        p63, p126, p189, p252 = ret(63), ret(126), ret(189), ret(252)
        if None not in (p63, p126, p189, p252): composite = (p63*2+p126+p189+p252)/5
        elif None not in (p63, p126, p189):     composite = (p63*2+p126+p189)/4
        elif None not in (p63, p126):           composite = (p63*2+p126)/3
        elif p63 is not None:                   composite = p63
        else:                                   composite = None
        if composite is not None:
            comp[sym] = composite
    srt = sorted(comp, key=lambda x: comp[x]); tot = len(srt)
    return {sym: round((i+1)/tot*99) for i, sym in enumerate(srt)} if tot else {}

# ── weekly + patterns ──
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

        if highs[-1] <= highs[-2] and lows[-1] >= lows[-2]:
            signals.append({"symbol": sym, "pattern": "Inside Bar", "date": today_d})

        if n >= 3 and highs[-3] is not None and lows[-3] is not None:
            if highs[-1] <= highs[-2] and lows[-1] >= lows[-2] and \
               highs[-2] <= highs[-3] and lows[-2] >= lows[-3]:
                signals.append({"symbol": sym, "pattern": "Double Inside Bar", "date": today_d})

        if n >= 7:
            l7h = [highs[-i] for i in range(1, 8)]; l7l = [lows[-i] for i in range(1, 8)]
            if all(v is not None for v in l7h + l7l):
                if (l7h[0]-l7l[0]) <= min(l7h[i]-l7l[i] for i in range(1, 7)):
                    signals.append({"symbol": sym, "pattern": "NR7", "date": today_d})

        seen = set()
        for m_idx in range(n-coil_min_babies-1, max(0, n-60), -1):
            mh = highs[m_idx]; ml = lows[m_idx]
            if mh is None or ml is None:
                continue
            mk = round(mh*200)
            if mk in seen:
                continue
            babies = 0; state = "Coiling"
            for b in range(m_idx+1, n):
                if highs[b] is None or lows[b] is None:
                    continue
                if highs[b] > mh: state = "BO"; break
                elif lows[b] < ml: state = "BD"; break
                else: babies += 1
            if babies >= coil_min_babies and state == "Coiling":
                seen.add(mk)
                signals.append({"symbol": sym,
                    "pattern": f"{babies} Bar MCP" if babies <= 6 else "Mini Coil",
                    "date": today_d, "baby_count": babies})

        weekly = _build_weekly(dates, opens, highs, lows, closes, volumes)
        if not weekly:
            continue
        cw = dt.fromisoformat(today_d).isocalendar()[:2]
        pw = sorted(k for k in weekly if k < cw)
        if len(pw) < 2:
            continue
        lw = weekly[pw[-1]]; lw2 = weekly[pw[-2]]

        if lw["h"] <= lw2["h"] and lw["l"] >= lw2["l"]:
            signals.append({"symbol": sym, "pattern": "Weekly IB", "date": today_d})
            if len(pw) >= 3:
                lw3 = weekly[pw[-3]]
                if lw2["h"] <= lw3["h"] and lw2["l"] >= lw3["l"]:
                    signals.append({"symbol": sym, "pattern": "Weekly Double IB", "date": today_d})

        if len(pw) >= 7:
            if (lw["h"]-lw["l"]) <= min(weekly[pw[-i]]["h"]-weekly[pw[-i]]["l"] for i in range(2, 8)):
                signals.append({"symbol": sym, "pattern": "Weekly NR7", "date": today_d})

        if len(pw) >= tight_close_weeks:
            ln = [weekly[pw[-i]]["c"] for i in range(1, tight_close_weeks+1)]
            if all(c is not None for c in ln) and min(ln) > 0:
                if (max(ln)-min(ln))/min(ln)*100 <= tight_close_pct:
                    signals.append({"symbol": sym, "pattern": "Weekly Tight Close", "date": today_d})
    return signals

def build_stock_feed(all_data, signals):
    rs = _rs_percentile(all_data)
    pats = {}
    for sig in signals:
        pats.setdefault(sig["symbol"], []).append(sig["pattern"])
    stocks = {}
    for sym, s in all_data.items():
        if len(s.get("c", [])) < MIN_BARS:
            continue
        m = _compute(s)
        if m["ltp"] is None:
            continue
        m["rs"] = rs.get(sym)
        m["patterns"] = sorted(set(pats.get(sym, [])))
        stocks[sym] = m
    return stocks

async def main():
    async with httpx.AsyncClient() as client:
        all_data = await download_chunks(client)
        if not all_data:
            print("❌ No BSE OHLC — run bse_ohlc.py first"); sys.exit(1)

        signals = _detect_patterns(all_data)
        counts = Counter(s["pattern"] for s in signals)
        for p, c in sorted(counts.items()):
            print(f"  {p}: {c}")
        print(f"Pattern signals: {len(signals)}")

        stocks = build_stock_feed(all_data, signals)
        print(f"Stock feed: {len(stocks)} stocks (full metrics)")

        today = next(iter(all_data.values()))["d"][-1]
        await r2_upload(client, OUT_FILE, {
            "updated": today, "count": len(signals),
            "summary": dict(counts), "signals": signals, "stocks": stocks,
        })
        print("✅ BSE pattern + metrics scan complete")

if __name__ == "__main__":
    asyncio.run(main())
