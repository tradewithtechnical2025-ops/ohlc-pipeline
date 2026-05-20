#!/usr/bin/env python3
"""
Test ep_scan locally using real R2 OHLC data.

Usage:
  WORKER_URL=https://... WORKER_TOKEN=xxx python test_ep.py
"""

import asyncio, json, os, sys
import httpx

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
R2_CHUNKS = 8

# ── Same _detect_ep from pipeline ─────────────────────────────
def _detect_ep(
    all_data: dict,
    min_gap_pct: float     = 2.0,
    volume_spike_x: float  = 2.0,
    volume_lookback: int   = 20,
    max_consolidation: int = 20,
) -> list[dict]:
    signals = []
    for sym, s in all_data.items():
        dates, highs, lows, closes, volumes = (
            s["d"], s["h"], s["l"], s["c"], s["v"]
        )
        n = len(dates)
        if n < volume_lookback + 2:
            continue
        for i in range(volume_lookback, n):
            prev_high = highs[i - 1]
            today_low = lows[i]
            if today_low <= prev_high:
                continue
            gap_pct = (today_low - prev_high) / prev_high * 100
            if gap_pct < min_gap_pct:
                continue
            avg_vol = sum(volumes[i - volume_lookback:i]) / volume_lookback
            if avg_vol == 0:
                continue
            vol_x = volumes[i] / avg_vol
            if vol_x < volume_spike_x:
                continue
            gap_lower    = prev_high
            consol_count = 0
            ep_intact    = True
            for j in range(i + 1, min(i + max_consolidation + 1, n)):
                if closes[j] < gap_lower:
                    ep_intact = False
                    break
                consol_count += 1
            if not ep_intact:
                continue
            ep_type  = "Delayed EP" if consol_count > 2 else "Normal EP"
            last_idx = min(i + consol_count, n - 1)
            signals.append({
                "symbol"        : sym,
                "ep_date"       : dates[i],
                "gap_lower"     : round(gap_lower, 2),
                "gap_pct"       : round(gap_pct, 2),
                "vol_spike_x"   : round(vol_x, 1),
                "ep_candle_low" : round(today_low, 2),
                "last_close"    : round(closes[last_idx], 2),
                "last_date"     : dates[last_idx],
                "consolidation" : consol_count,
                "ep_type"       : ep_type,
            })
    return signals


# ── Download R2 chunks ─────────────────────────────────────────
async def download_chunk(client, i):
    url = f"{WORKER_URL}/ohlc_{i+1}.json"
    r   = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 200:
        print(f"  ↓ ohlc_{i+1}.json ({len(r.content)//1024} KB)")
        return r.json()
    print(f"  ✗ ohlc_{i+1}.json → HTTP {r.status_code}")
    return None


async def main():
    print("━━━ EP Scan — Local Test ━━━")

    # 1. Download chunks
    print(f"\nDownloading {R2_CHUNKS} chunks from R2…")
    async with httpx.AsyncClient() as client:
        tasks   = [download_chunk(client, i) for i in range(R2_CHUNKS)]
        results = await asyncio.gather(*tasks)

    all_data = {}
    for res in results:
        if res and "stocks" in res:
            all_data.update(res["stocks"])
    print(f"Loaded {len(all_data)} stocks\n")

    if not all_data:
        print("❌ No data loaded — check WORKER_URL and WORKER_TOKEN")
        sys.exit(1)

    # 2. Run EP scan
    print("Scanning…")
    signals = _detect_ep(all_data)
    signals.sort(key=lambda x: (x["ep_date"], x["gap_pct"]), reverse=True)

    normal  = sum(1 for s in signals if s["ep_type"] == "Normal EP")
    delayed = sum(1 for s in signals if s["ep_type"] == "Delayed EP")

    # 3. Print results
    print(f"\n{'━'*75}")
    print(f"  Total: {len(signals)}   Normal: {normal}   Delayed: {delayed}")
    print(f"{'━'*75}")
    print(f"  {'Symbol':<15} {'EP Date':<12} {'Type':<12} {'Gap%':>6} {'Vol x':>6} {'Consol':>7} {'Last Close':>11} {'Last Date':<12}")
    print(f"  {'-'*73}")

    for s in signals:
        print(
            f"  {s['symbol']:<15} {s['ep_date']:<12} {s['ep_type']:<12} "
            f"{s['gap_pct']:>5.1f}% {s['vol_spike_x']:>5.1f}x "
            f"{s['consolidation']:>6}d {s['last_close']:>11.2f} {s['last_date']:<12}"
        )

    print(f"{'━'*75}")

    # 4. Save to file
    out = json.dumps({
        "count": len(signals), "normal": normal,
        "delayed": delayed, "signals": signals
    }, indent=2)
    open("ep_signals_test.json", "w").write(out)
    print(f"\n✅ Saved → ep_signals_test.json")

    # 5. Spot check — known stocks
    known = {"CARERATING", "RAIN", "WOCKPHARMA"}
    found = {s["symbol"] for s in signals}
    hits  = known & found
    if hits:
        print(f"✅ Known EP stocks found: {', '.join(hits)}")
    else:
        print(f"⚠️  Known stocks {known} not in results — check thresholds")


asyncio.run(main())
