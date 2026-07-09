"""
ma_expansion_scanner.py
------------------------
MA Expansion / Launch-Pad scan across ALL stocks — same convention as
htf_test_scan.py: downloads the 8 OHLC chunks from your R2 worker, no
Upstox/Finedge tokens needed.

Detects stocks whose MA stack (fast/mid/slow) was coiled/flat, and has
JUST started to fan out (expand) with bullish alignment, within the last
`trigger_window` days.

Usage:
    export WORKER_URL="https://your-worker-url"
    export WORKER_TOKEN="your-secret-token"
    python ma_expansion_scanner.py
    python ma_expansion_scanner.py --trigger-window 5 --save results.json
    python ma_expansion_scanner.py --r2-key ma_expansion_results.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import httpx

R2_CHUNKS = 8
WORKER_URL = os.environ.get("WORKER_URL", "").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}


def _calc_ema(closes, period):
    """Same convention as pipeline.py's _calc_ema."""
    n = len(closes)
    ema = [None] * n
    if n < period:
        return ema
    k = 2 / (period + 1)
    seed_vals = [v for v in closes[:period] if v is not None]
    if not seed_vals:
        return ema
    ema[period - 1] = sum(seed_vals) / len(seed_vals)
    for i in range(period, n):
        c = closes[i]
        ema[i] = c * k + ema[i - 1] * (1 - k) if c is not None else ema[i - 1]
    return ema


def _calc_sma(closes, period):
    n = len(closes)
    sma = [None] * n
    for i in range(period - 1, n):
        window = closes[i - period + 1:i + 1]
        if any(v is None for v in window):
            continue
        sma[i] = sum(window) / period
    return sma


def _linear_slope(values):
    """OLS slope over a short window of (index, value) points, ignoring Nones."""
    vals = [v for v in values if v is not None]
    n = len(vals)
    if n < 2:
        return None
    x_mean = (n - 1) / 2
    y_mean = sum(vals) / n
    num = sum((i - x_mean) * (vals[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den != 0 else None


def detect_ma_expansion(s, fast=21, mid=50, slow=150,
                         contraction_window=20, trigger_window=7,
                         contraction_thresh_pct=4.0, expansion_thresh_pct=6.0,
                         min_efficiency_ratio=0.5):
    """s: {"d": dates, "c": closes, ...} same shape as htf_test_scan.py's per-symbol dict.
    Returns a match dict if a fresh MA expansion is found, else None."""
    dates, closes = s["d"], s["c"]
    n = len(dates)
    if n < slow + contraction_window + trigger_window:
        return None

    ema_fast = _calc_ema(closes, fast)
    ema_mid = _calc_ema(closes, mid)
    sma_slow = _calc_sma(closes, slow)

    spread_pct = [None] * n
    stacked = [False] * n
    for i in range(n):
        ef, em, ss = ema_fast[i], ema_mid[i], sma_slow[i]
        if ef is not None and ss not in (None, 0):
            spread_pct[i] = (ef - ss) / ss * 100
        if ef is not None and em is not None and ss is not None:
            stacked[i] = ef > em > ss

    window_size = contraction_window + trigger_window
    win_start = n - window_size
    contraction_part_spread = [v for v in spread_pct[win_start:win_start + contraction_window] if v is not None]
    contraction_part_stacked = stacked[win_start:win_start + contraction_window]
    trigger_part_stacked = stacked[win_start + contraction_window:]

    if not contraction_part_spread:
        return None

    sorted_spread = sorted(abs(v) for v in contraction_part_spread)
    median_spread = sorted_spread[len(sorted_spread) // 2]
    was_coiled = median_spread < contraction_thresh_pct
    stacking_before = (sum(contraction_part_stacked) / len(contraction_part_stacked)) < 0.5

    trigger_idx = None
    for i in range(len(trigger_part_stacked)):
        if all(trigger_part_stacked[i:]):
            trigger_idx = i
            break

    if trigger_idx is None:
        return None

    days_since_trigger = len(trigger_part_stacked) - trigger_idx
    spread_now = spread_pct[-1]
    last3 = [v for v in spread_pct[-3:] if v is not None]
    spread_expanding = len(last3) == 3 and last3[0] < last3[1] < last3[2]

    slope_now = _linear_slope(ema_fast[-5:])

    pole_closes = [c for c in closes[-fast:] if c is not None]
    er = None
    if len(pole_closes) >= 2:
        net_move = abs(pole_closes[-1] - pole_closes[0])
        total_move = sum(abs(pole_closes[k] - pole_closes[k - 1]) for k in range(1, len(pole_closes)))
        er = (net_move / total_move) if total_move > 0 else 0

    signal = (
        was_coiled
        and stacking_before
        and spread_now is not None and spread_now > expansion_thresh_pct * 0.5
        and spread_expanding
        and slope_now is not None and slope_now > 0
        and er is not None and er >= min_efficiency_ratio
    )

    if not signal:
        return None

    return {
        "as_of_date": dates[-1],
        "days_since_trigger": days_since_trigger,
        "spread_now_pct": round(spread_now, 2),
        "efficiency_ratio": round(er, 3),
        "fast_ma_slope": round(slope_now, 4),
    }


def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    """Same liquidity filter as pipeline.py / htf_test_scan.py — skip illiquid stocks."""
    lookback = min(50, n)
    if lookback < 20:
        return True
    vols = [v for v in volumes[-lookback:] if v is not None]
    prices = [c for c in closes[-lookback:] if c is not None and c > 0]
    if len(vols) < 20 or len(prices) < 20:
        return False
    return (sum(vols) / len(vols) * sum(prices) / len(prices)) >= min_turnover


def download_all_chunks():
    if not WORKER_URL or not WORKER_TOKEN:
        print("ERROR: set WORKER_URL and WORKER_TOKEN env vars first (same values pipeline.py uses).")
        sys.exit(1)

    all_data = {}
    with httpx.Client() as client:
        for i in range(R2_CHUNKS):
            fname = f"ohlc_{i+1}.json"
            r = client.get(f"{WORKER_URL}/{fname}", headers=WORKER_HEADERS, timeout=90)
            if r.status_code != 200:
                print(f"  [warn] {fname} -> HTTP {r.status_code}, skipping")
                continue
            data = r.json()
            stocks = data.get("stocks", {})
            all_data.update(stocks)
            print(f"  {fname}: {len(stocks)} stocks")
    return all_data


def upload_to_r2(filename, data_str):
    """Same convention as pipeline.py's r2_upload()."""
    if not WORKER_URL or not WORKER_TOKEN:
        print("ERROR: set WORKER_URL and WORKER_TOKEN env vars first.")
        sys.exit(1)
    url = f"{WORKER_URL}?file={filename}"
    with httpx.Client() as client:
        r = client.post(url, headers={**WORKER_HEADERS, "Content-Type": "application/json"},
                         content=data_str.encode(), timeout=90)
    if r.status_code != 200:
        print(f"  [warn] R2 upload failed for {filename}: HTTP {r.status_code} {r.text[:200]}")
        return False
    print(f"  ↑ {filename} ({len(data_str)/1024:.1f} KB) uploaded to R2")
    return True


def main():
    ap = argparse.ArgumentParser(description="Full-universe MA Expansion scan")
    ap.add_argument("--fast", type=int, default=10)
    ap.add_argument("--mid", type=int, default=21)
    ap.add_argument("--slow", type=int, default=50)
    ap.add_argument("--contraction-window", type=int, default=10)
    ap.add_argument("--contraction-thresh", type=float, default=2.0,
                     help="max median |spread%%| during contraction window to call it 'coiled'")
    ap.add_argument("--expansion-thresh", type=float, default=3.0,
                     help="spread%% threshold used as the expansion trigger level")
    ap.add_argument("--min-er", type=float, default=0.5,
                     help="minimum efficiency ratio (0-1) - rejects choppy/noisy expansion")
    ap.add_argument("--trigger-window", type=int, default=3,
                     help="only report if expansion started within this many recent days")
    ap.add_argument("--save", help="optional path to save results as JSON (local file)")
    ap.add_argument("--r2-key", help="optional R2 filename to push results to, e.g. ma_expansion_results.json")
    args = ap.parse_args()

    print("Downloading OHLC chunks...")
    all_data = download_all_chunks()
    print(f"\nTotal loaded: {len(all_data)} stocks\n")

    liquid = {}
    skipped_illiquid = 0
    for sym, s in all_data.items():
        if not _check_liquidity(s.get("v", []), s.get("c", []), len(s.get("d", []))):
            skipped_illiquid += 1
            continue
        liquid[sym] = s
    print(f"Skipped (illiquid): {skipped_illiquid}\n")

    signals = []
    for sym, s in liquid.items():
        m = detect_ma_expansion(s, fast=args.fast, mid=args.mid, slow=args.slow,
                                 contraction_window=args.contraction_window,
                                 trigger_window=args.trigger_window,
                                 contraction_thresh_pct=args.contraction_thresh,
                                 expansion_thresh_pct=args.expansion_thresh,
                                 min_efficiency_ratio=args.min_er)
        if m:
            signals.append({"symbol": sym, **m})

    signals.sort(key=lambda x: x["days_since_trigger"])

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "count": len(signals),
        "signals": signals,
    }

    print(f"=== MA Expansion (fast={args.fast} mid={args.mid} slow={args.slow}  "
          f"trigger_window={args.trigger_window}d) ===")
    print(f"Found: {len(signals)}\n")
    for x in signals:
        print(f"  {x['symbol']:<15} started {x['days_since_trigger']}d ago  "
              f"spread {x['spread_now_pct']}%  ER {x['efficiency_ratio']}  "
              f"slope {x['fast_ma_slope']}")

    if args.save:
        with open(args.save, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved to {args.save}")

    if args.r2_key:
        print(f"\nPushing results to R2 as {args.r2_key}...")
        upload_to_r2(args.r2_key, json.dumps(result))


if __name__ == "__main__":
    main()
