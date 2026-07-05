"""
htf_test_scan.py
----------------
Test the HTF (High Tight Flag) scan across ALL stocks — without running the
full pipeline.py (no Upstox/Finedge tokens needed, since we skip
build_isin_map() entirely). Just downloads the 8 OHLC chunks (same R2 worker
your pipeline already uses) and runs detection on every symbol.

Usage:
    export WORKER_URL="https://your-worker-url"
    export WORKER_TOKEN="your-secret-token"
    python htf_test_scan.py
    python htf_test_scan.py --min-gain 70 --max-pullback 30
    python htf_test_scan.py --save results.json
"""

import argparse
import json
import os
import sys
from datetime import date

import httpx

R2_CHUNKS = 8
WORKER_URL = os.environ.get("WORKER_URL", "").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}


# ── same _detect_htf logic as the pipeline.py patch ──────────────────────

def _htf_swing_low_idx(lows, end_idx, lookback):
    start = max(0, end_idx - lookback)
    seg = [(i, lows[i]) for i in range(start, end_idx + 1) if lows[i] is not None]
    if not seg:
        return None
    return min(seg, key=lambda x: x[1])[0]


def detect_htf(s, min_gain_pct=90.0, pole_min_days=10, pole_max_days=40,
               max_pullback_pct=25.0, flag_min_days=10, flag_max_days=40,
               lookback_days=260):
    dates, highs, lows, closes = s["d"], s["h"], s["l"], s["c"]
    n = len(dates)
    if n < pole_min_days + flag_min_days:
        return []

    matches = []
    scan_start = max(0, n - lookback_days)

    for hi in range(scan_start + pole_min_days, n):
        pole_high = highs[hi]
        if pole_high is None:
            continue

        lo = _htf_swing_low_idx(lows, hi, pole_max_days)
        if lo is None or lo >= hi:
            continue
        pole_low = lows[lo]
        pole_days = hi - lo
        if pole_days < pole_min_days or pole_days > pole_max_days:
            continue
        if not pole_low or pole_low <= 0:
            continue

        gain_pct = (pole_high - pole_low) / pole_low * 100.0
        if gain_pct < min_gain_pct:
            continue

        if any(highs[k] is not None and highs[k] > pole_high for k in range(lo, hi)):
            continue
        peak_check_end = min(n, hi + 5)
        if any(highs[k] is not None and highs[k] > pole_high * 1.03 for k in range(hi + 1, peak_check_end)):
            continue

        flag_end_max = min(n - 1, hi + flag_max_days)
        for fe in range(hi + flag_min_days, flag_end_max + 1):
            flag_lows = [lows[k] for k in range(hi, fe + 1) if lows[k] is not None]
            flag_highs = [highs[k] for k in range(hi, fe + 1) if highs[k] is not None]
            if not flag_lows or not flag_highs:
                continue
            flag_low = min(flag_lows)
            flag_high = max(flag_highs)

            pullback_pct = (pole_high - flag_low) / pole_high * 100.0
            if pullback_pct > max_pullback_pct:
                break

            flag_days = fe - hi
            last_close = closes[-1]
            last_low = lows[-1]
            status = "forming"
            if last_close is not None and last_close > flag_high:
                status = "breakout"
            elif last_low is not None and last_low < flag_low * 0.98:
                status = "failed"

            matches.append({
                "pole_low_date": dates[lo], "pole_low": round(pole_low, 2),
                "pole_high_date": dates[hi], "pole_high": round(pole_high, 2),
                "pole_gain_pct": round(gain_pct, 1), "pole_days": pole_days,
                "flag_end_date": dates[fe], "flag_low": round(flag_low, 2),
                "flag_high": round(flag_high, 2), "flag_pullback_pct": round(pullback_pct, 1),
                "flag_days": flag_days, "as_of_date": dates[-1],
                "as_of_close": round(last_close, 2) if last_close is not None else None,
                "status": status,
            })
            break

    matches.sort(key=lambda m: m["pole_high_date"], reverse=True)
    deduped = []
    for m in matches:
        if not any(abs((date.fromisoformat(m["pole_high_date"]) - date.fromisoformat(k["pole_high_date"])).days) <= 10
                   for k in deduped):
            deduped.append(m)
    return deduped


def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    """Same liquidity filter as pipeline.py — skip illiquid stocks."""
    lookback = min(50, n)
    if lookback < 20:
        return True
    vols = [v for v in volumes[-lookback:] if v is not None]
    prices = [c for c in closes[-lookback:] if c is not None and c > 0]
    if len(vols) < 20 or len(prices) < 20:
        return False
    return (sum(vols) / len(vols) * sum(prices) / len(prices)) >= min_turnover


# ── download all 8 chunks -> {symbol: {d,o,h,l,c,v,oi}} ─────────────────

def download_all_chunks():
    if not WORKER_URL or not WORKER_TOKEN:
        print("ERROR: set WORKER_URL and WORKER_TOKEN env vars first (same values your pipeline.py GitHub Actions secrets use).")
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


def main():
    ap = argparse.ArgumentParser(description="Full-universe HTF scan test (no full pipeline run)")
    ap.add_argument("--min-gain", type=float, default=90.0)
    ap.add_argument("--max-pullback", type=float, default=25.0)
    ap.add_argument("--pole-max-days", type=int, default=40)
    ap.add_argument("--flag-max-days", type=int, default=40)
    ap.add_argument("--save", help="optional path to save results as JSON")
    args = ap.parse_args()

    print("Downloading OHLC chunks...")
    all_data = download_all_chunks()
    print(f"\nTotal loaded: {len(all_data)} stocks\n")

    signals = []
    skipped_illiquid = 0
    for sym, s in all_data.items():
        if not _check_liquidity(s.get("v", []), s.get("c", []), len(s.get("d", []))):
            skipped_illiquid += 1
            continue
        for m in detect_htf(
            s,
            min_gain_pct=args.min_gain,
            max_pullback_pct=args.max_pullback,
            pole_max_days=args.pole_max_days,
            flag_max_days=args.flag_max_days,
        ):
            signals.append({"symbol": sym, **m})

    order = {"breakout": 0, "forming": 1, "failed": 2}
    signals.sort(key=lambda x: (order.get(x["status"], 9), -x["pole_gain_pct"]))

    breakout = sum(1 for x in signals if x["status"] == "breakout")
    forming = sum(1 for x in signals if x["status"] == "forming")
    failed = sum(1 for x in signals if x["status"] == "failed")

    print(f"Skipped (illiquid): {skipped_illiquid}")
    print(f"HTF setups found: {len(signals)}  (Breakout: {breakout}  Forming: {forming}  Failed: {failed})\n")

    for x in signals:
        print(f"  {x['symbol']:<15} {x['status'].upper():<9} "
              f"pole +{x['pole_gain_pct']}%  flag pullback {x['flag_pullback_pct']}%  "
              f"pole: {x['pole_low_date']}->{x['pole_high_date']}  flag through {x['flag_end_date']}")

    if args.save:
        with open(args.save, "w") as f:
            json.dump({
                "count": len(signals), "breakout": breakout, "forming": forming,
                "failed": failed, "signals": signals,
            }, f, indent=2)
        print(f"\nSaved to {args.save}")


if __name__ == "__main__":
    main()
