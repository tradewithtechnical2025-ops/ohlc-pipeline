"""
shakeout_scanner.py
-------------------
NEW shakeout scanner design (separate from pipeline.py's existing
_detect_shakeout, which uses a different wick/Supertrend-based definition).

Pattern:
  - Stock trading above a given EMA (10, 21, or 50) for at least
    min_days_above consecutive days.
  - Then CLOSES below that EMA (the "shakeout"/breakdown day).
  - Then closes back ABOVE that same EMA within max_recovery_days.
  - Trend filter: EMA21 > EMA50 on the breakdown day (confirms the stock is
    still in an underlying uptrend structure, not a genuine trend reversal).

Usage:
    export WORKER_URL="https://your-worker-url"
    export WORKER_TOKEN="your-secret-token"
    python shakeout_scanner.py
    python shakeout_scanner.py --symbol RELIANCE
    python shakeout_scanner.py --save results.json
    python shakeout_scanner.py --r2-key shakeout_signals.json
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


def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    lookback = min(50, n)
    if lookback < 20:
        return True
    vols = [v for v in volumes[-lookback:] if v is not None]
    prices = [c for c in closes[-lookback:] if c is not None and c > 0]
    if len(vols) < 20 or len(prices) < 20:
        return False
    return (sum(vols) / len(vols) * sum(prices) / len(prices)) >= min_turnover


def _merge_compound_shakeouts(raw_signals, dates, closes):
    """Merges signals whose [breakdown_idx, recovery_idx] windows overlap
    ACROSS DIFFERENT EMAs into one compound signal. Two signals overlap if
    either one's breakdown falls within the other's breakdown-to-recovery
    span — meaning the same underlying dip-and-recover move touched
    multiple EMA levels, not two unrelated events."""
    if not raw_signals:
        return []

    raw_signals = sorted(raw_signals, key=lambda s: s["breakdown_idx"])
    groups = [[raw_signals[0]]]

    def overlaps(a, b):
        return not (b["breakdown_idx"] > a["recovery_idx"] or a["breakdown_idx"] > b["recovery_idx"])

    for sig in raw_signals[1:]:
        if any(overlaps(g, sig) for g in groups[-1]):
            groups[-1].append(sig)
        else:
            groups.append([sig])

    merged = []
    as_of_date = dates[-1]
    as_of_close = round(closes[-1], 2) if closes[-1] is not None else None

    for group in groups:
        periods = sorted(set(g["ema_period"] for g in group))
        breakdown_idx = min(g["breakdown_idx"] for g in group)
        recovery_idx = max(g["recovery_idx"] for g in group)
        entry = {
            "ema_periods": periods,
            "compound": len(group) > 1,
            "breakdown_date": dates[breakdown_idx],
            "recovery_date": dates[recovery_idx],
            "days_to_recover": recovery_idx - breakdown_idx,
            "details": [
                {"ema_period": g["ema_period"], "breakdown_date": g["breakdown_date"],
                 "breakdown_close": g["breakdown_close"], "ema_value": g["ema_value"],
                 "recovery_date": g["recovery_date"], "recovery_close": g["recovery_close"]}
                for g in sorted(group, key=lambda x: x["ema_period"])
            ],
            "as_of_date": as_of_date, "as_of_close": as_of_close,
        }
        merged.append(entry)
    return merged


def detect_shakeout(s, ema_periods=(10, 21, 50), min_days_above=None, max_recovery_days=5,
                     lookback_days=260):
    """Returns a list of shakeout signals for this stock (most recent first).
    When breakdown+recovery windows for DIFFERENT EMAs overlap (e.g. EMA10
    breaks down, then EMA21 also breaks down before EMA10 recovers, then
    everything recovers together), they're merged into a single "compound"
    signal listing all EMA periods involved, instead of separate entries.

    min_days_above: how many consecutive days the stock must have closed
    above a given EMA before the breakdown counts as a shakeout. Can differ
    per EMA — pass a dict like {10: 5, 21: 3, 50: 3}, or leave as None to
    use the default (EMA10 needs 5 days, EMA21/EMA50 need 3)."""
    if min_days_above is None:
        min_days_above = {10: 5, 21: 3, 50: 3}
    elif isinstance(min_days_above, int):
        min_days_above = {p: min_days_above for p in ema_periods}

    dates, highs, lows, closes = s["d"], s["h"], s["l"], s["c"]
    n = len(dates)
    if n < 60:
        return []

    ema10 = _calc_ema(closes, 10)
    ema21 = _calc_ema(closes, 21)
    ema50 = _calc_ema(closes, 50)
    emas = {10: ema10, 21: ema21, 50: ema50}

    max_min_days = max(min_days_above.get(p, 3) for p in ema_periods)
    scan_start = max(max_min_days + 1, n - lookback_days)
    raw_signals = []

    for period in ema_periods:
        ema = emas[period]
        days_above_needed = min_days_above.get(period, 3)
        for i in range(scan_start, n):
            # Trend filter: EMA21 > EMA50 on the breakdown day.
            if ema21[i] is None or ema50[i] is None or ema21[i] <= ema50[i]:
                continue

            # Breakdown day: close must be below this EMA.
            if closes[i] is None or ema[i] is None or closes[i] >= ema[i]:
                continue

            # Must have been above this EMA for at least days_above_needed
            # consecutive days immediately before the breakdown.
            above_ok = True
            for k in range(i - days_above_needed, i):
                if closes[k] is None or ema[k] is None or closes[k] <= ema[k]:
                    above_ok = False
                    break
            if not above_ok:
                continue

            # Recovery: a close back above the SAME EMA within
            # max_recovery_days after the breakdown.
            recovery_idx = None
            for j in range(i + 1, min(n, i + 1 + max_recovery_days)):
                if closes[j] is not None and ema[j] is not None and closes[j] > ema[j]:
                    recovery_idx = j
                    break
            if recovery_idx is None:
                continue

            raw_signals.append({
                "ema_period": period,
                "breakdown_idx": i, "recovery_idx": recovery_idx,
                "breakdown_date": dates[i],
                "breakdown_close": round(closes[i], 2),
                "ema_value": round(ema[i], 2),
                "recovery_date": dates[recovery_idx],
                "recovery_close": round(closes[recovery_idx], 2),
                "days_to_recover": recovery_idx - i,
            })

    signals = _merge_compound_shakeouts(raw_signals, dates, closes)
    signals.sort(key=lambda x: x["breakdown_date"], reverse=True)
    return signals


# ── R2 helpers (same convention as pipeline.py) ─────────────────────────

def download_all_chunks():
    if not WORKER_URL or not WORKER_TOKEN:
        print("ERROR: set WORKER_URL and WORKER_TOKEN env vars first.")
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
    ap = argparse.ArgumentParser(description="EMA breakdown + quick-recovery shakeout scan")
    ap.add_argument("--symbol", help="run for just one symbol")
    ap.add_argument("--min-days-above", type=int, default=None,
                     help="override for ALL EMAs (default: EMA10=5, EMA21/50=3)")
    ap.add_argument("--max-recovery-days", type=int, default=5)
    ap.add_argument("--save", help="optional path to save results as JSON")
    ap.add_argument("--r2-key", help="optional R2 filename to push results to")
    args = ap.parse_args()

    print("Downloading OHLC chunks...")
    all_data = download_all_chunks()
    print(f"\nTotal loaded: {len(all_data)} stocks\n")

    signals = []
    skipped_illiquid = 0
    for sym, s in all_data.items():
        if args.symbol and sym != args.symbol:
            continue
        if not _check_liquidity(s.get("v", []), s.get("c", []), len(s.get("d", []))):
            skipped_illiquid += 1
            continue
        for sig in detect_shakeout(s, min_days_above=args.min_days_above,
                                    max_recovery_days=args.max_recovery_days):
            signals.append({"symbol": sym, **sig})

    print(f"Skipped (illiquid): {skipped_illiquid}")
    signals.sort(key=lambda x: x["breakdown_date"], reverse=True)
    print(f"Shakeout signals found: {len(signals)}\n")
    for x in signals:
        emas_str = "+".join(f"EMA{p}" for p in x["ema_periods"])
        tag = " [COMPOUND]" if x["compound"] else ""
        print(f"  {x['symbol']:<15} {emas_str:<15}{tag} "
              f"breakdown {x['breakdown_date']}  recovered {x['recovery_date']} (+{x['days_to_recover']}d)")
        for d in x["details"]:
            print(f"      EMA{d['ema_period']}: {d['breakdown_date']} @ {d['breakdown_close']} "
                  f"(EMA {d['ema_value']}) -> recovered {d['recovery_date']} @ {d['recovery_close']}")

    result = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "count": len(signals),
        "signals": signals,
    }

    if args.save:
        with open(args.save, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved to {args.save}")

    if args.r2_key:
        print(f"\nPushing results to R2 as {args.r2_key}...")
        upload_to_r2(args.r2_key, json.dumps(result))


if __name__ == "__main__":
    main()
