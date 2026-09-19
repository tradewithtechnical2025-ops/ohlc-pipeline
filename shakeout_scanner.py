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


def _calc_atr(highs, lows, closes, period=14):
    """Simple (SMA-based) ATR, aligned by index. atr[i] uses True Range of
    days up to and including i, averaged over the trailing `period` days.
    Returns None where there isn't enough history yet."""
    n = len(closes)
    atr = [None] * n
    tr = [None] * n
    for i in range(n):
        h, l, c = highs[i], lows[i], closes[i]
        if h is None or l is None:
            continue
        if i == 0 or closes[i - 1] is None:
            tr[i] = h - l
        else:
            pc = closes[i - 1]
            tr[i] = max(h - l, abs(h - pc), abs(l - pc))
    for i in range(n):
        if i < period - 1:
            continue
        window = [v for v in tr[i - period + 1:i + 1] if v is not None]
        if len(window) < period:
            continue
        atr[i] = sum(window) / len(window)
    return atr


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
        best_pause_days = max(g["pause_days"] for g in group)
        entry = {
            "ema_periods": periods,
            "compound": len(group) > 1,
            "breakdown_date": dates[breakdown_idx],
            "recovery_date": dates[recovery_idx],
            "days_to_recover": recovery_idx - breakdown_idx,
            "pause_days": best_pause_days,
            "pause_valid": any(g["pause_valid"] for g in group),
            "details": [
                {"ema_period": g["ema_period"], "breakdown_date": g["breakdown_date"],
                 "breakdown_close": g["breakdown_close"], "ema_value": g["ema_value"],
                 "recovery_date": g["recovery_date"], "recovery_close": g["recovery_close"],
                 "pause_days": g["pause_days"], "pause_valid": g["pause_valid"],
                 "pause_band_low": g["pause_band_low"], "pause_band_high": g["pause_band_high"],
                 "pause_end_date": g["pause_end_date"]}
                for g in sorted(group, key=lambda x: x["ema_period"])
            ],
            "as_of_date": as_of_date, "as_of_close": as_of_close,
        }
        merged.append(entry)
    return merged


def _detect_pause(closes, ema, atr, recovery_idx, breakdown_idx, n,
                   min_pause_days=2, max_pause_days=15):
    """Checks the days AFTER the reclaim/recovery day for a tight pause/base.

    Rule (finalized in conversation):
      - Anchor = reclaim_close (Close on recovery_idx).
      - ATR = 1x the 14-day ATR as of the breakdown day (pre-breakdown,
        so the shakeout's own volatility doesn't distort the band).
      - A day qualifies as a valid pause day if BOTH:
          reclaim_close - ATR <= close[j] <= reclaim_close + ATR
          close[j] > ema[j]   (still respecting the reclaimed EMA)
      - pause_days = length of the CONSECUTIVE run of valid days starting
        right after recovery_idx. First failing day ends the run.

    Returns a dict: pause_days, pause_valid, band_low, band_high,
    pause_end_date_idx (last valid pause index, or None).
    """
    reclaim_close = closes[recovery_idx]
    # ATR as of the breakdown day (pre-breakdown data only). Falls back to
    # the recovery day's ATR if that's unavailable (e.g. early in history).
    atr_ref = None
    if breakdown_idx - 1 >= 0 and atr[breakdown_idx - 1] is not None:
        atr_ref = atr[breakdown_idx - 1]
    elif atr[breakdown_idx] is not None:
        atr_ref = atr[breakdown_idx]
    elif atr[recovery_idx] is not None:
        atr_ref = atr[recovery_idx]

    if reclaim_close is None or atr_ref is None:
        return {"pause_days": 0, "pause_valid": False, "band_low": None,
                "band_high": None, "pause_end_idx": None}

    band_low = round(reclaim_close - atr_ref, 2)
    band_high = round(reclaim_close + atr_ref, 2)

    pause_days = 0
    last_valid_idx = None
    for j in range(recovery_idx + 1, min(n, recovery_idx + 1 + max_pause_days)):
        c, e = closes[j], ema[j]
        if c is None or e is None:
            break
        if not (band_low <= c <= band_high):
            break
        if not (c > e):
            break
        pause_days += 1
        last_valid_idx = j

    return {
        "pause_days": pause_days,
        "pause_valid": pause_days >= min_pause_days,
        "band_low": band_low,
        "band_high": band_high,
        "pause_end_idx": last_valid_idx,
    }


def detect_shakeout(s, ema_periods=(10, 21, 50), min_days_above=None, max_recovery_days=5,
                     lookback_days=260, min_pause_days=2, max_pause_days=15, atr_period=14):
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
    atr = _calc_atr(highs, lows, closes, period=atr_period)

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

            pause = _detect_pause(closes, ema, atr, recovery_idx, i, n,
                                   min_pause_days=min_pause_days,
                                   max_pause_days=max_pause_days)

            raw_signals.append({
                "ema_period": period,
                "breakdown_idx": i, "recovery_idx": recovery_idx,
                "breakdown_date": dates[i],
                "breakdown_close": round(closes[i], 2),
                "ema_value": round(ema[i], 2),
                "recovery_date": dates[recovery_idx],
                "recovery_close": round(closes[recovery_idx], 2),
                "days_to_recover": recovery_idx - i,
                "pause_days": pause["pause_days"],
                "pause_valid": pause["pause_valid"],
                "pause_band_low": pause["band_low"],
                "pause_band_high": pause["band_high"],
                "pause_end_date": dates[pause["pause_end_idx"]] if pause["pause_end_idx"] is not None else None,
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
    ap.add_argument("--min-pause-days", type=int, default=2,
                     help="min consecutive tight-pause days after reclaim (default 2)")
    ap.add_argument("--max-pause-days", type=int, default=15,
                     help="how many days after reclaim to scan for the pause window")
    ap.add_argument("--atr-period", type=int, default=14)
    ap.add_argument("--pause-only", action="store_true",
                     help="only keep signals where a valid pause/base formed")
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
                                    max_recovery_days=args.max_recovery_days,
                                    min_pause_days=args.min_pause_days,
                                    max_pause_days=args.max_pause_days,
                                    atr_period=args.atr_period):
            signals.append({"symbol": sym, **sig})

    print(f"Skipped (illiquid): {skipped_illiquid}")
    if args.pause_only:
        signals = [x for x in signals if x["pause_valid"]]
    signals.sort(key=lambda x: x["breakdown_date"], reverse=True)
    label = "Shakeout+Pause signals found" if args.pause_only else "Shakeout signals found"
    print(f"{label}: {len(signals)}\n")
    for x in signals:
        emas_str = "+".join(f"EMA{p}" for p in x["ema_periods"])
        tag = " [COMPOUND]" if x["compound"] else ""
        pause_tag = f" [PAUSE OK: {x['pause_days']}d]" if x["pause_valid"] else f" [pause: {x['pause_days']}d, not enough]"
        print(f"  {x['symbol']:<15} {emas_str:<15}{tag}{pause_tag} "
              f"breakdown {x['breakdown_date']}  recovered {x['recovery_date']} (+{x['days_to_recover']}d)")
        for d in x["details"]:
            print(f"      EMA{d['ema_period']}: {d['breakdown_date']} @ {d['breakdown_close']} "
                  f"(EMA {d['ema_value']}) -> recovered {d['recovery_date']} @ {d['recovery_close']}  "
                  f"band [{d['pause_band_low']}, {d['pause_band_high']}]")

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
