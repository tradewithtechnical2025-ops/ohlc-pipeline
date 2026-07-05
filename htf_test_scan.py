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

def _htf_swing_low_idx(lows, end_idx, lookback, floor=0):
    start = max(0, end_idx - lookback, floor)
    seg = [(i, lows[i]) for i in range(start, end_idx + 1) if lows[i] is not None]
    if not seg:
        return None
    return min(seg, key=lambda x: x[1])[0]


def _try_build_htf_match(lo, hi, dates, highs, lows, closes, n,
                          pole_min_days, pole_max_days, min_gain_pct, max_gain_pct,
                          max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple):
    """Validates a single (lo, hi) candidate pole and, if it holds up, builds
    the full match dict (including flag walk + post-breakout tracking).
    Returns None if this specific (lo, hi) pair doesn't qualify, so the
    caller can try a different lo for the same hi."""
    if lo is None or lo >= hi:
        return None

    pole_high = highs[hi]
    pole_low = lows[lo]
    pole_days = hi - lo
    if pole_days < pole_min_days or pole_days > pole_max_days:
        return None
    if not pole_low or pole_low <= 0:
        return None

    gain_pct = (pole_high - pole_low) / pole_low * 100.0
    if gain_pct < min_gain_pct:
        return None
    if max_gain_pct is not None and gain_pct >= max_gain_pct:
        return None

    if any(highs[k] is not None and highs[k] > pole_high for k in range(lo, hi)):
        return None
    # Forward check uses CLOSES, not highs: a brief intraday wick above the
    # peak shouldn't disqualify it as "the" pole-top — only a sustained
    # CLOSE meaningfully above it means the rally genuinely continued and
    # this candidate wasn't the real peak yet.
    peak_check_end = min(n, hi + 5)
    if any(closes[k] is not None and closes[k] > pole_high * 1.03 for k in range(hi + 1, peak_check_end)):
        return None

    # Walk forward day-by-day from the pole high, tracking the flag's
    # cumulative low (for pullback/failure AND as the next pole's future
    # base) and high (informational only).
    #
    # BREAKOUT THRESHOLD IS FIXED AT pole_high — not a moving cum_high, so a
    # separate new rally forming inside this flag's window can't silently
    # extend THIS pole's breakout threshold.
    flag_end_max = min(n - 1, hi + flag_max_days)
    cum_low = pole_high
    cum_low_idx = hi
    cum_high = pole_high
    status = "forming"
    resolved_idx = None

    for j in range(hi + 1, flag_end_max + 1):
        c, h, l = closes[j], highs[j], lows[j]

        if c is not None and c > pole_high:
            status = "breakout"
            resolved_idx = j
            break

        if h is not None:
            cum_high = max(cum_high, h)
        if l is not None and l < cum_low:
            cum_low = l
            cum_low_idx = j

        pullback_pct = (pole_high - cum_low) / pole_high * 100.0
        if pullback_pct > max_pullback_pct:
            status = "failed"
            resolved_idx = j
            break

    fe = resolved_idx if resolved_idx is not None else flag_end_max
    pullback_pct = (pole_high - cum_low) / pole_high * 100.0
    flag_days = fe - hi

    # If breakout fired before the minimum flag duration, this candidate's
    # consolidation was too short to qualify as this preset's pattern
    # (e.g. HTF needs a 3-5 week flag) — drop it entirely.
    if status == "breakout" and flag_days < flag_min_days:
        return None

    flag_low_final = cum_low
    flag_high_final = cum_high
    last_close = closes[-1]

    # hi is the very last available bar -> the pole just completed today
    # with zero days of consolidation yet — label distinctly.
    if flag_days == 0 and status == "forming":
        status = "pole_just_formed"

    # ── Post-breakout tracking ──
    # Target is dynamic per-stock: risk = distance from breakout_close down
    # to flag_low, target = success_rr_multiple x that risk. Resolve by
    # whichever happens FIRST, chronologically.
    post_info = {}
    if status == "breakout":
        bo_idx = resolved_idx
        breakout_close = closes[bo_idx]
        post_high = breakout_close
        post_high_date = dates[bo_idx]
        success_idx = None
        broke_back_idx = None

        risk_pct = ((breakout_close - flag_low_final) / breakout_close * 100.0
                    if breakout_close else None)
        target_gain_pct = (risk_pct * success_rr_multiple) if risk_pct is not None else None

        for k in range(bo_idx + 1, n):
            if highs[k] is not None and highs[k] > post_high:
                post_high = highs[k]
                post_high_date = dates[k]

            if success_idx is None and breakout_close and target_gain_pct is not None:
                gain_so_far = (post_high - breakout_close) / breakout_close * 100.0
                if gain_so_far >= target_gain_pct:
                    success_idx = k

            if broke_back_idx is None and lows[k] is not None and lows[k] < flag_low_final:
                broke_back_idx = k

        if success_idx is not None and (broke_back_idx is None or success_idx <= broke_back_idx):
            status = "success"
        elif broke_back_idx is not None and (success_idx is None or broke_back_idx < success_idx):
            status = "breakout_failed"

        post_info = {
            "breakout_date": dates[bo_idx],
            "breakout_close": round(breakout_close, 2) if breakout_close is not None else None,
            "risk_pct": round(risk_pct, 1) if risk_pct is not None else None,
            "target_gain_pct": round(target_gain_pct, 1) if target_gain_pct is not None else None,
            "post_breakout_high": round(post_high, 2),
            "post_breakout_high_date": post_high_date,
            "post_breakout_gain_pct": round((post_high - breakout_close) / breakout_close * 100, 1)
                if breakout_close else None,
            "success_date": dates[success_idx] if status == "success" else None,
            "broke_flag_low_date": dates[broke_back_idx] if status == "breakout_failed" else None,
        }

    match = {
        "pole_low_date": dates[lo], "pole_low": round(pole_low, 2),
        "pole_high_date": dates[hi], "pole_high": round(pole_high, 2),
        "pole_gain_pct": round(gain_pct, 1), "pole_days": pole_days,
        "flag_end_date": dates[fe], "flag_low": round(flag_low_final, 2),
        "flag_high": round(flag_high_final, 2), "flag_pullback_pct": round(pullback_pct, 1),
        "flag_days": flag_days, "as_of_date": dates[-1],
        "as_of_close": round(last_close, 2) if last_close is not None else None,
        "status": status,
    }
    match.update(post_info)
    match["_cum_low_idx"] = cum_low_idx  # internal use: chaining to next pole's base
    match["_fe"] = fe
    return match


def detect_htf(s, min_gain_pct=90.0, max_gain_pct=None, pole_min_days=10, pole_max_days=40,
               max_pullback_pct=25.0, flag_min_days=10, flag_max_days=40,
               lookback_days=260, success_rr_multiple=2.0):
    dates, highs, lows, closes = s["d"], s["h"], s["l"], s["c"]
    n = len(dates)
    if n < pole_min_days + flag_min_days:
        return []

    matches = []
    scan_start = max(0, n - lookback_days)

    # chain_lo: the next candidate pole's base is normally the PREVIOUS pole's
    # own flag-low point (the lowest point its flag actually reached) — not a
    # fresh independent swing-low search. This is what naturally happens on a
    # real chart: pole1 tops out, pulls back to some low, and pole2 builds
    # directly off that same low.
    #
    # But if chaining off that exact point doesn't produce a valid pole for a
    # given hi (e.g. the combined gain from that far back is too large for
    # this preset), we still try a plain fallback swing-low search for that
    # SAME hi before giving up on it — since the "real" base for this next
    # leg may be a later, higher low than the one we're chained to.
    chain_lo = None
    fallback_floor = -1

    for hi in range(scan_start + pole_min_days, n):
        if highs[hi] is None:
            continue

        candidates = []
        if chain_lo is not None and hi - chain_lo <= pole_max_days:
            candidates.append(chain_lo)
        search_lo = _htf_swing_low_idx(lows, hi, pole_max_days, floor=fallback_floor)
        if search_lo is not None and search_lo not in candidates:
            candidates.append(search_lo)

        match = None
        for lo in candidates:
            match = _try_build_htf_match(lo, hi, dates, highs, lows, closes, n,
                                          pole_min_days, pole_max_days, min_gain_pct, max_gain_pct,
                                          max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple)
            if match is not None:
                break

        if match is None:
            continue

        cum_low_idx = match.pop("_cum_low_idx")
        fe = match.pop("_fe")
        matches.append(match)
        chain_lo = cum_low_idx if cum_low_idx > hi else None
        fallback_floor = fe + 2

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


def upload_to_r2(filename, data_str):
    """Same convention as pipeline.py's r2_upload(): POST to {WORKER_URL}?file={filename}
    with the X-Secret-Token header."""
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


PRESETS = {
    "HTF": dict(min_gain_pct=90.0, max_pullback_pct=25.0, pole_min_days=10,
                pole_max_days=40, flag_min_days=15, flag_max_days=25,
                success_rr_multiple=2.0),
    "Mini HTF": dict(min_gain_pct=20.0, max_gain_pct=90.0, max_pullback_pct=15.0, pole_min_days=10,
                     pole_max_days=40, flag_min_days=5, flag_max_days=40,
                     success_rr_multiple=2.0),
}


def main():
    ap = argparse.ArgumentParser(description="Full-universe HTF scan test (no full pipeline run)")
    ap.add_argument("--save", help="optional path to save results as JSON (local file)")
    ap.add_argument("--r2-key", help="optional R2 filename to push results to, e.g. htf_test_results.json")
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

    all_results = {}
    for label, params in PRESETS.items():
        signals = []
        for sym, s in liquid.items():
            for m in detect_htf(s, **params):
                if m["status"] == "failed":
                    continue   # never confirmed a valid HTF flag — not actionable, skip
                signals.append({"symbol": sym, "pattern": label, **m})

        order = {"success": 0, "breakout": 1, "forming": 2, "pole_just_formed": 3,
                 "breakout_failed": 4, "failed": 5}
        signals.sort(key=lambda x: (order.get(x["status"], 9), -x["pole_gain_pct"]))
        all_results[label] = signals

        success = sum(1 for x in signals if x["status"] == "success")
        breakout = sum(1 for x in signals if x["status"] == "breakout")
        forming = sum(1 for x in signals if x["status"] == "forming")
        pole_just_formed = sum(1 for x in signals if x["status"] == "pole_just_formed")
        breakout_failed = sum(1 for x in signals if x["status"] == "breakout_failed")

        print(f"=== {label}  (gain>={params['min_gain_pct']}%  pullback<={params['max_pullback_pct']}%  "
              f"flag_min_days={params['flag_min_days']}  success={params['success_rr_multiple']}x flag risk) ===")
        print(f"Found: {len(signals)}  (Success: {success}  Breakout: {breakout}  Forming: {forming}  "
              f"Pole-Just-Formed: {pole_just_formed}  Breakout-Failed: {breakout_failed})  "
              f"[note: 'failed' patterns (never broke out) are excluded entirely]\n")
        for x in signals:
            line = (f"  {x['symbol']:<15} {x['status'].upper():<16} "
                    f"pole +{x['pole_gain_pct']}%  flag pullback {x['flag_pullback_pct']}%  "
                    f"pole: {x['pole_low_date']}->{x['pole_high_date']}  flag through {x['flag_end_date']}")
            if x["status"] in ("success", "breakout", "breakout_failed"):
                line += (f"  | BO on {x['breakout_date']} @ {x['breakout_close']}"
                         f"  (risk {x['risk_pct']}%  target {x['target_gain_pct']}%)"
                         f"  ran to {x['post_breakout_high']} (+{x['post_breakout_gain_pct']}%)")
                if x["status"] == "success":
                    line += f"  TARGET HIT on {x['success_date']}"
                if x["status"] == "breakout_failed":
                    line += f"  BROKE flag_low on {x['broke_flag_low_date']}"
            print(line)
        print()

    if args.save:
        with open(args.save, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"Saved to {args.save}")

    if args.r2_key:
        print(f"\nPushing results to R2 as {args.r2_key}...")
        upload_to_r2(args.r2_key, json.dumps(all_results))


if __name__ == "__main__":
    main()
