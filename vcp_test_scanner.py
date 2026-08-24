#!/usr/bin/env python3
"""
vcp_test_scanner.py
--------------------
Standalone VCP (Volatility Contraction Pattern) tester — separate from
pipeline.py so it can be run/tuned/debugged without touching the main
production pipeline. Uses the EXACT same detection logic as pipeline.py's
_detect_vcp() (resistance/pivot-chain based, percentage-ZigZag on closes,
nested-swing filtering, asymmetric ceiling check for flat/descending
resistance bases).

USAGE — single stock from a TradingView CSV export:
    python vcp_test_scanner.py --csv NSE_SOLARINDS__1D.csv
    python vcp_test_scanner.py --csv NSE_SOLARINDS__1D.csv --debug
    python vcp_test_scanner.py --csv NSE_SOLARINDS__1D.csv --end-date 2024-03-02

USAGE — single stock, or full universe, from R2 (same OHLC store as pipeline.py):
    export WORKER_URL="https://your-worker-url"
    export WORKER_TOKEN="your-secret-token"
    python vcp_test_scanner.py --symbol SOLARINDS
    python vcp_test_scanner.py --all --save vcp_test_results.json
    python vcp_test_scanner.py --all --r2-key vcp_signals_test.json

Flags:
    --csv PATH        Read OHLC from a TradingView-exported CSV (time,open,
                       high,low,close,...,Volume,...). Overrides --symbol/--all.
    --symbol SYM       Fetch just one symbol's history from R2 (ohlc_*.json chunks).
    --all              Scan every symbol in R2 (same universe as pipeline.py vcp_scan).
    --end-date YYYY-MM-DD
                       Truncate history to end on/before this date — useful for
                       replaying "what would the scan have said on that day",
                       e.g. testing a base right before a historical breakout.
    --debug            Print the full pivot list, nested-filter before/after,
                       and every candidate base tried (not just the winner).
    --params k=v,k=v   Override any _detect_vcp() keyword arg, e.g.
                       --params zigzag_pct=0.05,max_final_depth=0.15
    --save PATH        Write results as JSON to a local file.
    --r2-key NAME      Push results JSON to R2 under this filename.
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone

import httpx

R2_CHUNKS = 8
WORKER_URL = os.environ.get("WORKER_URL", "").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}


# ══════════════════════════════════════════════════════════════
# VCP core detection logic — kept byte-identical to pipeline.py so this
# script always tests exactly what production runs. If you tune params
# here, port the same change back into pipeline.py's _detect_vcp.
# ══════════════════════════════════════════════════════════════

def _vcp_sma(arr, period, end=None):
    end = len(arr) if end is None else end
    if end < period: return None
    seg = arr[end - period:end]
    if not seg or any(v is None for v in seg): return None
    return sum(seg) / period

def _vcp_zigzag_pct(highs, lows, pct_threshold=0.04):
    n = len(highs)
    if n < 2: return []
    piv = []
    ext_high = highs[0]; ext_high_idx = 0
    ext_low  = lows[0];  ext_low_idx  = 0
    direction = None
    for i in range(1, n):
        h, l = highs[i], lows[i]
        if h is None or l is None: continue
        if ext_high is None or h > ext_high: ext_high, ext_high_idx = h, i
        if ext_low  is None or l < ext_low:  ext_low,  ext_low_idx  = l, i
        if direction is None:
            if ext_high is not None and l <= ext_high * (1 - pct_threshold):
                piv.append((ext_high_idx, ext_high, "H", i))
                direction = "down"; ext_low, ext_low_idx = l, i
            elif ext_low is not None and h >= ext_low * (1 + pct_threshold):
                piv.append((ext_low_idx, ext_low, "L", i))
                direction = "up"; ext_high, ext_high_idx = h, i
        elif direction == "up":
            if l <= ext_high * (1 - pct_threshold):
                piv.append((ext_high_idx, ext_high, "H", i))
                direction = "down"; ext_low, ext_low_idx = l, i
        else:
            if h >= ext_low * (1 + pct_threshold):
                piv.append((ext_low_idx, ext_low, "L", i))
                direction = "up"; ext_high, ext_high_idx = h, i
    return piv

def _vcp_zigzag_close_pct(highs, lows, closes, pct_threshold=0.04):
    n = len(closes)
    if n < 2: return []
    close_piv = _vcp_zigzag_pct(closes, closes, pct_threshold)
    if not close_piv: return []
    piv = []
    span_start = 0
    for idx, _price, kind, confirm_idx in close_piv:
        scan_end = confirm_idx
        seg = highs[span_start:scan_end+1] if kind == "H" else lows[span_start:scan_end+1]
        vals = [(span_start + off, v) for off, v in enumerate(seg) if v is not None]
        if vals:
            true_idx, true_price = (max(vals, key=lambda x: x[1]) if kind == "H"
                                     else min(vals, key=lambda x: x[1]))
            piv.append((true_idx, true_price, kind))
            span_start = true_idx + 1
        else:
            span_start = idx + 1
    return piv

def _vcp_filter_nested(piv, max_nested_ratio=0.65):
    if len(piv) < 5: return piv
    out = list(piv)
    i = 1
    while i < len(out) - 2:
        pa, pb = out[i], out[i + 1]
        prev_p, next_p = out[i - 1], out[i + 2]
        nested = False
        if pa[2] == "H" and pb[2] == "L":
            if pa[1] <= next_p[1] and pb[1] >= prev_p[1]: nested = True
        elif pa[2] == "L" and pb[2] == "H":
            if pa[1] >= next_p[1] and pb[1] <= prev_p[1]: nested = True
        if nested:
            inner = abs(pa[1] - pb[1])
            left = abs(prev_p[1] - pa[1])
            right = abs(pb[1] - next_p[1])
            neighbor = min(left, right) if left and right else max(left, right)
            if neighbor and inner <= neighbor * max_nested_ratio:
                del out[i:i + 2]
                continue
        i += 1
    return out


def _detect_vcp(hist, lookback=150, zigzag_pct=0.04, min_contractions=3, max_contractions=6,
                max_base_depth=0.45, max_final_depth=0.12, tighten_tol=0.03,
                max_ceiling_jump=0.02, max_dist_from_pivot=0.08, min_prior_move=0.20,
                max_52wh_dist=0.20, max_post_breakout_run=0.03,
                live_min_bars=5, live_min_depth=0.02, min_first_leg_bars=15,
                ceiling_band_tol=0.04, min_leg_span_bars=5, debug=False):
    highs  = hist.get("h") or []
    lows   = hist.get("l") or []
    closes = hist.get("c") or []
    vols   = hist.get("v") or []
    dates  = hist.get("d") or []
    n = len(closes)

    if n < 60: return None
    if any(x is None for x in (closes[-1], highs[-1], lows[-1])): return None
    last_close = closes[-1]

    sma50 = _vcp_sma(closes, 50)
    sma150 = _vcp_sma(closes, 150) if n >= 150 else _vcp_sma(closes, min(n, 100))
    if sma50 is None or sma150 is None: return None
    if not (last_close > sma50 > sma150): return None

    lb = min(lookback, n)
    start = n - lb
    h_w = highs[start:]; l_w = lows[start:]; c_w = closes[start:]
    piv = _vcp_zigzag_close_pct(h_w, l_w, c_w, zigzag_pct)
    piv = [(i + start, p, k) for (i, p, k) in piv]
    piv = _vcp_filter_nested(piv)
    if len(piv) < 3: return None

    h_pivots = [p for p in piv if p[2] == "H"]
    if not h_pivots: return None

    def _try_base(base_high, dbg=False):
        seq = [p for p in piv if p[0] >= base_high[0]]
        if not seq or seq[0][2] != "H": return None

        search_start = max(0, base_high[0] - 252)
        prior_lows = [lows[i] for i in range(search_start, base_high[0]) if lows[i] is not None]
        if not prior_lows: return None
        prior_low = min(prior_lows)
        prior_move = (base_high[1] - prior_low) / prior_low
        if prior_move < min_prior_move: return None

        contractions = []
        i = 0
        while i < len(seq) - 1:
            if seq[i][2] == "H" and seq[i+1][2] == "L":
                hi, hp = seq[i][0], seq[i][1]
                span_end = seq[i+2][0] if i+2 < len(seq) else n - 1
                span = [(idx, lows[idx]) for idx in range(hi+1, span_end+1) if lows[idx] is not None]
                if span:
                    li, lp = min(span, key=lambda x: x[1])
                else:
                    li, lp = seq[i+1][0], seq[i+1][1]
                n_bars = li - hi
                if hp > 0 and n_bars >= min_leg_span_bars:
                    contractions.append((hi, hp, li, lp, (hp - lp) / hp))
                i += 2
            else:
                i += 1

        last_piv = seq[-1]
        live_leg = None
        if last_piv[2] == "H":
            hi, hp = last_piv[0], last_piv[1]
            span = [(idx, lows[idx]) for idx in range(hi + 1, n) if lows[idx] is not None]
            if span:
                li, lp = min(span, key=lambda x: x[1])
                depth = (hp - lp) / hp if hp > 0 else 0
                if hp > 0 and (n - 1 - hi) >= live_min_bars and depth >= live_min_depth:
                    live_leg = (hi, hp, li, lp, depth)
        else:
            after = [(idx, highs[idx]) for idx in range(last_piv[0] + 1, n) if highs[idx] is not None]
            if after:
                hi, hp = max(after, key=lambda x: x[1])
                span = [(idx, lows[idx]) for idx in range(hi + 1, n) if lows[idx] is not None]
                if span:
                    li, lp = min(span, key=lambda x: x[1])
                    depth = (hp - lp) / hp if hp > 0 else 0
                    if hp > 0 and (n - 1 - hi) >= live_min_bars and depth >= live_min_depth:
                        live_leg = (hi, hp, li, lp, depth)

        if live_leg is not None:
            if contractions and contractions[-1][0] == live_leg[0]:
                if live_leg[3] < contractions[-1][3]:
                    contractions[-1] = live_leg
            elif not contractions or live_leg[0] > contractions[-1][0]:
                contractions.append(live_leg)

        if len(contractions) < min_contractions: return None

        # ---- METHOD A: two-pass zigzag-chain walk. Ceiling check is
        # IMMEDIATE-NEIGHBOR only -- no high may jump up more than
        # max_ceiling_jump versus the leg right before it, anywhere in the
        # chain (not just the final/pivot leg). Legitimate revisits of an
        # OLDER, higher ceiling (like ABB's case) are handled separately by
        # METHOD B (ceiling-cluster) below, so Method A no longer needs to
        # reach back through unconfirmed earlier legs to excuse a jump --
        # that was what let CRISIL's chain through with a 9.7% jump on its
        # final leg versus its immediate neighbor, which is exactly the
        # pattern that shouldn't be excused.
        depths = [c[4] for c in contractions]
        run_end = len(depths) - 1
        j = run_end - 1
        while j >= 0:
            if depths[j] < depths[j+1] - tighten_tol:
                break
            hi_a, hi_b = contractions[j][1], contractions[j+1][1]
            if hi_b > hi_a * (1 + max_ceiling_jump):
                break
            j -= 1
        run_a = contractions[j+1:]

        # ---- METHOD B: ceiling-cluster (handles flat-top, multi-touch
        # cup-with-handle shapes -- only the H's that actually touch the
        # base's own ceiling count as contraction boundaries; smaller
        # internal highs that never approach the ceiling are treated as
        # noise inside the base, not separate contractions) ----
        h_only = [c for c in contractions if True]  # contractions are H->L legs; each c[1] is an H
        ceiling = max(c[1] for c in contractions)
        node_idxs = [idx for idx, c in enumerate(contractions) if c[1] >= ceiling * (1 - ceiling_band_tol)]
        run_b = None
        if len(node_idxs) >= 2:
            run_b = []
            for a_i, b_i in zip(node_idxs, node_idxs[1:]):
                hi, hp = contractions[a_i][0], contractions[a_i][1]
                span_end = contractions[b_i][0]
                span = [(idx, lows[idx]) for idx in range(hi+1, span_end+1) if lows[idx] is not None]
                if not span: continue
                li, lp = min(span, key=lambda x: x[1])
                run_b.append((hi, hp, li, lp, (hp-lp)/hp if hp>0 else 0))
            last_idx = node_idxs[-1]
            last_c = contractions[last_idx]
            if last_c[0] == contractions[-1][0]:
                run_b.append(contractions[-1])

        def _validate_and_score(run):
            if run is None: return None
            run_depths = [c[4] for c in run]
            if not (min_contractions <= len(run) <= max_contractions): return None
            for k in range(1, len(run)):
                if run[k][3] < run[k-1][3]: return None
            if len(run) >= 2 and (run[1][0] - run[0][0]) < min_first_leg_bars: return None
            for k in range(len(run)):
                low_k = run[k][3]; check_from = run[k][2]
                for idx in range(check_from + 1, n):
                    if lows[idx] is not None and lows[idx] < low_k: return None
            for k in range(1, len(run_depths)):
                if run_depths[k] >= run_depths[k-1] + tighten_tol: return None
            base_depth  = run_depths[0]
            final_depth = run_depths[-1]
            if base_depth > max_base_depth: return None
            if final_depth > max_final_depth: return None
            pivot_price = run[-1][1]
            if pivot_price <= 0: return None
            dist = (pivot_price - last_close) / pivot_price
            if dist > max_dist_from_pivot or dist < -0.02: return None
            post_base_start = run[-1][2] + 1
            if post_base_start < n:
                highs_since = [highs[idx] for idx in range(post_base_start, n) if highs[idx] is not None]
                if highs_since and max(highs_since) > pivot_price * (1 + max_post_breakout_run):
                    return None
            def _leg_vol(c):
                a, b = c[0], c[2]
                seg = [v for v in vols[a:b+1] if v]
                return sum(seg) / len(seg) if seg else 0
            first_vol = _leg_vol(run[0])
            last_vol  = _leg_vol(run[-1])
            vol_dryup = last_vol < first_vol * 0.75 if first_vol else False
            base_start = run[0][0]; base_end = run[-1][2]
            base_len = base_end - base_start
            if base_len < 10: return None
            score = 0
            score += min(len(run), 4) * 10
            score += max(0, (max_final_depth - final_depth) / max_final_depth) * 25
            score += max(0, (0.25 - (last_vol / first_vol if first_vol else 1)) / 0.25) * 20
            score += max(0, (max_dist_from_pivot - abs(dist)) / max_dist_from_pivot) * 15
            score += min(prior_move / 1.0, 1.0) * 10
            score = round(min(score, 100), 1)
            return {
                "is_vcp": True, "contractions": len(run),
                "depths_pct": [round(d * 100, 1) for d in run_depths],
                "base_depth_pct": round(base_depth * 100, 1),
                "final_depth_pct": round(final_depth * 100, 1),
                "pivot": round(pivot_price, 2),
                "pivot_date": dates[run[-1][0]] if dates else None,
                "base_start_date": dates[run[0][0]] if dates else None,
                "base_end_date": dates[run[-1][2]] if dates else None,
                "resistance_shape": "descending" if run[0][1] > run[-1][1] * 1.01 else "flat",
                "contraction_dates": [
                    {"h_date": dates[c[0]], "h_price": round(c[1], 2),
                     "l_date": dates[c[2]], "l_price": round(c[3], 2)}
                    for c in run if dates
                ],
                "dist_from_pivot_pct": round(dist * 100, 2),
                "vol_dryup": vol_dryup,
                "prior_move_pct": round(prior_move * 100, 1),
                "base_len": base_len,
                "score": score,
                "method": "zigzag_chain",
            }

        res_a = _validate_and_score(run_a)
        res_b = _validate_and_score(run_b)
        if res_b is not None:
            res_b["method"] = "ceiling_cluster"
        candidates = [r for r in (res_a, res_b) if r is not None]
        if not candidates: return None
        # Ceiling-cluster preferred whenever valid -- it represents the
        # cleaner, textbook "flat-top, multiple ceiling touches" shape.
        # zigzag_chain (which can carry extra internal-noise legs) is only
        # used as a fallback when ceiling_cluster itself isn't valid, e.g.
        # genuine descending-resistance bases where highs never cluster.
        return res_b if res_b is not None else res_a

    best = None
    for base_high in sorted(h_pivots, key=lambda x: -x[0]):
        result = _try_base(base_high, dbg=debug)
        if result and (best is None or result["score"] > best["score"]):
            best = result
    return best




def load_csv(path, end_date=None):
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError(f"{path}: no rows found")

    vol_col = next((c for c in rows[0].keys() if c.strip().lower() == "volume"), None)

    dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
    for r in rows:
        d = r.get("time", "").strip()
        if not d:
            continue
        if end_date and d > end_date:
            break
        try:
            dates.append(d)
            opens.append(float(r["open"]))
            highs.append(float(r["high"]))
            lows.append(float(r["low"]))
            closes.append(float(r["close"]))
            vols.append(float(r[vol_col]) if vol_col and r.get(vol_col) else None)
        except (KeyError, ValueError):
            continue

    if not dates:
        raise ValueError(f"{path}: no usable rows (check --end-date isn't before all data)")

    return {"d": dates, "o": opens, "h": highs, "l": lows, "c": closes, "v": vols}


# ══════════════════════════════════════════════════════════════
# R2 helpers (same convention as pipeline.py / shakeout_scanner.py)
# ══════════════════════════════════════════════════════════════

def _require_r2():
    if not WORKER_URL or not WORKER_TOKEN:
        print("ERROR: set WORKER_URL and WORKER_TOKEN env vars first.")
        sys.exit(1)


def download_all_chunks():
    _require_r2()
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


def download_one_symbol(symbol):
    all_data = download_all_chunks()
    s = all_data.get(symbol.upper())
    if s is None:
        print(f"ERROR: {symbol} not found in R2 OHLC store.")
        sys.exit(1)
    return s


def upload_to_r2(filename, data_str):
    _require_r2()
    url = f"{WORKER_URL}?file={filename}"
    with httpx.Client() as client:
        r = client.post(url, headers={**WORKER_HEADERS, "Content-Type": "application/json"},
                         content=data_str.encode(), timeout=90)
    if r.status_code != 200:
        print(f"  [warn] R2 upload failed for {filename}: HTTP {r.status_code} {r.text[:200]}")
        return False
    print(f"  ↑ {filename} ({len(data_str)/1024:.1f} KB) uploaded to R2")
    return True


def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    lookback = min(50, n)
    if lookback < 20:
        return True
    vols = [v for v in volumes[-lookback:] if v is not None]
    prices = [c for c in closes[-lookback:] if c is not None and c > 0]
    if len(vols) < 20 or len(prices) < 20:
        return False
    return (sum(vols)/len(vols) * sum(prices)/len(prices)) >= min_turnover


# ══════════════════════════════════════════════════════════════
# Debug printing
# ══════════════════════════════════════════════════════════════

def print_debug_trace(hist, params):
    highs, lows, closes, dates = hist["h"], hist["l"], hist["c"], hist["d"]
    n = len(closes)
    lookback = params.get("lookback", 150)
    zigzag_pct = params.get("zigzag_pct", 0.04)

    lb = min(lookback, n)
    start = n - lb
    h_w, l_w, c_w = highs[start:], lows[start:], closes[start:]

    piv_raw = _vcp_zigzag_close_pct(h_w, l_w, c_w, zigzag_pct)
    piv_raw = [(i + start, p, k) for (i, p, k) in piv_raw]
    piv_filtered = _vcp_filter_nested(piv_raw, params.get("max_nested_ratio", 0.65))

    print(f"\n--- DEBUG: pivots within last {lb} days (zigzag_pct={zigzag_pct*100:.0f}%) ---")
    print(f"Before nested-filter ({len(piv_raw)} pivots):")
    for i, p, k in piv_raw:
        print(f"   {dates[i]}  {k}  {p:.2f}")
    print(f"\nAfter nested-filter ({len(piv_filtered)} pivots):")
    for i, p, k in piv_filtered:
        print(f"   {dates[i]}  {k}  {p:.2f}")

    h_pivots = [p for p in piv_filtered if p[2] == "H"]
    print(f"\nCandidate base_high anchors tried (most recent first): "
          f"{[dates[p[0]] for p in sorted(h_pivots, key=lambda x: -x[0])]}")
    print()


def print_result(symbol, result):
    if not result:
        print(f"{symbol}: no VCP detected")
        return
    print(f"\n{symbol}  —  VCP detected  (score {result['score']}/100, "
          f"shape: {result['resistance_shape']})")
    print(f"  Pivot (buy point): ₹{result['pivot']}  ({result['dist_from_pivot_pct']}% from last close)")
    print(f"  Contractions ({result['contractions']}):")
    for c in result["contraction_dates"]:
        print(f"    {c['h_date']}  ₹{c['h_price']}  ->  {c['l_date']}  ₹{c['l_price']}")
    print(f"  Depths: {result['depths_pct']}")
    print(f"  Base: {result['base_start_date']} -> {result['base_end_date']}  "
          f"({result['base_len']} days)")
    print(f"  Prior move: {result['prior_move_pct']}%  |  Vol dry-up: {result['vol_dryup']}")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def _parse_params(s):
    out = {}
    if not s:
        return out
    for pair in s.split(","):
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        k, v = k.strip(), v.strip()
        try:
            out[k] = float(v) if "." in v or "e" in v.lower() else int(v)
        except ValueError:
            out[k] = v
    return out


def main():
    ap = argparse.ArgumentParser(description="Standalone VCP detector tester")
    ap.add_argument("--csv", help="path to a TradingView-exported daily CSV")
    ap.add_argument("--symbol", help="fetch one symbol's OHLC from R2")
    ap.add_argument("--all", action="store_true", help="scan every symbol in R2")
    ap.add_argument("--end-date", help="truncate history to end on/before this date (YYYY-MM-DD)")
    ap.add_argument("--debug", action="store_true", help="print pivot/filter trace")
    ap.add_argument("--params", help="override _detect_vcp kwargs, e.g. zigzag_pct=0.05,max_final_depth=0.15")
    ap.add_argument("--save", help="save results as JSON to this local path")
    ap.add_argument("--r2-key", help="push results JSON to R2 under this filename")
    args = ap.parse_args()

    extra_params = _parse_params(args.params)

    if args.csv:
        hist = load_csv(args.csv, end_date=args.end_date)
        symbol = os.path.basename(args.csv).split(".")[0]
        print(f"Loaded {len(hist['d'])} days: {hist['d'][0]} -> {hist['d'][-1]}")
        if args.debug:
            print_debug_trace(hist, extra_params)
        result = _detect_vcp(hist, **extra_params)
        print_result(symbol, result)
        if args.save:
            with open(args.save, "w") as f:
                json.dump({"symbol": symbol, "result": result}, f, indent=2)
            print(f"\nSaved to {args.save}")
        return

    if args.symbol:
        s = download_one_symbol(args.symbol)
        if args.end_date:
            keep = [i for i, d in enumerate(s["d"]) if d <= args.end_date]
            s = {k: [v[i] for i in keep] for k, v in s.items()}
        print(f"Loaded {len(s['d'])} days: {s['d'][0]} -> {s['d'][-1]}")
        if args.debug:
            print_debug_trace(s, extra_params)
        result = _detect_vcp(s, **extra_params)
        print_result(args.symbol.upper(), result)
        if args.save:
            with open(args.save, "w") as f:
                json.dump({"symbol": args.symbol.upper(), "result": result}, f, indent=2)
            print(f"\nSaved to {args.save}")
        return

    if args.all:
        print("Downloading OHLC chunks...")
        all_data = download_all_chunks()
        print(f"\nTotal loaded: {len(all_data)} stocks\n")

        signals = []
        skipped_illiquid = 0
        for sym, s in all_data.items():
            if not _check_liquidity(s.get("v", []), s.get("c", []), len(s.get("d", []))):
                skipped_illiquid += 1
                continue
            r = _detect_vcp(s, **extra_params)
            if r:
                signals.append({"symbol": sym, **r})

        signals.sort(key=lambda x: x["score"], reverse=True)
        print(f"Skipped (illiquid): {skipped_illiquid}")
        print(f"VCP signals found: {len(signals)}\n")
        for sig in signals[:30]:
            print(f"  {sig['symbol']:<15} score={sig['score']:<6} "
                  f"shape={sig['resistance_shape']:<11} "
                  f"contractions={sig['contractions']}  "
                  f"pivot=₹{sig['pivot']}  dist={sig['dist_from_pivot_pct']}%")
        if len(signals) > 30:
            print(f"  ... and {len(signals)-30} more")

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
        return

    ap.print_help()


if __name__ == "__main__":
    main()
