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
    """
    Percentage-based ZigZag — a pivot (H or L) is only confirmed once
    price has reversed by at least pct_threshold from the running
    extreme tracked since the last confirmed pivot. Amplitude-based,
    not time-based, so small noisy wiggles get filtered regardless of
    how many bars they span, while genuine reversals are always caught
    no matter how long they take to form.
    Returns [(idx, price, 'H'/'L', confirm_idx), ...] in chronological
    order — confirm_idx is the index where the reversal was actually
    DETECTED (which can be a day or more after the pivot's own idx),
    needed by callers that want to scan the true price extreme over
    the whole up/down swing, not just up to the pivot's own bar.
    """
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
        else:  # direction == "down"
            if h >= ext_low * (1 + pct_threshold):
                piv.append((ext_low_idx, ext_low, "L", i))
                direction = "up"; ext_high, ext_high_idx = h, i

    return piv

def _vcp_zigzag_close_pct(highs, lows, closes, pct_threshold=0.04):
    """
    Same idea as _vcp_zigzag_pct, but uses CLOSING prices to decide WHEN
    a swing reverses (far less noisy than using intrabar highs/lows — a
    single wide-range wick can otherwise trigger a spurious pivot on its
    own). Once a swing's direction change is confirmed via closes, the
    pivot's reported price is the TRUE extreme (highest high for 'H',
    lowest low for 'L') reached anywhere within that swing's span —
    scanned through confirm_idx (the day the reversal was detected),
    not just through the pivot's own close-based day. This matters
    because the single highest intrabar high of a swing can land on the
    very day price starts reversing (a day that already closes lower,
    but still prints a higher wick than the prior "peak" day).
    """
    n = len(closes)
    if n < 2: return []
    close_piv = _vcp_zigzag_pct(closes, closes, pct_threshold)
    if not close_piv: return []

    piv = []
    span_start = 0
    for idx, _price, kind, confirm_idx in close_piv:
        scan_end = confirm_idx  # not idx — extend through the confirmation day
        seg = highs[span_start:scan_end+1] if kind == "H" else lows[span_start:scan_end+1]
        vals = [(span_start + off, v) for off, v in enumerate(seg) if v is not None]
        if vals:
            true_idx, true_price = (max(vals, key=lambda x: x[1]) if kind == "H"
                                     else min(vals, key=lambda x: x[1]))
            piv.append((true_idx, true_price, kind))
            # FIX: advance from true_idx (the actual reported pivot day),
            # not idx (the close-based anchor day) — using idx here let
            # the next pivot's scan window overlap backward and re-include
            # the day just used as this pivot, which on an extreme-range
            # single candle (huge high AND huge low the same day) could
            # get double-counted as both an H pivot and the very next L
            # pivot's low.
            span_start = true_idx + 1
        else:
            span_start = idx + 1
    return piv


def _vcp_filter_nested(piv, max_nested_ratio=0.65):
    """
    Removes an interior H/L pivot pair when BOTH:
      (a) it is fully "nested" inside the surrounding bigger swing — its
          high does not exceed the NEXT pivot high and its low does not
          undercut the PREVIOUS pivot low (or the mirror L-then-H case), and
      (b) its own range is meaningfully smaller than its immediate
          neighboring legs (at most max_nested_ratio of the smaller one).
    Condition (b) is essential — without it, condition (a) alone cascades
    and collapses comparably-sized, genuinely separate contraction legs
    into one, since removing one nested pair changes what counts as the
    next pair's immediate neighbor and can trigger a runaway chain
    reaction. Only a SINGLE forward pass is made (no cascading re-scan
    from the start), so removal stays bounded to genuinely small nested
    wiggles — e.g. a brief 2-3 day pullback-and-bounce riding inside a
    much bigger multi-week decline/rally, which shouldn't count as its
    own separate VCP contraction.
    """
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


def _detect_vcp(hist, lookback=150, zigzag_pct=0.04, min_contractions=2, max_contractions=6,
                max_base_depth=0.45, max_final_depth=0.12, tighten_tol=0.02,
                max_ceiling_jump=0.05, max_dist_from_pivot=0.08, min_prior_move=0.20,
                max_52wh_dist=0.20, max_post_breakout_run=0.03):
    """
    VCP (Volatility Contraction Pattern) detector.

    Chains consecutive swing-high -> swing-low legs (via a closing-price
    percentage ZigZag, so pivot timing is amplitude-based and noise-
    resistant) into the longest run of progressively tightening
    contractions ending at the most recent leg.

    Handles BOTH common VCP shapes:
      - Flat/horizontal resistance — successive highs stay roughly level
        while lows rise (the "textbook" cup-after-cup base).
      - Descending resistance — a converging/symmetrical-triangle base
        where each successive high is itself LOWER than the one before,
        while lows still rise, narrowing the range from both sides.
    The chaining rule only blocks a leg from joining the current base
    when its high jumps *UP* by more than max_ceiling_jump versus the
    next leg — that signals a genuine breakout past the old base's
    ceiling into an unrelated, freshly-forming higher base. A high that
    is level OR LOWER than the next leg's is always allowed to chain,
    since that's completely normal for both base shapes above.
    """
    highs  = hist.get("h") or []
    lows   = hist.get("l") or []
    closes = hist.get("c") or []
    vols   = hist.get("v") or []
    dates  = hist.get("d") or []
    n = len(closes)

    if n < 60: return None
    if any(x is None for x in (closes[-1], highs[-1], lows[-1])): return None

    last_close = closes[-1]

    # ---- 0. Trend filter (Stage 2 uptrend) ----
    sma50 = _vcp_sma(closes, 50)
    sma150 = _vcp_sma(closes, 150) if n >= 150 else _vcp_sma(closes, min(n, 100))
    if sma50 is None or sma150 is None: return None
    if not (last_close > sma50 > sma150): return None

    # ---- 1. Pivots within lookback ----
    # (NOTE: an early global-52W-high proximity gate used to sit here, but
    # it compared last_close against the single highest point anywhere in
    # the last 252 days — for a descending-resistance VCP, that stale peak
    # was set BEFORE the base even started narrowing, and is well above
    # the base is current, relevant ceiling. That wrongly rejected valid
    # descending-triangle setups. The correct, per-candidate distance
    # check already happens in step 7 below (dist_from_pivot, measured
    # against the ACTUAL pivot of the base being evaluated) — no separate
    # blanket 52W check is needed on top of that.)
    lb = min(lookback, n)
    start = n - lb
    h_w = highs[start:]; l_w = lows[start:]; c_w = closes[start:]
    piv = _vcp_zigzag_close_pct(h_w, l_w, c_w, zigzag_pct)
    piv = [(i + start, p, k) for (i, p, k) in piv]
    piv = _vcp_filter_nested(piv)
    if len(piv) < 3: return None

    h_pivots = [p for p in piv if p[2] == "H"]
    if not h_pivots: return None

    def _try_base(base_high):
        seq = [p for p in piv if p[0] >= base_high[0]]
        if not seq or seq[0][2] != "H": return None

        # ---- 3. Prior move — from lowest point before base to base_high ----
        search_start = max(0, base_high[0] - 252)
        prior_lows = [lows[i] for i in range(search_start, base_high[0]) if lows[i] is not None]
        if not prior_lows: return None
        prior_low = min(prior_lows)
        prior_move = (base_high[1] - prior_low) / prior_low
        if prior_move < min_prior_move: return None

        # ---- 4. Build contractions (H -> next L), true min-low over the span ----
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
                if hp > 0 and n_bars >= 2:
                    contractions.append((hi, hp, li, lp, (hp - lp) / hp))
                i += 2
            else:
                i += 1

        # ---- 4b. Live/in-progress final leg — the tightest leg in a real
        # VCP is often the MOST RECENT one, and can genuinely be smaller
        # than zigzag_pct — which means the pivot detector's own threshold
        # could never "confirm" it as a discrete pivot on its own. Build a
        # live final leg from whatever's happened since the last CONFIRMED
        # pivot, using true intrabar highs/lows through TODAY rather than
        # waiting for a full threshold-confirmed reversal on either side:
        #   - if the chain currently ends on an H, extend that H's low
        #     using the true minimum low seen since, through today;
        #   - if it ends on an L, price may have already rallied to a
        #     fresh (unconfirmed) peak since then and be pulling back
        #     again — find that live peak's true high, then the true
        #     minimum low since THAT peak, through today.
        # Only replaces/extends an existing contraction when today's true
        # low is genuinely DEEPER than what's already recorded — this
        # never shrinks or removes an already-valid confirmed leg.
        last_piv = seq[-1]
        live_leg = None
        if last_piv[2] == "H":
            hi, hp = last_piv[0], last_piv[1]
            span = [(idx, lows[idx]) for idx in range(hi + 1, n) if lows[idx] is not None]
            if span:
                li, lp = min(span, key=lambda x: x[1])
                if hp > 0 and li - hi >= 2:
                    live_leg = (hi, hp, li, lp, (hp - lp) / hp)
        else:  # last_piv[2] == "L"
            after = [(idx, highs[idx]) for idx in range(last_piv[0] + 1, n) if highs[idx] is not None]
            if after:
                hi, hp = max(after, key=lambda x: x[1])
                span = [(idx, lows[idx]) for idx in range(hi + 1, n) if lows[idx] is not None]
                if span:
                    li, lp = min(span, key=lambda x: x[1])
                    if hp > 0 and li - hi >= 2:
                        live_leg = (hi, hp, li, lp, (hp - lp) / hp)

        if live_leg is not None:
            if contractions and contractions[-1][0] == live_leg[0]:
                if live_leg[3] < contractions[-1][3]:
                    contractions[-1] = live_leg
            elif not contractions or live_leg[0] > contractions[-1][0]:
                contractions.append(live_leg)

        if len(contractions) < min_contractions: return None

        # ---- 5. Longest tightening run ending at most recent contraction ----
        # Chain rule: depths must (roughly) tighten going forward, AND the
        # ceiling must not jump UP significantly between legs (a big rise
        # in the high from one leg to the next means a breakout happened
        # in between — that's an unrelated, newer base, not a continuation).
        # A ceiling that stays flat OR declines is always fine — that's
        # completely normal for both flat-resistance and descending-
        # resistance (converging triangle) VCP shapes.
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
        run = contractions[j+1:]
        run_depths = [c[4] for c in run]
        if not (min_contractions <= len(run) <= max_contractions): return None

        # ---- 5b. No contraction low must be broken by subsequent price ----
        for k in range(len(run)):
            low_k = run[k][3]
            check_from = run[k][2]
            for idx in range(check_from + 1, n):
                if lows[idx] is not None and lows[idx] < low_k:
                    return None

        # ---- 6. Strictly decreasing depths ----
        for k in range(1, len(run_depths)):
            if run_depths[k] >= run_depths[k-1]:
                return None

        base_depth  = run_depths[0]
        final_depth = run_depths[-1]
        if base_depth > max_base_depth: return None
        if final_depth > max_final_depth: return None

        # ---- 7. Pivot (buy point) ----
        pivot_price = run[-1][1]
        if pivot_price <= 0: return None
        dist = (pivot_price - last_close) / pivot_price
        if dist > max_dist_from_pivot or dist < -0.02: return None

        # ---- 7b. Reject if this is a POST-BREAKOUT RETEST, not a fresh
        # pre-breakout setup. dist alone can't tell these apart: a stock
        # that broke out weeks ago, rallied well past the pivot, and has
        # now pulled back down near/below it again will show the same
        # small "dist" as a stock that's genuinely still approaching the
        # pivot for the first time. Check whether price ever closed
        # meaningfully above the pivot at any point since the base's
        # final low — if so, the breakout already happened. ----
        post_base_start = run[-1][2] + 1
        if post_base_start < n:
            highs_since = [highs[idx] for idx in range(post_base_start, n) if highs[idx] is not None]
            if highs_since and max(highs_since) > pivot_price * (1 + max_post_breakout_run):
                return None

        # ---- 8. Volume dry-up — informational / scoring only ----
        def _leg_vol(c):
            a, b = c[0], c[2]
            seg = [v for v in vols[a:b+1] if v]
            return sum(seg) / len(seg) if seg else 0
        first_vol = _leg_vol(run[0])
        last_vol  = _leg_vol(run[-1])
        vol_dryup = last_vol < first_vol * 0.75 if first_vol else False

        # ---- 9. Base length check ----
        base_start = run[0][0]; base_end = run[-1][2]
        base_len = base_end - base_start
        if base_len < 10: return None

        # ---- 10. Score ----
        score = 0
        score += min(len(run), 4) * 10
        score += max(0, (max_final_depth - final_depth) / max_final_depth) * 25
        score += max(0, (0.25 - (last_vol / first_vol if first_vol else 1)) / 0.25) * 20
        score += max(0, (max_dist_from_pivot - abs(dist)) / max_dist_from_pivot) * 15
        score += min(prior_move / 1.0, 1.0) * 10
        score = round(min(score, 100), 1)

        return {
            "is_vcp"             : True,
            "contractions"       : len(run),
            "depths_pct"         : [round(d * 100, 1) for d in run_depths],
            "base_depth_pct"     : round(base_depth * 100, 1),
            "final_depth_pct"    : round(final_depth * 100, 1),
            "pivot"              : round(pivot_price, 2),
            "pivot_date"         : dates[run[-1][0]] if dates else None,
            "base_start_date"    : dates[run[0][0]]  if dates else None,
            "base_end_date"      : dates[run[-1][2]] if dates else None,
            "resistance_shape"   : "descending" if run[0][1] > run[-1][1] * 1.01 else "flat",
            "contraction_dates"  : [
                {"h_date": dates[c[0]], "h_price": round(c[1], 2),
                 "l_date": dates[c[2]], "l_price": round(c[3], 2)}
                for c in run if dates
            ],
            "dist_from_pivot_pct": round(dist * 100, 2),
            "vol_dryup"          : vol_dryup,
            "prior_move_pct"     : round(prior_move * 100, 1),
            "base_len"           : base_len,
            "score"              : score,
        }

    # Try every candidate base and keep the best-scoring valid result —
    # returning on the FIRST success (most-recent-first) meant an older
    # candidate producing a longer, more complete tightening chain (e.g.
    # after nested-pair merging exposes a earlier valid base_high) never
    # even got tried once a more recent, shorter-chain candidate already
    # succeeded on its own.
    best = None
    for base_high in sorted(h_pivots, key=lambda x: -x[0]):
        result = _try_base(base_high)
        if result and (best is None or result["score"] > best["score"]):
            best = result
    return best


async def run_vcp_scan() -> None:
    status = PipelineStatus("run_vcp_scan")
    try:
    
        today = today_ist()
        log.info(f"━━━ VCP Scan  {today} ━━━")
        async with httpx.AsyncClient() as client:
            global ISIN_MAP, BSE_ISIN_MAP, BSE_META
            ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)
            all_data = await download_all_chunks(client)
            log.info(f"Loaded {len(all_data)} stocks")
            signals = []
            for sym, s in all_data.items():
                if not _check_liquidity(s["v"], s["c"], len(s["c"])):
                    continue
                r = _detect_vcp(s)
                if r:
                    signals.append({"symbol": sym, **r})
            signals.sort(key=lambda x: x["score"], reverse=True)
            log.info(f"VCP signals: {len(signals)}")
            await upload_str_with_manifest(client, r2_upload, "vcp_signals.json", json.dumps({
                "updated": today,
                "count": len(signals),
                "signals": signals,
            }), schema_v=1, extra_meta={"count": len(signals)})
        status.success()
        log.info("━━━ VCP Scan complete ━━━")
    except Exception as e:
        status.failure(e)



# ══════════════════════════════════════════════════════════════
# CPR (Central Pivot Range) — Daily / Weekly / Monthly
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
# CSV reader — TradingView export format
# ══════════════════════════════════════════════════════════════

def load_csv(path, end_date=None):
    """
    Reads a TradingView-exported daily CSV (columns: time,open,high,low,
    close,...,Volume,...) into pipeline.py's candle-dict format.
    end_date (YYYY-MM-DD): if given, truncates history to end on/before
    that date — lets you replay "what would this have looked like on
    that day", e.g. right before a historical breakout.
    """
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
        depth = None
        print(f"    {c['h_date']}  ₹{c['h_price']}  ->  {c['l_date']}  ₹{c['l_price']}")
    print(f"  Depths: {result['depths_pct']}")
    print(f"  Base: {result['base_start_date']} -> {result['base_end_date']}  "
          f"({result['base_len']} days)")
    print(f"  Prior move: {result['prior_move_pct']}%  |  Vol dry-up: {result['vol_dryup']}")


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def _parse_params(s):
    """--params zigzag_pct=0.05,max_final_depth=0.15 -> dict with floats where possible."""
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
