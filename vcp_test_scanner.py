#!/usr/bin/env python3
"""
Final combined patch over pipeline.py's _detect_vcp():

CHANGE 1 (bug fix, low risk) — Step 5 ceiling-jump check now compares each
new leg's high against the RUNNING MAX of all highs already included in the
tentative chain (i.e. the base's own established ceiling), instead of only
its immediate previous neighbor. This correctly allows:
  - descending-resistance VCPs (each high legitimately lower — always passes)
  - flat-resistance VCPs where an interior leg dips deep then price
    RE-TESTS the base's own original ceiling (previously misread as a
    fresh breakout into an unrelated new base)
It still correctly BLOCKS a leg whose high exceeds everything seen so far
in the chain by more than max_ceiling_jump — a genuine breakout.

CHANGE 2 (tolerance widen, real behavior change) — tighten_tol raised from
2% -> 3%, and applied CONSISTENTLY in both the run-boundary walk (Step 5)
and the final strict-decreasing depth check (Step 6), which previously used
two different (inconsistent) strictness levels for the same "should depths
keep shrinking" idea.
"""
import csv, json

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
                max_ceiling_jump=0.05, max_dist_from_pivot=0.08, min_prior_move=0.20,
                max_52wh_dist=0.20, max_post_breakout_run=0.03,
                live_min_bars=5, live_min_depth=0.02, min_first_leg_bars=15, debug=False):
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
                if hp > 0 and n_bars >= 2:
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

        # ---- CHANGE 1: running-ceiling chain walk (replaces pairwise) ----
        depths = [c[4] for c in contractions]
        run_end = len(depths) - 1
        j = run_end - 1
        while j >= 0:
            if depths[j] < depths[j+1] - tighten_tol:
                break
            hi_this = contractions[j+1][1]
            earlier_ceiling = max(c[1] for c in contractions[:j+1])
            if hi_this > earlier_ceiling * (1 + max_ceiling_jump):
                break
            j -= 1
        run = contractions[j+1:]
        run_depths = [c[4] for c in run]
        if not (min_contractions <= len(run) <= max_contractions): return None

        if len(run) >= 2 and (run[1][0] - run[0][0]) < min_first_leg_bars: return None

        for k in range(len(run)):
            low_k = run[k][3]
            check_from = run[k][2]
            for idx in range(check_from + 1, n):
                if lows[idx] is not None and lows[idx] < low_k:
                    return None

        # ---- CHANGE 2: same tighten_tol used here as in Step 5 walk ----
        for k in range(1, len(run_depths)):
            if run_depths[k] >= run_depths[k-1] + tighten_tol:
                return None

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
            "dist_from_pivot_pct": round(dist * 100, 2),
            "vol_dryup": vol_dryup,
            "prior_move_pct": round(prior_move * 100, 1),
            "base_len": base_len,
            "score": score,
        }

    best = None
    for base_high in sorted(h_pivots, key=lambda x: -x[0]):
        result = _try_base(base_high, dbg=debug)
        if result and (best is None or result["score"] > best["score"]):
            best = result
    return best


def load_csv(path, end_date=None):
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    vol_col = next((c for c in rows[0].keys() if c.strip().lower() == "volume"), None)
    dates, opens, highs, lows, closes, vols = [], [], [], [], [], []
    for r in rows:
        d = r.get("time", "").strip()
        if not d: continue
        if end_date and d > end_date: break
        try:
            dates.append(d)
            opens.append(float(r["open"]))
            highs.append(float(r["high"]))
            lows.append(float(r["low"]))
            closes.append(float(r["close"]))
            vols.append(float(r[vol_col]) if vol_col and r.get(vol_col) else None)
        except (KeyError, ValueError):
            continue
    return {"d": dates, "o": opens, "h": highs, "l": lows, "c": closes, "v": vols}
