    n = len(highs)
    if n < period + 1: return 0.0
    trs = []
    for i in range(1, n):
        h, l, c_prev = highs[i], lows[i], closes[i - 1]
        if h is None or l is None or c_prev is None: continue
        trs.append(max(h - l, abs(h - c_prev), abs(l - c_prev)))
    if len(trs) < period: return 0.0
    return sum(trs[-period:]) / period


def _vcp_sma(arr, period, end=None):
    end = len(arr) if end is None else end
    if end < period: return None
    seg = arr[end - period:end]
    if not seg or any(v is None for v in seg): return None
    return sum(seg) / period


def _vcp_zigzag_abs(highs, lows, atr_threshold):
    """
    ATR-based ZigZag — a pivot (H or L) is only confirmed once price has
    reversed by at least atr_threshold (an absolute price amount, derived
    from ATR so it auto-scales to each stock's own volatility) from the
    running extreme. Replaces the older fixed-percentage ZigZag, which used
    the same % threshold for a ₹50 penny stock and a ₹5,000 large-cap.
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
            if ext_high is not None and l <= ext_high - atr_threshold:
                piv.append((ext_high_idx, ext_high, "H", i))
                direction = "down"; ext_low, ext_low_idx = l, i
            elif ext_low is not None and h >= ext_low + atr_threshold:
                piv.append((ext_low_idx, ext_low, "L", i))
                direction = "up"; ext_high, ext_high_idx = h, i
        elif direction == "up":
            if l <= ext_high - atr_threshold:
                piv.append((ext_high_idx, ext_high, "H", i))
                direction = "down"; ext_low, ext_low_idx = l, i
        else:
            if h >= ext_low + atr_threshold:
                piv.append((ext_low_idx, ext_low, "L", i))
                direction = "up"; ext_high, ext_high_idx = h, i
    return piv


def _vcp_zigzag_close_atr(highs, lows, closes, atr_threshold):
    """
    Same idea as _vcp_zigzag_abs, but uses CLOSING prices to decide WHEN a
    reversal is confirmed (noise-resistant against intraday wick spikes),
    then looks back to find the TRUE high/low within that confirmed span.
    """
    n = len(closes)
    if n < 2: return []
    close_piv = _vcp_zigzag_abs(closes, closes, atr_threshold)
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


def _detect_vcp(hist, lookback=150, atr_multiplier=1.5, atr_period=14,
                min_contractions=3, max_contractions=6,
                max_base_depth=0.45, max_final_depth=0.12, tighten_tol=0.03,
                max_ceiling_jump=0.025, max_dist_from_pivot=0.08, min_prior_move=0.20,
                max_52wh_dist=0.20, max_post_breakout_run=0.03,
                live_min_bars=5, live_min_depth=0.02, min_first_leg_bars=15,
                ceiling_band_tol=0.04, min_leg_span_bars=5, max_depth_ratio=0.75,
                min_pattern_days=15, max_pattern_days=325, debug=False):
    """
    VCP (Volatility Contraction Pattern) detector — resistance/pivot-chain
    based, ATR-scaled ZigZag on closes (auto-adjusts to each stock's own
    volatility, unlike a fixed-percentage threshold), with nested-swing
    filtering and an asymmetric ceiling check for flat/descending
    resistance bases.

    Chains consecutive swing-high -> swing-low legs into the longest run
    of progressively tightening contractions ending at the most recent
    leg, and scores each candidate base via TWO independent methods,
    keeping whichever is valid (Method B preferred when both are):

      METHOD A (zigzag_chain) — walks the raw pivot chain backward,
      stopping the run the moment a leg's depth stops tightening OR its
      high jumps up more than max_ceiling_jump versus its own immediate
      neighbor. Handles descending-resistance (converging/symmetrical-
      triangle) bases where each successive high is itself lower than the
      one before.

      METHOD B (ceiling_cluster) — only the highs that actually touch the
      base's own ceiling (within ceiling_band_tol) count as contraction
      boundaries; smaller internal highs that never approach the ceiling
      are noise inside the base, not separate contractions. Same
      immediate-neighbor jump rule as Method A applies to ceiling-touching
      nodes. Handles flat-top, multi-touch cup-with-handle shapes.

    Both methods require the FIRST contraction to be meaningfully deeper
    than the base leg (max_depth_ratio), and every later leg to be no
    deeper than the one before it by more than tighten_tol (additive).
    A live/still-forming final leg (not yet confirmed by a ZigZag reversal)
    is included if it already meets live_min_bars/live_min_depth, so a
    base can be caught mid-formation, not just after it closes.
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

    sma50 = _vcp_sma(closes, 50)
    sma150 = _vcp_sma(closes, 150) if n >= 150 else _vcp_sma(closes, min(n, 100))
    if sma50 is None or sma150 is None: return None
    if not (last_close > sma50 > sma150): return None

    lb = min(lookback, n)
    start = n - lb
    h_w = highs[start:]; l_w = lows[start:]; c_w = closes[start:]

    atr_val = _vcp_atr(h_w, l_w, c_w, atr_period)
    if atr_val <= 0: return None
    atr_threshold = atr_val * atr_multiplier

    piv = _vcp_zigzag_close_atr(h_w, l_w, c_w, atr_threshold)
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
