"""
VCP (Volatility Contraction Pattern) detector.

Input `hist` matches the pipeline candle format:
    {"d": [...], "o": [...], "h": [...], "l": [...], "c": [...], "v": [...]}
All lists are chronological (oldest -> newest).

Returns None if no valid VCP, else a dict with pattern details + a 0-100 score.
NOTE: VCP is inherently heuristic. Tune params on your own universe.
"""
from statistics import mean


def _sma(arr, period, end=None):
    end = len(arr) if end is None else end
    if end < period:
        return None
    seg = arr[end - period:end]
    if not seg or any(v is None for v in seg):
        return None
    return sum(seg) / period


def _find_pivots(highs, lows, w):
    """Local swing highs/lows over a +/- w window. Returns [(idx, price, 'H'/'L')]."""
    piv = []
    n = len(highs)
    for i in range(w, n - w):
        seg_h = highs[i - w:i + w + 1]
        seg_l = lows[i - w:i + w + 1]
        if highs[i] is None or lows[i] is None:
            continue
        if highs[i] == max(seg_h):
            piv.append((i, highs[i], "H"))
        elif lows[i] == min(seg_l):
            piv.append((i, lows[i], "L"))
    return piv


def _zigzag(piv):
    """Collapse pivots into a strictly alternating H/L sequence."""
    if not piv:
        return []
    z = [piv[0]]
    for p in piv[1:]:
        if p[2] == z[-1][2]:
            # same kind in a row -> keep the more extreme one
            if (p[2] == "H" and p[1] >= z[-1][1]) or (p[2] == "L" and p[1] <= z[-1][1]):
                z[-1] = p
        else:
            z.append(p)
    return z


def _detect_vcp(
    hist,
    lookback=90,
    swing_window=3,
    min_contractions=2,
    max_contractions=6,
    max_base_depth=0.40,
    max_final_depth=0.12,
    tighten_tol=0.01,
    max_dist_from_pivot=0.10,
    require_uptrend=True,
):
    highs = hist.get("h") or []
    lows = hist.get("l") or []
    closes = hist.get("c") or []
    vols = hist.get("v") or []
    n = len(closes)

    if n < max(lookback, 50):
        return None
    if any(x is None for x in (closes[-1], highs[-1], lows[-1])):
        return None

    last_close = closes[-1]

    # ---- 1. Trend filter (Stage 2 uptrend) ----
    if require_uptrend:
        sma50 = _sma(closes, 50)
        sma150 = _sma(closes, 150) if n >= 150 else _sma(closes, min(n, 100))
        if sma50 is None or sma150 is None:
            return None
        if not (last_close > sma50 > sma150):
            return None

    # ---- 2. Pivots within lookback ----
    start = n - lookback
    h_w = highs[start:]
    l_w = lows[start:]
    piv = _zigzag(_find_pivots(h_w, l_w, swing_window))
    piv = [(i + start, p, k) for (i, p, k) in piv]  # re-index to absolute
    if len(piv) < 3:
        return None

    # Base starts at the highest H in the window
    h_pivots = [p for p in piv if p[2] == "H"]
    if not h_pivots:
        return None
    base_high = max(h_pivots, key=lambda x: x[1])
    seq = [p for p in piv if p[0] >= base_high[0]]
    if seq[0][2] != "H":
        return None

    # ---- 3. Build contractions (H -> next L) ----
    contractions = []  # (high_idx, high, low_idx, low, depth)
    i = 0
    while i < len(seq) - 1:
        if seq[i][2] == "H" and seq[i + 1][2] == "L":
            hi, hp = seq[i][0], seq[i][1]
            li, lp = seq[i + 1][0], seq[i + 1][1]
            if hp > 0:
                contractions.append((hi, hp, li, lp, (hp - lp) / hp))
            i += 2
        else:
            i += 1

    if len(contractions) < min_contractions:
        return None

    depths = [c[4] for c in contractions]

    # ---- 4. Longest tightening run ending at the most recent contraction ----
    run_end = len(depths) - 1
    j = run_end - 1
    while j >= 0 and depths[j] >= depths[j + 1] - tighten_tol:
        j -= 1
    run = contractions[j + 1:]  # tightening sequence
    run_depths = [c[4] for c in run]

    if not (min_contractions <= len(run) <= max_contractions):
        return None

    base_depth = max(run_depths)
    final_depth = run_depths[-1]
    if base_depth > max_base_depth or final_depth > max_final_depth:
        return None

    # ---- 5. Pivot (buy point) = high of the last contraction, distance check ----
    pivot_price = run[-1][1]
    if pivot_price <= 0:
        return None
    dist = (pivot_price - last_close) / pivot_price
    # must be just under (or slightly above) the pivot, not far below / over-extended
    if dist > max_dist_from_pivot or dist < -0.02:
        return None

    # ---- 6. Volume dry-up: last contraction vs first contraction of the run ----
    def _leg_vol(c):
        a, b = c[0], c[2]
        seg = [v for v in vols[a:b + 1] if v]
        return mean(seg) if seg else 0

    first_vol = _leg_vol(run[0])
    last_vol = _leg_vol(run[-1])
    vol_dryup = last_vol < first_vol if first_vol else False

    # ---- 7. Score (0-100) ----
    score = 0
    score += min(len(run), 4) * 10                       # more footprints (cap 4) -> +40
    score += max(0, (max_final_depth - final_depth) / max_final_depth) * 25  # tighter final
    score += 20 if vol_dryup else 0
    score += max(0, (max_dist_from_pivot - abs(dist)) / max_dist_from_pivot) * 15
    score = round(min(score, 100), 1)

    return {
        "is_vcp": True,
        "contractions": len(run),                 # the "T" footprint, e.g. 3T
        "depths_pct": [round(d * 100, 1) for d in run_depths],
        "base_depth_pct": round(base_depth * 100, 1),
        "final_depth_pct": round(final_depth * 100, 1),
        "pivot": round(pivot_price, 2),
        "dist_from_pivot_pct": round(dist * 100, 2),
        "vol_dryup": vol_dryup,
        "score": score,
    }
