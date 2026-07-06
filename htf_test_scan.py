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
from datetime import date, datetime, timezone

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


# ── same _detect_htf logic as the pipeline.py patch ──────────────────────

def _htf_swing_low_idx(lows, end_idx, lookback, floor=0):
    start = max(0, end_idx - lookback, floor)
    seg = [(i, lows[i]) for i in range(start, end_idx + 1) if lows[i] is not None]
    if not seg:
        return None
    return min(seg, key=lambda x: x[1])[0]


def _swing_low_candidates(lows, end_idx, lookback, floor=0):
    """Returns ALL candidate base indices within [floor, end_idx], sorted so
    lower lows (bigger potential gain%) are tried first. Trying every point
    (not just the single absolute minimum) avoids settling for a higher,
    non-optimal base when a genuinely lower valid one exists in the same
    window — same fix already applied in weekly_pullback_signal.py."""
    start = max(0, end_idx - lookback, floor)
    seg_idx = [i for i in range(start, end_idx + 1) if lows[i] is not None]
    if not seg_idx:
        return []
    return sorted(seg_idx, key=lambda i: lows[i])


def _build_weekly_map(dates, highs, lows, closes):
    """Groups daily bars into ISO weeks. Returns weekly OHLC arrays plus,
    for each week, the DAILY index where that week's high/low actually
    occurred and the daily index of the week's last trading day — so pole
    candidates found on smoothed weekly bars can be mapped straight back to
    exact daily dates/values for the flag+breakout+post-BO tracking."""
    weeks = {}
    for i, d in enumerate(dates):
        h, l, c = highs[i], lows[i], closes[i]
        if h is None or l is None or c is None:
            continue
        k = date.fromisoformat(d).isocalendar()[:2]
        weeks.setdefault(k, []).append((i, h, l, c))

    keys = sorted(weeks.keys())
    wd, wh, wl, wc, wh_day, wl_day, week_end_day = [], [], [], [], [], [], []
    for k in keys:
        entries = weeks[k]
        h_idx, _ = max(((i, h) for i, h, l, c in entries), key=lambda x: x[1])
        l_idx, _ = min(((i, l) for i, h, l, c in entries), key=lambda x: x[1])
        y, w = k
        wd.append(date.fromisocalendar(y, w, 1).isoformat())
        wh.append(highs[h_idx]); wl.append(lows[l_idx]); wc.append(entries[-1][3])
        wh_day.append(h_idx); wl_day.append(l_idx); week_end_day.append(entries[-1][0])

    # per-daily-index -> week-index lookup, for mapping a daily flag-low back
    # to "which week does this belong to" (needed for chaining)
    day_to_week = [0] * len(dates)
    wk = 0
    for i in range(len(dates)):
        while wk < len(week_end_day) - 1 and i > week_end_day[wk]:
            wk += 1
        day_to_week[i] = wk

    return wd, wh, wl, wc, wh_day, wl_day, week_end_day, day_to_week


def _validate_weekly_pole(lo_w, hi_w, wh, wl, wc, n_weekly,
                           pole_min_weeks, pole_max_weeks, min_gain_pct, max_gain_pct):
    """Pole validation (gain%, duration, wick-safe peak-check) on WEEKLY
    bars — smooths out the daily noise that let clearly choppy, zig-zag
    stretches qualify as a "pole" just because their net gain cleared the
    threshold. Returns the gain_pct if valid, else None."""
    if lo_w is None or lo_w >= hi_w:
        return None
    pole_high = wh[hi_w]
    pole_low = wl[lo_w]
    weeks = hi_w - lo_w
    if weeks < pole_min_weeks or weeks > pole_max_weeks:
        return None
    if not pole_low or pole_low <= 0:
        return None
    gain_pct = (pole_high - pole_low) / pole_low * 100.0
    if gain_pct < min_gain_pct:
        return None
    if max_gain_pct is not None and gain_pct >= max_gain_pct:
        return None
    if any(wh[k] is not None and wh[k] > pole_high for k in range(lo_w, hi_w)):
        return None
    peak_check_end = min(n_weekly, hi_w + 2)
    if any(wc[k] is not None and wc[k] > pole_high * 1.03 for k in range(hi_w + 1, peak_check_end)):
        return None
    return gain_pct


def _build_flag_and_signal(lo, hi, pole_low, pole_high, gain_pct, pole_days,
                            dates, highs, lows, closes, n,
                            max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple,
                            ema21=None):
    """Everything AFTER the pole itself is validated: the daily flag walk,
    breakout/failure resolution, and post-breakout success tracking. Shared
    by both the daily-only path (Mini HTF) and the weekly-pole path (HTF) —
    pole_low/pole_high/gain_pct/pole_days are passed in already-validated,
    lo/hi are the DAILY indices of the pole's low/high (for HTF these come
    from mapping the weekly pole back to its exact daily day).

    ema21 (optional): if provided, a flag that's still "forming" (never
    resolved via breakout/failure) but has closed below the 21 EMA on more
    than 30% of its days so far is downgraded to "failed" — a flag that
    keeps closing below its short-term average isn't a tight, orderly
    consolidation anymore, it's early breakdown/distribution, and shouldn't
    be reported as a live "still forming, watch for breakout" setup."""
    # A small pole shouldn't get the same flat pullback allowance as a huge
    # one — a 21%-gain pole giving back 15% (71% of its own gain) isn't a
    # "flag", it's most of the move undone. Cap the effective pullback at
    # whichever is smaller: the preset's own max_pullback_pct, or half of
    # this pole's own gain.
    effective_max_pullback_pct = min(max_pullback_pct, gain_pct * 0.5)

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
    flag_days_seen = 0
    flag_days_below_ema = 0

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

        if ema21 is not None and c is not None and ema21[j] is not None:
            flag_days_seen += 1
            if c < ema21[j]:
                flag_days_below_ema += 1

        pullback_pct = (pole_high - cum_low) / pole_high * 100.0
        if pullback_pct > effective_max_pullback_pct:
            status = "failed"
            resolved_idx = j
            break

    if (status == "forming" and ema21 is not None
            and flag_days_seen >= 5 and flag_days_below_ema / flag_days_seen > 0.30):
        status = "failed"
        resolved_idx = None

    fe = resolved_idx if resolved_idx is not None else flag_end_max
    pullback_pct = (pole_high - cum_low) / pole_high * 100.0
    flag_days = fe - hi

    # If breakout fired before the minimum flag duration, this candidate's
    # consolidation was too short to qualify as this preset's pattern
    # (e.g. HTF needs a 3-5 week flag) — drop it entirely.
    if status == "breakout" and flag_days < flag_min_days:
        return None

    # If the flag ran all the way to flag_max_days without EVER resolving
    # (no breakout, no failure), this setup is stale/expired — not "still
    # forming". Reporting it as forming forever (even a year later, once
    # price has moved on for entirely unrelated reasons) is misleading.
    # Drop it; a later, genuinely fresh pole picks up from here instead.
    if status == "forming" and flag_days >= flag_max_days:
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


def _try_build_htf_match(lo, hi, dates, highs, lows, closes, n,
                          pole_min_days, pole_max_days, min_gain_pct, max_gain_pct,
                          max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple,
                          ema21=None):
    """Daily-only path (used by Mini HTF): validates the pole itself using
    daily bars, then hands off to _build_flag_and_signal for the rest.
    Returns None if this (lo, hi) pair doesn't qualify."""
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

    # Pole-tightness (Kaufman's Efficiency Ratio): gain% and duration alone
    # let a long, gradual, staircase-style uptrend qualify as a "pole" just
    # because it eventually cleared the gain threshold — even a 20-30 day
    # slow grind with real interim pullbacks along the way. A genuine HIGH
    # TIGHT flag pole should be a compressed, mostly-uninterrupted move: ER
    # = |net move| / (sum of all day-to-day absolute moves) is close to 1
    # for a straight, efficient rally and drops toward 0 for a choppy,
    # back-and-forth grind. Reject anything below 0.5 as not tight enough
    # to call a pole, regardless of how the price action resolved later.
    pole_closes = [closes[k] for k in range(lo, hi + 1) if closes[k] is not None]
    if len(pole_closes) >= 2:
        net_move = abs(pole_closes[-1] - pole_closes[0])
        total_move = sum(abs(pole_closes[k] - pole_closes[k - 1]) for k in range(1, len(pole_closes)))
        efficiency_ratio = (net_move / total_move) if total_move > 0 else 0
        if efficiency_ratio < 0.5:
            return None

    # Pole-cleanliness: a genuine swing low is often the exact day price is
    # still catching up to its 21 EMA (that's normal at a bottom) — requiring
    # EVERY close to be above the EMA throws out the real low and quietly
    # shifts the pole's start to a later, shallower point instead. Allow up
    # to 20% of the pole's days to close below EMA21 before rejecting it as
    # genuinely un-clean/choppy.
    if ema21 is not None:
        total = 0
        below = 0
        for k in range(lo, hi + 1):
            e = ema21[k]
            if e is not None and closes[k] is not None:
                total += 1
                if closes[k] < e:
                    below += 1
        if total > 0 and below / total > 0.20:
            return None

    return _build_flag_and_signal(lo, hi, pole_low, pole_high, gain_pct, pole_days,
                                   dates, highs, lows, closes, n,
                                   max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple,
                                   ema21=ema21)



def _detect_htf_weekly_pole(s, min_gain_pct, max_gain_pct, pole_min_weeks, pole_max_weeks,
                             max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple,
                             lookback_days):
    """HTF path: pole is found on WEEKLY bars (smooths out daily noise that
    let clearly choppy zig-zag stretches qualify just because their net
    gain cleared the threshold), then mapped to exact daily dates — the
    flag, breakout, and post-breakout tracking all stay on DAILY bars via
    the same _build_flag_and_signal used by the daily-only (Mini HTF) path."""
    dates, highs, lows, closes = s["d"], s["h"], s["l"], s["c"]
    n = len(dates)
    if n < 60:
        return []

    wd, wh, wl, wc, wh_day, wl_day, week_end_day, day_to_week = _build_weekly_map(dates, highs, lows, closes)
    n_weekly = len(wd)
    if n_weekly < pole_min_weeks + 2:
        return []

    matches = []
    scan_start_w = max(0, n_weekly - (lookback_days // 5 + 10))
    chain_lo_w = None
    fallback_floor_w = -1

    hi_w = scan_start_w + pole_min_weeks
    while hi_w < n_weekly:
        if wh[hi_w] is None:
            hi_w += 1
            continue

        candidates_w = []
        if chain_lo_w is not None and hi_w - chain_lo_w <= pole_max_weeks:
            candidates_w.append(chain_lo_w)
        for search_lo_w in _swing_low_candidates(wl, hi_w, pole_max_weeks, floor=fallback_floor_w):
            if search_lo_w not in candidates_w:
                candidates_w.append(search_lo_w)

        best = None  # (lo_w, gain_pct)
        for lo_w in candidates_w:
            gp = _validate_weekly_pole(lo_w, hi_w, wh, wl, wc, n_weekly,
                                        pole_min_weeks, pole_max_weeks, min_gain_pct, max_gain_pct)
            if gp is not None and (best is None or wl[lo_w] < wl[best[0]]):
                best = (lo_w, gp)

        if best is None:
            hi_w += 1
            continue
        lo_w, gain_pct = best

        # Prefer the highest valid weekly peak reachable from this lo_w —
        # same "don't settle for the first candidate" fix as the daily path.
        best_hi_w, best_gain = hi_w, gain_pct
        search_end_w = min(n_weekly - 1, lo_w + pole_max_weeks)
        for hi2_w in range(hi_w + 1, search_end_w + 1):
            if wh[hi2_w] is None or wh[hi2_w] <= wh[best_hi_w]:
                continue
            gp2 = _validate_weekly_pole(lo_w, hi2_w, wh, wl, wc, n_weekly,
                                         pole_min_weeks, pole_max_weeks, min_gain_pct, max_gain_pct)
            if gp2 is not None:
                best_hi_w, best_gain = hi2_w, gp2

        hi_w_final, gain_pct = best_hi_w, best_gain

        # Map the validated weekly pole back to its exact daily low/high day.
        lo_day = wl_day[lo_w]
        hi_day = wh_day[hi_w_final]
        pole_low = lows[lo_day]
        pole_high = highs[hi_day]
        pole_days = hi_day - lo_day

        match = _build_flag_and_signal(lo_day, hi_day, pole_low, pole_high, gain_pct, pole_days,
                                        dates, highs, lows, closes, n,
                                        max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple)
        if match is None:
            hi_w = hi_w_final + 1
            continue

        cum_low_idx = match.pop("_cum_low_idx")
        fe = match.pop("_fe")
        matches.append(match)

        chain_lo_w = day_to_week[cum_low_idx] if cum_low_idx > hi_day else None
        fallback_floor_w = day_to_week[min(fe + 2, n - 1)]
        hi_w = hi_w_final + 1

    matches.sort(key=lambda m: m["pole_high_date"], reverse=True)
    deduped = []
    for m in matches:
        if not any(abs((date.fromisoformat(m["pole_high_date"]) - date.fromisoformat(k["pole_high_date"])).days) <= 10
                   for k in deduped):
            deduped.append(m)
    return deduped


def detect_htf(s, min_gain_pct=90.0, max_gain_pct=None, pole_min_days=10, pole_max_days=40,
               max_pullback_pct=25.0, flag_min_days=10, flag_max_days=40,
               lookback_days=260, success_rr_multiple=2.0,
               use_weekly_pole=False, pole_min_weeks=3, pole_max_weeks=8):
    if use_weekly_pole:
        return _detect_htf_weekly_pole(s, min_gain_pct, max_gain_pct, pole_min_weeks, pole_max_weeks,
                                        max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple,
                                        lookback_days)

    dates, highs, lows, closes = s["d"], s["h"], s["l"], s["c"]
    n = len(dates)
    if n < pole_min_days + flag_min_days:
        return []

    ema21 = _calc_ema(closes, 21)

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

    hi = scan_start + pole_min_days
    while hi < n:
        if highs[hi] is None:
            hi += 1
            continue

        candidates = []
        if chain_lo is not None and hi - chain_lo <= pole_max_days:
            candidates.append(chain_lo)
        for search_lo in _swing_low_candidates(lows, hi, pole_max_days, floor=fallback_floor):
            if search_lo not in candidates:
                candidates.append(search_lo)

        # Try every candidate base, not just the first that works — a lower
        # low elsewhere in the same valid window measures the rally more
        # completely (bigger, more accurate gain%). chain_lo still gets a
        # chance first (preserves intentional staircase continuation), but
        # if a genuinely lower valid base also exists, prefer it.
        match = None
        used_lo = None
        for lo in candidates:
            m2 = _try_build_htf_match(lo, hi, dates, highs, lows, closes, n,
                                       pole_min_days, pole_max_days, min_gain_pct, max_gain_pct,
                                       max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple,
                                       ema21)
            if m2 is not None and (match is None or lows[lo] < lows[used_lo]):
                match, used_lo = m2, lo

        if match is None:
            hi += 1
            continue

        # Prefer the highest valid peak reachable from this same lo — but
        # ONLY while the current best match is still "forming"/
        # "pole_just_formed" (genuinely unresolved, so a later higher high
        # might really be part of the same unfinished rally). Once a match
        # has already resolved (success, failed, breakout, breakout_failed),
        # the flag's outcome is locked in as history; overwriting it with a
        # "bigger" pole from a later peak would silently discard a real,
        # already-completed pattern in favor of a different one.
        best_hi, best_match = hi, match
        if best_match["status"] in ("forming", "pole_just_formed"):
            search_end = min(n - 1, used_lo + pole_max_days)
            for hi2 in range(hi + 1, search_end + 1):
                if highs[hi2] is None or highs[hi2] <= highs[best_hi]:
                    continue
                match2 = _try_build_htf_match(used_lo, hi2, dates, highs, lows, closes, n,
                                               pole_min_days, pole_max_days, min_gain_pct, max_gain_pct,
                                               max_pullback_pct, flag_min_days, flag_max_days, success_rr_multiple,
                                               ema21)
                if match2 is not None:
                    best_hi, best_match = hi2, match2
                    if best_match["status"] not in ("forming", "pole_just_formed"):
                        break

        match = best_match
        cum_low_idx = match.pop("_cum_low_idx")
        fe = match.pop("_fe")
        matches.append(match)
        chain_lo = cum_low_idx if cum_low_idx > best_hi else None
        fallback_floor = fe + 2
        hi = best_hi + 1

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
    "HTF": dict(min_gain_pct=90.0, max_pullback_pct=25.0,
                flag_min_days=15, flag_max_days=25,
                success_rr_multiple=2.0,
                use_weekly_pole=True, pole_min_weeks=3, pole_max_weeks=15),
    "Mini HTF": dict(min_gain_pct=20.0, max_gain_pct=90.0, max_pullback_pct=15.0, pole_min_days=5,
                     pole_max_days=40, flag_min_days=5, flag_max_days=21,
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

        # Flat list, matching your other scan files (hlr_signals.json,
        # ep_signals.json, pattern_signals.json): {"updated","count","signals"}.
        all_results[label] = {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "count": len(signals),
            "signals": signals,
        }

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
