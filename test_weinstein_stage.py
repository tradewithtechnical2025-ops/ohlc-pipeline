#!/usr/bin/env python3
"""
Standalone offline test for _detect_weinstein_stages() — the Weinstein 4-Stage
Analysis detector from pipeline.py. No network calls, no env vars (UPSTOX_TOKEN
etc), no R2/holiday-file dependency — pure logic test on synthetic price data.

This file duplicates the small set of pure functions the detector needs
(_build_tf_series simplified to skip NSE-holiday awareness, _calc_sma,
_check_liquidity, _isoweek_to_date) plus the detector itself, copied verbatim
from pipeline.py's _detect_weinstein_stages(). If you change the real
_detect_weinstein_stages() in pipeline.py, copy the updated version in here
too — this is a separate file on purpose, so it never needs Upstox/R2 access.

Usage:
  python test_weinstein_stage.py
"""

import re
from datetime import date, timedelta

# ══════════════════════════════════════════════════════════════
# Minimal copies of pipeline.py helpers (no holiday-calendar dependency —
# synthetic data below only uses Mon-Fri, so plain weekday logic is enough)
# ══════════════════════════════════════════════════════════════

def _is_week_complete_simple(today_d: str) -> bool:
    dt_today = date.fromisoformat(today_d)
    return dt_today.isocalendar()[2] == 5  # Friday = last bar of a complete week


def _build_tf_series(dates, highs, lows, closes, volumes, tf):
    """Same logic as pipeline.py's _build_tf_series, holiday-check swapped
    for a plain Friday-close rule (fine for Mon-Fri synthetic test data)."""
    agg = {}
    key_fn = (lambda d: date.fromisoformat(d).isocalendar()[:2]) if tf == "W" else (lambda d: d[:7])
    for d, h, l, c, v in zip(dates, highs, lows, closes, volumes):
        if h is None or l is None or c is None: continue
        k = key_fn(d)
        if k not in agg: agg[k] = {"h": h, "l": l, "c": c, "v": v or 0}
        else:
            agg[k]["h"] = max(agg[k]["h"], h); agg[k]["l"] = min(agg[k]["l"], l)
            agg[k]["c"] = c; agg[k]["v"] += v or 0
    if not agg or not dates: return [], [], [], [], []
    today_d = dates[-1]
    if tf == "W":
        current_key = date.fromisoformat(today_d).isocalendar()[:2]
        complete = _is_week_complete_simple(today_d)
    else:
        current_key = today_d[:7]
        complete = True
    keys = sorted(k for k in agg if (k <= current_key if complete else k < current_key))
    return ([str(k) for k in keys],
            [agg[k]["h"] for k in keys], [agg[k]["l"] for k in keys],
            [agg[k]["c"] for k in keys], [agg[k]["v"] for k in keys])


def _calc_sma(closes, period):
    n = len(closes)
    sma = [None] * n
    for i in range(period - 1, n):
        window = closes[i - period + 1:i + 1]
        if any(v is None for v in window):
            continue
        sma[i] = sum(window) / period
    return sma


def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    lookback = min(50, n)
    if lookback < 20: return True
    vols = [v for v in volumes[-lookback:] if v is not None]
    prices = [c for c in closes[-lookback:] if c is not None and c > 0]
    if len(vols) < 20 or len(prices) < 20: return False
    return (sum(vols) / len(vols) * sum(prices) / len(prices)) >= min_turnover


def _isoweek_to_date(week_str):
    m = re.match(r"\((\d+),\s*(\d+)\)", str(week_str))
    if not m: return week_str
    year, week = int(m.group(1)), int(m.group(2))
    return date.fromisocalendar(year, week, 1).isoformat()


STAGE_NAMES = {1: "Basing", 2: "Advancing", 3: "Topping", 4: "Declining"}


# ══════════════════════════════════════════════════════════════
# _detect_weinstein_stages — EXACT COPY from pipeline.py
# ══════════════════════════════════════════════════════════════

def _detect_weinstein_stages(all_data, sma_period=30, slope_lookback=4,
                              flat_threshold_pct=1.0, min_weeks=40):
    current_signals = []
    breadth = {}

    for sym, s in all_data.items():
        dates, highs, lows, closes, volumes = s["d"], s["h"], s["l"], s["c"], s["v"]
        n_daily = len(dates)
        if n_daily < 200 or not _check_liquidity(volumes, closes, n_daily):
            continue

        w_labels, wh, wl, wc, wv = _build_tf_series(dates, highs, lows, closes, volumes, "W")
        n = len(wc)
        if n < sma_period + slope_lookback + min_weeks:
            continue

        sma = _calc_sma(wc, sma_period)
        start = sma_period + slope_lookback

        last_trend = None
        stage_seq = [None] * start

        for i in range(start, n):
            price, s30, s30_prev = wc[i], sma[i], sma[i - slope_lookback]
            if price is None or s30 is None or s30_prev is None or s30_prev == 0:
                stage_seq.append(None)
                continue
            slope_pct = (s30 - s30_prev) / s30_prev * 100

            if price > s30 and slope_pct > flat_threshold_pct:
                stage = 2
            elif price < s30 and slope_pct < -flat_threshold_pct:
                stage = 4
            else:
                stage = 3 if last_trend == 2 else 1

            if stage in (2, 4):
                last_trend = stage
            stage_seq.append(stage)

            breadth.setdefault(w_labels[i], {1: 0, 2: 0, 3: 0, 4: 0})
            breadth[w_labels[i]][stage] += 1

        if stage_seq and stage_seq[-1] is not None:
            cur_stage = stage_seq[-1]
            prev_stage = next((v for v in reversed(stage_seq[:-1]) if v is not None), None)
            weeks_in_stage = 0
            for v in reversed(stage_seq):
                if v == cur_stage: weeks_in_stage += 1
                else: break
            i_last = n - 1
            current_signals.append({
                "symbol": sym, "week": w_labels[i_last], "stage": cur_stage,
                "prev_stage": prev_stage,
                "stage_change": bool(prev_stage is not None and prev_stage != cur_stage),
                "weeks_in_stage": weeks_in_stage,
                "close": round(wc[i_last], 2),
                "sma30": round(sma[i_last], 2) if sma[i_last] is not None else None,
            })

    breadth_history = [
        {"week": wk, "stage1": c[1], "stage2": c[2], "stage3": c[3], "stage4": c[4]}
        for wk, c in sorted(breadth.items())
    ]
    return current_signals, breadth_history


# ══════════════════════════════════════════════════════════════
# SYNTHETIC DATA — one clean weekday-only price path per test case
# Volume fixed at 5,00,000 & price ~₹100+ so turnover clears the
# 3,00,00,000 liquidity floor easily.
# ══════════════════════════════════════════════════════════════

def _weekdays(start, n):
    """n weekday (Mon-Fri) ISO date strings starting from `start` (a date)."""
    out = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _build_stock(closes, volume=10_000_000):
    """closes -> full {d,h,l,c,v} stock dict. h/l = c +/- 0.5% (tight, realistic).
    volume default high enough that liquidity floor (turnover >= 3,00,00,000)
    clears even down to ~₹30 share price — DOWNTREND_STOCK needs this since it
    decays a lot over 750 days."""
    n = len(closes)
    dates = _weekdays(date(2023, 1, 2), n)
    highs = [round(c * 1.005, 2) for c in closes]
    lows = [round(c * 0.995, 2) for c in closes]
    volumes = [volume] * n
    return {"d": dates, "o": closes[:], "h": highs, "l": lows, "c": closes, "v": volumes}


def make_uptrend(n=750, start=100.0, weekly_pct=1.2):
    """Steady advance -> should classify Stage 2 (Advancing)."""
    closes, c = [], start
    daily_pct = weekly_pct / 5 / 100
    for i in range(n):
        c *= (1 + daily_pct)
        closes.append(round(c, 2))
    return closes


def make_downtrend(n=750, start=300.0, weekly_pct=1.2):
    """Steady decline -> should classify Stage 4 (Declining)."""
    closes, c = [], start
    daily_pct = weekly_pct / 5 / 100
    for i in range(n):
        c *= (1 - daily_pct)
        closes.append(round(c, 2))
    return closes


def make_basing(n=750, start=300.0, decline_weeks_frac=0.5):
    """Decline for the first ~half, then flat/sideways -> Stage 1 (Basing)."""
    decline_n = int(n * decline_weeks_frac)
    closes = make_downtrend(decline_n, start=start, weekly_pct=1.2)
    base = closes[-1]
    for i in range(n - decline_n):
        # small sideways oscillation, no net drift
        wobble = base * 0.015 * (1 if i % 10 < 5 else -1)
        closes.append(round(base + wobble, 2))
    return closes


def make_topping(n=750, start=100.0, advance_weeks_frac=0.5):
    """Advance for the first ~half, then flat/sideways -> Stage 3 (Topping)."""
    advance_n = int(n * advance_weeks_frac)
    closes = make_uptrend(advance_n, start=start, weekly_pct=1.2)
    top = closes[-1]
    for i in range(n - advance_n):
        wobble = top * 0.015 * (1 if i % 10 < 5 else -1)
        closes.append(round(top + wobble, 2))
    return closes


def make_breakout(n=750, start=100.0, base_weeks_frac=0.92):
    """Long flat base, then a recent breakout into an advance
    -> should show stage_change=True, prev_stage=1, stage=2."""
    base_n = int(n * base_weeks_frac)
    closes = []
    base = start
    for i in range(base_n):
        wobble = base * 0.015 * (1 if i % 10 < 5 else -1)
        closes.append(round(base + wobble, 2))
    breakout = make_uptrend(n - base_n, start=closes[-1], weekly_pct=2.0)
    closes.extend(breakout)
    return closes


def build_test_dataset():
    return {
        "UPTREND_STOCK":  _build_stock(make_uptrend()),
        "DOWNTREND_STOCK": _build_stock(make_downtrend()),
        "BASING_STOCK":   _build_stock(make_basing()),
        "TOPPING_STOCK":  _build_stock(make_topping()),
        "BREAKOUT_STOCK": _build_stock(make_breakout()),
    }


EXPECTED = {
    "UPTREND_STOCK":  {"stage": 2},
    "DOWNTREND_STOCK": {"stage": 4},
    "BASING_STOCK":   {"stage": 1},
    "TOPPING_STOCK":  {"stage": 3},
    # stage_change fires True only in the exact transition week; with synthetic
    # weekly-resampled data hitting that exact week is fiddly, so this checks
    # "recently flipped to Advancing" (<=10 weeks) rather than the literal last week.
    "BREAKOUT_STOCK": {"stage": 2, "weeks_in_stage_max": 10},
}


def main():
    all_data = build_test_dataset()
    signals, breadth_history = _detect_weinstein_stages(all_data)
    by_sym = {s["symbol"]: s for s in signals}

    print(f"{'Symbol':<18}{'Stage':<12}{'Prev':<6}{'Changed':<9}{'WeeksInStage':<14}{'Close':<10}{'SMA30'}")
    print("-" * 80)
    for sym in EXPECTED:
        s = by_sym.get(sym)
        if not s:
            print(f"{sym:<18}NO SIGNAL (not enough qualifying weeks — check min_weeks/liquidity)")
            continue
        print(f"{sym:<18}{STAGE_NAMES[s['stage']]:<12}{str(s['prev_stage']):<6}"
              f"{str(s['stage_change']):<9}{s['weeks_in_stage']:<14}{s['close']:<10}{s['sma30']}")

    print(f"\nBreadth history: {len(breadth_history)} weeks tracked "
          f"(latest: {breadth_history[-1] if breadth_history else None})")

    # ── Assertions against expected stages ──
    print("\n--- Checks ---")
    all_ok = True
    for sym, exp in EXPECTED.items():
        s = by_sym.get(sym)
        if not s:
            print(f"FAIL {sym}: no signal produced")
            all_ok = False
            continue
        for key, exp_val in exp.items():
            if key == "weeks_in_stage_max":
                got = s.get("weeks_in_stage")
                ok = got is not None and got <= exp_val
                all_ok &= ok
                print(f"{'OK  ' if ok else 'FAIL'} {sym}: weeks_in_stage = {got} (expected <= {exp_val})")
                continue
            got = s.get(key)
            ok = got == exp_val
            all_ok &= ok
            print(f"{'OK  ' if ok else 'FAIL'} {sym}: {key} = {got} (expected {exp_val})")

    print("\n" + ("ALL CHECKS PASSED ✅" if all_ok else "SOME CHECKS FAILED ❌"))


if __name__ == "__main__":
    main()
