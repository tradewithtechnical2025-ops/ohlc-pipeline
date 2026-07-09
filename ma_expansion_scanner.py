"""
MA Expansion / Launch-Pad Scanner
----------------------------------
Detects stocks where a coiled/flat MA stack (fast, mid, slow) has just
started to fan out (expand) with proper bullish alignment, within the
last `trigger_window` days. Mirrors the contraction->expansion logic
used in htf_test_scan.py, applied to MA spread instead of price range.

Expected input: df with columns ['date','close'] sorted ascending by date,
one df per symbol (same convention as your other scanners).
"""

import pandas as pd
import numpy as np


def _slope(series: pd.Series, lookback: int = 5) -> float:
    """Simple slope via linear fit over the last `lookback` points."""
    y = series.tail(lookback).values
    if len(y) < lookback or np.any(pd.isna(y)):
        return np.nan
    x = np.arange(len(y))
    return np.polyfit(x, y, 1)[0]


def detect_ma_expansion(
    df: pd.DataFrame,
    fast: int = 21,
    mid: int = 50,
    slow: int = 150,
    contraction_window: int = 20,     # bars to look back for the "coil"
    trigger_window: int = 7,          # "few days back" -> how recent the trigger must be
    contraction_thresh_pct: float = 4.0,   # spread%% considered "tight"
    expansion_thresh_pct: float = 6.0,     # spread%% considered "expanded"
    min_er: float = 0.5,              # reuse your Kaufman ER filter for genuineness
) -> dict:
    d = df.copy()
    d["ema_fast"] = d["close"].ewm(span=fast, adjust=False).mean()
    d["ema_mid"] = d["close"].ewm(span=mid, adjust=False).mean()
    d["sma_slow"] = d["close"].rolling(slow).mean()

    d["spread_pct"] = (d["ema_fast"] - d["sma_slow"]) / d["sma_slow"] * 100
    d["stacked"] = (d["ema_fast"] > d["ema_mid"]) & (d["ema_mid"] > d["sma_slow"])

    if len(d) < slow + contraction_window + trigger_window:
        return {"signal": False, "reason": "not_enough_history"}

    recent = d.tail(contraction_window + trigger_window).reset_index(drop=True)
    contraction_part = recent.iloc[:contraction_window]
    trigger_part = recent.iloc[contraction_window:]

    # 1. Was it coiled? median spread during contraction window stayed tight
    was_coiled = contraction_part["spread_pct"].abs().median() < contraction_thresh_pct

    # 2. Did stacking NOT hold through most of the contraction window?
    stacking_before = contraction_part["stacked"].mean() < 0.5

    # 3. Find first bar in trigger window where stacking turns true AND stays true after
    trigger_idx = None
    for i in range(len(trigger_part)):
        if trigger_part["stacked"].iloc[i:].all():
            trigger_idx = i
            break

    if trigger_idx is None:
        return {"signal": False, "reason": "no_recent_stacking_trigger"}

    days_since_trigger = len(trigger_part) - trigger_idx  # bars since expansion started
    spread_now = recent["spread_pct"].iloc[-1]
    spread_expanding = recent["spread_pct"].iloc[-3:].is_monotonic_increasing

    # 4. Fast MA slope must have flipped from flat/negative to positive
    slope_now = _slope(d["ema_fast"], lookback=5)

    # 5. Optional: Kaufman Efficiency Ratio to reject noisy/choppy "expansion"
    change = (d["close"].iloc[-fast] - d["close"].iloc[-1])
    volatility = d["close"].diff().abs().tail(fast).sum()
    er = abs(change) / volatility if volatility != 0 else 0

    signal = (
        was_coiled
        and stacking_before
        and spread_now > expansion_thresh_pct * 0.5  # allow early-stage expansion
        and spread_expanding
        and slope_now is not None and not np.isnan(slope_now) and slope_now > 0
        and er >= min_er
    )

    return {
        "signal": bool(signal),
        "days_since_trigger": days_since_trigger,
        "spread_now_pct": round(spread_now, 2),
        "was_coiled": was_coiled,
        "er": round(er, 2),
        "fast_ma_slope": slope_now,
    }


def run_scan(data_dir: str = "data/ohlc", output_path: str = "ma_expansion_results.json") -> None:
    """
    Loops over per-symbol OHLC files, runs detect_ma_expansion, and writes
    matches to output_path (so the GitHub Actions job has something to commit).

    ASSUMPTION: one CSV per symbol at data_dir/{symbol}.csv with a 'close'
    column, sorted ascending by date. Change this loader to match however
    your pipeline actually stores OHLC (R2 chunks / manifest JSON / etc).
    """
    import os
    import json

    results = []
    if not os.path.isdir(data_dir):
        print(f"data_dir '{data_dir}' not found - nothing to scan.")
        with open(output_path, "w") as f:
            json.dump([], f)
        return

    for fname in os.listdir(data_dir):
        if not fname.endswith(".csv"):
            continue
        symbol = fname.replace(".csv", "")
        try:
            df = pd.read_csv(os.path.join(data_dir, fname))
            out = detect_ma_expansion(df)
            if out.get("signal"):
                results.append({"symbol": symbol, **out})
        except Exception as e:
            print(f"skipping {symbol}: {e}")

    results.sort(key=lambda r: r["days_since_trigger"])

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Scan complete. {len(results)} matches written to {output_path}.")


if __name__ == "__main__":
    run_scan()
