"""
compare_er_filter.py
---------------------
Shows exactly which Mini HTF signals the new Efficiency Ratio (ER) filter
dropped, by running detect_htf() twice per stock — once with the ER filter
active (min_efficiency_ratio=0.5, i.e. your current/new behavior) and once
with it disabled (min_efficiency_ratio=0.0, i.e. old behavior) — then diffing
the two result sets.

Place this file in the same folder as your updated htf_test_scan.py and run:

    export WORKER_URL="https://your-worker-url"
    export WORKER_TOKEN="your-secret-token"
    python compare_er_filter.py
    python compare_er_filter.py --save dropped_by_er.json

For every stock where a Mini HTF signal disappeared once the ER filter is
applied, prints the OLD signal (the one that no longer shows up) plus its
efficiency ratio, so you can quickly eyeball the chart and confirm the drop
was correct (choppy/flat) or flag it if it looks wrong.
"""

import argparse
import json
from datetime import datetime, timezone

from htf_test_scan import (
    download_all_chunks,
    _check_liquidity,
    detect_htf,
)

MINI_HTF_PARAMS = dict(min_gain_pct=20.0, max_gain_pct=90.0, max_pullback_pct=15.0,
                        pole_min_days=5, pole_max_days=40, flag_min_days=5, flag_max_days=21,
                        success_rr_multiple=2.0)


def _final_signals(s, min_efficiency_ratio):
    """Same shape/filter as main()'s output: drop 'failed', keep the rest."""
    out = {}
    for m in detect_htf(s, min_efficiency_ratio=min_efficiency_ratio, **MINI_HTF_PARAMS):
        if m["status"] == "failed":
            continue
        out[(m["pole_low_date"], m["pole_high_date"])] = m
    return out


def _efficiency_ratio(closes, lo_date, hi_date, dates):
    lo = dates.index(lo_date)
    hi = dates.index(hi_date)
    vals = [closes[k] for k in range(lo, hi + 1) if closes[k] is not None]
    if len(vals) < 2:
        return None
    net = abs(vals[-1] - vals[0])
    total = sum(abs(vals[k] - vals[k - 1]) for k in range(1, len(vals)))
    return round(net / total, 3) if total else None


def main():
    ap = argparse.ArgumentParser(description="Show which Mini HTF signals the ER filter dropped")
    ap.add_argument("--save", help="optional path to save the dropped-signals list as JSON")
    args = ap.parse_args()

    print("Downloading OHLC chunks...")
    all_data = download_all_chunks()
    print(f"Total loaded: {len(all_data)} stocks\n")

    liquid = {}
    for sym, s in all_data.items():
        if _check_liquidity(s.get("v", []), s.get("c", []), len(s.get("d", []))):
            liquid[sym] = s

    dropped = []
    new_total = old_total = 0

    for sym, s in liquid.items():
        old_signals = _final_signals(s, min_efficiency_ratio=0.0)   # ER filter OFF (old behavior)
        new_signals = _final_signals(s, min_efficiency_ratio=0.5)   # ER filter ON  (current behavior)
        old_total += len(old_signals)
        new_total += len(new_signals)

        for key, old_m in old_signals.items():
            if key not in new_signals:
                er = _efficiency_ratio(s["c"], old_m["pole_low_date"], old_m["pole_high_date"], s["d"])
                dropped.append({
                    "symbol": sym,
                    "pole_low_date": old_m["pole_low_date"],
                    "pole_high_date": old_m["pole_high_date"],
                    "pole_gain_pct": old_m["pole_gain_pct"],
                    "pole_days": old_m["pole_days"],
                    "status_before_drop": old_m["status"],
                    "efficiency_ratio": er,
                })

    dropped.sort(key=lambda x: x["efficiency_ratio"] if x["efficiency_ratio"] is not None else 0, reverse=True)

    print(f"Mini HTF signal count — OLD (no ER filter): {old_total}   NEW (ER >= 0.5): {new_total}")
    print(f"Dropped: {len(dropped)}\n")
    print(f"{'SYMBOL':<15} {'POLE LOW':<12} {'POLE HIGH':<12} {'GAIN%':>7} {'DAYS':>5} {'ER':>6}  STATUS(before)")
    for d in dropped:
        print(f"{d['symbol']:<15} {d['pole_low_date']:<12} {d['pole_high_date']:<12} "
              f"{d['pole_gain_pct']:>6.1f}% {d['pole_days']:>5} {d['efficiency_ratio']:>6}  {d['status_before_drop']}")

    if args.save:
        with open(args.save, "w") as f:
            json.dump({
                "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "old_total": old_total,
                "new_total": new_total,
                "dropped_count": len(dropped),
                "dropped": dropped,
            }, f, indent=2)
        print(f"\nSaved to {args.save}")


if __name__ == "__main__":
    main()
