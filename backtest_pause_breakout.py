"""
backtest_pause_breakout.py
---------------------------
Backtests the entry/exit rule discussed:

  ENTRY:  Pause stayed "clean" (High <= reclaim_high * 1.005) for at least
          --min-clean-days consecutive days right after the reclaim day.
          Then buy when price trades ABOVE the high of that clean box
          (box_high = max(reclaim day's High, highs of the clean streak)).
          Entry is assumed filled exactly at box_high on the first day the
          High reaches/exceeds it (standard backtest simplification — no
          intraday open price is available in this OHLC feed).

  STOP:   4% below entry price (--sl-pct), fixed for the life of the trade
          (no trailing / breakeven move unless you ask for one).

  TARGETS: R = entry_price - stop_price (the risk per share).
          - Sell 50% of the position at entry + 4R (--target1-r / --exit1-frac)
          - Sell the remaining 50% at entry + 6R (--target2-r)
          If stop is hit before a target, whatever quantity is still open
          exits at the stop. If neither stop nor final target is hit within
          --max-hold-days, the remaining quantity exits at the last close
          (a "time exit" — NOT a stop, NOT a target, just ran out of runway).

  Ordering rule for a day that could hit both stop and a target: the stop
  is checked first (conservative/worst-case assumption), so a stop-out
  always wins over a target on the same day.

This reuses shakeout_scanner.detect_shakeout() for the shakeout+pause
detection (so the two scripts always agree on what counts as a valid
pause) and only adds the clean-streak / breakout / trade-simulation layer
on top.

Usage:
    export WORKER_URL="https://your-worker-url"
    export WORKER_TOKEN="your-secret-token"
    python backtest_pause_breakout.py
    python backtest_pause_breakout.py --symbol MARKSANS
    python backtest_pause_breakout.py --min-clean-days 4 --save results.json
"""

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone

from shakeout_scanner import (
    detect_shakeout,
    _check_liquidity,
    download_all_chunks,
)


def _primary_detail(entry):
    """Pick the per-EMA detail that produced the entry's best pause_days —
    that's the one whose reclaim_high / pause_high_cap we anchor the
    breakout box to."""
    return max(entry["details"], key=lambda d: d["pause_days"])


def _find_clean_streak_and_box(highs, recovery_idx, pause_days, pause_high_cap, reclaim_high):
    """Walks the (already-validated) pause window day by day and finds how
    many CONSECUTIVE days from the start stayed under pause_high_cap (the
    "clean streak"), plus the resulting breakout box's high.

    Returns (clean_streak_days, box_high, break_idx) where break_idx is the
    index of the first day that broke the clean streak (either by poking
    above the cap, or — if the streak used up the whole pause — the index
    right after the last pause day). break_idx is where we start scanning
    for the actual breakout.
    """
    box_high = reclaim_high
    streak = 0
    j = recovery_idx + 1
    last_pause_idx = recovery_idx + pause_days  # inclusive, per detect_shakeout's pause_days count
    while j <= last_pause_idx and j < len(highs):
        h = highs[j]
        if h is None:
            break
        if h <= pause_high_cap:
            streak += 1
            box_high = max(box_high, h)
            j += 1
        else:
            break
    return streak, box_high, j


def backtest_signal(dates, highs, lows, closes, entry_signal,
                     min_clean_days=3, sl_pct=0.04,
                     target1_r=4, target2_r=6, exit1_frac=0.5,
                     breakout_search_days=20, max_hold_days=60):
    n = len(dates)
    detail = _primary_detail(entry_signal)
    date_to_idx = {d: i for i, d in enumerate(dates)}
    recovery_idx = date_to_idx.get(detail["recovery_date"])
    if recovery_idx is None:
        return None

    pause_high_cap = detail["pause_high_cap"]
    reclaim_high = detail["reclaim_high"]
    pause_days = detail["pause_days"]
    if pause_high_cap is None or reclaim_high is None or pause_days == 0:
        return None

    clean_streak, box_high, break_idx = _find_clean_streak_and_box(
        highs, recovery_idx, pause_days, pause_high_cap, reclaim_high)

    if clean_streak < min_clean_days:
        return {"status": "no_setup", "clean_streak": clean_streak, "box_high": box_high}

    # Search forward from break_idx for the breakout (High >= box_high).
    entry_idx = None
    for k in range(break_idx, min(n, break_idx + breakout_search_days)):
        if highs[k] is not None and highs[k] >= box_high:
            entry_idx = k
            break

    if entry_idx is None:
        return {"status": "no_breakout", "clean_streak": clean_streak, "box_high": box_high}

    entry_price = box_high
    sl_price = round(entry_price * (1 - sl_pct), 2)
    risk = entry_price - sl_price
    target1_price = round(entry_price + target1_r * risk, 2)
    target2_price = round(entry_price + target2_r * risk, 2)

    tranche1_booked = False
    tranche1_date = tranche1_price = None
    tranche2_booked = False
    tranche2_date = tranche2_price = None
    stopped = False
    stop_date = None
    stop_tranche = None  # "full" or "remainder"
    time_exit = False
    time_exit_date = time_exit_price = None

    end_idx = min(n - 1, entry_idx + max_hold_days)
    k = entry_idx
    while k <= end_idx:
        lo, hi = lows[k], highs[k]
        if lo is None or hi is None:
            k += 1
            continue

        if lo <= sl_price:
            stopped = True
            stop_date = dates[k]
            stop_tranche = "remainder" if tranche1_booked else "full"
            break

        if not tranche1_booked and hi >= target1_price:
            tranche1_booked = True
            tranche1_date = dates[k]
            tranche1_price = target1_price

        if tranche1_booked and not tranche2_booked and hi >= target2_price:
            tranche2_booked = True
            tranche2_date = dates[k]
            tranche2_price = target2_price
            break  # fully closed

        k += 1

    if not stopped and not tranche2_booked:
        time_exit = True
        time_exit_date = dates[end_idx]
        time_exit_price = closes[end_idx]

    # ---- Compute weighted R-multiple for the whole trade ----
    if stopped and stop_tranche == "full":
        r_multiple = -1.0
        outcome = "stopped_before_target"
    elif stopped and stop_tranche == "remainder":
        r_multiple = exit1_frac * target1_r + (1 - exit1_frac) * (-1.0)
        outcome = "partial_target_then_stopped"
    elif tranche2_booked:
        r_multiple = exit1_frac * target1_r + (1 - exit1_frac) * target2_r
        outcome = "full_target"
    elif time_exit:
        time_exit_r = (time_exit_price - entry_price) / risk
        if tranche1_booked:
            r_multiple = exit1_frac * target1_r + (1 - exit1_frac) * time_exit_r
            outcome = "partial_target_then_time_exit"
        else:
            r_multiple = time_exit_r
            outcome = "time_exit_no_target"
    else:
        r_multiple = 0.0
        outcome = "unknown"

    return {
        "status": "trade",
        "clean_streak": clean_streak,
        "box_high": round(box_high, 2),
        "entry_date": dates[entry_idx],
        "entry_price": round(entry_price, 2),
        "sl_price": sl_price,
        "target1_price": target1_price,
        "target2_price": target2_price,
        "tranche1_date": tranche1_date,
        "tranche2_date": tranche2_date,
        "stopped": stopped,
        "stop_date": stop_date,
        "stop_tranche": stop_tranche,
        "time_exit": time_exit,
        "time_exit_date": time_exit_date,
        "outcome": outcome,
        "r_multiple": round(r_multiple, 2),
    }


def summarize(trades):
    real_trades = [t for t in trades if t["status"] == "trade"]
    n_setups = len(trades)
    n_trades = len(real_trades)
    if n_trades == 0:
        return {"n_setups_found": n_setups, "n_trades_taken": 0}

    r_values = [t["r_multiple"] for t in real_trades]
    wins = [r for r in r_values if r > 0]
    outcome_counts = {}
    for t in real_trades:
        outcome_counts[t["outcome"]] = outcome_counts.get(t["outcome"], 0) + 1

    return {
        "n_setups_found": n_setups,
        "n_no_breakout": sum(1 for t in trades if t["status"] == "no_breakout"),
        "n_no_setup_not_enough_clean_days": sum(1 for t in trades if t["status"] == "no_setup"),
        "n_trades_taken": n_trades,
        "win_rate_pct": round(100 * len(wins) / n_trades, 1),
        "avg_r": round(statistics.mean(r_values), 2),
        "median_r": round(statistics.median(r_values), 2),
        "best_r": round(max(r_values), 2),
        "worst_r": round(min(r_values), 2),
        "outcome_breakdown": outcome_counts,
    }


def main():
    ap = argparse.ArgumentParser(description="Backtest: clean-pause breakout, 4%% SL, 4R/6R scale-out")
    ap.add_argument("--symbol", help="run for just one symbol")
    ap.add_argument("--min-clean-days", type=int, default=3)
    ap.add_argument("--sl-pct", type=float, default=0.04)
    ap.add_argument("--target1-r", type=float, default=4.0)
    ap.add_argument("--target2-r", type=float, default=6.0)
    ap.add_argument("--exit1-frac", type=float, default=0.5)
    ap.add_argument("--breakout-search-days", type=int, default=20)
    ap.add_argument("--max-hold-days", type=int, default=60)
    ap.add_argument("--min-pause-days", type=int, default=2,
                     help="passed through to detect_shakeout")
    ap.add_argument("--save", help="optional path to save results as JSON")
    args = ap.parse_args()

    print("Downloading OHLC chunks...")
    all_data = download_all_chunks()
    print(f"\nTotal loaded: {len(all_data)} stocks\n")

    all_trades = []
    for sym, s in all_data.items():
        if args.symbol and sym != args.symbol:
            continue
        if not _check_liquidity(s.get("v", []), s.get("c", []), len(s.get("d", []))):
            continue

        dates, highs, lows, closes = s["d"], s["h"], s["l"], s["c"]
        signals = detect_shakeout(s, min_pause_days=args.min_pause_days)
        for sig in signals:
            if not sig["pause_valid"]:
                continue
            result = backtest_signal(
                dates, highs, lows, closes, sig,
                min_clean_days=args.min_clean_days,
                sl_pct=args.sl_pct,
                target1_r=args.target1_r,
                target2_r=args.target2_r,
                exit1_frac=args.exit1_frac,
                breakout_search_days=args.breakout_search_days,
                max_hold_days=args.max_hold_days,
            )
            if result is None:
                continue
            result["symbol"] = sym
            result["breakdown_date"] = sig["breakdown_date"]
            result["recovery_date"] = sig["recovery_date"]
            all_trades.append(result)

    summary = summarize(all_trades)
    print("=== Summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    print("\n=== Trades ===")
    for t in all_trades:
        if t["status"] != "trade":
            continue
        print(f"  {t['symbol']:<15} entry {t['entry_date']} @ {t['entry_price']}  "
              f"SL {t['sl_price']}  T1 {t['target1_price']}  T2 {t['target2_price']}  "
              f"-> {t['outcome']}  R={t['r_multiple']}")

    if args.save:
        with open(args.save, "w") as f:
            json.dump({
                "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "params": {
                    "min_clean_days": args.min_clean_days, "sl_pct": args.sl_pct,
                    "target1_r": args.target1_r, "target2_r": args.target2_r,
                    "exit1_frac": args.exit1_frac,
                },
                "summary": summary,
                "trades": all_trades,
            }, f, indent=2)
        print(f"\nSaved to {args.save}")


if __name__ == "__main__":
    main()
