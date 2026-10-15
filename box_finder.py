#!/usr/bin/env python3
"""TwT Box Finder: independent confirmed 9-bar swing boxes with dynamic lows.

A box's resistance is fixed at its confirmed swing high. Its support follows
subsequent candle lows, until a breakout or the maximum 30% depth is exceeded.
Each box has its own lifecycle; a newer pivot never suppresses an older box.
"""
import numpy as np

MAX_DEPTH = .30
MIN_INITIAL_DEPTH = .055
BREAKOUT_BUFFER = .0025


def cons(closes, top, start):
    """Count consolidation only after this box's confirmed swing low."""
    c = np.asarray(closes, dtype=float)
    for w in range(min(15, len(c) - start), 6, -1):
        x = c[-w:]
        near = np.mean((x >= top * .95) & (x <= top * 1.003))
        last = np.mean((x[-5:] >= top * .95) & (x[-5:] <= top * 1.003))
        if (0 <= (top - c[-1]) / top <= .05
                and (max(x) - min(x)) / top <= .05
                and near >= .7 and last >= .8
                and (np.mean(x[-3:]) - np.mean(x[:3])) / top >= -.015):
            return w
    return 0


def detect(a):
    h = np.asarray(a['h'], dtype=float)
    l = np.asarray(a['l'], dtype=float)
    c = np.asarray(a['c'], dtype=float)
    n = len(c)
    if n < 40 or len(h) != n or len(l) != n:
        return [], None
    begin = max(4, n - 160)
    sh = [i for i in range(begin, n - 4)
          if h[i] > max(h[i-4:i]) and h[i] > max(h[i+1:i+5])]
    sl = [j for j in range(begin, n - 4)
          if l[j] < min(l[j-4:j]) and l[j] < min(l[j+1:j+5])]
    boxes = []
    for i in sh:
        js = [j for j in sl if j > i and j + 4 < n]
        if not js:
            continue
        j = js[0]
        top = float(h[i])
        initial_low = float(l[j])
        initial_depth = (top - initial_low) / top
        if top <= 0 or not MIN_INITIAL_DEPTH <= initial_depth <= MAX_DEPTH:
            continue

        # A box starts once its first subsequent swing low is confirmed.
        # It then tracks the lowest actual candle LOW, not the close.
        # An upper breakout freezes the box; exceeding 30% depth retires it.
        low = initial_low
        low_idx = j
        breakout = None
        overdepth = None
        for t in range(j + 1, n):
            if l[t] < low:
                low = float(l[t])
                low_idx = t
            if (top - low) / top > MAX_DEPTH + 1e-10:
                overdepth = t
                break
            if t >= j + 4 and c[t] > top * (1 + BREAKOUT_BUFFER):
                breakout = t
                break
        if overdepth is not None:
            # Historical box remains visible in box_history but never active.
            state = 'Depth Exceeded'
        elif breakout is not None:
            state = 'BO'
        else:
            distance = (top - c[-1]) / top
            window = cons(c, top, j + 4)
            state = ('Consolidation Near BO' if window else
                     'Near BO' if 0 <= distance <= .02 else 'Box Forming')
        window = cons(c, top, j + 4) if state == 'Consolidation Near BO' else 0
        boxes.append(dict(i=i, j=j, top=top, bot=low, low_idx=low_idx,
                          bo=breakout, overdepth=overdepth, state=state,
                          cons=window, depth=(top-low)/top))

    # Only remove true duplicate pivot identities, never nearby independent boxes.
    kept = list({(b['i'], b['j']): b for b in boxes}.values())
    # Show the dominant recent resistance, rather than the smallest newest nested box.
    # Independent boxes still remain in history and evolve separately.
    recent = [b for b in kept if b['bo'] is None and b['overdepth'] is None
              and b['i'] >= n - 90]
    active = max(recent, key=lambda b: (b['top'], b['i'])) if recent else next(
        (b for b in reversed(kept) if b['bo'] is None and b['overdepth'] is None), None)
    return kept, active


def select_box(stock):
    if len(stock.get('c', [])) < 40:
        return [], None, None
    boxes, active = detect(stock)
    today = len(stock['c']) - 1
    fresh = [b for b in boxes if b['bo'] == today]
    if fresh:
        chosen = max(fresh, key=lambda b: b['i'])
        return boxes, chosen, 'BO'
    return boxes, active, active['state'] if active else None


def build_box_finder(all_data, asof=None):
    results, history = [], {}
    for symbol, stock in sorted(all_data.items()):
        try:
            boxes, chosen, status = select_box(stock)
        except (ValueError, TypeError, IndexError, ZeroDivisionError):
            continue
        if not boxes:
            continue
        dates = stock['d']
        def encode(b):
            return {
                'start_date': dates[b['i']],
                'swing_low_date': dates[b['j']],
                'lower_updated_date': dates[b['low_idx']],
                'upper': round(float(b['top']), 4),
                'lower': round(float(b['bot']), 4),
                'depth_pct': round(float(b['depth']) * 100, 2),
                'breakout_date': dates[b['bo']] if b['bo'] is not None else None,
                'depth_exceeded_date': dates[b['overdepth']] if b['overdepth'] is not None else None,
                'consolidation_candles': int(b['cons']),
                'status': b['state'],
            }
        history[symbol] = [encode(b) for b in boxes]
        if chosen is None:
            continue
        close = float(stock['c'][-1])
        results.append({
            'symbol': symbol, 'date': dates[-1], 'status': status,
            'start_date': dates[chosen['i']],
            'upper': round(float(chosen['top']), 4),
            'lower': round(float(chosen['bot']), 4),
            'close': round(close, 4),
            'distance_pct': round(100 * (float(chosen['top']) - close) / float(chosen['top']), 2),
            'consolidation_candles': int(chosen['cons']),
            'retained_boxes': len(boxes),
        })
    return {
        'updated': asof or max((v['d'][-1] for v in all_data.values() if v.get('d')), default=None),
        'total': len(results), 'stocks': results, 'box_history': history,
    }
