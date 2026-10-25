#!/usr/bin/env python3
"""TwT Box Finder: independent confirmed 9-bar swing boxes with dynamic lows.

A box's resistance is fixed at its confirmed swing high. Its support follows
subsequent candle lows, until a breakout or the maximum 30% depth is exceeded.
Each box has its own lifecycle; a newer pivot never suppresses an older box.

Same rules as the TwT BO Signal script v1.5:
- a box starts at the FIRST touch of its resistance (EXTEND_BACK);
- after a breakout, a close up to RETEST_BAND under the top is a retest, a close
  further back inside within FAIL_WINDOW bars is a failed breakout (poke);
- a poke raises the top to the highest CLOSE of the poke, never to a wick;
- up to three nested boxes are kept (outer, middle, inner).
"""
import numpy as np

MAX_DEPTH = .30
MIN_INITIAL_DEPTH = .055
BREAKOUT_BUFFER = .0025
MIN_BASE_CANDLES = 9  # a box must hold at least this many bars (swing high .. bar before BO)
SAME_TOP_TOL = .02     # boxes whose tops are this close are the same resistance
MERGE_GAP = 5          # a same-top box starting within this many bars after a breakout continues it
FAIL_WINDOW = 5        # a breakout that closes back below the top within this many bars failed (poke)
POKE_MAX = .05         # total poke allowance above the box's ORIGINAL top; beyond it a move is a real breakout
RETEST_BAND = .02      # a close up to 2% under the top after a breakout is a retest, not a failed breakout
POKE_CLOSE_MAX = .02   # a poke only closes marginally above the top; a stronger close is a real breakout
EXTEND_BACK = 160      # how far back to look for the first touch of the same resistance
EXTEND_LOW_TOL = .02   # the walk back stops at a low more than this under the swing low
MAX_NEST = 2           # a box may sit inside at most this many other boxes (three levels in all)


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


def prune(boxes):
    """Remove redundant boxes.

    1. Same resistance as an older box:
       - the swing high forms while the older box is running and is within
         -SAME_TOP_TOL .. +POKE_MAX of the older box's top at that bar (or its
         poke-raised top ends up there) -> part of the older box, dropped
         (a clearly lower high is an inner base);
       - same top (SAME_TOP_TOL) starting within MERGE_GAP bars after the older
         box's breakout -> one box: the older start, the newer top and status.
    2. Boxes shorter than MIN_BASE_CANDLES are dropped.
    3. Nesting: keep up to three levels (outer, middle, inner); drop boxes
       that sit inside more than MAX_NEST other boxes (inside = starts later,
       ends no later, top not higher, bottom not lower).
    """
    def top_at(a, t):
        cur = a['top_hist'][0][1]
        for bar, val in a['top_hist']:
            if bar <= t:
                cur = val
        return cur

    out = []
    for b in sorted(boxes, key=lambda x: x['i']):
        action = None
        for k, a in enumerate(out):
            if a['overdepth'] is not None:
                continue
            if a['i'] <= b['i'] <= a['last']:
                # b's swing high forms while a is running: same resistance
                # (or a small poke over it) -> b is part of a.
                # Also when pokes later lift b's top up to a's top: compare
                # b's final top with a's top at b's last bar.
                ratio = b['top0'] / top_at(a, b['i']) - 1
                ratio_end = b['top'] / top_at(a, min(b['last'], a['last'])) - 1
                if -SAME_TOP_TOL <= ratio <= POKE_MAX or -SAME_TOP_TOL <= ratio_end <= POKE_MAX:
                    action = ('drop', k)
                    break
            elif (a['bo'] is not None and a['last'] < b['i'] <= a['last'] + 1 + MERGE_GAP
                  and abs(a['top'] - b['top0']) / max(a['top'], b['top0']) <= SAME_TOP_TOL):
                action = ('merge', k)
                break
        if action is None:
            out.append(b)
        elif action[0] == 'merge':
            a = out[action[1]]
            m = dict(b)
            m['i'], m['j'], m['start'] = a['i'], a['j'], a['start']
            m['top_hist'] = a['top_hist'] + b['top_hist']
            if a['bot'] < b['bot']:
                m['bot'], m['low_idx'] = a['bot'], a['low_idx']
            m['depth'] = (m['top'] - m['bot']) / m['top']
            m['base_length_candles'] = m['last'] - m['start'] + 1
            if m['depth'] > MAX_DEPTH + 1e-10:
                out.append(b)  # too deep as one box: keep them separate
            else:
                out[action[1]] = m
    out = [b for b in out if b['base_length_candles'] >= MIN_BASE_CANDLES]

    def inside(b, a):
        return (a is not b and a['start'] <= b['start'] and a['last'] >= b['last']
                and a['top'] >= b['top'] and a['bot'] <= b['bot'])
    return [b for b in out if sum(1 for a in out if inside(b, a)) <= MAX_NEST]


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
        # Price already broke out above this swing high before the swing low
        # that starts the box was confirmed: there is no base here.
        if any(c[t] > top * (1 + POKE_CLOSE_MAX) for t in range(i + 1, j + 1)):
            continue

        # A box starts once its first subsequent swing low is confirmed.
        # It then tracks the lowest actual candle LOW, not the close.
        # An upper breakout freezes the box; exceeding 30% depth retires it.
        low = initial_low
        low_idx = j

        # Start the box at the FIRST touch of the same resistance: walk back from
        # the swing high while no close went above the top and no low went more
        # than EXTEND_LOW_TOL under the swing low; the earliest high within
        # SAME_TOP_TOL of the top starts the box (it must stay within MAX_DEPTH).
        start = i
        run_low, run_idx = low, low_idx
        for k in range(i - 1, max(-1, i - 1 - EXTEND_BACK), -1):
            if c[k] > top * (1 + BREAKOUT_BUFFER) or l[k] < initial_low * (1 - EXTEND_LOW_TOL):
                break
            if l[k] < run_low:
                run_low, run_idx = float(l[k]), k
            if h[k] >= top * (1 - SAME_TOP_TOL):
                if (top - run_low) / top > MAX_DEPTH + 1e-10:
                    break
                start, low, low_idx = k, run_low, run_idx

        breakout = None
        overdepth = None
        skip_until = -1
        top_hist = [(i, top)]  # (bar, top) whenever a poke raises the top
        for t in range(j + 1, n):
            if l[t] < low:
                low = float(l[t])
                low_idx = t
            if (top - low) / top > MAX_DEPTH + 1e-10:
                overdepth = t
                break
            if t < skip_until:
                continue
            if t >= j + 4 and c[t] > top * (1 + BREAKOUT_BUFFER):
                # Failed breakout (poke): within FAIL_WINDOW bars a close is
                # back below the top, and closes never went more than
                # POKE_CLOSE_MAX above the top / POKE_MAX above the box's
                # original top (pokes do not stack). Wicks may reach POKE_MAX.
                # The box then continues with its top
                # raised to the highest close of the poke (wicks are ignored).
                back, hi_max, cl_max = None, top, top
                for u in range(t, min(n, t + FAIL_WINDOW + 1)):
                    # a close back below the top only fails the breakout when it is clearly
                    # back inside the box; a close within RETEST_BAND of the top is a retest
                    if u > t and c[u] < top * (1 - RETEST_BAND):
                        back = u
                        break
                    hi_max = max(hi_max, float(h[u]))
                    cl_max = max(cl_max, float(c[u]))
                cap = top_hist[0][1] * (1 + POKE_MAX)  # pokes never lift the top past this
                if back is None or cl_max > cap or cl_max > top * (1 + POKE_CLOSE_MAX):
                    breakout = t
                    break
                top = max(top, cl_max)
                top_hist.append((t, top))
                skip_until = back
                if (top - low) / top > MAX_DEPTH + 1e-10:
                    overdepth = t
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
        # The breakout / depth-exceed bar is not part of the base.
        end_idx = breakout if breakout is not None else overdepth
        last = end_idx - 1 if end_idx is not None else n - 1
        base_length_candles = last - start + 1  # trading bars from the first touch to the last base bar
        boxes.append(dict(i=i, start=start, j=j, last=last, top0=float(h[i]), top_hist=top_hist, base_length_candles=base_length_candles, top=top, bot=low, low_idx=low_idx,
                          bo=breakout, overdepth=overdepth, state=state,
                          cons=window, depth=(top-low)/top))

    # Only remove true duplicate pivot identities, never nearby independent boxes.
    kept = list({(b['i'], b['j']): b for b in boxes}.values())
    kept = prune(kept)
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
                'start_date': dates[b['start']],
                'swing_high_date': dates[b['i']],
                'base_length_candles': int(b['base_length_candles']),
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
            'start_date': dates[chosen['start']],
            'base_length_candles': int(chosen['base_length_candles']),
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
