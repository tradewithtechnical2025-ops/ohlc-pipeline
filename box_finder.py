#!/usr/bin/env python3
"""TwT Box Finder: independent box lifecycle and one screener row per stock.

Source: validated V7 detector plus V8 fresh-breakout selection.
Uses existing R2 OHLC chunks; no separate market data API.
"""
import numpy as np

def cons(c,top):
 for w in range(min(15,len(c)),6,-1):
  x=c[-w:];near=np.mean((x>=top*.95)&(x<=top*1.003));last=np.mean((x[-5:]>=top*.95)&(x[-5:]<=top*1.003))
  if 0<=(top-c[-1])/top<=.05 and (max(x)-min(x))/top<=.05 and near>=.7 and last>=.8 and (np.mean(x[-3:])-np.mean(x[:3]))/top>=-.015:return w
 return 0
def detect(a):
 h=np.array(a['h'],float);l=np.array(a['l'],float);c=np.array(a['c'],float);n=len(c);begin=max(4,n-160)
 sh=[i for i in range(begin,n-4) if h[i]>max(h[i-4:i]) and h[i]>max(h[i+1:i+5])]
 sl=[j for j in range(begin,n-4) if l[j]<min(l[j-4:j]) and l[j]<min(l[j+1:j+5])]
 cand=[]
 for i in sh:
  js=[j for j in sl if j>i and j+4<n]
  if not js:continue
  j=js[0];top=h[i];bot=l[j];depth=(top-bot)/top
  if not .055<=depth<=.30 or n-j<5:continue
  # Only use a swing low if subsequent candles show recovery/holding rather than ongoing breakdown.
  after=c[j+4:];
  if len(after)<1 or np.mean(after[:min(10,len(after))]<top*1.003)<.6:continue
  if np.sum(after<bot*.975)>max(2,int(.12*len(after))):continue
  # Require meaningful sideways occupancy or a U-shaped recovery, rather than a fresh monotonic slide.
  recent=c[max(i,j-2):];slope=(np.mean(recent[-min(5,len(recent)):])-np.mean(recent[:min(5,len(recent))]))/top
  if slope<-.07:continue
  # Confirm breakout by closing above upper boundary; do not label every later candle as new BO.
  bo=next((t for t in range(j+4,n) if c[t]>top*1.0025),None)
  window=cons(c,top) if bo is None else 0
  dist=(top-c[-1])/top
  state='BO' if bo is not None else 'Consolidation Near BO' if window else 'Near BO' if 0<=dist<=.02 else 'Box Forming'
  cand.append(dict(i=i,j=j,top=top,bot=bot,bo=bo,state=state,cons=window,depth=depth))
 # Merge near-identical price structures only, not every nested box. Favor the older well-established pivot when two candidates share nearly the same levels.
 kept=[]
 for z in sorted(cand,key=lambda x:x['i']):
  if any(abs(z['top']-q['top'])/max(z['top'],q['top'])<.05 and (abs(z['bot']-q['bot'])/max(z['bot'],q['bot'])<.08 or abs(z['i']-q['i'])<=12) for q in kept):continue
  # A higher wick near established resistance is a retest, not a new box,
  # unless price has actually closed above the established level.
  if any(q['bo'] is None and 0 <= (z['top']-q['top'])/q['top'] <= .05
         and z['bot'] > q['bot'] and np.max(c[q['i']+1:]) <= q['top']*1.0025
         for q in kept):continue
  kept.append(z)
 active=next((z for z in reversed(kept) if z['bo'] is None),None)
 return kept,active


def select_box(stock):
    """Return (all_retained_boxes, visible_box, visible_status).

    Fresh BO is shown on breakout day; on following sessions that box is
    closed and the next eligible unbroken box becomes visible.
    """
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
    """Build JSON-serializable screener payload and per-symbol box history."""
    results = []
    history = {}
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
                'upper': round(float(b['top']), 4),
                'lower': round(float(b['bot']), 4),
                'depth_pct': round(float(b['depth']) * 100, 2),
                'breakout_date': dates[b['bo']] if b['bo'] is not None else None,
                'consolidation_candles': int(b['cons']),
                'status': ('BO' if b['bo'] is not None else b['state']),
            }
        history[symbol] = [encode(b) for b in boxes]
        if chosen is None:
            continue
        last_close = float(stock['c'][-1])
        results.append({
            'symbol': symbol,
            'date': dates[-1],
            'status': status,
            'start_date': dates[chosen['i']],
            'upper': round(float(chosen['top']), 4),
            'lower': round(float(chosen['bot']), 4),
            'close': round(last_close, 4),
            'distance_pct': round(100 * (float(chosen['top']) - last_close) / float(chosen['top']), 2),
            'consolidation_candles': int(chosen['cons']),
            'retained_boxes': len(boxes),
        })
    return {
        'updated': asof or max((v['d'][-1] for v in all_data.values() if v.get('d')), default=None),
        'total': len(results),
        'stocks': results,
        'box_history': history,
    }
