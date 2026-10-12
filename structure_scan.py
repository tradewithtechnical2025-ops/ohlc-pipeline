"""
structure_scan.py — price structure from swing points, daily and weekly.

For every liquid stock, from its OHLC (all_data):

  HH-HL / LL-LH (trend structure)
      Swing high = a bar whose high is above the N bars before it and not below
      the N bars after it; swing low the same with lows. A swing is known only
      N bars after it forms.
      HH-HL : last swing high > the one before AND last swing low > the one before,
              while today's close is still at or above the last swing low.
      LL-LH : last swing low < the one before AND last swing high < the one before,
              while today's close is still at or below the last swing high.
      Equal levels do not count as higher / lower.

  Consolidation (from the same last two swing highs and lows)
      Contracting : lower high + higher low (triangle).
      Range       : both highs within 3% of each other and both lows within 3%.
      Zone = highest of the two highs to lowest of the two lows. Kept only when
      the zone is at most MAX_WIDTH wide and all four swings are recent.
      Status: inside / breakout (close above the zone) / breakdown (close below),
      plus how many bars ago the close left the zone.

                     daily      weekly
      pivot N        5 bars     3 bars
      max zone width 20%        35%
      max swing age  63 bars    26 bars
      break          on close   on close

Feed fields (weekly ones prefixed "w_"); a stock with nothing to report gets none:
  hhhl      "HH-HL" | "LL-LH"
  hhhl_sh   last swing high      hhhl_sl  last swing low
  hhhl_age  bars since the newer of those two swings was confirmed
  cons      "Contracting" | "Range"
  cons_st   "inside" | "breakout" | "breakdown"
  cons_top / cons_bot   zone levels      cons_w   zone width %
  cons_days bars since the oldest swing of the zone
  cons_brk  bars since the close left the zone (0 = today); null while inside

Wired into pipeline.py run_ep_scan; merged into screener_feed.json rows.
"""
from datetime import date

SETTINGS = {
    "d": {"n": 5, "max_width": 20.0, "max_age": 63},
    "w": {"n": 3, "max_width": 35.0, "max_age": 26},
}
FLAT_PCT = 3.0


def swings(highs, lows, n):
    sh, sl = [], []
    m = len(highs)
    for i in range(n, m - n):
        h, l = highs[i], lows[i]
        if h is None or l is None:
            continue
        lh = [x for x in highs[i - n:i] if x is not None]
        rh = [x for x in highs[i + 1:i + n + 1] if x is not None]
        if lh and rh and h > max(lh) and h >= max(rh):
            sh.append(i)
        ll = [x for x in lows[i - n:i] if x is not None]
        rl = [x for x in lows[i + 1:i + n + 1] if x is not None]
        if ll and rl and l < min(ll) and l <= min(rl):
            sl.append(i)
    return sh, sl


def _r(v):
    return None if v is None else round(v, 2)


def analyse(highs, lows, closes, n, max_width, max_age, flat=FLAT_PCT):
    """Both results for one series; {} when there is not enough history."""
    m = len(closes)
    if m < 4 * n + 2 or closes[-1] is None:
        return {}
    sh, sl = swings(highs, lows, n)
    if len(sh) < 2 or len(sl) < 2:
        return {}
    h1, h0, l1, l0 = sh[-1], sh[-2], sl[-1], sl[-2]
    H1, H0, L1, L0 = highs[h1], highs[h0], lows[l1], lows[l0]
    c = closes[-1]
    out = {}

    up = H1 > H0 and L1 > L0 and c >= L1
    dn = L1 < L0 and H1 < H0 and c <= H1
    if up or dn:
        out.update({"hhhl": "HH-HL" if up else "LL-LH", "hhhl_sh": _r(H1), "hhhl_sl": _r(L1),
                    "hhhl_age": (m - 1) - (max(h1, l1) + n)})

    contracting = H1 < H0 and L1 > L0
    rng = abs(H1 - H0) / H0 * 100 <= flat and abs(L1 - L0) / L0 * 100 <= flat
    oldest = min(h0, l0)
    if (contracting or rng) and (m - 1) - oldest <= max_age:
        top, bot = max(H1, H0), min(L1, L0)
        width = (top - bot) / bot * 100 if bot > 0 else None
        if width is not None and width <= max_width:
            st = "breakout" if c > top else "breakdown" if c < bot else "inside"
            brk = None
            if st != "inside":
                start = max(h1, l1) + 1
                for i in range(m - 1, start - 1, -1):
                    ci = closes[i]
                    if ci is None or not ((ci > top) if st == "breakout" else (ci < bot)):
                        break
                    brk = (m - 1) - i
            out.update({"cons": "Contracting" if contracting else "Range", "cons_st": st,
                        "cons_top": _r(top), "cons_bot": _r(bot), "cons_w": round(width, 1),
                        "cons_days": (m - 1) - oldest, "cons_brk": brk})
    return out


def to_weekly(d, h, l, c):
    """ISO-week bars (the current, unfinished week included, like the other weekly scans)."""
    wk, order = {}, []
    for di, hi, li, ci in zip(d, h, l, c):
        if None in (hi, li, ci) or not di:
            continue
        k = date.fromisoformat(di).isocalendar()[:2]
        if k not in wk:
            wk[k] = [hi, li, ci]
            order.append(k)
        else:
            w = wk[k]
            w[0] = max(w[0], hi); w[1] = min(w[1], li); w[2] = ci
    rows = [wk[k] for k in order]
    return [r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows]


def build_structure_map(all_data, is_liquid=None):
    """{symbol: feed fields} for every stock with something to report.
    is_liquid(volumes, closes, n) -> bool skips thin stocks (pipeline's _check_liquidity)."""
    out = {}
    for sym, s in (all_data or {}).items():
        try:
            d, h, l, c, v = s.get("d") or [], s.get("h") or [], s.get("l") or [], s.get("c") or [], s.get("v") or []
            n = len(c)
            if n < 60 or not (len(d) == len(h) == len(l) == n):
                continue
            if is_liquid and not is_liquid(v, c, n):
                continue
            row = analyse(h, l, c, **SETTINGS["d"])
            wh, wl, wc = to_weekly(d, h, l, c)
            row.update({"w_" + k: val for k, val in analyse(wh, wl, wc, **SETTINGS["w"]).items()})
            if row:
                out[sym] = row
        except Exception:
            continue
    return out
