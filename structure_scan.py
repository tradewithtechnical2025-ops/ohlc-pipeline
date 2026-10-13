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
      Swings within 1% of each other count as equal, not higher / lower.

  Consolidation (from the same last two swing highs and lows)
      Any zone whose swings are not a clear HH-HL / LL-LH. Its shape is kept:
      Contracting : the range narrows — lower high and/or higher low, neither side widening.
      Range       : both highs within 3% of each other and both lows within 3%.
      Sideways    : anything else (e.g. a flat top with a slightly lower low).
      Zone = highest of the two highs to lowest of the two lows. Kept only when
      the zone is at most MAX_WIDTH wide and all four swings are recent.
      Status: inside / breakout (close above the zone) / breakdown (close below),
      plus how many bars ago the close left the zone.
      Trend before the zone: the zone began at a new high (its first swing
      high more than 1% above the one before) with the close above the 50 EMA
      (weekly 30 EMA) = up; began at a new low with the close below it = down.

                     daily      weekly
      pivot N        5 bars     3 bars
      max zone width 20%        35%
      max swing age  63 bars    26 bars
      break          on close   on close

Feed fields (weekly ones prefixed "w_"); a stock with nothing to report gets none:
  hhhl      "HH-HL" | "LL-LH"
  hhhl_sh   last swing high      hhhl_sl  last swing low
  hhhl_age  bars since the newer of those two swings was confirmed
  cons      "Contracting" | "Range" | "Sideways"
  cons_st   "inside" | "breakout" | "breakdown"
  cons_top / cons_bot   zone levels      cons_w   zone width %
  cons_days bars since the oldest swing of the zone
  cons_brk  bars since the close left the zone (0 = today); null while inside
  cons_trend "up" | "down" | null — the zone began at a new high / new low
            (with the close on the matching side of the EMA)

Summary fields the website filters on (two rows of chips):
  st_trend  "up" | "down"        (needs close above / below the 50 EMA; weekly 30 EMA)
  st_stage  "trending" | "consolidating" | "breakout" | "breakdown"
  A breakout / breakdown stays the stage for 5 sessions (weekly: 1 week).

Wired into pipeline.py run_ep_scan; merged into screener_feed.json rows.
"""
from datetime import date

SETTINGS = {
    "d": {"n": 5, "max_width": 20.0, "max_age": 63, "ema": 50},
    "w": {"n": 3, "max_width": 35.0, "max_age": 26, "ema": 30},
}
# Trend / stage summary (the two rows of filter chips on the website)
EMA_GUARD = {"d": 50, "w": 30}     # up-trend needs close above this EMA, down-trend below
BREAK_LIVE = {"d": 5, "w": 1}      # a breakout / breakdown stays the stage for this many bars
FLAT_PCT = 3.0
TOL_PCT = 1.0        # swings closer than this count as equal (not higher / lower)


def _cmp(a, b, tol=TOL_PCT):
    """+1 when a is more than tol % above b, -1 when more than tol % below, else 0."""
    p = (a - b) / b * 100
    return 1 if p > tol else -1 if p < -tol else 0


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


def _ema_last(closes, period):
    vals = [c for c in closes if c is not None]
    if len(vals) < period:
        return None
    k = 2 / (period + 1)
    e = sum(vals[:period]) / period
    for v in vals[period:]:
        e = v * k + e * (1 - k)
    return e


def _r(v):
    return None if v is None else round(v, 2)


def analyse(highs, lows, closes, n, max_width, max_age, flat=FLAT_PCT, ema=None):
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

    ch, cl = _cmp(H1, H0), _cmp(L1, L0)
    up = ch == 1 and cl == 1 and c >= L1
    dn = ch == -1 and cl == -1 and c <= H1
    if up or dn:
        out.update({"hhhl": "HH-HL" if up else "LL-LH", "hhhl_sh": _r(H1), "hhhl_sl": _r(L1),
                    "hhhl_age": (m - 1) - (max(h1, l1) + n)})

    # Consolidation: any zone that is not a clear swing trend (HH-HL or LL-LH),
    # or whose highs and lows are both flat (a box wins over a tiny HH-HL),
    # as long as it is narrow and recent. The shape is kept for reference:
    # contracting = the range narrows (lower high and/or higher low, neither
    # side widening), range = both sides flat within FLAT_PCT, else sideways.
    trend_swings = (ch == 1 and cl == 1) or (ch == -1 and cl == -1)
    contracting = (ch == -1 and cl >= 0) or (ch <= 0 and cl == 1)
    rng = abs(H1 - H0) / H0 * 100 <= flat and abs(L1 - L0) / L0 * 100 <= flat
    shape = "Contracting" if contracting else "Range" if rng else "Sideways"
    oldest = min(h0, l0)
    if (rng or not trend_swings) and (m - 1) - oldest <= max_age:
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
            # trend before the zone: the zone began at a new high (its first swing
            # high clearly above the one before) with the close above the EMA = up;
            # began at a new low with the close below the EMA = down
            trend = None
            if len(sh) >= 3 and len(sl) >= 3 and ema:
                e = _ema_last(closes, ema)
                if e is not None:
                    if _cmp(H0, highs[sh[-3]]) == 1 and c > e:
                        trend = "up"
                    elif _cmp(L0, lows[sl[-3]]) == -1 and c < e:
                        trend = "down"
            out.update({"cons": shape, "cons_st": st,
                        "cons_top": _r(top), "cons_bot": _r(bot), "cons_w": round(width, 1),
                        "cons_days": (m - 1) - oldest, "cons_brk": brk, "cons_trend": trend})
    return out


def summarise(row, closes, tf):
    """Collapse the detail fields into the two values the website filters on.
    stage: consolidating / breakout / breakdown while a zone is live (inside it,
           or the close left it within BREAK_LIVE bars); otherwise trending when
           the swings are HH-HL or LL-LH; otherwise none.
    trend: the zone's cons_trend (began at a new high / low) while a zone is live, else HH-HL = up,
           LL-LH = down; kept only when the close agrees with the EMA guard
           (up above it, down below it), so small swings inside a big move
           don't flip the trend. A "trending" stage whose trend fails the guard
           is dropped too."""
    p = "w_" if tf == "w" else ""
    trend = stage = None
    cons, st, brk = row.get(p + "cons"), row.get(p + "cons_st"), row.get(p + "cons_brk")
    if cons and (st == "inside" or (brk is not None and brk <= BREAK_LIVE[tf])):
        stage = {"inside": "consolidating", "breakout": "breakout", "breakdown": "breakdown"}[st]
        trend = row.get(p + "cons_trend")
    elif row.get(p + "hhhl"):
        stage = "trending"
        trend = "up" if row[p + "hhhl"] == "HH-HL" else "down"
    if trend:
        ema, c = _ema_last(closes, EMA_GUARD[tf]), closes[-1]
        if ema is None or c is None or (trend == "up" and c <= ema) or (trend == "down" and c >= ema):
            trend = None
            if stage == "trending":   # a swing trend against the EMA is no clear trend
                stage = None
    out = {}
    if trend: out[p + "st_trend"] = trend
    if stage: out[p + "st_stage"] = stage
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
            row.update(summarise(row, c, "d"))
            wh, wl, wc = to_weekly(d, h, l, c)
            wrow = {"w_" + k: val for k, val in analyse(wh, wl, wc, **SETTINGS["w"]).items()}
            wrow.update(summarise(wrow, wc, "w"))
            row.update(wrow)
            if row:
                out[sym] = row
        except Exception:
            continue
    return out
