"""
structure_history.py — how many index stocks are in an HH-HL / LL-LH swing
structure, day by day (daily and weekly), for the dashboard's Market Structure
widget.

Same rules as structure_scan.analyse (so today's numbers match the screener's
HH-HL / LL-LH): last two confirmed swing highs and lows, 1% tolerance, close
still on the right side of the last swing. A swing at bar i is confirmed once
n bars after it exist, so the history needs no look-ahead: each past day sees
only the bars up to that day. Weekly bars include the unfinished current week
(as of that day), like the screener.

Output — index_structure_history.json:
  { updated, dates:[...],
    indices: { KEY: { name, constituents, matched,
                      d: { hh:[count…], ll:[count…], n:[stocks with data…] },
                      w: { hh:[…], ll:[…], n:[…] } } } }

Stocks are the index's current constituents; thin stocks are skipped with the
same liquidity check the screener uses (when one is passed in).
"""
from datetime import date

from structure_scan import SETTINGS, _cmp, swings
from breadth_returns import _calendar, _norm

STRUCTURE_INDICES = {
    "NIFTY50": "Nifty 50",
    "NIFMID400": "Nifty MidSmallcap 400",
    "NIF500": "Nifty 500",
}
STRUCTURE_DAYS = 252


def _is_swing(highs, lows, i, n):
    """(is swing high, is swing low) at bar i — swing_scan.swings' rule for one bar."""
    h, l = highs[i], lows[i]
    if h is None or l is None or i < n or i + n >= len(highs):
        return False, False
    lh = [x for x in highs[i - n:i] if x is not None]
    rh = [x for x in highs[i + 1:i + n + 1] if x is not None]
    ll = [x for x in lows[i - n:i] if x is not None]
    rl = [x for x in lows[i + 1:i + n + 1] if x is not None]
    return (bool(lh and rh and h > max(lh) and h >= max(rh)),
            bool(ll and rl and l < min(ll) and l <= min(rl)))


def _state(highs, lows, c, sh, sl, n):
    """'hh' / 'll' / None from the last two swing highs and lows (analyse's rule)."""
    if len(sh) < 2 or len(sl) < 2 or c is None:
        return None
    H1, H0, L1, L0 = highs[sh[-1]], highs[sh[-2]], lows[sl[-1]], lows[sl[-2]]
    ch, cl = _cmp(H1, H0), _cmp(L1, L0)
    if ch == 1 and cl == 1 and c >= L1:
        return "hh"
    if ch == -1 and cl == -1 and c <= H1:
        return "ll"
    return None


def _daily_series(h, l, c, upto, n):
    """state for each bar index t in `upto` (sorted), using bars 0..t only."""
    sh_all, sl_all = swings(h, l, n)
    out, a, b = {}, 0, 0
    sh, sl = [], []
    for t in upto:
        while a < len(sh_all) and sh_all[a] + n <= t:
            sh.append(sh_all[a]); a += 1
        while b < len(sl_all) and sl_all[b] + n <= t:
            sl.append(sl_all[b]); b += 1
        m = t + 1
        if m < 4 * n + 2 or c[t] is None:
            out[t] = None
            continue
        out[t] = _state(h, l, c[t], sh, sl, n)
    return out


def _week_bars(d, h, l, c):
    """Completed ISO-week bars plus, for every daily bar, which week it is in."""
    keys, wh, wl, wc, wk_of = [], [], [], [], []
    for di, hi, li, ci in zip(d, h, l, c):
        if None in (hi, li, ci) or not di:
            wk_of.append(None)
            continue
        k = date.fromisoformat(str(di)[:10]).isocalendar()[:2]
        if not keys or keys[-1] != k:
            keys.append(k); wh.append(hi); wl.append(li); wc.append(ci)
        else:
            wh[-1] = max(wh[-1], hi); wl[-1] = min(wl[-1], li); wc[-1] = ci
        wk_of.append(len(keys) - 1)
    return wh, wl, wc, wk_of


def _weekly_series(d, h, l, c, upto, n):
    """Weekly state as of each daily bar t: completed weeks + the partial week to t."""
    wh, wl, wc, wk_of = _week_bars(d, h, l, c)
    sh_full, sl_full = swings(wh, wl, n)   # valid for swings whose right window is all complete weeks
    out = {}
    # running partial bar of the current week
    cur_k, ph, pl, pc = None, None, None, None
    ti = 0
    upto_set = set(upto)
    for t in range(len(c)):
        k = wk_of[t]
        if k is None:
            if t in upto_set:
                out[t] = None
            continue
        if k != cur_k:
            cur_k, ph, pl, pc = k, h[t], l[t], c[t]
        else:
            ph = max(ph, h[t]); pl = min(pl, l[t]); pc = c[t]
        if t not in upto_set:
            continue
        # weeks 0..k-1 complete, week k partial (ph, pl, pc)
        m = k + 1
        if m < 4 * n + 2:
            out[t] = None
            continue
        sh = [i for i in sh_full if i + n <= k - 1]
        sl = [i for i in sl_full if i + n <= k - 1]
        i = k - n                          # its right window ends in the partial week
        if i >= n:
            hh = wh[:k] + [ph]; ll = wl[:k] + [pl]
            is_h, is_l = _is_swing(hh, ll, i, n)
            if is_h: sh.append(i)
            if is_l: sl.append(i)
            out[t] = _state(hh, ll, pc, sh, sl, n)
        else:
            out[t] = None
    return out


def build_index_structure_history(all_data, index_master, is_liquid=None,
                                  indices=STRUCTURE_INDICES, days=STRUCTURE_DAYS):
    dates = _calendar(all_data, days)
    pos = {dd: i for i, dd in enumerate(dates)}
    by_norm = {_norm(s): s for s in all_data}

    members, meta = {}, {}
    for key, name in indices.items():
        entry = (index_master or {}).get(key) or {}
        raw = entry.get("constituents") or []
        syms = []
        for x in raw:
            x = x if isinstance(x, str) else (x or {}).get("symbol") or ""
            x = str(x).strip().upper()
            s = x if x in all_data else by_norm.get(_norm(x))
            if s:
                syms.append(s)
        members[key] = sorted(set(syms))
        meta[key] = {"name": entry.get("name") or name, "constituents": len(raw), "matched": len(members[key])}

    if not any(members.values()):
        raise ValueError("no index constituents matched (index_master.json)")

    nd, nw = SETTINGS["d"]["n"], SETTINGS["w"]["n"]
    per = {}   # sym -> (daily states on calendar, weekly states on calendar)
    for sym in set(s for v in members.values() for s in v):
        try:
            s = all_data[sym]
            d, h, l, c, v = s.get("d") or [], s.get("h") or [], s.get("l") or [], s.get("c") or [], s.get("v") or []
            if len(c) < 60 or not (len(d) == len(h) == len(l) == len(c)):
                continue
            if is_liquid and not is_liquid(v, c, len(c)):
                continue
            idx = [i for i in range(len(d)) if str(d[i])[:10] in pos or d[i] in pos]
            ds, ws = _daily_series(h, l, c, idx, nd), _weekly_series(d, h, l, c, idx, nw)
            dcal, wcal = [False] * len(dates), [False] * len(dates)   # False = no data that day
            for i in idx:
                j = pos.get(d[i], pos.get(str(d[i])[:10]))
                dcal[j], wcal[j] = ds.get(i), ws.get(i)
            per[sym] = (dcal, wcal)
        except Exception:
            continue

    out = {}
    for key, syms in members.items():
        e = dict(meta[key])
        for tf, slot in (("d", 0), ("w", 1)):
            hh, ll, nn = [], [], []
            for j in range(len(dates)):
                a = b = k = 0
                for sym in syms:
                    p = per.get(sym)
                    if not p or p[slot][j] is False:
                        continue
                    k += 1
                    st = p[slot][j]
                    if st == "hh": a += 1
                    elif st == "ll": b += 1
                hh.append(a); ll.append(b); nn.append(k)
            e[tf] = {"hh": hh, "ll": ll, "n": nn}
        out[key] = e

    return {"updated": date.today().isoformat(), "dates": dates, "indices": out}
