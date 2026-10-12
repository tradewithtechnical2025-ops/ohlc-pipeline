"""
breadth_returns.py — two daily outputs built from the OHLC pipeline.py already
has in memory (all_data), plus the index list and classification:

  index_ma_history.json
      For each broad index (Nifty 50 / 500 / Midcap 150 / Smallcap 250 /
      MidSmallcap 400): % of its stocks closing above EMA and SMA 10, 21, 50
      and 200, every trading day for the last year.

  sector_returns_history.json / industry_returns_history.json
      For each screener sector (classification sector_group) and industry
      (display_industry): the 1D / 1W / 1M / 6M / 1Y return of its stocks,
      every trading day for the last year, three ways —
        median  : the middle stock's return (one wild stock can't skew it)
        equal   : plain average of the stocks' returns
        mcap    : market-cap weighted, like an index (weights at the start
                  of each window: today's market cap × close then ÷ close now)

Wired into pipeline.py run_daily next to the sector MA history:

    from breadth_returns import build_index_ma_history, build_group_returns_history
    index_master = await r2_download(client, "index_master.json") or {}
    index_ma_history = build_index_ma_history(all_data, index_master)
    sector_returns = build_group_returns_history(all_data, classification, "sector_group")
    industry_returns = build_group_returns_history(all_data, classification, "display_industry")

All files are columnar (one shared "dates" list, one array per series) to
stay small. Every list is oldest → newest; the last value is today.
"""

import math
from datetime import date

HISTORY_DAYS = 252                 # ~1 trading year
MA_PERIODS = (10, 21, 50, 200)
MA_TYPES = ("ema", "sma")

# key in index_master.json → display name. Keys are FinEdge's normalised
# index symbols (pipeline_indices.normalize_index_symbol).
BREADTH_INDICES = {
    "NIFTY50": "Nifty 50",
    "NIF500": "Nifty 500",
    "NIFMID150": "Nifty Midcap 150",
    "NIFSMA250": "Nifty Smallcap 250",
    "NIFMID400": "Nifty MidSmallcap 400",
}

# window lengths in trading days
RETURN_PERIODS = (("1D", 1), ("1W", 5), ("1M", 21), ("6M", 126), ("1Y", 252))
MIN_STOCKS = 3                     # a group needs this many stocks with data on a day


# ── moving averages (same maths as pipeline._calc_ema / _calc_sma) ────────

def _ema(closes, period):
    n = len(closes)
    out = [None] * n
    if n < period:
        return out
    seed = [v for v in closes[:period] if v is not None]
    if not seed:
        return out
    k = 2 / (period + 1)
    out[period - 1] = sum(seed) / len(seed)
    for i in range(period, n):
        c = closes[i]
        out[i] = c * k + out[i - 1] * (1 - k) if c is not None else out[i - 1]
    return out


def _sma(closes, period):
    n = len(closes)
    out = [None] * n
    run, bad = 0.0, 0
    for i in range(n):
        c = closes[i]
        if c is None:
            bad += 1
        else:
            run += c
        if i >= period:
            old = closes[i - period]
            if old is None:
                bad -= 1
            else:
                run -= old
        if i >= period - 1 and bad == 0:
            out[i] = run / period
    return out


def _norm(sym):
    return "".join(ch for ch in str(sym).upper() if ch.isalnum())


def _calendar(all_data, days):
    """The last `days` trading dates — taken from the stock with the longest
    history, so one suspended stock can't shorten the calendar."""
    if not all_data:
        return []
    sample = max(all_data.values(), key=lambda s: len(s.get("d") or []))
    return list((sample.get("d") or [])[-days:])


# ══════════════════════════════════════════════════════════════
# 1. Index breadth
# ══════════════════════════════════════════════════════════════

def build_index_ma_history(all_data, index_master, indices=BREADTH_INDICES, days=HISTORY_DAYS):
    dates = _calendar(all_data, days)
    pos = {d: i for i, d in enumerate(dates)}
    by_norm = {_norm(s): s for s in all_data}
    labels = [f"{t}{p}" for t in MA_TYPES for p in MA_PERIODS]

    members, meta = {}, {}
    for key, name in indices.items():
        entry = (index_master or {}).get(key) or {}
        raw = entry.get("constituents") or []
        syms = []
        for c in raw:
            c = str(c).strip().upper()
            s = c if c in all_data else by_norm.get(_norm(c))
            if s:
                syms.append(s)
        members[key] = sorted(set(syms))
        meta[key] = {"name": entry.get("name") or name, "constituents": len(raw), "matched": len(members[key])}

    # per-stock 0/1 flags on the calendar, computed once per stock
    flags = {}
    needed = set(s for v in members.values() for s in v)
    for sym in needed:
        s = all_data[sym]
        d, c = s.get("d") or [], s.get("c") or []
        if not d or len(d) != len(c):
            continue
        mas = {}
        for t in MA_TYPES:
            fn = _ema if t == "ema" else _sma
            for p in MA_PERIODS:
                mas[f"{t}{p}"] = fn(c, p)
        out = {lab: [None] * len(dates) for lab in labels}
        start = max(0, len(d) - days - 5)
        for i in range(start, len(d)):
            j = pos.get(d[i])
            if j is None or c[i] is None:
                continue
            for lab in labels:
                m = mas[lab][i]
                if m is not None:
                    out[lab][j] = 1 if c[i] > m else 0
        flags[sym] = out

    series = {}
    for key, syms in members.items():
        ser = {lab: [] for lab in labels}
        ser["stocks"] = []
        for j in range(len(dates)):
            counted = 0
            for lab in labels:
                valid = above = 0
                for sym in syms:
                    f = flags.get(sym)
                    if not f:
                        continue
                    v = f[lab][j]
                    if v is None:
                        continue
                    valid += 1
                    above += v
                ser[lab].append(round(above / valid * 100, 1) if valid >= 5 else None)
                if lab == "ema10":
                    counted = valid
            ser["stocks"].append(counted)
        series[key] = ser

    return {
        "updated": date.today().isoformat(),
        "dates": dates,
        "labels": labels,
        "indices": meta,
        "series": series,
    }


# ══════════════════════════════════════════════════════════════
# 2. Sector / industry returns
# ══════════════════════════════════════════════════════════════

def _median(xs):
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def _r1(v):
    return None if v is None or not math.isfinite(v) else round(v, 1)


def build_group_returns_history(all_data, classification, field, days=HISTORY_DAYS, periods=RETURN_PERIODS):
    dates = _calendar(all_data, days)
    pos = {d: i for i, d in enumerate(dates)}
    groups, mcap_now = {}, {}
    for row in classification or []:
        if not isinstance(row, dict):
            continue
        sym, g = row.get("symbol"), row.get(field)
        if not sym or not g or sym not in all_data:
            continue
        groups.setdefault(str(g).strip(), []).append(sym)
        try:
            m = float(row.get("market_cap_cr") or 0)
        except (TypeError, ValueError):
            m = 0
        if m > 0:
            mcap_now[sym] = m

    # per stock, per calendar day: (return %, weight) for each period
    per = {}
    for syms in groups.values():
        for sym in syms:
            if sym in per:
                continue
            s = all_data[sym]
            d, c = s.get("d") or [], s.get("c") or []
            if not d or len(d) != len(c):
                continue
            last = next((v for v in reversed(c) if v), None)
            m_now = mcap_now.get(sym)
            out = {p: [None] * len(dates) for p, _ in periods}
            start = max(0, len(d) - days - 5)
            for i in range(start, len(d)):
                j = pos.get(d[i])
                if j is None or not c[i]:
                    continue
                for p, w in periods:
                    k = i - w
                    if k < 0 or not c[k] or c[k] <= 0:
                        continue
                    r = (c[i] / c[k] - 1) * 100
                    wt = m_now * c[k] / last if (m_now and last) else None
                    out[p][j] = (r, wt)
            per[sym] = out

    result = {}
    for g, syms in groups.items():
        entry = {"stocks": len(syms)}
        for p, _ in periods:
            med, eq, mc = [], [], []
            for j in range(len(dates)):
                rs, ws = [], []
                for sym in syms:
                    v = per.get(sym, {}).get(p)
                    if not v or v[j] is None:
                        continue
                    rs.append(v[j][0])
                    if v[j][1]:
                        ws.append(v[j])
                if len(rs) < MIN_STOCKS:
                    med.append(None); eq.append(None); mc.append(None)
                    continue
                med.append(_r1(_median(rs)))
                eq.append(_r1(sum(rs) / len(rs)))
                tw = sum(w for _, w in ws)
                mc.append(_r1(sum(r * w for r, w in ws) / tw) if tw > 0 and len(ws) >= MIN_STOCKS else None)
            entry[p] = {"median": med, "equal": eq, "mcap": mc}
        result[g] = entry

    return {
        "updated": date.today().isoformat(),
        "field": field,
        "dates": dates,
        "periods": [p for p, _ in periods],
        "methods": ["median", "equal", "mcap"],
        "groups": result,
    }
