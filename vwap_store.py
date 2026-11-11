"""
vwap_store.py — shared daily-VWAP history used by pipeline.py (scanner chunks) and
pipeline_chart.py (chart files).

R2 files (one per calendar year):
  vwap_hist_2026.json   {"updated": ..., "fields": ["vwap", "close"],
                         "days": {"YYYY-MM-DD": {SYMBOL: [raw_vwap, raw_close]}}}

Both numbers are RAW (the prices actually traded that day): NSE bhavcopy AVG_PRICE /
CLOSE_PRICE for the past (backfill_vwap.py), Upstox average_price / last price from now on
(pipeline_master.py). NSE stocks only for now.

The OHLC in the chunks / chart files is ADJUSTED by the data provider (splits, bonus,
rights, demergers). vwap_array() brings the raw VWAP onto the same scale per day:
    factor = adjusted close (our OHLC) / raw close (same day)
    a      = raw_vwap x factor
so no corporate-action list is needed, and a later re-adjustment of the history is picked
up automatically on the next run.
"""

from datetime import date

VWAP_HIST_PREFIX = "vwap_hist_"
VWAP_BAND_TOL    = 0.005    # adjusted VWAP must lie within [low, high] +/- 0.5%, else None


def year_file(year: int) -> str:
    return f"{VWAP_HIST_PREFIX}{year}.json"


async def load_vwap_store(getter, client, from_date: str, to_date: str) -> dict:
    """{date: {symbol: [raw_vwap, raw_close]}} for every year from_date..to_date.
    getter(client, name) -> parsed JSON or None (each pipeline passes its own R2 download).
    Missing years / errors give an empty part — callers then fall back to (H+L+C)/3."""
    store = {}
    for y in range(int(str(from_date)[:4]), int(str(to_date)[:4]) + 1):
        try:
            j = await getter(client, year_file(y))
        except Exception:
            j = None
        days = (j or {}).get("days") if isinstance(j, dict) else None
        if isinstance(days, dict):
            store.update(days)
    return store


def vwap_array(sym: str, s: dict, store: dict) -> list:
    """Adjusted daily VWAP aligned with s["d"] (None where unknown).
    s: stock object with parallel lists d, h, l, c (adjusted)."""
    d, h, l, c = s.get("d") or [], s.get("h") or [], s.get("l") or [], s.get("c") or []
    out = [None] * len(d)
    if not store:
        return out
    for i, day in enumerate(d):
        rec = (store.get(str(day)[:10]) or {}).get(sym)
        if not rec:
            continue
        try:
            raw_v, raw_c = float(rec[0]), float(rec[1])
            ci, hi, li = float(c[i]), float(h[i]), float(l[i])
        except (TypeError, ValueError, IndexError):
            continue
        if raw_v <= 0 or raw_c <= 0 or ci <= 0:
            continue
        a = raw_v * (ci / raw_c)
        # VWAP of a day always lies inside that day's range; outside = data mismatch
        if li * (1 - VWAP_BAND_TOL) <= a <= hi * (1 + VWAP_BAND_TOL):
            out[i] = round(a, 2)
    return out


def coverage(arrays: dict) -> tuple[int, int]:
    """(bars with VWAP, total bars) over {sym: [a...]} — for logs."""
    have = sum(sum(1 for x in a if x is not None) for a in arrays.values())
    total = sum(len(a) for a in arrays.values())
    return have, total


def years_back_floor(today: str, years: int) -> str:
    t = date.fromisoformat(today)
    try:
        return t.replace(year=t.year - years).isoformat()
    except ValueError:
        return t.replace(year=t.year - years, day=28).isoformat()
