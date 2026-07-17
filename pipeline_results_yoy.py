#!/usr/bin/env python3
"""
Results YoY Pipeline — STANDALONE (R2-only, no Finedge calls)
============================================================
Reads already-fetched per-symbol fundamentals from R2
(fundamentals_full/{SYMBOL}.json, written by pipeline_fundamentals_prod.py)
and computes, for the last 8 quarters, YoY (year-over-year, i.e. same
quarter last year) % change for Sales and EPS.

Does NOT call Finedge at all — this is a pure derivation step over data
that's already sitting in R2. Same pattern as pipeline_fundamentals_prod.py's
`backfill_summary` mode: universe + sector data from classification.json,
per-symbol data from fundamentals_full/{SYM}.json, all via R2 reads only.

Run on-demand, standalone:
  python pipeline_results_yoy.py

Why 8 quarters exactly:
  pipeline_fundamentals_prod.py stores up to QUARTERLY_CAP=12 quarters of
  core PL rows per stock (most-recent-first). YoY needs comparing quarter[i]
  against quarter[i+4] (same quarter, prior year) — with 12 stored quarters,
  that's computable for i = 0..7, i.e. exactly the last 8 quarters.

stype (consolidated vs standalone) selection:
  Mirrors pipeline_fundamentals_prod.py's _build_summary_entry: ONE overall
  stype is picked per stock from quarterly PL recency (whichever of c/s has
  the more recent latest row), then used consistently for every quarter in
  that stock's output — not picked independently per quarter.

Output — results_yoy.json in R2:
  {
    "updated": "YYYY-MM-DD",
    "stocks": {
      "SYMBOL": {
        "name": "...",
        "sector_group": "...",       # from classification.json (canonical taxonomy)
        "display_industry": "...",
        "stype": "c" | "s",
        "quarters": [                 # most-recent-first, up to 8 entries
          {
            "header": "Jun 2026",
            "sales": 12345.6,
            "sales_yoy_pct": 8.42,    # null if prior-year quarter or sales missing
            "eps": 12.3,
            "eps_yoy_pct": -2.1       # null if prior-year quarter or eps missing
          },
          ...
        ]
      },
      ...
    }
  }
"""

import asyncio, json, logging, sys
from datetime import datetime
from urllib.parse import quote
import os
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

BATCH        = 50    # R2 reads per checkpoint, mirrors backfill_summary's pattern
QUARTERS_OUT = 8      # how many YoY quarters to emit per stock

ETF_ENDSWITH = ("ETF", "BEES", "LIQUID", "GILT", "IETF", "MMQS", "TOTAL")
ETF_CONTAINS = ("NIFTY", "BANKEX", "MSCIN")

OUTPUT_FILE = "results_yoy.json"


def _is_etf(sym):
    s = sym.upper()
    return any(s.endswith(k) for k in ETF_ENDSWITH) or any(k in s for k in ETF_CONTAINS)


def today_ist() -> str:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════
# R2 HELPERS
# ══════════════════════════════════════════════════════════════

async def r2_download(client, filename):
    url = f"{WORKER_URL}/{quote(filename, safe='/')}"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        log.warning(f"  ⚠️  Download failed {filename}: HTTP {r.status_code}")
        return None
    try:
        return r.json()
    except Exception:
        return None


async def r2_upload(client, filename, data):
    if isinstance(data, str):
        data = data.encode()
    r = await client.post(WORKER_URL, params={"file": filename},
                           headers={**WORKER_HEADERS, "Content-Type": "application/json"}, content=data, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")


# ══════════════════════════════════════════════════════════════
# UNIVERSE + SECTOR LOOKUP (from classification.json)
# ══════════════════════════════════════════════════════════════

async def get_nse_universe_and_lookup(client):
    classification = await r2_download(client, "classification.json")
    if not classification or not isinstance(classification, list):
        raise RuntimeError("classification.json missing or invalid in R2!")

    symbols = []
    lookup = {}
    for s in classification:
        sym = str(s.get("symbol", "")).strip().upper()
        exch = str(s.get("exchange", "")).strip()
        if not sym or exch != "NSE" or _is_etf(sym):
            continue
        symbols.append(sym)
        lookup[sym] = {
            "name": s.get("name"),
            "sector_group": s.get("sector_group"),
            "display_industry": s.get("display_industry"),
        }
    symbols = sorted(set(symbols))
    log.info(f"Universe: {len(symbols)} NSE equity symbols (ETFs excluded)")
    return symbols, lookup


# ══════════════════════════════════════════════════════════════
# YoY COMPUTATION
# ══════════════════════════════════════════════════════════════

def _latest_key(rows):
    """Same recency key as pipeline_fundamentals_prod.py's _latest_key —
    used to decide whether consolidated or standalone data is more current
    for a given stock, rather than just preferring consolidated whenever it
    has any rows at all."""
    if not rows:
        return -1
    r0 = rows[0]
    pe = r0.get("period_end")
    if pe:
        try:
            return int(pe)
        except (TypeError, ValueError):
            pass
    yr = r0.get("year")
    if yr:
        try:
            return int(yr) * 10000
        except (TypeError, ValueError):
            pass
    return -1


_MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _fmt_period_end(period_end):
    if not period_end:
        return ""
    s = str(int(period_end))
    if len(s) == 8:
        m = int(s[4:6])
        return f"{_MONTHS[m]} {s[:4]}" if 1 <= m <= 12 else s
    return str(period_end)


def _pct_change(curr, prior):
    """YoY % change. None if either value is missing or prior is zero
    (division-by-zero / meaningless-base-year guard)."""
    if curr is None or prior is None or prior == 0:
        return None
    return round((curr - prior) / abs(prior) * 100, 2)


def _row_sales(row):
    # Banks report interest_earned instead of sales — same fallback used
    # elsewhere in the fundamentals pipeline (_build_summary_entry).
    return row.get("sales") if row.get("sales") is not None else row.get("interest_earned")


def compute_yoy_quarters(core_rows):
    """core_rows: quarterly PL core list, most-recent-first (as stored by
    pipeline_fundamentals_prod.py, capped at 12 quarters).
    Returns up to QUARTERS_OUT entries, most-recent-first, each with YoY
    % change for sales and eps against the same quarter one year prior
    (index i vs index i+4)."""
    out = []
    n = len(core_rows)
    for i in range(min(QUARTERS_OUT, max(0, n - 4))):
        cur, prior = core_rows[i], core_rows[i + 4]
        sales_cur, sales_prior = _row_sales(cur), _row_sales(prior)
        out.append({
            "header":        _fmt_period_end(cur.get("period_end")),
            "sales":         sales_cur,
            "sales_yoy_pct": _pct_change(sales_cur, sales_prior),
            "eps":           cur.get("eps"),
            "eps_yoy_pct":   _pct_change(cur.get("eps"), prior.get("eps")),
        })
    return out


def build_entry(sym, obj, lookup_info):
    pl = (obj or {}).get("pl", {})
    c_core = (pl.get("quarterly", {}).get("c", {}) or {}).get("core") or []
    s_core = (pl.get("quarterly", {}).get("s", {}) or {}).get("core") or []

    # Single consistent stype per stock — same rule as _build_summary_entry
    # in pipeline_fundamentals_prod.py.
    stype = "s" if _latest_key(s_core) > _latest_key(c_core) else "c"
    if stype == "c" and not c_core and s_core:
        stype = "s"

    core = s_core if stype == "s" else c_core
    quarters = compute_yoy_quarters(core)

    return {
        "name":             lookup_info.get("name"),
        "sector_group":     lookup_info.get("sector_group"),
        "display_industry": lookup_info.get("display_industry"),
        "stype":            stype,
        "quarters":         quarters,
    }


# ══════════════════════════════════════════════════════════════
# MAIN — batch through universe, R2-only
# ══════════════════════════════════════════════════════════════

async def _process_one(client, sym, lookup):
    obj = await r2_download(client, f"fundamentals_full/{sym}.json")
    if not obj:
        return sym, None
    lookup_info = lookup.get(sym, {})
    return sym, build_entry(sym, obj, lookup_info)


async def main():
    async with httpx.AsyncClient() as client:
        symbols, lookup = await get_nse_universe_and_lookup(client)

        results = {}
        ok = failed = 0
        failed_syms = []

        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i + BATCH]
            batch_results = await asyncio.gather(*[_process_one(client, sym, lookup) for sym in batch])
            for sym, entry in batch_results:
                if entry:
                    results[sym] = entry
                    ok += 1
                else:
                    failed += 1
                    failed_syms.append(sym)
            done = min(i + BATCH, len(symbols))
            log.info(f"  {done}/{len(symbols)}  ✓{ok}  ✗{failed}")

        if failed_syms:
            log.warning(f"  {len(failed_syms)} symbols had no fundamentals_full file "
                        f"(run pipeline_fundamentals_prod.py full for these first): "
                        f"{', '.join(failed_syms[:20])}{' ...' if len(failed_syms) > 20 else ''}")

        payload = json.dumps({"updated": today_ist(), "stocks": results}, separators=(",", ":"))
        await r2_upload(client, OUTPUT_FILE, payload)

    log.info(f"━━━ Results YoY complete — ✓{ok}  ✗{failed}  → {OUTPUT_FILE} ━━━")


if __name__ == "__main__":
    asyncio.run(main())
