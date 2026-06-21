#!/usr/bin/env python3
"""
Finedge FULL Fundamentals — TEST pipeline (standalone)
========================================================
Purana pipeline.py ko BILKUL touch nahi karta, na R2 ke fundamentals.json
ko. Yeh sirf locally JSON save karta hai taaki naya data shape test kar
sakein, phir decide karein production mein kaise integrate karna hai.

Kya fetch hota hai (har symbol ke liye, RAW — no trimming/no field drop):
  - PL  : annual, quarterly, halfyearly, ttm   × consolidated + standalone
  - BS  : annual, quarterly, ytd               × consolidated + standalone
  - CF  : annual, quarterly, ytd               × consolidated + standalone
  - Ratios: pr (profitability), le (leverage), li (liquidity), ef (efficiency)
            × consolidated + standalone
  - basic-financials (TTM)                     × consolidated + standalone
  - financial-metrics (growth, ratio_type=gr)  × consolidated + standalone
  - annual-price-ratios (PE/PB/PS history)     × consolidated + standalone
  - shareholdings/pattern (raw columns+rows, untouched)
  - company-profile (raw, untouched)

Usage:
  python pipeline_fundamentals_full.py test ITC RELIANCE TCS
      → fetches, saves locally to fundamentals_full_test.json
      → NO R2 upload, NO interaction with old pipeline/fundamentals.json

  python pipeline_fundamentals_full.py full
      → (not wired yet) all NSE symbols + R2 upload to a NEW file
        fundamentals_full.json — old fundamentals.json untouched
"""

import asyncio, json, logging, os, sys
from datetime import datetime
from pathlib import Path
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
FINEDGE_BASE  = "https://data.finedgeapi.com/api/v1"

FINEDGE_DELAY = 0.25
RETRY         = 5

PL_PERIODS  = ["annual", "quarterly", "halfyearly", "ttm"]
BS_PERIODS  = ["annual", "quarterly", "ytd"]
CF_PERIODS  = ["annual", "quarterly", "ytd"]
RATIO_TYPES = ["pr", "le", "li", "ef"]
STYPES      = ["c", "s"]   # consolidated, standalone — BOTH fetched (no fallback-only)

ROWS_LIMIT  = int(os.environ.get("ROWS_LIMIT", "1"))  # sirf latest N period rakho — testing ke liye 1

HERE = Path(__file__).parent
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)


async def _finedge_get(client, sem, path, params):
    params = {**params, "token": FINEDGE_TOKEN}
    url = f"{FINEDGE_BASE}/{path}"
    async with sem:
        for attempt in range(RETRY):
            await asyncio.sleep(FINEDGE_DELAY)
            try:
                r = await client.get(url, params=params, timeout=30)
            except httpx.RequestError as e:
                log.warning(f"  network error: {e}, retry {attempt+1}")
                await asyncio.sleep(2 ** attempt); continue
            if r.status_code == 401:
                log.error("❌ FINEDGE_TOKEN invalid"); sys.exit(1)
            if r.status_code == 429:
                log.warning("  rate limit — 20s"); await asyncio.sleep(20); continue
            if r.status_code in (502, 503, 504):
                await asyncio.sleep(2 ** attempt); continue
            if r.status_code != 200 or not r.text.strip():
                return None
            try:
                return r.json()
            except Exception:
                return None
    return None


async def _fetch_financials_all(client, sem, sym, code, periods):
    """Raw, UNTRIMMED financials rows — every (period × statement_type) combo."""
    out = {}
    for period in periods:
        out[period] = {}
        for stype in STYPES:
            d = await _finedge_get(client, sem, f"financials/{sym}",
                                    {"statement_type": stype, "statement_code": code, "period": period})
            out[period][stype] = (d or {}).get("financials", [])[:ROWS_LIMIT]
    return out


async def _fetch_ratios_all(client, sem, sym):
    out = {}
    for rtype in RATIO_TYPES:
        out[rtype] = {}
        for stype in STYPES:
            d = await _finedge_get(client, sem, f"ratios/{sym}", {"statement_type": stype, "ratio_type": rtype})
            out[rtype][stype] = (d or {}).get("ratios", [])[:ROWS_LIMIT]
    return out


async def _fetch_basic_financials(client, sem, sym):
    out = {}
    for stype in STYPES:
        d = await _finedge_get(client, sem, f"basic-financials/{sym}", {"statement_type": stype, "statement_code": "pl"})
        out[stype] = (d or {}).get("ratios", [])[:ROWS_LIMIT]
    return out


async def _fetch_growth_metrics(client, sem, sym):
    out = {}
    for stype in STYPES:
        d = await _finedge_get(client, sem, f"financial-metrics/{sym}", {"statement_type": stype, "ratio_type": "gr"})
        out[stype] = (d or {}).get("financial_metrics")
    return out


async def _fetch_annual_price_ratios(client, sem, sym):
    out = {}
    for stype in STYPES:
        d = await _finedge_get(client, sem, f"annual-price-ratios/{sym}", {"statement_type": stype})
        out[stype] = (d or {}).get("price_ratios", [])[:ROWS_LIMIT]
    return out


async def _fetch_shareholding_raw(client, sem, sym):
    return await _finedge_get(client, sem, f"shareholdings/pattern/{sym}", {"period": "quarterly"})  # raw, untouched


async def _fetch_profile_raw(client, sem, sym):
    return await _finedge_get(client, sem, f"company-profile/{sym}", {})  # raw, untouched


async def fetch_full_fundamentals(client, sem, sym):
    log.info(f"→ {sym}")
    (pl, bs, cf, ratios, basic, growth, price_ratios, shareholding, profile) = await asyncio.gather(
        _fetch_financials_all(client, sem, sym, "pl", PL_PERIODS),
        _fetch_financials_all(client, sem, sym, "bs", BS_PERIODS),
        _fetch_financials_all(client, sem, sym, "cf", CF_PERIODS),
        _fetch_ratios_all(client, sem, sym),
        _fetch_basic_financials(client, sem, sym),
        _fetch_growth_metrics(client, sem, sym),
        _fetch_annual_price_ratios(client, sem, sym),
        _fetch_shareholding_raw(client, sem, sym),
        _fetch_profile_raw(client, sem, sym),
    )
    obj = {
        "symbol": sym,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "profile": profile,
        "pl": pl,
        "bs": bs,
        "cf": cf,
        "ratios": ratios,
        "basic_financials": basic,
        "growth_metrics": growth,
        "annual_price_ratios": price_ratios,
        "shareholding": shareholding,
    }
    n_rows = (
        sum(len(rows) for period in pl.values() for rows in period.values())
        + sum(len(rows) for period in bs.values() for rows in period.values())
        + sum(len(rows) for period in cf.values() for rows in period.values())
    )
    log.info(f"  ✓ {sym}: {n_rows} financial rows fetched (pl+bs+cf, both statement types)")
    return sym, obj


async def run_test(symbols):
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[fetch_full_fundamentals(client, sem, sym.upper()) for sym in symbols])
    data = {sym: obj for sym, obj in results}
    out_path = OUT_DIR / "fundamentals_full_test.json"
    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    log.info(f"💾 Saved → {out_path}  ({out_path.stat().st_size/1024:.1f} KB)")
    log.info("⚠ R2 ko kuch upload nahi hua — purana fundamentals.json aur pipeline.py bilkul untouched.")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "test":
        syms = sys.argv[2:]
        if not syms:
            print("Usage: python pipeline_fundamentals_full.py test SYMBOL [SYMBOL2 ...]")
            sys.exit(1)
        asyncio.run(run_test(syms))
    else:
        print(__doc__)
        sys.exit(1)
