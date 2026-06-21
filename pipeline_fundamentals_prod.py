#!/usr/bin/env python3
"""
Finedge Fundamentals — PRODUCTION pipeline (v1)
=================================================
Symbols: SBIN, TCS, ITC, SBILIFE (hardcoded — change SYMBOLS list to extend)

Decisions baked in (from testing phase):
  - PL  periods : annual, quarterly, ttm        (halfyearly dropped — redundant)
  - BS  periods : annual, quarterly             (ttm/ytd not meaningful — point-in-time)
  - CF  periods : annual, quarterly, ytd        (ttm not offered by Finedge for cf)
  - Both statement_types (c + s) fetched always — banking ratios like CET1/NPA
    are ONLY populated in standalone, never in consolidated.
  - shareholdings/pattern endpoint REMOVED — cuts 1 call/symbol, not needed right now.
  - Full history kept (no row-count trimming) — set ROWS_LIMIT env var to cap if needed.
  - PL/BS get an alias-resolved "core" object (handles bank vs non-bank field-naming,
    e.g. revenueFromOperations vs n/a, profitBeforeTax vs profitLossBeforeTax) PLUS
    the full untouched "raw" rows — so nothing is lost even for schemas we haven't
    seen yet (e.g. SBILIFE/insurance, which may not match either alias set well).
  - CF / ratios / basic_financials / growth_metrics / annual_price_ratios stored as
    raw full rows — their schemas were consistent enough across bank/non-bank in
    testing to not need alias mapping.

Usage:
  python pipeline_fundamentals_prod.py run
      → fetches all SYMBOLS, saves to output/fundamentals_prod.json
  python pipeline_fundamentals_prod.py run SBIN TCS
      → override symbol list via CLI args
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

SYMBOLS     = ["SBIN", "TCS", "ITC", "SBILIFE"]
PL_PERIODS  = ["annual", "quarterly", "ttm"]
BS_PERIODS  = ["annual", "quarterly"]
CF_PERIODS  = ["annual", "quarterly", "ytd"]
RATIO_TYPES = ["pr", "le", "li", "ef"]
STYPES      = ["c", "s"]

ROWS_LIMIT = os.environ.get("ROWS_LIMIT")
ROWS_LIMIT = int(ROWS_LIMIT) if ROWS_LIMIT else None  # None = full history, no trimming

HERE = Path(__file__).parent
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════
# FINEDGE HELPERS
# ══════════════════════════════════════════════════════════════

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


def _trim(rows):
    return rows[:ROWS_LIMIT] if ROWS_LIMIT else rows


# ══════════════════════════════════════════════════════════════
# COMPANY TYPE DETECTION (from company-profile)
# ══════════════════════════════════════════════════════════════

def _classify_company(profile):
    if not profile:
        return "other"
    text = " ".join(str(profile.get(k, "")) for k in ("sector", "industry", "macro_sector")).lower()
    if "bank" in text:
        return "bank"
    if "insurance" in text:
        return "insurance"
    return "other"


# ══════════════════════════════════════════════════════════════
# CORE FIELD ALIAS RESOLUTION — PL & BS
# (handles bank vs non-bank naming differences without branching;
#  degrades to None gracefully for schemas we haven't mapped yet,
#  e.g. insurance — raw rows are kept alongside so nothing is lost)
# ══════════════════════════════════════════════════════════════

CORE_PL_ALIASES = {
    "year":              ["year"],
    "period_end":        ["period_end"],
    "period_start":      ["period_start"],
    "sales":             ["revenueFromOperations"],
    "pbt":               ["profitBeforeTax", "profitLossBeforeTax"],
    "pat":               ["profitLossForPeriod", "profitLossForThePeriod"],
    "pat_attributable":  ["profitOrLossAttributableToOwners"],
    "eps":               ["eps"],
    "other_income":      ["otherIncome"],
    "finance_costs":     ["financeCosts"],
    "depreciation":      ["depreciationAndAmortisation"],
    "employee_cost":     ["employeeBenefitExpense", "employeesCost"],
    "exceptional_items": ["exceptionalItemsBeforeTax", "exceptionalItems"],
    "minority_interest": ["nonControllingInterests", "profitLossOfMinorityInterest"],
    "associates_share":  ["profitOrLossOfAssociates", "profitLossOfAssociates"],
    "tax_expense":       ["taxExpense"],
    "diluted_shares":    ["dilutedOutstandingShares"],
    # bank-only — None for non-bank/insurance, that's expected
    "interest_earned":   ["interestEarned"],
    "interest_expended": ["interestExpended"],
    "provisions_loan_loss": ["provisionsForLoanLoss"],
    "npa_pct":           ["percentageOfNpa"],
    "gross_npa_pct":     ["percentageOfGrossNpa"],
    "cet1_ratio":        ["cET1Ratio"],
    "at1_ratio":         ["additionalTier1Ratio"],
}

CORE_BS_ALIASES = {
    "year":           ["year"],
    "period_end":     ["period_end"],
    "total_assets":   ["assets"],
    "reserves":       ["reserves"],
    "equity_capital": ["equityCapital", "capital"],
    "cash":           ["cashAndCashEquivalents", "cashAndBalancesWithRBI"],
    "investments":    ["investments", "noncurrentInvestments"],
    "fixed_assets":   ["propertyPlantAndEquipment", "fixedAssets"],
    # bank-only
    "advances":       ["advances"],
    "deposits":       ["deposits"],
    # non-bank-only
    "inventories":      ["inventories"],
    "trade_receivables": ["tradeReceivablesCurrent"],
    "trade_payables":    ["tradePayablesCurrent"],
}


def _resolve_aliases(row, alias_map):
    out = {}
    for key, aliases in alias_map.items():
        val = None
        for a in aliases:
            if a in row and row[a] is not None:
                val = row[a]
                break
        out[key] = val
    return out


def _bs_total_equity(row):
    te = row.get("totalEquity")
    if te is not None:
        return te
    cap = row.get("equityCapital", row.get("capital"))
    res = row.get("reserves")
    if cap is not None and res is not None:
        return cap + res
    return None


def _bs_borrowings_total(row):
    if "borrowingsCurrent" in row or "borrowingsNoncurrent" in row:
        return (row.get("borrowingsCurrent") or 0) + (row.get("borrowingsNoncurrent") or 0)
    return row.get("borrowings")


def _build_pl_core(row):
    core = _resolve_aliases(row, CORE_PL_ALIASES)
    if core.get("interest_earned") is not None and core.get("interest_expended") is not None:
        core["net_interest_income"] = core["interest_earned"] - core["interest_expended"]
    return core


def _build_bs_core(row):
    core = _resolve_aliases(row, CORE_BS_ALIASES)
    core["total_equity"] = _bs_total_equity(row)
    core["borrowings_total"] = _bs_borrowings_total(row)
    return core


# ══════════════════════════════════════════════════════════════
# FETCH FUNCTIONS
# ══════════════════════════════════════════════════════════════

async def _fetch_financials_all(client, sem, sym, code, periods, build_core_fn=None):
    out = {}
    for period in periods:
        out[period] = {}
        for stype in STYPES:
            d = await _finedge_get(client, sem, f"financials/{sym}",
                                    {"statement_type": stype, "statement_code": code, "period": period})
            rows = _trim((d or {}).get("financials", []))
            if build_core_fn:
                out[period][stype] = {"core": [build_core_fn(r) for r in rows], "raw": rows}
            else:
                out[period][stype] = rows
    return out


async def _fetch_ratios_all(client, sem, sym):
    out = {}
    for rtype in RATIO_TYPES:
        out[rtype] = {}
        for stype in STYPES:
            d = await _finedge_get(client, sem, f"ratios/{sym}", {"statement_type": stype, "ratio_type": rtype})
            out[rtype][stype] = _trim((d or {}).get("ratios", []))
    return out


async def _fetch_basic_financials(client, sem, sym):
    out = {}
    for stype in STYPES:
        d = await _finedge_get(client, sem, f"basic-financials/{sym}", {"statement_type": stype, "statement_code": "pl"})
        out[stype] = _trim((d or {}).get("ratios", []))
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
        out[stype] = _trim((d or {}).get("price_ratios", []))
    return out


async def _fetch_profile_raw(client, sem, sym):
    return await _finedge_get(client, sem, f"company-profile/{sym}", {})


# ══════════════════════════════════════════════════════════════
# ASSEMBLE PER-SYMBOL OBJECT
# ══════════════════════════════════════════════════════════════

async def fetch_one_symbol(client, sem, sym):
    log.info(f"→ {sym}")
    profile = await _fetch_profile_raw(client, sem, sym)
    company_type = _classify_company(profile)

    pl, bs, cf, ratios, basic, growth, price_ratios = await asyncio.gather(
        _fetch_financials_all(client, sem, sym, "pl", PL_PERIODS, build_core_fn=_build_pl_core),
        _fetch_financials_all(client, sem, sym, "bs", BS_PERIODS, build_core_fn=_build_bs_core),
        _fetch_financials_all(client, sem, sym, "cf", CF_PERIODS),   # raw only — schema consistent
        _fetch_ratios_all(client, sem, sym),
        _fetch_basic_financials(client, sem, sym),
        _fetch_growth_metrics(client, sem, sym),
        _fetch_annual_price_ratios(client, sem, sym),
    )

    obj = {
        "symbol": sym,
        "company_type": company_type,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "profile": profile,
        "pl": pl,
        "bs": bs,
        "cf": cf,
        "ratios": ratios,
        "basic_financials": basic,
        "growth_metrics": growth,
        "annual_price_ratios": price_ratios,
    }
    log.info(f"  ✓ {sym} ({company_type})")
    return sym, obj


async def run(symbols):
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[fetch_one_symbol(client, sem, sym.upper()) for sym in symbols])
    data = {sym: obj for sym, obj in results}
    out_path = OUT_DIR / "fundamentals_prod.json"
    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    log.info(f"💾 Saved → {out_path}  ({out_path.stat().st_size/1024:.1f} KB)")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "run":
        syms = sys.argv[2:] or SYMBOLS
        asyncio.run(run(syms))
    else:
        print(__doc__)
        sys.exit(1)
