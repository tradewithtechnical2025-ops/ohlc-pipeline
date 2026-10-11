#!/usr/bin/env python3
"""
Finedge Fundamentals — PRODUCTION pipeline (v3, R2-wired)
============================================================
(See module docstring history in repo — dual-track quarters added July 2026,
see notes near _build_summary_entry below.)

FIX (Aug 2026): insurance P&L fields were resolving to None — see the note
above CORE_PL_ALIASES for details.
"""

import asyncio, hashlib, json, logging, os, re, sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]
FINEDGE_BASE  = "https://data.finedgeapi.com/api/v1"
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

FINEDGE_DELAY = 0.25
RETRY         = 5
CONCURRENCY   = 4
TOTAL_PARTS   = 10

PL_PERIODS    = ["annual", "quarterly", "ttm"]
BS_PERIODS    = ["annual", "quarterly"]
CF_PERIODS    = ["annual", "quarterly", "ytd"]
RATIO_TYPES   = ["pr", "le", "li", "ef"]
QUARTERLY_CAP = 12

# CONFIRMED (Aug 2026, from live docs) — /segment-revenue/{symbol}.
# statement_type: c/s (same as elsewhere). statement_code: pl/bs only (no
# cf). period: quarterly/annual/halfyearly/ttm — halfyearly isn't fetched
# here (not used elsewhere in this pipeline); add it to SEGMENT_PERIODS if
# needed later.
SEGMENT_PERIODS = ["annual", "quarterly"]

MAX_PENDING_ATTEMPTS = 5

ETF_ENDSWITH = ("ETF", "BEES", "LIQUID", "GILT", "IETF", "MMQS", "TOTAL")
ETF_CONTAINS = ("NIFTY", "BANKEX", "MSCIN")

HERE = Path(__file__).parent
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _is_etf(sym):
    s = sym.upper()
    return any(s.endswith(k) for k in ETF_ENDSWITH) or any(k in s for k in ETF_CONTAINS)


def _period_limit(period):
    return QUARTERLY_CAP if period in ("quarterly", "ytd") else None


_MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _fmt_period_end(period_end):
    if not period_end:
        return ""
    s = str(int(period_end))
    if len(s) == 8:
        m = int(s[4:6])
        return f"{_MONTHS[m]} {s[:4]}" if 1 <= m <= 12 else s
    return str(period_end)


def today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")


def _latest_key(rows):
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


def is_trading_day(d: str) -> bool:
    holidays_path = HERE / "nse_holidays.json"
    holidays = set()
    if holidays_path.exists():
        try:
            holidays = set(json.loads(holidays_path.read_text()))
        except Exception:
            pass
    dt = date.fromisoformat(d)
    return dt.weekday() < 5 and d not in holidays


# ══════════════════════════════════════════════════════════════
# R2 HELPERS
# ══════════════════════════════════════════════════════════════

async def r2_download(client, filename):
    from urllib.parse import quote
    url = f"{WORKER_URL}/{quote(filename, safe='/')}"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↓ {filename} ({len(r.content)/1024:.0f} KB)")
    return r.json()


async def r2_upload(client, filename, data):
    if isinstance(data, str):
        data = data.encode()
    r = await client.post(WORKER_URL, params={"file": filename},
                           headers={**WORKER_HEADERS, "Content-Type": "application/json"}, content=data, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")


def compute_hash(payload: str) -> str:
    return hashlib.md5(payload.encode()).hexdigest()[:10]


async def r2_upload_symbol(client, sym, obj):
    payload = json.dumps(obj, separators=(",", ":"))
    await r2_upload(client, f"fundamentals_full/{sym}.json", payload)
    return compute_hash(payload)


SUMMARY_FILE = "fundamentals_summary.json"
PENDING_FILE = "fundamentals_pending.json"


async def r2_download_summary(client):
    d = await r2_download(client, SUMMARY_FILE)
    if isinstance(d, dict) and "stocks" in d:
        return d["stocks"]
    return {}


async def r2_upload_summary(client, stocks):
    payload = json.dumps({"updated": today_ist(), "stocks": stocks}, separators=(",", ":"))
    await r2_upload(client, SUMMARY_FILE, payload)


# fundamentals_rich.json (schema 2) — see _build_rich_entry. Lives beside
# the summary and is saved at the same moments, from the same stored objects.
async def r2_download_rich(client):
    d = await r2_download(client, "fundamentals_rich.json")
    if isinstance(d, dict) and d.get("schema_version") == 2 and isinstance(d.get("stocks"), dict):
        return d["stocks"]
    return {}


async def r2_upload_rich(client, stocks):
    payload = json.dumps({
        "schema_version": 2,
        "updated": today_ist(),
        "units": {"money": "crore", "per_share": "rupee", "ratios": "percent", "order": "newest_first"},
        "stocks": stocks,
    }, separators=(",", ":"), allow_nan=False)
    await r2_upload(client, "fundamentals_rich.json", payload)


async def r2_download_pending(client):
    d = await r2_download(client, PENDING_FILE)
    return d if isinstance(d, dict) else {}


async def r2_upload_pending(client, pending):
    payload = json.dumps(pending, separators=(",", ":"))
    await r2_upload(client, PENDING_FILE, payload)


async def get_nse_universe(client, exchange_filter=None):
    # NOTE (Sep 2026): classification.json now carries BSE stocks too (merged
    # NSE+BSE universe). For a BSE-exclusive stock, "symbol" is the numeric
    # BSE scrip code (e.g. "544467") — Finedge itself uses this numeric code
    # as the query symbol for BSE stocks, so "symbol" as-is is already the
    # right thing to both query Finedge with AND use as the storage key
    # (fundamentals_full/{sym}.json, summary dict) matching the "stock"
    # field used elsewhere on the platform. No trading_symbol substitution
    # needed — this was previously restricted to exchange == "NSE" only,
    # which is the actual bug: BSE-exclusive stocks never entered the
    # universe at all.
    #
    # exchange_filter: None = NSE+BSE both (default). Pass "NSE" or "BSE" to
    # restrict to just that exchange (used by the "bse" CLI mode below).
    allowed = {"NSE", "BSE"} if exchange_filter is None else {exchange_filter.strip().upper()}
    classification = await r2_download(client, "classification.json")
    if not classification or not isinstance(classification, list):
        raise RuntimeError("classification.json missing or invalid in R2!")
    symbols = []
    for s in classification:
        sym = str(s.get("symbol", "")).strip().upper()
        exch = str(s.get("exchange", "")).strip().upper()
        if sym and exch in allowed and not _is_etf(sym):
            symbols.append(sym)
    symbols = sorted(set(symbols))
    log.info(f"Universe: {len(symbols)} {'/'.join(sorted(allowed))} equity symbols (ETFs excluded)")
    return symbols


async def get_classification_lookup(client):
    classification = await r2_download(client, "classification.json")
    if not classification or not isinstance(classification, list):
        log.warning("classification.json missing/invalid — sector_group/display_industry will be blank")
        return {}
    out = {}
    for s in classification:
        sym = str(s.get("symbol", "")).strip().upper()
        if not sym:
            continue
        out[sym] = {
            "sector_group": s.get("sector_group"),
            "display_industry": s.get("display_industry"),
            "exchange": str(s.get("exchange", "")).strip().upper(),
        }
    return out


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


async def get_today_result_symbols(client, sem, valid_symbols):
    today = today_ist()
    next1 = (date.fromisoformat(today) + timedelta(days=1)).isoformat()
    d = await _finedge_get(client, sem, "results-calendar", {"from_date": today, "to_date": next1})
    if not d or not isinstance(d, list):
        log.info("Finedge results-calendar — empty or error")
        return []
    valid = set(valid_symbols)
    matched = sorted({item["symbol"] for item in d
                       if item.get("symbol") in valid and item.get("expected_result_date") == today})
    log.info(f"Results today ({today}): {len(matched)} stocks")
    return matched


# ══════════════════════════════════════════════════════════════
# COMPANY TYPE DETECTION
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
# CORE FIELD ALIAS RESOLUTION — PL, BS, CF
# ══════════════════════════════════════════════════════════════

# NOTE (fix, Aug 2026): FinEdge uses 4 different P&L schemas — regular /
# bank / life-insurance / general-insurance (see
# FinEdge_API_field_reference_guide.pdf). "sales"/"pbt"/"pat"/"tax_expense"
# previously only carried the regular+bank field names, so insurance
# companies' PAT, Tax, and (for general insurance) Sales/Expenses/PBT
# resolved to None end-to-end (confirmed live on SBILIFE and GICRE).
# company_type isn't threaded into alias resolution anywhere in this file,
# so rather than plumb that through, each affected key's alias list is a
# UNION across all 4 schemas. Safe because none of these field names
# collide across company types (verified against the PDF).
CORE_PL_ALIASES = {
    "materials_cost": ['costOfMaterialsConsumed'],
    "purchases_stock": ['purchasesOfStockInTrade'],
    "inventory_change": ['changesInInventories'],
    "other_expenses": ['otherExpenses'],
    "other_operating_expenses": ['otherOperatingExpenses'],
    "interest_advances": ['interestOrDiscountOnAdvancesOrBills'],
    "interest_investments": ['revenueOnInvestments'],
    "interest_rbi": ['interestOnBalancesWithRBIAndOthers'],
    "other_interest": ['otherInterest'],
    "cost_of_goods_sold": ['costofGoodsSold'],

    "year": ["year"], "period_end": ["period_end"], "period_start": ["period_start"],
    "sales": [
        "revenueFromOperations", "income",
        "operatingIncome",          # general insurance (PDF p.4 row 1)
    ],
    "expenses": ["expenses", "expenditureExcludingProvisions", "operatingExpenses"],
    "pbt": [
        "profitBeforeTax", "profitLossBeforeTax",
        "profitOrLossBeforeTax",    # general insurance (PDF p.4 row 31)
    ],
    "pat": [
        "profitLossForPeriod", "profitLossForThePeriod",
        "profitLossAfterTaxAndExtraordinaryItems",  # life insurance (PDF p.3 row 39)
        "profitLossAfterTax",                        # general insurance (PDF p.4 row 33)
    ],
    "pat_attributable": ["profitOrLossAttributableToOwners"],
    "eps": ["eps"],
    "other_income": ["otherIncome"],
    "finance_costs": ["financeCosts"],
    "depreciation": ["depreciationAndAmortisation"],
    "employee_cost": ["employeeBenefitExpense", "employeesCost"],
    "exceptional_items": ["exceptionalItemsBeforeTax", "exceptionalItems"],
    "minority_interest": ["nonControllingInterests", "profitLossOfMinorityInterest"],
    "associates_share": ["profitOrLossOfAssociates", "profitLossOfAssociates"],
    "tax_expense": [
        "taxExpense",
        "provisionForTax",          # life insurance (PDF p.3 row 24)
        "provisionsForTaxes",       # general insurance (PDF p.4 row 32)
    ],
    "diluted_shares": ["dilutedOutstandingShares"],
    "interest_earned": ["interestEarned"],
    "interest_expended": ["interestExpended"],
    "provisions_loan_loss": ["provisionsForLoanLoss"],
    "npa_pct": ["percentageOfNpa"],
    "gross_npa_pct": ["percentageOfGrossNpa"],
    "cet1_ratio": ["cET1Ratio"],
    "at1_ratio": ["additionalTier1Ratio"],
}

CORE_BS_ALIASES = {
    "cwip": ['capitalWorkInProgress'],
    "investments_current": ['currentInvestments'],
    "investments_noncurrent": ['noncurrentInvestments'],
    "trade_receivables_noncurrent": ['tradeReceivablesNoncurrent'],
    "trade_payables_noncurrent": ['tradePayablesNoncurrent'],
    "other_assets": ['otherAssets'],
    "other_liabilities_provisions": ['otherLiabilitiesAndProvisions'],
    "other_current_assets": ['otherCurrentAssets'],
    "other_noncurrent_assets": ['otherNoncurrentAssets'],
    "other_current_liabilities": ['otherCurrentLiabilities'],
    "other_noncurrent_liabilities": ['otherNoncurrentLiabilities'],
    "goodwill": ['goodwill'],
    "intangibles": ['otherIntangibleAssets'],
    "bank_balances": ['otherBankBalances', 'bankBalancesAndMoneyAtCallAndShortNotice'],

    "year": ["year"], "period_end": ["period_end"],
    "total_assets": ["assets"],
    "reserves": ["reserves"],
    "equity_capital": ["equityCapital", "capital"],
    "cash": ["cashAndCashEquivalents", "cashAndBalancesWithRBI"],
    # FIX (Aug 2026): was missing currentInvestments — only non-current was
    # ever captured, so short-term investments dropped out of the total.
    "investments": ["investments", "noncurrentInvestments", "currentInvestments"],
    "fixed_assets": ["propertyPlantAndEquipment", "fixedAssets"],
    "borrowings_current": ["borrowingsCurrent"],
    "borrowings_noncurrent": ["borrowingsNoncurrent"],
    "current_assets": ["currentAssets"],
    "current_liabilities": ["currentLiabilities"],
    "advances": ["advances"],
    "deposits": ["deposits"],
    "inventories": ["inventories"],
    "trade_receivables": ["tradeReceivablesCurrent"],
    "trade_payables": ["tradePayablesCurrent"],
}

CORE_CF_ALIASES = {
    "working_capital_change": ['adjustmentsForWorkingCapitalChange'],
    "inventory_adjustment": ['adjForInventories'],
    "receivables_adjustment": ['adjForTradeReceivables', 'adjForTradeReceivablesCurrent'],
    "depreciation_adjustment": ['adjForDepreciationAndAmortisationExpense'],
    "taxes_paid": ['incomeTaxesPaidOrRefundClassifiedAsOperating'],
    "asset_sales": ['saleOfFixed&IntangibleAssets', 'proceedsFromPPESalesClassifiedAsInvesting'],
    "borrowings_proceeds": ['proceedsFromBorrowingsClassifiedAsFinancing'],
    "borrowings_repaid": ['repaymentsOfBorrowings'],
    "shares_issued": ['proceedsFromSharesClassifiedAsFinancing', 'proceedsFromIssuingShares'],
    "fx_effects": ['foreignExchangeEffects'],

    "year": ["year"], "period_end": ["period_end"], "period_start": ["period_start"],
    "cfo": ["cashFlowsFromOperatingActivities"],
    "cfi": ["cashFlowFromInvestingActivities", "cashFlowsFromInvestingActivities"],
    "cff": ["cashFlowsFromFinancingActivities"],
    "net_cash_flow": ["netCashFlow"],
    "capex": ["purchaseOfPPEClassifiedAsInvesting", "purchaseOfFixed&IntangibleAssets"],
    "dividends_paid": ["dividendsPaidClassifiedAsFinancing"],
    "interest_paid": ["interestPaidClassifiedAsFinancing"],
    "pbt": ["profitBeforeTax", "profitBeforeExtraordinaryItemsAndTax"],
}


def _resolve_aliases(row, alias_map):
    # FIX (Aug 2026): FinEdge pads every /financials response with field
    # names from ALL FOUR P&L schemas (regular/bank/life-insurance/general-
    # insurance) — fields not native to a company's actual type come back
    # as a literal 0, not null. Confirmed live on GICRE (general insurance):
    # row["income"]=0 (regular-schema placeholder) sits right next to
    # row["operatingIncome"]=133605400000 (GI's real revenue field). The
    # old `is not None` check treated 0 as "present" and locked onto
    # whichever alias came first in the list — which, for "sales"/"pbt"/
    # "expenses", happened to be the regular/bank placeholder, not the real
    # GI value — regardless of alias ordering. Collecting every non-null
    # candidate and preferring the first NON-ZERO one fixes this without
    # needing to guess a "correct" alias order per company type; falls back
    # to the first candidate (possibly 0) only when every alias for a key
    # is 0, so a genuinely-zero figure (e.g. finance_costs=0 that quarter)
    # still comes through correctly.
    out = {}
    for key, aliases in alias_map.items():
        candidates = [row[a] for a in aliases if a in row and row[a] is not None]
        if not candidates:
            out[key] = None
        else:
            nonzero = next((v for v in candidates if v != 0), None)
            out[key] = nonzero if nonzero is not None else candidates[0]
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


def _sum_components(row, keys):
    values = [row.get(k) for k in keys if row.get(k) is not None]
    return sum(values) if values else None


def _bs_borrowings_total(row):
    total = row.get("borrowings")
    components = _sum_components(row, ("borrowingsCurrent", "borrowingsNoncurrent"))
    # Padded cross-schema zero fields must not mask a reported bank total.
    return total if total is not None and (total != 0 or components is None) else components


def _build_pl_core(row, company_type="other"):
    core = _resolve_aliases(row, CORE_PL_ALIASES)
    core["company_type"] = company_type
    # Life insurance: tax on shareholder profit differs from policy-account tax.
    if company_type == "insurance" and (row.get("netPremiumIncome") not in (None, 0) or row.get("profitLossAfterTaxAndExtraordinaryItems") not in (None, 0)):
        core["tax_expense"] = row.get("provisionsForTaxes")
    if core.get("interest_earned") is not None and core.get("interest_expended") is not None:
        core["net_interest_income"] = core["interest_earned"] - core["interest_expended"]
    if company_type == "bank" and core.get("interest_earned") is not None:
        core["total_income"] = row.get("income")
        core["sales"] = core["interest_earned"]
    return core


def _build_bs_core(row):
    core = _resolve_aliases(row, CORE_BS_ALIASES)
    core["total_equity"] = _bs_total_equity(row)
    core["borrowings_total"] = _bs_borrowings_total(row)
    components = _sum_components(row, ("noncurrentInvestments", "currentInvestments"))
    reported = row.get("investments")
    core["investments"] = reported if reported is not None and (reported != 0 or components is None) else components
    core["trade_receivables_total"] = _sum_components(row, ("tradeReceivablesCurrent", "tradeReceivablesNoncurrent"))
    core["trade_payables_total"] = _sum_components(row, ("tradePayablesCurrent", "tradePayablesNoncurrent"))
    return core


def _build_cf_core(row):
    core = _resolve_aliases(row, CORE_CF_ALIASES)
    if core.get("cfo") is not None and core.get("capex") is not None:
        core["fcf"] = core["cfo"] - abs(core["capex"])
    return core


# ══════════════════════════════════════════════════════════════
# FETCH — DUAL statement_type
# ══════════════════════════════════════════════════════════════

async def _fetch_financials_dual(client, sem, sym, code, periods, build_core_fn=None):
    out = {}
    for period in periods:
        limit = _period_limit(period)
        out[period] = {}
        for stype in ("c", "s"):
            d = await _finedge_get(client, sem, f"financials/{sym}",
                                    {"statement_type": stype, "statement_code": code, "period": period})
            rows = (d or {}).get("financials", [])
            if limit:
                rows = rows[:limit]
            if build_core_fn:
                out[period][stype] = {"core": [build_core_fn(r) for r in rows], "raw": rows}
            else:
                out[period][stype] = rows
    return out


async def _fetch_basic_financials_dual(client, sem, sym):
    out = {}
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"basic-financials/{sym}", {"statement_type": stype, "statement_code": "pl"})
        out[stype] = (d or {}).get("ratios", [])
    return out


def _seg_val(d, keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _build_segment_core(entry):
    """Flattens one segment_revenues[] entry (company-level 'data' block +
    per-segment 'segments' list) into a simpler shape.

    FIX (Aug 2026): confirmed live on GICRE (general insurance) that
    insurers use a COMPLETELY DISJOINT field set from regular companies —
    zero key overlap with ITC's schema (see comment on _seg_val callers
    below). The original version only knew the regular-company field names
    (segmentRevenue / segmentProfitLossBeforeTaxAndFinanceCosts), so every
    insurer's segment data silently came out all-null even though the
    company-provided segment names (e.g. "Fire", "Aviation", "Personal
    Accident") were present. For insurers there's no direct "revenue" line
    in segment reporting — netPremium (premium income for that line of
    business) is the closest available proxy, and operatingProfitOrLoss is
    used as the profit proxy in place of PBT-before-finance-costs. Note:
    FinEdge's own field name has a typo — "incomeFormInvestments", not
    "...From..." — kept as-is since it's the live API's actual key.
    """
    d = entry.get("data") or {}
    return {
        "header": entry.get("header"),
        "total_revenue": _seg_val(d, ["segmentRevenue", "netPremium"]),
        "total_revenue_from_operations": d.get("segmentRevenueFromOperations"),
        "inter_segment_revenue": d.get("interSegmentRevenue"),
        "total_pbt": d.get("segmentProfitBeforeTax"),
        "total_pbt_before_finance_costs": _seg_val(d, ["segmentProfitLossBeforeTaxAndFinanceCosts", "operatingProfitOrLoss"]),
        "finance_costs": d.get("segmentFinanceCosts"),
        "net_unallocable_expense": d.get("netOtherUnallocableExpense"),
        "segments": [
            {
                "name": s.get("name"),
                "revenue": _seg_val((s.get("data") or {}), ["segmentRevenue", "netPremium"]),
                "pbt_before_finance_costs": _seg_val((s.get("data") or {}), ["segmentProfitLossBeforeTaxAndFinanceCosts", "operatingProfitOrLoss"]),
            }
            for s in (entry.get("segments") or [])
        ],
    }


def _seg_canon_key(name):
    """Case/whitespace/hyphen-spacing-insensitive key for grouping segment
    name spelling variants — "FMCG - Others"/"FMCG- Others"/"FMCG-Others"/
    "FMCg - Others" all collapse to the same key."""
    if not name:
        return ""
    s = re.sub(r"\s+", " ", str(name).strip().lower())
    s = re.sub(r"\s*-\s*", "-", s)
    return s


def _canonicalize_segment_series(core_rows):
    """FIX (Aug 2026): confirmed live on ITC — the same business segment
    gets spelled slightly differently across different quarters'/years'
    filings (extra/missing space around the hyphen, inconsistent
    capitalization — e.g. "FMCG - Cigarettes" vs "FMCG-Cigarettes" vs
    "FMCg - Others"). Treating these as distinct segments produced a table
    full of near-duplicate rows that were empty everywhere except the one
    period using that exact spelling. Groups by a normalized key
    (_seg_canon_key), picks the most-common raw spelling across the whole
    series as the canonical display label, and rewrites every period's
    segments to use it — merging same-key entries within a single row
    (defensive; not expected in practice, but keeps output well-formed if
    a filing ever lists a segment twice under two spellings in one row)."""
    label_counts = defaultdict(Counter)
    for row in core_rows:
        for s in (row.get("segments") or []):
            key = _seg_canon_key(s.get("name"))
            if key:
                label_counts[key][s.get("name")] += 1
    canonical_label = {key: counts.most_common(1)[0][0] for key, counts in label_counts.items()}

    for row in core_rows:
        merged = {}
        for s in (row.get("segments") or []):
            key = _seg_canon_key(s.get("name"))
            if not key:
                continue
            label = canonical_label[key]
            if label not in merged:
                merged[label] = {"name": label, "revenue": None, "pbt_before_finance_costs": None}
            for field in ("revenue", "pbt_before_finance_costs"):
                v = s.get(field)
                if v is not None:
                    merged[label][field] = (merged[label][field] or 0) + v
        row["segments"] = list(merged.values())
    return core_rows


async def _fetch_segment_revenue_dual(client, sem, sym):
    out = {}
    for period in SEGMENT_PERIODS:
        out[period] = {}
        for stype in ("c", "s"):
            d = await _finedge_get(client, sem, f"segment-revenue/{sym}",
                                    {"statement_type": stype, "statement_code": "pl", "period": period})
            rows = (d or {}).get("segment_revenues", [])
            core = _canonicalize_segment_series([_build_segment_core(r) for r in rows])
            out[period][stype] = {"core": core, "raw": rows}
    return out


# ══════════════════════════════════════════════════════════════
# FETCH — SINGLE statement_type
# ══════════════════════════════════════════════════════════════

async def _fetch_financials_single(client, sem, sym, code, periods, build_core_fn=None, keep_raw=True):
    out = {}
    for period in periods:
        limit = _period_limit(period)
        rows, stype_used = [], None
        for stype in ("c", "s"):
            d = await _finedge_get(client, sem, f"financials/{sym}",
                                    {"statement_type": stype, "statement_code": code, "period": period})
            r = (d or {}).get("financials", [])
            if r:
                rows, stype_used = r, stype
                break
        if limit:
            rows = rows[:limit]
        entry = {"stype_used": stype_used}
        if keep_raw:
            entry["raw"] = rows
        if build_core_fn:
            entry["core"] = [build_core_fn(r) for r in rows]
        out[period] = entry
    return out


async def _fetch_ratios_single(client, sem, sym):
    out = {}
    for rtype in RATIO_TYPES:
        rows_by_stype = {}
        for stype in ("c", "s"):
            d = await _finedge_get(client, sem, f"ratios/{sym}", {"statement_type": stype, "ratio_type": rtype})
            rows_by_stype[stype] = (d or {}).get("ratios", [])
        c_rows, s_rows = rows_by_stype["c"], rows_by_stype["s"]
        stype_used = "s" if _latest_key(s_rows) > _latest_key(c_rows) else "c"
        rows = rows_by_stype[stype_used]
        if not rows:
            rows = c_rows or s_rows
            stype_used = "c" if c_rows else ("s" if s_rows else None)
        out[rtype] = {"stype_used": stype_used, "raw": rows, "raw_c": c_rows, "raw_s": s_rows}
    return out


async def _fetch_growth_metrics_single(client, sem, sym):
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"financial-metrics/{sym}", {"statement_type": stype, "ratio_type": "gr"})
        fm = (d or {}).get("financial_metrics")
        if fm:
            return {"stype_used": stype, "data": fm}
    return {"stype_used": None, "data": None}


async def _fetch_annual_price_ratios_single(client, sem, sym):
    rows_by_stype = {}
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"annual-price-ratios/{sym}", {"statement_type": stype})
        rows_by_stype[stype] = (d or {}).get("price_ratios", [])
    c_rows, s_rows = rows_by_stype["c"], rows_by_stype["s"]
    stype_used = "s" if _latest_key(s_rows) > _latest_key(c_rows) else "c"
    rows = rows_by_stype[stype_used]
    if not rows:
        rows = c_rows or s_rows
        stype_used = "c" if c_rows else ("s" if s_rows else None)
    return {"stype_used": stype_used, "raw": rows, "raw_c": c_rows, "raw_s": s_rows}


async def _fetch_profile_raw(client, sem, sym):
    return await _finedge_get(client, sem, f"company-profile/{sym}", {})


# ══════════════════════════════════════════════════════════════
# LIGHTWEIGHT SUMMARY ENTRY
# ══════════════════════════════════════════════════════════════

def _compute_opm(row):
    # Bank operating profit is not conventional EBITDA/OPM.
    if row.get("company_type") == "bank":
        return None
    sales = row.get("sales")
    interest_earned = row.get("interest_earned")
    if not sales and interest_earned:
        ie = row.get("interest_expended") or 0
        return round((interest_earned - ie) / interest_earned, 4) if interest_earned else None
    if not sales:
        return None
    exp = row.get("expenses")
    if exp is not None:
        # FIX (Sep 2026): FinEdge "expenses" is the filing's Total Expenses,
        # which INCLUDES depreciation and finance costs (verified on
        # 3BBLACKBIO: materials+inventory chg+employee+other+D&A+finance ==
        # expenses, all 12 quarters). Add both back so OPM is EBITDA-style
        # and matches Screener's OPM exactly (e.g. 3BBLACKBIO Mar'26: 24.0%,
        # was showing 12.7%).
        op_exp = exp - (row.get("depreciation") or 0) - (row.get("finance_costs") or 0)
        return round((sales - op_exp) / sales, 4)
    pbt  = row.get("pbt")
    dep  = row.get("depreciation")
    fin  = row.get("finance_costs")
    oth  = row.get("other_income") or 0
    if pbt is not None and dep is not None and fin is not None:
        op = pbt + dep + fin - oth
        return round(op / sales, 4)
    return None


def _compute_ebitda_abs(row):
    """Mirrors _compute_opm()'s branches exactly, but returns the absolute
    EBITDA-ish figure (the numerator) instead of dividing by sales to get
    a margin. Banks/NBFCs (no `sales`, only `interest_earned`) are handled
    the same way _compute_opm() handles them."""
    # Bank operating profit is not conventional EBITDA/OPM.
    if row.get("company_type") == "bank":
        return None
    sales = row.get("sales")
    interest_earned = row.get("interest_earned")
    if not sales and interest_earned:
        ie = row.get("interest_expended") or 0
        return interest_earned - ie
    if not sales:
        return None
    exp = row.get("expenses")
    if exp is not None:
        # Same D&A + finance-cost add-back as _compute_opm() (see FIX note there).
        return sales - (exp - (row.get("depreciation") or 0) - (row.get("finance_costs") or 0))
    pbt = row.get("pbt")
    dep = row.get("depreciation")
    fin = row.get("finance_costs")
    oth = row.get("other_income") or 0
    if pbt is not None and dep is not None and fin is not None:
        return pbt + dep + fin - oth
    return None


def _build_quarters_list(q_core, company_type="other"):
    """Builds the standard 'quarters' array shape from a stype's quarterly
    PL core rows. Factored out so both the primary and the alt (dual-track)
    stype can build the same shape without duplicating this logic."""
    return [{
        "header": _fmt_period_end(row.get("period_end")),
        "sales":    row.get("interest_earned") if company_type == "bank" else row.get("sales"),
        "total_income": row.get("total_income"),
        "interest_earned": row.get("interest_earned"),
        "net_interest_income": row.get("net_interest_income"),
        "pat_attributable": row.get("pat_attributable"),
        "associates_share": row.get("associates_share"),
        "expenses": row.get("expenses"),
        "opm":      None if company_type == "bank" else _compute_opm(row),
        "ebitda":   None if company_type == "bank" else _compute_ebitda_abs(row),
        "eps":      row.get("eps"),
        "pat":      row.get("pat"),
        "pbt":      row.get("pbt"),
    } for row in q_core[:9]]


def _build_summary_entry(sym, profile, pl, ratios, price_ratios, cf=None, classification=None):
    profile = profile or {}
    classification = classification or {}
    cf = cf or {}

    diluted_shares = None
    for period in ("ttm", "annual", "quarterly"):
        for stype in ("c", "s"):
            core = pl.get(period, {}).get(stype, {}).get("core", [])
            if core and core[0].get("diluted_shares") is not None:
                diluted_shares = core[0]["diluted_shares"]
                break
        if diluted_shares is not None:
            break

    # ── Summary — single consistent stype per stock ──────────────────────
    c_core = pl.get("quarterly", {}).get("c", {}).get("core") or []
    s_core = pl.get("quarterly", {}).get("s", {}).get("core") or []
    stype = "s" if _latest_key(s_core) > _latest_key(c_core) else "c"
    if stype == "c" and not c_core and s_core:
        stype = "s"

    q_core = s_core if stype == "s" else c_core
    quarters = _build_quarters_list(q_core, _classify_company(profile))

    # ── Dual-track: also keep the OTHER stype's quarters, if it has any
    # data, as quarters_alt/stype_alt (added July 2026). The single-stype
    # pick above is a strict-recency tie-break (Consolidated wins on a tie
    # even when Standalone is equally current) — a consumer that needs to
    # match a SPECIFIC basis (e.g. pipeline_news.py matching a live XBRL
    # filing's own Standalone/Consolidated nature) would otherwise never
    # find that basis's quarters even though the data exists and is
    # current. Purely additive — existing consumers of quarters/stype see
    # no change at all.
    alt_stype = "s" if stype == "c" else "c"
    alt_core = s_core if alt_stype == "s" else c_core
    quarters_alt = _build_quarters_list(alt_core, _classify_company(profile)) if alt_core else []

    pr_rows  = ratios.get("pr", {}).get(f"raw_{stype}") or ratios.get("pr", {}).get("raw") or []
    le_rows  = ratios.get("le", {}).get(f"raw_{stype}") or ratios.get("le", {}).get("raw") or []
    apr_rows = price_ratios.get(f"raw_{stype}") or price_ratios.get("raw") or []
    pr0  = (pr_rows or [{}])[0]
    le0  = (le_rows or [{}])[0]
    apr0 = (apr_rows or [{}])[0]

    # ── FCF ────────────────────────────────────────────────────────────
    # _fetch_financials_single (unlike the PL fetch) already auto-picks a
    # single stype for cash flow — cf["quarterly"] is {"stype_used":...,
    # "core":[...]}, not split by stype the way pl["quarterly"] is. Take
    # whatever its most recent quarterly row's fcf is. fcf itself is
    # computed upstream in _build_cf_core() as cfo + capex.
    cf_core = (cf.get("quarterly", {}) or {}).get("core") or []
    fcf_latest = cf_core[0].get("fcf") if cf_core else None

    ebitda_latest = quarters[0].get("ebitda") if quarters else None
    fcf_to_ebitda = (
        round(fcf_latest / ebitda_latest, 4)
        if fcf_latest is not None and ebitda_latest not in (None, 0)
        else None
    )

    return {
        "symbol": sym,
        "name": profile.get("name"),
        "sector": profile.get("sector"),
        "industry": profile.get("industry"),
        "macro_sector": profile.get("macro_sector"),
        "sector_group": classification.get("sector_group"),
        "display_industry": classification.get("display_industry"),
        "diluted_shares": diluted_shares,
        "stype": stype,
        "pe": apr0.get("pe"),
        "pb": apr0.get("pb"),
        "roe": pr0.get("returnOnEquity"),
        "roce": pr0.get("returnOnCapital"),
        "ebitda_margin": pr0.get("ebitdaMargin"),
        "de_ratio": le0.get("totalDebtToEquity"),
        "fcf": fcf_latest,
        "fcf_to_ebitda": fcf_to_ebitda,
        "quarters": quarters,
        "stype_alt": alt_stype if quarters_alt else None,
        "quarters_alt": quarters_alt,
    }


# ══════════════════════════════════════════════════════════════
# RICH SUMMARY ENTRY  (schema 2 → fundamentals_rich.json, Oct 2026)
# ══════════════════════════════════════════════════════════════
# Built from the stored per-symbol object only, so it can be rebuilt for
# the whole universe with reprocess_existing / backfill_summary — no FinEdge
# calls. Written to its own file; fundamentals_summary.json and its keys are
# left exactly as before so nothing that reads it today changes.
#
# Units in this file:
#   money        → ₹ crore, 2 decimals
#   per-share    → ₹ (eps, book_value_ps)
#   ratios/growth/margins → percent (29.3 means 29.3%), 2 decimals
#   *_days       → days;  current/quick/de/turnover → plain ratio (x)
#   period_end   → int YYYYMMDD;  period → "Mar 2026"
# Every list is newest first.

RICH_FILE = "fundamentals_rich.json"
RICH_SCHEMA = 2
RICH_QUARTERS = QUARTERLY_CAP   # 12 — what the pipeline stores
RICH_YEARS = 5


def _cr(v):
    return round(v / 1e7, 2) if isinstance(v, (int, float)) else None


def _pct(v):
    """fraction → percent"""
    return round(v * 100, 2) if isinstance(v, (int, float)) else None


def _r2(v):
    return round(v, 2) if isinstance(v, (int, float)) else None


def _div(a, b):
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)) or b == 0:
        return None
    return a / b


def _growth(cur, prev):
    """% change; None when the base is missing, zero or negative-to-anything
    would give a meaningless sign."""
    if not isinstance(cur, (int, float)) or not isinstance(prev, (int, float)) or prev == 0:
        return None
    return round((cur - prev) / abs(prev) * 100, 2)


def _cagr(end, start, years):
    if not isinstance(end, (int, float)) or not isinstance(start, (int, float)) or years <= 0:
        return None
    if start <= 0 or end <= 0:
        return None
    return round(((end / start) ** (1 / years) - 1) * 100, 2)


def _month_index(period_end):
    try:
        pe = int(period_end)
    except (TypeError, ValueError):
        return None
    return (pe // 10000) * 12 + (pe // 100) % 100


def _fmt_date(yyyymmdd):
    try:
        s = str(int(yyyymmdd))
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 else None
    except (TypeError, ValueError):
        return None


def _pick_basis(pl):
    """Consolidated unless it is missing or its latest quarter is more than
    one quarter behind standalone's — then standalone."""
    c_core = pl.get("quarterly", {}).get("c", {}).get("core") or []
    s_core = pl.get("quarterly", {}).get("s", {}).get("core") or []
    if not c_core and not s_core:
        return None, "no_quarterly_data"
    if not s_core:
        return "c", "only_consolidated"
    if not c_core:
        return "s", "only_standalone"
    c_m = _month_index(c_core[0].get("period_end"))
    s_m = _month_index(s_core[0].get("period_end"))
    if c_m is None:
        return "s", "consolidated_undated"
    if s_m is None or c_m >= s_m - 3:
        return "c", "consolidated"
    return "s", "consolidated_stale"


def _pl_rows(pl, period, basis):
    entry = pl.get(period, {}).get(basis, {}) if basis else {}
    return entry.get("core") or [], entry.get("raw") or []


def _rich_pl_row(row, kind):
    bank = kind == "bank"
    pat = row.get("pat")
    out = {
        "period": _fmt_period_end(row.get("period_end")),
        "period_end": row.get("period_end"),
        "sales": _cr(row.get("sales")),
        "ebitda": None if bank else _cr(_compute_ebitda_abs(row)),
        "opm": None if bank else _pct(_compute_opm(row)),
        "other_income": _cr(row.get("other_income")),
        "interest": _cr(row.get("interest_expended") if bank else row.get("finance_costs")),
        "depreciation": _cr(row.get("depreciation")),
        "exceptional": _cr(row.get("exceptional_items")),
        "pbt": _cr(row.get("pbt")),
        "tax": _cr(row.get("tax_expense")),
        "pat": _cr(pat),
        "pat_attr": _cr(row.get("pat_attributable") if row.get("pat_attributable") not in (None, 0) else pat),
        "eps": _r2(row.get("eps")),
        "npm": _pct(_div(pat, row.get("sales"))),
    }
    if bank:
        out.update({
            "nii": _cr(row.get("net_interest_income")),
            "provisions": _cr(row.get("provisions_loan_loss")),
            # pre-provision operating profit
            "ppop": _cr(row["pbt"] + (row.get("provisions_loan_loss") or 0)) if row.get("pbt") is not None else None,
            # FinEdge gives these as fractions and pads them with 0 when not
            # reported (always 0 in consolidated) — 0 is treated as missing.
            "gnpa_pct": _pct(row.get("gross_npa_pct") or None),
            "nnpa_pct": _pct(row.get("npa_pct") or None),
            "cet1": _pct(row.get("cet1_ratio") or None),
        })
    return out


_BANK_ASSET_QUALITY = (("gnpa_pct", "gross_npa_pct"), ("nnpa_pct", "npa_pct"), ("cet1", "cet1_ratio"))


def _fill_bank_asset_quality(rows, pl, period, basis):
    """Banks report NPA % and CET1 only in standalone results. When the
    chosen basis is consolidated, borrow them from the standalone row of the
    same period."""
    other = "s" if basis == "c" else "c"
    alt = {r.get("period_end"): r for r in (pl.get(period, {}).get(other, {}).get("core") or [])}
    for r in rows:
        src = alt.get(r.get("period_end"))
        if not src:
            continue
        for key, field in _BANK_ASSET_QUALITY:
            if r.get(key) is None:
                r[key] = _pct(src.get(field) or None)
    return rows


def _add_yoy(rows, keys=("sales", "ebitda", "pat", "eps")):
    """YoY matched on the period_end one year earlier — never on list
    position, so a missing quarter can't shift the comparison."""
    by_end = {r.get("period_end"): r for r in rows if r.get("period_end")}
    for r in rows:
        pe = r.get("period_end")
        prev = by_end.get(pe - 10000) if isinstance(pe, int) else None
        for k in keys:
            r[f"{k}_yoy"] = _growth(r.get(k), prev.get(k)) if prev else None
    return rows


def _row_by_end(rows, period_end):
    return next((r for r in rows if r.get("period_end") == period_end), None)


def _ratio_row(ratios, rtype, basis, header):
    block = ratios.get(rtype, {}) or {}
    rows = block.get(f"raw_{basis}") or block.get("raw") or []
    return next((r for r in rows if r.get("header") == header), {}) or {}


def _build_rich_entry(sym, obj, classification=None):
    obj = obj or {}
    profile = obj.get("profile") or {}
    classification = classification or {}
    kind = obj.get("company_type") or _classify_company(profile)
    pl = obj.get("pl") or {}
    bs = obj.get("bs") or {}
    cf = obj.get("cf") or {}
    ratios = obj.get("ratios") or {}
    basic = obj.get("basic_financials") or {}
    warnings = []

    basis, basis_reason = _pick_basis(pl)

    # ── P&L: quarterly / TTM / annual, all on the chosen basis ──────────
    q_core, q_raw = _pl_rows(pl, "quarterly", basis)
    quarters = _add_yoy([_rich_pl_row(r, kind) for r in q_core[:RICH_QUARTERS]])
    if len(quarters) >= 2:
        quarters[0]["sales_qoq"] = _growth(quarters[0]["sales"], quarters[1]["sales"])
        quarters[0]["pat_qoq"] = _growth(quarters[0]["pat"], quarters[1]["pat"])
    result_date = _fmt_date(q_raw[0].get("result_date")) if q_raw else None

    t_core, _ = _pl_rows(pl, "ttm", basis)
    ttm = _rich_pl_row(t_core[0], kind) if t_core else None
    if ttm and quarters and ttm.get("period_end") != quarters[0].get("period_end"):
        warnings.append("ttm_not_on_latest_quarter")

    a_core_all, _ = _pl_rows(pl, "annual", basis)
    annual_all = _add_yoy([_rich_pl_row(r, kind) for r in a_core_all], keys=("sales", "pat", "eps"))
    if kind == "bank":
        _fill_bank_asset_quality(quarters, pl, "quarterly", basis)
        _fill_bank_asset_quality(annual_all, pl, "annual", basis)
    bf_rows = basic.get(basis) or []
    for r in annual_all:
        bf = next((b for b in bf_rows if b.get("header") == r["period"]), None)
        r["dividend_payout"] = _pct(bf.get("dividendPayout")) if bf else None
    annual = annual_all[:RICH_YEARS]

    # ── Balance sheet / cash flow (pipeline stores one basis for these) ──
    bs_basis = (bs.get("annual") or {}).get("stype_used")
    cf_basis = (cf.get("annual") or {}).get("stype_used")
    if bs_basis and basis and bs_basis != basis:
        warnings.append(f"bs_basis_{bs_basis}_vs_pl_{basis}")
    if cf_basis and basis and cf_basis != basis:
        warnings.append(f"cf_basis_{cf_basis}_vs_pl_{basis}")

    bs_core = (bs.get("annual") or {}).get("core") or []
    bs_annual = [{
        "period": _fmt_period_end(r.get("period_end")),
        "period_end": r.get("period_end"),
        "equity_capital": _cr(r.get("equity_capital")),
        "reserves": _cr(r.get("reserves")),
        "shareholders_equity": _cr((r.get("equity_capital") or 0) + (r.get("reserves") or 0))
            if (r.get("equity_capital") is not None or r.get("reserves") is not None) else None,
        "total_equity": _cr(r.get("total_equity")),
        "borrowings": _cr(r.get("borrowings_total")),
        "cash": _cr(r.get("cash")),
        "bank_balances": _cr(r.get("bank_balances")),
        "investments": _cr(r.get("investments")),
        "investments_current": _cr(r.get("investments_current")),
        "fixed_assets": _cr(r.get("fixed_assets")),
        "cwip": _cr(r.get("cwip")),
        "total_assets": _cr(r.get("total_assets")),
        "current_assets": _cr(r.get("current_assets")),
        "current_liabilities": _cr(r.get("current_liabilities")),
        "inventories": _cr(r.get("inventories")),
        "receivables": _cr(r.get("trade_receivables_total")),
        "payables": _cr(r.get("trade_payables_total")),
        "deposits": _cr(r.get("deposits")),
        "advances": _cr(r.get("advances")),
    } for r in bs_core[:RICH_YEARS]]

    cf_core = (cf.get("annual") or {}).get("core") or []
    cf_annual = [{
        "period": _fmt_period_end(r.get("period_end")),
        "period_end": r.get("period_end"),
        "cfo": _cr(r.get("cfo")),
        "cfi": _cr(r.get("cfi")),
        "cff": _cr(r.get("cff")),
        "capex": _cr(-abs(r["capex"]) if isinstance(r.get("capex"), (int, float)) else None),
        "fcf": _cr(r.get("fcf")),
        "dividends_paid": _cr(r.get("dividends_paid")),
    } for r in cf_core[:RICH_YEARS]]

    # ── Ratios (screener.in convention, computed from raw statements) ────
    diluted = next((r.get("diluted_shares") for r in q_core if r.get("diluted_shares")), None)
    fy = a_core_all[0] if a_core_all else {}
    fy_end = fy.get("period_end")
    bs_fy = _row_by_end(bs_core, fy_end) if fy_end else None
    bs_prev = _row_by_end(bs_core, fy_end - 10000) if isinstance(fy_end, int) else None
    if fy_end and not bs_fy:
        warnings.append("no_bs_for_latest_fy")

    # Matched against screener.in on RELIANCE FY26 (ROE 8.91, ROCE 10.3,
    # BV 668): all on CLOSING balances of the FY, and
    #   shareholders' equity = equity capital + reserves (no minority)
    #   ROE  = PAT attributable to owners / shareholders' equity
    #   ROCE = (PBT + finance cost) / (total equity incl. minority + borrowings)
    #   BV   = shareholders' equity / diluted shares
    #   D/E  = borrowings / shareholders' equity
    def sh_eq(r):
        if not r:
            return None
        cap, res = r.get("equity_capital"), r.get("reserves")
        if cap is None and res is None:
            return r.get("total_equity")
        return (cap or 0) + (res or 0)

    def cap_employed(r):
        if not r or r.get("total_equity") is None:
            return None
        return r.get("total_equity") + (r.get("borrowings_total") or 0)

    fy_pat = fy.get("pat")
    fy_pat_attr = fy.get("pat_attributable") if fy.get("pat_attributable") not in (None, 0) else fy_pat
    ebit = (fy.get("pbt") + (fy.get("finance_costs") or 0)) if fy.get("pbt") is not None else None
    roe = _pct(_div(fy_pat_attr, sh_eq(bs_fy)))
    roce = None if kind == "bank" else _pct(_div(ebit, cap_employed(bs_fy)))
    roa = _pct(_div(fy_pat, bs_fy.get("total_assets") if bs_fy else None))

    bs_last = bs_core[0] if bs_core else {}
    debt = bs_last.get("borrowings_total")
    equity = sh_eq(bs_last)
    liquid = sum(v for v in (bs_last.get("cash"), bs_last.get("bank_balances"), bs_last.get("investments_current"))
                 if isinstance(v, (int, float)))
    net_debt = (debt - liquid) if isinstance(debt, (int, float)) else None

    cf_fy = _row_by_end(cf_core, fy_end) if fy_end else None
    fy_ebitda = None if kind == "bank" else _compute_ebitda_abs(fy)
    li = _ratio_row(ratios, "li", basis, "TTM")
    ef = _ratio_row(ratios, "ef", basis, "TTM")

    ratio_block = {
        "fy": _fmt_period_end(fy_end) if fy_end else None,
        "roe": roe,
        "roce": roce,
        "roa": roa,
        "opm_ttm": ttm.get("opm") if ttm else None,
        "npm_ttm": ttm.get("npm") if ttm else None,
        "de": _r2(_div(debt, equity)),
        "net_debt": _cr(net_debt),
        "book_value_ps": _r2(_div(equity, diluted)),
        "fcf_to_ebitda": _pct(_div(cf_fy.get("fcf"), fy_ebitda)) if cf_fy else None,
        "cfo_to_pat": _pct(_div(cf_fy.get("cfo"), fy_pat)) if cf_fy else None,
        "dividend_payout": annual[0].get("dividend_payout") if annual else None,
        "current_ratio": _r2(li.get("currentRatio")),
        "quick_ratio": _r2(li.get("quickRatio")),
        "interest_coverage": _r2(li.get("interestCoverage")),
        "debtor_days": _r2(ef.get("debtorDays")),
        "inventory_days": _r2(ef.get("inventoryDays")),
        "payable_days": _r2(ef.get("daysPayable")),
        "cash_conversion_cycle": _r2(ef.get("cashConversionCycle")),
        "working_capital_days": _r2(ef.get("workingCapitalDays")),
        "asset_turnover": _r2(ef.get("assetTurnover")),
    }
    if fy_end and cf_core and not cf_fy:
        warnings.append("no_cf_for_latest_fy")
    if kind == "bank":
        # Borrowings/deposits are a bank's raw material, not leverage, and
        # its cash flow is mostly lending — these would mislead a filter.
        for k in ("de", "net_debt", "fcf_to_ebitda", "cfo_to_pat", "current_ratio", "quick_ratio",
                  "interest_coverage", "debtor_days", "inventory_days", "payable_days",
                  "cash_conversion_cycle", "working_capital_days", "asset_turnover"):
            ratio_block[k] = None
        pr_ttm = _ratio_row(ratios, "pr", basis, "TTM")
        ratio_block["nim"] = _pct(pr_ttm.get("netInterestMargin"))
        latest = quarters[0] if quarters else {}
        ratio_block["gnpa_pct"] = latest.get("gnpa_pct")
        ratio_block["nnpa_pct"] = latest.get("nnpa_pct")
        ratio_block["cet1"] = latest.get("cet1")

    # ── Growth: CAGR from the annual P&L on the same basis ──────────────
    def cagr_of(key, years):
        if len(annual_all) <= years:
            return None
        end, start = annual_all[0], annual_all[years]
        if _month_index(end.get("period_end")) is None or _month_index(start.get("period_end")) is None:
            return None
        span = (_month_index(end["period_end"]) - _month_index(start["period_end"])) / 12
        if abs(span - years) > 0.25:      # a missing year — don't guess
            return None
        return _cagr(end.get(key), start.get(key), years)

    def bv_cagr(years):
        if not bs_core or len(bs_core) <= years:
            return None
        return _cagr(sh_eq(bs_core[0]), sh_eq(bs_core[years]), years)

    growth = {
        "sales_cagr_3y": cagr_of("sales", 3), "sales_cagr_5y": cagr_of("sales", 5),
        "ebitda_cagr_3y": cagr_of("ebitda", 3), "ebitda_cagr_5y": cagr_of("ebitda", 5),
        "pat_cagr_3y": cagr_of("pat", 3), "pat_cagr_5y": cagr_of("pat", 5),
        "eps_cagr_3y": cagr_of("eps", 3), "eps_cagr_5y": cagr_of("eps", 5),
        "bv_cagr_3y": bv_cagr(3), "bv_cagr_5y": bv_cagr(5),
    }

    return _finite({
        "symbol": sym,
        "name": profile.get("name"),
        "nse_code": profile.get("nse_code"),
        "bse_code": profile.get("bse_code"),
        "exchange": classification.get("exchange"),
        "sector": profile.get("sector"),
        "industry": profile.get("industry"),
        "macro_sector": profile.get("macro_sector"),
        "sector_group": classification.get("sector_group"),
        "display_industry": classification.get("display_industry"),
        "company_type": kind,
        "basis": basis,
        "basis_reason": basis_reason,
        "bs_basis": bs_basis,
        "cf_basis": cf_basis,
        "diluted_shares": diluted,
        "last_result_date": result_date,
        "fetched_at": obj.get("fetched_at"),
        "ttm": ttm,
        "ratios": ratio_block,
        "growth": growth,
        "quarters": quarters,
        "annual": annual,
        "bs_annual": bs_annual,
        "cf_annual": cf_annual,
        "warnings": warnings,
    })


def _finite(o):
    """NaN/inf → None, so the file always parses in a browser."""
    if isinstance(o, float):
        return o if o == o and o not in (float("inf"), float("-inf")) else None
    if isinstance(o, dict):
        return {k: _finite(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_finite(v) for v in o]
    return o


# ══════════════════════════════════════════════════════════════
# ASSEMBLE PER-SYMBOL OBJECT
# ══════════════════════════════════════════════════════════════

async def fetch_one_symbol(client, sem, sym, classification_lookup=None):
    # "sym" is both the Finedge query symbol AND the storage/output key —
    # for a BSE-exclusive stock this is the numeric BSE code, which is what
    # Finedge itself expects (confirmed — no trading_symbol substitution).
    classification = (classification_lookup or {}).get(sym)

    profile = await _fetch_profile_raw(client, sem, sym)
    company_type = _classify_company(profile)

    pl, basic, bs, cf, ratios, growth, price_ratios, segment_revenue = await asyncio.gather(
        _fetch_financials_dual(client, sem, sym, "pl", PL_PERIODS, build_core_fn=lambda row: _build_pl_core(row, company_type)),
        _fetch_basic_financials_dual(client, sem, sym),
        _fetch_financials_single(client, sem, sym, "bs", BS_PERIODS, build_core_fn=_build_bs_core),
        _fetch_financials_single(client, sem, sym, "cf", CF_PERIODS, build_core_fn=_build_cf_core, keep_raw=True),
        _fetch_ratios_single(client, sem, sym),
        _fetch_growth_metrics_single(client, sem, sym),
        _fetch_annual_price_ratios_single(client, sem, sym),
        _fetch_segment_revenue_dual(client, sem, sym),
    )

    obj = {
        "symbol": sym,
        "company_type": company_type,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "profile": profile,
        "pl": pl,
        "basic_financials": basic,
        "bs": bs,
        "cf": cf,
        "ratios": ratios,
        "growth_metrics": growth,
        "annual_price_ratios": price_ratios,
        "segment_revenue": segment_revenue,
    }
    summary_entry = _build_summary_entry(sym, profile, pl, ratios, price_ratios, cf, classification)
    return sym, obj, summary_entry


# ══════════════════════════════════════════════════════════════
# MODE: full / full_1..10
# ══════════════════════════════════════════════════════════════

async def run_full(part=0, exchange_filter=None):
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client, exchange_filter=exchange_filter)
        classification_lookup = await get_classification_lookup(client)
        label_prefix = exchange_filter.strip().upper() + " " if exchange_filter else ""
        if part == 0:
            chunk, label = symbols, f"{label_prefix}Full"
        else:
            part_size = (len(symbols) + TOTAL_PARTS - 1) // TOTAL_PARTS
            start, end = (part - 1) * part_size, part * part_size
            chunk, label = symbols[start:end], f"{label_prefix}Part {part}/{TOTAL_PARTS}"
        log.info(f"━━━ Fundamentals Full {label}  ({len(chunk)} stocks) ━━━")

        summary = await r2_download_summary(client)
        rich = await r2_download_rich(client)

        ok = failed = 0
        for i, sym in enumerate(chunk, 1):
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym, classification_lookup)
                file_hash = await r2_upload_symbol(client, sym, obj)
                summ["hash"] = file_hash
                summary[sym] = summ
                rich[sym] = {**_build_rich_entry(sym, obj, classification_lookup.get(sym)), "hash": file_hash}
                ok += 1
            except Exception as e:
                failed += 1
                log.warning(f"  ✗ {sym}: {e}")
            if i % 25 == 0 or i == len(chunk):
                log.info(f"  {i}/{len(chunk)}  ✓{ok}  ✗{failed}")
                await r2_upload_summary(client, summary)
                await r2_upload_rich(client, rich)
    log.info(f"━━━ Fundamentals Full {label} complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# MODE: daily
# ══════════════════════════════════════════════════════════════

async def run_daily():
    today = today_ist()

    # FIX (Aug 2026): guard against non-trading days — was defined but never
    # wired in, so weekend/NSE-holiday cron triggers would still burn a
    # fetch_one_symbol() call per carried-over pending symbol for nothing.
    if not is_trading_day(today):
        log.info(f"{today} is not a trading day — skipping"); return

    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        classification_lookup = await get_classification_lookup(client)
        summary = await r2_download_summary(client)
        rich = await r2_download_rich(client)
        pending = await r2_download_pending(client)

        todays_new = await get_today_result_symbols(client, sem, symbols)
        for sym in todays_new:
            if sym not in pending:
                prev = summary.get(sym)
                last_known = (prev.get("quarters") or [{}])[0].get("header") if prev else None
                pending[sym] = {
                    "last_known_quarter": last_known,
                    "attempts": 0,
                    "first_seen": today,
                }

        # FIX (Aug 2026): FinEdge picks up a company's result late night or
        # the next day, not same-day — checking a symbol the moment it's
        # added to pending (first_seen == today) almost always burns a full
        # fetch_one_symbol() call for a guaranteed no-op. Skip those; they
        # get their first real check on the next daily run, where attempts
        # starts counting from 1 as before.
        skipped_same_day = [s for s, p in pending.items() if p["first_seen"] == today]
        check_list = [s for s, p in pending.items() if p["first_seen"] != today]
        if not check_list:
            if skipped_same_day:
                log.info(f"No symbols to check yet — {len(skipped_same_day)} added today, "
                          f"first check will be on the next daily run")
            else:
                log.info("No pending result symbols to check — exiting")
            await r2_upload_pending(client, pending)
            return

        log.info(f"━━━ Fundamentals Daily {today} — checking {len(check_list)} symbols "
                  f"({len(skipped_same_day)} new today deferred to next run, "
                  f"{len(check_list)} carried over from earlier) ━━━")

        updated = still_pending = dropped = failed = 0
        for sym in check_list:
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym, classification_lookup)
                new_q = (summ.get("quarters") or [{}])[0].get("header")
                old_q = pending[sym]["last_known_quarter"]

                if new_q and new_q != old_q:
                    file_hash = await r2_upload_symbol(client, sym, obj)
                    summ["hash"] = file_hash
                    summary[sym] = summ
                    rich[sym] = {**_build_rich_entry(sym, obj, classification_lookup.get(sym)), "hash": file_hash}
                    del pending[sym]
                    updated += 1
                    log.info(f"  ✓ {sym} updated ({old_q} → {new_q})")
                else:
                    pending[sym]["attempts"] += 1
                    if pending[sym]["attempts"] >= MAX_PENDING_ATTEMPTS:
                        log.warning(f"  ⚠ {sym}: {MAX_PENDING_ATTEMPTS} attempts, still no new quarter — dropping "
                                    f"(will self-heal on next full run)")
                        del pending[sym]
                        dropped += 1
                    else:
                        still_pending += 1
                        log.info(f"  ⏳ {sym}: no new quarter yet "
                                 f"(attempt {pending[sym]['attempts']}/{MAX_PENDING_ATTEMPTS})")
            except Exception as e:
                failed += 1
                log.warning(f"  ✗ {sym}: {e}")

        await r2_upload_summary(client, summary)
        if updated:
            await r2_upload_rich(client, rich)
        await r2_upload_pending(client, pending)

    log.info(f"━━━ Daily complete — updated:{updated}  still_pending:{still_pending}  "
             f"dropped:{dropped}  failed:{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# MODE: local
# ══════════════════════════════════════════════════════════════

async def run_local(symbols):
    sem = asyncio.Semaphore(4)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[fetch_one_symbol(client, sem, sym.upper()) for sym in symbols])
    data = {sym: obj for sym, obj, _ in results}
    summary = {sym: summ for sym, _, summ in results}
    rich = {sym: _build_rich_entry(sym, obj) for sym, obj, _ in results}
    rich_path = OUT_DIR / "fundamentals_rich_local.json"
    rich_path.write_text(json.dumps(rich, indent=2, ensure_ascii=False))
    log.info(f"💾 Saved locally → {rich_path}  ({rich_path.stat().st_size/1024:.1f} KB)")
    out_path = OUT_DIR / "fundamentals_prod.json"
    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    summary_path = OUT_DIR / "fundamentals_summary_local.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    log.info(f"💾 Saved locally → {out_path}  ({out_path.stat().st_size/1024:.1f} KB)")
    log.info(f"💾 Saved locally → {summary_path}  ({summary_path.stat().st_size/1024:.1f} KB)")


# ══════════════════════════════════════════════════════════════
# MODE: backfill_summary
# ══════════════════════════════════════════════════════════════

async def _backfill_one(client, sym, classification_lookup=None):
    obj = await r2_download(client, f"fundamentals_full/{sym}.json")
    if not obj:
        return sym, None, None
    classification = (classification_lookup or {}).get(sym)
    summ = _build_summary_entry(sym, obj.get("profile"), obj.get("pl", {}),
                                 obj.get("ratios", {}), obj.get("annual_price_ratios", {}),
                                 obj.get("cf", {}), classification)
    summ["hash"] = compute_hash(json.dumps(obj, separators=(",", ":")))
    rich_entry = {**_build_rich_entry(sym, obj, classification), "hash": summ["hash"]}
    return sym, summ, rich_entry


async def run_backfill_summary():
    BATCH = 50
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        classification_lookup = await get_classification_lookup(client)
        summary = {}
        rich = {}
        ok = failed = 0
        failed_syms = []
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i + BATCH]
            results = await asyncio.gather(*[_backfill_one(client, sym, classification_lookup) for sym in batch])
            for sym, summ, rich_entry in results:
                if summ:
                    summary[sym] = summ
                    rich[sym] = rich_entry
                    ok += 1
                else:
                    failed += 1
                    failed_syms.append(sym)
                    log.warning(f"  ✗ {sym}: not found in R2 (run full for this symbol first)")
            done = min(i + BATCH, len(symbols))
            log.info(f"  {done}/{len(symbols)}  ✓{ok}  ✗{failed}")
            await r2_upload_summary(client, summary)
            await r2_upload_rich(client, rich)
        if failed_syms:
            log.info(f"  Auto-syncing {len(failed_syms)} missing symbols via Finedge...")
            sem = asyncio.Semaphore(CONCURRENCY)
            s_ok = s_fail = 0
            for sym in failed_syms:
                try:
                    _, obj, summ = await fetch_one_symbol(client, sem, sym, classification_lookup)
                    file_hash = await r2_upload_symbol(client, sym, obj)
                    summ["hash"] = file_hash
                    summary[sym] = summ
                    rich[sym] = {**_build_rich_entry(sym, obj, classification_lookup.get(sym)), "hash": file_hash}
                    ok += 1; failed -= 1; s_ok += 1
                    log.info(f"  ✓ {sym} (auto-synced)")
                except Exception as e:
                    s_fail += 1
                    log.error(f"  ✗ {sym} auto-sync failed: {e}")
            await r2_upload_summary(client, summary)
            await r2_upload_rich(client, rich)
            log.info(f"  Auto-sync done — ✓{s_ok}  ✗{s_fail}")
    log.info(f"━━━ Summary backfill complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# MODE: sync
# ══════════════════════════════════════════════════════════════

async def run_sync(symbols):
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        classification_lookup = await get_classification_lookup(client)
        summary = await r2_download_summary(client)
        rich = await r2_download_rich(client)
        ok = failed = 0
        for sym in symbols:
            sym = sym.upper()
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym, classification_lookup)
                file_hash = await r2_upload_symbol(client, sym, obj)
                summ["hash"] = file_hash
                summary[sym] = summ
                rich[sym] = {**_build_rich_entry(sym, obj, classification_lookup.get(sym)), "hash": file_hash}
                ok += 1
                log.info(f"  ✓ {sym}")
            except Exception as e:
                failed += 1
                log.warning(f"  ✗ {sym}: {e}")
        await r2_upload_summary(client, summary)
        if ok:
            await r2_upload_rich(client, rich)
    log.info(f"━━━ Sync complete — ✓{ok}  ✗{failed} ━━━")


async def run_reprocess_existing(symbols=None):
    """Rebuild stored raw statements and summary without FinEdge calls.

    Legacy CF rows lacking raw data remain unchanged; sync those symbols
    separately to populate the newly added cash-flow components.
    """
    async with httpx.AsyncClient() as client:
        summary = await r2_download_summary(client)
        rich = await r2_download_rich(client)
        lookup = await get_classification_lookup(client)
        selected = symbols or await get_nse_universe(client)
        ok = failed = legacy_cf = 0
        for sym in selected:
            sym = sym.upper()
            try:
                obj = await r2_download(client, f"fundamentals_full/{sym}.json")
                if not obj:
                    failed += 1
                    continue
                kind = obj.get("company_type") or _classify_company(obj.get("profile"))
                for period, bases in obj.get("pl", {}).items():
                    if not isinstance(bases, dict):
                        continue
                    for basis, entry in bases.items():
                        if isinstance(entry, dict) and entry.get("raw"):
                            entry["core"] = [_build_pl_core(r, kind) for r in entry["raw"]]
                for section, builder in (("bs", _build_bs_core), ("cf", _build_cf_core)):
                    for entry in obj.get(section, {}).values():
                        if not isinstance(entry, dict):
                            continue
                        if entry.get("raw"):
                            entry["core"] = [builder(r) for r in entry["raw"]]
                        elif section == "cf" and entry.get("core"):
                            legacy_cf += 1
                # Preserve filing-verified overrides if present in the stored object.
                for correction in obj.get("verification", {}).get("corrections", []):
                    rows = obj.get("pl", {}).get(correction["period"], {}).get(correction["basis"], {}).get("core", [])
                    for row in rows:
                        if row.get("period_end") == correction.get("period_end"):
                            row[correction["field"]] = correction["corrected_value"]
                            if kind == "bank":
                                row["sales"] = row.get("interest_earned")
                                if row.get("interest_earned") is not None and row.get("interest_expended") is not None:
                                    row["net_interest_income"] = row["interest_earned"] - row["interest_expended"]
                summ = _build_summary_entry(sym, obj.get("profile"), obj.get("pl", {}),
                    obj.get("ratios", {}), obj.get("annual_price_ratios", {}),
                    obj.get("cf", {}), lookup.get(sym))
                summ["hash"] = await r2_upload_symbol(client, sym, obj)
                summary[sym] = summ
                rich[sym] = {**_build_rich_entry(sym, obj, lookup.get(sym)), "hash": summ["hash"]}
                ok += 1
            except Exception as exc:
                failed += 1
                log.warning("Reprocess %s failed (%s)", sym, type(exc).__name__)
        if ok:
            await r2_upload_summary(client, summary)
            await r2_upload_rich(client, rich)
        log.info("Reprocessed:%s failed:%s legacy CF periods needing refresh:%s", ok, failed, legacy_cf)
        if failed:
            raise RuntimeError("Some stored symbols could not be reprocessed")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "full":
        asyncio.run(run_full(0))
    elif mode.startswith("full_") and mode.split("_")[1].isdigit():
        asyncio.run(run_full(int(mode.split("_")[1])))
    elif mode == "bse":
        asyncio.run(run_full(0, exchange_filter="BSE"))
    elif mode == "daily":
        asyncio.run(run_daily())
    elif mode == "reprocess_existing":
        asyncio.run(run_reprocess_existing(sys.argv[2:] or None))
    elif mode == "backfill_summary":
        asyncio.run(run_backfill_summary())
    elif mode == "sync":
        syms = " ".join(sys.argv[2:]).split()
        if not syms:
            print("Usage: python pipeline_fundamentals_prod.py sync SYM [SYM2 ...]")
            sys.exit(1)
        asyncio.run(run_sync(syms))
    elif mode == "local":
        syms = sys.argv[2:]
        if not syms:
            print("Usage: python pipeline_fundamentals_prod.py local SYM [SYM2 ...]")
            sys.exit(1)
        asyncio.run(run_local(syms))
    else:
        print(__doc__)
        sys.exit(1)
