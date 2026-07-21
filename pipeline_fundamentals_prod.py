#!/usr/bin/env python3
"""
Finedge Fundamentals — PRODUCTION pipeline (v3, R2-wired)
============================================================
(See module docstring history in repo — dual-track quarters added July 2026,
see notes near _build_summary_entry below.)
"""

import asyncio, hashlib, json, logging, os, sys
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


async def r2_download_pending(client):
    d = await r2_download(client, PENDING_FILE)
    return d if isinstance(d, dict) else {}


async def r2_upload_pending(client, pending):
    payload = json.dumps(pending, separators=(",", ":"))
    await r2_upload(client, PENDING_FILE, payload)


async def get_nse_universe(client):
    classification = await r2_download(client, "classification.json")
    if not classification or not isinstance(classification, list):
        raise RuntimeError("classification.json missing or invalid in R2!")
    symbols = []
    for s in classification:
        sym = str(s.get("symbol", "")).strip().upper()
        exch = str(s.get("exchange", "")).strip()
        if sym and exch == "NSE" and not _is_etf(sym):
            symbols.append(sym)
    symbols = sorted(set(symbols))
    log.info(f"Universe: {len(symbols)} NSE equity symbols (ETFs excluded)")
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

CORE_PL_ALIASES = {
    "year": ["year"], "period_end": ["period_end"], "period_start": ["period_start"],
    "sales": ["revenueFromOperations", "income"],
    "expenses": ["expenses", "expenditureExcludingProvisions"],
    "pbt": ["profitBeforeTax", "profitLossBeforeTax"],
    "pat": ["profitLossForPeriod", "profitLossForThePeriod"],
    "pat_attributable": ["profitOrLossAttributableToOwners"],
    "eps": ["eps"],
    "other_income": ["otherIncome"],
    "finance_costs": ["financeCosts"],
    "depreciation": ["depreciationAndAmortisation"],
    "employee_cost": ["employeeBenefitExpense", "employeesCost"],
    "exceptional_items": ["exceptionalItemsBeforeTax", "exceptionalItems"],
    "minority_interest": ["nonControllingInterests", "profitLossOfMinorityInterest"],
    "associates_share": ["profitOrLossOfAssociates", "profitLossOfAssociates"],
    "tax_expense": ["taxExpense"],
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
    "year": ["year"], "period_end": ["period_end"],
    "total_assets": ["assets"],
    "reserves": ["reserves"],
    "equity_capital": ["equityCapital", "capital"],
    "cash": ["cashAndCashEquivalents", "cashAndBalancesWithRBI"],
    "investments": ["investments", "noncurrentInvestments"],
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
    "year": ["year"], "period_end": ["period_end"], "period_start": ["period_start"],
    "cfo": ["cashFlowsFromOperatingActivities"],
    "cfi": ["cashFlowsFromInvestingActivities"],
    "cff": ["cashFlowsFromFinancingActivities"],
    "net_cash_flow": ["netCashFlow"],
    "capex": ["purchaseOfPPEClassifiedAsInvesting", "purchaseOfFixed&IntangibleAssets"],
    "dividends_paid": ["dividendsPaidClassifiedAsFinancing"],
    "interest_paid": ["interestPaidClassifiedAsFinancing"],
    "pbt": ["profitBeforeTax", "profitBeforeExtraordinaryItemsAndTax"],
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


def _build_cf_core(row):
    core = _resolve_aliases(row, CORE_CF_ALIASES)
    if core.get("cfo") is not None and core.get("capex") is not None:
        core["fcf"] = core["cfo"] + core["capex"]
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
    sales = row.get("sales")
    interest_earned = row.get("interest_earned")
    if not sales and interest_earned:
        ie = row.get("interest_expended") or 0
        return round((interest_earned - ie) / interest_earned, 4) if interest_earned else None
    if not sales:
        return None
    exp = row.get("expenses")
    if exp is not None:
        return round((sales - exp) / sales, 4)
    pbt  = row.get("pbt")
    dep  = row.get("depreciation")
    fin  = row.get("finance_costs")
    oth  = row.get("other_income") or 0
    if pbt is not None and dep is not None and fin is not None:
        op = pbt + dep + fin - oth
        return round(op / sales, 4)
    return None


def _build_quarters_list(q_core):
    """Builds the standard 'quarters' array shape from a stype's quarterly
    PL core rows. Factored out so both the primary and the alt (dual-track)
    stype can build the same shape without duplicating this logic."""
    return [{
        "header": _fmt_period_end(row.get("period_end")),
        "sales":    row.get("sales") if row.get("sales") is not None else row.get("interest_earned"),
        "expenses": row.get("expenses"),
        "opm":      _compute_opm(row),
        "eps":      row.get("eps"),
        "pat":      row.get("pat"),
        "pbt":      row.get("pbt"),
    } for row in q_core[:9]]


def _build_summary_entry(sym, profile, pl, ratios, price_ratios, classification=None):
    profile = profile or {}
    classification = classification or {}

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
    quarters = _build_quarters_list(q_core)

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
    quarters_alt = _build_quarters_list(alt_core) if alt_core else []

    pr_rows  = ratios.get("pr", {}).get(f"raw_{stype}") or ratios.get("pr", {}).get("raw") or []
    le_rows  = ratios.get("le", {}).get(f"raw_{stype}") or ratios.get("le", {}).get("raw") or []
    apr_rows = price_ratios.get(f"raw_{stype}") or price_ratios.get("raw") or []
    pr0  = (pr_rows or [{}])[0]
    le0  = (le_rows or [{}])[0]
    apr0 = (apr_rows or [{}])[0]

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
        "quarters": quarters,
        "stype_alt": alt_stype if quarters_alt else None,
        "quarters_alt": quarters_alt,
    }


# ══════════════════════════════════════════════════════════════
# ASSEMBLE PER-SYMBOL OBJECT
# ══════════════════════════════════════════════════════════════

async def fetch_one_symbol(client, sem, sym, classification_lookup=None):
    profile = await _fetch_profile_raw(client, sem, sym)
    company_type = _classify_company(profile)

    pl, basic, bs, cf, ratios, growth, price_ratios = await asyncio.gather(
        _fetch_financials_dual(client, sem, sym, "pl", PL_PERIODS, build_core_fn=_build_pl_core),
        _fetch_basic_financials_dual(client, sem, sym),
        _fetch_financials_single(client, sem, sym, "bs", BS_PERIODS, build_core_fn=_build_bs_core),
        _fetch_financials_single(client, sem, sym, "cf", CF_PERIODS, build_core_fn=_build_cf_core, keep_raw=False),
        _fetch_ratios_single(client, sem, sym),
        _fetch_growth_metrics_single(client, sem, sym),
        _fetch_annual_price_ratios_single(client, sem, sym),
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
    }
    classification = (classification_lookup or {}).get(sym)
    summary_entry = _build_summary_entry(sym, profile, pl, ratios, price_ratios, classification)
    return sym, obj, summary_entry


# ══════════════════════════════════════════════════════════════
# MODE: full / full_1..10
# ══════════════════════════════════════════════════════════════

async def run_full(part=0):
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        classification_lookup = await get_classification_lookup(client)
        if part == 0:
            chunk, label = symbols, "Full"
        else:
            part_size = (len(symbols) + TOTAL_PARTS - 1) // TOTAL_PARTS
            start, end = (part - 1) * part_size, part * part_size
            chunk, label = symbols[start:end], f"Part {part}/{TOTAL_PARTS}"
        log.info(f"━━━ Fundamentals Full {label}  ({len(chunk)} stocks) ━━━")

        summary = await r2_download_summary(client)

        ok = failed = 0
        for i, sym in enumerate(chunk, 1):
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym, classification_lookup)
                file_hash = await r2_upload_symbol(client, sym, obj)
                summ["hash"] = file_hash
                summary[sym] = summ
                ok += 1
            except Exception as e:
                failed += 1
                log.warning(f"  ✗ {sym}: {e}")
            if i % 25 == 0 or i == len(chunk):
                log.info(f"  {i}/{len(chunk)}  ✓{ok}  ✗{failed}")
                await r2_upload_summary(client, summary)
    log.info(f"━━━ Fundamentals Full {label} complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# MODE: daily
# ══════════════════════════════════════════════════════════════

async def run_daily():
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} not a trading day"); return

    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        classification_lookup = await get_classification_lookup(client)
        summary = await r2_download_summary(client)
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

        check_list = list(pending.keys())
        if not check_list:
            log.info("No pending result symbols to check — exiting"); return

        log.info(f"━━━ Fundamentals Daily {today} — checking {len(check_list)} symbols "
                  f"({len(todays_new)} new, {len(check_list) - len(todays_new)} carried over) ━━━")

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
        return sym, None
    classification = (classification_lookup or {}).get(sym)
    summ = _build_summary_entry(sym, obj.get("profile"), obj.get("pl", {}),
                                 obj.get("ratios", {}), obj.get("annual_price_ratios", {}), classification)
    summ["hash"] = compute_hash(json.dumps(obj, separators=(",", ":")))
    return sym, summ


async def run_backfill_summary():
    BATCH = 50
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        classification_lookup = await get_classification_lookup(client)
        summary = {}
        ok = failed = 0
        failed_syms = []
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i + BATCH]
            results = await asyncio.gather(*[_backfill_one(client, sym, classification_lookup) for sym in batch])
            for sym, summ in results:
                if summ:
                    summary[sym] = summ
                    ok += 1
                else:
                    failed += 1
                    failed_syms.append(sym)
                    log.warning(f"  ✗ {sym}: not found in R2 (run full for this symbol first)")
            done = min(i + BATCH, len(symbols))
            log.info(f"  {done}/{len(symbols)}  ✓{ok}  ✗{failed}")
            await r2_upload_summary(client, summary)
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
                    ok += 1; failed -= 1; s_ok += 1
                    log.info(f"  ✓ {sym} (auto-synced)")
                except Exception as e:
                    s_fail += 1
                    log.error(f"  ✗ {sym} auto-sync failed: {e}")
            await r2_upload_summary(client, summary)
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
        ok = failed = 0
        for sym in symbols:
            sym = sym.upper()
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym, classification_lookup)
                file_hash = await r2_upload_symbol(client, sym, obj)
                summ["hash"] = file_hash
                summary[sym] = summ
                ok += 1
                log.info(f"  ✓ {sym}")
            except Exception as e:
                failed += 1
                log.warning(f"  ✗ {sym}: {e}")
        await r2_upload_summary(client, summary)
    log.info(f"━━━ Sync complete — ✓{ok}  ✗{failed} ━━━")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "full":
        asyncio.run(run_full(0))
    elif mode.startswith("full_") and mode.split("_")[1].isdigit():
        asyncio.run(run_full(int(mode.split("_")[1])))
    elif mode == "daily":
        asyncio.run(run_daily())
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
