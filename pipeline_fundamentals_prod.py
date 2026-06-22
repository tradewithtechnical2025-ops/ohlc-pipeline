#!/usr/bin/env python3
"""
Finedge Fundamentals — PRODUCTION pipeline (v3, R2-wired)
============================================================
Standalone pipeline — does NOT touch pipeline.py or fundamentals.json.
Writes to R2 as PER-SYMBOL files: fundamentals_full/{SYMBOL}.json
(not one combined blob — keeps per-page load light, see size discussion).

ALSO maintains one lightweight combined file: fundamentals_summary.json
(all stocks, ~15 fields each + last 5 quarters' PL) — feeds the frontend's
Results Comparison and Peer Comparison features, which genuinely need
many stocks at once. Built from data already fetched for the per-symbol
file — no extra API calls. market_cap is deliberately NOT included here
(it's price-driven, changes daily for every stock — frontend computes it
live as diluted_shares × ltp from screener_feed.json).

Stock universe: read from classification.json (already in R2, same source
pipeline.py uses for sector/industry), filtered to NSE equities, ETFs excluded
(same ETF filter pattern as run_fund_full in pipeline.py).

Modes:
  python pipeline_fundamentals_prod.py full          → all NSE stocks
  python pipeline_fundamentals_prod.py full_1         (1..10)
                                                       → 1/10th of universe each,
                                                         for chunked GitHub Actions runs
  python pipeline_fundamentals_prod.py daily          → ONLY stocks with a result
                                                         today (via Finedge
                                                         results-calendar), updates
                                                         just those per-symbol files
  python pipeline_fundamentals_prod.py backfill_summary
                                                       → ONE-TIME: builds
                                                         fundamentals_summary.json
                                                         from per-symbol files
                                                         already in R2 — NO Finedge
                                                         calls, just R2 reads. Use
                                                         this if full_1..10 already
                                                         ran before summary support
                                                         was added.
  python pipeline_fundamentals_prod.py local SYM SYM  → local-only test (no R2),
                                                         saves to output/fundamentals_prod.json

Data-shape decisions (locked in from testing phase):
  - PL  periods : annual (full), quarterly (last 12), ttm (1 row) — BOTH stypes (c+s)
  - basic_financials — BOTH stypes (c+s)
    (PL + basic_financials need dual fetch — CET1/NPA-type fields are populated
     ONLY in standalone, even when consolidated rows exist, so a c→s fallback
     would never trigger; we need both, always, for these two.)
  - BS  periods : annual (full), quarterly (last 12) — SINGLE stype (c, fallback s)
  - CF  periods : annual (full), quarterly (last 12), ytd (last 12) — SINGLE stype,
    CORE-ONLY (cfo/cfi/cff/net_cash_flow/capex/fcf/dividends_paid/pbt) —
    raw ~100-field granular rows dropped entirely for size.
  - ratios/growth_metrics/annual_price_ratios — SINGLE stype (c, fallback s)
  - shareholdings/pattern — REMOVED (not needed right now)
  - PL/BS get an alias-resolved "core" object (bank vs non-bank field-naming)
    PLUS full "raw" rows (schemas not yet mapped, e.g. insurance, still captured).
"""

import asyncio, json, logging, os, sys
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


def is_trading_day(d: str) -> bool:
    # weekday check only — holiday file optional, not bundled with this standalone pipeline
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
    # Worker GET endpoint routes by URL path, NOT by ?file= query param.
    # quote(filename, safe='/') correctly encodes & → %26 while leaving / intact,
    # so M&M.json → fundamentals_full/M%26M.json which the Worker decodes correctly.
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


async def r2_upload_symbol(client, sym, obj):
    payload = json.dumps(obj, separators=(",", ":"))
    await r2_upload(client, f"fundamentals_full/{sym}.json", payload)


SUMMARY_FILE = "fundamentals_summary.json"


async def r2_download_summary(client):
    d = await r2_download(client, SUMMARY_FILE)
    if isinstance(d, dict) and "stocks" in d:
        return d["stocks"]
    return {}


async def r2_upload_summary(client, stocks):
    payload = json.dumps({"updated": today_ist(), "stocks": stocks}, separators=(",", ":"))
    await r2_upload(client, SUMMARY_FILE, payload)


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
# CORE FIELD ALIAS RESOLUTION — PL, BS, CF
# (handles bank vs non-bank naming differences without branching;
#  degrades to None gracefully for schemas not yet mapped, e.g.
#  insurance — raw rows kept alongside for PL/BS so nothing is lost)
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
# FETCH — DUAL statement_type (always both c + s)
#   used for: PL, basic_financials
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
# FETCH — SINGLE statement_type, c preferred, fallback to s only
# if c returns zero rows
#   used for: BS, CF, ratios, growth_metrics, annual_price_ratios
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
        rows, stype_used = [], None
        for stype in ("c", "s"):
            d = await _finedge_get(client, sem, f"ratios/{sym}", {"statement_type": stype, "ratio_type": rtype})
            r = (d or {}).get("ratios", [])
            if r:
                rows, stype_used = r, stype
                break
        out[rtype] = {"stype_used": stype_used, "raw": rows}
    return out


async def _fetch_growth_metrics_single(client, sem, sym):
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"financial-metrics/{sym}", {"statement_type": stype, "ratio_type": "gr"})
        fm = (d or {}).get("financial_metrics")
        if fm:
            return {"stype_used": stype, "data": fm}
    return {"stype_used": None, "data": None}


async def _fetch_annual_price_ratios_single(client, sem, sym):
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"annual-price-ratios/{sym}", {"statement_type": stype})
        rows = (d or {}).get("price_ratios", [])
        if rows:
            return {"stype_used": stype, "raw": rows}
    return {"stype_used": None, "raw": []}


async def _fetch_profile_raw(client, sem, sym):
    return await _finedge_get(client, sem, f"company-profile/{sym}", {})


# ══════════════════════════════════════════════════════════════
# LIGHTWEIGHT SUMMARY ENTRY — built from data already fetched for
# the per-symbol file, no extra API calls. Feeds fundamentals_summary.json
# (Results Comparison + Peer Comparison on the frontend).
#
# NOTE: market_cap deliberately NOT stored here — it's price-driven and
# changes every trading day for every stock, while this pipeline only
# refreshes a stock on result-day or full bulk runs. Frontend computes
# live market_cap = diluted_shares × ltp (ltp from screener_feed.json,
# which IS updated daily for all stocks by the OHLC pipeline).
# ══════════════════════════════════════════════════════════════

def _build_summary_entry(sym, profile, pl, ratios, price_ratios):
    profile = profile or {}

    diluted_shares = None
    for period in ("ttm", "annual", "quarterly"):
        for stype in ("c", "s"):
            core = pl.get(period, {}).get(stype, {}).get("core", [])
            if core and core[0].get("diluted_shares") is not None:
                diluted_shares = core[0]["diluted_shares"]
                break
        if diluted_shares is not None:
            break

    q_core = (pl.get("quarterly", {}).get("c", {}).get("core")
              or pl.get("quarterly", {}).get("s", {}).get("core") or [])
    quarters = [{
        "header": _fmt_period_end(row.get("period_end")),
        "sales":    row.get("sales") if row.get("sales") is not None else row.get("interest_earned"),
        "expenses": row.get("expenses"),   # needed for OPM in Results Comparison
        "eps":      row.get("eps"),
        "pat":      row.get("pat"),
        "pbt":      row.get("pbt"),
    } for row in q_core[:9]]  # 9 so YoY base (idx+4) exists for any of the 5 displayed quarters

    pr0 = (ratios.get("pr", {}).get("raw") or [{}])[0]
    le0 = (ratios.get("le", {}).get("raw") or [{}])[0]
    apr0 = (price_ratios.get("raw") or [{}])[0]

    return {
        "symbol": sym,
        "name": profile.get("name"),
        "sector": profile.get("sector"),
        "industry": profile.get("industry"),
        "macro_sector": profile.get("macro_sector"),
        "diluted_shares": diluted_shares,
        "pe": apr0.get("pe"),
        "pb": apr0.get("pb"),
        "roe": pr0.get("returnOnEquity"),
        "roce": pr0.get("returnOnCapital"),
        "ebitda_margin": pr0.get("ebitdaMargin"),
        "de_ratio": le0.get("totalDebtToEquity"),
        "quarters": quarters,
    }


# ══════════════════════════════════════════════════════════════
# ASSEMBLE PER-SYMBOL OBJECT
# ══════════════════════════════════════════════════════════════

async def fetch_one_symbol(client, sem, sym):
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
    summary_entry = _build_summary_entry(sym, profile, pl, ratios, price_ratios)
    return sym, obj, summary_entry


# ══════════════════════════════════════════════════════════════
# MODE: full / full_1..10 — bulk universe, per-symbol R2 upload
# ══════════════════════════════════════════════════════════════

async def run_full(part=0):
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        if part == 0:
            chunk, label = symbols, "Full"
        else:
            part_size = (len(symbols) + TOTAL_PARTS - 1) // TOTAL_PARTS
            start, end = (part - 1) * part_size, part * part_size
            chunk, label = symbols[start:end], f"Part {part}/{TOTAL_PARTS}"
        log.info(f"━━━ Fundamentals Full {label}  ({len(chunk)} stocks) ━━━")

        # NOTE: if full_1..full_10 run truly simultaneously (parallel GitHub
        # Actions), there's a small race window on this shared summary file —
        # each part downloads-merges-uploads independently. Since parts touch
        # disjoint symbol sets and we checkpoint every 25 stocks (not just at
        # the end), the overwrite window is short, but for zero risk run the
        # parts sequentially rather than all-at-once.
        summary = await r2_download_summary(client)

        ok = failed = 0
        for i, sym in enumerate(chunk, 1):
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym)
                await r2_upload_symbol(client, sym, obj)
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
# MODE: daily — only stocks with a result today
# ══════════════════════════════════════════════════════════════

async def run_daily():
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} not a trading day"); return
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        result_symbols = await get_today_result_symbols(client, sem, symbols)
        if not result_symbols:
            log.info("No results today — exiting"); return
        log.info(f"━━━ Fundamentals Daily  {today}  ({len(result_symbols)} stocks) ━━━")
        summary = await r2_download_summary(client)
        ok = failed = 0
        for sym in result_symbols:
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym)
                await r2_upload_symbol(client, sym, obj)
                summary[sym] = summ
                ok += 1
                log.info(f"  ✓ {sym}")
            except Exception as e:
                failed += 1
                log.warning(f"  ✗ {sym}: {e}")
        await r2_upload_summary(client, summary)
    log.info(f"━━━ Fundamentals Daily complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# MODE: local — no R2, for quick testing
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
# MODE: backfill_summary — one-time, builds fundamentals_summary.json
# from per-symbol files ALREADY in R2 (fundamentals_full/{SYM}.json).
# No Finedge API calls at all — just R2 reads. Use this once if full_1..10
# already ran with an older pipeline version that didn't write the summary.
# ══════════════════════════════════════════════════════════════

async def _backfill_one(client, sym):
    obj = await r2_download(client, f"fundamentals_full/{sym}.json")
    if not obj:
        return sym, None
    summ = _build_summary_entry(sym, obj.get("profile"), obj.get("pl", {}),
                                 obj.get("ratios", {}), obj.get("annual_price_ratios", {}))
    return sym, summ


async def run_backfill_summary():
    BATCH = 50
    async with httpx.AsyncClient() as client:
        symbols = await get_nse_universe(client)
        summary = {}
        ok = failed = 0
        failed_syms = []
        for i in range(0, len(symbols), BATCH):
            batch = symbols[i:i + BATCH]
            results = await asyncio.gather(*[_backfill_one(client, sym) for sym in batch])
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
        # Auto-retry ALL symbols not found in R2 — could be new listings,
        # migrations, or & symbols where path routing failed.
        if failed_syms:
            log.info(f"  Auto-syncing {len(failed_syms)} missing symbols via Finedge...")
            sem = asyncio.Semaphore(CONCURRENCY)
            s_ok = s_fail = 0
            for sym in failed_syms:
                try:
                    _, obj, summ = await fetch_one_symbol(client, sem, sym)
                    await r2_upload_symbol(client, sym, obj)
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
# MODE: sync — retry specific symbols (e.g. ones that failed earlier
# due to the & URL-encoding bug), without rerunning a whole part.
# ══════════════════════════════════════════════════════════════

async def run_sync(symbols):
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        summary = await r2_download_summary(client)
        ok = failed = 0
        for sym in symbols:
            sym = sym.upper()
            try:
                _, obj, summ = await fetch_one_symbol(client, sem, sym)
                await r2_upload_symbol(client, sym, obj)
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
