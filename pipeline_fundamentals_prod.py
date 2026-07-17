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
                                                         just those per-symbol files.
                                                         Also retries any symbol still
                                                         pending from a previous day
                                                         (see fundamentals_pending.json)
                                                         whose result-day data hasn't
                                                         landed on Finedge yet — Finedge
                                                         confirms 12-36h typical
                                                         turnaround, mostly same-day but
                                                         not guaranteed same-day.
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
  - ratios/annual_price_ratios — BOTH stypes fetched, RECENCY-PICKED (see note
    below) — was "SINGLE stype (c, fallback s only if c is empty)" until
    July 2026; changed for the same reason as the PL quarters fix.
  - growth_metrics — still SINGLE stype (c, fallback s only if c is empty),
    UNCHANGED. The financial-metrics endpoint returns one metrics object per
    stype, not a dated rows list, so there's no reliable per-row date field
    to compare recency against — revisit if Finedge exposes one.
  - shareholdings/pattern — REMOVED (not needed right now)
  - PL/BS get an alias-resolved "core" object (bank vs non-bank field-naming)
    PLUS full "raw" rows (schemas not yet mapped, e.g. insurance, still captured).

Daily-mode pending-retry (added post Finedge support reply, July 2026):
  Finedge confirmed there is no per-symbol data-refresh status field on
  results-calendar, and updates (PL + all derived ratios together) typically
  land 12-36h after announcement, "mostly same day" but not guaranteed.
  So run_daily() no longer treats a same-day fetch as final: it snapshots
  the last-known quarter header for every symbol with a result today, and
  only marks a symbol "done" once the fetched data's latest quarter header
  actually changes. Until then the symbol stays in fundamentals_pending.json
  and gets retried on every subsequent daily run, up to MAX_PENDING_ATTEMPTS
  trading days, after which it's dropped (self-heals on the next full run).

Recency-based stype selection (added July 2026, extended same month):
  _build_summary_entry() used to prefer consolidated whenever it had ANY
  rows at all — for PL quarters, and independently for each ratio field
  (PE/PB, ROE/ROCE/EBITDA, D/E) — falling back to standalone only if
  consolidated was completely empty. Many companies stop filing
  consolidated results after a few years while standalone keeps being
  reported every quarter, so "any consolidated rows" stayed true (the old
  rows are still there) and the summary kept surfacing stale consolidated
  data even though standalone was current.

  Fixed in two steps:
  1. _fetch_ratios_single / _fetch_annual_price_ratios_single now always
     fetch both stypes and keep both raw arrays (raw_c/raw_s) alongside the
     existing raw/stype_used pair, so a consumer can pick per its own needs
     instead of only seeing whichever one the fetch function guessed was best.
  2. _build_summary_entry() decides ONE overall stype per stock — from
     quarterly PL recency (_latest_key, by period_end or year), the
     highest-frequency signal a company reports on — and applies that same
     stype to every summary field (quarters, PE/PB, ROE/ROCE/EBITDA, D/E).
     Earlier each field picked its own most-recent stype independently,
     which could technically be "more current" per field but showed a
     confusing mix (e.g. PE from standalone, ROE from consolidated) on the
     same stock. summary["stype"] now carries this single 'c'/'s' decision.

  Trade-off: ratios/price-ratios now always cost 2x the Finedge calls per
  stock instead of "1 call in the common case where c has data" — full-run
  duration and API usage will go up somewhat; watch RETRY/rate-limit behavior
  on the next full run.

Sector/Industry — canonical taxonomy from classification.json (added July 2026):
  fundamentals_summary.json's sector/industry/macro_sector come from Finedge's
  own company-profile fields, which use a different, more fragmented taxonomy
  (182 distinct "sector" values) than the one used everywhere else on the
  platform — the RS Dashboard's sector heatmap and industry drilldown, both
  built from classification.json's sector_group (32 values, e.g. "Banks",
  "Cement") and display_industry (132 values, e.g. "PSU Banks", "Microfinance").
  Frontend filters built against Finedge's fields showed confusing, overly
  granular options that didn't match the rest of the site. Added
  get_classification_lookup() (a second classification.json read, symbol ->
  {sector_group, display_industry}) threaded through fetch_one_symbol /
  _build_summary_entry / _backfill_one, so summary entries now carry both:
  the original Finedge fields (kept for backward compat) AND sector_group /
  display_industry, which the frontend should use for Sector/Industry filters
  going forward. run_backfill_summary is the fast way to retrofit this onto
  all existing stocks — no Finedge calls, just two R2 reads.

Daily-mode empty-quarters guard (added July 2026):
  run_daily() previously did
    summary.get(sym, {}).get("quarters", [{}])[0].get("header")
  to snapshot a symbol's last-known quarter before adding it to the pending
  list. dict.get(key, default) only substitutes the default when the KEY is
  missing — if summary[sym] exists but its "quarters" list is genuinely
  empty (e.g. a fresh listing with no quarterly PL data ingested yet), the
  real (empty) list is returned instead of the default, and [0] on an empty
  list raises IndexError, crashing the whole daily run. Fixed by resolving
  the quarters list with `or [{}]` first (which substitutes on ANY falsy
  value, not just a missing key) before indexing into it.
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

# Daily-mode pending-retry tuning — see module docstring.
MAX_PENDING_ATTEMPTS = 5  # ~5 trading days of retries before a symbol is dropped

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
    """Recency key for the first (assumed most-recent) row of a Finedge rows
    list — used to compare consolidated vs standalone data and pick whichever
    is actually more current, instead of always preferring consolidated
    whenever it has *any* rows. Tries period_end (YYYYMMDD) first, falls back
    to year if that's all a row carries. Returns -1 for empty/unusable input."""
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
            return int(yr) * 10000  # coarse fallback, only used when period_end is absent
        except (TypeError, ValueError):
            pass
    return -1


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


def compute_hash(payload: str) -> str:
    """Hash of the exact JSON string uploaded for a symbol's per-symbol file.
    Mirrors r2_upload_symbol's serialization exactly, so the hash stored in
    fundamentals_summary.json always reflects what's actually on R2 — this is
    what the frontend will compare against its IndexedDB cache to decide
    whether to refetch fundamentals_full/{SYMBOL}.json or not."""
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
    """Symbol -> {sector_group, display_industry} lookup from classification.json.

    Added because fundamentals_summary.json's sector/industry/macro_sector
    come from Finedge's own company-profile fields, which use a DIFFERENT,
    more fragmented taxonomy (182 distinct "sector" values, mixing grain
    levels confusingly) than the platform's canonical one already used
    everywhere else (RS Dashboard sector heatmap, industry drilldown):
    classification.json's sector_group (32 values, e.g. "Banks", "Cement")
    and display_industry (132 values, e.g. "PSU Banks", "Microfinance").
    This keeps the Results Comparison page's Sector/Industry filters
    consistent with the rest of the site instead of Finedge's raw fields.
    """
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
#   used for: BS, CF, growth_metrics
# (ratios and annual_price_ratios moved to a dual-fetch, recency-picked
#  pattern in July 2026 — see the two functions further below and the
#  "Recency-based stype selection" note in the module docstring)
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
        # Prefer whichever stype's latest row is actually more recent, same
        # recency-over-existence rule as the PL quarters fix above. Falls
        # back to whichever one has data if the "winner" turns out empty.
        stype_used = "s" if _latest_key(s_rows) > _latest_key(c_rows) else "c"
        rows = rows_by_stype[stype_used]
        if not rows:
            rows = c_rows or s_rows
            stype_used = "c" if c_rows else ("s" if s_rows else None)
        # raw_c/raw_s kept alongside so _build_summary_entry can pick ONE
        # stype for the whole company and apply it consistently everywhere
        # (see "Summary — single consistent stype per stock" in the module
        # docstring), instead of each ratio type picking independently.
        out[rtype] = {"stype_used": stype_used, "raw": rows, "raw_c": c_rows, "raw_s": s_rows}
    return out


async def _fetch_growth_metrics_single(client, sem, sym):
    # NOT extended with the recency check above — financial-metrics returns a
    # single aggregate dict (e.g. multi-year CAGR figures), not a list of
    # dated rows, so there's no per-row date to compare c vs s recency on.
    # Left as "prefer c, fallback to s only if c is empty" (original design).
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

def _compute_opm(row):
    """OPM from quarterly PL core row.
    Non-bank: (pbt + dep + fin_costs - other_income) / sales
    Bank:     NIM = (interest_earned - interest_expended) / interest_earned
    Returns a decimal fraction (e.g. 0.142 = 14.2%) or None.
    """
    sales = row.get("sales")
    interest_earned = row.get("interest_earned")

    # Bank path — NIM as OPM proxy
    if not sales and interest_earned:
        ie = row.get("interest_expended") or 0
        return round((interest_earned - ie) / interest_earned, 4) if interest_earned else None

    if not sales:
        return None

    # Primary: expenses field available
    exp = row.get("expenses")
    if exp is not None:
        return round((sales - exp) / sales, 4)

    # Fallback: reconstruct operating profit from available fields
    pbt  = row.get("pbt")
    dep  = row.get("depreciation")
    fin  = row.get("finance_costs")
    oth  = row.get("other_income") or 0
    if pbt is not None and dep is not None and fin is not None:
        op = pbt + dep + fin - oth
        return round(op / sales, 4)
    return None


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
    # Decide ONE overall c/s choice per stock — from quarterly PL recency,
    # since that's the highest-frequency signal a company reports on — and
    # apply it to every field below (quarters, PE/PB, ROE/ROCE/EBITDA, D/E).
    # Previously each field picked its own most-recent stype independently,
    # which could show e.g. PE from standalone and ROE from consolidated on
    # the same stock — technically "most current" per field, but confusing
    # to read. One flag per stock, used everywhere, is simpler and predictable.
    c_core = pl.get("quarterly", {}).get("c", {}).get("core") or []
    s_core = pl.get("quarterly", {}).get("s", {}).get("core") or []
    stype = "s" if _latest_key(s_core) > _latest_key(c_core) else "c"
    if stype == "c" and not c_core and s_core:
        stype = "s"  # c "won" the tie-break but is actually empty — use s

    q_core = s_core if stype == "s" else c_core
    quarters = [{
        "header": _fmt_period_end(row.get("period_end")),
        "sales":    row.get("sales") if row.get("sales") is not None else row.get("interest_earned"),
        "expenses": row.get("expenses"),   # needed for OPM in Results Comparison
        "opm":      _compute_opm(row),     # pre-computed so Results Comparison doesn't need expenses
        "eps":      row.get("eps"),
        "pat":      row.get("pat"),
        "pbt":      row.get("pbt"),
    } for row in q_core[:9]]  # 9 so YoY base (idx+4) exists for any of the 5 displayed quarters

    # Pull ratios/price-ratios for the SAME chosen stype. Falls back to that
    # endpoint's own best-available row only if the chosen stype genuinely
    # has nothing for it (rare) — better to show a number than none, but
    # this doesn't happen for the vast majority of stocks.
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
        # Canonical platform taxonomy from classification.json — same fields
        # used by the RS Dashboard's sector heatmap (sector_group) and
        # industry drilldown (display_industry). Use these for Sector/
        # Industry filters instead of the Finedge fields above, which use a
        # different, more fragmented naming scheme.
        "sector_group": classification.get("sector_group"),
        "display_industry": classification.get("display_industry"),
        "diluted_shares": diluted_shares,
        "stype": stype,   # single 'c'/'s' flag — applies to every field below
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
# MODE: full / full_1..10 — bulk universe, per-symbol R2 upload
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
# MODE: daily — only stocks with a result today, plus retries for
# symbols still pending from previous days (see module docstring:
# Finedge confirmed there's no per-symbol refresh-status field, and
# updated data can take 12-36h to land, not always same-day).
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

        # 1. New result-today symbols — snapshot their "before" quarter so we
        #    can tell once Finedge has actually ingested the fresh quarter.
        todays_new = await get_today_result_symbols(client, sem, symbols)
        for sym in todays_new:
            if sym not in pending:
                # FIX: `.get("quarters", [{}])` only substitutes the default
                # when the "quarters" KEY is missing — if summary[sym] exists
                # but its quarters list is genuinely empty (e.g. a fresh
                # listing with no PL data yet), the real [] is returned and
                # [0] on it raises IndexError. `or [{}]` catches that case too.
                prev = summary.get(sym)
                last_known = (prev.get("quarters") or [{}])[0].get("header") if prev else None
                pending[sym] = {
                    "last_known_quarter": last_known,
                    "attempts": 0,
                    "first_seen": today,
                }

        # 2. Carry-over: symbols still pending from earlier days get retried too.
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
                    # ✅ Finedge has ingested the new quarter — publish and clear pending.
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

async def _backfill_one(client, sym, classification_lookup=None):
    obj = await r2_download(client, f"fundamentals_full/{sym}.json")
    if not obj:
        return sym, None
    classification = (classification_lookup or {}).get(sym)
    summ = _build_summary_entry(sym, obj.get("profile"), obj.get("pl", {}),
                                 obj.get("ratios", {}), obj.get("annual_price_ratios", {}), classification)
    # Re-serialize with the same separators r2_upload_symbol uses, so this
    # backfilled hash is consistent with hashes future live runs will produce.
    # NOTE: since this re-serializes a downloaded-and-reparsed object rather
    # than the original upload bytes, it's a reliable proxy but not guaranteed
    # byte-identical to the original upload (e.g. exotic float formatting).
    # Fine for a one-time backfill — any drift self-corrects on the next
    # full/daily/sync run, which always hashes the freshly-built object.
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
        # Auto-retry ALL symbols not found in R2 — could be new listings,
        # migrations, or & symbols where path routing failed.
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
# MODE: sync — retry specific symbols (e.g. ones that failed earlier
# due to the & URL-encoding bug), without rerunning a whole part.
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
