#!/usr/bin/env python3
"""
NSE OHLC + Fundamentals Pipeline — GitHub Actions (Finedge powered)

Usage:
  python pipeline.py daily                # prev-day OHLC           (4:00 PM IST, weekdays)
  python pipeline.py today                # T+0 intraday candle      (4:50 PM IST, weekdays)
  python pipeline.py full                 # initial 1.5yr OHLC load  (manual, once)
  python pipeline.py status               # print R2 chunk summary
  python pipeline.py fund_daily           # result stocks update      (4:30 PM IST, weekdays)
  python pipeline.py fund_full            # one-time full load        (manual)
  python pipeline.py fund_full_1..10      # one-time full load parts  (250 stocks each)
  python pipeline.py enrich_master        # enrich master.json with company profiles (once)
  python pipeline.py enrich_master_1..10  # enrich master parts
  python pipeline.py bse_profiles         # BSE-only profiles full    (manual / Sunday)
  python pipeline.py bse_profiles_1..10   # BSE profiles parts
  python pipeline.py ep_scan              # EP + Post-Result T+1 scan (4:35 PM IST, weekdays)
  python pipeline.py hlr_scan             # HLR + Pullback scan        (4:20 PM IST, weekdays)
  python pipeline.py pattern_scan         # Price action pattern scan   (4:25 PM IST, weekdays)
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)   # 400s suppress

UPSTOX_TOKEN  = os.environ["UPSTOX_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]
FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]

BASE_URL      = "https://api.upstox.com/v2/historical-candle"
V3_URL        = "https://api.upstox.com/v3/historical-candle/intraday"
FINEDGE_BASE  = "https://data.finedgeapi.com/api/v1"

ROLLING_DAYS     = 548
R2_CHUNKS        = 8
CONCURRENCY      = 5
FUND_CONCURRENCY = 4
RETRY            = 3
RATE_DELAY       = 0.5
FINEDGE_DELAY    = 0.25    # safety with 13 calls/stock — ~300/min limit

HERE = Path(__file__).parent

with open(HERE / "nse_holidays.json") as f:
    NSE_HOLIDAYS: set[str] = set(json.load(f))

UPSTOX_HEADERS = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept": "application/json",
}
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

ISIN_MAP:     dict[str, str] = {}
BSE_ISIN_MAP: dict[str, str] = {}
BSE_META:     dict[str, dict] = {}


# ══════════════════════════════════════════════════════════════
# INSTRUMENT MAP BUILDER
# ══════════════════════════════════════════════════════════════

async def build_isin_map(client: httpx.AsyncClient) -> tuple[dict, dict, dict]:
    log.info("Fetching master.json from R2…")
    master = await r2_download(client, "master.json")

    if not master or not isinstance(master, list):
        raise RuntimeError("master.json missing or invalid in R2 — push from Google Sheet first!")

    nse_map:  dict[str, str]  = {}
    bse_map:  dict[str, str]  = {}
    bse_meta: dict[str, dict] = {}

    for stock in master:
        sym      = str(stock.get("symbol",   "")).strip().upper()
        isin     = str(stock.get("isin",     "")).strip()
        exchange = str(stock.get("exchange", "")).strip()
        name     = str(stock.get("name",     "")).strip()

        if not sym or not isin:
            continue

        if exchange == "NSE":
            nse_map[sym] = isin
        elif exchange == "BSE Only":
            bse_map[sym]  = isin
            bse_meta[isin] = {"name": name}

    log.info(f"✓ NSE stocks : {len(nse_map)}")
    log.info(f"✓ BSE-only   : {len(bse_map)}")
    return nse_map, bse_map, bse_meta


# ══════════════════════════════════════════════════════════════
# TRADING CALENDAR
# ══════════════════════════════════════════════════════════════

def today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")

def is_trading_day(d: str) -> bool:
    dt = date.fromisoformat(d)
    return dt.weekday() < 5 and d not in NSE_HOLIDAYS

def last_trading_day() -> str:
    dt = date.fromisoformat(today_ist())
    for _ in range(14):
        if is_trading_day(dt.isoformat()):
            return dt.isoformat()
        dt -= timedelta(days=1)
    raise RuntimeError("No trading day in last 14 days")

def prev_trading_day(d: str) -> str:
    dt = date.fromisoformat(d) - timedelta(days=1)
    for _ in range(14):
        if is_trading_day(dt.isoformat()):
            return dt.isoformat()
        dt -= timedelta(days=1)
    raise RuntimeError(f"No prev trading day before {d}")

def rolling_cutoff(anchor: str) -> str:
    return (date.fromisoformat(anchor) - timedelta(days=ROLLING_DAYS)).isoformat()


# ══════════════════════════════════════════════════════════════
# UPSTOX OHLC API
# ══════════════════════════════════════════════════════════════

async def fetch_ohlc(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    sym: str,
    isin: str,
    from_date: str,
    to_date: str,
    exchange: str = "NSE_EQ",
) -> tuple[str, list | None]:
    key = quote(f"{exchange}|{isin}", safe="")
    url = f"{BASE_URL}/{key}/day/{to_date}/{from_date}"

    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            try:
                r = await client.get(url, headers=UPSTOX_HEADERS, timeout=20)
            except httpx.RequestError as e:
                log.warning(f"  {sym}: network error ({e}), retry {attempt+1}")
                await asyncio.sleep(2 ** attempt)
                continue

        if r.status_code == 401:
            log.error("❌ TOKEN EXPIRED — update UPSTOX_TOKEN secret!")
            sys.exit(1)
        if r.status_code == 429:
            log.warning(f"  {sym}: 429 — waiting 10s")
            await asyncio.sleep(10)
            continue
        if r.status_code != 200:
            return sym, None

        payload = r.json()
        if payload.get("status") != "success":
            return sym, None

        raw = payload.get("data", {}).get("candles", [])
        if not raw:
            return sym, None

        candles = sorted(
            [{"d": c[0][:10], "o": c[1], "h": c[2], "l": c[3],
              "c": c[4], "v": c[5], "oi": c[6] if len(c) > 6 else 0}
             for c in raw],
            key=lambda x: x["d"],
        )
        return sym, candles

    return sym, None


async def fetch_today_candle(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    sym: str,
    isin: str,
    exchange: str = "NSE_EQ",
) -> tuple[str, dict | None]:
    key = quote(f"{exchange}|{isin}", safe="")
    url = f"{V3_URL}/{key}/days/1"

    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            try:
                r = await client.get(url, headers=UPSTOX_HEADERS, timeout=20)
            except httpx.RequestError:
                await asyncio.sleep(2 ** attempt)
                continue

        if r.status_code == 401:
            log.error("❌ TOKEN EXPIRED")
            sys.exit(1)
        if r.status_code == 429:
            log.warning(f"  {sym}: 429 — waiting 10s")
            await asyncio.sleep(10)
            continue
        if r.status_code != 200:
            return sym, None

        raw = r.json().get("data", {}).get("candles", [])
        if not raw:
            return sym, None

        c = raw[0]
        return sym, {"d": c[0][:10], "o": c[1], "h": c[2], "l": c[3],
                     "c": c[4], "v": c[5], "oi": c[6] if len(c) > 6 else 0}

    return sym, None


# ══════════════════════════════════════════════════════════════
# FINEDGE API — BASE HELPER
# ══════════════════════════════════════════════════════════════

async def _finedge_get(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    path: str,
    params: dict,
) -> dict | list | None:
    params["token"] = FINEDGE_TOKEN
    url = f"{FINEDGE_BASE}/{path}"

    async with sem:
        for attempt in range(RETRY):
            await asyncio.sleep(FINEDGE_DELAY)
            try:
                r = await client.get(url, params=params, timeout=30)
            except httpx.RequestError as e:
                log.warning(f"  Finedge network error: {e}, retry {attempt+1}")
                await asyncio.sleep(2 ** attempt)
                continue

            if r.status_code == 401:
                log.error("❌ FINEDGE TOKEN INVALID — check FINEDGE_TOKEN secret!")
                sys.exit(1)
            if r.status_code == 429:
                log.warning("  Finedge rate limit — waiting 20s")
                await asyncio.sleep(20)
                continue
            if r.status_code in (502, 503, 504):
                log.warning(f"  Finedge {r.status_code} — retry {attempt+1}")
                await asyncio.sleep(2 ** attempt)
                continue
            if r.status_code != 200 or not r.text.strip():
                return None
            try:
                return r.json()
            except Exception:
                return None
    return None


# ══════════════════════════════════════════════════════════════
# FINEDGE FUNDAMENTAL FETCHERS
# ══════════════════════════════════════════════════════════════

def _fmt_period_end(period_end) -> str:
    """20260331 (int) → 'Mar 2026'"""
    if not period_end:
        return ""
    MONTHS = ["","Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]
    s = str(int(period_end))
    if len(s) == 8:
        m = int(s[4:6])
        return f"{MONTHS[m]} {s[:4]}" if 1 <= m <= 12 else s
    return str(period_end)


async def _finedge_financials(client, sem, sym, code, period) -> list | None:
    """Consolidated first, fallback to standalone."""
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"financials/{sym}", {
            "statement_type": stype, "statement_code": code, "period": period,
        })
        rows = (d or {}).get("financials", [])
        if rows:
            return rows
    return None


async def _finedge_basic(client, sem, sym) -> list | None:
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"basic-financials/{sym}", {
            "statement_type": stype, "statement_code": "pl",
        })
        rows = (d or {}).get("ratios", [])
        if rows:
            return rows
    return None


async def _finedge_ratios_pr(client, sem, sym) -> list | None:
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}", {
            "statement_type": stype, "ratio_type": "pr",
        })
        rows = (d or {}).get("ratios", [])
        if rows:
            return rows
    return None


async def _finedge_shareholding(client, sem, sym) -> dict | None:
    d = await _finedge_get(client, sem, f"shareholdings/pattern/{sym}", {"period": "quarterly"})
    if not d:
        return None
    columns = d.get("columns", [])
    rows    = d.get("rows", [])
    if not columns or not rows:
        return None

    n_qtrs = min(8, len(columns))
    qtrs   = columns[:n_qtrs]

    def get_row(*names) -> list:
        for name in names:
            r = next(
                (x for x in rows if name.lower() in x.get("catagory", "").lower()),
                None
            )
            if r is None:
                continue
            data = r.get("data", {})
            if isinstance(data, dict):
                return [data.get(q) for q in qtrs]
            elif isinstance(data, list):
                return list(data[:n_qtrs])
        return []

    fii      = get_row("institutionsforeign", "foreign", "fii")
    dii      = get_row("institutionsdomestic", "domestic", "dii")
    public   = get_row("noninstitutions", "public", "retail")
    govt     = get_row("goverment", "government")
    promoter = get_row("promoter")

    # ITC jaise stocks mein Promoter category nahi hoti — compute karo
    if not any(v is not None for v in promoter):
        promoter_computed = []
        for i in range(n_qtrs):
            vals = [
                fii[i]    if i < len(fii)    else None,
                dii[i]    if i < len(dii)    else None,
                public[i] if i < len(public) else None,
                govt[i]   if i < len(govt)   else 0,
            ]
            if all(v is not None for v in vals[:3]):
                promoter_computed.append(round(100 - sum(v or 0 for v in vals), 2))
            else:
                promoter_computed.append(None)
        promoter = promoter_computed

    def _first(lst):
        return next((v for v in lst if v is not None), None)

    return {
        "sh_quarters" : qtrs,
        "sh_promoter" : promoter,
        "sh_fii"      : fii,
        "sh_dii"      : dii,
        "sh_public"   : public,
        "promoter"    : _first(promoter),
        "fii"         : _first(fii),
        "dii"         : _first(dii),
        "public"      : _first(public),
        "promoter_ch" : (
            round(promoter[0] - promoter[1], 2)
            if len(promoter) >= 2
            and promoter[0] is not None
            and promoter[1] is not None
            else None
        ),
    }


async def _finedge_profile(client, sem, sym) -> dict | None:
    d = await _finedge_get(client, sem, f"company-profile/{sym}", {})
    if not d:
        return None
    return {
        "name"        : d.get("name", ""),
        "sector"      : d.get("sector", ""),
        "industry"    : d.get("industry", ""),
        "sub_industry": d.get("sub_industry", ""),
        "macro_sector": d.get("macro_sector", ""),
        "market_cap"  : d.get("market_cap"),
        "bse_code"    : d.get("bse_code", ""),
        "description" : d.get("description", ""),
        "website"     : d.get("website", ""),
    }


async def _finedge_growth_metrics(client, sem, sym) -> dict | None:
    """financial-metrics?ratio_type=gr — revenue/eps/ebitda CAGR 3yr+5yr.
       statement_type=c first, fallback s.
    """
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"financial-metrics/{sym}",
                               {"statement_type": stype, "ratio_type": "gr"})
        fm = (d or {}).get("financial_metrics")
        if fm:
            return fm
    return None


async def _finedge_annual_price_ratios(client, sem, sym) -> list | None:
    """annual-price-ratios — historical PE/PB/PS/PFCF per year.
       statement_type=c first, fallback s.
    """
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"annual-price-ratios/{sym}",
                               {"statement_type": stype})
        rows = (d or {}).get("price_ratios", [])
        if rows:
            return rows
    return None


async def _finedge_ratios_le(client, sem, sym) -> list | None:
    """ratios?ratio_type=le — leverage: D/E, financial leverage, debt/assets."""
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}",
                               {"statement_type": stype, "ratio_type": "le"})
        rows = (d or {}).get("ratios", [])
        if rows:
            return rows
    return None


async def _finedge_ratios_li(client, sem, sym) -> list | None:
    """ratios?ratio_type=li — liquidity: current ratio, quick ratio, interest coverage."""
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}",
                               {"statement_type": stype, "ratio_type": "li"})
        rows = (d or {}).get("ratios", [])
        if rows:
            return rows
    return None


async def _finedge_ratios_ef(client, sem, sym) -> list | None:
    """ratios?ratio_type=ef — efficiency: asset turnover, CCC, inventory/debtor days."""
    for stype in ("c", "s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}",
                               {"statement_type": stype, "ratio_type": "ef"})
        rows = (d or {}).get("ratios", [])
        if rows:
            return rows
    return None


async def fetch_one_fundamental(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    sym: str,
    isin: str = "",
) -> tuple[str, dict | None]:
    """
    13 parallel Finedge calls per stock:
      1. P&L quarterly      2. P&L annual
      3. BS annual          4. CF annual
      5. Basic financials   6. Profitability ratios (pr)
      7. Shareholding       8. Company profile
      9. Growth metrics    10. Annual price ratios
     11. Leverage ratios   12. Liquidity ratios
     13. Efficiency ratios
    """
    (pl_qtr, pl_ann, bs_ann, cf_ann,
     basic, prof_ratios, sh, profile,
     growth, ann_pr,
     ratios_le, ratios_li, ratios_ef) = await asyncio.gather(
        _finedge_financials(client, sem, sym, "pl", "quarterly"),
        _finedge_financials(client, sem, sym, "pl", "annual"),
        _finedge_financials(client, sem, sym, "bs", "annual"),
        _finedge_financials(client, sem, sym, "cf", "annual"),
        _finedge_basic(client, sem, sym),
        _finedge_ratios_pr(client, sem, sym),
        _finedge_shareholding(client, sem, sym),
        _finedge_profile(client, sem, sym),
        _finedge_growth_metrics(client, sem, sym),
        _finedge_annual_price_ratios(client, sem, sym),
        _finedge_ratios_le(client, sem, sym),
        _finedge_ratios_li(client, sem, sym),
        _finedge_ratios_ef(client, sem, sym),
    )

    if not any([pl_qtr, pl_ann, bs_ann, cf_ann]):
        return sym, None

    obj: dict = {"symbol": sym, "updated": today_ist(), "source": "finedge"}

    # ── Company Profile ────────────────────────────────────────
    if profile:
        obj.update({
            "name"        : profile.get("name", ""),
            "sector"      : profile.get("sector", ""),
            "industry"    : profile.get("industry", ""),
            "sub_industry": profile.get("sub_industry", ""),
            "macro_sector": profile.get("macro_sector", ""),
            "market_cap"  : profile.get("market_cap"),
            "bse_code"    : profile.get("bse_code", ""),
            "description" : profile.get("description", ""),
            "website"     : profile.get("website", ""),
        })

    # ── Basic Financials TTM ───────────────────────────────────
    # Build lookups: year → dividendPayout, year → shares
    div_payout_by_year: dict = {}
    shares_by_year:     dict = {}
    if basic:
        ttm = basic[0]
        obj.update({
            "ebit"              : ttm.get("ebit"),
            "ebitda"            : ttm.get("ebitda"),
            "operating_revenue" : ttm.get("operatingRevenue"),
            "operating_profit"  : ttm.get("operatingProfit"),
            "shares_outstanding": ttm.get("dilutedSharesOutstanding"),
        })
        for row in basic:
            yr = row.get("year")
            if yr is not None:
                if row.get("dividendPayout") is not None:
                    div_payout_by_year[yr] = row["dividendPayout"]
                if row.get("dilutedSharesOutstanding") is not None:
                    shares_by_year[yr] = row["dilutedSharesOutstanding"]

    # ── P&L Quarterly — 12 quarters ───────────────────────────
    if pl_qtr:
        obj["pl_quarterly"] = [
            {
                "header"       : _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "period_end"   : q.get("period_end"),
                "sales"        : q.get("revenueFromOperations"),
                "expenses"     : q.get("expenses"),
                "pbt"          : q.get("profitBeforeTax"),
                "pat"          : q.get("profitLossForPeriod"),
                "eps"          : q.get("eps"),
                "depreciation" : q.get("depreciationAndAmortisation"),
                "finance_costs": q.get("financeCosts"),
                "tax"          : q.get("taxExpense"),
                "other_income" : q.get("otherIncome"),
            }
            for q in pl_qtr[:12]
        ]

    # ── P&L Annual — 5 years ──────────────────────────────────
    if pl_ann:
        obj["pl_annual"] = [
            {
                "header"          : _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "year"            : q.get("year"),
                "sales"           : q.get("revenueFromOperations"),
                "expenses"        : q.get("expenses"),
                "pbt"             : q.get("profitBeforeTax"),
                "pat"             : q.get("profitLossForPeriod"),
                "eps"             : q.get("eps"),
                "depreciation"    : q.get("depreciationAndAmortisation"),
                "finance_costs"   : q.get("financeCosts"),
                "other_income"    : q.get("otherIncome"),
                "dividend_payout" : div_payout_by_year.get(q.get("year")),
                "shares"          : shares_by_year.get(q.get("year")),  # Piotroski F7
            }
            for q in pl_ann[:5]
        ]

    # ── Balance Sheet Annual — 5 years ────────────────────────
    if bs_ann:
        obj["bs_annual"] = [
            {
                "header"               : _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "year"                 : q.get("year"),
                "total_assets"         : q.get("assets"),
                "equity_capital"       : q.get("equityCapital"),
                "reserves"             : q.get("reserves"),
                "borrowings_current"   : q.get("borrowingsCurrent"),
                "borrowings_noncurrent": q.get("borrowingsNoncurrent"),
                "borrowings_total"     : (q.get("borrowingsCurrent") or 0) + (q.get("borrowingsNoncurrent") or 0),
                "cash"                 : q.get("cashAndCashEquivalents"),
                "current_assets"       : q.get("currentAssets"),
                "current_liabilities"  : q.get("currentLiabilities"),
                "fixed_assets"         : q.get("propertyPlantAndEquipmentNet"),
                "investments"          : q.get("investments"),
            }
            for q in bs_ann[:5]
        ]

    # ── Cash Flow Annual — 5 years ────────────────────────────
    if cf_ann:
        obj["cf_annual"] = [
            {
                "header": _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "year"  : q.get("year"),
                "cfo"   : q.get("cashFlowsFromOperatingActivities"),
                "cfi"   : q.get("cashFlowsFromInvestingActivities"),
                "cff"   : q.get("cashFlowsFromFinancingActivities"),
                "net_cf": q.get("netCashFlow"),
                "capex" : q.get("purchaseOfPPEClassifiedAsInvesting"),
                "fcf"   : (
                    (q.get("cashFlowsFromOperatingActivities") or 0) +
                    (q.get("purchaseOfPPEClassifiedAsInvesting") or 0)
                ) if q.get("cashFlowsFromOperatingActivities") is not None else None,
            }
            for q in cf_ann[:5]
        ]

    # ── Profitability Ratios — 5 years ────────────────────────
    if prof_ratios:
        obj["ratios_annual"] = [
            {
                "header"          : r.get("header", ""),
                "year"            : r.get("year"),
                "gross_margin"    : r.get("grossMargin"),
                "ebit_margin"     : r.get("ebitMargin"),
                "ebitda_margin"   : r.get("ebitdaMargin"),
                "net_margin"      : r.get("netMargin"),
                "operating_margin": r.get("operatingMargin"),
                "roe"             : r.get("returnOnEquity"),
                "roa"             : r.get("returnOnAsset"),
                "roce"            : r.get("returnOnCapital"),
                "pretax_margin"   : r.get("preTaxMargin"),
                "tax_rate"        : r.get("effectiveTaxRate"),
            }
            for r in prof_ratios[:5]
        ]

    # ── Shareholding — 8 quarters ─────────────────────────────
    if sh:
        obj.update({
            "promoter"    : sh.get("promoter"),
            "fii"         : sh.get("fii"),
            "dii"         : sh.get("dii"),
            "public"      : sh.get("public"),
            "promoter_ch" : sh.get("promoter_ch"),
            "sh_quarters" : sh.get("sh_quarters", []),
            "sh_promoter" : sh.get("sh_promoter", []),
            "sh_fii"      : sh.get("sh_fii", []),
            "sh_dii"      : sh.get("sh_dii", []),
            "sh_public"   : sh.get("sh_public", []),
        })

    # ── Growth Metrics (CAGR) ─────────────────────────────────
    if growth:
        obj.update({
            "revenue_cagr_3y"   : growth.get("revenueGrowth3years"),
            "revenue_cagr_5y"   : growth.get("revenueGrowth5years"),
            "pat_cagr_3y"       : growth.get("netIncomeGrowth3years"),
            "pat_cagr_5y"       : growth.get("netIncomeGrowth5years"),
            "eps_cagr_3y"       : growth.get("epsGrowth3years"),
            "eps_cagr_5y"       : growth.get("epsGrowth5years"),
            "ebitda_cagr_3y"    : growth.get("EBITDAGrowth3years"),
            "ebitda_cagr_5y"    : growth.get("EBITDAGrowth5years"),
            "cfo_cagr_3y"       : growth.get("cfoGrowth3years"),
            "fcf_cagr_3y"       : growth.get("freeCashFlowGrowth3Years"),
            "share_dilution_3y" : growth.get("dilutedSharesGrowth3years"),
            "share_dilution_5y" : growth.get("dilutedSharesGrowth5years"),
        })

    # ── Annual Price Ratios — 5 years ─────────────────────────
    if ann_pr:
        obj["price_ratios_annual"] = [
            {
                "header"    : r.get("header", ""),
                "year"      : r.get("year"),
                "avg_price" : r.get("average_price"),
                "pe"        : r.get("pe"),
                "pb"        : r.get("pb"),
                "ps"        : r.get("ps"),
                "pfcf"      : r.get("pfcf") or None,
            }
            for r in ann_pr[:5]
            if r.get("year") and r.get("pe")
        ]

    # ── Leverage Ratios — 5 years ─────────────────────────────
    if ratios_le:
        obj["ratios_leverage"] = [
            {
                "header"            : r.get("header", ""),
                "year"              : r.get("year"),
                "de_ratio"          : r.get("totalDebtToEquity"),
                "lt_de_ratio"       : r.get("longTermDebtToEquity"),
                "financial_leverage": r.get("financialLeverage"),
                "debt_to_assets"    : r.get("totalDebttoAssets"),
                "debt_to_fcf"       : r.get("totalDebtTofcf"),
            }
            for r in ratios_le[:6]
            if r.get("year") and r.get("header") != "TTM"
        ]

    # ── Liquidity Ratios — 5 years ────────────────────────────
    if ratios_li:
        obj["ratios_liquidity"] = [
            {
                "header"            : r.get("header", ""),
                "year"              : r.get("year"),
                "current_ratio"     : r.get("currentRatio"),
                "quick_ratio"       : r.get("quickRatio"),
                "interest_coverage" : r.get("interestCoverage"),
            }
            for r in ratios_li[:6]
            if r.get("year") and r.get("header") != "TTM"
        ]

    # ── Efficiency Ratios — 5 years ───────────────────────────
    if ratios_ef:
        obj["ratios_efficiency"] = [
            {
                "header"                : r.get("header", ""),
                "year"                  : r.get("year"),
                "asset_turnover"        : r.get("assetTurnover"),
                "inventory_turnover"    : r.get("inventoryTurnover"),
                "receivable_turnover"   : r.get("receivableTurnover"),
                "cash_conversion_cycle" : r.get("cashConversionCycle"),
                "debtor_days"           : r.get("debtorDays"),
                "inventory_days"        : r.get("inventoryDays"),
                "days_payable"          : r.get("daysPayable"),
            }
            for r in ratios_ef[:6]
            if r.get("year") and r.get("header") != "TTM"
        ]

    # ── Post-processing: derive from PL/BS ────────────────────
    if obj.get("pl_quarterly"):
        obj["eps_diluted"] = obj["pl_quarterly"][0].get("eps")

    if obj.get("bs_annual"):
        bs0    = obj["bs_annual"][0]
        eq_cap = bs0.get("equity_capital") or 0
        res    = bs0.get("reserves") or 0
        borr   = bs0.get("borrowings_total") or 0
        cash   = bs0.get("cash") or 0
        shares = obj.get("shares_outstanding") or 0
        obj["book_value_ps"] = round((eq_cap + res) / shares, 2) if shares else None
        obj["net_debt"]      = round(borr - cash) if (borr or cash) else None

    return sym, obj


async def fetch_one_bse_profile(client, sem, sym, isin, meta: dict):
    """BSE-only stocks ke liye sirf company profile fetch karo via Finedge."""
    profile = await _finedge_profile(client, sem, sym)
    obj = {
        "symbol"     : sym,
        "updated"    : today_ist(),
        "name"       : meta.get("name", ""),
        "sector"     : profile.get("sector", "")      if profile else "",
        "industry"   : profile.get("industry", "")    if profile else "",
        "description": profile.get("description", "") if profile else "",
        "market_cap" : profile.get("market_cap")      if profile else None,
    }
    return sym, obj


# ══════════════════════════════════════════════════════════════
# FINEDGE RESULTS CALENDAR
# ══════════════════════════════════════════════════════════════

async def get_result_symbols_finedge(client: httpx.AsyncClient) -> list[str]:
    today = today_ist()
    next7 = (date.fromisoformat(today) + timedelta(days=1)).isoformat()

    sem = asyncio.Semaphore(1)
    d = await _finedge_get(client, sem, "results-calendar", {
        "from_date": today,
        "to_date"  : next7,
    })
    if not d or not isinstance(d, list):
        log.warning("Finedge results calendar — empty or error")
        return []

    isin_symbols = set(ISIN_MAP.keys())
    matched = list({
        item["symbol"]
        for item in d
        if item.get("symbol") in isin_symbols
        and item.get("expected_result_date") == today
    })
    log.info(f"Finedge results today ({today}): {len(matched)} stocks — {', '.join(matched) or 'none'}")
    return matched


# ══════════════════════════════════════════════════════════════
# R2 HELPERS
# ══════════════════════════════════════════════════════════════

async def r2_download_fund(client: httpx.AsyncClient) -> dict:
    url = f"{WORKER_URL}/fundamentals.json"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404:
        log.info("fundamentals.json not found in R2 — starting fresh")
        return {}
    if r.status_code != 200:
        raise RuntimeError(f"Download failed: HTTP {r.status_code}")
    log.info(f"  ↓ fundamentals.json ({len(r.content)/1024:.0f} KB)")
    data = r.json()
    if isinstance(data, list):
        return {d["symbol"]: d for d in data if d.get("symbol")}
    if isinstance(data, dict):
        return data.get("stocks", data)
    return {}


async def r2_upload_fund(client: httpx.AsyncClient, data: dict) -> None:
    arr = list(data.values())
    payload = json.dumps(arr)
    url = f"{WORKER_URL}?file=fundamentals.json"
    r = await client.post(
        url,
        headers={**WORKER_HEADERS, "Content-Type": "application/json"},
        content=payload.encode(),
        timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed: HTTP {r.status_code}")
    log.info(f"  ↑ fundamentals.json ({len(payload)/1024:.1f} KB)")


async def r2_upload(client: httpx.AsyncClient, filename: str, data: str | bytes) -> None:
    if isinstance(data, str):
        data = data.encode()
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(
        url,
        headers={**WORKER_HEADERS, "Content-Type": "application/json"},
        content=data,
        timeout=90,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")


async def r2_download(client: httpx.AsyncClient, filename: str) -> dict | list | None:
    url = f"{WORKER_URL}/{filename}"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↓ {filename} ({len(r.content)/1024:.0f} KB)")
    return r.json()


async def save_result_calendar(
    client: httpx.AsyncClient,
    symbols: list[str],
    date_str: str,
    keep_days: int = 60,
) -> None:
    try:
        existing = await r2_download(client, "result_calendar.json")
        cal: dict = existing if isinstance(existing, dict) else {}
    except Exception:
        cal = {}

    cal[date_str] = symbols
    cutoff = (date.fromisoformat(date_str) - timedelta(days=keep_days)).isoformat()
    cal = {d: v for d, v in cal.items() if d >= cutoff}

    await r2_upload(client, "result_calendar.json", json.dumps(cal))
    log.info(f"  📅 result_calendar.json — {len(cal)} days stored, {len(symbols)} stocks today")


async def download_all_chunks(client: httpx.AsyncClient) -> dict:
    tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data: dict = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            log.warning(f"  ohlc_{i+1}.json error: {res}")
        elif res and "stocks" in res:
            all_data.update(res["stocks"])
    log.info(f"Master: {len(all_data)} stocks across {R2_CHUNKS} chunks")
    return all_data


async def upload_all_chunks(client: httpx.AsyncClient, all_data: dict, today: str) -> None:
    symbols = sorted(all_data.keys())
    n = len(symbols)
    size = (n + R2_CHUNKS - 1) // R2_CHUNKS
    tasks = []
    for i in range(R2_CHUNKS):
        chunk_syms = symbols[i * size : (i + 1) * size]
        chunk = {s: all_data[s] for s in chunk_syms}
        payload = json.dumps({"updated": today, "chunk": i + 1, "total": R2_CHUNKS, "stocks": chunk})
        tasks.append(r2_upload(client, f"ohlc_{i+1}.json", payload))
    await asyncio.gather(*tasks)
    log.info(f"✓ {R2_CHUNKS} chunks uploaded ({n} stocks)")


# ══════════════════════════════════════════════════════════════
# DATA HELPERS
# ══════════════════════════════════════════════════════════════

def build_stock_obj(candles: list) -> dict:
    return {k: [c[k] for c in candles] for k in ("d", "o", "h", "l", "c", "v", "oi")}

def apply_rolling_window(all_data: dict, cutoff: str) -> int:
    dropped = 0
    for s in all_data.values():
        keep = [i for i, d in enumerate(s["d"]) if d >= cutoff]
        dropped += len(s["d"]) - len(keep)
        for k in s:
            s[k] = [s[k][i] for i in keep]
    return dropped

def _sort_stock(s: dict) -> None:
    order = sorted(range(len(s["d"])), key=lambda i: s["d"][i])
    for k in s:
        s[k] = [s[k][i] for i in order]

def merge_candles_into(all_data: dict, sym: str, candles: list, cutoff: str) -> int:
    if sym not in all_data:
        all_data[sym] = {k: [] for k in ("d", "o", "h", "l", "c", "v", "oi")}
    s = all_data[sym]
    existing = set(s["d"])
    added = 0
    for c in candles:
        if c["d"] < cutoff or c["d"] in existing:
            continue
        for k in s:
            s[k].append(c[k])
        existing.add(c["d"])
        added += 1
    if added:
        _sort_stock(s)
    return added

def upsert_candle(all_data: dict, sym: str, c: dict) -> None:
    if sym not in all_data:
        all_data[sym] = {k: [] for k in ("d", "o", "h", "l", "c", "v", "oi")}
    s = all_data[sym]
    if c["d"] in s["d"]:
        idx = s["d"].index(c["d"])
        for k in ("o", "h", "l", "c", "v", "oi"):
            s[k][idx] = c[k]
    else:
        for k in s:
            s[k].append(c[k])
        _sort_stock(s)



# ══════════════════════════════════════════════════════════════
# PIPELINE MODES — OHLC
# ══════════════════════════════════════════════════════════════

async def run_daily() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    prev   = prev_trading_day(today)
    cutoff = rolling_cutoff(today)
    log.info(f"━━━ Daily  {prev} → {today}  cutoff {cutoff} ━━━")

    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        nse_tasks = [fetch_ohlc(client, sem, sym, ISIN_MAP[sym], prev, today, "NSE_EQ") for sym in ISIN_MAP]
        bse_tasks = [fetch_ohlc(client, sem, sym, BSE_ISIN_MAP[sym], prev, today, "BSE_EQ") for sym in BSE_ISIN_MAP]

        nse_results, bse_results = await asyncio.gather(
            asyncio.gather(*nse_tasks),
            asyncio.gather(*bse_tasks),
        )

        fetched = {sym: c for sym, c in [*nse_results, *bse_results] if c}
        total   = len(ISIN_MAP) + len(BSE_ISIN_MAP)
        log.info(f"✓ {len(fetched)} fetched  ✗ {total - len(fetched)} no data")

        all_data = await download_all_chunks(client)

        live   = set(ISIN_MAP) | set(BSE_ISIN_MAP)
        pruned = [s for s in list(all_data) if s not in live]
        for s in pruned:
            del all_data[s]
        if pruned:
            log.info(f"🗑  Pruned {len(pruned)} delisted/removed stocks")

        total_new = 0
        delta: dict = {}
        for sym, candles in fetched.items():
            total_new += merge_candles_into(all_data, sym, candles, cutoff)
            today_c = next((c for c in candles if c["d"] == today), None)
            if today_c:
                delta[sym] = today_c

        log.info(f"Merged: {total_new} new candles  Delta: {len(delta)} stocks")
        dropped = apply_rolling_window(all_data, cutoff)
        log.info(f"Rolling: dropped {dropped} old candles")

        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_delta.json", json.dumps({"date": today, "stocks": delta})),
        )
    log.info("━━━ Daily complete ━━━")


async def run_today() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ Today (v3 intraday)  {today} ━━━")
    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        nse_tasks = [fetch_today_candle(client, sem, sym, ISIN_MAP[sym], "NSE_EQ") for sym in ISIN_MAP]
        bse_tasks = [fetch_today_candle(client, sem, sym, BSE_ISIN_MAP[sym], "BSE_EQ") for sym in BSE_ISIN_MAP]

        nse_results, bse_results = await asyncio.gather(
            asyncio.gather(*nse_tasks),
            asyncio.gather(*bse_tasks),
        )

        fetched = {sym: c for sym, c in [*nse_results, *bse_results] if c}
        log.info(f"✓ {len(fetched)} have today's candle")

        all_data = await download_all_chunks(client)
        for sym, c in fetched.items():
            upsert_candle(all_data, sym, c)

        delta = {sym: c for sym, c in fetched.items() if c["d"] == today}
        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_delta.json", json.dumps({"date": today, "stocks": delta})),
        )
        log.info(f"✅ delta: {len(delta)} stocks with today's candle")
    log.info("━━━ Today complete ━━━")


async def run_full() -> None:
    today  = last_trading_day()
    start  = (date.fromisoformat(today) - timedelta(days=ROLLING_DAYS)).isoformat()
    cutoff = start

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

    total_stocks = len(ISIN_MAP) + len(BSE_ISIN_MAP)
    log.info(f"━━━ Full Load  {start} → {today}  ({total_stocks} stocks) ━━━")

    sem      = asyncio.Semaphore(CONCURRENCY)
    all_data: dict = {}
    failed:   list[str] = []

    all_sym_map = {sym: (isin, "NSE_EQ") for sym, isin in ISIN_MAP.items()}
    all_sym_map.update({sym: (isin, "BSE_EQ") for sym, isin in BSE_ISIN_MAP.items()})

    async with httpx.AsyncClient() as client:
        symbols = list(all_sym_map.keys())
        for i in range(0, len(symbols), 50):
            chunk_syms = symbols[i : i + 50]
            tasks   = [fetch_ohlc(client, sem, sym, all_sym_map[sym][0], start, today, all_sym_map[sym][1]) for sym in chunk_syms]
            results = await asyncio.gather(*tasks)
            for sym, candles in results:
                if candles:
                    filtered = [c for c in candles if c["d"] >= cutoff]
                    if filtered:
                        all_data[sym] = build_stock_obj(filtered)
                else:
                    failed.append(sym)

            pct = min(i + 50, len(symbols))
            log.info(f"  {pct}/{len(symbols)}  OK:{len(all_data)}  Failed:{len(failed)}")
            if pct % 500 == 0:
                await upload_all_chunks(client, all_data, today)

        log.info(f"✓ {len(all_data)} loaded  ✗ {len(failed)} failed")
        if failed:
            (HERE / "failed_stocks.txt").write_text("\n".join(failed))
        apply_rolling_window(all_data, cutoff)
        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_all.json", json.dumps({"updated": today, "stocks": all_data})),
        )
    log.info("━━━ Full load complete ━━━")


async def run_status() -> None:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)], return_exceptions=True)

    print(f"\n{'File':<20} {'Stocks':>7}  {'From':>12}  {'To':>12}  {'Updated':>12}")
    print("─" * 70)
    total = 0
    for i, res in enumerate(results):
        fname = f"ohlc_{i+1}.json"
        if isinstance(res, Exception) or res is None:
            print(f"{fname:<20}  ERROR"); continue
        stocks = res.get("stocks", {})
        if not stocks:
            print(f"{fname:<20}  (empty)"); continue
        s0 = next(iter(stocks.values()))
        total += len(stocks)
        print(f"{fname:<20} {len(stocks):>7}  {s0['d'][0]:>12}  {s0['d'][-1]:>12}  {res.get('updated','?'):>12}")
    print(f"\nTotal: {total} stocks\n")


# ══════════════════════════════════════════════════════════════
# PIPELINE MODES — FUNDAMENTALS
# ══════════════════════════════════════════════════════════════

async def run_fund_daily() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting"); return

    log.info(f"━━━ Fundamentals Daily  {today} ━━━")

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        symbols = await get_result_symbols_finedge(client)
        if not symbols:
            log.info("No results today — exiting"); return

        await save_result_calendar(client, symbols, today)

        fund_data = await r2_download_fund(client)
        sem       = asyncio.Semaphore(FUND_CONCURRENCY)
        tasks     = [fetch_one_fundamental(client, sem, sym) for sym in symbols if sym in ISIN_MAP]
        results   = await asyncio.gather(*tasks)

        ok = 0
        for sym, data in results:
            if data:
                fund_data[sym] = data; ok += 1
                log.info(f"  ✓ {sym}")
            else:
                log.warning(f"  ✗ {sym}: no data")

        log.info(f"Updated {ok}/{len(symbols)} stocks")
        await r2_upload_fund(client, fund_data)
    log.info("━━━ Fundamentals Daily complete ━━━")


async def run_fund_full(part: int = 0) -> None:
    """
    One-time full load — all NSE stocks.
    10 parts: fund_full_1..10. Each ~250 stocks × 10 calls.
    Already-fetched stocks are skipped (checkpoint safe).
    """
    TOTAL_PARTS = 10
    BATCH_SIZE  = 20   # 20 stocks × 10 calls = 200 concurrent tasks

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        nse_symbols = list(ISIN_MAP.keys())
        total       = len(nse_symbols)
        part_size   = (total + TOTAL_PARTS - 1) // TOTAL_PARTS

        if part == 0:
            start_idx, end_idx = 0, total; label = "Full"
        else:
            start_idx = (part - 1) * part_size
            end_idx   = min(part * part_size, total)
            label     = f"Part {part}/{TOTAL_PARTS}"

        chunk = nse_symbols[start_idx:end_idx]
        log.info(f"━━━ Fund Full {label}  ({len(chunk)} NSE stocks) ━━━")

        ETF_KEYWORDS = ("ETF","BEES","LIQUID","GILT","GOLD","SILVER","NIFTY","BANKEX","IETF","MSCIN","MMQS","TOTAL")
        equity_chunk = [sym for sym in chunk if not any(sym.upper().endswith(k) or k in sym.upper() for k in ETF_KEYWORDS)]
        if len(chunk) - len(equity_chunk):
            log.info(f"Skipping {len(chunk)-len(equity_chunk)} ETFs")

        fund_data = await r2_download_fund(client)
        missing   = [sym for sym in equity_chunk if sym not in fund_data]
        log.info(f"Already done: {len(equity_chunk)-len(missing)}  Remaining: {len(missing)}")

        if not missing:
            log.info("✅ All stocks already fetched!"); return

        sem    = asyncio.Semaphore(FUND_CONCURRENCY)
        ok = failed = 0

        for i in range(0, len(missing), BATCH_SIZE):
            batch   = missing[i : i + BATCH_SIZE]
            results = await asyncio.gather(*[fetch_one_fundamental(client, sem, sym) for sym in batch])

            for sym, data in results:
                if data:
                    fund_data[sym] = data; ok += 1
                else:
                    failed += 1
                    log.warning(f"  ✗ {sym}: no data")

            pct = min(i + BATCH_SIZE, len(missing))
            log.info(f"  {pct}/{len(missing)}  ✓{ok}  ✗{failed}")

            if pct % 100 == 0 or pct == len(missing):
                log.info("  💾 Checkpoint upload…")
                await r2_upload_fund(client, fund_data)

    log.info(f"━━━ Fund Full {label} complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# ENRICH MASTER — company profile → master.json
# ══════════════════════════════════════════════════════════════

async def run_enrich_master(part: int = 0) -> None:
    """
    Finedge company-profile se master.json enrich karo.
    sector, industry, sub_industry, macro_sector, bse_code, website, description add honge.
    Checkpoint safe — already enriched stocks skip honge.
    Run: enrich_master_1..10
    """
    TOTAL_PARTS = 10
    BATCH_SIZE  = 30

    async with httpx.AsyncClient() as client:
        log.info("Downloading master.json from R2…")
        master = await r2_download(client, "master.json")
        if not master or not isinstance(master, list):
            raise RuntimeError("master.json missing!")

        total     = len(master)
        part_size = (total + TOTAL_PARTS - 1) // TOTAL_PARTS

        if part == 0:
            start_idx, end_idx = 0, total; label = "Full"
        else:
            start_idx = (part - 1) * part_size
            end_idx   = min(part * part_size, total)
            label     = f"Part {part}/{TOTAL_PARTS}"

        chunk = master[start_idx:end_idx]
        log.info(f"━━━ Enrich Master {label}  ({len(chunk)} stocks) ━━━")

        missing = [s for s in chunk if not s.get("sector")]
        log.info(f"Already enriched: {len(chunk)-len(missing)}  Remaining: {len(missing)}")
        if not missing:
            log.info("✅ All stocks already enriched!"); return

        master_dict = {s["symbol"]: s for s in master if s.get("symbol")}
        sem         = asyncio.Semaphore(FUND_CONCURRENCY)
        ok = failed = 0

        for i in range(0, len(missing), BATCH_SIZE):
            batch   = missing[i : i + BATCH_SIZE]
            results = await asyncio.gather(*[_finedge_profile(client, sem, s["symbol"]) for s in batch])

            for stock, profile in zip(batch, results):
                sym = stock["symbol"]
                if profile:
                    master_dict[sym].update({
                        "sector"      : profile.get("sector",       ""),
                        "industry"    : profile.get("industry",     ""),
                        "sub_industry": profile.get("sub_industry", ""),
                        "macro_sector": profile.get("macro_sector", ""),
                        "bse_code"    : profile.get("bse_code",     ""),
                        "website"     : profile.get("website",      ""),
                        "description" : profile.get("description",  ""),
                    })
                    if profile.get("name") and not master_dict[sym].get("name"):
                        master_dict[sym]["name"] = profile["name"]
                    ok += 1
                else:
                    failed += 1
                    log.warning(f"  ✗ {sym}: no profile")

            pct = min(i + BATCH_SIZE, len(missing))
            log.info(f"  {pct}/{len(missing)}  ✓{ok}  ✗{failed}")

            if pct % 100 == 0 or pct == len(missing):
                await r2_upload(client, "master.json", json.dumps(list(master_dict.values())))
                log.info(f"  💾 Checkpoint saved")

    log.info(f"━━━ Enrich Master {label} complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# BSE PROFILES
# ══════════════════════════════════════════════════════════════

async def run_bse_profiles(part: int = 0) -> None:
    today       = today_ist()
    TOTAL_PARTS = 10

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        bse_syms  = list(BSE_ISIN_MAP.keys())
        total     = len(bse_syms)
        part_size = (total + TOTAL_PARTS - 1) // TOTAL_PARTS

        if part == 0:
            start_idx, end_idx = 0, total; label = "Full"
        else:
            start_idx = (part - 1) * part_size
            end_idx   = min(part * part_size, total)
            label     = f"Part {part}/{TOTAL_PARTS}"

        chunk = bse_syms[start_idx:end_idx]
        log.info(f"━━━ BSE Profiles {label}  {today}  ({len(chunk)} stocks) ━━━")

        bse_raw  = await r2_download(client, "bse_profiles.json")
        bse_fund: dict = {}
        if isinstance(bse_raw, list):
            bse_fund = {d["symbol"]: d for d in bse_raw if d.get("symbol")}
        elif isinstance(bse_raw, dict):
            bse_fund = bse_raw

        missing = [sym for sym in chunk if sym not in bse_fund]
        log.info(f"Already fetched: {len(chunk)-len(missing)}  Missing: {len(missing)}")
        if not missing:
            log.info("✅ All stocks already fetched — nothing to do"); return

        sem = asyncio.Semaphore(1)
        ok = fail = 0

        for i in range(0, len(missing), 20):
            batch   = missing[i : i + 20]
            results = await asyncio.gather(*[fetch_one_bse_profile(client, sem, sym, BSE_ISIN_MAP[sym], BSE_META.get(BSE_ISIN_MAP[sym], {})) for sym in batch])
            for sym, data in results:
                if data: bse_fund[sym] = data; ok += 1
                else: fail += 1
            log.info(f"  {min(i+20,len(missing))}/{len(missing)}  ✓{ok}  ✗{fail}")
            await r2_upload(client, "bse_profiles.json", json.dumps(list(bse_fund.values())))

    log.info(f"━━━ BSE Profiles {label} complete — ✓{ok}  ✗{fail} ━━━")



# ══════════════════════════════════════════════════════════════
# EP FORMATION SCANNER
# ══════════════════════════════════════════════════════════════

def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    lookback = min(50, n)
    if lookback < 20: return True
    avg_vol   = sum(volumes[-lookback:]) / lookback
    avg_price = sum(closes[-lookback:])  / lookback
    return (avg_vol * avg_price) >= min_turnover

def _detect_ep(
    all_data: dict,
    min_gap_pct: float     = 2.0,
    volume_spike_x: float  = 2.0,
    volume_lookback: int   = 20,
    max_consolidation: int = 30,
    max_ep_age_days: int   = 30,
) -> list[dict]:
    signals = []
    for sym, s in all_data.items():
        dates, highs, lows, closes, volumes = s["d"], s["h"], s["l"], s["c"], s["v"]
        n = len(dates)
        if n < volume_lookback + 2: continue
        if not _check_liquidity(volumes, closes, n): continue
        scan_from = max(volume_lookback, n - max_ep_age_days)
        for i in range(scan_from, n):
            prev_high = highs[i - 1]
            today_low = lows[i]
            if today_low <= prev_high: continue
            gap_pct = (today_low - prev_high) / prev_high * 100
            if gap_pct < min_gap_pct: continue
            avg_vol = sum(volumes[i - volume_lookback:i]) / volume_lookback
            if avg_vol == 0: continue
            vol_x = volumes[i] / avg_vol
            if vol_x < volume_spike_x: continue
            gap_lower = prev_high
            consol_count = 0
            ep_intact = True
            for j in range(i + 1, min(i + max_consolidation + 1, n)):
                if closes[j] < gap_lower:
                    ep_intact = False; break
                consol_count += 1
            if not ep_intact: continue
            if consol_count >= max_consolidation: continue
            last_idx  = min(i + consol_count, n - 1)
            ep_close  = closes[i]
            ep_5d_idx = min(i + 5, n - 1)
            ep_5d_return = round((closes[ep_5d_idx] - ep_close) / ep_close * 100, 2) if ep_5d_idx > i else ""
            ep_return    = round((closes[last_idx]  - ep_close) / ep_close * 100, 2) if ep_close else 0.0
            ep_high = highs[i]
            never_broke_high = all(closes[j] <= ep_high for j in range(i + 1, last_idx + 1))
            ep_type = "Consolidating below EP high" if never_broke_high else "EP Follow-through"
            signals.append({
                "symbol": sym, "ep_date": dates[i],
                "gap_lower": round(gap_lower, 2), "gap_pct": round(gap_pct, 2),
                "vol_spike_x": round(vol_x, 1),
                "ep_candle_high": round(highs[i], 2), "ep_candle_low": round(today_low, 2),
                "ep_candle_close": round(ep_close, 2),
                "ep_return": ep_return, "ep_5d_return": ep_5d_return,
                "last_close": round(closes[last_idx], 2), "last_date": dates[last_idx],
                "consolidation": consol_count, "ep_type": ep_type,
            })
    seen: dict[str, dict] = {}
    for sig in signals:
        sym = sig["symbol"]
        if sym not in seen or sig["ep_date"] > seen[sym]["ep_date"]:
            seen[sym] = sig
    return list(seen.values())

def _detect_post_result_thrust(all_data,result_calendar,min_price_ch_pct=1.5,volume_spike_x=1.5,close_position_min=0.5,volume_lookback=20,max_result_age_days=30):
    today_str=today_ist(); cutoff=(date.fromisoformat(today_str)-timedelta(days=max_result_age_days)).isoformat()
    sym_to_result_date:dict[str,str]={}
    for date_str,syms in result_calendar.items():
        if date_str<cutoff: continue
        for sym in syms:
            if sym not in sym_to_result_date or date_str>sym_to_result_date[sym]: sym_to_result_date[sym]=date_str
    signals=[]
    for sym,result_date in sym_to_result_date.items():
        if sym not in all_data: continue
        s=all_data[sym]; dates=s["d"]; opens=s["o"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<volume_lookback+2: continue
        if result_date not in dates: continue
        ri=dates.index(result_date); ti=ri+1
        if ti>=n: continue
        lookback=min(volume_lookback,ri)
        if lookback==0: continue
        avg_vol=sum(volumes[ri-lookback:ri])/lookback
        if avg_vol==0: continue
        result_day_ch=round((closes[ri]-closes[ri-1])/closes[ri-1]*100,2) if ri>0 and closes[ri-1] else 0.0
        result_day_vol_x=round(volumes[ri]/avg_vol,1)
        if lows[ti]>highs[ri]: continue
        prev_close=closes[ri]
        if prev_close==0: continue
        price_ch_pct=(closes[ti]-prev_close)/prev_close*100
        if price_ch_pct<min_price_ch_pct: continue
        vol_x=volumes[ti]/avg_vol
        if vol_x<volume_spike_x: continue
        candle_range=highs[ti]-lows[ti]
        close_pos=(closes[ti]-lows[ti])/candle_range if candle_range>0 else 1.0
        if close_pos<close_position_min: continue
        if abs(result_day_ch)<1.5 and result_day_vol_x<2.0: reaction_type="AH Result → T+1 Primary"
        elif result_day_ch>=1.5: reaction_type="IH Result → T+1 Follow-through"
        else: reaction_type="Mixed"
        signals.append({"symbol":sym,"result_date":result_date,"result_day_ch":result_day_ch,"result_day_vol_x":result_day_vol_x,"t1_date":dates[ti],"t1_open":round(opens[ti],2),"t1_high":round(highs[ti],2),"t1_low":round(lows[ti],2),"t1_close":round(closes[ti],2),"price_ch_pct":round(price_ch_pct,2),"vol_pct":f"+{round((vol_x-1)*100)}%","close_position":round(close_pos*100,1),"reaction_type":reaction_type})
    order={"AH Result → T+1 Primary":0,"IH Result → T+1 Follow-through":1,"Mixed":2}
    signals.sort(key=lambda x:(order.get(x["reaction_type"],9),-x["price_ch_pct"]))
    return signals


def _calculate_rs(all_data,history_days=30):
    all_syms=list(all_data.keys()); result={}; day_scores={}
    for sym,s in all_data.items():
        closes=s["c"]; n=len(closes); scores=[]
        for day_offset in range(history_days,-1,-1):
            idx=n-1-day_offset
            if idx<63: scores.append(None); continue
            def ret(lookback):
                prev_idx=idx-lookback
                if prev_idx<0: return None
                prev=closes[prev_idx]
                if prev==0: return None
                return (closes[idx]-prev)/prev*100
            p63=ret(63); p126=ret(126); p189=ret(189); p252=ret(252)
            if p252 is not None and p189 is not None and p126 is not None and p63 is not None: composite=(p63*2+p126+p189+p252)/5
            elif p189 is not None and p126 is not None and p63 is not None: composite=(p63*2+p126+p189)/4
            elif p126 is not None and p63 is not None: composite=(p63*2+p126)/3
            elif p63 is not None: composite=p63
            else: scores.append(None); continue
            scores.append(composite)
        day_scores[sym]=scores
    n_days=history_days+1; rs_history={sym:[] for sym in all_syms}
    for d in range(n_days):
        day_composites={sym:day_scores[sym][d] for sym in all_syms if day_scores[sym][d] is not None}
        if not day_composites:
            for sym in all_syms: rs_history[sym].append(None)
            continue
        sorted_syms=sorted(day_composites,key=lambda x:day_composites[x]); total=len(sorted_syms)
        ranks={sym:round((i+1)/total*99) for i,sym in enumerate(sorted_syms)}
        for sym in all_syms: rs_history[sym].append(ranks.get(sym))
    final_composites={sym:day_scores[sym][-1] for sym in all_syms if day_scores[sym][-1] is not None}
    final_sorted=sorted(final_composites,key=lambda x:final_composites[x]); final_total=len(final_sorted)
    final_rank_pos={sym:i+1 for i,sym in enumerate(final_sorted)}
    for sym in all_syms:
        hist=rs_history[sym]; current_rs=next((v for v in reversed(hist) if v is not None),None)
        result[sym]={"rs":current_rs,"rs_rank":final_rank_pos.get(sym),"rs_total":final_total,"history":hist}
    return result


def _build_rs_history_json(all_data,rs_data):
    from datetime import date as dt
    sample_sym=max(all_data.keys(),key=lambda s:len(all_data[s].get("d",[])))
    dates=all_data[sample_sym]["d"]; n=len(dates)
    history_len=len(next(iter(rs_data.values()))["history"]); start_idx=n-history_len
    def fmt_date(d_str): return dt.fromisoformat(d_str).strftime("%-d-%b-%y")
    date_labels=[(i,fmt_date(dates[start_idx+i])) for i in range(history_len) if 0<=start_idx+i<n]
    rows=[]
    for sym,v in rs_data.items():
        row={"Stock Name":sym}
        for i,label in date_labels:
            if v["history"][i] is not None: row[label]=v["history"][i]
        rows.append(row)
    return rows


def _calculate_mswing(all_data,history_days=60):
    result={}
    for sym,s in all_data.items():
        closes=s["c"]; n=len(closes); history=[]
        for day_offset in range(history_days,-1,-1):
            idx=n-1-day_offset
            if idx<51: history.append(None); continue
            try:
                ret_20=(closes[idx]-closes[idx-20])/closes[idx-20]*100/20
                ret_50=(closes[idx]-closes[idx-50])/closes[idx-50]*100/50
                history.append(round(ret_20+ret_50,4))
            except ZeroDivisionError: history.append(None)
        valid=[v for v in history[-9:] if v is not None]
        avg9=round(sum(valid)/len(valid),4) if valid else None
        result[sym]={"mswing":history[-1] if history else None,"mswing_avg9":avg9,"mswing_history":history}
    return result


def _build_mswing_json(all_data,mswing_data):
    from datetime import date as dt
    sample_sym=max(all_data.keys(),key=lambda s:len(all_data[s].get("d",[])))
    dates=all_data[sample_sym]["d"]; n=len(dates)
    history_len=len(next(iter(mswing_data.values()))["mswing_history"]); start_idx=n-history_len
    def fmt_date(d_str): return dt.fromisoformat(d_str).strftime("%-d-%b-%y")
    date_labels=[(i,fmt_date(dates[start_idx+i])) for i in range(history_len) if 0<=start_idx+i<n]
    rows=[]
    for sym,v in mswing_data.items():
        row={"Stock Name":sym}
        for i,label in date_labels:
            if v["mswing_history"][i] is not None: row[label]=v["mswing_history"][i]
        rows.append(row)
    return rows


async def run_ep_scan() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting"); return

    log.info(f"━━━ EP + Post-Result Scan  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        today_symbols = await get_result_symbols_finedge(client)
        if today_symbols:
            await save_result_calendar(client, today_symbols, today)

        ohlc_tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
        ohlc_results, screener_raw, fund_raw, cal_raw = await asyncio.gather(
            asyncio.gather(*ohlc_tasks, return_exceptions=True),
            r2_download(client, "screener.json"),
            r2_download_fund(client),
            r2_download(client, "result_calendar.json"),
        )

        all_data: dict = {}
        for i, res in enumerate(ohlc_results):
            if isinstance(res, Exception): log.warning(f"  ohlc_{i+1}.json error: {res}")
            elif res and "stocks" in res: all_data.update(res["stocks"])
        log.info(f"Loaded {len(all_data)} stocks")

        screener: dict = {}
        if isinstance(screener_raw, list):
            for row in screener_raw:
                sym = row.get("Stocks", "").strip()
                if not sym: continue
                try: sc=float(row.get("SALES CH%",0))*100; sales_ch=f"+{sc:.1f}%" if sc>=0 else f"{sc:.1f}%"
                except: sales_ch=""
                try: ec=float(row.get("EPS CHANGE",0))*100; eps_ch=f"+{ec:.1f}%" if ec>=0 else f"{ec:.1f}%"
                except: eps_ch=""
                pat_cols=["NR7","WIB","DIB","MCP","W-MCP","HVQ","VD","PullBack","ATR Tightness","Volume footprint","Launchpad","HLR","BS","GAPUP","PP","HPBC","TL/HL BO","3WTC"]
                combined=set()
                for p in (row.get("Patterns","") or "").split("||"):
                    p=p.strip()
                    if p: combined.add(p)
                for col in pat_cols:
                    v=row.get(col,"")
                    if v and v not in ("",None,0,"No"): combined.add(v if isinstance(v,str) else col)
                screener[sym]={"sales_ch":sales_ch,"eps_ch":eps_ch,"patterns":"||".join(sorted(combined)),"sector":row.get("SECTOR",""),"rs":row.get("RS Rating",""),"ltp":row.get("LTP","")}

        fund_lookup: dict = {}
        if isinstance(fund_raw, dict): fund_lookup = fund_raw
        elif isinstance(fund_raw, list): fund_lookup = {d["symbol"]:d for d in fund_raw if d.get("symbol")}
        log.info(f"Fundamentals loaded: {len(fund_lookup)} stocks")

        result_calendar: dict = cal_raw if isinstance(cal_raw, dict) else {}

        signals = _detect_ep(all_data)
        signals.sort(key=lambda x:(x["ep_date"],x["gap_pct"]),reverse=True)
        for sig in signals:
            sym=sig["symbol"]; sc=screener.get(sym,{})
            sig.update({"sales_ch":sc.get("sales_ch",""),"eps_ch":sc.get("eps_ch",""),"patterns":sc.get("patterns",""),"sector":sc.get("sector",""),"rs":sc.get("rs",""),"ltp":sc.get("ltp","")})
            fund=fund_lookup.get(sym,{}); pl_qtr=fund.get("pl_quarterly",[])
            sig["q_name"]=pl_qtr[0].get("header","") if pl_qtr else ""
            vol_x=sig.pop("vol_spike_x",1); sig["vol_pct"]=f"+{round((vol_x-1)*100)}%"
        log.info(f"EP signals: {len(signals)}")

        pr_signals: list[dict] = []
        if result_calendar:
            pr_signals = _detect_post_result_thrust(all_data, result_calendar)
            for sig in pr_signals:
                sym=sig["symbol"]; sc=screener.get(sym,{}); fund=fund_lookup.get(sym,{})
                sig.update({"sales_ch":sc.get("sales_ch",""),"eps_ch":sc.get("eps_ch",""),"patterns":sc.get("patterns",""),"sector":sc.get("sector",""),"rs":sc.get("rs",""),"ltp":sc.get("ltp","")})
                pl_qtr=fund.get("pl_quarterly",[]); sig["q_name"]=pl_qtr[0].get("header","") if pl_qtr else ""
            ah=sum(1 for s in pr_signals if "AH" in s["reaction_type"]); ih=sum(1 for s in pr_signals if "IH" in s["reaction_type"])
            log.info(f"Post-Result signals: {len(pr_signals)}  (AH→T+1: {ah}  IH→T+1: {ih})")

        rs_data = _calculate_rs(all_data, history_days=60)
        for sig in signals: sig["rs_calc"]=rs_data.get(sig["symbol"],{}).get("rs")
        for sig in pr_signals: sig["rs_calc"]=rs_data.get(sig["symbol"],{}).get("rs")
        rs_history_list = _build_rs_history_json(all_data, rs_data)

        mswing_data = _calculate_mswing(all_data, history_days=ROLLING_DAYS - 50)
        mswing_list = _build_mswing_json(all_data, mswing_data)
        for sig in signals:
            sym=sig["symbol"]; sig["mswing"]=mswing_data.get(sym,{}).get("mswing"); sig["mswing_avg9"]=mswing_data.get(sym,{}).get("mswing_avg9")
        for sig in pr_signals:
            sym=sig["symbol"]; sig["mswing"]=mswing_data.get(sym,{}).get("mswing"); sig["mswing_avg9"]=mswing_data.get(sym,{}).get("mswing_avg9")

        await asyncio.gather(
            r2_upload(client, "ep_signals.json",          json.dumps({"updated":today,"count":len(signals),"signals":signals})),
            r2_upload(client, "rs_ratings.json",          json.dumps({"updated":today,"count":len(rs_data),"stocks":rs_data})),
            r2_upload(client, "rs_history.json",          json.dumps(rs_history_list)),
            r2_upload(client, "mswing.json",              json.dumps(mswing_list)),
            r2_upload(client, "post_result_signals.json", json.dumps({"updated":today,"count":len(pr_signals),"ah_count":sum(1 for s in pr_signals if "AH" in s["reaction_type"]),"ih_count":sum(1 for s in pr_signals if "IH" in s["reaction_type"]),"signals":pr_signals})),
        )
        log.info(f"✅ EP:{len(signals)}  PostResult:{len(pr_signals)}  RS+MSwing:{len(rs_data)}")
    log.info("━━━ EP + Post-Result Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# HLR + PULLBACK + PATTERN SCANNERS (unchanged from original)
# ══════════════════════════════════════════════════════════════

def _calc_ema(closes,period):
    if len(closes)<period: return [None]*len(closes)
    ema=[None]*len(closes); k=2/(period+1)
    ema[period-1]=sum(closes[:period])/period
    for i in range(period,len(closes)): ema[i]=closes[i]*k+ema[i-1]*(1-k)
    return ema

def _detect_pullback(all_data,length_pull=4,min_swing_range_pct=10.0,min_pullback_pct=5.0,ema_proximity_pct=1.0,max_candle_range_pct=6.0):
    signals=[]
    for sym,s in all_data.items():
        dates=s["d"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<60: continue
        if not _check_liquidity(volumes,closes,n): continue
        ema10=_calc_ema(closes,10); ema21=_calc_ema(closes,21); ema50=_calc_ema(closes,50)
        if any(v is None for v in [ema10[-1],ema21[-1],ema50[-1]]): continue
        ema12=_calc_ema(closes,12); ema26=_calc_ema(closes,26)
        macd_line=[(ema12[i]-ema26[i]) if ema12[i] is not None and ema26[i] is not None else None for i in range(n)]
        if len([v for v in macd_line if v is not None])<9: continue
        macd_arr=[v if v is not None else 0.0 for v in macd_line]; macd_signal=_calc_ema(macd_arr,9)
        last_swing_high_price=last_swing_high_bar=last_swing_low_price=last_swing_low_bar=None
        for i in range(length_pull,n-length_pull):
            if all(highs[i]>=highs[i-k] for k in range(1,length_pull+1)) and all(highs[i]>=highs[i+k] for k in range(1,length_pull+1)):
                last_swing_high_price=highs[i]; last_swing_high_bar=i
            if all(lows[i]<=lows[i-k] for k in range(1,length_pull+1)) and all(lows[i]<=lows[i+k] for k in range(1,length_pull+1)):
                last_swing_low_price=lows[i]; last_swing_low_bar=i
        if last_swing_high_price is None or last_swing_low_price is None: continue
        i=n-1
        if last_swing_high_bar is None or last_swing_low_bar is None: continue
        if last_swing_high_bar<=last_swing_low_bar: continue
        swing_range_pct=(last_swing_high_price-last_swing_low_price)/last_swing_low_price*100
        if swing_range_pct<min_swing_range_pct: continue
        pullback_pct=(last_swing_high_price-lows[i])/last_swing_high_price*100
        if pullback_pct<min_pullback_pct: continue
        e10=ema10[i]; e21=ema21[i]
        near_ema10=abs(lows[i]-e10)/e10*100<=ema_proximity_pct or abs(closes[i]-e10)/e10*100<=ema_proximity_pct
        near_ema21=abs(lows[i]-e21)/e21*100<=ema_proximity_pct or abs(closes[i]-e21)/e21*100<=ema_proximity_pct
        reversal_ema10=lows[i]<e10 and closes[i]>e10; reversal_ema21=lows[i]<e21 and closes[i]>e21
        if not(near_ema10 or near_ema21 or reversal_ema10 or reversal_ema21): continue
        e50=ema50[i]
        if not(e21>e50 and e10>e50 and closes[i]>e21): continue
        if i<5 or any(ema50[i-k] is None for k in range(6)): continue
        if not all(ema50[i-k]>ema50[i-k-1] for k in range(5)): continue
        candle_range_pct=(highs[i]-lows[i])/lows[i]*100 if lows[i]>0 else 0
        if candle_range_pct>=max_candle_range_pct: continue
        if macd_line[i] is None or macd_signal[i] is None: continue
        if macd_line[i]<macd_signal[i]: continue
        ema_touch="Reversal" if (reversal_ema10 or reversal_ema21) else "Near EMA10" if near_ema10 else "Near EMA21"
        signals.append({"symbol":sym,"date":dates[i],"close":round(closes[i],2),"swing_high":round(last_swing_high_price,2),"swing_low":round(last_swing_low_price,2),"swing_range_pct":round(swing_range_pct,2),"pullback_pct":round(pullback_pct,2),"ema10":round(e10,2),"ema21":round(e21,2),"ema50":round(e50,2),"ema_touch":ema_touch,"candle_range_pct":round(candle_range_pct,2),"macd":round(macd_line[i],4),"macd_signal":round(macd_signal[i],4)})
    return signals

def _detect_hlr(all_data,swing_n=5,cluster_pct=2.0,near_pct=4.0,consol_days=5,consol_pct=4.0):
    signals=[]
    for sym,s in all_data.items():
        dates=s["d"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<swing_n*2+consol_days+2: continue
        if not _check_liquidity(volumes,closes,n): continue
        vol_lookback=min(50,n-1)
        avg_vol_50=sum(volumes[-vol_lookback-1:-1])/vol_lookback if vol_lookback>0 else 0
        today_vol=volumes[-1]
        vol_spike=today_vol/avg_vol_50 if avg_vol_50>0 and vol_lookback>=20 else None
        swing_highs=[]
        for i in range(swing_n,n-swing_n):
            if all(highs[i]>=highs[i-k] for k in range(1,swing_n+1)) and all(highs[i]>=highs[i+k] for k in range(1,swing_n+1)):
                sh_price=highs[i]
                broken_before=any(closes[j]>sh_price for j in range(i+1,n-1))
                broke_today=closes[-1]>sh_price
                if broken_before: continue
                swing_highs.append((sh_price,dates[i],"BO" if broke_today else "valid"))
        if not swing_highs: continue
        swing_highs.sort(key=lambda x:x[0],reverse=True); used=[False]*len(swing_highs); levels=[]
        for i,(h,d,tag) in enumerate(swing_highs):
            if used[i]: continue
            cluster=[(h,d,tag)]
            for j in range(i+1,len(swing_highs)):
                if not used[j] and abs(swing_highs[j][0]-h)/h*100<=cluster_pct: cluster.append(swing_highs[j]); used[j]=True
            used[i]=True
            level=max(c[0] for c in cluster); zone_low=min(c[0] for c in cluster)
            is_zone=len(cluster)>=2; touches=len(cluster)
            cluster_tag="BO" if any(c[2]=="BO" for c in cluster) else "valid"
            touch_pts=sorted([{"date":c[1],"price":round(c[0],2)} for c in cluster],key=lambda x:x["date"])
            levels.append((level,zone_low,touches,is_zone,touch_pts,cluster_tag))
        curr_close=closes[-1]; curr_date=dates[-1]
        if n>=consol_days:
            recent_high=max(highs[-consol_days:]); recent_low=min(lows[-consol_days:])
            range_pct=(recent_high-recent_low)/curr_close*100; is_consol=range_pct<consol_pct
        else: range_pct=0; is_consol=False
        for (level,zone_low,touches,is_zone,touch_pts,cluster_tag) in levels:
            dist_pct=(level-curr_close)/level*100
            if cluster_tag=="BO": vol_ok=(vol_spike is None) or (vol_spike>=2.0); state="BO" if vol_ok else "Near HLR"
            elif 0<=dist_pct<=near_pct: state="Consolidating near HLR" if is_consol else "Near HLR"
            else: continue
            signals.append({"symbol":sym,"state":state,"resistance":round(level,2),"zone_low":round(zone_low,2),"is_zone":is_zone,"touches":touches,"touch_points":touch_pts,"dist_pct":round(dist_pct,2),"last_close":round(curr_close,2),"last_date":curr_date,"consol_range":round(range_pct,2),"vol_spike":round(vol_spike,1) if vol_spike is not None else None})
    return signals

async def run_hlr_scan() -> None:
    today=today_ist()
    if not is_trading_day(today): log.info(f"⏭  {today} is not a trading day — exiting"); return
    log.info(f"━━━ HLR + Pullback Scan  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP,BSE_ISIN_MAP,BSE_META
        ISIN_MAP,BSE_ISIN_MAP,BSE_META=await build_isin_map(client)
        all_data=await download_all_chunks(client); log.info(f"Loaded {len(all_data)} stocks")
        hlr_signals=_detect_hlr(all_data)
        order={"BO":0,"Consolidating near HLR":1,"Near HLR":2}
        hlr_signals.sort(key=lambda x:(order.get(x["state"],9),-x["touches"]))
        bo=sum(1 for s in hlr_signals if s["state"]=="BO"); consol=sum(1 for s in hlr_signals if s["state"]=="Consolidating near HLR"); near=sum(1 for s in hlr_signals if s["state"]=="Near HLR")
        log.info(f"HLR — BO:{bo} Consolidating:{consol} Near:{near} Total:{len(hlr_signals)}")
        pb_signals=_detect_pullback(all_data); pb_signals.sort(key=lambda x:x["pullback_pct"],reverse=True)
        log.info(f"Pullback signals: {len(pb_signals)}")
        await asyncio.gather(
            r2_upload(client,"hlr_signals.json",json.dumps({"updated":today,"count":len(hlr_signals),"bo":bo,"consolidating":consol,"near":near,"signals":hlr_signals})),
            r2_upload(client,"pullback_signals.json",json.dumps({"updated":today,"count":len(pb_signals),"signals":pb_signals})),
        )
        log.info(f"✅ HLR:{len(hlr_signals)}  Pullback:{len(pb_signals)}")
    log.info("━━━ HLR + Pullback Scan complete ━━━")

def _build_weekly(dates,opens,highs,lows,closes,volumes):
    from datetime import date as dt
    weekly={}
    for d,o,h,l,c,v in zip(dates,opens,highs,lows,closes,volumes):
        key=dt.fromisoformat(d).isocalendar()[:2]
        if key not in weekly: weekly[key]={"o":o,"h":h,"l":l,"c":c,"v":v,"d":d}
        else: weekly[key]["h"]=max(weekly[key]["h"],h); weekly[key]["l"]=min(weekly[key]["l"],l); weekly[key]["c"]=c; weekly[key]["v"]+=v
    return weekly

def _detect_patterns(all_data,min_volume=2500,coil_min_babies=3,tight_close_weeks=3,tight_close_pct=2.0):
    from datetime import date as dt; signals=[]
    for sym,s in all_data.items():
        dates=s["d"]; opens=s["o"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<10: continue
        if not _check_liquidity(volumes,closes,n): continue
        if volumes[-1]<min_volume: continue
        today=dates[-1]
        if highs[-1]<=highs[-2] and lows[-1]>=lows[-2]: signals.append({"symbol":sym,"pattern":"Inside Bar","date":today,"high":round(highs[-1],2),"low":round(lows[-1],2),"prev_high":round(highs[-2],2),"prev_low":round(lows[-2],2)})
        if n>=3 and highs[-1]<=highs[-2] and lows[-1]>=lows[-2] and highs[-2]<=highs[-3] and lows[-2]>=lows[-3]: signals.append({"symbol":sym,"pattern":"Double Inside Bar","date":today,"high":round(highs[-1],2),"low":round(lows[-1],2),"mother_high":round(highs[-3],2),"mother_low":round(lows[-3],2)})
        if n>=7:
            today_range=highs[-1]-lows[-1]; past_ranges=[highs[-i]-lows[-i] for i in range(2,8)]
            if today_range<=min(past_ranges): signals.append({"symbol":sym,"pattern":"NR7","date":today,"range":round(today_range,2),"high":round(highs[-1],2),"low":round(lows[-1],2)})
        seen_mothers=set()
        for m_idx in range(n-coil_min_babies-1,max(0,n-60),-1):
            m_high=highs[m_idx]; m_low=lows[m_idx]; m_key=round(m_high*200)
            if m_key in seen_mothers: continue
            baby_count=0; coil_state="Coiling"
            for b in range(m_idx+1,n):
                if highs[b]>m_high: coil_state="Upper BO"; break
                elif lows[b]<m_low: coil_state="Lower BD"; break
                else: baby_count+=1
            if baby_count>=coil_min_babies and coil_state=="Coiling":
                seen_mothers.add(m_key)
                signals.append({"symbol":sym,"pattern":f"{baby_count} Bar MCP" if baby_count<=6 else "Mini Coil","date":today,"mcp_high":round(m_high,2),"mcp_low":round(m_low,2),"baby_count":baby_count,"coil_state":coil_state,"mother_date":dates[m_idx]})
        weekly=_build_weekly(dates,opens,highs,lows,closes,volumes)
        if not weekly: continue
        current_week=dt.fromisoformat(today).isocalendar()[:2]
        past_weeks=sorted(k for k in weekly if k<current_week)
        if len(past_weeks)<2: continue
        lw=weekly[past_weeks[-1]]; lw2=weekly[past_weeks[-2]]
        if lw["h"]<=lw2["h"] and lw["l"]>=lw2["l"]:
            signals.append({"symbol":sym,"pattern":"Weekly IB","date":today,"w_high":round(lw["h"],2),"w_low":round(lw["l"],2),"w_close":round(lw["c"],2),"prev_w_high":round(lw2["h"],2),"prev_w_low":round(lw2["l"],2)})
            if len(past_weeks)>=3:
                lw3=weekly[past_weeks[-3]]
                if lw2["h"]<=lw3["h"] and lw2["l"]>=lw3["l"]: signals.append({"symbol":sym,"pattern":"Weekly Double IB","date":today,"w_high":round(lw["h"],2),"w_low":round(lw["l"],2),"mother_w_high":round(lw3["h"],2),"mother_w_low":round(lw3["l"],2)})
        if len(past_weeks)>=7:
            lw_range=lw["h"]-lw["l"]; past_ranges=[weekly[past_weeks[-i]]["h"]-weekly[past_weeks[-i]]["l"] for i in range(2,8)]
            if lw_range<=min(past_ranges): signals.append({"symbol":sym,"pattern":"Weekly NR7","date":today,"w_range":round(lw_range,2),"w_high":round(lw["h"],2),"w_low":round(lw["l"],2)})
        if len(past_weeks)>=tight_close_weeks:
            last_n_closes=[weekly[past_weeks[-i]]["c"] for i in range(1,tight_close_weeks+1)]
            tc_range=(max(last_n_closes)-min(last_n_closes))/min(last_n_closes)*100
            if tc_range<=tight_close_pct: signals.append({"symbol":sym,"pattern":"Weekly Tight Close","date":today,"closes":[round(c,2) for c in last_n_closes],"range_pct":round(tc_range,2)})
    return signals

async def run_pattern_scan() -> None:
    today=today_ist()
    if not is_trading_day(today): log.info(f"⏭  {today} is not a trading day — exiting"); return
    log.info(f"━━━ Pattern Scan  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP,BSE_ISIN_MAP,BSE_META
        ISIN_MAP,BSE_ISIN_MAP,BSE_META=await build_isin_map(client)
        all_data=await download_all_chunks(client); log.info(f"Loaded {len(all_data)} stocks")
        signals=_detect_patterns(all_data)
        from collections import Counter; counts=Counter(s["pattern"] for s in signals)
        for pat,cnt in sorted(counts.items()): log.info(f"  {pat}: {cnt}")
        log.info(f"Total: {len(signals)} signals")
        await r2_upload(client,"pattern_signals.json",json.dumps({"updated":today,"count":len(signals),"summary":dict(counts),"signals":signals}))
    log.info("━━━ Pattern Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "daily":              asyncio.run(run_daily())
        case "today":              asyncio.run(run_today())
        case "full":               asyncio.run(run_full())
        case "status":             asyncio.run(run_status())
        case "fund_daily":         asyncio.run(run_fund_daily())
        case "fund_full":          asyncio.run(run_fund_full(0))
        case "fund_full_1":        asyncio.run(run_fund_full(1))
        case "fund_full_2":        asyncio.run(run_fund_full(2))
        case "fund_full_3":        asyncio.run(run_fund_full(3))
        case "fund_full_4":        asyncio.run(run_fund_full(4))
        case "fund_full_5":        asyncio.run(run_fund_full(5))
        case "fund_full_6":        asyncio.run(run_fund_full(6))
        case "fund_full_7":        asyncio.run(run_fund_full(7))
        case "fund_full_8":        asyncio.run(run_fund_full(8))
        case "fund_full_9":        asyncio.run(run_fund_full(9))
        case "fund_full_10":       asyncio.run(run_fund_full(10))
        case "enrich_master":      asyncio.run(run_enrich_master(0))
        case "enrich_master_1":    asyncio.run(run_enrich_master(1))
        case "enrich_master_2":    asyncio.run(run_enrich_master(2))
        case "enrich_master_3":    asyncio.run(run_enrich_master(3))
        case "enrich_master_4":    asyncio.run(run_enrich_master(4))
        case "enrich_master_5":    asyncio.run(run_enrich_master(5))
        case "enrich_master_6":    asyncio.run(run_enrich_master(6))
        case "enrich_master_7":    asyncio.run(run_enrich_master(7))
        case "enrich_master_8":    asyncio.run(run_enrich_master(8))
        case "enrich_master_9":    asyncio.run(run_enrich_master(9))
        case "enrich_master_10":   asyncio.run(run_enrich_master(10))
        case "bse_profiles":       asyncio.run(run_bse_profiles(0))
        case "bse_profiles_1":     asyncio.run(run_bse_profiles(1))
        case "bse_profiles_2":     asyncio.run(run_bse_profiles(2))
        case "bse_profiles_3":     asyncio.run(run_bse_profiles(3))
        case "bse_profiles_4":     asyncio.run(run_bse_profiles(4))
        case "bse_profiles_5":     asyncio.run(run_bse_profiles(5))
        case "bse_profiles_6":     asyncio.run(run_bse_profiles(6))
        case "bse_profiles_7":     asyncio.run(run_bse_profiles(7))
        case "bse_profiles_8":     asyncio.run(run_bse_profiles(8))
        case "bse_profiles_9":     asyncio.run(run_bse_profiles(9))
        case "bse_profiles_10":    asyncio.run(run_bse_profiles(10))
        case "ep_scan":            asyncio.run(run_ep_scan())
        case "hlr_scan":           asyncio.run(run_hlr_scan())
        case "pattern_scan":       asyncio.run(run_pattern_scan())
        case _:
            print(__doc__)
            sys.exit(1)
