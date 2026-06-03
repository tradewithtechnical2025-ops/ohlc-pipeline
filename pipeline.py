#!/usr/bin/env python3
"""
NSE OHLC + Fundamentals Pipeline — GitHub Actions (Finedge powered)

Usage:
  python pipeline.py daily          # prev-day OHLC           (4:00 PM IST, weekdays)
  python pipeline.py today          # T+0 intraday candle      (4:50 PM IST, weekdays)
  python pipeline.py full           # initial 1.5yr OHLC load  (manual, once)
  python pipeline.py status         # print R2 chunk summary
  python pipeline.py fund_daily     # result stocks update      (4:30 PM IST, weekdays)
  python pipeline.py fund_full      # one-time full load        (manual)
  python pipeline.py fund_full_1..10
  python pipeline.py ep_scan        # EP + Post-Result + RS scan (4:35 PM IST, weekdays)
  python pipeline.py hlr_scan       # HLR + Pullback scan        (4:20 PM IST, weekdays)
  python pipeline.py pattern_scan   # Price action pattern scan   (4:25 PM IST, weekdays)
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

UPSTOX_TOKEN  = os.environ["UPSTOX_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]
FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]

FINEDGE_BASE  = "https://data.finedgeapi.com/api/v1"

ROLLING_DAYS      = 548
R2_CHUNKS         = 8
CONCURRENCY       = 5
FUND_CONCURRENCY  = 4
RETRY             = 5
RATE_DELAY        = 0.5
FINEDGE_DELAY     = 0.25
TODAY_CONCURRENCY = 10
TODAY_RATE_DELAY  = 0.4

HERE = Path(__file__).parent

with open(HERE / "nse_holidays.json") as f:
    NSE_HOLIDAYS: set[str] = set(json.load(f))

WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

ISIN_MAP:     dict[str, str] = {}
BSE_ISIN_MAP: dict[str, str] = {}
BSE_META:     dict[str, dict] = {}

# Index symbols on R2
INDEX_SYMBOLS = {
    "nifty50"    : "NIFTY50",
    "nifty500"   : "NIF500",
    "smallmid400": "NIFMID400",
}


# ══════════════════════════════════════════════════════════════
# INSTRUMENT MAP
# ══════════════════════════════════════════════════════════════

async def build_isin_map(client):
    log.info("Fetching classification.json from R2…")
    master = await r2_download(client, "classification.json")
    if not master or not isinstance(master, list):
        raise RuntimeError("classification.json missing or invalid in R2!")
    nse_map = {}; bse_map = {}; bse_meta = {}
    for stock in master:
        sym      = str(stock.get("symbol", "")).strip().upper()
        exchange = str(stock.get("exchange", "")).strip()
        name     = str(stock.get("name", "")).strip()
        if not sym: continue
        if exchange == "NSE":   nse_map[sym] = sym
        elif exchange == "BSE": bse_map[sym] = sym; bse_meta[sym] = {"name": name}
    log.info(f"✓ NSE: {len(nse_map)}  BSE: {len(bse_map)}")
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
        if is_trading_day(dt.isoformat()): return dt.isoformat()
        dt -= timedelta(days=1)
    raise RuntimeError("No trading day in last 14 days")

def prev_trading_day(d: str) -> str:
    dt = date.fromisoformat(d) - timedelta(days=1)
    for _ in range(14):
        if is_trading_day(dt.isoformat()): return dt.isoformat()
        dt -= timedelta(days=1)
    raise RuntimeError(f"No prev trading day before {d}")

def rolling_cutoff(anchor: str) -> str:
    return (date.fromisoformat(anchor) - timedelta(days=ROLLING_DAYS)).isoformat()


# ══════════════════════════════════════════════════════════════
# FINEDGE API
# ══════════════════════════════════════════════════════════════

async def fetch_ohlc(client, sem, sym, from_year, to_year):
    url = f"{FINEDGE_BASE}/daily-quotes/{sym}"
    params = {"from": from_year, "to": to_year, "token": FINEDGE_TOKEN}
    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            try: r = await client.get(url, params=params, timeout=30)
            except httpx.RequestError as e:
                log.warning(f"{sym}: network error ({e}), retry {attempt+1}")
                await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 401: log.error("❌ FINEDGE TOKEN INVALID"); sys.exit(1)
        if r.status_code == 429: log.warning(f"{sym}: rate limited — 15s"); await asyncio.sleep(15); continue
        if r.status_code in (502,503,504): await asyncio.sleep(2**attempt); continue
        if r.status_code != 200: return sym, None
        try: payload = r.json()
        except: return sym, None
        raw = payload.get("price", [])
        if not raw: return sym, None
        candles = sorted([
            {"d":c["quote_date"],"o":c["open_price"],"h":c["high_price"],
             "l":c["low_price"],"c":c["close_price"],"v":c["volume"],"oi":0}
            for c in raw if c.get("quote_date") and None not in
            (c.get("open_price"),c.get("high_price"),c.get("low_price"),c.get("close_price"))
        ], key=lambda x: x["d"])
        return sym, candles
    return sym, None


async def fetch_today_candle(client, sem, sym):
    data = await _finedge_get(client, sem, "quote", {"symbol": sym})
    if not data or sym not in data: return sym, None
    q = data[sym]
    o,h,l,c = q.get("open_price"),q.get("high_price"),q.get("low_price"),q.get("current_price")
    if None in (o,h,l,c): return sym, None
    return sym, {"d":today_ist(),"o":o,"h":h,"l":l,"c":c,"v":q.get("volume",0),"oi":0}


async def _finedge_get(client, sem, path, params):
    params["token"] = FINEDGE_TOKEN
    url = f"{FINEDGE_BASE}/{path}"
    async with sem:
        for attempt in range(RETRY):
            await asyncio.sleep(FINEDGE_DELAY)
            try: r = await client.get(url, params=params, timeout=30)
            except httpx.RequestError as e:
                log.warning(f"  Finedge network error: {e}, retry {attempt+1}")
                await asyncio.sleep(2**attempt); continue
            if r.status_code == 401: log.error("❌ FINEDGE TOKEN INVALID"); sys.exit(1)
            if r.status_code == 429: log.warning("  rate limit — 20s"); await asyncio.sleep(20); continue
            if r.status_code in (502,503,504): await asyncio.sleep(2**attempt); continue
            if r.status_code != 200 or not r.text.strip(): return None
            try: return r.json()
            except: return None
    return None


# ══════════════════════════════════════════════════════════════
# FUNDAMENTAL FETCHERS
# ══════════════════════════════════════════════════════════════

def _fmt_period_end(period_end) -> str:
    if not period_end: return ""
    MONTHS = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    s = str(int(period_end))
    if len(s) == 8:
        m = int(s[4:6])
        return f"{MONTHS[m]} {s[:4]}" if 1 <= m <= 12 else s
    return str(period_end)

async def _finedge_financials(client, sem, sym, code, period):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"financials/{sym}",
            {"statement_type":stype,"statement_code":code,"period":period})
        rows = (d or {}).get("financials", [])
        if rows: return rows
    return None

async def _finedge_basic(client, sem, sym):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"basic-financials/{sym}",
            {"statement_type":stype,"statement_code":"pl"})
        rows = (d or {}).get("ratios", [])
        if rows: return rows
    return None

async def _finedge_ratios_pr(client, sem, sym):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}",
            {"statement_type":stype,"ratio_type":"pr"})
        rows = (d or {}).get("ratios", [])
        if rows: return rows
    return None

async def _finedge_ratios_le(client, sem, sym):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}",
            {"statement_type":stype,"ratio_type":"le"})
        rows = (d or {}).get("ratios", [])
        if rows: return rows
    return None

async def _finedge_ratios_li(client, sem, sym):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}",
            {"statement_type":stype,"ratio_type":"li"})
        rows = (d or {}).get("ratios", [])
        if rows: return rows
    return None

async def _finedge_ratios_ef(client, sem, sym):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"ratios/{sym}",
            {"statement_type":stype,"ratio_type":"ef"})
        rows = (d or {}).get("ratios", [])
        if rows: return rows
    return None

async def _finedge_shareholding(client, sem, sym):
    d = await _finedge_get(client, sem, f"shareholdings/pattern/{sym}", {"period":"quarterly"})
    if not d: return None
    columns = d.get("columns", []); rows = d.get("rows", [])
    if not columns or not rows: return None
    n_qtrs = min(8, len(columns)); qtrs = columns[:n_qtrs]
    def get_row(*names):
        for name in names:
            r = next((x for x in rows if name.lower() in x.get("catagory","").lower()), None)
            if r is None: continue
            data = r.get("data", {})
            if isinstance(data, dict): return [data.get(q) for q in qtrs]
            elif isinstance(data, list): return list(data[:n_qtrs])
        return []
    fii=get_row("institutionsforeign","foreign","fii")
    dii=get_row("institutionsdomestic","domestic","dii")
    public=get_row("noninstitutions","public","retail")
    govt=get_row("goverment","government")
    promoter=get_row("promoter")
    if not any(v is not None for v in promoter):
        promoter_computed = []
        for i in range(n_qtrs):
            vals=[fii[i] if i<len(fii) else None,dii[i] if i<len(dii) else None,
                  public[i] if i<len(public) else None,govt[i] if i<len(govt) else 0]
            if all(v is not None for v in vals[:3]):
                promoter_computed.append(round(100-sum(v or 0 for v in vals),2))
            else: promoter_computed.append(None)
        promoter = promoter_computed
    def _first(lst): return next((v for v in lst if v is not None), None)
    return {"sh_quarters":qtrs,"sh_promoter":promoter,"sh_fii":fii,"sh_dii":dii,"sh_public":public,
            "promoter":_first(promoter),"fii":_first(fii),"dii":_first(dii),"public":_first(public),
            "promoter_ch":(round(promoter[0]-promoter[1],2) if len(promoter)>=2
                           and promoter[0] is not None and promoter[1] is not None else None)}

async def _finedge_profile(client, sem, sym):
    d = await _finedge_get(client, sem, f"company-profile/{sym}", {})
    if not d: return None
    return {"name":d.get("name",""),"sector":d.get("sector",""),"industry":d.get("industry",""),
            "sub_industry":d.get("sub_industry",""),"macro_sector":d.get("macro_sector",""),
            "market_cap":d.get("market_cap"),"bse_code":d.get("bse_code",""),
            "description":d.get("description",""),"website":d.get("website","")}

async def _finedge_growth_metrics(client, sem, sym):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"financial-metrics/{sym}",
            {"statement_type":stype,"ratio_type":"gr"})
        fm = (d or {}).get("financial_metrics")
        if fm: return fm
    return None

async def _finedge_annual_price_ratios(client, sem, sym):
    for stype in ("c","s"):
        d = await _finedge_get(client, sem, f"annual-price-ratios/{sym}",
            {"statement_type":stype})
        rows = (d or {}).get("price_ratios", [])
        if rows: return rows
    return None

async def fetch_one_fundamental(client, sem, sym, isin=""):
    (pl_qtr,pl_ann,bs_ann,cf_ann,basic,prof_ratios,sh,profile,
     growth,ann_pr,ratios_le,ratios_li,ratios_ef) = await asyncio.gather(
        _finedge_financials(client,sem,sym,"pl","quarterly"),
        _finedge_financials(client,sem,sym,"pl","annual"),
        _finedge_financials(client,sem,sym,"bs","annual"),
        _finedge_financials(client,sem,sym,"cf","annual"),
        _finedge_basic(client,sem,sym),
        _finedge_ratios_pr(client,sem,sym),
        _finedge_shareholding(client,sem,sym),
        _finedge_profile(client,sem,sym),
        _finedge_growth_metrics(client,sem,sym),
        _finedge_annual_price_ratios(client,sem,sym),
        _finedge_ratios_le(client,sem,sym),
        _finedge_ratios_li(client,sem,sym),
        _finedge_ratios_ef(client,sem,sym),
    )
    if not any([pl_qtr,pl_ann,bs_ann,cf_ann]): return sym, None
    obj = {"symbol":sym,"updated":today_ist(),"source":"finedge"}
    if profile: obj.update({"name":profile.get("name",""),"sector":profile.get("sector",""),
        "industry":profile.get("industry",""),"sub_industry":profile.get("sub_industry",""),
        "macro_sector":profile.get("macro_sector",""),"market_cap":profile.get("market_cap"),
        "bse_code":profile.get("bse_code",""),"description":profile.get("description",""),
        "website":profile.get("website","")})
    div_payout_by_year = {}; shares_by_year = {}
    if basic:
        ttm = basic[0]
        obj.update({"ebit":ttm.get("ebit"),"ebitda":ttm.get("ebitda"),
            "operating_revenue":ttm.get("operatingRevenue"),
            "operating_profit":ttm.get("operatingProfit"),
            "shares_outstanding":ttm.get("dilutedSharesOutstanding")})
        for row in basic:
            yr = row.get("year")
            if yr is not None:
                if row.get("dividendPayout") is not None: div_payout_by_year[yr]=row["dividendPayout"]
                if row.get("dilutedSharesOutstanding") is not None: shares_by_year[yr]=row["dilutedSharesOutstanding"]
    if pl_qtr:
        obj["pl_quarterly"]=[{"header":_fmt_period_end(q.get("period_end")) or q.get("header",""),
            "period_end":q.get("period_end"),"sales":q.get("revenueFromOperations"),
            "expenses":q.get("expenses"),"pbt":q.get("profitBeforeTax"),
            "pat":q.get("profitLossForPeriod"),"eps":q.get("eps"),
            "depreciation":q.get("depreciationAndAmortisation"),
            "finance_costs":q.get("financeCosts"),"tax":q.get("taxExpense"),
            "other_income":q.get("otherIncome")} for q in pl_qtr[:12]]
    if pl_ann:
        obj["pl_annual"]=[{"header":_fmt_period_end(q.get("period_end")) or q.get("header",""),
            "year":q.get("year"),"sales":q.get("revenueFromOperations"),
            "expenses":q.get("expenses"),"pbt":q.get("profitBeforeTax"),
            "pat":q.get("profitLossForPeriod"),"eps":q.get("eps"),
            "depreciation":q.get("depreciationAndAmortisation"),
            "finance_costs":q.get("financeCosts"),"other_income":q.get("otherIncome"),
            "dividend_payout":div_payout_by_year.get(q.get("year")),
            "shares":shares_by_year.get(q.get("year"))} for q in pl_ann[:5]]
    if bs_ann:
        obj["bs_annual"]=[{"header":_fmt_period_end(q.get("period_end")) or q.get("header",""),
            "year":q.get("year"),"total_assets":q.get("assets"),
            "equity_capital":q.get("equityCapital"),"reserves":q.get("reserves"),
            "borrowings_current":q.get("borrowingsCurrent"),
            "borrowings_noncurrent":q.get("borrowingsNoncurrent"),
            "borrowings_total":(q.get("borrowingsCurrent") or 0)+(q.get("borrowingsNoncurrent") or 0),
            "cash":q.get("cashAndCashEquivalents"),"current_assets":q.get("currentAssets"),
            "current_liabilities":q.get("currentLiabilities"),
            "fixed_assets":q.get("propertyPlantAndEquipmentNet"),
            "investments":q.get("investments")} for q in bs_ann[:5]]
    if cf_ann:
        obj["cf_annual"]=[{"header":_fmt_period_end(q.get("period_end")) or q.get("header",""),
            "year":q.get("year"),"cfo":q.get("cashFlowsFromOperatingActivities"),
            "cfi":q.get("cashFlowsFromInvestingActivities"),
            "cff":q.get("cashFlowsFromFinancingActivities"),
            "net_cf":q.get("netCashFlow"),"capex":q.get("purchaseOfPPEClassifiedAsInvesting"),
            "fcf":((q.get("cashFlowsFromOperatingActivities") or 0)+
                   (q.get("purchaseOfPPEClassifiedAsInvesting") or 0))
                  if q.get("cashFlowsFromOperatingActivities") is not None else None}
            for q in cf_ann[:5]]
    if prof_ratios:
        obj["ratios_annual"]=[{"header":r.get("header",""),"year":r.get("year"),
            "gross_margin":r.get("grossMargin"),"ebit_margin":r.get("ebitMargin"),
            "ebitda_margin":r.get("ebitdaMargin"),"net_margin":r.get("netMargin"),
            "operating_margin":r.get("operatingMargin"),"roe":r.get("returnOnEquity"),
            "roa":r.get("returnOnAsset"),"roce":r.get("returnOnCapital"),
            "pretax_margin":r.get("preTaxMargin"),"tax_rate":r.get("effectiveTaxRate")}
            for r in prof_ratios[:5]]
    if sh:
        obj.update({"promoter":sh.get("promoter"),"fii":sh.get("fii"),"dii":sh.get("dii"),
            "public":sh.get("public"),"promoter_ch":sh.get("promoter_ch"),
            "sh_quarters":sh.get("sh_quarters",[]),"sh_promoter":sh.get("sh_promoter",[]),
            "sh_fii":sh.get("sh_fii",[]),"sh_dii":sh.get("sh_dii",[]),"sh_public":sh.get("sh_public",[])})
    if growth:
        obj.update({"revenue_cagr_3y":growth.get("revenueGrowth3years"),
            "revenue_cagr_5y":growth.get("revenueGrowth5years"),
            "pat_cagr_3y":growth.get("netIncomeGrowth3years"),
            "pat_cagr_5y":growth.get("netIncomeGrowth5years"),
            "eps_cagr_3y":growth.get("epsGrowth3years"),"eps_cagr_5y":growth.get("epsGrowth5years"),
            "ebitda_cagr_3y":growth.get("EBITDAGrowth3years"),
            "ebitda_cagr_5y":growth.get("EBITDAGrowth5years"),
            "cfo_cagr_3y":growth.get("cfoGrowth3years"),
            "fcf_cagr_3y":growth.get("freeCashFlowGrowth3Years"),
            "share_dilution_3y":growth.get("dilutedSharesGrowth3years"),
            "share_dilution_5y":growth.get("dilutedSharesGrowth5years")})
    if ann_pr:
        obj["price_ratios_annual"]=[{"header":r.get("header",""),"year":r.get("year"),
            "avg_price":r.get("average_price"),"pe":r.get("pe"),"pb":r.get("pb"),
            "ps":r.get("ps"),"pfcf":r.get("pfcf") or None}
            for r in ann_pr[:5] if r.get("year") and r.get("pe")]
    if ratios_le:
        obj["ratios_leverage"]=[{"header":r.get("header",""),"year":r.get("year"),
            "de_ratio":r.get("totalDebtToEquity"),"lt_de_ratio":r.get("longTermDebtToEquity"),
            "financial_leverage":r.get("financialLeverage"),
            "debt_to_assets":r.get("totalDebttoAssets"),"debt_to_fcf":r.get("totalDebtTofcf")}
            for r in ratios_le[:6] if r.get("year") and r.get("header")!="TTM"]
    if ratios_li:
        obj["ratios_liquidity"]=[{"header":r.get("header",""),"year":r.get("year"),
            "current_ratio":r.get("currentRatio"),"quick_ratio":r.get("quickRatio"),
            "interest_coverage":r.get("interestCoverage")}
            for r in ratios_li[:6] if r.get("year") and r.get("header")!="TTM"]
    if ratios_ef:
        obj["ratios_efficiency"]=[{"header":r.get("header",""),"year":r.get("year"),
            "asset_turnover":r.get("assetTurnover"),"inventory_turnover":r.get("inventoryTurnover"),
            "receivable_turnover":r.get("receivableTurnover"),
            "cash_conversion_cycle":r.get("cashConversionCycle"),
            "debtor_days":r.get("debtorDays"),"inventory_days":r.get("inventoryDays"),
            "days_payable":r.get("daysPayable")}
            for r in ratios_ef[:6] if r.get("year") and r.get("header")!="TTM"]
    if obj.get("pl_quarterly"): obj["eps_diluted"]=obj["pl_quarterly"][0].get("eps")
    if obj.get("bs_annual"):
        bs0=obj["bs_annual"][0]
        eq_cap=bs0.get("equity_capital") or 0; res=bs0.get("reserves") or 0
        borr=bs0.get("borrowings_total") or 0; cash=bs0.get("cash") or 0
        shares=obj.get("shares_outstanding") or 0
        obj["book_value_ps"]=round((eq_cap+res)/shares,2) if shares else None
        obj["net_debt"]=round(borr-cash) if (borr or cash) else None
    return sym, obj


# ══════════════════════════════════════════════════════════════
# RESULTS CALENDAR
# ══════════════════════════════════════════════════════════════

async def get_result_symbols_finedge(client) -> list[str]:
    today = today_ist()
    next7 = (date.fromisoformat(today)+timedelta(days=1)).isoformat()
    sem = asyncio.Semaphore(1)
    d = await _finedge_get(client, sem, "results-calendar", {"from_date":today,"to_date":next7})
    if not d or not isinstance(d, list):
        log.warning("Finedge results calendar — empty or error"); return []
    isin_symbols = set(ISIN_MAP.keys())
    matched = list({item["symbol"] for item in d
        if item.get("symbol") in isin_symbols and item.get("expected_result_date")==today})
    log.info(f"Results today ({today}): {len(matched)} stocks")
    return matched


# ══════════════════════════════════════════════════════════════
# R2 HELPERS
# ══════════════════════════════════════════════════════════════

async def r2_upload(client, filename, data):
    if isinstance(data, str): data = data.encode()
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(url, headers={**WORKER_HEADERS,"Content-Type":"application/json"},
        content=data, timeout=90)
    if r.status_code != 200: raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")

async def r2_download(client, filename):
    url = f"{WORKER_URL}/{filename}"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404: return None
    if r.status_code != 200: raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↓ {filename} ({len(r.content)/1024:.0f} KB)")
    return r.json()

async def r2_download_fund(client) -> dict:
    url = f"{WORKER_URL}/fundamentals.json"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404: log.info("fundamentals.json not found — starting fresh"); return {}
    if r.status_code != 200: raise RuntimeError(f"Download failed: HTTP {r.status_code}")
    log.info(f"  ↓ fundamentals.json ({len(r.content)/1024:.0f} KB)")
    data = r.json()
    if isinstance(data, list): return {d["symbol"]:d for d in data if d.get("symbol")}
    if isinstance(data, dict): return data.get("stocks", data)
    return {}

async def r2_upload_fund(client, data: dict) -> None:
    arr = list(data.values())
    payload = json.dumps(arr)
    url = f"{WORKER_URL}?file=fundamentals.json"
    r = await client.post(url, headers={**WORKER_HEADERS,"Content-Type":"application/json"},
        content=payload.encode(), timeout=120)
    if r.status_code != 200: raise RuntimeError(f"Upload failed: HTTP {r.status_code}")
    log.info(f"  ↑ fundamentals.json ({len(payload)/1024:.1f} KB)")

async def save_result_calendar(client, symbols, date_str, keep_days=60):
    try:
        existing = await r2_download(client, "result_calendar.json")
        cal = existing if isinstance(existing, dict) else {}
    except: cal = {}
    cal[date_str] = symbols
    cutoff = (date.fromisoformat(date_str)-timedelta(days=keep_days)).isoformat()
    cal = {d:v for d,v in cal.items() if d >= cutoff}
    await r2_upload(client, "result_calendar.json", json.dumps(cal))
    log.info(f"  📅 result_calendar.json — {len(cal)} days, {len(symbols)} stocks today")

async def download_all_chunks(client) -> dict:
    tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception): log.warning(f"  ohlc_{i+1}.json error: {res}")
        elif res and "stocks" in res: all_data.update(res["stocks"])
    log.info(f"Master: {len(all_data)} stocks across {R2_CHUNKS} chunks")
    return all_data

async def upload_all_chunks(client, all_data, today):
    symbols = sorted(all_data.keys()); n = len(symbols)
    size = (n + R2_CHUNKS - 1) // R2_CHUNKS
    tasks = []
    for i in range(R2_CHUNKS):
        chunk_syms = symbols[i*size:(i+1)*size]
        chunk = {s: all_data[s] for s in chunk_syms}
        payload = json.dumps({"updated":today,"chunk":i+1,"total":R2_CHUNKS,"stocks":chunk})
        tasks.append(r2_upload(client, f"ohlc_{i+1}.json", payload))
    await asyncio.gather(*tasks)
    log.info(f"✓ {R2_CHUNKS} chunks uploaded ({n} stocks)")


# ══════════════════════════════════════════════════════════════
# DATA HELPERS
# ══════════════════════════════════════════════════════════════

def build_stock_obj(candles):
    return {k:[c[k] for c in candles] for k in ("d","o","h","l","c","v","oi")}

def apply_rolling_window(all_data, cutoff):
    dropped = 0
    for s in all_data.values():
        keep = [i for i,d in enumerate(s["d"]) if d >= cutoff]
        dropped += len(s["d"]) - len(keep)
        for k in s: s[k] = [s[k][i] for i in keep]
    return dropped

def _sort_stock(s):
    order = sorted(range(len(s["d"])), key=lambda i: s["d"][i])
    for k in s: s[k] = [s[k][i] for i in order]

def merge_candles_into(all_data, sym, candles, cutoff):
    if sym not in all_data: all_data[sym] = {k:[] for k in ("d","o","h","l","c","v","oi")}
    s = all_data[sym]; existing = set(s["d"]); added = 0
    for c in candles:
        if c["d"] < cutoff or c["d"] in existing: continue
        for k in s: s[k].append(c[k])
        existing.add(c["d"]); added += 1
    if added: _sort_stock(s)
    return added

def upsert_candle(all_data, sym, c):
    if sym not in all_data: all_data[sym] = {k:[] for k in ("d","o","h","l","c","v","oi")}
    s = all_data[sym]
    if c["d"] in s["d"]:
        idx = s["d"].index(c["d"])
        for k in ("o","h","l","c","v","oi"): s[k][idx] = c[k]
    else:
        for k in s: s[k].append(c[k])
        _sort_stock(s)


# ══════════════════════════════════════════════════════════════
# OHLC PIPELINE MODES
# ══════════════════════════════════════════════════════════════

async def run_daily() -> None:
    today = today_ist()
    prev = prev_trading_day(today); cutoff = rolling_cutoff(today)
    log.info(f"━━━ Daily  {prev} → {today}  cutoff {cutoff} ━━━")
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)
        from_year = int(cutoff[:4]); to_year = int(today[:4])
        nse_results, bse_results = await asyncio.gather(
            asyncio.gather(*[fetch_ohlc(client,sem,sym,from_year,to_year) for sym in ISIN_MAP]),
            asyncio.gather(*[fetch_ohlc(client,sem,sym,from_year,to_year) for sym in BSE_ISIN_MAP]),
        )
        fetched = {sym:c for sym,c in [*nse_results,*bse_results] if c}
        log.info(f"✓ {len(fetched)} fetched  ✗ {len(ISIN_MAP)+len(BSE_ISIN_MAP)-len(fetched)} no data")
        all_data = await download_all_chunks(client)
        live = set(ISIN_MAP)|set(BSE_ISIN_MAP)
        pruned = [s for s in list(all_data) if s not in live]
        for s in pruned: del all_data[s]
        if pruned: log.info(f"🗑  Pruned {len(pruned)} stocks")
        total_new = 0; delta = {}
        for sym, candles in fetched.items():
            total_new += merge_candles_into(all_data, sym, candles, cutoff)
            today_c = next((c for c in candles if c["d"]==today), None)
            if today_c: delta[sym] = today_c
        log.info(f"Merged: {total_new} new  Delta: {len(delta)}")
        dropped = apply_rolling_window(all_data, cutoff)
        log.info(f"Rolling: dropped {dropped} old candles")
        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_delta.json", json.dumps({"date":today,"stocks":delta})),
        )
    log.info("━━━ Daily complete ━━━")


async def run_today() -> None:
    today = today_ist()
    log.info(f"━━━ Today  {today} ━━━")

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        my_universe = set(ISIN_MAP.keys()) | set(BSE_ISIN_MAP.keys())
        log.info(f"My universe: {len(my_universe)} stocks")

        sem = asyncio.Semaphore(1)
        chunk_task = asyncio.create_task(download_all_chunks(client))

        bulk_data = await _finedge_get(client, sem, "quote", {})

        if not bulk_data:
            log.error("❌ Bulk quote returned empty — aborting")
            return

        log.info(f"Bulk quote received: {len(bulk_data)} symbols")

        fetched = {}

        for sym, q in bulk_data.items():

            if sym not in my_universe:
                continue

            o = q.get("open_price")
            h = q.get("high_price")
            l = q.get("low_price")
            c = q.get("current_price")

            if None in (o, h, l, c):
                continue

            fetched[sym] = {
                "d": q.get("quote_date") or today,
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "v": q.get("volume", 0),
                "oi": 0,
            }

        log.info(f"Filtered to my universe: {len(fetched)} stocks")

        all_data = await chunk_task

        # -------------------------
        # Duplicate detection
        # -------------------------
        suspicious = []

        for sym, c in fetched.items():

            hist = all_data.get(sym)

            if not hist or not hist.get("d"):
                continue

            last_idx = len(hist["d"]) - 1

            if (
                c["o"] == hist["o"][last_idx]
                and c["h"] == hist["h"][last_idx]
                and c["l"] == hist["l"][last_idx]
                and c["c"] == hist["c"][last_idx]
            ):
                suspicious.append(sym)

        log.info(f"Initial duplicate candles: {len(suspicious)}")

        # -------------------------
        # Retry logic
        # -------------------------
        duplicate_ratio = len(suspicious) / max(len(fetched), 1)

        if duplicate_ratio > 0.10:

            log.warning(
                f"⚠ Duplicate ratio {duplicate_ratio:.1%} "
                f"({len(suspicious)} stocks) - retrying in 20 sec"
            )

            await asyncio.sleep(20)

            retry_bulk = await _finedge_get(client, sem, "quote", {})

            if retry_bulk:

                fetched_retry = {}

                for sym, q in retry_bulk.items():

                    if sym not in my_universe:
                        continue

                    o = q.get("open_price")
                    h = q.get("high_price")
                    l = q.get("low_price")
                    c = q.get("current_price")

                    if None in (o, h, l, c):
                        continue

                    fetched_retry[sym] = {
                        "d": q.get("quote_date") or today,
                        "o": o,
                        "h": h,
                        "l": l,
                        "c": c,
                        "v": q.get("volume", 0),
                        "oi": 0,
                    }

                fetched = fetched_retry

                suspicious_after_retry = []

                for sym, c in fetched.items():

                    hist = all_data.get(sym)

                    if not hist or not hist.get("d"):
                        continue

                    last_idx = len(hist["d"]) - 1

                    if (
                        c["o"] == hist["o"][last_idx]
                        and c["h"] == hist["h"][last_idx]
                        and c["l"] == hist["l"][last_idx]
                        and c["c"] == hist["c"][last_idx]
                    ):
                        suspicious_after_retry.append(sym)

                log.info(
                    f"After retry duplicates: {len(suspicious_after_retry)} "
                    f"(resolved {len(suspicious) - len(suspicious_after_retry)})"
                )

                suspicious = suspicious_after_retry

        # -------------------------
        # Update OHLC
        # -------------------------
        for sym, c in fetched.items():
            upsert_candle(all_data, sym, c)

        delta = {sym: c for sym, c in fetched.items() if c["d"] == today}

        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(
                client,
                "ohlc_delta.json",
                json.dumps({"date": today, "stocks": delta}),
            ),
        )

        log.info(f"✅ delta: {len(delta)} stocks")
        log.info(f"Final duplicate candles: {len(suspicious)}")

    log.info("━━━ Today complete ━━━")

async def run_full() -> None:
    today = last_trading_day()
    start = (date.fromisoformat(today)-timedelta(days=ROLLING_DAYS)).isoformat()
    cutoff = start; from_year = int(start[:4]); to_year = int(today[:4])
    sem = asyncio.Semaphore(CONCURRENCY); all_data = {}; failed = []
    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)
        all_sym_list = list(ISIN_MAP.keys())+list(BSE_ISIN_MAP.keys())
        log.info(f"━━━ Full Load  {start} → {today}  ({len(all_sym_list)} stocks) ━━━")
        for i in range(0, len(all_sym_list), 50):
            chunk_syms = all_sym_list[i:i+50]
            results = await asyncio.gather(*[fetch_ohlc(client,sem,sym,from_year,to_year) for sym in chunk_syms])
            for sym, candles in results:
                if candles:
                    filtered = [c for c in candles if c["d"] >= cutoff]
                    if filtered: all_data[sym] = build_stock_obj(filtered)
                else: failed.append(sym)
            pct = min(i+50, len(all_sym_list))
            log.info(f"  {pct}/{len(all_sym_list)}  OK:{len(all_data)}  Failed:{len(failed)}")
            if pct % 500 == 0: await upload_all_chunks(client, all_data, today)
        log.info(f"✓ {len(all_data)} loaded  ✗ {len(failed)} failed")
        if failed: (HERE/"failed_stocks.txt").write_text("\n".join(failed))
        apply_rolling_window(all_data, cutoff)
        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_all.json", json.dumps({"updated":today,"stocks":all_data})),
        )
    log.info("━━━ Full load complete ━━━")


async def run_status() -> None:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[r2_download(client,f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)], return_exceptions=True)
    print(f"\n{'File':<20} {'Stocks':>7}  {'From':>12}  {'To':>12}  {'Updated':>12}")
    print("─"*70); total = 0
    for i, res in enumerate(results):
        fname = f"ohlc_{i+1}.json"
        if isinstance(res, Exception) or res is None: print(f"{fname:<20}  ERROR"); continue
        stocks = res.get("stocks", {})
        if not stocks: print(f"{fname:<20}  (empty)"); continue
        s0 = next(iter(stocks.values())); total += len(stocks)
        print(f"{fname:<20} {len(stocks):>7}  {s0['d'][0]:>12}  {s0['d'][-1]:>12}  {res.get('updated','?'):>12}")
    print(f"\nTotal: {total} stocks\n")


# ══════════════════════════════════════════════════════════════
# FUNDAMENTALS PIPELINE
# ══════════════════════════════════════════════════════════════

async def run_fund_daily() -> None:
    today = today_ist()
    if not is_trading_day(today): log.info(f"⏭  {today} not a trading day"); return
    log.info(f"━━━ Fundamentals Daily  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)
        symbols = await get_result_symbols_finedge(client)
        if not symbols: log.info("No results today — exiting"); return
        await save_result_calendar(client, symbols, today)
        fund_data = await r2_download_fund(client)
        sem = asyncio.Semaphore(FUND_CONCURRENCY)
        results = await asyncio.gather(*[fetch_one_fundamental(client,sem,sym) for sym in symbols if sym in ISIN_MAP])
        ok = 0
        for sym, data in results:
            if data: fund_data[sym]=data; ok+=1; log.info(f"  ✓ {sym}")
            else: log.warning(f"  ✗ {sym}: no data")
        log.info(f"Updated {ok}/{len(symbols)} stocks")
        await r2_upload_fund(client, fund_data)
    log.info("━━━ Fundamentals Daily complete ━━━")


async def run_fund_full(part=0) -> None:
    TOTAL_PARTS = 10; BATCH_SIZE = 20
    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)
        nse_symbols = list(ISIN_MAP.keys()); total = len(nse_symbols)
        part_size = (total+TOTAL_PARTS-1)//TOTAL_PARTS
        if part == 0: start_idx,end_idx,label = 0,total,"Full"
        else:
            start_idx=(part-1)*part_size; end_idx=min(part*part_size,total)
            label=f"Part {part}/{TOTAL_PARTS}"
        chunk = nse_symbols[start_idx:end_idx]
        log.info(f"━━━ Fund Full {label}  ({len(chunk)} stocks) ━━━")
        ETF_ENDSWITH = ("ETF","BEES","LIQUID","GILT","IETF","MMQS","TOTAL")
        ETF_CONTAINS = ("NIFTY","BANKEX","MSCIN")
        def _is_etf(sym):
            s = sym.upper()
            return any(s.endswith(k) for k in ETF_ENDSWITH) or any(k in s for k in ETF_CONTAINS)
        equity_chunk = [sym for sym in chunk if not _is_etf(sym)]
        skipped_etf = len(chunk)-len(equity_chunk)
        if skipped_etf: log.info(f"Skipping {skipped_etf} ETFs")
        fund_data = await r2_download_fund(client)
        missing = [sym for sym in equity_chunk if sym not in fund_data]
        log.info(f"Already done: {len(equity_chunk)-len(missing)}  Remaining: {len(missing)}")
        if not missing: log.info("✅ All stocks already fetched!"); return
        sem = asyncio.Semaphore(FUND_CONCURRENCY); ok = failed = 0
        for i in range(0, len(missing), BATCH_SIZE):
            batch = missing[i:i+BATCH_SIZE]
            results = await asyncio.gather(*[fetch_one_fundamental(client,sem,sym) for sym in batch])
            for sym, data in results:
                if data: fund_data[sym]=data; ok+=1
                else: failed+=1; log.warning(f"  ✗ {sym}: no data")
            pct = min(i+BATCH_SIZE, len(missing))
            log.info(f"  {pct}/{len(missing)}  ✓{ok}  ✗{failed}")
            if pct % 100 == 0 or pct == len(missing):
                log.info("  💾 Checkpoint upload…")
                await r2_upload_fund(client, fund_data)
    log.info(f"━━━ Fund Full {label} complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# EP SCANNER HELPERS
# ══════════════════════════════════════════════════════════════

def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    lookback = min(50, n)
    if lookback < 20: return True
    vols   = [v for v in volumes[-lookback:] if v is not None]
    prices = [c for c in closes[-lookback:] if c is not None and c > 0]
    if len(vols) < 20 or len(prices) < 20: return False
    return (sum(vols)/len(vols) * sum(prices)/len(prices)) >= min_turnover

def _detect_ep(all_data, min_gap_pct=2.0, volume_spike_x=2.0,
               volume_lookback=20, max_consolidation=30, max_ep_age_days=30):
    signals = []
    for sym, s in all_data.items():
        dates,highs,lows,closes,volumes = s["d"],s["h"],s["l"],s["c"],s["v"]
        n = len(dates)
        if n < volume_lookback+2: continue
        if not _check_liquidity(volumes, closes, n): continue
        scan_from = max(volume_lookback, n-max_ep_age_days)
        for i in range(scan_from, n):
            prev_high=highs[i-1]; today_low=lows[i]
            if prev_high is None or today_low is None: continue
            if prev_high<=0 or today_low<=0 or today_low<=prev_high: continue
            gap_pct = (today_low-prev_high)/prev_high*100
            if gap_pct < min_gap_pct: continue
            avg_vol = sum(volumes[i-volume_lookback:i])/volume_lookback
            if avg_vol == 0: continue
            vol_x = volumes[i]/avg_vol
            if vol_x < volume_spike_x: continue
            gap_lower=prev_high; consol_count=0; ep_intact=True
            for j in range(i+1, min(i+max_consolidation+1, n)):
                if closes[j] is None: continue
                if closes[j] < gap_lower: ep_intact=False; break
                consol_count += 1
            if not ep_intact or consol_count >= max_consolidation: continue
            last_idx=min(i+consol_count, n-1); ep_close=closes[i]
            ep_5d_idx=min(i+5, n-1)
            ep_5d_return=round((closes[ep_5d_idx]-ep_close)/ep_close*100,2) if ep_5d_idx>i else ""
            ep_return=round((closes[last_idx]-ep_close)/ep_close*100,2) if ep_close else 0.0
            never_broke_high=all(closes[j]<=highs[i] for j in range(i+1,last_idx+1))
            signals.append({"symbol":sym,"ep_date":dates[i],"gap_lower":round(gap_lower,2),
                "gap_pct":round(gap_pct,2),"vol_spike_x":round(vol_x,1),
                "ep_candle_high":round(highs[i],2),"ep_candle_low":round(today_low,2),
                "ep_candle_close":round(ep_close,2),"ep_return":ep_return,
                "ep_5d_return":ep_5d_return,"last_close":round(closes[last_idx],2),
                "last_date":dates[last_idx],"consolidation":consol_count,
                "ep_type":"Consolidating below EP high" if never_broke_high else "EP Follow-through"})
    seen = {}
    for sig in signals:
        sym=sig["symbol"]
        if sym not in seen or sig["ep_date"]>seen[sym]["ep_date"]: seen[sym]=sig
    return list(seen.values())

def _detect_post_result_thrust(all_data, result_calendar, min_price_ch_pct=1.5,
    volume_spike_x=1.5, close_position_min=0.5, volume_lookback=20, max_result_age_days=30):
    today_str=today_ist(); cutoff=(date.fromisoformat(today_str)-timedelta(days=max_result_age_days)).isoformat()
    sym_to_result_date = {}
    for date_str, syms in result_calendar.items():
        if date_str < cutoff: continue
        for sym in syms:
            if sym not in sym_to_result_date or date_str>sym_to_result_date[sym]:
                sym_to_result_date[sym]=date_str
    signals = []
    for sym, result_date in sym_to_result_date.items():
        if sym not in all_data: continue
        s=all_data[sym]; dates=s["d"]; opens=s["o"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<volume_lookback+2 or result_date not in dates: continue
        ri_list=[i for i,d in enumerate(dates) if d==result_date]
        if not ri_list: continue
        ri=ri_list[-1]; ti=ri+1
        if ti>=n: continue
        lookback=min(volume_lookback,ri)
        if lookback==0: continue
        avg_vol=sum(volumes[ri-lookback:ri])/lookback
        if avg_vol==0: continue
        result_day_ch=round((closes[ri]-closes[ri-1])/closes[ri-1]*100,2) if ri>0 and closes[ri-1] else 0.0
        result_day_vol_x=round(volumes[ri]/avg_vol,1)
        if lows[ti] is None or highs[ri] is None or lows[ti]>highs[ri]: continue
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
        signals.append({"symbol":sym,"result_date":result_date,"result_day_ch":result_day_ch,
            "result_day_vol_x":result_day_vol_x,"t1_date":dates[ti],"t1_open":round(opens[ti],2),
            "t1_high":round(highs[ti],2),"t1_low":round(lows[ti],2),"t1_close":round(closes[ti],2),
            "price_ch_pct":round(price_ch_pct,2),"vol_pct":f"+{round((vol_x-1)*100)}%",
            "close_position":round(close_pos*100,1),"reaction_type":reaction_type})
    order={"AH Result → T+1 Primary":0,"IH Result → T+1 Follow-through":1,"Mixed":2}
    signals.sort(key=lambda x:(order.get(x["reaction_type"],9),-x["price_ch_pct"]))
    return signals


# ══════════════════════════════════════════════════════════════
# RS RATING (percentile)
# ══════════════════════════════════════════════════════════════

def _calculate_rs(all_data, history_days=30):
    all_syms=list(all_data.keys()); result={}; day_scores={}
    for sym, s in all_data.items():
        closes=s["c"]; n=len(closes); scores=[]
        for day_offset in range(history_days, -1, -1):
            idx=n-1-day_offset
            if idx < 63: scores.append(None); continue
            def ret(lookback):
                prev_idx=idx-lookback
                if prev_idx<0: return None
                prev=closes[prev_idx]; c=closes[idx]
                if not prev or c is None: return None
                return (c-prev)/prev*100
            p63=ret(63); p126=ret(126); p189=ret(189); p252=ret(252)
            if p252 is not None and p189 is not None and p126 is not None and p63 is not None:
                composite=(p63*2+p126+p189+p252)/5
            elif p189 is not None and p126 is not None and p63 is not None:
                composite=(p63*2+p126+p189)/4
            elif p126 is not None and p63 is not None:
                composite=(p63*2+p126)/3
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
        sorted_syms=sorted(day_composites, key=lambda x:day_composites[x])
        total=len(sorted_syms)
        ranks={sym:round((i+1)/total*99) for i,sym in enumerate(sorted_syms)}
        for sym in all_syms: rs_history[sym].append(ranks.get(sym))
    final_composites={sym:day_scores[sym][-1] for sym in all_syms if day_scores[sym][-1] is not None}
    final_sorted=sorted(final_composites, key=lambda x:final_composites[x])
    final_total=len(final_sorted)
    final_rank_pos={sym:i+1 for i,sym in enumerate(final_sorted)}
    for sym in all_syms:
        hist=rs_history[sym]
        current_rs=next((v for v in reversed(hist) if v is not None), None)
        result[sym]={"rs":current_rs,"rs_rank":final_rank_pos.get(sym),
                     "rs_total":final_total,"history":hist}
    return result


def _build_rs_history_json(all_data, rs_data):
    from datetime import date as dt
    sample_sym=max(all_data.keys(), key=lambda s:len(all_data[s].get("d",[])))
    dates=all_data[sample_sym]["d"]; n=len(dates)
    history_len=len(next(iter(rs_data.values()))["history"])
    start_idx=n-history_len
    def fmt_date(d_str): return dt.fromisoformat(d_str).strftime("%-d-%b-%y")
    date_labels=[(i,fmt_date(dates[start_idx+i])) for i in range(history_len) if 0<=start_idx+i<n]
    rows=[]
    for sym, v in rs_data.items():
        row={"Stock Name":sym}
        for i,label in date_labels:
            if v["history"][i] is not None: row[label]=v["history"][i]
        rows.append(row)
    return rows


# ══════════════════════════════════════════════════════════════
# MANSFIELD RS (stock / index)
# ══════════════════════════════════════════════════════════════

def _build_index_close_map(history: list, daily_close, today: str) -> dict:
    """R2 index history + today's close from index_daily → date:close map"""
    close_map = {}
    for row in (history or []):
        d = row.get("date"); c = row.get("close")
        if d and c is not None: close_map[d] = c
    # Merge today's close from index_daily feed
    if daily_close is not None and today not in close_map:
        close_map[today] = daily_close
    return close_map


def _calculate_mansfield_rs(all_data, index_maps: dict) -> dict:
    """
    index_maps = {
        "nifty50":     {date: close, ...},
        "nifty500":    {date: close, ...},
        "smallmid400": {date: close, ...},
    }
    Returns per-stock Mansfield RS metrics added to rs_ratings.
    """
    NHL = 50   # lookback for new high/low
    NHL_SHORT = 21

    result = {}

    for sym, s in all_data.items():
        dates  = s["d"]
        closes = s["c"]
        highs  = s["h"]
        n      = len(dates)
        if n < NHL + 1:
            result[sym] = {}
            continue

        stock_metrics = {}

        for idx_key, close_map in index_maps.items():
            # Build aligned rs_line
            rs_line  = []
            rs_dates = []
            for i, (d, c) in enumerate(zip(dates, closes)):
                idx_c = close_map.get(d)
                if c is None or not idx_c:
                    rs_line.append(None)
                else:
                    rs_line.append(round(c / idx_c, 6))
                rs_dates.append(d)

            m = len(rs_line)
            if m < NHL + 1:
                stock_metrics[idx_key] = {}
                continue

            # Current RS value
            current_rs = next((v for v in reversed(rs_line) if v is not None), None)

            # RS New High / Low — 50d
            valid_50 = [v for v in rs_line[-NHL:] if v is not None]
            rs_nh_50 = bool(valid_50 and current_rs is not None and current_rs >= max(valid_50))
            rs_nl_50 = bool(valid_50 and current_rs is not None and current_rs <= min(valid_50))

            # RS New High / Low — 21d
            valid_21 = [v for v in rs_line[-NHL_SHORT:] if v is not None]
            rs_nh_21 = bool(valid_21 and current_rs is not None and current_rs >= max(valid_21))
            rs_nl_21 = bool(valid_21 and current_rs is not None and current_rs <= min(valid_21))

            # Price New High — 50d and 21d
            valid_h_50 = [v for v in highs[-NHL:] if v is not None]
            valid_h_21 = [v for v in highs[-NHL_SHORT:] if v is not None]
            last_close = next((v for v in reversed(closes) if v is not None), None)
            price_nh_50 = bool(valid_h_50 and last_close is not None and last_close >= max(valid_h_50))
            price_nh_21 = bool(valid_h_21 and last_close is not None and last_close >= max(valid_h_21))

            # RS Divergence — RS making NH but price not (Purple dot in Pine)
            rs_div_50 = rs_nh_50 and not price_nh_50
            rs_div_21 = rs_nh_21 and not price_nh_21

            stock_metrics[idx_key] = {
                "rs_val"    : current_rs,
                "rs_nh_21"  : rs_nh_21,
                "rs_nl_21"  : rs_nl_21,
                "rs_div_21" : rs_div_21,
                "rs_nh_50"  : rs_nh_50,
                "rs_nl_50"  : rs_nl_50,
                "rs_div_50" : rs_div_50,
            }

        result[sym] = stock_metrics
    return result


# ══════════════════════════════════════════════════════════════
# SECTOR RS HISTORY
# ══════════════════════════════════════════════════════════════

def _build_group_rs_history(classification, rs_history_json, field_name):
    if not classification or not isinstance(classification, list):
        log.warning(f"_build_group_rs_history: classification empty — skipping {field_name}")
        return []
    group_map = {}
    for s in classification:
        sym=s.get("symbol"); group=s.get(field_name)
        if not sym or not group: continue
        group_map.setdefault(group, []).append(sym)
    if not rs_history_json: return []
    sample=rs_history_json[0]; date_cols=[k for k in sample.keys() if k!="Stock Name"]
    output = []
    for dt in date_cols:
        stocks = {}
        for row in rs_history_json:
            sym=row.get("Stock Name"); rs=row.get(dt)
            if sym and rs is not None: stocks[sym]=rs
        groups = {}
        for group, syms in group_map.items():
            valid=rs60=rs70=rs80=rs90=0; rs_sum=0
            for sym in syms:
                rs=stocks.get(sym)
                if rs is None: continue
                valid+=1; rs_sum+=rs
                if rs>=60: rs60+=1
                if rs>=70: rs70+=1
                if rs>=80: rs80+=1
                if rs>=90: rs90+=1
            if valid < 5: continue
            groups[group]={"stocks":valid,
                "rs60":round(rs60/valid*100,1),"rs70":round(rs70/valid*100,1),
                "rs80":round(rs80/valid*100,1),"rs90":round(rs90/valid*100,1),
                "avg_rs":round(rs_sum/valid,1)}
        output.append({"date":dt,"groups":groups})
    return output


# ══════════════════════════════════════════════════════════════
# MSWING
# ══════════════════════════════════════════════════════════════

def _calculate_mswing(all_data, history_days=60):
    result = {}
    for sym, s in all_data.items():
        closes=s["c"]; n=len(closes); history=[]
        for day_offset in range(history_days, -1, -1):
            idx=n-1-day_offset
            if idx<51: history.append(None); continue
            c_now=closes[idx]; c20=closes[idx-20]; c50=closes[idx-50]
            if c_now is None or not c20 or not c50: history.append(None); continue
            try:
                ret_20=(c_now-c20)/c20*100/20; ret_50=(c_now-c50)/c50*100/50
                history.append(round(ret_20+ret_50,4))
            except ZeroDivisionError: history.append(None)
        valid=[v for v in history[-9:] if v is not None]
        result[sym]={"mswing":history[-1] if history else None,
            "mswing_avg9":round(sum(valid)/len(valid),4) if valid else None,
            "mswing_history":history}
    return result

def _build_mswing_json(all_data, mswing_data):
    from datetime import date as dt
    sample_sym=max(all_data.keys(), key=lambda s:len(all_data[s].get("d",[])))
    dates=all_data[sample_sym]["d"]; n=len(dates)
    history_len=len(next(iter(mswing_data.values()))["mswing_history"])
    start_idx=n-history_len
    def fmt_date(d_str): return dt.fromisoformat(d_str).strftime("%-d-%b-%y")
    date_labels=[(i,fmt_date(dates[start_idx+i])) for i in range(history_len) if 0<=start_idx+i<n]
    rows=[]
    for sym, v in mswing_data.items():
        row={"Stock Name":sym}
        for i,label in date_labels:
            if v["mswing_history"][i] is not None: row[label]=v["mswing_history"][i]
        rows.append(row)
    return rows


# ══════════════════════════════════════════════════════════════
# SCREENER FEED BUILDER
# ══════════════════════════════════════════════════════════════

def _calc_rsi(closes, period=14):
    """RSI from close prices list."""
    if len(closes) < period + 1:
        return None
    gains = []; losses = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period-1) + gains[i]) / period
        avg_loss = (avg_loss * (period-1) + losses[i]) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100/(1+rs), 2)
# ══════════════════════════════════════════════════════════════
# PATTERN HISTORY BACKUP (yearly rollover)
# ══════════════════════════════════════════════════════════════

PATTERN_BACKUP_FIELDS = [
    "ib", "dib", "nr7", "pullback",                          # daily candle
    "wib", "w_dib", "w_nr7", "w_3tc",                        # weekly candle
    "mcp", "launchpad", "bs", "pp", "atr_tightness", "vol_footprint",  # setups
    "new_52wh", "new_52wl",                                  # 52W breakout
    "hvq", "hvm", "hvy", "lvq", "lvm", "lvy",                # volume (VD excluded)
    "hpbc", "tl_hl_bo",                                      # sheet flags
]
HLR_STATE_KEYS = {
    "BO": "hlr_bo", "Near HLR": "hlr_near", "Consolidating near HLR": "hlr_consol",
}

def _build_pattern_day(feed):
    """screener_feed list -> {signal: [symbols]} (sirf fired signals)."""
    day = {}
    for f in PATTERN_BACKUP_FIELDS:
        syms = [r["symbol"] for r in feed if r.get(f)]
        if syms: day[f] = syms
    for state, key in HLR_STATE_KEYS.items():
        syms = [r["symbol"] for r in feed if r.get("hlr_state") == state]
        if syms: day[key] = syms
    return day

async def backup_pattern_history(client, feed, today):
    """Fetch yearly file -> aaj ki date key merge -> wapas upload. Re-run safe."""
    fname = f"pattern_history_{today[:4]}.json"
    day = _build_pattern_day(feed)
    if not day:
        log.info(f"  🗄  pattern backup: no patterns on {today}, skip"); return
    hist = await r2_download(client, fname)
    if not isinstance(hist, dict): hist = {}
    hist[today] = day
    await r2_upload(client, fname, json.dumps(hist, separators=(",", ":")))
    n_sym = len({s for v in day.values() for s in v})
    log.info(f"  🗄  pattern_history: {today} → {fname}  "
             f"({len(day)} signals, {n_sym} stocks, {len(hist)} dates)")

def _build_screener_feed(
    all_data, classification, rs_data, mswing_data,
    result_calendar, sheet_data, today,
    hlr_map=None, pb_map=None, pat_map=None
):
    """Build unified screener_feed.json from all sources."""

    # classification lookup
    cls_map = {}
    for x in (classification or []):
        sym = x.get("symbol") or x.get("nse_code")
        if sym: cls_map[sym] = x

    # result dates per symbol
    result_map = {}
    for date_str, syms in (result_calendar or {}).items():
        for sym in syms:
            if sym not in result_map or date_str > result_map[sym]:
                result_map[sym] = date_str

    feed = []

    for sym, s in all_data.items():
        dates  = s["d"]; opens = s["o"]; highs = s["h"]
        lows   = s["l"]; closes = s["c"]; volumes = s["v"]
        n = len(dates)
        if n < 20: continue

        # ── Last candle ──
        ltp      = closes[-1]
        prev_cls = closes[-2] if n >= 2 else None
        pct_ch   = round((ltp - prev_cls) / prev_cls * 100, 2) if ltp and prev_cls else None
        vol      = volumes[-1]

        if not ltp: continue

        # ── 52W High/Low (252 trading days) ──
        w52_highs = [v for v in highs[-252:] if v is not None]
        w52_lows  = [v for v in lows[-252:]  if v is not None]
        high52    = max(w52_highs) if w52_highs else None
        low52     = min(w52_lows)  if w52_lows  else None
        whd52     = round((ltp - high52) / high52 * 100, 2) if high52 else None
        wld52     = round((ltp - low52)  / low52  * 100, 2) if low52  else None
        new_52wh  = bool(high52 and ltp >= high52)
        new_52wl  = bool(low52  and ltp <= low52)

        # ── RVol (vs 20d avg) ──
        avg_vol20 = sum(v for v in volumes[-21:-1] if v) / 20 if n >= 21 else None
        rvol      = round(vol / avg_vol20, 2) if avg_vol20 and vol else None

        # ── %ATR 14d ──
        trs = []
        for i in range(max(1, n-14), n):
            h=highs[i]; l=lows[i]; pc=closes[i-1]
            if None in (h,l,pc): continue
            trs.append(max(h-l, abs(h-pc), abs(l-pc)))
        atr14    = sum(trs)/len(trs) if trs else None
        pct_atr  = round(atr14/ltp*100, 2) if atr14 and ltp else None

        # ── %BBW 20d ──
        cls20 = [v for v in closes[-20:] if v is not None]
        if len(cls20) >= 20:
            sma20   = sum(cls20) / 20
            std20   = (sum((x-sma20)**2 for x in cls20)/20)**0.5
            upper   = sma20 + 2*std20; lower = sma20 - 2*std20
            pct_bbw = round((upper-lower)/sma20*100, 2) if sma20 else None
        else:
            pct_bbw = None

        # ── EMA helpers ──
        def ema(period):
            if n < period: return None
            k = 2/(period+1)
            vals = [v for v in closes[:period] if v]
            if not vals: return None
            e = sum(vals)/len(vals)
            for v in closes[period:]:
                e = v*k + e*(1-k) if v else e
            return round(e, 2)

        ema10  = ema(10);  ema21 = ema(21)
        ema50  = ema(50);  ema200 = ema(200)

        above_21  = bool(ema21  and ltp > ema21)
        above_50  = bool(ema50  and ltp > ema50)
        above_200 = bool(ema200 and ltp > ema200)
        gt_50_200 = bool(ema50  and ema200 and ema50 > ema200)
        gt_21_50  = bool(ema21  and ema50  and ema21 > ema50)

        # ── EMD% ──
        emad10  = round((ltp-ema10) /ema10 *100, 2) if ema10  else None
        emad21  = round((ltp-ema21) /ema21 *100, 2) if ema21  else None
        emad50  = round((ltp-ema50) /ema50 *100, 2) if ema50  else None

        # ── Returns ──
        def ret(n_days):
            idx = n - 1 - n_days
            if idx < 0 or closes[idx] is None or not closes[idx]: return None
            return round((ltp - closes[idx]) / closes[idx] * 100, 2)

        mg1=ret(21); mg3=ret(63); mg6=ret(126); mg9=ret(189); mg12=ret(252)

        # ── Range ──
        def rng(n_days):
            h = [v for v in highs[-n_days:] if v]
            l = [v for v in lows[-n_days:]  if v]
            if not h or not l or not ltp: return None
            return round((max(h)-min(l))/ltp*100, 2)

        range3d=rng(3); range5d=rng(5)

        # ── RSI Daily ──
        drsi = _calc_rsi([v for v in closes[-30:] if v is not None])

        # ── RSI Weekly (build weekly closes) ──
        from datetime import date as dt
        weekly_closes = []
        week_map = {}
        for i, (d, c) in enumerate(zip(dates, closes)):
            if c is None: continue
            try:
                parts = d.split("-")
                d_norm = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                wk = dt.fromisoformat(d_norm).isocalendar()[:2]
                week_map[wk] = c
            except: continue
        weekly_closes = [week_map[k] for k in sorted(week_map.keys())]
        wrsi = _calc_rsi(weekly_closes[-30:]) if len(weekly_closes) >= 15 else None

        # ── RSI Monthly ──
        monthly_closes = []
        month_map = {}
        for d, c in zip(dates, closes):
            if c is None: continue
            try:
                parts = d.split("-")
                mk = f"{parts[0]}-{parts[1].zfill(2)}"
                month_map[mk] = c
            except: continue
        monthly_closes = [month_map[k] for k in sorted(month_map.keys())]
        mrsi = _calc_rsi(monthly_closes) if len(monthly_closes) >= 15 else None

        # ── Volume patterns ──
        sma_vol20 = sum(v for v in volumes[-21:-1] if v) / 20 if n >= 21 else None
        sma_vol50 = sum(v for v in volumes[-51:-1] if v) / 50 if n >= 51 else None
        # ── Turnover (₹ Cr) ──

        turnover = None
        if sma_vol50 and n >= 51:
            closes50 = [c for c in closes[-51:-1] if c is not None]
            if len(closes50) >= 40:
                avg_price50 = sum(closes50) / len(closes50)
                turnover = round(
                    (avg_price50 * sma_vol50) / 10000000,
                    2
                )
        elif sma_vol20 and n >= 21:
              closes20 = [c for c in closes[-21:-1] if c is not None]
              if len(closes20) >= 15:
                  avg_price20 = sum(closes20) / len(closes20)
                  turnover = round(
                      (avg_price20 * sma_vol20) / 10000000,
                      2
              )
        if turnover is None and ltp and vol:
            turnover = round(
                (ltp * vol) / 10000000,
                2
            )
        # VD — volume dry up
        sma_ref = sma_vol50 if sma_vol50 else sma_vol20
        vd = bool(sma_ref and vol and vol < sma_ref * 0.5)

        # HVQ / HVM / HVY / LVQ / LVM / LVY
        vols_63  = [v for v in volumes[-64:-1] if v]
        vols_21  = [v for v in volumes[-22:-1] if v]
        vols_252 = [v for v in volumes[-253:-1] if v]
        hvq = bool(vols_63  and vol and vol > max(vols_63))
        hvm = bool(vols_21  and vol and vol > max(vols_21))
        hvy = bool(vols_252 and vol and vol > max(vols_252))
        lvq = bool(vols_63  and vol and vol < min(vols_63))
        lvm = bool(vols_21  and vol and vol < min(vols_21))
        lvy = bool(vols_252 and vol and vol < min(vols_252))

        # Volume Footprint — unusual volume in last 21 days
        unusual_vol_idx = None
        if vols_252:
            max_vol_252 = max(volumes[-252:])
            for i in range(n-1, max(n-22, 0), -1):
                if volumes[i] and volumes[i] >= max_vol_252 * 0.95:
                    unusual_vol_idx = i; break
        vol_footprint = bool(
            (hvq or hvm or hvy or (rvol and rvol >= 5.0)) and
            (unusual_vol_idx is not None and (n - 1 - unusual_vol_idx) <= 21)
        )

        # ── EMA of lows (33) for patterns ──
        def ema_series(arr, period):
            """Return full EMA series"""
            if len(arr) < period: return [None]*len(arr)
            result = [None]*len(arr)
            k = 2/(period+1)
            vals = [v for v in arr[:period] if v]
            if not vals: return result
            result[period-1] = sum(vals)/len(vals)
            for i in range(period, len(arr)):
                v = arr[i]
                result[i] = v*k + result[i-1]*(1-k) if v and result[i-1] else result[i-1]
            return result

        ema_low33_series = ema_series(lows, 33)
        ema_low33 = ema_low33_series[-1]

        # ── ATR Tightness ──
        atr_tightness = bool(
            range3d is not None and pct_atr is not None and
            ema50 is not None and
            range3d <= pct_atr and ltp > ema50
        )

        # ── Bull Snort (BS) ──
        candle_range = highs[-1] - lows[-1] if highs[-1] and lows[-1] else 0
        close_pos = (ltp - lows[-1]) / candle_range if candle_range > 0 else 0
        bs = bool(
            rvol and rvol >= 2.0 and
            closes[-2] and ltp > closes[-2] and
            close_pos >= 0.65
        )

        # ── Pocket Pivot (PP) ──
        down_vols_20 = [volumes[i] for i in range(max(0,n-21), n-1)
                        if closes[i] is not None and opens[i] is not None
                        and closes[i] < opens[i] and volumes[i]]
        max_down_vol = max(down_vols_20) if down_vols_20 else 0
        pp = bool(
            opens[-1] and ltp > opens[-1] and
            vol and vol > max_down_vol and
            close_pos >= 0.5
        )

        # ── MCP detection (for Launchpad) ──
        mcp_high = mcp_low = None
        seen_mothers = set()
        for m_idx in range(n - 4, max(0, n - 60), -1):
            mh = highs[m_idx]; ml = lows[m_idx]
            if mh is None or ml is None: continue
            mk = round(mh * 200)
            if mk in seen_mothers: continue
            baby_count = 0; intact = True
            for b in range(m_idx+1, n):
                if highs[b] is None or lows[b] is None: continue
                if highs[b] > mh or lows[b] < ml: intact = False; break
                baby_count += 1
            if baby_count >= 3 and intact:
                seen_mothers.add(mk)
                mcp_high = mh; mcp_low = ml; break

        mcp_flag = mcp_high is not None

        # ── Launchpad ──
        launchpad = bool(
            mcp_flag and ema10 and ema21 and ema50 and
            mcp_low <= ema10 <= mcp_high and
            mcp_low <= ema21 <= mcp_high and
            mcp_low <= ema50 <= mcp_high
        )

        # ── Gap Filling ──
        gap_fill_state = None
        for i in range(n-2, max(0, n-120), -1):
            if i == 0: break
            prev_low = lows[i-1]; today_high = highs[i]
            if prev_low is None or today_high is None: continue
            if today_high >= prev_low: continue
            gap_pct = (prev_low - today_high) / prev_low * 100
            if gap_pct < 2.0: continue
            # Check unfilled — no candle after gap crossed prev_low
            gap_top = prev_low
            filled = any(highs[j] and highs[j] >= gap_top for j in range(i+1, n-1))
            if filled: continue
            # Current price near gap (within 5%)
            dist_pct = (gap_top - ltp) / gap_top * 100
            if 0 <= dist_pct <= 5.0:
                # Consolidating = last 5 days near gap
                recent_near = all(
                    highs[j] and lows[j] and
                    abs((gap_top - closes[j]) / gap_top * 100) <= 5.0
                    for j in range(max(0, n-6), n-1)
                    if closes[j]
                )
                gap_fill_state = "Consolidating near Gap" if recent_near else "Near Gap"
                break

        # ── RS data ──
        rs_info  = rs_data.get(sym, {})
        ms_info  = mswing_data.get(sym, {})
        cls_info = cls_map.get(sym, {})
        sh_info  = sheet_data.get(sym, {})
        # ── Mansfield RS vs index (merged into rs_data before this call) ──
        IDX_SHORT = {"nifty50": "n50", "nifty500": "n500", "smallmid400": "sm400"}
        rs_idx = {}
        for ikey, short in IDX_SHORT.items():
            rs_idx[f"rs_val_{short}"]   = rs_info.get(f"rs_val_{ikey}")
            rs_idx[f"rs_nh21_{short}"]  = rs_info.get(f"rs_nh_21_{ikey}")
            rs_idx[f"rs_nl21_{short}"]  = rs_info.get(f"rs_nl_21_{ikey}")
            rs_idx[f"rs_div21_{short}"] = rs_info.get(f"rs_div_21_{ikey}")
            rs_idx[f"rs_nh50_{short}"]  = rs_info.get(f"rs_nh_50_{ikey}")
            rs_idx[f"rs_nl50_{short}"]  = rs_info.get(f"rs_nl_50_{ikey}")
            rs_idx[f"rs_div50_{short}"] = rs_info.get(f"rs_div_50_{ikey}")
        # ── Result date ──
        result_date = result_map.get(sym)

        row = {
            "symbol"    : sym,
            "name"      : cls_info.get("name", ""),
            "tv_code"   : sh_info.get("tv_code", f"NSE:{sym},"),
            "sector"    : cls_info.get("sector_group", ""),
            "industry"  : cls_info.get("display_industry", ""),
            "mcap"      : cls_info.get("market_cap_cr"),
            "themes"    : cls_info.get("themes", []),

            # Price
            "ltp"       : ltp,
            "pct_ch"    : pct_ch,
            "volume"    : vol,
            "rvol"      : rvol,

            # 52W
            "high52"    : high52,
            "low52"     : low52,
            "52whd"     : whd52,
            "52wld"     : wld52,
            "new_52wh"  : new_52wh,
            "new_52wl"  : new_52wl,

            # Technical
            "pct_atr"   : pct_atr,
            "pct_bbw"   : pct_bbw,
            "ema10"     : ema10,
            "ema21"     : ema21,
            "ema50"     : ema50,
            "ema200"    : ema200,
            "emad10"    : emad10,
            "emad21"    : emad21,
            "emad50"    : emad50,
            "above_21"  : above_21,
            "above_50"  : above_50,
            "above_200" : above_200,
            "gt_50_200" : gt_50_200,
            "gt_21_50"  : gt_21_50,

            # Returns
            "1mg"       : mg1,
            "3mg"       : mg3,
            "6mg"       : mg6,
            "9mg"       : mg9,
            "12mg"      : mg12,
            "range3d"   : range3d,
            "range5d"   : range5d,

            # RSI
            "drsi"      : drsi,
            "wrsi"      : wrsi,
            "mrsi"      : mrsi,

            # RS
            "rs_rating" : rs_info.get("rs"),
            "mswing"    : ms_info.get("mswing"),
            "mswing_avg9": ms_info.get("mswing_avg9"),
            **rs_idx,
            # Fundamentals (from rs_data enriched by fund_lookup later)
            "sales_ch"  : None,
            "eps_ch"    : None,

            # Patterns (filled later)
            "patterns"  : "",

            # Result
            "results"   : result_date,

            # Volume patterns
            "vd"            : vd,
            "hvq"           : hvq,
            "hvm"           : hvm,
            "hvy"           : hvy,
            "lvq"           : lvq,
            "lvm"           : lvm,
            "lvy"           : lvy,
            "to"            : turnover,
            "vol_footprint" : vol_footprint,

            # Price patterns
            "atr_tightness" : atr_tightness,
            "bs"            : bs,
            "pp"            : pp,
            "mcp"           : mcp_flag,
            "mcp_high"      : mcp_high,
            "mcp_low"       : mcp_low,
            "launchpad"     : launchpad,
            "gap_fill"      : gap_fill_state,

            # Pattern signals from pattern_signals.json
            "ib"        : "Inside Bar"        in (pat_map or {}).get(sym, set()),
            "dib"       : "Double Inside Bar" in (pat_map or {}).get(sym, set()),
            "nr7"       : "NR7"               in (pat_map or {}).get(sym, set()),
            "wib"       : "Weekly IB"         in (pat_map or {}).get(sym, set()),
            "w_dib"     : "Weekly Double IB"  in (pat_map or {}).get(sym, set()),
            "w_nr7"     : "Weekly NR7"        in (pat_map or {}).get(sym, set()),
            "w_3tc"     : "Weekly Tight Close" in (pat_map or {}).get(sym, set()),

            # HLR & Pullback
            "hlr_state" : (hlr_map or {}).get(sym, {}).get("state"),
            "hlr_res"   : (hlr_map or {}).get(sym, {}).get("resistance"),
            "hlr_dist"  : (hlr_map or {}).get(sym, {}).get("dist_pct"),
            "hlr_touches": (hlr_map or {}).get(sym, {}).get("touches"),
            "pullback"  : sym in (pb_map or {}),

            # Sheet fields
            "circuit"   : sh_info.get("circuit"),
            "hpbc"      : sh_info.get("hpbc"),
            "tl_hl_bo"  : sh_info.get("tl_hl_bo"),
        }

        feed.append(row)

    log.info(f"screener_feed: {len(feed)} stocks")
    return feed


# ══════════════════════════════════════════════════════════════
# EP SCAN — MAIN
# ══════════════════════════════════════════════════════════════

async def run_ep_scan() -> None:
    today = today_ist()
    log.info(f"━━━ EP + Post-Result + RS Scan  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        today_symbols = await get_result_symbols_finedge(client)
        if today_symbols: await save_result_calendar(client, today_symbols, today)

        # Download everything in parallel
        ohlc_tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
        (ohlc_results, screener_raw, fund_raw, cal_raw, classification,
         idx_hist_n50, idx_hist_n500, idx_hist_sm400, idx_daily, sheet_raw,
         hlr_raw, pb_raw, pat_raw) = await asyncio.gather(
            asyncio.gather(*ohlc_tasks, return_exceptions=True),
            r2_download(client, "screener.json"),
            r2_download_fund(client),
            r2_download(client, "result_calendar.json"),
            r2_download(client, "classification.json"),
            r2_download(client, f"index_history/{INDEX_SYMBOLS['nifty50']}.json"),
            r2_download(client, f"index_history/{INDEX_SYMBOLS['nifty500']}.json"),
            r2_download(client, f"index_history/{INDEX_SYMBOLS['smallmid400']}.json"),
            r2_download(client, "index_daily.json"),
            r2_download(client, "sheet_data.json"),
            r2_download(client, "hlr_signals.json"),
            r2_download(client, "pullback_signals.json"),
            r2_download(client, "pattern_signals.json"),
        )

        # OHLC
        all_data = {}
        for i, res in enumerate(ohlc_results):
            if isinstance(res, Exception): log.warning(f"  ohlc_{i+1}.json error: {res}")
            elif res and "stocks" in res: all_data.update(res["stocks"])
        log.info(f"Loaded {len(all_data)} stocks")

        # Screener
        screener = {}
        if isinstance(screener_raw, list):
            for row in screener_raw:
                sym=row.get("Stocks","").strip()
                if not sym: continue
                try: sc=float(row.get("SALES CH%",0))*100; sales_ch=f"+{sc:.1f}%" if sc>=0 else f"{sc:.1f}%"
                except: sales_ch=""
                try: ec=float(row.get("EPS CHANGE",0))*100; eps_ch=f"+{ec:.1f}%" if ec>=0 else f"{ec:.1f}%"
                except: eps_ch=""
                pat_cols=["NR7","WIB","DIB","MCP","W-MCP","HVQ","VD","PullBack","ATR Tightness",
                          "Volume footprint","Launchpad","HLR","BS","GAPUP","PP","HPBC","TL/HL BO","3WTC"]
                combined=set()
                for p in (row.get("Patterns","") or "").split("||"):
                    p=p.strip()
                    if p: combined.add(p)
                for col in pat_cols:
                    v=row.get(col,"")
                    if v and v not in ("",None,0,"No"): combined.add(v if isinstance(v,str) else col)
                screener[sym]={"sales_ch":sales_ch,"eps_ch":eps_ch,
                    "patterns":"||".join(sorted(combined)),
                    "sector":row.get("SECTOR",""),"rs":row.get("RS Rating",""),"ltp":row.get("LTP","")}

        fund_lookup = {}
        if isinstance(fund_raw, dict): fund_lookup=fund_raw
        elif isinstance(fund_raw, list): fund_lookup={d["symbol"]:d for d in fund_raw if d.get("symbol")}
        log.info(f"Fundamentals: {len(fund_lookup)} stocks")

        result_calendar = cal_raw if isinstance(cal_raw, dict) else {}
        classification  = classification or []

        # Sheet data (circuit, tv_code, hpbc)
        sheet_data = {}
        if isinstance(sheet_raw, list):
            for row in sheet_raw:
                sym = row.get("symbol") or row.get("Stocks","")
                if sym: sheet_data[sym] = {
                    "circuit"  : row.get("Circuit") or row.get("circuit"),
                    "tv_code"  : row.get("TV CODE") or row.get("tv_code",""),
                    "hpbc"     : row.get("HPBC") or row.get("hpbc",""),
                    "tl_hl_bo" : row.get("TL/HL BO") or row.get("tl_hl_bo",""),
                }
        elif isinstance(sheet_raw, dict):
            sheet_data = sheet_raw
        log.info(f"Sheet data: {len(sheet_data)} stocks")

        # HLR signals map
        hlr_map = {}
        if isinstance(hlr_raw, dict):
            for sig in (hlr_raw.get("signals") or []):
                sym = sig.get("symbol")
                if sym:
                    if sym not in hlr_map or sig.get("touches",0) > hlr_map[sym].get("touches",0):
                        hlr_map[sym] = sig
        log.info(f"HLR signals: {len(hlr_map)} stocks")

        # Pullback signals map
        pb_map = {}
        if isinstance(pb_raw, dict):
            for sig in (pb_raw.get("signals") or []):
                sym = sig.get("symbol")
                if sym: pb_map[sym] = sig
        log.info(f"Pullback signals: {len(pb_map)} stocks")

        # Pattern signals map — per symbol, collect all pattern names
        pat_map = {}
        if isinstance(pat_raw, dict):
            for sig in (pat_raw.get("signals") or []):
                sym = sig.get("symbol")
                pat = sig.get("pattern")
                if sym and pat:
                    pat_map.setdefault(sym, set()).add(pat)
        log.info(f"Pattern signals: {len(pat_map)} stocks")

        # EP signals
        signals = _detect_ep(all_data)
        signals.sort(key=lambda x:(x["ep_date"],x["gap_pct"]), reverse=True)
        for sig in signals:
            sym=sig["symbol"]; sc=screener.get(sym,{})
            sig.update({"sales_ch":sc.get("sales_ch",""),"eps_ch":sc.get("eps_ch",""),
                "patterns":sc.get("patterns",""),"sector":sc.get("sector",""),
                "rs":sc.get("rs",""),"ltp":sc.get("ltp","")})
            fund=fund_lookup.get(sym,{}); pl_qtr=fund.get("pl_quarterly",[])
            sig["q_name"]=pl_qtr[0].get("header","") if pl_qtr else ""
            vol_x=sig.pop("vol_spike_x",1); sig["vol_pct"]=f"+{round((vol_x-1)*100)}%"
        log.info(f"EP signals: {len(signals)}")

        # Post-result signals
        pr_signals = []
        if result_calendar:
            pr_signals = _detect_post_result_thrust(all_data, result_calendar)
            for sig in pr_signals:
                sym=sig["symbol"]; sc=screener.get(sym,{}); fund=fund_lookup.get(sym,{})
                sig.update({"sales_ch":sc.get("sales_ch",""),"eps_ch":sc.get("eps_ch",""),
                    "patterns":sc.get("patterns",""),"sector":sc.get("sector",""),
                    "rs":sc.get("rs",""),"ltp":sc.get("ltp","")})
                pl_qtr=fund.get("pl_quarterly",[]); sig["q_name"]=pl_qtr[0].get("header","") if pl_qtr else ""
            ah=sum(1 for s in pr_signals if "AH" in s["reaction_type"])
            ih=sum(1 for s in pr_signals if "IH" in s["reaction_type"])
            log.info(f"Post-Result signals: {len(pr_signals)}  AH:{ah}  IH:{ih}")

        # Percentile RS
        rs_data         = _calculate_rs(all_data, history_days=60)
        rs_history_list = _build_rs_history_json(all_data, rs_data)
        for sig in signals:    sig["rs_calc"]=rs_data.get(sig["symbol"],{}).get("rs")
        for sig in pr_signals: sig["rs_calc"]=rs_data.get(sig["symbol"],{}).get("rs")

        # Mansfield RS — build index close maps (history + today's daily feed)
        idx_daily = idx_daily or {}
        index_maps = {
            "nifty50"    : _build_index_close_map(idx_hist_n50,
                idx_daily.get(INDEX_SYMBOLS["nifty50"],    {}).get("close"), today),
            "nifty500"   : _build_index_close_map(idx_hist_n500,
                idx_daily.get(INDEX_SYMBOLS["nifty500"],   {}).get("close"), today),
            "smallmid400": _build_index_close_map(idx_hist_sm400,
                idx_daily.get(INDEX_SYMBOLS["smallmid400"],{}).get("close"), today),
        }
        log.info(f"Index close maps — N50:{len(index_maps['nifty50'])}  "
                 f"N500:{len(index_maps['nifty500'])}  SM400:{len(index_maps['smallmid400'])}")

        mansfield = _calculate_mansfield_rs(all_data, index_maps)

        # Merge mansfield into rs_ratings
        for sym in rs_data:
            m = mansfield.get(sym, {})
            for idx_key, metrics in m.items():
                for k, v in metrics.items():
                    rs_data[sym][f"{k}_{idx_key}"] = v

        # Sector / Industry RS history
        sector_group_rs_history = _build_group_rs_history(classification, rs_history_list, "sector_group")
        industry_rs_history     = _build_group_rs_history(classification, rs_history_list, "display_industry")

        # MSwing
        mswing_data = _calculate_mswing(all_data, history_days=ROLLING_DAYS-50)
        mswing_list = _build_mswing_json(all_data, mswing_data)
        for sig in signals:
            sym=sig["symbol"]; sig["mswing"]=mswing_data.get(sym,{}).get("mswing")
            sig["mswing_avg9"]=mswing_data.get(sym,{}).get("mswing_avg9")
        for sig in pr_signals:
            sym=sig["symbol"]; sig["mswing"]=mswing_data.get(sym,{}).get("mswing")
            sig["mswing_avg9"]=mswing_data.get(sym,{}).get("mswing_avg9")

        # Build screener_feed
        screener_feed = _build_screener_feed(
            all_data, classification, rs_data, mswing_data,
            result_calendar, sheet_data, today,
            hlr_map=hlr_map, pb_map=pb_map, pat_map=pat_map
        )
        # Enrich with patterns and fundamentals
        pat_map = {}
        for sig in signals:
            sym = sig["symbol"]
            pat_map.setdefault(sym, set()).add("EP")
        # Add pattern_signals patterns
        for row in screener_feed:
            sym = row["symbol"]
            sc   = screener.get(sym, {})
            fund = fund_lookup.get(sym, {})
            pl   = fund.get("pl_quarterly", [])
            row["q_name"] = pl[0].get("header","") if pl else ""

            # Sales/EPS YoY (same quarter last year = index 4)
            if pl and len(pl) >= 5:
                s0 = pl[0].get("sales"); s4 = pl[4].get("sales")
                row["sales_ch"] = round((s0-s4)/s4*100, 1) if s0 and s4 else None
                e0 = pl[0].get("eps"); e4 = pl[4].get("eps")
                row["eps_ch"]   = round((e0-e4)/e4*100, 1) if e0 and e4 else None
            else:
                row["sales_ch"] = None; row["eps_ch"] = None

            # Build patterns string from calculated flags
            pats = set()
            if row.get("vd"):            pats.add("VD")
            if row.get("hvq"):           pats.add("HVQ")
            if row.get("hvm"):           pats.add("HVM")
            if row.get("hvy"):           pats.add("HVY")
            if row.get("lvq"):           pats.add("LVQ")
            if row.get("lvm"):           pats.add("LVM")
            if row.get("lvy"):           pats.add("LVY")
            if row.get("vol_footprint"): pats.add("Volume Footprint")
            if row.get("atr_tightness"): pats.add("ATR Tightness")
            if row.get("bs"):            pats.add("BS")
            if row.get("pp"):            pats.add("PP")
            if row.get("mcp"):           pats.add("MCP")
            if row.get("launchpad"):     pats.add("Launchpad")
            if row.get("gap_fill"):      pats.add(row["gap_fill"])
            if row.get("tl_hl_bo"):      pats.add("TL/HL BO")
            if row.get("hpbc"):          pats.add("HPBC")
            if row.get("ib"):            pats.add("IB")
            if row.get("dib"):           pats.add("DIB")
            if row.get("nr7"):           pats.add("NR7")
            if row.get("wib"):           pats.add("WIB")
            if row.get("w_dib"):         pats.add("W-DIB")
            if row.get("w_nr7"):         pats.add("W-NR7")
            if row.get("w_3tc"):         pats.add("3WTC")
            if row.get("hlr_state"):     pats.add(row["hlr_state"])
            if row.get("pullback"):      pats.add("PullBack")
            # Add EP flag
            if sym in pat_map:           pats |= pat_map[sym]
            row["patterns"] = "||".join(sorted(pats))
            # Add EP flag
            if sym in pat_map:
                existing = set(row["patterns"].split("||")) if row["patterns"] else set()
                existing |= pat_map[sym]
                row["patterns"] = "||".join(sorted(existing))

        await asyncio.gather(
            r2_upload(client, "ep_signals.json",
                json.dumps({"updated":today,"count":len(signals),"signals":signals})),
            r2_upload(client, "rs_ratings.json",
                json.dumps({"updated":today,"count":len(rs_data),"stocks":rs_data})),
            r2_upload(client, "rs_history.json",          json.dumps(rs_history_list)),
            r2_upload(client, "mswing.json",              json.dumps(mswing_list)),
            r2_upload(client, "post_result_signals.json",
                json.dumps({"updated":today,"count":len(pr_signals),
                    "ah_count":sum(1 for s in pr_signals if "AH" in s["reaction_type"]),
                    "ih_count":sum(1 for s in pr_signals if "IH" in s["reaction_type"]),
                    "signals":pr_signals})),
            r2_upload(client, "sector_group_rs_history.json", json.dumps(sector_group_rs_history)),
            r2_upload(client, "industry_rs_history.json",     json.dumps(industry_rs_history)),
            r2_upload(client, "screener_feed.json",            json.dumps(screener_feed)),
            backup_pattern_history(client, screener_feed, today),
        )
        log.info(f"✅ EP:{len(signals)}  PostResult:{len(pr_signals)}  RS:{len(rs_data)}")
    log.info("━━━ EP + Post-Result + RS Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# HLR + PULLBACK
# ══════════════════════════════════════════════════════════════

def _calc_ema(closes, period):
    if len(closes) < period: return [None]*len(closes)
    ema=[None]*len(closes); k=2/(period+1)
    seed_vals=[v for v in closes[:period] if v is not None]
    if not seed_vals: return ema
    ema[period-1]=sum(seed_vals)/len(seed_vals)
    for i in range(period, len(closes)):
        c=closes[i]
        ema[i]=c*k+ema[i-1]*(1-k) if c is not None else ema[i-1]
    return ema

def _detect_pullback(all_data, length_pull=4, min_swing_range_pct=10.0,
    min_pullback_pct=5.0, ema_proximity_pct=1.0, max_candle_range_pct=6.0):
    signals=[]
    for sym,s in all_data.items():
        dates=s["d"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<60 or not _check_liquidity(volumes,closes,n): continue
        ema10=_calc_ema(closes,10); ema21=_calc_ema(closes,21); ema50=_calc_ema(closes,50)
        if any(v is None for v in [ema10[-1],ema21[-1],ema50[-1]]): continue
        ema12=_calc_ema(closes,12); ema26=_calc_ema(closes,26)
        macd_line=[(ema12[i]-ema26[i]) if ema12[i] is not None and ema26[i] is not None else None for i in range(n)]
        if len([v for v in macd_line if v is not None])<9: continue
        macd_arr=[v if v is not None else 0.0 for v in macd_line]
        macd_signal=_calc_ema(macd_arr,9)
        last_swing_high_price=last_swing_high_bar=last_swing_low_price=last_swing_low_bar=None
        for i in range(length_pull,n-length_pull):
            if all(highs[i]>=highs[i-k] for k in range(1,length_pull+1) if highs[i-k] is not None) and \
               all(highs[i]>=highs[i+k] for k in range(1,length_pull+1) if highs[i+k] is not None):
                last_swing_high_price=highs[i]; last_swing_high_bar=i
            if all(lows[i]<=lows[i-k] for k in range(1,length_pull+1) if lows[i-k] is not None) and \
               all(lows[i]<=lows[i+k] for k in range(1,length_pull+1) if lows[i+k] is not None):
                last_swing_low_price=lows[i]; last_swing_low_bar=i
        if last_swing_high_price is None or last_swing_low_price is None: continue
        i=n-1
        if last_swing_high_bar is None or last_swing_low_bar is None: continue
        if last_swing_high_bar<=last_swing_low_bar: continue
        if closes[i] is None or highs[i] is None or lows[i] is None: continue
        swing_range_pct=(last_swing_high_price-last_swing_low_price)/last_swing_low_price*100
        if swing_range_pct<min_swing_range_pct: continue
        pullback_pct=(last_swing_high_price-lows[i])/last_swing_high_price*100
        if pullback_pct<min_pullback_pct: continue
        e10=ema10[i]; e21=ema21[i]
        near_ema10=abs(lows[i]-e10)/e10*100<=ema_proximity_pct or abs(closes[i]-e10)/e10*100<=ema_proximity_pct
        near_ema21=abs(lows[i]-e21)/e21*100<=ema_proximity_pct or abs(closes[i]-e21)/e21*100<=ema_proximity_pct
        reversal_ema10=lows[i]<e10 and closes[i]>e10
        reversal_ema21=lows[i]<e21 and closes[i]>e21
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
        signals.append({"symbol":sym,"date":dates[i],"close":round(closes[i],2),
            "swing_high":round(last_swing_high_price,2),"swing_low":round(last_swing_low_price,2),
            "swing_range_pct":round(swing_range_pct,2),"pullback_pct":round(pullback_pct,2),
            "ema10":round(e10,2),"ema21":round(e21,2),"ema50":round(e50,2),
            "ema_touch":ema_touch,"candle_range_pct":round(candle_range_pct,2),
            "macd":round(macd_line[i],4),"macd_signal":round(macd_signal[i],4)})
    return signals

def _detect_hlr(all_data, swing_n=5, cluster_pct=2.0, near_pct=4.0, consol_days=5, consol_pct=4.0):
    signals=[]
    for sym,s in all_data.items():
        dates=s["d"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<swing_n*2+consol_days+2 or not _check_liquidity(volumes,closes,n): continue
        vol_lookback=min(50,n-1)
        avg_vol_50=sum(volumes[-vol_lookback-1:-1])/vol_lookback if vol_lookback>0 else 0
        vol_spike=volumes[-1]/avg_vol_50 if avg_vol_50>0 and vol_lookback>=20 else None
        swing_highs=[]
        for i in range(swing_n,n-swing_n):
            if all(highs[i]>=highs[i-k] for k in range(1,swing_n+1)) and \
               all(highs[i]>=highs[i+k] for k in range(1,swing_n+1)):
                sh_price=highs[i]
                if any(closes[j]>sh_price for j in range(i+1,n-1) if closes[j] is not None): continue
                broke_today=closes[-1] is not None and closes[-1]>sh_price
                swing_highs.append((sh_price,dates[i],"BO" if broke_today else "valid"))
        if not swing_highs: continue
        swing_highs.sort(key=lambda x:x[0],reverse=True); used=[False]*len(swing_highs); levels=[]
        for i,(h,d,tag) in enumerate(swing_highs):
            if used[i]: continue
            cluster=[(h,d,tag)]
            for j in range(i+1,len(swing_highs)):
                if not used[j] and abs(swing_highs[j][0]-h)/h*100<=cluster_pct:
                    cluster.append(swing_highs[j]); used[j]=True
            used[i]=True
            level=max(c[0] for c in cluster); zone_low=min(c[0] for c in cluster)
            cluster_tag="BO" if any(c[2]=="BO" for c in cluster) else "valid"
            touch_pts=sorted([{"date":c[1],"price":round(c[0],2)} for c in cluster],key=lambda x:x["date"])
            levels.append((level,zone_low,len(cluster),len(cluster)>=2,touch_pts,cluster_tag))
        curr_close=closes[-1]
        if curr_close is None: continue
        curr_date=dates[-1]
        if n>=consol_days:
            rh=[v for v in highs[-consol_days:] if v is not None]
            rl=[v for v in lows[-consol_days:] if v is not None]
            range_pct=(max(rh)-min(rl))/curr_close*100 if rh and rl else 0
            is_consol=range_pct<consol_pct
        else: range_pct=0; is_consol=False
        for (level,zone_low,touches,is_zone,touch_pts,cluster_tag) in levels:
            dist_pct=(level-curr_close)/level*100
            if cluster_tag=="BO":
                vol_ok=(vol_spike is None) or (vol_spike>=2.0)
                state="BO" if vol_ok else "Near HLR"
            elif 0<=dist_pct<=near_pct:
                state="Consolidating near HLR" if is_consol else "Near HLR"
            else: continue
            signals.append({"symbol":sym,"state":state,"resistance":round(level,2),
                "zone_low":round(zone_low,2),"is_zone":is_zone,"touches":touches,
                "touch_points":touch_pts,"dist_pct":round(dist_pct,2),
                "last_close":round(curr_close,2),"last_date":curr_date,
                "consol_range":round(range_pct,2),
                "vol_spike":round(vol_spike,1) if vol_spike is not None else None})
    return signals

async def run_hlr_scan() -> None:
    today=today_ist()
    #if not is_trading_day(today): log.info(f"⏭  {today} not a trading day"); return
    log.info(f"━━━ HLR + Pullback Scan  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP,BSE_ISIN_MAP,BSE_META
        ISIN_MAP,BSE_ISIN_MAP,BSE_META=await build_isin_map(client)
        all_data=await download_all_chunks(client)
        log.info(f"Loaded {len(all_data)} stocks")
        hlr_signals=_detect_hlr(all_data)
        order={"BO":0,"Consolidating near HLR":1,"Near HLR":2}
        hlr_signals.sort(key=lambda x:(order.get(x["state"],9),-x["touches"]))
        bo=sum(1 for s in hlr_signals if s["state"]=="BO")
        consol=sum(1 for s in hlr_signals if s["state"]=="Consolidating near HLR")
        near=sum(1 for s in hlr_signals if s["state"]=="Near HLR")
        log.info(f"HLR — BO:{bo} Consolidating:{consol} Near:{near} Total:{len(hlr_signals)}")
        pb_signals=_detect_pullback(all_data)
        pb_signals.sort(key=lambda x:x["pullback_pct"],reverse=True)
        log.info(f"Pullback signals: {len(pb_signals)}")
        await asyncio.gather(
            r2_upload(client,"hlr_signals.json",json.dumps({"updated":today,"count":len(hlr_signals),
                "bo":bo,"consolidating":consol,"near":near,"signals":hlr_signals})),
            r2_upload(client,"pullback_signals.json",json.dumps({"updated":today,
                "count":len(pb_signals),"signals":pb_signals})),
        )
        log.info(f"✅ HLR:{len(hlr_signals)}  Pullback:{len(pb_signals)}")
    log.info("━━━ HLR + Pullback Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# PATTERN SCAN
# ══════════════════════════════════════════════════════════════

def _build_weekly(dates, opens, highs, lows, closes, volumes):
    from datetime import date as dt
    weekly={}
    for d,o,h,l,c,v in zip(dates,opens,highs,lows,closes,volumes):
        if h is None or l is None or c is None: continue
        key=dt.fromisoformat(d).isocalendar()[:2]
        if key not in weekly: weekly[key]={"o":o,"h":h,"l":l,"c":c,"v":v or 0,"d":d}
        else:
            weekly[key]["h"]=max(weekly[key]["h"],h)
            weekly[key]["l"]=min(weekly[key]["l"],l)
            weekly[key]["c"]=c; weekly[key]["v"]+=v or 0
    return weekly

def _detect_patterns(all_data, min_volume=2500, coil_min_babies=3,
                     tight_close_weeks=3, tight_close_pct=2.0):
    from datetime import date as dt
    signals=[]
    for sym,s in all_data.items():
        dates=s["d"]; opens=s["o"]; highs=s["h"]; lows=s["l"]; closes=s["c"]; volumes=s["v"]; n=len(dates)
        if n<10 or not _check_liquidity(volumes,closes,n): continue
        if any(v is None for v in [highs[-1],highs[-2],lows[-1],lows[-2],closes[-1]]): continue
        if volumes[-1] is None or volumes[-1]<min_volume: continue
        today_d=dates[-1]
        # Inside Bar
        if highs[-1]<=highs[-2] and lows[-1]>=lows[-2]:
            signals.append({"symbol":sym,"pattern":"Inside Bar","date":today_d,
                "high":round(highs[-1],2),"low":round(lows[-1],2),
                "prev_high":round(highs[-2],2),"prev_low":round(lows[-2],2)})
        # Double Inside Bar
        if n>=3 and highs[-3] is not None and lows[-3] is not None:
            if highs[-1]<=highs[-2] and lows[-1]>=lows[-2] and highs[-2]<=highs[-3] and lows[-2]>=lows[-3]:
                signals.append({"symbol":sym,"pattern":"Double Inside Bar","date":today_d,
                    "high":round(highs[-1],2),"low":round(lows[-1],2),
                    "mother_high":round(highs[-3],2),"mother_low":round(lows[-3],2)})
        # NR7
        if n>=7:
            last7_h=[highs[-i] for i in range(1,8)]; last7_l=[lows[-i] for i in range(1,8)]
            if all(v is not None for v in last7_h+last7_l):
                today_range=last7_h[0]-last7_l[0]
                if today_range<=min(last7_h[i]-last7_l[i] for i in range(1,7)):
                    signals.append({"symbol":sym,"pattern":"NR7","date":today_d,
                        "range":round(today_range,2),"high":round(highs[-1],2),"low":round(lows[-1],2)})
        # MCP
        seen_mothers=set()
        for m_idx in range(n-coil_min_babies-1, max(0,n-60), -1):
            m_high=highs[m_idx]; m_low=lows[m_idx]
            if m_high is None or m_low is None: continue
            m_key=round(m_high*200)
            if m_key in seen_mothers: continue
            baby_count=0; coil_state="Coiling"
            for b in range(m_idx+1,n):
                if highs[b] is None or lows[b] is None: continue
                if highs[b]>m_high: coil_state="Upper BO"; break
                elif lows[b]<m_low: coil_state="Lower BD"; break
                else: baby_count+=1
            if baby_count>=coil_min_babies and coil_state=="Coiling":
                seen_mothers.add(m_key)
                signals.append({"symbol":sym,"pattern":f"{baby_count} Bar MCP" if baby_count<=6 else "Mini Coil",
                    "date":today_d,"mcp_high":round(m_high,2),"mcp_low":round(m_low,2),
                    "baby_count":baby_count,"coil_state":coil_state,"mother_date":dates[m_idx]})
        # Weekly patterns
        weekly=_build_weekly(dates,opens,highs,lows,closes,volumes)
        if not weekly: continue
        current_week=dt.fromisoformat(today_d).isocalendar()[:2]
        past_weeks=sorted(k for k in weekly if k<current_week)
        if len(past_weeks)<2: continue
        lw=weekly[past_weeks[-1]]; lw2=weekly[past_weeks[-2]]
        if lw["h"]<=lw2["h"] and lw["l"]>=lw2["l"]:
            signals.append({"symbol":sym,"pattern":"Weekly IB","date":today_d,
                "w_high":round(lw["h"],2),"w_low":round(lw["l"],2),"w_close":round(lw["c"],2),
                "prev_w_high":round(lw2["h"],2),"prev_w_low":round(lw2["l"],2)})
            if len(past_weeks)>=3:
                lw3=weekly[past_weeks[-3]]
                if lw2["h"]<=lw3["h"] and lw2["l"]>=lw3["l"]:
                    signals.append({"symbol":sym,"pattern":"Weekly Double IB","date":today_d,
                        "w_high":round(lw["h"],2),"w_low":round(lw["l"],2),
                        "mother_w_high":round(lw3["h"],2),"mother_w_low":round(lw3["l"],2)})
        if len(past_weeks)>=7:
            lw_range=lw["h"]-lw["l"]
            if lw_range<=min(weekly[past_weeks[-i]]["h"]-weekly[past_weeks[-i]]["l"] for i in range(2,8)):
                signals.append({"symbol":sym,"pattern":"Weekly NR7","date":today_d,
                    "w_range":round(lw_range,2),"w_high":round(lw["h"],2),"w_low":round(lw["l"],2)})
        if len(past_weeks)>=tight_close_weeks:
            last_n=[weekly[past_weeks[-i]]["c"] for i in range(1,tight_close_weeks+1)]
            if all(c is not None for c in last_n) and min(last_n)>0:
                tc_range=(max(last_n)-min(last_n))/min(last_n)*100
                if tc_range<=tight_close_pct:
                    signals.append({"symbol":sym,"pattern":"Weekly Tight Close","date":today_d,
                        "closes":[round(c,2) for c in last_n],"range_pct":round(tc_range,2)})
    return signals

async def run_pattern_scan() -> None:
    today=today_ist()
    if not is_trading_day(today): log.info(f"⏭  {today} not a trading day"); return
    log.info(f"━━━ Pattern Scan  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP,BSE_ISIN_MAP,BSE_META
        ISIN_MAP,BSE_ISIN_MAP,BSE_META=await build_isin_map(client)
        all_data=await download_all_chunks(client); log.info(f"Loaded {len(all_data)} stocks")
        signals=_detect_patterns(all_data)
        from collections import Counter; counts=Counter(s["pattern"] for s in signals)
        for pat,cnt in sorted(counts.items()): log.info(f"  {pat}: {cnt}")
        log.info(f"Total: {len(signals)} signals")
        await r2_upload(client,"pattern_signals.json",json.dumps({"updated":today,
            "count":len(signals),"summary":dict(counts),"signals":signals}))
    log.info("━━━ Pattern Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "daily":         asyncio.run(run_daily())
        case "today":         asyncio.run(run_today())
        case "full":          asyncio.run(run_full())
        case "status":        asyncio.run(run_status())
        case "fund_daily":    asyncio.run(run_fund_daily())
        case "fund_full":     asyncio.run(run_fund_full(0))
        case "fund_full_1":   asyncio.run(run_fund_full(1))
        case "fund_full_2":   asyncio.run(run_fund_full(2))
        case "fund_full_3":   asyncio.run(run_fund_full(3))
        case "fund_full_4":   asyncio.run(run_fund_full(4))
        case "fund_full_5":   asyncio.run(run_fund_full(5))
        case "fund_full_6":   asyncio.run(run_fund_full(6))
        case "fund_full_7":   asyncio.run(run_fund_full(7))
        case "fund_full_8":   asyncio.run(run_fund_full(8))
        case "fund_full_9":   asyncio.run(run_fund_full(9))
        case "fund_full_10":  asyncio.run(run_fund_full(10))
        case "ep_scan":       asyncio.run(run_ep_scan())
        case "hlr_scan":      asyncio.run(run_hlr_scan())
        case "pattern_scan":  asyncio.run(run_pattern_scan())
        case _:
            print(__doc__)
            sys.exit(1)
