#!/usr/bin/env python3
"""
NSE OHLC + Fundamentals Pipeline — GitHub Actions

Usage:
  python pipeline.py daily                # prev-day OHLC          (4:00 PM IST, weekdays)
  python pipeline.py today                # T+0 intraday candle     (4:50 PM IST, weekdays)
  python pipeline.py full                 # initial 1.5yr load      (manual, once)
  python pipeline.py status              # print R2 chunk summary
  python pipeline.py fund_daily          # BSE result stocks update (4:30 PM IST, weekdays)
  python pipeline.py fund_weekly         # NSE full fundamentals    (Sunday, split in parts)
  python pipeline.py fund_weekly_1..10   # NSE fundamentals parts
  python pipeline.py bse_profiles        # BSE-only profiles full   (manual / Sunday)
  python pipeline.py bse_profiles_1..10  # BSE profiles parts
  python pipeline.py ep_scan             # EP + Post-Result T+1 scan (4:35 PM IST, weekdays)
  python pipeline.py hlr_scan            # HLR + Pullback scan       (4:20 PM IST, weekdays)
  python pipeline.py pattern_scan        # Price action pattern scan  (4:25 PM IST, weekdays)
"""

import asyncio
import gzip
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

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

BASE_URL     = "https://api.upstox.com/v2/historical-candle"
V3_URL       = "https://api.upstox.com/v3/historical-candle/intraday"
FUND_URL     = "https://api.upstox.com/v2/fundamentals"
ROLLING_DAYS = 548
R2_CHUNKS    = 8
CONCURRENCY  = 5
FUND_CONCURRENCY = 1
RETRY        = 3
SLEEP_MS     = 3.0
RATE_DELAY   = 0.5

HERE = Path(__file__).parent

NSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
BSE_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"

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
    log.info("Fetching instrument files from Upstox…")
    nse_r, bse_r = await asyncio.gather(
        client.get(NSE_INSTRUMENTS_URL, timeout=60),
        client.get(BSE_INSTRUMENTS_URL, timeout=60),
    )

    nse_data: list = json.loads(gzip.decompress(nse_r.content))
    bse_data: list = json.loads(gzip.decompress(bse_r.content))

    nse_stocks: dict[str, str] = {}
    for item in nse_data:
        if (item.get("instrument_type") == "EQ"
                and item.get("segment") == "NSE_EQ"
                and item.get("isin")
                and item.get("trading_symbol")
                and str(item.get("security_type", "")).upper() == "NORMAL"):
            sym  = item["trading_symbol"].strip().upper()
            isin = item["isin"].strip()
            nse_stocks[sym] = isin

    nse_isins = set(nse_stocks.values())

    BSE_EQUITY_TYPES = {"A", "B", "T", "XT", "X", "Z", "ZP", "R", "P"}

    bse_stocks: dict[str, str] = {}
    bse_meta:   dict[str, dict] = {}
    for item in bse_data:
        if (item.get("segment") == "BSE_EQ"
                and str(item.get("instrument_type", "")).upper() in BSE_EQUITY_TYPES
                and item.get("isin")
                and item.get("trading_symbol")):
            sym  = item["trading_symbol"].strip().upper()
            isin = item["isin"].strip()
            bse_stocks[sym] = isin
            bse_meta[isin]  = {
                "name"        : item.get("name", ""),
                "listing_date": item.get("listing_date", ""),
            }

    nse_map = dict(nse_stocks)
    bse_map = {
        sym: isin
        for sym, isin in bse_stocks.items()
        if isin not in nse_isins
    }

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
# UPSTOX FUNDAMENTALS API
# ══════════════════════════════════════════════════════════════

async def _upstox_get(client: httpx.AsyncClient, sem: asyncio.Semaphore, endpoint: str) -> dict | None:
    url = f"{FUND_URL}{endpoint}"
    await asyncio.sleep(SLEEP_MS)
    async with sem:
        for attempt in range(RETRY):
            try:
                r = await client.get(url, headers=UPSTOX_HEADERS, timeout=30)
            except httpx.RequestError as e:
                log.warning(f"  Fund network error: {e}, retry {attempt+1}")
                await asyncio.sleep(2 ** attempt)
                continue

            if r.status_code == 401:
                log.error("❌ TOKEN EXPIRED")
                sys.exit(1)
            if r.status_code == 429:
                if attempt < RETRY - 1:
                    log.warning("  Rate limited — waiting 30s")
                    await asyncio.sleep(30)
                    continue
                else:
                    log.warning("  Rate limited after all retries — skipping")
                    return None
            if r.status_code != 200:
                return None
            d = r.json()
            return d if d.get("status") == "success" else None
    return None


async def fetch_company_profile(client, sem, isin):
    d = await _upstox_get(client, sem, f"/{isin}/profile")
    if not d:
        return None
    data = d.get("data", {})
    return {
        "profile_sector"     : data.get("sector", ""),
        "profile_description": data.get("company_profile", ""),
    }


async def fetch_income_statement(client, sem, isin):
    for typ in ["consolidated", "standalone"]:
        d = await _upstox_get(client, sem, f"/{isin}/income-statement?type={typ}&time_period=quarterly&fs=true")
        if not d:
            continue
        stmt = d["data"].get("income_statement", [])
        full = d["data"].get("full_statement", [])
        rev  = next((s for s in stmt if s["category"] == "revenue"), None)
        np_  = next((s for s in stmt if s["category"] == "net_profit"), None)
        if not rev and not np_:
            continue

        src      = rev or np_
        n        = min(len(src["history"]), 4)
        quarters = [q["period"] for q in src["history"][:n]]

        def ex(arr, key="value"):
            return [(q.get(key) if q.get(key) is not None else "") for q in (arr or [])[:n]]

        def full_hist(name):
            return (next((s for s in full if s.get("particular") == name), {})
                    .get("history", []))

        pbt_ = next((s for s in stmt if s["category"] == "profit_before_tax"), None)
        if not pbt_:
            pbt_h = full_hist("Profit Before Tax")
            if pbt_h:
                pbt_ = {"history": pbt_h}

        eps_api = next((s for s in stmt
                        if s["category"] in ("eps", "earnings_per_share", "basic_eps")), None)

        if eps_api and eps_api.get("history"):
            eps_vals   = ex(eps_api["history"])
            eps_d_api  = next((s for s in stmt
                               if s["category"] in ("diluted_eps", "eps_diluted")), None)
            eps_d_vals = ex(eps_d_api["history"]) if eps_d_api else eps_vals[:]
        else:
            eps_names  = ["EPS - Basic", "Basic EPS", "Earnings Per Share", "EPS(Basic)"]
            eps_full_h = []
            for name in eps_names:
                eps_full_h = full_hist(name)
                if eps_full_h:
                    break

            shares = None
            if eps_full_h and np_ and np_.get("history"):
                estimates = []
                for i in range(min(4, len(eps_full_h), len(np_["history"]))):
                    pat_v = np_["history"][i].get("value")
                    eps_v = eps_full_h[i].get("value")
                    if pat_v not in (None, "") and eps_v not in (None, "", 0):
                        try:
                            est = float(pat_v) * 1e7 / float(eps_v)
                            if 1e6 < est < 1e10:
                                estimates.append(est)
                        except (TypeError, ValueError, ZeroDivisionError):
                            pass
                if estimates:
                    estimates.sort()
                    shares = estimates[len(estimates) // 2]
                    log.info(f"  [{isin}] Shares: {shares/1e7:.2f} Cr (median of {len(estimates)} quarters)")

            if not shares:
                for eps_name in ["EPS - Basic", "Basic EPS", "Earnings Per Share"]:
                    ann_eps_h = full_hist(eps_name)
                    ann_pat_h = full_hist("Profit After Tax")
                    if ann_eps_h and ann_pat_h:
                        try:
                            av = ann_eps_h[0].get("value")
                            pv = ann_pat_h[0].get("value")
                            if av not in (None, "", 0) and pv not in (None, ""):
                                est = float(pv) * 1e7 / float(av)
                                if 1e6 < est < 1e10:
                                    shares = est
                                    log.info(f"  [{isin}] Shares (annual fallback): {shares/1e7:.2f} Cr")
                                    break
                        except (TypeError, ValueError, ZeroDivisionError):
                            pass

            eps_vals = []
            pat_hist = np_["history"] if np_ else []
            for i in range(n):
                pv = pat_hist[i].get("value") if i < len(pat_hist) else None
                if pv not in (None, "") and shares:
                    try:
                        eps_vals.append(round(float(pv) * 1e7 / shares, 2))
                    except (TypeError, ValueError):
                        eps_vals.append("")
                else:
                    eps_vals.append("")
            eps_d_vals = eps_vals[:]

        pat_vals = ex(np_["history"] if np_ else [])

        return {
            "quarters": quarters,
            "sales":    ex(rev["history"] if rev else []),
            "sales_ch": ex(rev["history"] if rev else [], "change"),
            "pbt":      ex(pbt_["history"] if pbt_ else []),
            "pbt_ch":   ex(pbt_["history"] if pbt_ else [], "change"),
            "pat":      pat_vals,
            "pat_ch":   ex(np_["history"] if np_ else [], "change"),
            "eps":      eps_vals[:n],
            "eps_d":    eps_d_vals[:n],
        }
    return None


async def fetch_key_ratios(client, sem, isin):
    d = await _upstox_get(client, sem, f"/{isin}/key-ratios")
    if not d:
        return None
    ratios = d.get("data", [])
    def gn(name, field="company_value"):
        r = next((x for x in ratios if x["name"] == name), None)
        if not r or not r.get(field):
            return None
        try: return float(str(r[field]).replace("%", ""))
        except: return None
    return {
        "pe": gn("P/E"), "pb": gn("P/B"), "roe": gn("ROE"), "roce": gn("ROCE"),
        "roa": gn("ROA"), "ev_ebitda": gn("EV/EBITDA"),
        "sec_pe": gn("P/E","sector_value"), "sec_pb": gn("P/B","sector_value"),
        "sec_roe": gn("ROE","sector_value"), "sec_roce": gn("ROCE","sector_value"),
        "sec_roa": gn("ROA","sector_value"), "sec_ev_ebitda": gn("EV/EBITDA","sector_value"),
    }


async def fetch_shareholding(client, sem, isin):
    d = await _upstox_get(client, sem, f"/{isin}/share-holdings")
    if not d:
        return None
    holdings = d.get("data", [])
    if not holdings:
        return None
    def gh(cat): return next((h for h in holdings if h["category"] == cat), {}).get("history", [])
    def gv(arr, i): return arr[i]["value"] if i < len(arr) else None
    def gp(arr, i): return arr[i]["period"] if i < len(arr) else ""
    pH, fH, mH, dH, rH = gh("promoters"), gh("fii"), gh("mutual_funds"), gh("other_dii"), gh("retail_and_other")
    pr, prp = gv(pH,0), gv(pH,1)
    return {
        "promoter": pr, "fii": gv(fH,0), "mutual_fund": gv(mH,0),
        "dii": gv(dH,0), "public": gv(rH,0),
        "promoter_ch": round(pr - prp, 2) if pr is not None and prp is not None else None,
        "sh_quarter": gp(pH,0),
        "sh_qtrs": [gp(pH,i) for i in range(4) if gp(pH,i)],
        **{f"sh_promoter_q{i+1}": gv(pH,i) for i in range(4)},
        **{f"sh_fii_q{i+1}": gv(fH,i) for i in range(4)},
        **{f"sh_mf_q{i+1}": gv(mH,i) for i in range(4)},
        **{f"sh_dii_q{i+1}": gv(dH,i) for i in range(4)},
        **{f"sh_public_q{i+1}": gv(rH,i) for i in range(4)},
    }


async def fetch_balance_sheet(client, sem, isin):
    for typ in ["consolidated", "standalone"]:
        d = await _upstox_get(client, sem, f"/{isin}/balance-sheet?type={typ}&time_period=annual&fs=true")
        if not d:
            continue
        history = d["data"].get("history", [])
        if not history:
            continue
        fs = d["data"].get("full_statement", [])
        n = min(len(history), 4)
        def gfs(name): return [r["value"] for r in (next((x for x in fs if x.get("particular") == name), {}).get("history", []))[:n]]
        return {
            "bs_type": typ, "bs_units": d["data"].get("units_in","crore"),
            "bs_periods": [h["period"] for h in history[:n]],
            "bs_assets":  [h.get("total_asset") for h in history[:n]],
            "bs_liab":    [h.get("total_liability") for h in history[:n]],
            "bs_equity":  [round(h["total_asset"]-h["total_liability"],2) if h.get("total_asset") and h.get("total_liability") else None for h in history[:n]],
            "bs_cur_assets":    gfs("Current Assets"),
            "bs_noncur_assets": gfs("Non-Current Assets"),
            "bs_cur_liab":      gfs("Current Liabilities"),
            "bs_noncur_liab":   gfs("Non-Current Liabilities"),
        }
    return None


async def fetch_cash_flow(client, sem, isin):
    for typ in ["consolidated", "standalone"]:
        d = await _upstox_get(client, sem, f"/{isin}/cash-flow?type={typ}&time_period=annual")
        if not d:
            continue
        flows = d["data"].get("cash_flow", [])
        if not flows:
            continue
        def gf(cat): return next((f for f in flows if f["category"] == cat), None)
        op, inv, fin = gf("operating"), gf("investing"), gf("financing")
        src = op or inv or fin
        if not src or not src.get("history"):
            continue
        n = min(len(src["history"]), 4)
        def ev(x): return [(h.get("value") or "") for h in (x or {}).get("history",[])][:n]
        def ec(x): return [(h.get("change") or "") for h in (x or {}).get("history",[])][:n]
        opv, invv = ev(op), ev(inv)
        return {
            "cf_type": typ, "cf_units": d["data"].get("units_in","crore"),
            "cf_periods": [h["period"] for h in src["history"][:n]],
            "cf_op": opv, "cf_op_ch": ec(op),
            "cf_inv": invv, "cf_inv_ch": ec(inv),
            "cf_fin": ev(fin), "cf_fin_ch": ec(fin),
            "cf_fcf": [round(o+i,2) if o!="" and i!="" else "" for o,i in zip(opv,invv)],
        }
    return None


async def fetch_corporate_actions(client, sem, isin):
    d = await _upstox_get(client, sem, f"/{isin}/corporate-actions")
    if not d:
        return None
    actions = d.get("data", []) if isinstance(d.get("data"), list) else []
    if not actions:
        return None
    result = {"ca_count": len(actions)}
    for i, a in enumerate(actions[:5]):
        n = i + 1
        dets = a.get("event_details", []) if isinstance(a.get("event_details"), list) else []
        def gd(key): return next((x["value"] for x in dets if x["name"] == key), "")
        result.update({
            f"ca{n}_name": a.get("name",""), f"ca{n}_date": a.get("expiry_date",""),
            f"ca{n}_amount": a.get("amount",""), f"ca{n}_ratio": a.get("ratio",""),
            f"ca{n}_type": gd("Dividend type"), f"ca{n}_record_date": gd("Record date"),
        })
    ld = next((a for a in actions if a.get("name") == "Dividend"), None)
    if ld:
        result["latest_div_amount"] = ld.get("amount","")
        result["latest_div_date"]   = ld.get("expiry_date","")
    return result


async def fetch_one_fundamental(client, sem, sym, isin):
    income  = await fetch_income_statement(client, sem, isin)
    ratios  = await fetch_key_ratios(client, sem, isin)
    sh      = await fetch_shareholding(client, sem, isin)
    bs      = await fetch_balance_sheet(client, sem, isin)
    cf      = await fetch_cash_flow(client, sem, isin)
    ca      = await fetch_corporate_actions(client, sem, isin)
    profile = await fetch_company_profile(client, sem, isin)

    if not any([income, ratios, sh, bs, cf]):
        return sym, None

    obj = {"symbol": sym, "updated": today_ist()}

    if profile:
        obj["profile_sector"]      = profile.get("profile_sector", "")
        obj["profile_description"] = profile.get("profile_description", "")

    if ratios: obj.update(ratios)
    if sh: obj.update({k: sh[k] for k in sh})

    if income:
        quarters = income.get("quarters", [])
        for i, q in enumerate(quarters[:4]):
            n = i + 1
            obj[f"q{n}_period"]   = q
            obj[f"q{n}_sales"]    = income["sales"][i]    if i < len(income.get("sales",[]))    else ""
            obj[f"q{n}_sales_ch"] = income["sales_ch"][i] if i < len(income.get("sales_ch",[])) else ""
            obj[f"q{n}_pbt"]      = income["pbt"][i]      if i < len(income.get("pbt",[]))      else ""
            obj[f"q{n}_pbt_ch"]   = income["pbt_ch"][i]   if i < len(income.get("pbt_ch",[]))   else ""
            obj[f"q{n}_pat"]      = income["pat"][i]      if i < len(income.get("pat",[]))      else ""
            obj[f"q{n}_pat_ch"]   = income["pat_ch"][i]   if i < len(income.get("pat_ch",[]))   else ""
            obj[f"q{n}_eps"]      = income["eps"][i]      if i < len(income.get("eps",[]))      else ""
            obj[f"q{n}_eps_d"]    = income["eps_d"][i]    if i < len(income.get("eps_d",[]))    else ""

    if bs:
        obj["bs_type"]  = bs.get("bs_type", "")
        obj["bs_units"] = bs.get("bs_units", "crore")
        for i in range(4):
            n = i + 1
            obj[f"bs_y{n}_period"]        = bs["bs_periods"][i]       if i < len(bs.get("bs_periods",[]))       else ""
            obj[f"bs_y{n}_assets"]        = bs["bs_assets"][i]        if i < len(bs.get("bs_assets",[]))        else ""
            obj[f"bs_y{n}_liab"]          = bs["bs_liab"][i]          if i < len(bs.get("bs_liab",[]))          else ""
            obj[f"bs_y{n}_equity"]        = bs["bs_equity"][i]        if i < len(bs.get("bs_equity",[]))        else ""
            obj[f"bs_y{n}_cur_assets"]    = bs["bs_cur_assets"][i]    if i < len(bs.get("bs_cur_assets",[]))    else ""
            obj[f"bs_y{n}_noncur_assets"] = bs["bs_noncur_assets"][i] if i < len(bs.get("bs_noncur_assets",[])) else ""
            obj[f"bs_y{n}_cur_liab"]      = bs["bs_cur_liab"][i]      if i < len(bs.get("bs_cur_liab",[]))      else ""
            obj[f"bs_y{n}_noncur_liab"]   = bs["bs_noncur_liab"][i]   if i < len(bs.get("bs_noncur_liab",[]))   else ""

    if cf:
        obj["cf_type"]  = cf.get("cf_type", "")
        obj["cf_units"] = cf.get("cf_units", "crore")
        for i in range(4):
            n = i + 1
            obj[f"cf_y{n}_period"] = cf["cf_periods"][i] if i < len(cf.get("cf_periods",[])) else ""
            obj[f"cf_y{n}_op"]     = cf["cf_op"][i]      if i < len(cf.get("cf_op",[]))      else ""
            obj[f"cf_y{n}_op_ch"]  = cf["cf_op_ch"][i]   if i < len(cf.get("cf_op_ch",[]))   else ""
            obj[f"cf_y{n}_inv"]    = cf["cf_inv"][i]     if i < len(cf.get("cf_inv",[]))     else ""
            obj[f"cf_y{n}_fin"]    = cf["cf_fin"][i]     if i < len(cf.get("cf_fin",[]))     else ""
            obj[f"cf_y{n}_fcf"]    = cf["cf_fcf"][i]     if i < len(cf.get("cf_fcf",[]))     else ""

    if ca: obj.update(ca)
    return sym, obj


async def fetch_one_bse_profile(client, sem, sym, isin, meta: dict):
    """BSE-only stocks ke liye sirf profile fetch karo."""
    await asyncio.sleep(2.0)  # extra delay — BSE profile rate limit tight hai
    profile = await fetch_company_profile(client, sem, isin)
    obj = {
        "symbol"             : sym,
        "updated"            : today_ist(),
        "name"               : meta.get("name", ""),
        "listing_date"       : meta.get("listing_date", ""),
        "profile_sector"     : profile.get("profile_sector", "")      if profile else "",
        "profile_description": profile.get("profile_description", "") if profile else "",
    }
    return sym, obj


# ══════════════════════════════════════════════════════════════
# BSE RESULT FETCHER
# ══════════════════════════════════════════════════════════════

async def get_bse_result_symbols(client: httpx.AsyncClient) -> list[str]:
    today = today_ist()
    next_day = (date.fromisoformat(today) + timedelta(days=1)).isoformat()
    url = (f"https://api.bseindia.com/BseIndiaAPI/api/DownloadCSV1/w"
           f"?fromdate={today}&todate={next_day}&scripcode=")
    req_headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.bseindia.com/"}
    try:
        r = await client.get(url, headers=req_headers, timeout=30)
        if r.status_code != 200:
            log.warning(f"BSE API returned {r.status_code}")
            return []
    except Exception as e:
        log.warning(f"BSE API error: {e}")
        return []

    lines = r.text.strip().splitlines()
    if len(lines) < 2:
        log.info("BSE CSV empty — no results today")
        return []

    headers_row = [h.strip().strip('"') for h in lines[0].split(",")]
    log.info(f"BSE CSV headers: {headers_row}")
    date_idx = next((i for i,h in enumerate(headers_row) if "DATE" in h.upper() or "RESULT" in h.upper()), -1)
    name_idx = next((i for i,h in enumerate(headers_row) if "SECURITY" in h.upper() and "NAME" in h.upper()), -1)
    if name_idx == -1:
        name_idx = next((i for i,h in enumerate(headers_row) if "NAME" in h.upper()), -1)
    if date_idx == -1 or name_idx == -1:
        log.warning(f"BSE CSV column not found — headers: {headers_row}")
        return []

    isin_symbols = set(ISIN_MAP.keys())

    def normalize_date(val):
        import re
        val = val.strip()
        MONTHS = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
                  "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
        m = re.match(r'^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$', val)
        if m:
            mon = MONTHS.get(m.group(2)[:3].lower())
            if mon: return f"{m.group(3)}-{mon}-{m.group(1).zfill(2)}"
        m2 = re.match(r'^(\d{2})-(\d{2})-(\d{4})$', val)
        if m2: return f"{m2.group(3)}-{m2.group(2)}-{m2.group(1)}"
        if re.match(r'\d{4}-\d{2}-\d{2}', val): return val[:10]
        return val

    matched = []
    for line in lines[1:]:
        cols = [c.strip().strip('"') for c in line.split(",")]
        if len(cols) <= max(name_idx, date_idx):
            continue
        raw_date = cols[date_idx] if date_idx < len(cols) else ""
        if not raw_date:
            continue
        if normalize_date(raw_date) != today:
            continue
        bse_name = cols[name_idx].strip().upper() if name_idx < len(cols) else ""
        if bse_name in isin_symbols:
            matched.append(bse_name)
            continue
        for sym in isin_symbols:
            if sym in bse_name or bse_name.startswith(sym[:5]):
                matched.append(sym)
                break

    matched = list(set(matched))
    log.info(f"BSE results today ({today}): {len(matched)} stocks — {', '.join(matched) or 'none'}")
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


async def r2_download(client: httpx.AsyncClient, filename: str) -> dict | None:
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

        log.info(f"Fetching {len(ISIN_MAP)} NSE + {len(BSE_ISIN_MAP)} BSE-only stocks…")

        nse_tasks = [
            fetch_ohlc(client, sem, sym, ISIN_MAP[sym], prev, today, "NSE_EQ")
            for sym in ISIN_MAP
        ]
        bse_tasks = [
            fetch_ohlc(client, sem, sym, BSE_ISIN_MAP[sym], prev, today, "BSE_EQ")
            for sym in BSE_ISIN_MAP
        ]

        nse_results, bse_results = await asyncio.gather(
            asyncio.gather(*nse_tasks),
            asyncio.gather(*bse_tasks),
        )

        fetched = {sym: c for sym, c in [*nse_results, *bse_results] if c}
        total   = len(ISIN_MAP) + len(BSE_ISIN_MAP)
        log.info(f"✓ {len(fetched)} fetched  ✗ {total - len(fetched)} no data")

        log.info("Downloading master chunks…")
        all_data = await download_all_chunks(client)

        live   = set(ISIN_MAP) | set(BSE_ISIN_MAP)
        pruned = [s for s in list(all_data) if s not in live]
        for s in pruned:
            del all_data[s]
        if pruned:
            log.info(f"🗑  Pruned {len(pruned)} delisted stocks")

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

        log.info("Uploading…")
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

        log.info(f"Fetching {len(ISIN_MAP)} NSE + {len(BSE_ISIN_MAP)} BSE-only stocks (v3)…")

        nse_tasks = [
            fetch_today_candle(client, sem, sym, ISIN_MAP[sym], "NSE_EQ")
            for sym in ISIN_MAP
        ]
        bse_tasks = [
            fetch_today_candle(client, sem, sym, BSE_ISIN_MAP[sym], "BSE_EQ")
            for sym in BSE_ISIN_MAP
        ]

        nse_results, bse_results = await asyncio.gather(
            asyncio.gather(*nse_tasks),
            asyncio.gather(*bse_tasks),
        )

        fetched = {sym: c for sym, c in [*nse_results, *bse_results] if c}
        log.info(f"✓ {len(fetched)} have today's candle")

        log.info("Downloading master chunks…")
        all_data = await download_all_chunks(client)

        for sym, c in fetched.items():
            upsert_candle(all_data, sym, c)

        delta = {sym: c for sym, c in fetched.items() if c["d"] == today}
        log.info("Uploading…")
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
    failed: list[str] = []

    all_sym_map: dict[str, tuple[str, str]] = {
        sym: (isin, "NSE_EQ") for sym, isin in ISIN_MAP.items()
    }
    all_sym_map.update({
        sym: (isin, "BSE_EQ") for sym, isin in BSE_ISIN_MAP.items()
    })

    async with httpx.AsyncClient() as client:
        symbols = list(all_sym_map.keys())
        batch   = 50
        for i in range(0, len(symbols), batch):
            chunk_syms = symbols[i : i + batch]
            tasks = [
                fetch_ohlc(client, sem, sym, all_sym_map[sym][0], start, today, all_sym_map[sym][1])
                for sym in chunk_syms
            ]
            results = await asyncio.gather(*tasks)
            for sym, candles in results:
                if candles:
                    filtered = [c for c in candles if c["d"] >= cutoff]
                    if filtered:
                        all_data[sym] = build_stock_obj(filtered)
                else:
                    failed.append(sym)

            pct = min(i + batch, len(symbols))
            log.info(f"  {pct}/{len(symbols)}  OK:{len(all_data)}  Failed:{len(failed)}")

            # Checkpoint har 500 stocks pe
            if pct % 500 == 0:
                log.info(f"  💾 Checkpoint upload at {pct} stocks…")
                await upload_all_chunks(client, all_data, today)

        log.info(f"✓ {len(all_data)} loaded  ✗ {len(failed)} failed")
        if failed:
            (HERE / "failed_stocks.txt").write_text("\n".join(failed))
        apply_rolling_window(all_data, cutoff)
        log.info("Uploading to R2…")
        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_all.json", json.dumps({"updated": today, "stocks": all_data})),
        )
    log.info("━━━ Full load complete ━━━")


async def run_status() -> None:
    async with httpx.AsyncClient() as client:
        tasks   = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    print(f"\n{'File':<20} {'Stocks':>7}  {'From':>12}  {'To':>12}  {'Updated':>12}")
    print("─" * 70)
    total = 0
    for i, res in enumerate(results):
        fname = f"ohlc_{i+1}.json"
        if isinstance(res, Exception) or res is None:
            print(f"{fname:<20}  ERROR")
            continue
        stocks = res.get("stocks", {})
        if not stocks:
            print(f"{fname:<20}  (empty)")
            continue
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
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ Fundamentals Daily  {today} ━━━")
    sem = asyncio.Semaphore(FUND_CONCURRENCY)

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        symbols = await get_bse_result_symbols(client)
        if not symbols:
            log.info("No BSE results today — exiting")
            return

        await save_result_calendar(client, symbols, today)

        log.info("Downloading fundamentals.json…")
        fund_data = await r2_download_fund(client)

        log.info(f"Fetching fundamentals for {len(symbols)} NSE stocks…")
        tasks   = [fetch_one_fundamental(client, sem, sym, ISIN_MAP[sym]) for sym in symbols if sym in ISIN_MAP]
        results = await asyncio.gather(*tasks)

        ok = 0
        for sym, data in results:
            if data:
                fund_data[sym] = data
                ok += 1
                log.info(f"  ✓ {sym}")
            else:
                log.warning(f"  ✗ {sym}: no data")

        log.info(f"Updated {ok}/{len(symbols)} stocks")
        await r2_upload_fund(client, fund_data)
    log.info("━━━ Fundamentals Daily complete ━━━")


async def run_fund_weekly(part: int = 0) -> None:
    """NSE stocks ke liye full fundamentals — BSE profiles alag run_bse_profiles se."""
    today       = today_ist()
    TOTAL_PARTS = 10

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        nse_symbols = list(ISIN_MAP.keys())
        total       = len(nse_symbols)
        part_size   = (total + TOTAL_PARTS - 1) // TOTAL_PARTS

        if part == 0:
            start_idx, end_idx = 0, total
            label = "Full"
        else:
            start_idx = (part - 1) * part_size
            end_idx   = min(part * part_size, total)
            label     = f"Part {part}/{TOTAL_PARTS}"

        nse_chunk = nse_symbols[start_idx:end_idx]
        log.info(f"━━━ Fundamentals Weekly {label}  {today}  ({len(nse_chunk)} NSE stocks) ━━━")

        sem = asyncio.Semaphore(FUND_CONCURRENCY)

        log.info("Downloading existing fundamentals.json…")
        fund_data = await r2_download_fund(client)

        batch_size = 50
        ok = 0
        failed = 0
        for i in range(0, len(nse_chunk), batch_size):
            batch = nse_chunk[i : i + batch_size]
            tasks = [fetch_one_fundamental(client, sem, sym, ISIN_MAP[sym]) for sym in batch]
            results = await asyncio.gather(*tasks)
            for sym, data in results:
                if data:
                    fund_data[sym] = data
                    ok += 1
                else:
                    failed += 1
            pct = min(i + batch_size, len(nse_chunk))
            log.info(f"  NSE {pct}/{len(nse_chunk)}  ✓{ok}  ✗{failed}")
            if pct % 200 == 0 or pct == len(nse_chunk):
                log.info("  Checkpoint upload…")
                await r2_upload_fund(client, fund_data)

    log.info(f"━━━ Weekly {label} complete — ✓{ok} ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# BSE PROFILES — ALAG SLOW PIPELINE
# ══════════════════════════════════════════════════════════════

async def run_bse_profiles(part: int = 0) -> None:
    """
    BSE-only stocks ke liye sirf company profiles fetch karo.
    Alag pipeline — rate limit ke liye slow aur checkpoint wala.
    Parts mein chalao: bse_profiles_1 se bse_profiles_10 tak.
    """
    today       = today_ist()
    TOTAL_PARTS = 10

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        bse_syms  = list(BSE_ISIN_MAP.keys())
        total     = len(bse_syms)
        part_size = (total + TOTAL_PARTS - 1) // TOTAL_PARTS

        if part == 0:
            start_idx, end_idx = 0, total
            label = "Full"
        else:
            start_idx = (part - 1) * part_size
            end_idx   = min(part * part_size, total)
            label     = f"Part {part}/{TOTAL_PARTS}"

        chunk = bse_syms[start_idx:end_idx]
        log.info(f"━━━ BSE Profiles {label}  {today}  ({len(chunk)} stocks) ━━━")

        # Download existing — skip already fetched
        bse_raw  = await r2_download(client, "bse_profiles.json")
        bse_fund: dict = {}
        if isinstance(bse_raw, list):
            bse_fund = {d["symbol"]: d for d in bse_raw if d.get("symbol")}
        elif isinstance(bse_raw, dict):
            bse_fund = bse_raw

        # Sirf missing stocks fetch karo
        missing = [sym for sym in chunk if sym not in bse_fund]
        log.info(f"Already fetched: {len(chunk) - len(missing)}  Missing: {len(missing)}")

        if not missing:
            log.info("✅ All stocks already fetched — nothing to do")
            return

        sem        = asyncio.Semaphore(1)  # strictly 1 at a time
        batch_size = 20                    # small batches + checkpoint upload
        ok         = 0
        fail       = 0

        for i in range(0, len(missing), batch_size):
            batch   = missing[i : i + batch_size]
            tasks   = [
                fetch_one_bse_profile(
                    client, sem, sym, BSE_ISIN_MAP[sym],
                    BSE_META.get(BSE_ISIN_MAP[sym], {})
                )
                for sym in batch
            ]
            results = await asyncio.gather(*tasks)
            for sym, data in results:
                if data:
                    bse_fund[sym] = data
                    ok += 1
                else:
                    fail += 1

            pct = min(i + batch_size, len(missing))
            log.info(f"  {pct}/{len(missing)}  ✓{ok}  ✗{fail}")

            # Checkpoint har 20 stocks pe — agar fail bhi ho toh progress save hoga
            await r2_upload(client, "bse_profiles.json",
                            json.dumps(list(bse_fund.values())))
            log.info(f"  💾 Checkpoint saved ({len(bse_fund)} total)")

    log.info(f"━━━ BSE Profiles {label} complete — ✓{ok}  ✗{fail} ━━━")


# ══════════════════════════════════════════════════════════════
# EP FORMATION SCANNER
# ══════════════════════════════════════════════════════════════

def _check_liquidity(volumes, closes, n, min_turnover=3_00_00_000):
    lookback = min(50, n)
    if lookback < 20:
        return True
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
        dates, highs, lows, closes, volumes = (
            s["d"], s["h"], s["l"], s["c"], s["v"]
        )
        n = len(dates)
        if n < volume_lookback + 2:
            continue

        if not _check_liquidity(volumes, closes, n):
            continue

        scan_from = max(volume_lookback, n - max_ep_age_days)

        for i in range(scan_from, n):
            prev_high = highs[i - 1]
            today_low = lows[i]
            if today_low <= prev_high:
                continue
            gap_pct = (today_low - prev_high) / prev_high * 100
            if gap_pct < min_gap_pct:
                continue
            avg_vol = sum(volumes[i - volume_lookback:i]) / volume_lookback
            if avg_vol == 0:
                continue
            vol_x = volumes[i] / avg_vol
            if vol_x < volume_spike_x:
                continue

            gap_lower    = prev_high
            consol_count = 0
            ep_intact    = True

            for j in range(i + 1, min(i + max_consolidation + 1, n)):
                if closes[j] < gap_lower:
                    ep_intact = False
                    break
                consol_count += 1

            if not ep_intact:
                continue
            if consol_count >= max_consolidation:
                continue

            last_idx = min(i + consol_count, n - 1)
            ep_close = closes[i]
            ep_5d_idx = min(i + 5, n - 1)
            ep_5d_return = round((closes[ep_5d_idx] - ep_close) / ep_close * 100, 2) \
                           if ep_5d_idx > i else ""
            ep_return = round((closes[last_idx] - ep_close) / ep_close * 100, 2) \
                        if ep_close else 0.0

            ep_high = highs[i]
            never_broke_high = all(
                closes[j] <= ep_high
                for j in range(i + 1, last_idx + 1)
            )
            ep_type = "Consolidating below EP high" if never_broke_high else "EP Follow-through"

            signals.append({
                "symbol"          : sym,
                "ep_date"         : dates[i],
                "gap_lower"       : round(gap_lower, 2),
                "gap_pct"         : round(gap_pct, 2),
                "vol_spike_x"     : round(vol_x, 1),
                "ep_candle_high"  : round(highs[i], 2),
                "ep_candle_low"   : round(today_low, 2),
                "ep_candle_close" : round(ep_close, 2),
                "ep_return"       : ep_return,
                "ep_5d_return"    : ep_5d_return,
                "last_close"      : round(closes[last_idx], 2),
                "last_date"       : dates[last_idx],
                "consolidation"   : consol_count,
                "ep_type"         : ep_type,
            })

    seen: dict[str, dict] = {}
    for sig in signals:
        sym = sig["symbol"]
        if sym not in seen or sig["ep_date"] > seen[sym]["ep_date"]:
            seen[sym] = sig

    return list(seen.values())


# ══════════════════════════════════════════════════════════════
# POST-RESULT THRUST SCANNER
# ══════════════════════════════════════════════════════════════

def _detect_post_result_thrust(
    all_data: dict,
    result_calendar: dict,
    min_price_ch_pct: float  = 1.5,
    volume_spike_x: float    = 1.5,
    close_position_min: float = 0.5,
    volume_lookback: int     = 20,
    max_result_age_days: int = 30,
) -> list[dict]:
    today_str = today_ist()
    cutoff    = (date.fromisoformat(today_str) - timedelta(days=max_result_age_days)).isoformat()

    sym_to_result_date: dict[str, str] = {}
    for date_str, syms in result_calendar.items():
        if date_str < cutoff:
            continue
        for sym in syms:
            if sym not in sym_to_result_date or date_str > sym_to_result_date[sym]:
                sym_to_result_date[sym] = date_str

    signals = []

    for sym, result_date in sym_to_result_date.items():
        if sym not in all_data:
            continue

        s       = all_data[sym]
        dates   = s["d"]
        opens   = s["o"]
        highs   = s["h"]
        lows    = s["l"]
        closes  = s["c"]
        volumes = s["v"]
        n       = len(dates)

        if n < volume_lookback + 2:
            continue
        if result_date not in dates:
            continue

        ri = dates.index(result_date)
        ti = ri + 1

        if ti >= n:
            continue

        lookback = min(volume_lookback, ri)
        if lookback == 0:
            continue
        avg_vol = sum(volumes[ri - lookback : ri]) / lookback
        if avg_vol == 0:
            continue

        result_day_ch = (
            round((closes[ri] - closes[ri - 1]) / closes[ri - 1] * 100, 2)
            if ri > 0 and closes[ri - 1] else 0.0
        )
        result_day_vol_x = round(volumes[ri] / avg_vol, 1)

        if lows[ti] > highs[ri]:
            continue

        prev_close = closes[ri]
        if prev_close == 0:
            continue
        price_ch_pct = (closes[ti] - prev_close) / prev_close * 100
        if price_ch_pct < min_price_ch_pct:
            continue

        vol_x = volumes[ti] / avg_vol
        if vol_x < volume_spike_x:
            continue

        candle_range = highs[ti] - lows[ti]
        close_pos    = (
            (closes[ti] - lows[ti]) / candle_range
            if candle_range > 0 else 1.0
        )
        if close_pos < close_position_min:
            continue

        if abs(result_day_ch) < 1.5 and result_day_vol_x < 2.0:
            reaction_type = "AH Result → T+1 Primary"
        elif result_day_ch >= 1.5:
            reaction_type = "IH Result → T+1 Follow-through"
        else:
            reaction_type = "Mixed"

        signals.append({
            "symbol"           : sym,
            "result_date"      : result_date,
            "result_day_ch"    : result_day_ch,
            "result_day_vol_x" : result_day_vol_x,
            "t1_date"          : dates[ti],
            "t1_open"          : round(opens[ti], 2),
            "t1_high"          : round(highs[ti], 2),
            "t1_low"           : round(lows[ti], 2),
            "t1_close"         : round(closes[ti], 2),
            "price_ch_pct"     : round(price_ch_pct, 2),
            "vol_pct"          : f"+{round((vol_x - 1) * 100)}%",
            "close_position"   : round(close_pos * 100, 1),
            "reaction_type"    : reaction_type,
        })

    order = {"AH Result → T+1 Primary": 0, "IH Result → T+1 Follow-through": 1, "Mixed": 2}
    signals.sort(key=lambda x: (order.get(x["reaction_type"], 9), -x["price_ch_pct"]))
    return signals


def _calculate_rs(all_data: dict, history_days: int = 30) -> dict:
    all_syms  = list(all_data.keys())
    result    = {}
    day_scores = {}

    for sym, s in all_data.items():
        closes = s["c"]
        n      = len(closes)
        scores = []

        for day_offset in range(history_days, -1, -1):
            idx = n - 1 - day_offset
            if idx < 63:
                scores.append(None)
                continue

            def ret(lookback):
                prev_idx = idx - lookback
                if prev_idx < 0: return None
                prev = closes[prev_idx]
                if prev == 0: return None
                return (closes[idx] - prev) / prev * 100

            p63  = ret(63)
            p126 = ret(126)
            p189 = ret(189)
            p252 = ret(252)

            if p252 is not None and p189 is not None and p126 is not None and p63 is not None:
                composite = (p63 * 2 + p126 + p189 + p252) / 5
            elif p189 is not None and p126 is not None and p63 is not None:
                composite = (p63 * 2 + p126 + p189) / 4
            elif p126 is not None and p63 is not None:
                composite = (p63 * 2 + p126) / 3
            elif p63 is not None:
                composite = p63
            else:
                scores.append(None)
                continue
            scores.append(composite)

        day_scores[sym] = scores

    n_days = history_days + 1
    rs_history = {sym: [] for sym in all_syms}

    for d in range(n_days):
        day_composites = {
            sym: day_scores[sym][d]
            for sym in all_syms
            if day_scores[sym][d] is not None
        }
        if not day_composites:
            for sym in all_syms:
                rs_history[sym].append(None)
            continue

        sorted_syms = sorted(day_composites, key=lambda x: day_composites[x])
        total = len(sorted_syms)
        ranks = {sym: round((i + 1) / total * 99) for i, sym in enumerate(sorted_syms)}

        for sym in all_syms:
            rs_history[sym].append(ranks.get(sym))

    final_composites = {
        sym: day_scores[sym][-1]
        for sym in all_syms
        if day_scores[sym][-1] is not None
    }
    final_sorted   = sorted(final_composites, key=lambda x: final_composites[x])
    final_total    = len(final_sorted)
    final_rank_pos = {sym: i + 1 for i, sym in enumerate(final_sorted)}

    for sym in all_syms:
        hist = rs_history[sym]
        current_rs = next((v for v in reversed(hist) if v is not None), None)
        result[sym] = {
            "rs"      : current_rs,
            "rs_rank" : final_rank_pos.get(sym),
            "rs_total": final_total,
            "history" : hist,
        }

    return result


def _build_rs_history_json(all_data: dict, rs_data: dict) -> list:
    from datetime import date as dt

    sample_sym = max(all_data.keys(), key=lambda s: len(all_data[s].get("d", [])))
    dates       = all_data[sample_sym]["d"]
    n           = len(dates)
    history_len = len(next(iter(rs_data.values()))["history"])
    start_idx   = n - history_len

    def fmt_date(d_str):
        d = dt.fromisoformat(d_str)
        return d.strftime("%-d-%b-%y")

    date_labels = []
    for i in range(history_len):
        date_idx = start_idx + i
        if 0 <= date_idx < n:
            date_labels.append((i, fmt_date(dates[date_idx])))

    rows = []
    for sym, v in rs_data.items():
        row = {"Stock Name": sym}
        for i, label in date_labels:
            rs_val = v["history"][i]
            if rs_val is not None:
                row[label] = rs_val
        rows.append(row)

    return rows


def _calculate_mswing(all_data: dict, history_days: int = 60) -> dict:
    result = {}

    for sym, s in all_data.items():
        closes = s["c"]
        n      = len(closes)

        history = []
        for day_offset in range(history_days, -1, -1):
            idx = n - 1 - day_offset
            if idx < 51:
                history.append(None)
                continue
            try:
                ret_20 = (closes[idx] - closes[idx-20]) / closes[idx-20] * 100 / 20
                ret_50 = (closes[idx] - closes[idx-50]) / closes[idx-50] * 100 / 50
                history.append(round(ret_20 + ret_50, 4))
            except ZeroDivisionError:
                history.append(None)

        valid = [v for v in history[-9:] if v is not None]
        avg9  = round(sum(valid) / len(valid), 4) if valid else None

        result[sym] = {
            "mswing"        : history[-1] if history else None,
            "mswing_avg9"   : avg9,
            "mswing_history": history,
        }

    return result


def _build_mswing_json(all_data: dict, mswing_data: dict) -> list:
    from datetime import date as dt

    sample_sym = max(all_data.keys(), key=lambda s: len(all_data[s].get("d", [])))
    dates       = all_data[sample_sym]["d"]
    n           = len(dates)
    history_len = len(next(iter(mswing_data.values()))["mswing_history"])
    start_idx   = n - history_len

    def fmt_date(d_str):
        d = dt.fromisoformat(d_str)
        return d.strftime("%-d-%b-%y")

    date_labels = []
    for i in range(history_len):
        date_idx = start_idx + i
        if 0 <= date_idx < n:
            date_labels.append((i, fmt_date(dates[date_idx])))

    rows = []
    for sym, v in mswing_data.items():
        row = {"Stock Name": sym}
        for i, label in date_labels:
            val = v["mswing_history"][i]
            if val is not None:
                row[label] = val
        rows.append(row)

    return rows


async def run_ep_scan() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ EP + Post-Result Scan  {today} ━━━")

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        log.info("Downloading OHLC chunks, screener, fundamentals, result calendar…")
        ohlc_tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]

        ohlc_results, screener_raw, fund_raw, cal_raw = await asyncio.gather(
            asyncio.gather(*ohlc_tasks, return_exceptions=True),
            r2_download(client, "screener.json"),
            r2_download_fund(client),
            r2_download(client, "result_calendar.json"),
        )

        all_data: dict = {}
        for i, res in enumerate(ohlc_results):
            if isinstance(res, Exception):
                log.warning(f"  ohlc_{i+1}.json error: {res}")
            elif res and "stocks" in res:
                all_data.update(res["stocks"])
        log.info(f"Loaded {len(all_data)} stocks")

        screener: dict = {}
        if isinstance(screener_raw, list):
            for row in screener_raw:
                sym = row.get("Stocks", "").strip()
                if not sym:
                    continue
                try:
                    sc = float(row.get("SALES CH%", 0)) * 100
                    sales_ch = f"+{sc:.1f}%" if sc >= 0 else f"{sc:.1f}%"
                except:
                    sales_ch = ""
                try:
                    ec = float(row.get("EPS CHANGE", 0)) * 100
                    eps_ch = f"+{ec:.1f}%" if ec >= 0 else f"{ec:.1f}%"
                except:
                    eps_ch = ""
                pat_cols = ["NR7","WIB","DIB","MCP","W-MCP","HVQ","VD",
                            "PullBack","ATR Tightness","Volume footprint",
                            "Launchpad","HLR","BS","GAPUP","PP","HPBC",
                            "TL/HL BO","3WTC"]
                combined = set()
                for p in (row.get("Patterns","") or "").split("||"):
                    p = p.strip()
                    if p: combined.add(p)
                for col in pat_cols:
                    v = row.get(col, "")
                    if v and v not in ("", None, 0, "No"):
                        combined.add(v if isinstance(v, str) else col)
                screener[sym] = {
                    "sales_ch" : sales_ch,
                    "eps_ch"   : eps_ch,
                    "patterns" : "||".join(sorted(combined)),
                    "sector"   : row.get("SECTOR", ""),
                    "rs"       : row.get("RS Rating", ""),
                    "ltp"      : row.get("LTP", ""),
                }
        log.info(f"Screener loaded: {len(screener)} stocks")

        fund_lookup: dict = {}
        if isinstance(fund_raw, dict):
            fund_lookup = fund_raw
        elif isinstance(fund_raw, list):
            fund_lookup = {d["symbol"]: d for d in fund_raw if d.get("symbol")}
        log.info(f"Fundamentals loaded: {len(fund_lookup)} stocks")

        result_calendar: dict = cal_raw if isinstance(cal_raw, dict) else {}
        if result_calendar:
            total_cal_entries = sum(len(v) for v in result_calendar.values())
            log.info(f"Result calendar: {len(result_calendar)} days, {total_cal_entries} entries")
        else:
            log.warning("result_calendar.json not found or empty — Post-Result scan will be skipped")

        log.info("Scanning for EP formations…")
        signals = _detect_ep(all_data)
        signals.sort(key=lambda x: (x["ep_date"], x["gap_pct"]), reverse=True)

        _MNUM = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                 "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
        def _pq(q):
            try:
                p = q.strip().split()
                return (int(p[1]), _MNUM.get(p[0].lower()[:3], 0))
            except: return (0, 0)

        for sig in signals:
            sym = sig["symbol"]
            sc  = screener.get(sym, {})
            sig["sales_ch"] = sc.get("sales_ch", "")
            sig["eps_ch"]   = sc.get("eps_ch", "")
            sig["patterns"] = sc.get("patterns", "")
            sig["sector"]   = sc.get("sector", "")
            sig["rs"]       = sc.get("rs", "")
            sig["ltp"]      = sc.get("ltp", "")
            fund = fund_lookup.get(sym, {})
            q_name = ""
            _best = (0, 0)
            for _n in range(1, 5):
                _p = fund.get(f"q{_n}_period", "")
                if _p and _pq(_p) > _best:
                    _best = _pq(_p)
                    q_name = _p
            sig["q_name"] = q_name
            vol_x = sig.pop("vol_spike_x", 1)
            sig["vol_pct"] = f"+{round((vol_x - 1) * 100)}%"

        log.info(f"EP signals: {len(signals)}")

        pr_signals: list[dict] = []
        if result_calendar:
            log.info("Scanning for Post-Result T+1 thrusts…")
            pr_signals = _detect_post_result_thrust(all_data, result_calendar)

            for sig in pr_signals:
                sym  = sig["symbol"]
                sc   = screener.get(sym, {})
                fund = fund_lookup.get(sym, {})
                sig["sales_ch"] = sc.get("sales_ch", "")
                sig["eps_ch"]   = sc.get("eps_ch", "")
                sig["patterns"] = sc.get("patterns", "")
                sig["sector"]   = sc.get("sector", "")
                sig["rs"]       = sc.get("rs", "")
                sig["ltp"]      = sc.get("ltp", "")
                q_name = ""
                _best  = (0, 0)
                for _n in range(1, 5):
                    _p = fund.get(f"q{_n}_period", "")
                    if _p and _pq(_p) > _best:
                        _best = _pq(_p)
                        q_name = _p
                sig["q_name"] = q_name

            ah = sum(1 for s in pr_signals if "AH" in s["reaction_type"])
            ih = sum(1 for s in pr_signals if "IH" in s["reaction_type"])
            log.info(f"Post-Result signals: {len(pr_signals)}  (AH→T+1: {ah}  IH→T+1: {ih})")

        log.info("Calculating RS ratings…")
        rs_data = _calculate_rs(all_data, history_days=60)

        for sig in signals:
            sig["rs_calc"] = rs_data.get(sig["symbol"], {}).get("rs")
        for sig in pr_signals:
            sig["rs_calc"] = rs_data.get(sig["symbol"], {}).get("rs")

        rs_history_list = _build_rs_history_json(all_data, rs_data)

        log.info("Calculating MSwing…")
        mswing_data = _calculate_mswing(all_data, history_days=ROLLING_DAYS - 50)
        mswing_list = _build_mswing_json(all_data, mswing_data)

        for sig in signals:
            sym = sig["symbol"]
            sig["mswing"]      = mswing_data.get(sym, {}).get("mswing")
            sig["mswing_avg9"] = mswing_data.get(sym, {}).get("mswing_avg9")
        for sig in pr_signals:
            sym = sig["symbol"]
            sig["mswing"]      = mswing_data.get(sym, {}).get("mswing")
            sig["mswing_avg9"] = mswing_data.get(sym, {}).get("mswing_avg9")

        ep_payload  = json.dumps({"updated": today, "count": len(signals), "signals": signals})
        rs_payload  = json.dumps({"updated": today, "count": len(rs_data), "stocks": rs_data})
        rs_hist_pay = json.dumps(rs_history_list)
        mswing_pay  = json.dumps(mswing_list)
        pr_payload  = json.dumps({
            "updated"  : today,
            "count"    : len(pr_signals),
            "ah_count" : sum(1 for s in pr_signals if "AH" in s["reaction_type"]),
            "ih_count" : sum(1 for s in pr_signals if "IH" in s["reaction_type"]),
            "signals"  : pr_signals,
        })

        await asyncio.gather(
            r2_upload(client, "ep_signals.json",          ep_payload),
            r2_upload(client, "rs_ratings.json",          rs_payload),
            r2_upload(client, "rs_history.json",          rs_hist_pay),
            r2_upload(client, "mswing.json",              mswing_pay),
            r2_upload(client, "post_result_signals.json", pr_payload),
        )
        log.info(
            f"✅ Uploaded — EP:{len(signals)}  PostResult:{len(pr_signals)}  "
            f"RS+MSwing:{len(rs_data)} stocks"
        )
    log.info("━━━ EP + Post-Result Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# HORIZONTAL RESISTANCE (HLR) + PULLBACK SCANNER
# ══════════════════════════════════════════════════════════════

def _calc_ema(closes: list, period: int) -> list:
    """Simple EMA calculation."""
    if len(closes) < period:
        return [None] * len(closes)
    ema = [None] * len(closes)
    k = 2 / (period + 1)
    # seed with SMA
    ema[period - 1] = sum(closes[:period]) / period
    for i in range(period, len(closes)):
        ema[i] = closes[i] * k + ema[i - 1] * (1 - k)
    return ema


def _detect_pullback(
    all_data: dict,
    length_pull: int           = 4,
    min_swing_range_pct: float = 10.0,
    min_pullback_pct: float    = 5.0,
    ema_proximity_pct: float   = 1.0,
    max_candle_range_pct: float = 6.0,
) -> list[dict]:
    """
    Pullback scanner — Pine Script logic ka Python translation.

    Conditions:
    1. Swing High then Swing Low (uptrend sequence)
    2. Swing range >= min_swing_range_pct
    3. Pullback from swing high >= min_pullback_pct
    4. Price near EMA10 or EMA21 (within ema_proximity_pct) OR reversal from EMA
    5. EMA alignment: EMA21 > EMA50, EMA10 > EMA50, close > EMA21
    6. EMA50 rising (last 5 bars)
    7. Candle range < max_candle_range_pct
    8. MACD: macd line >= signal line
    """
    signals = []

    for sym, s in all_data.items():
        dates   = s["d"]
        highs   = s["h"]
        lows    = s["l"]
        closes  = s["c"]
        volumes = s["v"]
        n       = len(dates)

        if n < 60:
            continue
        if not _check_liquidity(volumes, closes, n):
            continue

        # ── EMAs ──────────────────────────────────────────────
        ema10 = _calc_ema(closes, 10)
        ema21 = _calc_ema(closes, 21)
        ema50 = _calc_ema(closes, 50)

        if any(v is None for v in [ema10[-1], ema21[-1], ema50[-1]]):
            continue

        # ── MACD ──────────────────────────────────────────────
        ema12 = _calc_ema(closes, 12)
        ema26 = _calc_ema(closes, 26)
        macd_line = [
            (ema12[i] - ema26[i]) if ema12[i] is not None and ema26[i] is not None else None
            for i in range(n)
        ]
        macd_valid = [v for v in macd_line if v is not None]
        if len(macd_valid) < 9:
            continue
        # signal line = EMA9 of macd_line (only valid values)
        macd_arr = [v if v is not None else 0.0 for v in macd_line]
        macd_signal = _calc_ema(macd_arr, 9)

        # ── Pivot High/Low detection ───────────────────────────
        # ta.pivothigh(length, length) — high is highest in window
        last_swing_high_price = None
        last_swing_high_bar   = None
        last_swing_low_price  = None
        last_swing_low_bar    = None

        for i in range(length_pull, n - length_pull):
            # Pivot High
            if all(highs[i] >= highs[i - k] for k in range(1, length_pull + 1)) and \
               all(highs[i] >= highs[i + k] for k in range(1, length_pull + 1)):
                last_swing_high_price = highs[i]
                last_swing_high_bar   = i

            # Pivot Low
            if all(lows[i] <= lows[i - k] for k in range(1, length_pull + 1)) and \
               all(lows[i] <= lows[i + k] for k in range(1, length_pull + 1)):
                last_swing_low_price = lows[i]
                last_swing_low_bar   = i

        if last_swing_high_price is None or last_swing_low_price is None:
            continue

        # ── Current bar (last candle) ──────────────────────────
        i = n - 1

        # Gate 1: Swing sequence — high bar must come AFTER low bar
        if last_swing_high_bar is None or last_swing_low_bar is None:
            continue
        if last_swing_high_bar <= last_swing_low_bar:
            continue

        # Gate 2: Swing range
        swing_range_pct = (last_swing_high_price - last_swing_low_price) / last_swing_low_price * 100
        if swing_range_pct < min_swing_range_pct:
            continue

        # Gate 3: Pullback from swing high
        pullback_pct = (last_swing_high_price - lows[i]) / last_swing_high_price * 100
        if pullback_pct < min_pullback_pct:
            continue

        # Gate 4: Near EMA10 or EMA21 OR reversal wick
        e10 = ema10[i]
        e21 = ema21[i]
        near_ema10 = (
            abs(lows[i]   - e10) / e10 * 100 <= ema_proximity_pct or
            abs(closes[i] - e10) / e10 * 100 <= ema_proximity_pct
        )
        near_ema21 = (
            abs(lows[i]   - e21) / e21 * 100 <= ema_proximity_pct or
            abs(closes[i] - e21) / e21 * 100 <= ema_proximity_pct
        )
        reversal_ema10 = lows[i] < e10 and closes[i] > e10
        reversal_ema21 = lows[i] < e21 and closes[i] > e21
        pullback_proximity = near_ema10 or near_ema21 or reversal_ema10 or reversal_ema21
        if not pullback_proximity:
            continue

        # Gate 5: EMA alignment
        e50 = ema50[i]
        ema_alignment = e21 > e50 and e10 > e50 and closes[i] > e21
        if not ema_alignment:
            continue

        # Gate 6: EMA50 rising last 5 bars
        if i < 5 or any(ema50[i - k] is None for k in range(6)):
            continue
        ema50_rising = all(ema50[i - k] > ema50[i - k - 1] for k in range(5))
        if not ema50_rising:
            continue

        # Gate 7: Candle range
        candle_range_pct = (highs[i] - lows[i]) / lows[i] * 100 if lows[i] > 0 else 0
        if candle_range_pct >= max_candle_range_pct:
            continue

        # Gate 8: MACD >= Signal
        if macd_line[i] is None or macd_signal[i] is None:
            continue
        if macd_line[i] < macd_signal[i]:
            continue

        # ── EMA proximity type ─────────────────────────────────
        if reversal_ema10 or reversal_ema21:
            ema_touch = "Reversal"
        elif near_ema10:
            ema_touch = "Near EMA10"
        else:
            ema_touch = "Near EMA21"

        signals.append({
            "symbol"           : sym,
            "date"             : dates[i],
            "close"            : round(closes[i], 2),
            "swing_high"       : round(last_swing_high_price, 2),
            "swing_low"        : round(last_swing_low_price, 2),
            "swing_range_pct"  : round(swing_range_pct, 2),
            "pullback_pct"     : round(pullback_pct, 2),
            "ema10"            : round(e10, 2),
            "ema21"            : round(e21, 2),
            "ema50"            : round(e50, 2),
            "ema_touch"        : ema_touch,
            "candle_range_pct" : round(candle_range_pct, 2),
            "macd"             : round(macd_line[i], 4),
            "macd_signal"      : round(macd_signal[i], 4),
        })

    return signals


def _detect_hlr(
    all_data: dict,
    swing_n: int        = 5,
    cluster_pct: float  = 2.0,
    near_pct: float     = 4.0,
    consol_days: int    = 5,
    consol_pct: float   = 4.0,
) -> list[dict]:

    signals = []

    for sym, s in all_data.items():
        dates   = s["d"]
        highs   = s["h"]
        lows    = s["l"]
        closes  = s["c"]
        volumes = s["v"]
        n       = len(dates)
        if n < swing_n * 2 + consol_days + 2:
            continue

        if not _check_liquidity(volumes, closes, n):
            continue

        vol_lookback = min(50, n - 1)
        avg_vol_50   = sum(volumes[-vol_lookback-1:-1]) / vol_lookback if vol_lookback > 0 else 0
        today_vol    = volumes[-1]
        if avg_vol_50 > 0 and vol_lookback >= 20:
            vol_spike = today_vol / avg_vol_50
        else:
            vol_spike = None

        swing_highs = []
        for i in range(swing_n, n - swing_n):
            if (all(highs[i] >= highs[i - k] for k in range(1, swing_n + 1)) and
                    all(highs[i] >= highs[i + k] for k in range(1, swing_n + 1))):
                sh_price = highs[i]
                broken_before = any(closes[j] > sh_price for j in range(i + 1, n - 1))
                broke_today   = closes[-1] > sh_price
                if broken_before:
                    continue
                if broke_today:
                    swing_highs.append((sh_price, dates[i], "BO"))
                else:
                    swing_highs.append((sh_price, dates[i], "valid"))

        if not swing_highs:
            continue

        swing_highs.sort(key=lambda x: x[0], reverse=True)
        used   = [False] * len(swing_highs)
        levels = []

        for i, (h, d, tag) in enumerate(swing_highs):
            if used[i]:
                continue
            cluster = [(h, d, tag)]
            for j in range(i + 1, len(swing_highs)):
                if not used[j] and abs(swing_highs[j][0] - h) / h * 100 <= cluster_pct:
                    cluster.append(swing_highs[j])
                    used[j] = True
            used[i] = True

            level       = max(c[0] for c in cluster)
            zone_low    = min(c[0] for c in cluster)
            is_zone     = len(cluster) >= 2
            touches     = len(cluster)
            cluster_tag = "BO" if any(c[2] == "BO" for c in cluster) else "valid"
            touch_pts   = sorted(
                [{"date": c[1], "price": round(c[0], 2)} for c in cluster],
                key=lambda x: x["date"]
            )
            levels.append((level, zone_low, touches, is_zone, touch_pts, cluster_tag))

        curr_close = closes[-1]
        curr_date  = dates[-1]

        if n >= consol_days:
            recent_high = max(highs[-consol_days:])
            recent_low  = min(lows[-consol_days:])
            range_pct   = (recent_high - recent_low) / curr_close * 100
            is_consol   = range_pct < consol_pct
        else:
            range_pct = 0
            is_consol = False

        for (level, zone_low, touches, is_zone, touch_pts, cluster_tag) in levels:
            dist_pct = (level - curr_close) / level * 100

            if cluster_tag == "BO":
                vol_ok = (vol_spike is None) or (vol_spike >= 2.0)
                state  = "BO" if vol_ok else "Near HLR"
            elif 0 <= dist_pct <= near_pct:
                state = "Consolidating near HLR" if is_consol else "Near HLR"
            else:
                continue

            signals.append({
                "symbol"      : sym,
                "state"       : state,
                "resistance"  : round(level, 2),
                "zone_low"    : round(zone_low, 2),
                "is_zone"     : is_zone,
                "touches"     : touches,
                "touch_points": touch_pts,
                "dist_pct"    : round(dist_pct, 2),
                "last_close"  : round(curr_close, 2),
                "last_date"   : curr_date,
                "consol_range": round(range_pct, 2),
                "vol_spike"   : round(vol_spike, 1) if vol_spike is not None else None,
            })

    return signals


async def run_hlr_scan() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ HLR + Pullback Scan  {today} ━━━")

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        log.info("Downloading OHLC chunks…")
        all_data = await download_all_chunks(client)
        log.info(f"Loaded {len(all_data)} stocks")

        # ── HLR Scan ──────────────────────────────────────────
        hlr_signals = _detect_hlr(all_data)
        order = {"BO": 0, "Consolidating near HLR": 1, "Near HLR": 2}
        hlr_signals.sort(key=lambda x: (order.get(x["state"], 9), -x["touches"]))

        bo     = sum(1 for s in hlr_signals if s["state"] == "BO")
        consol = sum(1 for s in hlr_signals if s["state"] == "Consolidating near HLR")
        near   = sum(1 for s in hlr_signals if s["state"] == "Near HLR")
        log.info(f"HLR — BO: {bo}  Consolidating: {consol}  Near: {near}  Total: {len(hlr_signals)}")

        # ── Pullback Scan ─────────────────────────────────────
        log.info("Scanning for Pullbacks…")
        pb_signals = _detect_pullback(all_data)
        pb_signals.sort(key=lambda x: x["pullback_pct"], reverse=True)
        log.info(f"Pullback signals: {len(pb_signals)}")

        hlr_payload = json.dumps({
            "updated"      : today,
            "count"        : len(hlr_signals),
            "bo"           : bo,
            "consolidating": consol,
            "near"         : near,
            "signals"      : hlr_signals,
        })
        pb_payload = json.dumps({
            "updated": today,
            "count"  : len(pb_signals),
            "signals": pb_signals,
        })

        await asyncio.gather(
            r2_upload(client, "hlr_signals.json",      hlr_payload),
            r2_upload(client, "pullback_signals.json", pb_payload),
        )
        log.info(f"✅ Uploaded — HLR:{len(hlr_signals)}  Pullback:{len(pb_signals)}")

    log.info("━━━ HLR + Pullback Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# PATTERN SCANNER
# ══════════════════════════════════════════════════════════════

def _build_weekly(dates, opens, highs, lows, closes, volumes):
    from datetime import date as dt
    weekly = {}
    for d, o, h, l, c, v in zip(dates, opens, highs, lows, closes, volumes):
        key = dt.fromisoformat(d).isocalendar()[:2]
        if key not in weekly:
            weekly[key] = {"o": o, "h": h, "l": l, "c": c, "v": v, "d": d}
        else:
            weekly[key]["h"] = max(weekly[key]["h"], h)
            weekly[key]["l"] = min(weekly[key]["l"], l)
            weekly[key]["c"] = c
            weekly[key]["v"] += v
    return weekly


def _detect_patterns(
    all_data: dict,
    min_volume: int        = 2500,
    coil_min_babies: int   = 3,
    tight_close_weeks: int = 3,
    tight_close_pct: float = 2.0,
) -> list[dict]:

    from datetime import date as dt
    signals = []

    for sym, s in all_data.items():
        dates   = s["d"]
        opens   = s["o"]
        highs   = s["h"]
        lows    = s["l"]
        closes  = s["c"]
        volumes = s["v"]
        n       = len(dates)

        if n < 10:
            continue
        if not _check_liquidity(volumes, closes, n):
            continue
        if volumes[-1] < min_volume:
            continue

        today = dates[-1]

        if highs[-1] <= highs[-2] and lows[-1] >= lows[-2]:
            signals.append({
                "symbol"   : sym, "pattern": "Inside Bar", "date": today,
                "high"     : round(highs[-1], 2), "low": round(lows[-1], 2),
                "prev_high": round(highs[-2], 2), "prev_low": round(lows[-2], 2),
            })

        if n >= 3:
            if (highs[-1] <= highs[-2] and lows[-1] >= lows[-2] and
                    highs[-2] <= highs[-3] and lows[-2] >= lows[-3]):
                signals.append({
                    "symbol"     : sym, "pattern": "Double Inside Bar", "date": today,
                    "high"       : round(highs[-1], 2), "low": round(lows[-1], 2),
                    "mother_high": round(highs[-3], 2), "mother_low": round(lows[-3], 2),
                })

        if n >= 7:
            today_range = highs[-1] - lows[-1]
            past_ranges = [highs[-i] - lows[-i] for i in range(2, 8)]
            if today_range <= min(past_ranges):
                signals.append({
                    "symbol": sym, "pattern": "NR7", "date": today,
                    "range" : round(today_range, 2),
                    "high"  : round(highs[-1], 2), "low": round(lows[-1], 2),
                })

        seen_mothers = set()
        for m_idx in range(n - coil_min_babies - 1, max(0, n - 60), -1):
            m_high = highs[m_idx]
            m_low  = lows[m_idx]
            m_key  = round(m_high * 200)
            if m_key in seen_mothers:
                continue

            baby_count = 0
            coil_state = "Coiling"
            for b in range(m_idx + 1, n):
                if highs[b] > m_high:
                    coil_state = "Upper BO"
                    break
                elif lows[b] < m_low:
                    coil_state = "Lower BD"
                    break
                else:
                    baby_count += 1

            if baby_count >= coil_min_babies and coil_state == "Coiling":
                seen_mothers.add(m_key)
                signals.append({
                    "symbol"     : sym,
                    "pattern"    : f"{baby_count} Bar MCP" if baby_count <= 6 else "Mini Coil",
                    "date"       : today,
                    "mcp_high"   : round(m_high, 2), "mcp_low": round(m_low, 2),
                    "baby_count" : baby_count, "coil_state": coil_state,
                    "mother_date": dates[m_idx],
                })

        weekly = _build_weekly(dates, opens, highs, lows, closes, volumes)
        if not weekly:
            continue

        current_week = dt.fromisoformat(today).isocalendar()[:2]
        past_weeks   = sorted(k for k in weekly if k < current_week)

        if len(past_weeks) < 2:
            continue

        lw  = weekly[past_weeks[-1]]
        lw2 = weekly[past_weeks[-2]]

        if lw["h"] <= lw2["h"] and lw["l"] >= lw2["l"]:
            signals.append({
                "symbol"     : sym, "pattern": "Weekly IB", "date": today,
                "w_high"     : round(lw["h"], 2), "w_low": round(lw["l"], 2),
                "w_close"    : round(lw["c"], 2),
                "prev_w_high": round(lw2["h"], 2), "prev_w_low": round(lw2["l"], 2),
            })

            if len(past_weeks) >= 3:
                lw3 = weekly[past_weeks[-3]]
                if lw2["h"] <= lw3["h"] and lw2["l"] >= lw3["l"]:
                    signals.append({
                        "symbol"       : sym, "pattern": "Weekly Double IB", "date": today,
                        "w_high"       : round(lw["h"], 2), "w_low": round(lw["l"], 2),
                        "mother_w_high": round(lw3["h"], 2), "mother_w_low": round(lw3["l"], 2),
                    })

        if len(past_weeks) >= 7:
            lw_range    = lw["h"] - lw["l"]
            past_ranges = [weekly[past_weeks[-i]]["h"] - weekly[past_weeks[-i]]["l"]
                           for i in range(2, 8)]
            if lw_range <= min(past_ranges):
                signals.append({
                    "symbol" : sym, "pattern": "Weekly NR7", "date": today,
                    "w_range": round(lw_range, 2),
                    "w_high" : round(lw["h"], 2), "w_low": round(lw["l"], 2),
                })

        if len(past_weeks) >= tight_close_weeks:
            last_n_closes = [weekly[past_weeks[-i]]["c"]
                             for i in range(1, tight_close_weeks + 1)]
            tc_range = (max(last_n_closes) - min(last_n_closes)) / min(last_n_closes) * 100
            if tc_range <= tight_close_pct:
                signals.append({
                    "symbol"   : sym, "pattern": "Weekly Tight Close", "date": today,
                    "closes"   : [round(c, 2) for c in last_n_closes],
                    "range_pct": round(tc_range, 2),
                })

    return signals


async def run_pattern_scan() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ Pattern Scan  {today} ━━━")

    async with httpx.AsyncClient() as client:
        global ISIN_MAP, BSE_ISIN_MAP, BSE_META
        ISIN_MAP, BSE_ISIN_MAP, BSE_META = await build_isin_map(client)

        log.info("Downloading OHLC chunks…")
        all_data = await download_all_chunks(client)
        log.info(f"Loaded {len(all_data)} stocks")

        signals = _detect_patterns(all_data)

        from collections import Counter
        counts = Counter(s["pattern"] for s in signals)
        for pat, cnt in sorted(counts.items()):
            log.info(f"  {pat}: {cnt}")
        log.info(f"Total: {len(signals)} signals")

        payload = json.dumps({
            "updated" : today,
            "count"   : len(signals),
            "summary" : dict(counts),
            "signals" : signals,
        })
        await r2_upload(client, "pattern_signals.json", payload)

    log.info("━━━ Pattern Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "daily":           asyncio.run(run_daily())
        case "today":           asyncio.run(run_today())
        case "full":            asyncio.run(run_full())
        case "status":          asyncio.run(run_status())
        case "fund_daily":      asyncio.run(run_fund_daily())
        case "fund_weekly":     asyncio.run(run_fund_weekly(0))
        case "fund_weekly_1":   asyncio.run(run_fund_weekly(1))
        case "fund_weekly_2":   asyncio.run(run_fund_weekly(2))
        case "fund_weekly_3":   asyncio.run(run_fund_weekly(3))
        case "fund_weekly_4":   asyncio.run(run_fund_weekly(4))
        case "fund_weekly_5":   asyncio.run(run_fund_weekly(5))
        case "fund_weekly_6":   asyncio.run(run_fund_weekly(6))
        case "fund_weekly_7":   asyncio.run(run_fund_weekly(7))
        case "fund_weekly_8":   asyncio.run(run_fund_weekly(8))
        case "fund_weekly_9":   asyncio.run(run_fund_weekly(9))
        case "fund_weekly_10":  asyncio.run(run_fund_weekly(10))
        case "bse_profiles":    asyncio.run(run_bse_profiles(0))
        case "bse_profiles_1":  asyncio.run(run_bse_profiles(1))
        case "bse_profiles_2":  asyncio.run(run_bse_profiles(2))
        case "bse_profiles_3":  asyncio.run(run_bse_profiles(3))
        case "bse_profiles_4":  asyncio.run(run_bse_profiles(4))
        case "bse_profiles_5":  asyncio.run(run_bse_profiles(5))
        case "bse_profiles_6":  asyncio.run(run_bse_profiles(6))
        case "bse_profiles_7":  asyncio.run(run_bse_profiles(7))
        case "bse_profiles_8":  asyncio.run(run_bse_profiles(8))
        case "bse_profiles_9":  asyncio.run(run_bse_profiles(9))
        case "bse_profiles_10": asyncio.run(run_bse_profiles(10))
        case "ep_scan":         asyncio.run(run_ep_scan())
        case "hlr_scan":        asyncio.run(run_hlr_scan())
        case "pattern_scan":    asyncio.run(run_pattern_scan())
        case _:
            print(__doc__)
            sys.exit(1)
