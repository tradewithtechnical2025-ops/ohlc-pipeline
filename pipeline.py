#!/usr/bin/env python3
"""
NSE OHLC + Fundamentals Pipeline — GitHub Actions
Replaces Google Apps Script.

Usage:
  python pipeline.py daily                # prev-day OHLC          (4:00 PM IST, weekdays)
  python pipeline.py today                # T+0 intraday candle     (4:50 PM IST, weekdays)
  python pipeline.py full                 # initial 1.5yr load      (manual, once)
  python pipeline.py status              # print R2 chunk summary
  python pipeline.py fund_daily          # BSE result stocks update (4:30 PM IST, weekdays)
  python pipeline.py fund_weekly         # full 2300 stocks refresh (Sunday)
  python pipeline.py ep_scan             # EP formation scan        (4:15 PM IST, weekdays)
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

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

BASE_URL     = "https://api.upstox.com/v2/historical-candle"
V3_URL       = "https://api.upstox.com/v3/historical-candle/intraday"
FUND_URL     = "https://api.upstox.com/v2/fundamentals"
ROLLING_DAYS = 548
R2_CHUNKS    = 8
CONCURRENCY  = 5      # parallel Upstox calls
FUND_CONCURRENCY = 1
RETRY        = 3
SLEEP_MS     = 3.0
RATE_DELAY   = 0.5    # 500ms between requests = ~200 req/min

HERE = Path(__file__).parent

with open(HERE / "isin_map.json") as f:
    ISIN_MAP: dict[str, str] = json.load(f)

with open(HERE / "nse_holidays.json") as f:
    NSE_HOLIDAYS: set[str] = set(json.load(f))

UPSTOX_HEADERS = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept": "application/json",
}
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}


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
) -> tuple[str, list | None]:
    key = quote(f"NSE_EQ|{isin}", safe="")
    url = f"{BASE_URL}/{key}/day/{to_date}/{from_date}"

    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)   # ← rate limit guard
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
) -> tuple[str, dict | None]:
    key = quote(f"NSE_EQ|{isin}", safe="")
    url = f"{V3_URL}/{key}/days/1"

    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)   # ← rate limit guard
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


async def fetch_income_statement(client, sem, isin):
    for typ in ["consolidated", "standalone"]:
        d = await _upstox_get(client, sem, f"/{isin}/income-statement?type={typ}&time_period=quarterly&fs=true")
        if not d:
            continue
        stmt = d["data"].get("income_statement", [])
        full = d["data"].get("full_statement", [])
        rev = next((s for s in stmt if s["category"] == "revenue"), None)
        op  = next((s for s in stmt if s["category"] == "operating_profit"), None)
        np_ = next((s for s in stmt if s["category"] == "net_profit"), None)
        if not rev and not np_:
            continue
        src = rev or np_
        n = min(len(src["history"]), 4)
        quarters = [q["period"] for q in src["history"][:n]]
        def ex(arr, key="value"): return [(q.get(key) if q.get(key) is not None else "") for q in (arr or [])[:n]]
        def exf(name): return [(q["value"] if q.get("value") is not None else "") for q in (next((s for s in full if s.get("particular") == name), {}).get("history", []))[:n]]
        return {
            "quarters": quarters,
            "sales": ex(rev["history"] if rev else []), "sales_ch": ex(rev["history"] if rev else [], "change"),
            "op": ex(op["history"] if op else []),   "op_ch":    ex(op["history"] if op else [], "change"),
            "pat": ex(np_["history"] if np_ else []), "pat_ch":   ex(np_["history"] if np_ else [], "change"),
            "eps": exf("EPS - Basic"), "eps_d": exf("EPS - Diluted"),
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
    income = await fetch_income_statement(client, sem, isin)
    ratios = await fetch_key_ratios(client, sem, isin)
    sh     = await fetch_shareholding(client, sem, isin)
    bs     = await fetch_balance_sheet(client, sem, isin)
    cf     = await fetch_cash_flow(client, sem, isin)
    ca     = await fetch_corporate_actions(client, sem, isin)
    if not any([income, ratios, sh, bs, cf]):
        return sym, None

    obj = {"symbol": sym, "updated": today_ist()}

    if ratios: obj.update(ratios)
    if sh: obj.update({k: sh[k] for k in sh})

    if income:
        quarters = income.get("quarters", [])
        for i, q in enumerate(quarters[:4]):
            n = i + 1
            obj[f"q{n}_period"]   = q
            obj[f"q{n}_sales"]    = income["sales"][i]    if i < len(income.get("sales",[]))    else ""
            obj[f"q{n}_sales_ch"] = income["sales_ch"][i] if i < len(income.get("sales_ch",[])) else ""
            obj[f"q{n}_op"]       = income["op"][i]       if i < len(income.get("op",[]))       else ""
            obj[f"q{n}_op_ch"]    = income["op_ch"][i]    if i < len(income.get("op_ch",[]))    else ""
            obj[f"q{n}_pat"]      = income["pat"][i]      if i < len(income.get("pat",[]))      else ""
            obj[f"q{n}_pat_ch"]   = income["pat_ch"][i]   if i < len(income.get("pat_ch",[]))   else ""
            obj[f"q{n}_eps"]      = income["eps"][i]      if i < len(income.get("eps",[]))      else ""
            obj[f"q{n}_eps_d"]    = income["eps_d"][i]    if i < len(income.get("eps_d",[]))    else ""

    if bs:
        obj["bs_type"]  = bs.get("bs_type", "")
        obj["bs_units"] = bs.get("bs_units", "crore")
        for i in range(4):
            n = i + 1
            obj[f"bs_y{n}_period"]        = bs["bs_periods"][i]        if i < len(bs.get("bs_periods",[]))        else ""
            obj[f"bs_y{n}_assets"]        = bs["bs_assets"][i]         if i < len(bs.get("bs_assets",[]))         else ""
            obj[f"bs_y{n}_liab"]          = bs["bs_liab"][i]           if i < len(bs.get("bs_liab",[]))           else ""
            obj[f"bs_y{n}_equity"]        = bs["bs_equity"][i]         if i < len(bs.get("bs_equity",[]))         else ""
            obj[f"bs_y{n}_cur_assets"]    = bs["bs_cur_assets"][i]     if i < len(bs.get("bs_cur_assets",[]))     else ""
            obj[f"bs_y{n}_noncur_assets"] = bs["bs_noncur_assets"][i]  if i < len(bs.get("bs_noncur_assets",[])) else ""
            obj[f"bs_y{n}_cur_liab"]      = bs["bs_cur_liab"][i]       if i < len(bs.get("bs_cur_liab",[]))       else ""
            obj[f"bs_y{n}_noncur_liab"]   = bs["bs_noncur_liab"][i]    if i < len(bs.get("bs_noncur_liab",[]))    else ""

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

    sem     = asyncio.Semaphore(CONCURRENCY)
    symbols = list(ISIN_MAP.keys())

    async with httpx.AsyncClient() as client:
        log.info(f"Fetching {len(symbols)} stocks (v2)…")
        tasks   = [fetch_ohlc(client, sem, sym, ISIN_MAP[sym], prev, today) for sym in symbols]
        results = await asyncio.gather(*tasks)

        fetched = {sym: c for sym, c in results if c}
        log.info(f"✓ {len(fetched)} fetched  ✗ {len(symbols)-len(fetched)} no data")

        log.info("Downloading master chunks…")
        all_data = await download_all_chunks(client)

        live   = set(ISIN_MAP)
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
    sem     = asyncio.Semaphore(CONCURRENCY)
    symbols = list(ISIN_MAP.keys())

    async with httpx.AsyncClient() as client:
        log.info(f"Fetching {len(symbols)} stocks (v3)…")
        tasks   = [fetch_today_candle(client, sem, sym, ISIN_MAP[sym]) for sym in symbols]
        results = await asyncio.gather(*tasks)

        fetched = {sym: c for sym, c in results if c}
        log.info(f"✓ {len(fetched)} have today's candle")

        log.info("Downloading master chunks…")
        all_data = await download_all_chunks(client)

        for sym, c in fetched.items():
            upsert_candle(all_data, sym, c)

        log.info("Uploading…")
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
    log.info(f"━━━ Full Load  {start} → {today}  ({len(ISIN_MAP)} stocks) ━━━")

    sem      = asyncio.Semaphore(CONCURRENCY)
    all_data: dict = {}
    failed: list[str] = []

    async with httpx.AsyncClient() as client:
        symbols = list(ISIN_MAP.keys())
        batch   = 50
        for i in range(0, len(symbols), batch):
            chunk_syms = symbols[i : i + batch]
            tasks   = [fetch_ohlc(client, sem, sym, ISIN_MAP[sym], start, today) for sym in chunk_syms]
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
        symbols = await get_bse_result_symbols(client)
        if not symbols:
            log.info("No BSE results today — exiting")
            return

        log.info("Downloading fundamentals.json…")
        fund_data = await r2_download_fund(client)

        log.info(f"Fetching fundamentals for {len(symbols)} stocks…")
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
        log.info("Uploading fundamentals.json…")
        await r2_upload_fund(client, fund_data)
    log.info("━━━ Fundamentals Daily complete ━━━")


async def run_fund_weekly(part: int = 0) -> None:
    today   = today_ist()
    symbols = list(ISIN_MAP.keys())
    total   = len(symbols)
    TOTAL_PARTS = 10
    part_size = (total + TOTAL_PARTS - 1) // TOTAL_PARTS
    if part == 0:
        start_idx, end_idx = 0, total
        label = "Full"
    else:
        start_idx = (part - 1) * part_size
        end_idx   = min(part * part_size, total)
        label     = f"Part {part}/{TOTAL_PARTS}"

    chunk = symbols[start_idx:end_idx]
    log.info(f"━━━ Fundamentals Weekly {label}  {today}  ({len(chunk)} stocks) ━━━")

    sem = asyncio.Semaphore(FUND_CONCURRENCY)
    async with httpx.AsyncClient() as client:
        log.info("Downloading existing fundamentals.json…")
        fund_data = await r2_download_fund(client)

        batch_size = 50
        ok = 0
        failed = 0
        for i in range(0, len(chunk), batch_size):
            batch = chunk[i : i + batch_size]
            tasks = [fetch_one_fundamental(client, sem, sym, ISIN_MAP[sym]) for sym in batch]
            results = await asyncio.gather(*tasks)
            for sym, data in results:
                if data:
                    fund_data[sym] = data
                    ok += 1
                else:
                    failed += 1
            pct = min(i + batch_size, len(chunk))
            log.info(f"  {pct}/{len(chunk)}  ✓{ok}  ✗{failed}")
            if pct % 200 == 0 or pct == len(chunk):
                log.info("  Checkpoint upload…")
                await r2_upload_fund(client, fund_data)

    log.info(f"━━━ Weekly {label} complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# EP FORMATION SCANNER
# ══════════════════════════════════════════════════════════════

def _detect_ep(
    all_data: dict,
    min_gap_pct: float     = 2.0,
    volume_spike_x: float  = 2.0,
    volume_lookback: int   = 20,
    max_consolidation: int = 20,
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
            if consol_count < 3:
                continue
            if consol_count >= max_consolidation:
                continue

            last_idx = min(i + consol_count, n - 1)
            signals.append({
                "symbol"         : sym,
                "ep_date"        : dates[i],
                "gap_lower"      : round(gap_lower, 2),
                "gap_pct"        : round(gap_pct, 2),
                "vol_spike_x"    : round(vol_x, 1),
                "ep_candle_high" : round(highs[i], 2),
                "ep_candle_low"  : round(today_low, 2),
                "last_close"     : round(closes[last_idx], 2),
                "last_date"      : dates[last_idx],
                "consolidation"  : consol_count,
                "ep_type"        : "Delayed EP",
            })

    seen: dict[str, dict] = {}
    for sig in signals:
        sym = sig["symbol"]
        if sym not in seen or sig["ep_date"] > seen[sym]["ep_date"]:
            seen[sym] = sig

    return list(seen.values())


async def run_ep_scan() -> None:
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ EP Scan  {today} ━━━")

    async with httpx.AsyncClient() as client:
        log.info("Downloading OHLC chunks + screener + fundamentals…")
        ohlc_tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
        ohlc_results, screener_raw, fund_raw = await asyncio.gather(
            asyncio.gather(*ohlc_tasks, return_exceptions=True),
            r2_download(client, "screener.json"),
            r2_download_fund(client),
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
                screener[sym] = {"sales_ch": sales_ch, "eps_ch": eps_ch}
        log.info(f"Screener loaded: {len(screener)} stocks")

        fund_lookup: dict = {}
        if isinstance(fund_raw, dict):
            fund_lookup = fund_raw
        elif isinstance(fund_raw, list):
            fund_lookup = {d["symbol"]: d for d in fund_raw if d.get("symbol")}
        log.info(f"Fundamentals loaded: {len(fund_lookup)} stocks")

        log.info("Scanning for EP formations…")
        signals = _detect_ep(all_data)
        signals.sort(key=lambda x: (x["ep_date"], x["gap_pct"]), reverse=True)

        for sig in signals:
            sym = sig["symbol"]
            sc = screener.get(sym, {})
            sig["sales_ch"] = sc.get("sales_ch", "")
            sig["eps_ch"]   = sc.get("eps_ch", "")
            fund = fund_lookup.get(sym, {})
            sig["q_name"] = fund.get("q1_period", "")
            vol_x = sig.pop("vol_spike_x", 1)
            sig["vol_pct"] = f"+{round((vol_x - 1) * 100)}%"

        count = len(signals)
        log.info(f"Found {count} Delayed EP signals")

        payload = json.dumps({"updated": today, "count": count, "signals": signals})
        await r2_upload(client, "ep_signals.json", payload)
    log.info("━━━ EP Scan complete ━━━")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "daily":          asyncio.run(run_daily())
        case "today":          asyncio.run(run_today())
        case "full":           asyncio.run(run_full())
        case "status":         asyncio.run(run_status())
        case "fund_daily":     asyncio.run(run_fund_daily())
        case "fund_weekly":    asyncio.run(run_fund_weekly(0))
        case "fund_weekly_1":  asyncio.run(run_fund_weekly(1))
        case "fund_weekly_2":  asyncio.run(run_fund_weekly(2))
        case "fund_weekly_3":  asyncio.run(run_fund_weekly(3))
        case "fund_weekly_4":  asyncio.run(run_fund_weekly(4))
        case "fund_weekly_5":  asyncio.run(run_fund_weekly(5))
        case "fund_weekly_6":  asyncio.run(run_fund_weekly(6))
        case "fund_weekly_7":  asyncio.run(run_fund_weekly(7))
        case "fund_weekly_8":  asyncio.run(run_fund_weekly(8))
        case "fund_weekly_9":  asyncio.run(run_fund_weekly(9))
        case "fund_weekly_10": asyncio.run(run_fund_weekly(10))
        case "ep_scan":        asyncio.run(run_ep_scan())
        case _:
            print(__doc__)
            sys.exit(1)
