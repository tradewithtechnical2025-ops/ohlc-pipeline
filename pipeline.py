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

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Env ───────────────────────────────────────────────────────
UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

# ── Constants ─────────────────────────────────────────────────
BASE_URL     = "https://api.upstox.com/v2/historical-candle"
V3_URL       = "https://api.upstox.com/v3/historical-candle/intraday"
FUND_URL     = "https://api.upstox.com/v2/fundamentals"
ROLLING_DAYS = 548    # 1.5 years
R2_CHUNKS    = 8
CONCURRENCY  = 15     # parallel Upstox calls (stay under rate limit)
FUND_CONCURRENCY = 5  # lower — 6 API calls per stock
RETRY        = 3
SLEEP_MS     = 0.4    # rate limit guard for fundamentals

HERE = Path(__file__).parent

# ── Data files ────────────────────────────────────────────────
with open(HERE / "isin_map.json") as f:
    ISIN_MAP: dict[str, str] = json.load(f)       # { "RELIANCE": "INE002A01018", ... }

with open(HERE / "nse_holidays.json") as f:
    NSE_HOLIDAYS: set[str] = set(json.load(f))    # ["2025-01-26", "2025-08-15", ...]

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
# UPSTOX OHLC API  (async)
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
            await asyncio.sleep(3)
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
            try:
                r = await client.get(url, headers=UPSTOX_HEADERS, timeout=20)
            except httpx.RequestError:
                await asyncio.sleep(2 ** attempt)
                continue

        if r.status_code == 401:
            log.error("❌ TOKEN EXPIRED")
            sys.exit(1)
        if r.status_code == 429:
            await asyncio.sleep(3)
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
# UPSTOX FUNDAMENTALS API  (async)
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
                await asyncio.sleep(3)
                continue
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
    income, ratios, sh, bs, cf, ca = await asyncio.gather(
        fetch_income_statement(client, sem, isin),
        fetch_key_ratios(client, sem, isin),
        fetch_shareholding(client, sem, isin),
        fetch_balance_sheet(client, sem, isin),
        fetch_cash_flow(client, sem, isin),
        fetch_corporate_actions(client, sem, isin),
    )
    if not any([income, ratios, sh, bs, cf]):
        return sym, None

    obj = {"symbol": sym, "updated": today_ist()}
    if ratios: obj.update(ratios)
    if sh: obj.update({k: sh[k] for k in sh})
    if income: obj.update({k: income[k] for k in income})
    obj["_bs"] = bs or {}
    obj["_cf"] = cf or {}
    obj["_ca"] = ca or {}
    return sym, obj


# ── BSE result dates fetcher ───────────────────────────────────

async def get_bse_result_symbols(client: httpx.AsyncClient) -> list[str]:
    """Fetch today's BSE result symbols using BSE API."""
    today = today_ist()
    next30 = (date.fromisoformat(today) + timedelta(days=1)).isoformat()

    url = (f"https://api.bseindia.com/BseIndiaAPI/api/DownloadCSV1/w"
           f"?fromdate={today}&todate={next30}&scripcode=")
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bseindia.com/"
    }
    try:
        r = await client.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning(f"BSE API returned {r.status_code}")
            return []
    except Exception as e:
        log.warning(f"BSE API error: {e}")
        return []

    lines = r.text.strip().splitlines()
    if len(lines) < 2:
        return []

    headers_row = [h.strip() for h in lines[0].split(",")]
    try:
        scrip_idx = headers_row.index("SCRIP_CD")
        name_idx  = headers_row.index("SCRIP_NAME") if "SCRIP_NAME" in headers_row else -1
        date_idx  = headers_row.index("BOARD_MEETING_DATE") if "BOARD_MEETING_DATE" in headers_row else -1
    except ValueError:
        log.warning("BSE CSV column mismatch")
        return []

    matched = []
    for line in lines[1:]:
        cols = [c.strip() for c in line.split(",")]
        if len(cols) <= max(scrip_idx, date_idx):
            continue
        # Match by scrip name to ISIN_MAP symbol
        name = cols[name_idx].strip().upper() if name_idx >= 0 else ""
        if name in ISIN_MAP:
            matched.append(name)

    log.info(f"BSE results today ({today}): {len(matched)} stocks — {', '.join(matched) or 'none'}")
    return matched


# ── R2 fundamentals helpers ───────────────────────────────────

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
    # Handle both formats: {"updated":..., "stocks": {...}} or direct dict
    if isinstance(data, dict):
        return data.get("stocks", data)
    return {}  # fallback — start fresh


async def r2_upload_fund(client: httpx.AsyncClient, data: dict) -> None:
    payload = json.dumps({"updated": today_ist(), "stocks": data})
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


# ══════════════════════════════════════════════════════════════
# R2 / CLOUDFLARE WORKER  (OHLC)
# ══════════════════════════════════════════════════════════════

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
        payload = json.dumps(
            {"updated": today, "chunk": i + 1, "total": R2_CHUNKS, "stocks": chunk}
        )
        tasks.append(r2_upload(client, f"ohlc_{i+1}.json", payload))
    await asyncio.gather(*tasks)
    log.info(f"✓ {R2_CHUNKS} chunks uploaded ({n} stocks)")


# ══════════════════════════════════════════════════════════════
# DATA HELPERS (OHLC)
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
            r2_upload(client, "ohlc_delta.json",
                      json.dumps({"date": today, "stocks": delta})),
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
            r2_upload(client, "ohlc_delta.json",
                      json.dumps({"date": today, "stocks": delta})),
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
            r2_upload(client, "ohlc_all.json",
                      json.dumps({"updated": today, "stocks": all_data})),
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
    """
    Weekdays 4:30 PM IST — fetch BSE result stocks and update their fundamentals.
    Only updates stocks that have results today — fast run (~5 min).
    """
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ Fundamentals Daily  {today} ━━━")

    sem = asyncio.Semaphore(FUND_CONCURRENCY)

    async with httpx.AsyncClient() as client:
        # 1. Get today's BSE result symbols
        symbols = await get_bse_result_symbols(client)
        if not symbols:
            log.info("No BSE results today — exiting")
            return

        # 2. Download existing fundamentals
        log.info("Downloading fundamentals.json…")
        fund_data = await r2_download_fund(client)

        # 3. Fetch fundamentals for result stocks
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

        # 4. Upload
        log.info("Uploading fundamentals.json…")
        await r2_upload_fund(client, fund_data)

    log.info("━━━ Fundamentals Daily complete ━━━")


async def run_fund_weekly() -> None:
    """
    Sunday — full refresh of all 2300 stocks.
    Runs in batches to avoid rate limits. Takes ~20-30 min.
    """
    today   = today_ist()
    symbols = list(ISIN_MAP.keys())
    log.info(f"━━━ Fundamentals Weekly Full Refresh  {today}  ({len(symbols)} stocks) ━━━")

    sem = asyncio.Semaphore(FUND_CONCURRENCY)

    async with httpx.AsyncClient() as client:
        # Download existing data first
        log.info("Downloading existing fundamentals.json…")
        fund_data = await r2_download_fund(client)

        # Fetch all stocks in batches of 50
        batch_size = 50
        ok = 0
        failed = 0

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            tasks = [fetch_one_fundamental(client, sem, sym, ISIN_MAP[sym]) for sym in batch]
            results = await asyncio.gather(*tasks)

            for sym, data in results:
                if data:
                    fund_data[sym] = data
                    ok += 1
                else:
                    failed += 1

            pct = min(i + batch_size, len(symbols))
            log.info(f"  {pct}/{len(symbols)}  ✓{ok}  ✗{failed}")

            # Upload checkpoint every 500 stocks
            if pct % 500 == 0 or pct == len(symbols):
                log.info("  Checkpoint upload…")
                await r2_upload_fund(client, fund_data)

    log.info(f"━━━ Weekly complete — ✓{ok}  ✗{failed} ━━━")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "daily":        asyncio.run(run_daily())
        case "today":        asyncio.run(run_today())
        case "full":         asyncio.run(run_full())
        case "status":       asyncio.run(run_status())
        case "fund_daily":   asyncio.run(run_fund_daily())
        case "fund_weekly":  asyncio.run(run_fund_weekly())
        case _:
            print(__doc__)
            sys.exit(1)
