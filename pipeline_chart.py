#!/usr/bin/env python3
"""
pipeline_chart.py — chart-only OHLC data (separate from pipeline.py)

Builds one file per stock inside the R2 folder chart/ with up to 5 years of
daily candles, for the charts only. It never reads or writes the ohlc_N.json
chunks, so pipeline.py and every scan keep working exactly as before.

R2 layout:
  chart/_index.json     list of all stocks: file name, exchange, first/last date, bse_until
  chart/RELIANCE.json   {"sym","ex","updated","bse_until"?, "d":[],"o":[],"h":[],"l":[],"c":[],"v":[]}
  chart/M_M.json        (symbols are made URL-safe: M&M -> M_M)

NSE + BSE:
  Some companies traded on BSE for years and listed on NSE later (even last
  month). Their NSE series starts at the NSE listing date. For such a stock the
  script also fetches the BSE series (same ISIN) and adds the BSE candles from
  BEFORE the first NSE candle. It only does this when BSE and NSE closes agree
  on the first NSE days (within 3%), so a wrong series is never joined.
  "bse_until" in the file = first NSE date; candles before it are BSE.

Usage:
  python pipeline_chart.py backfill            # first time: build every stock (resumable)
  python pipeline_chart.py daily               # every day: add new candles, build new listings
  python pipeline_chart.py rebuild SYM1,SYM2   # rebuild selected stocks from scratch
  python pipeline_chart.py backfill_force      # rebuild every stock from scratch
  python pipeline_chart.py status              # print a summary of chart/_index.json

Env: UPSTOX_TOKEN, WORKER_URL, WORKER_TOKEN   (same secrets as pipeline.py)
"""

import asyncio
import gzip
import json
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

# ── Telegram notify (optional, same helper pipeline.py uses) ──
try:
    from telegram_notify import PipelineStatus
except ImportError:
    class PipelineStatus:
        def __init__(self, name): self.name = name
        def add(self, *a, **k): pass
        def set(self, *a, **k): pass
        def warn(self, msg): pass
        def success(self, *a, **k): pass
        def failure(self, exc, reraise=True, **k):
            if reraise: raise exc

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("pipeline_chart")
logging.getLogger("httpx").setLevel(logging.WARNING)

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

UPSTOX_BASE = "https://api.upstox.com/v2"
NSE_BOD_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
BSE_BOD_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"

FOLDER              = os.environ.get("CHART_FOLDER", "chart/")   # R2 folder for chart files
INDEX_FILE          = FOLDER + "_index.json"
CHART_YEARS         = 5
UPSTOX_CONCURRENCY  = 5        # same safe value pipeline.py uses
RATE_DELAY          = 0.4
RETRY               = 5
R2_CONCURRENCY      = 12       # parallel chart-file downloads/uploads
BATCH               = 50       # stocks per batch
CHECKPOINT_EVERY    = 200      # save _index.json every N built stocks

BSE_SPLICE_MIN_GAP_DAYS = 30   # try BSE only if NSE history starts this long after the 5y floor
BSE_SPLICE_OVERLAP_DAYS = 20   # calendar days after the first NSE candle used to compare closes
BSE_SPLICE_TOL          = 0.03 # median BSE/NSE close ratio must be within 3%

KEYS = ("d", "o", "h", "l", "c", "v")


# ══════════════════════════════════════════════════════════════
# DATES
# ══════════════════════════════════════════════════════════════

def today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()

def years_floor(today: str) -> str:
    t = date.fromisoformat(today)
    try: return t.replace(year=t.year - CHART_YEARS).isoformat()
    except ValueError: return t.replace(year=t.year - CHART_YEARS, day=28).isoformat()  # 29 Feb

def add_days(d: str, n: int) -> str:
    return (date.fromisoformat(d) + timedelta(days=n)).isoformat()

def file_key(sym: str) -> str:
    """URL-safe file name (M&M -> M_M). The website must use the same rule."""
    return re.sub(r"[^A-Z0-9_-]", "_", str(sym).upper())


# ══════════════════════════════════════════════════════════════
# R2 (through the same Worker pipeline.py uses)
# ══════════════════════════════════════════════════════════════

async def r2_get(client, name):
    url = f"{WORKER_URL}/{name}"; last = None
    for attempt in range(RETRY):
        try:
            r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
        except httpx.RequestError as e:
            last = e; await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 404: return None
        if r.status_code == 200:
            try: return r.json()
            except Exception: return None
        last = RuntimeError(f"GET {name}: HTTP {r.status_code}")
        await asyncio.sleep(2 ** attempt)
    raise last

async def r2_put(client, name, obj, quiet=True):
    data = json.dumps(obj, separators=(",", ":")).encode()
    url = f"{WORKER_URL}?file={name}"; last = None
    for attempt in range(RETRY):
        try:
            r = await client.post(url, headers={**WORKER_HEADERS, "Content-Type": "application/json"},
                                  content=data, timeout=90)
        except httpx.RequestError as e:
            last = e; await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 200:
            if not quiet: log.info(f"  ↑ {name} ({len(data)/1024:.1f} KB)")
            return
        last = RuntimeError(f"PUT {name}: HTTP {r.status_code}")
        await asyncio.sleep(2 ** attempt)
    raise last


# ══════════════════════════════════════════════════════════════
# UNIVERSE  (classification.json + Upstox BOD instrument files, read-only)
# ══════════════════════════════════════════════════════════════

def _parse_bod(instruments, segment) -> dict:
    suffixes = ("-EQ","-BE","-BL","-SM","-IL","-IV","-W1","-W2","-W3","-W4","-W5")
    out = {}
    for inst in instruments:
        if inst.get("segment") != segment: continue
        if segment == "NSE_EQ" and inst.get("instrument_type", "") in ("SG","GB","TB","GS","CE","PE","FF","MF"): continue
        tsym = (inst.get("trading_symbol") or "").upper(); ikey = inst.get("instrument_key")
        if not tsym or not ikey: continue
        out[tsym] = ikey
        for sfx in suffixes:
            if tsym.endswith(sfx):
                base = tsym[:-len(sfx)]
                if base and base not in out: out[base] = ikey
                break
    return out

async def _load_bod(client, url, segment) -> dict:
    for attempt in range(RETRY):
        try:
            r = await client.get(url, headers={"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_TOKEN}"},
                                 timeout=60, follow_redirects=True)
        except httpx.RequestError:
            await asyncio.sleep(2 ** attempt); continue
        if r.status_code != 200:
            await asyncio.sleep(2 ** attempt); continue
        try:
            m = _parse_bod(json.loads(gzip.decompress(r.content)), segment)
            log.info(f"  BOD {segment}: {len(m)} instruments"); return m
        except Exception as e:
            log.warning(f"  BOD {segment} parse error: {e}"); break
    return {}

async def load_universe(client):
    """-> (universe {sym: {"ikey","ex"}}, set of every known BSE_EQ instrument key)"""
    master = await r2_get(client, "classification.json")
    if not isinstance(master, list): raise RuntimeError("classification.json missing or invalid in R2")
    nse_bod, bse_bod = await asyncio.gather(_load_bod(client, NSE_BOD_URL, "NSE_EQ"),
                                            _load_bod(client, BSE_BOD_URL, "BSE_EQ"))
    if not nse_bod or not bse_bod:   # fall back to the map pipeline.py caches (read only)
        cached = await r2_get(client, "ikey_map.json") or {}
        nse_bod = nse_bod or cached.get("nse", {}); bse_bod = bse_bod or cached.get("bse", {})
        log.info("  Using cached ikey_map.json for missing BOD data")
    uni = {}
    for st in master:
        sym = str(st.get("symbol", "")).strip().upper(); ex = str(st.get("exchange", "")).strip()
        if not sym: continue
        if ex == "NSE":
            k = nse_bod.get(sym)
            if k: uni[sym] = {"ikey": k, "ex": "NSE"}
        elif ex == "BSE":
            tsym = str(st.get("trading_symbol") or "").strip().upper()
            k = bse_bod.get(sym) or (bse_bod.get(tsym) if tsym else None)
            if k: uni[sym] = {"ikey": k, "ex": "BSE"}
    log.info(f"Universe: {len(uni)} stocks ({sum(1 for v in uni.values() if v['ex']=='NSE')} NSE, "
             f"{sum(1 for v in uni.values() if v['ex']=='BSE')} BSE)")
    return uni, set(bse_bod.values())


# ══════════════════════════════════════════════════════════════
# UPSTOX HISTORICAL CANDLES
# ══════════════════════════════════════════════════════════════

async def fetch_candles(client, sem, sym, ikey, from_date, to_date) -> list[dict]:
    """Daily candles from_date..to_date (both inclusive), oldest first. One call per calendar year."""
    if from_date > to_date: return []
    ranges = []; cur = date.fromisoformat(from_date); end = date.fromisoformat(to_date)
    while cur <= end:
        ranges.append((cur.isoformat(), min(date(cur.year, 12, 31), end).isoformat()))
        cur = date(cur.year + 1, 1, 1)
    out = {}
    for f, t in ranges:
        url = f"{UPSTOX_BASE}/historical-candle/{ikey}/day/{t}/{f}"
        for attempt in range(RETRY):
            async with sem:
                await asyncio.sleep(RATE_DELAY)
                try:
                    r = await client.get(url, headers={"Accept": "application/json",
                                                       "Authorization": f"Bearer {UPSTOX_TOKEN}"}, timeout=30)
                except httpx.RequestError as e:
                    log.warning(f"{sym}: network error ({e}), retry {attempt+1}")
                    await asyncio.sleep(2 ** attempt); continue
            if r.status_code == 401: log.error("❌ UPSTOX_TOKEN invalid"); sys.exit(1)
            if r.status_code == 429:
                await asyncio.sleep(30 * (attempt + 1)); continue
            if r.status_code in (502, 503, 504): await asyncio.sleep(2 ** attempt); continue
            if r.status_code != 200: break
            try: rows = (r.json().get("data") or {}).get("candles") or []
            except Exception: break
            for row in rows:
                d = str(row[0])[:10]
                if f <= d <= t and row[4] is not None:
                    out[d] = {"d": d, "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5] or 0}
            break
    return [out[d] for d in sorted(out)]


# ══════════════════════════════════════════════════════════════
# BUILD / UPDATE ONE STOCK
# ══════════════════════════════════════════════════════════════

def to_file(sym, ex, candles, today, bse_until=None) -> dict:
    obj = {"sym": sym, "ex": ex, "updated": today}
    if bse_until: obj["bse_until"] = bse_until
    for k in KEYS: obj[k] = [c[k] for c in candles]
    return obj

def from_file(obj) -> list[dict]:
    d = obj.get("d") or []
    return [{k: obj[k][i] for k in KEYS} for i in range(len(d))]

def splice_bse(nse: list[dict], bse: list[dict]):
    """-> (BSE candles before the first NSE candle, reason). Empty list = do not join."""
    first = nse[0]["d"]; end = add_days(first, BSE_SPLICE_OVERLAP_DAYS)
    nse_c = {c["d"]: c["c"] for c in nse if c["d"] <= end}
    ratios = sorted(b["c"] / nse_c[b["d"]] for b in bse if nse_c.get(b["d"]) and b.get("c"))
    if not ratios: return [], "no common days to compare"
    med = ratios[len(ratios) // 2]
    if abs(med - 1) > BSE_SPLICE_TOL: return [], f"price mismatch (BSE/NSE x{med:.3f})"
    pre = [b for b in bse if b["d"] < first]
    return pre, f"{len(pre)} BSE candles before {first}"

async def build_stock(client, sem, sym, info, bse_keys, today):
    """Full 5-year build for one stock. -> (file obj, index entry) or (None, None)."""
    floor = years_floor(today)
    candles = await fetch_candles(client, sem, sym, info["ikey"], floor, today)
    bse_until = None
    if info["ex"] == "NSE" and info["ikey"].startswith("NSE_EQ|"):
        short = (not candles) or candles[0]["d"] > add_days(floor, BSE_SPLICE_MIN_GAP_DAYS)
        bse_key = "BSE_EQ|" + info["ikey"].split("|", 1)[1]
        if short and candles and bse_key in bse_keys:
            to = min(add_days(candles[0]["d"], BSE_SPLICE_OVERLAP_DAYS), today)
            bse = await fetch_candles(client, sem, sym, bse_key, floor, to)
            if bse:
                pre, why = splice_bse(candles, bse)
                if pre:
                    bse_until = candles[0]["d"]; candles = pre + candles
                    log.info(f"  {sym}: joined {why}")
                elif not why.startswith("0 "):
                    log.info(f"  {sym}: BSE not joined ({why})")
    if not candles: return None, None
    obj = to_file(sym, info["ex"], candles, today, bse_until)
    return obj, index_entry(obj)

def index_entry(obj) -> dict:
    e = {"file": file_key(obj["sym"]) + ".json", "ex": obj["ex"],
         "first": obj["d"][0], "last": obj["d"][-1], "bars": len(obj["d"])}
    if obj.get("bse_until"): e["bse_until"] = obj["bse_until"]
    return e

async def update_stock(client, sem, r2sem, sym, info, entry, today):
    """Daily: refetch from the stored last date (refreshes it) to today and append.
    -> new index entry, None when nothing changed, or "rebuild" if the file is missing."""
    new = await fetch_candles(client, sem, sym, info["ikey"], entry["last"], today)
    if not new: return None
    async with r2sem:
        obj = await r2_get(client, FOLDER + entry["file"])
    if not obj or not obj.get("d"): return "rebuild"
    by_date = {c["d"]: c for c in from_file(obj)}
    changed = False
    for c in new:
        if by_date.get(c["d"]) != c: by_date[c["d"]] = c; changed = True
    if not changed: return None
    floor = years_floor(today)
    candles = [by_date[d] for d in sorted(by_date) if d >= floor]
    out = to_file(sym, info["ex"], candles, today, obj.get("bse_until"))
    async with r2sem:
        await r2_put(client, FOLDER + entry["file"], out)
    return index_entry(out)


# ══════════════════════════════════════════════════════════════
# INDEX
# ══════════════════════════════════════════════════════════════

async def load_index(client) -> dict:
    j = await r2_get(client, INDEX_FILE)
    return (j or {}).get("stocks", {}) if isinstance(j, dict) else {}

async def save_index(client, stocks: dict, today: str):
    await r2_put(client, INDEX_FILE, {"updated": today, "years": CHART_YEARS, "count": len(stocks),
                                      "stocks": dict(sorted(stocks.items()))}, quiet=False)


# ══════════════════════════════════════════════════════════════
# MODES
# ══════════════════════════════════════════════════════════════

async def _build_many(client, uni, bse_keys, idx, syms, today):
    sem = asyncio.Semaphore(UPSTOX_CONCURRENCY); r2sem = asyncio.Semaphore(R2_CONCURRENCY)
    ok = 0; failed = []; since_save = 0
    async def one(sym):
        nonlocal ok
        try:
            obj, entry = await build_stock(client, sem, sym, uni[sym], bse_keys, today)
            if not obj: failed.append(sym); return
            async with r2sem:
                await r2_put(client, FOLDER + entry["file"], obj)
            idx[sym] = entry; ok += 1
        except SystemExit: raise
        except Exception as e:
            failed.append(sym); log.warning(f"  {sym}: {e}")
    for i in range(0, len(syms), BATCH):
        part = syms[i:i+BATCH]
        await asyncio.gather(*[one(s) for s in part])
        since_save += len(part)
        log.info(f"  built {min(i+BATCH, len(syms))}/{len(syms)}  ✓{ok}  ✗{len(failed)}")
        if since_save >= CHECKPOINT_EVERY:
            await save_index(client, idx, today); since_save = 0
    return ok, failed

async def run_backfill(force=False, only=None):
    status = PipelineStatus("chart_backfill")
    try:
        today = today_ist()
        async with httpx.AsyncClient() as client:
            uni, bse_keys = await load_universe(client)
            idx = await load_index(client)
            if only:
                syms = [s for s in only if s in uni]
                missing = [s for s in only if s not in uni]
                if missing: log.warning(f"Not in universe, skipped: {missing}")
            elif force:
                syms = sorted(uni)
            else:
                syms = sorted(s for s in uni if s not in idx)
            log.info(f"━━━ Chart backfill  {years_floor(today)} → {today}  ({len(syms)} stocks) ━━━")
            ok, failed = await _build_many(client, uni, bse_keys, idx, syms, today)
            await save_index(client, idx, today)
            joined = sum(1 for e in idx.values() if e.get("bse_until"))
            log.info(f"✓ {ok} built  ✗ {len(failed)} no data  |  {joined} stocks use BSE history before NSE listing")
            if failed: log.info(f"  No data (re-run to retry): {failed[:20]}{'…' if len(failed) > 20 else ''}")
        status.success()
    except Exception as e:
        status.failure(e)

async def run_daily():
    status = PipelineStatus("chart_daily")
    try:
        today = today_ist()
        log.info(f"━━━ Chart daily  {today} ━━━")
        async with httpx.AsyncClient() as client:
            uni, bse_keys = await load_universe(client)
            idx = await load_index(client)
            if not idx:
                log.warning("chart/_index.json is empty — run 'backfill' first"); status.success(); return

            # Stocks no longer in classification.json leave the index (their file stays, unused)
            gone = [s for s in idx if s not in uni]
            for s in gone: del idx[s]
            if gone: log.info(f"Removed from index: {len(gone)} {gone[:10]}")

            # New in the universe (new listing, or a stock that moved BSE -> NSE): full build
            # incl. BSE history; exchange changed for an existing symbol: rebuild too
            new = sorted(s for s in uni if s not in idx or idx[s].get("ex") != uni[s]["ex"])
            if new:
                log.info(f"🆕 {len(new)} new/changed stock(s): {new[:15]}{'…' if len(new) > 15 else ''}")
                await _build_many(client, uni, bse_keys, idx, new, today)

            sem = asyncio.Semaphore(UPSTOX_CONCURRENCY); r2sem = asyncio.Semaphore(R2_CONCURRENCY)
            todo = sorted(s for s in uni if s in idx and s not in new and idx[s]["last"] <= today)
            updated = 0; rebuild = []; errors = 0
            async def one(sym):
                nonlocal updated, errors
                try:
                    res = await update_stock(client, sem, r2sem, sym, uni[sym], idx[sym], today)
                    if res == "rebuild": rebuild.append(sym)
                    elif res: idx[sym] = res; updated += 1
                except SystemExit: raise
                except Exception as e:
                    errors += 1; log.warning(f"  {sym}: {e}")
            for i in range(0, len(todo), BATCH * 2):
                await asyncio.gather(*[one(s) for s in todo[i:i+BATCH*2]])
                log.info(f"  updated {min(i+BATCH*2, len(todo))}/{len(todo)}  ✓{updated}  ✗{errors}")
            if rebuild:
                log.info(f"Missing files, rebuilding {len(rebuild)}: {rebuild[:10]}")
                await _build_many(client, uni, bse_keys, idx, sorted(rebuild), today)
            await save_index(client, idx, today)
            log.info(f"✓ {updated} stocks updated  |  {len(idx)} in chart/_index.json")
        status.success()
    except Exception as e:
        status.failure(e)

async def run_status():
    async with httpx.AsyncClient() as client:
        idx = await load_index(client)
    if not idx: print("chart/_index.json is empty — run 'backfill' first"); return
    lasts = sorted(e["last"] for e in idx.values())
    joined = sorted((s, e["bse_until"]) for s, e in idx.items() if e.get("bse_until"))
    print(f"Stocks: {len(idx)}  NSE: {sum(1 for e in idx.values() if e['ex']=='NSE')}  "
          f"BSE: {sum(1 for e in idx.values() if e['ex']=='BSE')}")
    print(f"Last candle: oldest {lasts[0]}  newest {lasts[-1]}")
    print(f"Average bars: {sum(e['bars'] for e in idx.values()) // len(idx)}")
    print(f"Using BSE history before NSE listing: {len(joined)}")
    for s, d in joined[:30]: print(f"  {s:<15} NSE from {d}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "backfill":       asyncio.run(run_backfill())
        case "backfill_force": asyncio.run(run_backfill(force=True))
        case "daily":          asyncio.run(run_daily())
        case "rebuild":
            if len(sys.argv) < 3: print("Usage: python pipeline_chart.py rebuild SYM1,SYM2"); sys.exit(1)
            asyncio.run(run_backfill(only=[s.strip().upper() for s in sys.argv[2].split(",") if s.strip()]))
        case "status":         asyncio.run(run_status())
        case _:
            print(__doc__); sys.exit(1)
