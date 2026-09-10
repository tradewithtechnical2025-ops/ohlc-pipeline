#!/usr/bin/env python3
"""
pipeline_75min.py — Standalone 75-min OHLC pipeline (GitHub Actions)
OHLC source: Upstox V3 API, custom `minutes/75` interval

Confirmed live: Upstox's V3 custom-interval endpoint accepts `minutes/75`
directly, returns bars correctly anchored to market open (9:15, 10:30,
11:45, 13:00, 14:15 IST — 5 bars/trading day), and a single historical
call covers a full 30-day range with no truncation. No resampling from a
finer interval needed.

TWO-MODE SPLIT (mirrors the main pipeline.py's run_full() vs
run_daily()/run_today() pattern):
  - full  → HISTORICAL endpoint only. Backfills symbols that don't yet
    have enough 75m history (new listings, or the very first time this
    runs). NOT part of the daily schedule — run manually/occasionally.
  - daily → INTRADAY endpoint only, every symbol, every trading day.
    Confirmed live that the HISTORICAL endpoint doesn't reliably include
    TODAY's session even after market close (Upstox's own docs treat
    historical vs intraday as separate concepts), so the daily job never
    touches the historical endpoint at all — it just upserts today's
    bars via intraday and prunes anything rolled outside the rolling
    window. Half the API calls of merging historical+intraday every day.

Storage: its own ohlc_75m_1.json..ohlc_75m_N.json chunk set on R2 —
completely separate from the main pipeline's daily ohlc_*.json chunks.
Rolling 1-month window (not the main pipeline's 3-year window).

Usage:
  python pipeline_75min.py full     # one-time/occasional backfill
  python pipeline_75min.py daily    # every trading day, after close
  python pipeline_75min.py status   # quick summary of what's stored
"""

import asyncio
import gzip
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from r2_manifest import upload_str_with_manifest

# ── Telegram notify (same optional-import pattern as the main pipeline) ──
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
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

HERE = Path(__file__).parent
with open(HERE / "nse_holidays.json") as f:
    NSE_HOLIDAYS: set[str] = set(json.load(f))

WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

def _upstox_headers():
    return {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_TOKEN}"}

RETRY       = 5
RATE_DELAY  = 0.4
CONCURRENCY = 5

OHLC_75M_CHUNKS      = 4     # separate chunk count from the main pipeline's R2_CHUNKS
ROLLING_75M_DAYS     = 30    # 1 month — deliberately short; 75-min bars for 3 years
                              # would be ~5x the size of the entire daily 3-year store
                              # for no real screener benefit
MIN_75M_HISTORY_BARS = 100   # ~20 trading days worth (5 bars/day) — below this,
                              # run_full() treats the symbol as needing backfill

NSE_BOD_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
BSE_BOD_URL = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"


# ══════════════════════════════════════════════════════════════
# TRADING CALENDAR — same logic as the main pipeline.py
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


# ══════════════════════════════════════════════════════════════
# INSTRUMENT MAP — same BOD-file approach as the main pipeline.py
# ══════════════════════════════════════════════════════════════

def _parse_bod_instruments(instruments, segment) -> dict[str, str]:
    NSE_SUFFIXES = ("-EQ","-BE","-BL","-SM","-IL","-IV","-W1","-W2","-W3","-W4","-W5")
    sym_map = {}
    for inst in instruments:
        if inst.get("segment") != segment: continue
        if segment == "NSE_EQ":
            itype = inst.get("instrument_type", "")
            if itype in ("SG","GB","TB","GS","CE","PE","FF","MF"): continue
        tsym = (inst.get("trading_symbol") or "").upper()
        ikey = inst.get("instrument_key")
        if not tsym or not ikey: continue
        sym_map[tsym] = ikey
        for suffix in NSE_SUFFIXES:
            if tsym.endswith(suffix):
                base = tsym[:-len(suffix)]
                if base and base not in sym_map:
                    sym_map[base] = ikey
                break
    return sym_map

async def _load_bod_map(client, url, segment) -> dict[str, str]:
    for attempt in range(RETRY):
        try:
            r = await client.get(url, headers=_upstox_headers(), timeout=60, follow_redirects=True)
        except httpx.RequestError as e:
            log.warning(f"  BOD download error ({e}), retry {attempt+1}")
            await asyncio.sleep(2 ** attempt); continue
        if r.status_code != 200:
            log.warning(f"  BOD {url} → HTTP {r.status_code}, retry {attempt+1}")
            await asyncio.sleep(2 ** attempt); continue
        try:
            instruments = json.loads(gzip.decompress(r.content))
        except Exception as e:
            log.warning(f"  BOD decompress error: {e}"); break
        sym_map = _parse_bod_instruments(instruments, segment)
        log.info(f"  BOD {segment}: {len(sym_map)} instruments")
        return sym_map
    log.warning(f"  BOD {segment} failed — falling back to cached ikey_map.json")
    return {}

async def build_isin_map(client):
    """Same shape/behaviour as the main pipeline.py's build_isin_map(),
    reusing the same classification.json + ikey_map.json cache on R2 so
    both pipelines see an identical stock universe. This does NOT
    re-upload ikey_map.json itself (the main pipeline already keeps that
    cache fresh) — if the BOD download fails here and there's no cache
    yet, this will come back emptier than the main pipeline until that's
    run at least once."""
    log.info("Building instrument map…")
    master = await r2_download(client, "classification.json")
    if not master or not isinstance(master, list):
        raise RuntimeError("classification.json missing or invalid in R2!")

    nse_bod_task = asyncio.create_task(_load_bod_map(client, NSE_BOD_URL, "NSE_EQ"))
    bse_bod_task = asyncio.create_task(_load_bod_map(client, BSE_BOD_URL, "BSE_EQ"))
    cache_task   = asyncio.create_task(r2_download(client, "ikey_map.json"))
    nse_bod, bse_bod, cached = await asyncio.gather(nse_bod_task, bse_bod_task, cache_task)

    if not nse_bod and isinstance(cached, dict):
        log.info(f"  Using cached ikey_map.json ({len(cached.get('nse',{}))} NSE entries)")
        nse_bod = cached.get("nse", {})
        bse_bod = cached.get("bse", {})

    nse_map = {}; bse_map = {}
    for stock in master:
        sym = str(stock.get("symbol", "")).strip().upper()
        exchange = str(stock.get("exchange", "")).strip()
        if not sym: continue
        if exchange == "NSE":
            ikey = nse_bod.get(sym)
            if ikey: nse_map[sym] = ikey
        elif exchange == "BSE":
            ikey = bse_bod.get(sym)
            if ikey: bse_map[sym] = ikey

    log.info(f"✓ NSE: {len(nse_map)} resolved   ✓ BSE: {len(bse_map)} resolved")
    return nse_map, bse_map


# ══════════════════════════════════════════════════════════════
# R2 HELPERS — same shape as the main pipeline.py
# ══════════════════════════════════════════════════════════════

async def r2_upload(client, filename, data):
    if isinstance(data, str): data = data.encode()
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(url, headers={**WORKER_HEADERS,"Content-Type":"application/json"}, content=data, timeout=90)
    if r.status_code != 200: raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")

async def r2_download(client, filename):
    url = f"{WORKER_URL}/{filename}"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404: return None
    if r.status_code != 200: raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↓ {filename} ({len(r.content)/1024:.0f} KB)")
    return r.json()

async def download_75m_chunks(client) -> dict:
    tasks = [r2_download(client, f"ohlc_75m_{i+1}.json") for i in range(OHLC_75M_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception): log.warning(f"  ohlc_75m_{i+1}.json error: {res}")
        elif res and "stocks" in res: all_data.update(res["stocks"])
    log.info(f"75m master: {len(all_data)} stocks across {OHLC_75M_CHUNKS} chunks")
    return all_data

async def upload_75m_chunks(client, all_data, today):
    symbols = sorted(all_data.keys()); n = len(symbols)
    size = (n + OHLC_75M_CHUNKS - 1) // OHLC_75M_CHUNKS; tasks = []
    for i in range(OHLC_75M_CHUNKS):
        chunk_syms = symbols[i*size:(i+1)*size]; chunk = {s: all_data[s] for s in chunk_syms}
        payload = json.dumps({"updated": today, "chunk": i+1, "total": OHLC_75M_CHUNKS, "stocks": chunk})
        tasks.append(upload_str_with_manifest(
            client, r2_upload, f"ohlc_75m_{i+1}.json", payload,
            schema_v=1, extra_meta={"chunk": i+1, "total": OHLC_75M_CHUNKS, "stock_count": len(chunk_syms)}
        ))
    await asyncio.gather(*tasks)
    log.info(f"✓ {OHLC_75M_CHUNKS} 75m chunks uploaded ({n} stocks)")


# ══════════════════════════════════════════════════════════════
# UPSTOX 75-MIN FETCHERS
# ══════════════════════════════════════════════════════════════

async def fetch_75min(client, sem, sym, instrument_key, from_date, to_date):
    """HISTORICAL endpoint, custom minutes/75 interval. Used only by
    run_75min_full() for backfill — no yearly chunking needed, a full
    ROLLING_75M_DAYS-length request comes back in one call (confirmed
    live up to 35 calendar days, no truncation). Never used for "today"
    — see fetch_75min_intraday()."""
    url = f"https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/75/{to_date}/{from_date}"
    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            try:
                r = await client.get(url, headers=_upstox_headers(), timeout=30)
            except httpx.RequestError as e:
                log.warning(f"{sym}: 75m hist network error ({e}), retry {attempt+1}")
                await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 401: log.error("❌ UPSTOX_TOKEN invalid"); sys.exit(1)
        if r.status_code == 429:
            wait = 30 * (attempt + 1)
            log.warning(f"{sym}: 429 — {wait}s"); await asyncio.sleep(wait); continue
        if r.status_code in (502, 503, 504): await asyncio.sleep(2 ** attempt); continue
        if r.status_code in (404, 400): break
        if r.status_code != 200: break
        try: payload = r.json()
        except: break
        candles = []
        for row in (payload.get("data") or {}).get("candles") or []:
            # row: [timestamp, open, high, low, close, volume, oi]
            # timestamp is a full ISO datetime e.g. "2026-09-09T14:15:00+05:30"
            # — kept in full (unlike the main pipeline's plain daily date)
            # since there are multiple bars per day here.
            candles.append({"t": row[0], "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5]})
        candles.sort(key=lambda x: x["t"])
        return sym, candles
    return sym, None


async def fetch_75min_intraday(client, sem, sym, instrument_key):
    """INTRADAY endpoint — today's session only. Called for every symbol,
    every trading day, by run_75min_daily()."""
    url = f"https://api.upstox.com/v3/historical-candle/intraday/{instrument_key}/minutes/75"
    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            try:
                r = await client.get(url, headers=_upstox_headers(), timeout=30)
            except httpx.RequestError as e:
                log.warning(f"{sym}: 75m intraday network error ({e}), retry {attempt+1}")
                await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 401: log.error("❌ UPSTOX_TOKEN invalid"); sys.exit(1)
        if r.status_code == 429:
            wait = 30 * (attempt + 1)
            log.warning(f"{sym}: 429 — {wait}s"); await asyncio.sleep(wait); continue
        if r.status_code in (502, 503, 504): await asyncio.sleep(2 ** attempt); continue
        if r.status_code in (404, 400): break
        if r.status_code != 200: break
        try: payload = r.json()
        except: break
        candles = []
        for row in (payload.get("data") or {}).get("candles") or []:
            candles.append({"t": row[0], "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5]})
        candles.sort(key=lambda x: x["t"])
        return sym, candles
    return sym, None


# ══════════════════════════════════════════════════════════════
# DATA HELPERS
# ══════════════════════════════════════════════════════════════

def apply_75m_rolling_window(all_data, cutoff_date):
    """cutoff_date: 'YYYY-MM-DD'. Drops bars whose date portion (t[:10])
    is older than cutoff."""
    dropped = 0
    for s in all_data.values():
        keep = [i for i, t in enumerate(s["t"]) if t[:10] >= cutoff_date]
        dropped += len(s["t"]) - len(keep)
        for k in s: s[k] = [s[k][i] for i in keep]
    return dropped

def merge_75m_into(all_data, sym, candles, cutoff_date):
    if sym not in all_data: all_data[sym] = {k: [] for k in ("t", "o", "h", "l", "c", "v")}
    s = all_data[sym]; existing = set(s["t"]); added = 0
    for c in candles:
        if c["t"][:10] < cutoff_date or c["t"] in existing: continue
        for k in s: s[k].append(c[k])
        existing.add(c["t"]); added += 1
    if added:
        order = sorted(range(len(s["t"])), key=lambda i: s["t"][i])
        for k in s: s[k] = [s[k][i] for i in order]
    return added


# ══════════════════════════════════════════════════════════════
# PIPELINE MODES
# ══════════════════════════════════════════════════════════════

async def run_full() -> None:
    """Backfills 75m history for any symbol that doesn't have enough yet
    (new listings, or the very first time this pipeline ever runs). Uses
    the HISTORICAL endpoint only — never touches "today". NOT scheduled
    daily; run manually / occasionally."""
    status = PipelineStatus("75min_full")
    try:
        today = today_ist()
        cutoff = (date.fromisoformat(today) - timedelta(days=ROLLING_75M_DAYS)).isoformat()
        thru = prev_trading_day(today) if is_trading_day(today) else last_trading_day()
        log.info(f"━━━ 75-min Full Backfill  {cutoff} → {thru} ━━━")
        sem = asyncio.Semaphore(CONCURRENCY)
        async with httpx.AsyncClient() as client:
            nse_map, bse_map = await build_isin_map(client)
            all_ikeys = {**nse_map, **bse_map}
            live = set(all_ikeys)

            all_data = await download_75m_chunks(client)
            needs_backfill = {
                sym for sym in live
                if len((all_data.get(sym) or {}).get("t", [])) < MIN_75M_HISTORY_BARS
            }
            if not needs_backfill:
                log.info("✅ All stocks already have sufficient 75m history — nothing to backfill")
                return
            log.info(f"Backfilling {len(needs_backfill)} symbol(s)")

            tasks = [fetch_75min(client, sem, sym, all_ikeys[sym], cutoff, thru) for sym in needs_backfill]
            results = await asyncio.gather(*tasks)
            fetched = {sym: c for sym, c in results if c}
            log.info(f"✓ {len(fetched)} fetched  ✗ {len(needs_backfill) - len(fetched)} no data")

            total_new = 0
            for sym, candles in fetched.items():
                total_new += merge_75m_into(all_data, sym, candles, cutoff)
            log.info(f"Merged: {total_new} new bars")

            await upload_75m_chunks(client, all_data, today)
        status.success()
        log.info("━━━ 75-min Full Backfill complete ━━━")
    except Exception as e:
        status.failure(e)


async def run_daily() -> None:
    """Every trading day: fetches ONLY today's session via the INTRADAY
    endpoint, for every live symbol. Never calls the historical endpoint.
    Upserts today's bars and prunes anything rolled outside the
    ROLLING_75M_DAYS window."""
    status = PipelineStatus("75min_daily")
    try:
        today = today_ist()
        if not is_trading_day(today):
            log.info(f"⏭  {today} not a trading day — skipping 75-min daily")
            return
        cutoff = (date.fromisoformat(today) - timedelta(days=ROLLING_75M_DAYS)).isoformat()
        log.info(f"━━━ 75-min Daily (intraday-only)  {today}  cutoff {cutoff} ━━━")
        sem = asyncio.Semaphore(CONCURRENCY)
        async with httpx.AsyncClient() as client:
            nse_map, bse_map = await build_isin_map(client)
            all_ikeys = {**nse_map, **bse_map}
            live = set(all_ikeys)

            all_data = await download_75m_chunks(client)

            tasks = [fetch_75min_intraday(client, sem, sym, all_ikeys[sym]) for sym in live]
            results = await asyncio.gather(*tasks)
            fetched = {sym: c for sym, c in results if c}
            log.info(f"✓ {len(fetched)} fetched today  ✗ {len(live) - len(fetched)} no data")

            pruned = [s for s in list(all_data) if s not in live]
            for s in pruned: del all_data[s]
            if pruned: log.info(f"🗑  Pruned {len(pruned)} stocks")

            total_new = 0
            for sym, candles in fetched.items():
                total_new += merge_75m_into(all_data, sym, candles, cutoff)
            log.info(f"Merged: {total_new} new bars")

            dropped = apply_75m_rolling_window(all_data, cutoff)
            log.info(f"Rolling: dropped {dropped} old bars")

            await upload_75m_chunks(client, all_data, today)
        status.success()
        log.info("━━━ 75-min Daily complete ━━━")
    except Exception as e:
        status.failure(e)


async def run_status() -> None:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[r2_download(client, f"ohlc_75m_{i+1}.json") for i in range(OHLC_75M_CHUNKS)], return_exceptions=True)
    print(f"\n{'File':<20} {'Stocks':>7}  {'Bars/stock':>10}  {'Updated':>12}")
    print("─"*60); total = 0
    for i, res in enumerate(results):
        fname = f"ohlc_75m_{i+1}.json"
        if isinstance(res, Exception) or res is None: print(f"{fname:<20}  ERROR"); continue
        stocks = res.get("stocks", {})
        if not stocks: print(f"{fname:<20}  (empty)"); continue
        s0 = next(iter(stocks.values())); total += len(stocks)
        print(f"{fname:<20} {len(stocks):>7}  {len(s0['t']):>10}  {res.get('updated','?'):>12}")
    print(f"\nTotal: {total} stocks\n")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "full":   asyncio.run(run_full())
        case "daily":  asyncio.run(run_daily())
        case "status": asyncio.run(run_status())
        case _:
            print(__doc__)
            sys.exit(1)
