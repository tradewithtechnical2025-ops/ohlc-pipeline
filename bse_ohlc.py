#!/usr/bin/env python3
"""
BSE OHLC Pipeline (separate chunks) — Finedge powered.

Universe : bse.json (R2)            — BSE-only universe (price>20, mcap>=100cr)
Output   : bse_ohlc_1..N.json (R2)  — chunked OHLC
           bse_ohlc_delta.json (R2)  — today's delta

Usage:
  python bse_ohlc.py full     # 1.5yr initial load (manual, once)
  python bse_ohlc.py daily    # incremental update (scheduled, weekdays EOD)
  python bse_ohlc.py status   # chunk summary
"""

import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

UNIVERSE_FILE = "bse.json"
CHUNK_PREFIX  = "bse_ohlc"
DELTA_FILE    = "bse_ohlc_delta.json"

ROLLING_DAYS = 548
BSE_CHUNKS   = 2                 # 620 stocks -> ~310 each
CONCURRENCY  = 5
RETRY        = 5
RATE_DELAY   = 0.30

DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}

OHLC_KEYS = ("d", "o", "h", "l", "c", "v", "oi")


# ── calendar ──
def today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")

def rolling_cutoff(anchor: str) -> str:
    return (date.fromisoformat(anchor) - timedelta(days=ROLLING_DAYS)).isoformat()


# ── Finedge ──
async def fetch_ohlc(client, sem, sym, from_year, to_year):
    url = f"{FINEDGE_BASE}/daily-quotes/{sym}"
    params = {"from": from_year, "to": to_year, "token": FINEDGE_TOKEN}
    for attempt in range(RETRY):
        async with sem:
            await asyncio.sleep(RATE_DELAY)
            try:
                r = await client.get(url, params=params, timeout=30)
            except httpx.RequestError:
                await asyncio.sleep(2 ** attempt); continue
        if r.status_code == 401:
            print("❌ FINEDGE TOKEN INVALID"); sys.exit(1)
        if r.status_code == 429:
            await asyncio.sleep(15); continue
        if r.status_code in (502, 503, 504):
            await asyncio.sleep(2 ** attempt); continue
        if r.status_code != 200:
            return sym, None
        try:
            payload = r.json()
        except Exception:
            return sym, None
        raw = payload.get("price", [])
        if not raw:
            return sym, None
        candles = sorted([
            {"d": c["quote_date"], "o": c["open_price"], "h": c["high_price"],
             "l": c["low_price"], "c": c["close_price"], "v": c["volume"], "oi": 0}
            for c in raw
            if c.get("quote_date") and None not in
            (c.get("open_price"), c.get("high_price"), c.get("low_price"), c.get("close_price"))
        ], key=lambda x: x["d"])
        return sym, candles
    return sym, None


# ── R2 ──
async def r2_download(client, filename):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=120)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    return r.json()

async def r2_upload(client, filename, data):
    if not isinstance(data, (str, bytes)):
        data = json.dumps(data)
    if isinstance(data, str):
        data = data.encode()
    r = await client.post(f"{WORKER_URL}?file={filename}", headers=UP_HEADERS,
                          content=data, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    print(f"  ↑ {filename} ({len(data)/1024:.1f} KB)")


async def download_chunks(client) -> dict:
    tasks = [r2_download(client, f"{CHUNK_PREFIX}_{i+1}.json") for i in range(BSE_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception):
            print(f"  {CHUNK_PREFIX}_{i+1}.json error: {res}")
        elif res and "stocks" in res:
            all_data.update(res["stocks"])
    print(f"Master: {len(all_data)} stocks across {BSE_CHUNKS} chunks")
    return all_data

async def upload_chunks(client, all_data, today):
    symbols = sorted(all_data.keys()); n = len(symbols)
    size = (n + BSE_CHUNKS - 1) // BSE_CHUNKS
    tasks = []
    for i in range(BSE_CHUNKS):
        chunk_syms = symbols[i*size:(i+1)*size]
        chunk = {s: all_data[s] for s in chunk_syms}
        payload = json.dumps({"updated": today, "chunk": i+1, "total": BSE_CHUNKS, "stocks": chunk})
        tasks.append(r2_upload(client, f"{CHUNK_PREFIX}_{i+1}.json", payload))
    await asyncio.gather(*tasks)
    print(f"✓ {BSE_CHUNKS} chunks uploaded ({n} stocks)")


# ── data helpers ──
def build_stock_obj(candles):
    return {k: [c[k] for c in candles] for k in OHLC_KEYS}

def _sort_stock(s):
    order = sorted(range(len(s["d"])), key=lambda i: s["d"][i])
    for k in s:
        s[k] = [s[k][i] for i in order]

def merge_candles_into(all_data, sym, candles, cutoff):
    if sym not in all_data:
        all_data[sym] = {k: [] for k in OHLC_KEYS}
    s = all_data[sym]; existing = set(s["d"]); added = 0
    for c in candles:
        if c["d"] < cutoff or c["d"] in existing:
            continue
        for k in s:
            s[k].append(c[k])
        existing.add(c["d"]); added += 1
    if added:
        _sort_stock(s)
    return added

def apply_rolling(all_data, cutoff):
    dropped = 0
    for s in all_data.values():
        keep = [i for i, d in enumerate(s["d"]) if d >= cutoff]
        dropped += len(s["d"]) - len(keep)
        for k in s:
            s[k] = [s[k][i] for i in keep]
    return dropped


async def load_universe(client):
    bse = await r2_download(client, UNIVERSE_FILE)
    if not isinstance(bse, list) or not bse:
        raise RuntimeError(f"{UNIVERSE_FILE} not found / empty")
    syms = [str(s.get("symbol", "")).strip() for s in bse if s.get("symbol")]
    print(f"Universe: {len(syms)} BSE symbols")
    return syms


# ── modes ──
async def run_full():
    today = today_ist()
    cutoff = rolling_cutoff(today)
    from_year = int(cutoff[:4]); to_year = int(today[:4])
    print(f"━━━ BSE Full Load  {cutoff} → {today} ━━━")
    sem = asyncio.Semaphore(CONCURRENCY)
    all_data = {}; failed = []
    async with httpx.AsyncClient() as client:
        syms = await load_universe(client)
        for i in range(0, len(syms), 50):
            chunk_syms = syms[i:i+50]
            results = await asyncio.gather(*[fetch_ohlc(client, sem, s, from_year, to_year) for s in chunk_syms])
            for sym, candles in results:
                if candles:
                    filtered = [c for c in candles if c["d"] >= cutoff]
                    if filtered:
                        all_data[sym] = build_stock_obj(filtered)
                else:
                    failed.append(sym)
            print(f"  {min(i+50,len(syms))}/{len(syms)}  OK:{len(all_data)}  Failed:{len(failed)}")
        print(f"✓ {len(all_data)} loaded  ✗ {len(failed)} no data")
        await upload_chunks(client, all_data, today)
    print("━━━ BSE Full complete ━━━")


async def run_daily():
    today = today_ist()
    cutoff = rolling_cutoff(today)
    from_year = int(cutoff[:4]); to_year = int(today[:4])
    print(f"━━━ BSE Daily  {today}  cutoff {cutoff} ━━━")
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient() as client:
        syms = await load_universe(client)
        all_data = await download_chunks(client)

        # prune symbols no longer in universe
        live = set(syms)
        for s in [x for x in list(all_data) if x not in live]:
            del all_data[s]

        results = await asyncio.gather(*[fetch_ohlc(client, sem, s, from_year, to_year) for s in syms])
        total_new = 0; delta = {}
        for sym, candles in results:
            if not candles:
                continue
            total_new += merge_candles_into(all_data, sym, candles, cutoff)
            today_c = next((c for c in candles if c["d"] == today), None)
            if today_c:
                delta[sym] = today_c
        print(f"Merged: {total_new} new  Delta: {len(delta)}")
        dropped = apply_rolling(all_data, cutoff)
        print(f"Rolling: dropped {dropped} old candles")
        await asyncio.gather(
            upload_chunks(client, all_data, today),
            r2_upload(client, DELTA_FILE, json.dumps({"date": today, "stocks": delta})),
        )
    print("━━━ BSE Daily complete ━━━")


async def run_status():
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *[r2_download(client, f"{CHUNK_PREFIX}_{i+1}.json") for i in range(BSE_CHUNKS)],
            return_exceptions=True)
    total = 0
    for i, res in enumerate(results):
        fname = f"{CHUNK_PREFIX}_{i+1}.json"
        if isinstance(res, Exception) or res is None:
            print(f"{fname}: ERROR/empty"); continue
        stocks = res.get("stocks", {})
        if not stocks:
            print(f"{fname}: (empty)"); continue
        s0 = next(iter(stocks.values())); total += len(stocks)
        print(f"{fname}: {len(stocks)} stocks  {s0['d'][0]} → {s0['d'][-1]}  updated {res.get('updated','?')}")
    print(f"Total: {total} stocks")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if   mode == "full":   asyncio.run(run_full())
    elif mode == "daily":  asyncio.run(run_daily())
    elif mode == "status": asyncio.run(run_status())
    else:
        print(__doc__); sys.exit(1)
