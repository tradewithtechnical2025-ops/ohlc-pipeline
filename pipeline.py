#!/usr/bin/env python3
"""
NSE OHLC Pipeline — GitHub Actions
Replaces Google Apps Script.

Usage:
  python pipeline.py daily   # prev-day OHLC          (4:00 PM IST)
  python pipeline.py today   # T+0 intraday candle     (4:50 PM IST)
  python pipeline.py full    # initial 1.5yr load      (manual, once)
  python pipeline.py status  # print R2 chunk summary
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

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
ROLLING_DAYS = 548    # 1.5 years
R2_CHUNKS    = 8
CONCURRENCY  = 15     # parallel Upstox calls (stay under rate limit)
RETRY        = 3

HERE = Path(__file__).parent

# ── Data files ────────────────────────────────────────────────
# isin_map.json  → export from your GAS script via: Logger.log(JSON.stringify(getISINMap()))
# nse_holidays.json → list of "YYYY-MM-DD" strings from NSE official calendar
with open(HERE / "isin_map.json") as f:
    ISIN_MAP: dict[str, str] = json.load(f)       # { "RELIANCE": "INE002A01018", ... }

with open(HERE / "nse_holidays.json") as f:
    NSE_HOLIDAYS: set[str] = set(json.load(f))    # ["2025-01-26", "2025-08-15", ...]


# ══════════════════════════════════════════════════════════════
# TRADING CALENDAR
# ══════════════════════════════════════════════════════════════

def today_ist() -> str:
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")

def is_trading_day(d: str) -> bool:
    dt = date.fromisoformat(d)
    return dt.weekday() < 5 and d not in NSE_HOLIDAYS   # Mon–Fri, not holiday

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
# UPSTOX API  (async)
# ══════════════════════════════════════════════════════════════

UPSTOX_HEADERS = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept": "application/json",
}


async def fetch_ohlc(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    sym: str,
    isin: str,
    from_date: str,
    to_date: str,
) -> tuple[str, list | None]:
    """v2 historical OHLC (T+1). Returns (sym, sorted_candles | None)."""
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
            log.warning(f"  {sym}: rate limited, wait 3s")
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
    """v3 intraday T+0 candle. Returns (sym, candle | None)."""
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
# R2 / CLOUDFLARE WORKER  (async)
# ══════════════════════════════════════════════════════════════

WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}


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
    """Download all 8 R2 chunks in parallel and merge."""
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
    """Split all_data into 8 chunks and upload all in parallel."""
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
    """Add new candles (skipping existing dates and pre-cutoff). Returns count added."""
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
    """Insert or overwrite a single candle (for T+0 updates)."""
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
# PIPELINE MODES
# ══════════════════════════════════════════════════════════════

async def run_daily() -> None:
    """
    4:00 PM IST — fetch prev-day (and today's closed) OHLC via v2.
    Parallel fetch → parallel chunk download → merge → parallel upload.
    """
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    prev  = prev_trading_day(today)
    cutoff = rolling_cutoff(today)
    log.info(f"━━━ Daily  {prev} → {today}  cutoff {cutoff} ━━━")

    sem = asyncio.Semaphore(CONCURRENCY)
    symbols = list(ISIN_MAP.keys())

    async with httpx.AsyncClient() as client:
        # 1. Fetch all stocks in parallel
        log.info(f"Fetching {len(symbols)} stocks (v2)…")
        tasks = [
            fetch_ohlc(client, sem, sym, ISIN_MAP[sym], prev, today)
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)

        fetched = {sym: c for sym, c in results if c}
        log.info(f"✓ {len(fetched)} fetched  ✗ {len(symbols)-len(fetched)} no data")

        # 2. Download master in parallel
        log.info("Downloading master chunks…")
        all_data = await download_all_chunks(client)

        # 3. Prune delisted stocks
        live = set(ISIN_MAP)
        pruned = [s for s in list(all_data) if s not in live]
        for s in pruned:
            del all_data[s]
        if pruned:
            log.info(f"🗑  Pruned {len(pruned)} delisted stocks")

        # 4. Merge new candles
        total_new = 0
        delta: dict = {}
        for sym, candles in fetched.items():
            total_new += merge_candles_into(all_data, sym, candles, cutoff)
            today_c = next((c for c in candles if c["d"] == today), None)
            if today_c:
                delta[sym] = today_c

        log.info(f"Merged: {total_new} new candles  Delta: {len(delta)} stocks")

        # 5. Rolling window
        dropped = apply_rolling_window(all_data, cutoff)
        log.info(f"Rolling: dropped {dropped} old candles")

        # 6. Upload everything in parallel
        log.info("Uploading…")
        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_delta.json",
                      json.dumps({"date": today, "stocks": delta})),
        )

    log.info("━━━ Daily complete ━━━")


async def run_today() -> None:
    """
    4:50 PM IST — fetch T+0 intraday candle via v3 and patch chunks.
    Much lighter: only overwrites today's candle, no new history.
    """
    today = today_ist()
    if not is_trading_day(today):
        log.info(f"⏭  {today} is not a trading day — exiting")
        return

    log.info(f"━━━ Today (v3 intraday)  {today} ━━━")

    sem = asyncio.Semaphore(CONCURRENCY)
    symbols = list(ISIN_MAP.keys())

    async with httpx.AsyncClient() as client:
        # 1. Fetch all T+0 candles in parallel
        log.info(f"Fetching {len(symbols)} stocks (v3)…")
        tasks = [
            fetch_today_candle(client, sem, sym, ISIN_MAP[sym])
            for sym in symbols
        ]
        results = await asyncio.gather(*tasks)

        fetched = {sym: c for sym, c in results if c}
        log.info(f"✓ {len(fetched)} have today's candle")

        # 2. Download master in parallel
        log.info("Downloading master chunks…")
        all_data = await download_all_chunks(client)

        # 3. Upsert today's candles
        for sym, c in fetched.items():
            upsert_candle(all_data, sym, c)

        # 4. Upload in parallel
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
    """
    One-time initial load: 1.5 years of history for all stocks.
    Run manually: python pipeline.py full
    Takes ~3–5 minutes with CONCURRENCY=15.
    """
    today  = last_trading_day()
    start  = (date.fromisoformat(today) - timedelta(days=ROLLING_DAYS)).isoformat()
    cutoff = start
    log.info(f"━━━ Full Load  {start} → {today}  ({len(ISIN_MAP)} stocks) ━━━")

    sem = asyncio.Semaphore(CONCURRENCY)
    all_data: dict = {}
    failed: list[str] = []

    async with httpx.AsyncClient() as client:
        symbols = list(ISIN_MAP.keys())
        batch   = 50

        for i in range(0, len(symbols), batch):
            chunk_syms = symbols[i : i + batch]
            tasks = [
                fetch_ohlc(client, sem, sym, ISIN_MAP[sym], start, today)
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

        log.info(f"✓ {len(all_data)} loaded  ✗ {len(failed)} failed")
        if failed:
            log.warning(f"  Failed stocks: {failed[:30]}")
            (HERE / "failed_stocks.txt").write_text("\n".join(failed))

        apply_rolling_window(all_data, cutoff)

        log.info("Uploading to R2…")
        await asyncio.gather(
            upload_all_chunks(client, all_data, today),
            r2_upload(client, "ohlc_all.json",
                      json.dumps({"updated": today, "stocks": all_data})),
        )

    log.info("━━━ Full load complete ━━━")
    log.info("Next: set up GitHub Actions workflows for daily automation.")


async def run_status() -> None:
    """Print R2 chunk summary."""
    async with httpx.AsyncClient() as client:
        tasks = [r2_download(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
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
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    match mode:
        case "daily":  asyncio.run(run_daily())
        case "today":  asyncio.run(run_today())
        case "full":   asyncio.run(run_full())
        case "status": asyncio.run(run_status())
        case _:
            print(__doc__)
            sys.exit(1)
