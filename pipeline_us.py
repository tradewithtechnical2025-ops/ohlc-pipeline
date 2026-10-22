"""
pipeline_us.py
US Stocks OHLC pipeline (Yahoo Finance via yfinance) for tradewithtech.in

Symbols: everything in us_universe.json (S&P 500 + Nasdaq-100 + Dow 30 + a few ETFs, ~520 unique),
built by pipeline_us_meta.py from Wikipedia constituent lists. See us_universe.py.

R2 files written:
  us_ohlc_<SYMBOL>.json   -> {symbol, updated, company{...}, indices[...], bars[{date,o,h,l,c,v}]}
  us_screener_feed.json   -> {updated, count, stocks[{symbol, name, indices, exchange, sector, ..., ltp, pct_ch}]}
Read (not written): us_company_meta.json, us_universe.json

  - Prices are split/dividend ADJUSTED (no fake crashes on splits).
  - Daily mode pulls the last 10 days and merges, so a missed run never leaves a gap.
  - A symbol newly added to an index gets its full history automatically on the next daily run.

Env vars (GitHub Actions secrets): WORKER_URL, WORKER_TOKEN

Run modes:
  python pipeline_us.py --mode backfill   # 3y history for all symbols, overwrites per-symbol files
  python pipeline_us.py --mode daily      # last 10 days merged; backfills symbols with no file yet
"""

import os
import sys
import json
import time
import asyncio
import warnings
import argparse
import logging
from datetime import datetime, timezone

import httpx
import pandas as pd
import yfinance as yf

import us_universe

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# yfinance still uses pandas' deprecated inplace= calls; harmless, but floods the Actions log
warnings.filterwarnings("ignore", message=r".*inplace keyword.*")
warnings.filterwarnings("ignore", category=FutureWarning, module=r"yfinance.*")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"yfinance.*")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

WORKER_URL     = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN   = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

BACKFILL_PERIOD    = "3y"
DAILY_PERIOD       = "10d"   # overlap window: heals gaps from missed runs
YAHOO_CHUNK        = 100     # symbols per yf.download call
RETRY              = 5
UPLOAD_CONCURRENCY = 10      # parallel Worker requests
LOG_EVERY          = 50      # progress log interval (avoid 500 log lines)

# Filled at runtime from us_universe.json
US_SYMBOLS: list[str] = []
UNIVERSE: dict = {}          # symbol -> {"name", "indices"}
UNIVERSE_INDICES: dict = {}  # index key -> {"name", "count", "source"}

EXCHANGE_NAMES = {
    "NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ", "NAS": "NASDAQ",
    "NYQ": "NYSE", "NYS": "NYSE", "ASE": "NYSE American", "PCX": "NYSE Arca",
    "BTS": "Cboe BZX", "CXI": "Cboe BZX",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def r2_filename(symbol: str) -> str:
    return f"us_ohlc_{symbol.replace('.', '_')}.json"


# ---------------------------------------------------------------------------
# Worker upload / download (unchanged pattern from pipeline_nse.py)
# ---------------------------------------------------------------------------

async def r2_upload(client: httpx.AsyncClient, filename: str, data):
    if not isinstance(data, (bytes, str)):
        data = json.dumps(data, separators=(",", ":"))
    if isinstance(data, str):
        data = data.encode()
    url = f"{WORKER_URL}?file={filename}"
    last_err = None
    for attempt in range(RETRY):
        try:
            r = await client.post(url, headers={**WORKER_HEADERS, "Content-Type": "application/json"}, content=data, timeout=90)
        except httpx.RequestError as e:
            last_err = e
            log.warning(f"  Upload {filename}: {e}, retry {attempt + 1}")
            await asyncio.sleep(2 ** attempt)
            continue
        if r.status_code == 200:
            return
        last_err = RuntimeError(f"Upload failed: HTTP {r.status_code}")
        log.warning(f"  Upload {filename}: HTTP {r.status_code}, retry {attempt + 1}")
        await asyncio.sleep(2 ** attempt)
    raise last_err


async def r2_download(client: httpx.AsyncClient, filename: str):
    url = f"{WORKER_URL}/{filename}"
    last_err = None
    for attempt in range(RETRY):
        try:
            r = await client.get(url, headers=WORKER_HEADERS, timeout=90)
        except httpx.RequestError as e:
            last_err = e
            log.warning(f"  Download {filename}: {e}, retry {attempt + 1}")
            await asyncio.sleep(2 ** attempt)
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 200:
            return r.json()
        last_err = RuntimeError(f"Download failed: HTTP {r.status_code}")
        log.warning(f"  Download {filename}: HTTP {r.status_code}, retry {attempt + 1}")
        await asyncio.sleep(2 ** attempt)
    raise last_err


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------

async def init_universe(client, rebuild=False):
    """Load us_universe.json (built daily by the meta job); build it if missing or rebuild=True."""
    uni = {} if rebuild else await us_universe.load_universe(client, r2_download)
    if not uni.get("symbols"):
        log.info("  building universe from Wikipedia")
        uni = await us_universe.build_universe(client, r2_download, r2_upload, now_iso)
    UNIVERSE.clear()
    UNIVERSE.update(uni["symbols"])
    US_SYMBOLS[:] = list(UNIVERSE)
    UNIVERSE_INDICES.clear()
    UNIVERSE_INDICES.update(uni.get("indices", {}))
    counts = ", ".join(f"{k} {v['count']}" for k, v in uni.get("indices", {}).items())
    log.info(f"Universe: {len(US_SYMBOLS)} symbols ({counts})")


# ---------------------------------------------------------------------------
# Yahoo
# ---------------------------------------------------------------------------

def _r(x, nd=4):
    return None if pd.isna(x) else round(float(x), nd)


def _download_chunk(symbols, period):
    for attempt in range(3):
        try:
            data = yf.download(symbols, period=period, interval="1d", group_by="ticker",
                               auto_adjust=True, actions=False, threads=True, progress=False)
            if not data.empty:
                return data
        except Exception as e:
            log.warning(f"  Yahoo chunk error: {e}")
        log.warning(f"  Yahoo returned nothing for chunk, retry {attempt + 1}")
        time.sleep(10 * (attempt + 1))
    return pd.DataFrame()


def yahoo_bars(symbols, period) -> dict:
    """Batched downloads -> {symbol: [bar, ...]} (adjusted OHLC). Missing symbols are absent."""
    out = {}
    for i in range(0, len(symbols), YAHOO_CHUNK):
        chunk = symbols[i:i + YAHOO_CHUNK]
        data = _download_chunk(chunk, period)
        if data.empty:
            continue
        for sym in chunk:
            try:
                df = data[sym] if isinstance(data.columns, pd.MultiIndex) else data
            except KeyError:
                continue
            df = df.dropna(subset=["Close"])
            if df.empty:
                continue
            out[sym] = [
                {"date": idx.strftime("%Y-%m-%d"), "o": _r(row.Open), "h": _r(row.High),
                 "l": _r(row.Low), "c": _r(row.Close), "v": None if pd.isna(row.Volume) else int(row.Volume)}
                for idx, row in df.iterrows()
            ]
        log.info(f"  Yahoo {period}: {min(i + YAHOO_CHUNK, len(symbols))}/{len(symbols)} requested, {len(out)} received")
        time.sleep(1)
    return out


def merge_bars(old, new):
    by_date = {b["date"]: b for b in old}
    by_date.update({b["date"]: b for b in new})   # newer data wins (also fixes revised bars)
    return [by_date[d] for d in sorted(by_date)]


# ---------------------------------------------------------------------------
# Meta + screener
# ---------------------------------------------------------------------------

async def load_meta(client) -> dict:
    try:
        return ((await r2_download(client, "us_company_meta.json")) or {}).get("stocks", {})
    except Exception as e:
        log.warning(f"  couldn't load us_company_meta.json ({e}); sector/industry/mcap blank")
        return {}


def company_block(sym, meta_stocks) -> dict:
    m = meta_stocks.get(sym, {})
    return {
        "name": m.get("name") or UNIVERSE.get(sym, {}).get("name"),
        "exchangeCode": m.get("exchange"),
        "exchangeName": m.get("exchangeName"),
        "description": m.get("description"),
        "startDate": m.get("startDate"),
    }


# Trading-day lookbacks (approx: 5/week, 21/month)
PERF_PERIODS = {"1w": 5, "1m": 21, "3m": 63, "6m": 126, "9m": 189, "12m": 252}
# IBD-style RS score weights: most recent quarter counts double
RS_WEIGHTS = {"3m": 0.4, "6m": 0.2, "9m": 0.2, "12m": 0.2}


def compute_perf(bars) -> dict:
    """Returns % change over each period + raw RS score (None where history is too short)."""
    closes = [b["c"] for b in bars if b.get("c")]
    out = {}
    for k, n in PERF_PERIODS.items():
        out[k] = round((closes[-1] / closes[-1 - n] - 1) * 100, 2) if len(closes) > n else None
    rs_parts = [out[k] for k in RS_WEIGHTS]
    out["rs_score"] = (round(sum(RS_WEIGHTS[k] * out[k] for k in RS_WEIGHTS), 4)
                       if None not in rs_parts else None)
    return out


def rs_ratings(scores: dict, stock_syms: set) -> dict:
    """Percentile-rank RS scores into 1-99. Ranked among stocks only; ETFs are rated against the same stock distribution."""
    import bisect
    pool = sorted(v for s, v in scores.items() if v is not None and s in stock_syms)
    if not pool:
        return {}
    n = len(pool)
    out = {}
    for sym, v in scores.items():
        if v is None:
            continue
        pct = bisect.bisect_left(pool, v) / max(n - 1, 1)     # 0..1, share of stocks this one beats
        out[sym] = max(1, min(99, int(pct * 98) + 1))
    return out


async def generate_screener_feed(r2_client, latest_bars, meta_stocks):
    try:
        prev_feed = await r2_download(r2_client, "us_screener_feed.json") or {}
    except Exception:
        prev_feed = {}
    prev_rows = {r["symbol"]: r for r in prev_feed.get("stocks", [])}

    rows = []
    for symbol in US_SYMBOLS:
        indices = UNIVERSE.get(symbol, {}).get("indices", [])
        info = latest_bars.get(symbol)
        if not info:
            if symbol in prev_rows:          # failed this run: keep yesterday's row
                rows.append({**prev_rows[symbol], "indices": indices})
            continue
        last, prev = info["last"], info["prev"]
        change_pct = ((last["c"] - prev["c"]) / prev["c"]) * 100 if prev and prev.get("c") else None
        m = meta_stocks.get(symbol, {})
        rows.append({
            "symbol": symbol,
            "name": m.get("name") or info.get("name") or UNIVERSE.get(symbol, {}).get("name") or symbol,
            "indices": indices,
            "exchange": m.get("exchangeName"),
            "sector": m.get("sector"),
            "industry": m.get("industry"),
            "mcap": m.get("marketCap"),
            "ltp": last["c"],
            "pct_ch": None if change_pct is None else round(change_pct, 4),
            "volume": last.get("v"),
            "date": last["date"],
            **{f"ch_{k}": info["perf"][k] for k in PERF_PERIODS},
            "rs_score": info["perf"]["rs_score"],
            "rs_rating": None,      # filled below once all scores are known
        })

    # RS rating: rank every row's score (fresh rows + kept-over rows from failed symbols)
    stock_syms = {r["symbol"] for r in rows if r.get("indices") and r["indices"] != ["ETF"]}
    ratings = rs_ratings({r["symbol"]: r.get("rs_score") for r in rows}, stock_syms)
    for r in rows:
        r["rs_rating"] = ratings.get(r["symbol"])

    if not rows:
        log.warning("  screener feed: nothing to write, skipping")
        return
    await r2_upload(r2_client, "us_screener_feed.json", {"updated": now_iso(), "count": len(rows), "stocks": rows})
    log.info(f"  screener feed: uploaded ({len(rows)} symbols)")


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

async def _gather_limited(coros, limit):
    sem = asyncio.Semaphore(limit)
    async def run(c):
        async with sem:
            return await c
    return await asyncio.gather(*(run(c) for c in coros))


async def _upload_full(r2_client, symbols, all_bars, meta_stocks, stats, latest_bars):
    """Write complete per-symbol files (used by backfill and for brand-new symbols in daily)."""
    done = [0]

    async def upload(sym):
        bars = all_bars[sym]
        company = company_block(sym, meta_stocks)
        try:
            await r2_upload(r2_client, r2_filename(sym), {
                "symbol": sym, "updated": now_iso(), "company": company,
                "indices": UNIVERSE.get(sym, {}).get("indices", []), "bars": bars,
            })
            stats["ok"] += 1
            latest_bars[sym] = {"name": company.get("name"), "last": bars[-1],
                                "prev": bars[-2] if len(bars) > 1 else None, "perf": compute_perf(bars)}
        except Exception as e:
            log.error(f"  {sym}: {e}")
            stats["failed"].append(sym)
        done[0] += 1
        if done[0] % LOG_EVERY == 0 or done[0] == len(symbols):
            log.info(f"  uploaded {done[0]}/{len(symbols)}")

    await _gather_limited([upload(s) for s in symbols], UPLOAD_CONCURRENCY)


async def run_backfill():
    stats = {"ok": 0, "failed": []}
    latest_bars = {}
    async with httpx.AsyncClient() as r2_client:
        await init_universe(r2_client)
        log.info(f"Backfill: {BACKFILL_PERIOD} for {len(US_SYMBOLS)} symbols")
        all_bars = await asyncio.to_thread(yahoo_bars, US_SYMBOLS, BACKFILL_PERIOD)
        stats["failed"] = [s for s in US_SYMBOLS if s not in all_bars]
        meta_stocks = await load_meta(r2_client)
        await _upload_full(r2_client, list(all_bars), all_bars, meta_stocks, stats, latest_bars)
        await generate_screener_feed(r2_client, latest_bars, meta_stocks)
    return stats


async def run_daily():
    stats = {"ok": 0, "failed": []}
    latest_bars = {}
    async with httpx.AsyncClient() as r2_client:
        await init_universe(r2_client)
        log.info(f"Daily update: last {DAILY_PERIOD} for {len(US_SYMBOLS)} symbols")
        all_bars = await asyncio.to_thread(yahoo_bars, US_SYMBOLS, DAILY_PERIOD)
        stats["failed"] = [s for s in US_SYMBOLS if s not in all_bars]
        meta_stocks = await load_meta(r2_client)
        missing_files, done = [], [0]

        async def update(sym):
            try:
                existing = await r2_download(r2_client, r2_filename(sym))
                if existing is None:
                    missing_files.append(sym)
                    return
                bars = merge_bars(existing.get("bars", []), all_bars[sym])
                existing["bars"] = bars
                existing["updated"] = now_iso()
                existing["indices"] = UNIVERSE.get(sym, {}).get("indices", [])
                if sym in meta_stocks:      # keep company block fresh as meta improves
                    existing["company"] = {**(existing.get("company") or {}),
                                           **{k: v for k, v in company_block(sym, meta_stocks).items() if v}}
                await r2_upload(r2_client, r2_filename(sym), existing)
                stats["ok"] += 1
                latest_bars[sym] = {"name": (existing.get("company") or {}).get("name"),
                                    "last": bars[-1], "prev": bars[-2] if len(bars) > 1 else None,
                                    "perf": compute_perf(bars)}
            except Exception as e:
                log.error(f"  {sym}: {e}")
                stats["failed"].append(sym)
            done[0] += 1
            if done[0] % LOG_EVERY == 0:
                log.info(f"  updated {done[0]}/{len(all_bars)}")

        await _gather_limited([update(s) for s in all_bars], UPLOAD_CONCURRENCY)

        # New index members (or first run): pull full history for them
        if missing_files:
            names = sorted(missing_files)
            more = f" (+{len(names) - 20} more)" if len(names) > 20 else ""
            log.info(f"  {len(names)} symbols have no file yet, backfilling: {', '.join(names[:20])}{more}")
            new_bars = await asyncio.to_thread(yahoo_bars, missing_files, BACKFILL_PERIOD)
            stats["failed"] += [s for s in missing_files if s not in new_bars]
            await _upload_full(r2_client, list(new_bars), new_bars, meta_stocks, stats, latest_bars)

        await generate_screener_feed(r2_client, latest_bars, meta_stocks)
    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "daily"], required=True)
    args = parser.parse_args()

    stats = asyncio.run({"backfill": run_backfill, "daily": run_daily}[args.mode]())
    log.info(f"{args.mode} done: {stats['ok']} ok, {len(stats['failed'])} failed")
    if stats["failed"]:
        log.info("Failed symbols: " + ", ".join(sorted(stats["failed"])))
    # Fail the GitHub Action (so you get an email) if a big chunk failed
    if not US_SYMBOLS or len(stats["failed"]) > len(US_SYMBOLS) * 0.2:
        sys.exit(1)


if __name__ == "__main__":
    main()
