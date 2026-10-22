"""
pipeline_us.py
US Stocks OHLC pipeline (Yahoo Finance via yfinance) for tradewithtech.in

Symbols: everything in us_universe.json (S&P 500 + Nasdaq-100 + Dow 30 + a few ETFs, ~520 unique),
built by pipeline_us_meta.py from Wikipedia constituent lists. See us_universe.py.

R2 files written:
  us_ohlc_<SYMBOL>.json   -> {symbol, updated, company{...}, indices[...], bars[{date,o,h,l,c,v}]}
  us_market_overview.json -> historical US breadth (up to 504 sessions, ETFs excluded)
  us_breadth_state.json   -> compact per-symbol state for failed-run recovery
  us_screener_feed.json   -> {updated, count, stocks[{symbol, name, indices, exchange, sector, ..., ltp, pct_ch}]}
Read (not written): us_company_meta.json, us_universe.json

  - Prices are split/dividend ADJUSTED (no fake crashes on splits).
  - Screener feed includes daily/weekly/monthly EMA + SMA values and signals.
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
import math
from collections import deque
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



# Versioned MA contract shared with us_stocks.html. W/M include the current
# developing period, matching a chart's live weekly/monthly candle.
MA_PERIODS = {"D": (10, 21, 50, 200), "W": (10, 30, 40), "M": (10, 20)}
MA_SCHEMA_VERSION = 1


def ma_closes(bars, timeframe):
    """Deduplicate by date; last valid positive close wins; chronological output."""
    daily = {}
    for bar in bars:
        try:
            day = datetime.strptime(str(bar["date"]), "%Y-%m-%d").date()
            close = float(bar["c"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(close) and close > 0:
            daily[day] = close
    buckets = {}
    for day, close in sorted(daily.items()):
        if timeframe == "W":
            key = day.toordinal() - day.weekday()  # Monday, including year boundaries
        elif timeframe == "M":
            key = (day.year, day.month)
        else:
            key = day
        buckets[key] = close
    return list(buckets.values())


def ma_series(closes, period, kind):
    """SMA-seeded EMA; None until a full period exists. O(number of bars)."""
    out, window, total, ema = [], deque(), 0.0, None
    alpha = 2.0 / (period + 1)
    for close in closes:
        window.append(close)
        total += close
        if len(window) > period:
            total -= window.popleft()
        if len(window) < period:
            out.append(None)
            continue
        if kind == "SMA":
            value = total / period
        else:
            ema = total / period if ema is None else close * alpha + ema * (1 - alpha)
            value = ema
        out.append(value)
    return out


def compute_ma(bars):
    """Current/previous MA, signed distance %, rising bars, fresh price cross.

    Null means insufficient history. Signal comparisons use unrounded values.
    Rising days on W/M mean weekly/monthly bars, including developing candles.
    """
    result = {"version": MA_SCHEMA_VERSION, "asof": max((str(b.get("date", "")) for b in bars), default=""),
              "includes_developing_period": True, "timeframes": {}}
    for tf, periods in MA_PERIODS.items():
        closes = ma_closes(bars, tf)
        block = {"bars": len(closes), "close": closes[-1] if closes else None}
        for kind in ("EMA", "SMA"):
            values = {}
            for period in periods:
                series = ma_series(closes, period, kind)
                current = series[-1] if series else None
                previous = series[-2] if len(series) > 1 else None
                rising = 0
                if current is not None:
                    for i in range(len(series) - 1, 0, -1):
                        if series[i - 1] is None or series[i] <= series[i - 1]:
                            break
                        rising += 1
                values[str(period)] = {
                    "value": round(current, 8) if current is not None else None,
                    "previous": round(previous, 8) if previous is not None else None,
                    "distance_pct": round((closes[-1] / current - 1) * 100, 6) if current else None,
                    "above": closes[-1] > current if current is not None else None,
                    "rising_bars": rising if current is not None else None,
                    "price_cross_up": (closes[-1] > current and closes[-2] <= previous)
                                      if previous is not None else None,
                }
            crosses = {}
            pairs = list(zip(periods, periods[1:]))
            if tf == "D":
                pairs.append((50, 200))
            for fast, slow in dict.fromkeys(pairs):
                f, sl = ma_series(closes, fast, kind), ma_series(closes, slow, kind)
                crosses[f"{fast}_{slow}"] = (f[-1] > sl[-1] and f[-2] <= sl[-2]) if len(sl) > 1 and sl[-2] is not None else None
            block[kind + "_cross_up"] = crosses
            block[kind] = values
        result["timeframes"][tf] = block
    return result



TECH_VERSION = 1


def technical_bars(bars, tf):
    """Aggregate valid daily OHLCV; incomplete volume is unknown, never zero-filled."""
    daily = {}
    for b in bars:
        try:
            day = datetime.strptime(str(b["date"]), "%Y-%m-%d").date()
            o, h, l, c = (float(b[k]) for k in ("o", "h", "l", "c"))
            if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)) or h < max(o, c, l) or l > min(o, c):
                continue
            v = b.get("v")
            v = float(v) if v is not None else None
            if v is not None and (not math.isfinite(v) or v < 0):
                v = None
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        daily[day] = {"date": day.isoformat(), "o": o, "h": h, "l": l, "c": c, "v": v}
    grouped = {}
    for day, b in sorted(daily.items()):
        key = day if tf == "D" else day.toordinal() - day.weekday() if tf == "W" else (day.year, day.month)
        if key not in grouped:
            grouped[key] = dict(b)
        else:
            g = grouped[key]
            g.update(h=max(g["h"], b["h"]), l=min(g["l"], b["l"]), c=b["c"], date=b["date"],
                     v=g["v"] + b["v"] if g["v"] is not None and b["v"] is not None else None)
    return list(grouped.values())


def wilder_last(values, period):
    if len(values) < period:
        return None
    value = sum(values[:period]) / period
    for x in values[period:]:
        value = (value * (period - 1) + x) / period
    return value


def technical_snapshot(bars):
    """Period-bar metrics; volume baselines exclude the current bar.

    NR7 requires a strictly smaller range. MCP = last three bars inside
    bar[-4]. Breakouts compare today's close with prior completed highs.
    Tightness = (highest high / lowest low - 1) * 100 over 3/5 bars.
    """
    if not bars:
        return {}
    c = [b["c"] for b in bars]
    last, previous = bars[-1], bars[-2] if len(bars) > 1 else None
    out = {}
    for n in (20, 50):
        vols = [b["v"] for b in bars[-n-1:-1]]
        avg = sum(vols) / n if len(vols) == n and all(v is not None for v in vols) else None
        out[f"avg_volume{n}"] = avg
        out[f"rvol{n}"] = last["v"] / avg if avg and last["v"] is not None else None
    # ATR includes the first bar's own range, then true ranges; Wilder smoothing.
    tr = [max(b["h"] - b["l"], abs(b["h"] - bars[i-1]["c"]), abs(b["l"] - bars[i-1]["c"]))
          if i else b["h"] - b["l"] for i, b in enumerate(bars)]
    atr = wilder_last(tr, 14)
    out["atr14"] = atr
    out["atr_pct"] = atr / c[-1] * 100 if atr is not None else None
    mean = sum(c[-20:]) / 20 if len(c) >= 20 else None
    std = math.sqrt(sum((v - mean) ** 2 for v in c[-20:]) / 20) if mean else None
    out["bbw_pct"] = 4 * std / mean * 100 if mean else None
    changes = [b - a for a, b in zip(c, c[1:])]
    gain = wilder_last([max(x, 0) for x in changes], 14)
    loss = wilder_last([max(-x, 0) for x in changes], 14)
    out["rsi14"] = None if gain is None else (50 if gain == 0 and loss == 0 else 100 if loss == 0 else 100 - 100 / (1 + gain / loss))
    out["mswing"] = ((c[-1] / c[-21] - 1) / 20 + (c[-1] / c[-51] - 1) / 50) if len(c) > 50 else None
    for n in (3, 5):
        out[f"tight{n}_pct"] = (max(b["h"] for b in bars[-n:]) / min(b["l"] for b in bars[-n:]) - 1) * 100 if len(bars) >= n else None
    out["ib"] = last["h"] <= previous["h"] and last["l"] >= previous["l"] if previous else None
    out["nr7"] = tr[-1] is not None and (last["h"] - last["l"]) < min(b["h"] - b["l"] for b in bars[-7:-1]) if len(bars) >= 7 else None
    out["mcp"] = all(b["h"] <= bars[-4]["h"] and b["l"] >= bars[-4]["l"] for b in bars[-3:]) if len(bars) >= 4 else None
    out["gap_pct"] = (last["o"] / previous["c"] - 1) * 100 if previous else None
    out["gap_up"] = last["o"] > previous["h"] if previous else None
    out["gap_down"] = last["o"] < previous["l"] if previous else None
    for n in (20, 50, 252):
        prior = bars[-n-1:-1]
        out[f"breakout{n}"] = last["c"] > max(b["h"] for b in prior) if len(prior) == n else None
    return out


def compute_technical(bars):
    daily = technical_bars(bars, "D")
    result = {"version": TECH_VERSION, "includes_developing_period": True,
              "timeframes": {tf: technical_snapshot(technical_bars(bars, tf)) for tf in ("D", "W", "M")}}
    # A full 252-session window is required for a 52-week high/low field.
    window = daily[-252:]
    high = max(b["h"] for b in window) if len(window) == 252 else None
    low = min(b["l"] for b in window) if len(window) == 252 else None
    close = daily[-1]["c"] if daily else None
    result.update(high52=high, low52=low, below_high52_pct=(1 - close / high) * 100 if high else None,
                  above_low52_pct=(close / low - 1) * 100 if low else None,
                  asof=daily[-1]["date"] if daily else None)
    return result


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



def market_history(bars):
    """Historical breadth inputs for the current tracked universe, stocks only.

    Each date uses only history through that date. Keep the latest 504 sessions;
    per-MA eligible counts exclude securities lacking enough history.
    """
    daily = technical_bars(bars, "D")
    closes = [b["c"] for b in daily]
    series = {f"{kind.lower()}{n}": ma_series(closes, n, kind)
              for kind in ("EMA", "SMA") for n in (10, 20, 21, 40, 50, 200)}
    rows = []
    for i in range(max(0, len(daily) - 504), len(daily)):
        b = daily[i]
        r = {"date": b["date"], "advance": int(i > 0 and closes[i] > closes[i-1]),
             "decline": int(i > 0 and closes[i] < closes[i-1]), "ad_eligible": int(i > 0)}
        for key, values in series.items():
            r[key] = None if values[i] is None else int(closes[i] > values[i])
        window = daily[i-251:i+1] if i >= 251 else []
        r["high52"] = int(b["h"] >= max(x["h"] for x in window)) if window else None
        r["low52"] = int(b["l"] <= min(x["l"] for x in window)) if window else None
        r["mswing"] = ((closes[i] / closes[i-20] - 1) / 20 + (closes[i] / closes[i-50] - 1) / 50) if i >= 50 else None
        rows.append(r)
    return rows


def aggregate_breadth(histories):
    dates = {}
    keys = [f"{kind}{n}" for kind in ("ema", "sma") for n in (10, 20, 21, 40, 50, 200)] + ["high52", "low52"]
    for hist in histories.values():
        for row in hist:
            r = dates.setdefault(row["date"], {"date": row["date"], "total": 0, "advance": 0, "decline": 0,
                      "ad_eligible": 0, "mswing_sum": 0, "mswing_eligible": 0,
                      **{k: 0 for k in keys}, **{k + "_eligible": 0 for k in keys}})
            r["total"] += 1
            for key in ("advance", "decline", "ad_eligible"):
                r[key] += row[key]
            for key in keys:
                if row[key] is not None:
                    r[key] += row[key]
                    r[key + "_eligible"] += 1
            if row["mswing"] is not None:
                r["mswing_sum"] += row["mswing"]
                r["mswing_eligible"] += 1
    out, ad_line = [], 0
    for date in sorted(dates):
        r = dates[date]
        r["unchanged"] = r["ad_eligible"] - r["advance"] - r["decline"]
        r["net_ad"] = r["advance"] - r["decline"]
        ad_line += r["net_ad"]
        r["ad_line"] = ad_line
        r["net_high_low"] = r["high52"] - r["low52"]
        r["mswing"] = r.pop("mswing_sum") / r["mswing_eligible"] if r["mswing_eligible"] else None
        for key in keys:
            r[key + "_pct"] = r[key] / r[key + "_eligible"] * 100 if r[key + "_eligible"] else None
        out.append(r)
    return out[-504:]


async def publish_market_data(client, latest_bars, rows):
    # Existing rows are retained only for failed symbols, never assigned today's date.
    previous = await r2_download(client, "us_breadth_state.json") or {}
    histories = dict(previous.get("histories", {}))
    fields = previous.get("fields")
    if fields:
        histories = {sym: [dict(zip(fields, row)) for row in history] for sym, history in histories.items()}
    tracked = {s for s in US_SYMBOLS if UNIVERSE.get(s, {}).get("indices") != ["ETF"]}
    histories = {s: h for s, h in histories.items() if s in tracked}
    for sym, info in latest_bars.items():
        if sym in tracked:
            histories[sym] = info["market_history"]
    payload = {"version": 1, "updated": now_iso(), "universe": "Current tracked US stocks; excludes ETFs",
               "method": "Current-universe historical breadth, not point-in-time index membership",
               "tracked": len(tracked), "covered": len(histories),
               "history": aggregate_breadth(histories)}
    # Keep compact symbol histories privately in a second file for failed-run recovery.
    fields = list(next((row for hist in histories.values() for row in hist), {}).keys())
    packed = {sym: [[row.get(k) for k in fields] for row in hist] for sym, hist in histories.items()}
    await r2_upload(client, "us_breadth_state.json", {"version": 1, "fields": fields, "histories": packed})
    await r2_upload(client, "us_market_overview.json", payload)


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
            "themes": m.get("themes") if isinstance(m.get("themes"), list) else [],
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
            "ma": info["ma"],
            "technical": info["technical"],
        })

    # RS rating: rank every row's score (fresh rows + kept-over rows from failed symbols)
    stock_syms = {r["symbol"] for r in rows if r.get("indices") and r["indices"] != ["ETF"]}
    ratings = rs_ratings({r["symbol"]: r.get("rs_score") for r in rows}, stock_syms)
    for r in rows:
        r["rs_rating"] = ratings.get(r["symbol"])

    if not rows:
        log.warning("  screener feed: nothing to write, skipping")
        return
    await r2_upload(r2_client, "us_screener_feed.json", {"updated": now_iso(), "schema_version": 5, "ma_version": MA_SCHEMA_VERSION, "technical_version": TECH_VERSION, "count": len(rows), "stocks": rows})
    log.info(f"  screener feed: uploaded ({len(rows)} symbols)")
    await publish_market_data(r2_client, latest_bars, rows)
    log.info("  US market breadth: uploaded historical overview")


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
                                "prev": bars[-2] if len(bars) > 1 else None, "perf": compute_perf(bars), "ma": compute_ma(bars), "technical": compute_technical(bars), "market_history": market_history(bars)}
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
                                    "perf": compute_perf(bars), "ma": compute_ma(bars), "technical": compute_technical(bars), "market_history": market_history(bars)}
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
