#!/usr/bin/env python3

import asyncio
import json
import os
import random
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from r2_manifest import upload_with_manifest

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

# ─────────────────────────────────────────────
# Upstox — second source for BSE index history. Finedge stopped updating
# several BSE indices (12 froze on 2026-08-14) and has holes (e.g. 12–27 Jan
# 2026 for most BSE indices). Upstox candles fill every date Finedge is
# missing; on dates both have, Finedge's candle is kept. Without
# UPSTOX_TOKEN the pipeline runs exactly as before (Finedge only).
# ─────────────────────────────────────────────
UPSTOX_TOKEN   = os.environ.get("UPSTOX_TOKEN", "").strip()
UPSTOX_BASE    = "https://api.upstox.com/v2"
UPSTOX_BSE_BOD = "https://assets.upstox.com/market-quote/instruments/exchange/BSE.json.gz"
UPSTOX_CONCURRENCY = 3
# Finedge symbol → Upstox instrument_key, for any index the name match misses
# (the run log lists unmatched ones). Example: "SNSXFOCIT": "BSE_INDEX|FOCUSED IT"
UPSTOX_INDEX_OVERRIDES: dict = {
    # from Upstox's BSE instrument file (segment BSE_INDEX), Oct 2026
    "SNSXBSEFMCG": "BSE_INDEX|BSEFMC",
    "SNSXBSEIT":   "BSE_INDEX|BSEIT",
    "SNSXCAPINS":  "BSE_INDEX|CAPINS",
    "SNSXCONDIS":  "BSE_INDEX|CONDIS",
    "SNSXFINSER":  "BSE_INDEX|FINSER",
    "SNSXINFRA":   "BSE_INDEX|INFRA",
    "SNSXMFG":     "BSE_INDEX|MFG",
    "SNSXPRECON":  "BSE_INDEX|PRECON",
    # Not on Upstox at all: SNSXSMLCAP, SNSXMIDCAP, SNSXLRGCAP, SNSXNXT500,
    # SNSXNXT250, SNSXBSM250, SNSXCAPMKT, SNSXSPBSAIP / HOIP / LMIP / MSIP
}

# How many index-history requests to run in parallel. Sequential (1) was
# safe but slow. 8 was tried and caused Finedge to throw 503s on nearly
# every request (44% failure rate even with retries) — their server
# clearly can't handle that concurrency. Keep this low; raise cautiously
# and only after confirming failure rate stays near zero.
HISTORY_CONCURRENCY = 3

# Default historical lookback for every index (3 years, matches the stock
# pipeline's ROLLING_DAYS). EXTENDED_HISTORY_DAYS is only for a symbol that
# ever needs a lookback LONGER than this baseline — no other code changes
# required to add one.
DEFAULT_HISTORY_DAYS = 365 * 3

EXTENDED_HISTORY_DAYS = {
}

# ─────────────────────────────────────────────
# Trading calendar — the daily-feed candle is only merged on a real NSE
# trading day. On a weekend / holiday the daily feed still returns the LAST
# session's prices, and stamping them with today's date added a duplicate
# candle. Same nse_holidays.json the stock pipeline (pipeline.py) uses.
# ─────────────────────────────────────────────
HERE = Path(__file__).parent
try:
    with open(HERE / "nse_holidays.json") as f:
        NSE_HOLIDAYS = set(json.load(f))
except Exception as e:
    NSE_HOLIDAYS = set()
    print(f"⚠ nse_holidays.json not loaded ({e}) — only weekends are treated as non-trading days")


def today_ist() -> str:
    return datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()


def is_trading_day(d: str) -> bool:
    return date.fromisoformat(d).weekday() < 5 and d not in NSE_HOLIDAYS


# ─────────────────────────────────────────────
# Remove wrong candles — dates to drop from every index's history before
# upload. Pass them on the command line or via the DROP_DATES env var
# (handy in GitHub Actions), comma-separated:
#   python pipeline_indices.py --drop-date 2026-10-02
#   DROP_DATES=2026-10-02,2026-10-03 python pipeline_indices.py
# ─────────────────────────────────────────────
def _parse_drop_dates():
    raw = [os.environ.get("DROP_DATES", "")]
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--drop-date" and i + 1 < len(args):
            raw.append(args[i + 1])
        elif a.startswith("--drop-date="):
            raw.append(a.split("=", 1)[1])
    out = set()
    for chunk in raw:
        for d in chunk.split(","):
            d = d.strip()
            if not d:
                continue
            try:
                out.add(date.fromisoformat(d).isoformat())
            except ValueError:
                print(f"⚠ Ignoring invalid --drop-date value: {d!r} (use YYYY-MM-DD)")
    return out


DROP_DATES = _parse_drop_dates()

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}


# ─────────────────────────────────────────────
# Retry helper — Finedge occasionally throws transient
# 5xx / connection errors (e.g. 503 Service Temporarily
# Unavailable). A single blip shouldn't abort the whole
# pipeline run, so every Finedge GET goes through this.
# ─────────────────────────────────────────────

async def fetch_with_retry(client, url, params, *, retries=3, base_delay=2, timeout=300):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            r = await client.get(url, params=params, timeout=timeout)
            if r.status_code >= 500:
                raise httpx.HTTPStatusError(
                    f"Server error '{r.status_code}' for url '{r.request.url}'",
                    request=r.request, response=r,
                )
            r.raise_for_status()
            return r
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            last_exc = e
            if attempt < retries:
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)
                print(f"  ⚠ {url.split('/')[-1]} attempt {attempt}/{retries} failed "
                      f"({e.__class__.__name__}: {e}) — retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
            else:
                print(f"  ✗ {url.split('/')[-1]} — all {retries} attempts failed")
    raise last_exc


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def normalize_index_symbol(v):
    v = str(v).upper().strip()
    REPLACE = {
        "NIF50": "NIFTY50",
        "NIFTY 50": "NIFTY50",
        "NIFTY BANK": "NIFTYBANK",
        "NIFTY IT": "NIFTYIT",
        "NIFTY AUTO": "NIFTYAUTO",
        "NIFTY FMCG": "NIFTYFMCG",
        "NIFTY PHARMA": "NIFTYPHARMA",
        "NIFTY REALTY": "NIFTYREALTY",
        "NIFTY METAL": "NIFTYMETAL",
    }
    return REPLACE.get(v, v.replace(" ", ""))


BAD_KEYWORDS = [
    "2X", "1X", "INV", "LEV", "TRI", "EQW", "EQUAL",
    "LOWVOL", "ALPHA", "QUALITY", "MOM", "MOMENTUM",
    "ESG", "VOL", "MULT", "QUA", "VALUE", "SHODUR",
    "ENH", "30T", "50T", "LIQ", "VAR", "BETA",
    "SHA", "TMC", "FPI", "EMERGE", "RURAL", "WAVES",
]

BAD_TYPES = {"Strategy", "Strategy Indices", "Volatility"}

MANUAL_BLACKLIST = {
    "NIFFINSER255", "NIFFINSEREXB", "NIFHEA2", "NIFINDCORGRO4",
    "NIFMIDFINSER", "NIFMIDHEA", "NIFMIDITTEL", "SNSXBBGEFS",
    "SNSXBSE30", "SNSXIND150", "SNSXINSLDR",
}


# ─────────────────────────────────────────────
# Index Map — Frontend ke liye (categorized)
# ─────────────────────────────────────────────

INDEX_CATEGORIES = {
    "NIFTY50"    : ("major",    "Nifty 50"),
    "NIFNEX50"   : ("major",    "Nifty Next 50"),
    "NIF100"     : ("major",    "Nifty 100"),
    "NIF200"     : ("major",    "Nifty 200"),
    "NIF500"     : ("major",    "Nifty 500"),
    "NIFMID50"   : ("major",    "Nifty Midcap 50"),
    "NIFMID100"  : ("major",    "Nifty Midcap 100"),
    "NIFMID150"  : ("major",    "Nifty Midcap 150"),
    "NIFMID400"  : ("major",    "Nifty MidSmallcap 400"),
    "NIFLAR250"  : ("major",    "Nifty LargeMidcap 250"),
    "NIFSMA50"   : ("major",    "Nifty Smallcap 50"),
    "NIFSMA100"  : ("major",    "Nifty Smallcap 100"),
    "NIFSMA250"  : ("major",    "Nifty Smallcap 250"),
    "NIFMIC250"  : ("major",    "Nifty Microcap 250"),
    "NIFTOTMAR"  : ("major",    "Nifty Total Market"),
    "NIFIPO"     : ("major",    "Nifty IPO"),
    "NIFMIDSEL2" : ("major",    "Nifty Midcap Select"),
    "NIFBAN"     : ("sectoral", "Nifty Bank"),
    "NIFPRIBAN"  : ("sectoral", "Nifty Private Bank"),
    "NIFPSUBAN"  : ("sectoral", "Nifty PSU Bank"),
    "NIFIT"      : ("sectoral", "Nifty IT"),
    "NIFAUT"     : ("sectoral", "Nifty Auto"),
    "NIFPHA"     : ("sectoral", "Nifty Pharma"),
    "NIFHEAIND"  : ("sectoral", "Nifty Healthcare"),
    "NIFFMC"     : ("sectoral", "Nifty FMCG"),
    "NIFMET"     : ("sectoral", "Nifty Metal"),
    "NIFREA"     : ("sectoral", "Nifty Realty"),
    "NIFMED"     : ("sectoral", "Nifty Media"),
    "NIFFINSER"  : ("sectoral", "Nifty Financial Services"),
    "NIFCONDUR"  : ("sectoral", "Nifty Consumer Durables"),
    "NIFCHE"     : ("sectoral", "Nifty Chemicals"),
    "NIFOILGAS"  : ("sectoral", "Nifty Oil & Gas"),
    "NIFENE"     : ("sectoral", "Nifty Energy"),
    "NIFCOM"     : ("sectoral", "Nifty Commodities"),
    "NIFINF"     : ("sectoral", "Nifty Infrastructure"),
    "NIFSERSEC"  : ("sectoral", "Nifty Services"),
    "NIFPSE"     : ("sectoral", "Nifty PSE"),
    "NIFCPS"     : ("sectoral", "Nifty CPSE"),
    "NIFMNC"     : ("sectoral", "Nifty MNC"),
    "NIFCAPMAR"  : ("sectoral", "Nifty Capital Markets"),
    "NIFTRALOG"  : ("sectoral", "Nifty Transport & Logistics"),
    "NIFMOB"     : ("sectoral", "Nifty Mobility"),
    "NIFCORHOU"  : ("sectoral", "Nifty Core Housing"),
    "NIFHOU"     : ("sectoral", "Nifty Housing"),
    "NIFINDDEF"    : ("thematic", "Nifty India Defence"),
    "NIFEVNEWAGEA" : ("thematic", "Nifty EV & New Age Auto"),
    "NIFINDDIG2"   : ("thematic", "Nifty India Digital"),
    "NIFINDINT"    : ("thematic", "Nifty India Internet"),
    "NIFINDMAN"    : ("thematic", "Nifty India Manufacturing"),
    "NIFINDCON"    : ("thematic", "Nifty India Consumption"),
    "NIFINDNEWAGE" : ("thematic", "Nifty New Age Consumption"),
    "NIFINDTOU"    : ("thematic", "Nifty India Tourism"),
    "NIFNONCYCCON" : ("thematic", "Nifty Non-Cyclical Consumer"),
    "NIFINDINFLOG" : ("thematic", "Nifty Infra & Logistics"),
    "NIFINDSEL5CO" : ("thematic", "Nifty Select 5 Corp Groups"),
    "NIFMIDINDCON" : ("thematic", "Nifty MidSmall Consumption"),
}


def build_index_map(master_parsed):
    index_map = {}
    counts = {"major": 0, "sectoral": 0, "thematic": 0}
    for symbol, meta in master_parsed.items():
        if symbol not in INDEX_CATEGORIES:
            continue
        category, label = INDEX_CATEGORIES[symbol]
        stocks = [
            c if isinstance(c, str) else c.get("symbol", "")
            for c in meta.get("constituents", [])
        ]
        stocks = [s.upper() for s in stocks if s]
        if stocks:
            index_map[symbol] = {
                "label"   : label,
                "category": category,
                "count"   : len(stocks),
                "stocks"  : stocks,
            }
            counts[category] += 1
    print(f"✓ index_map: {len(index_map)} indices "
          f"(major:{counts['major']} "
          f"sectoral:{counts['sectoral']} "
          f"thematic:{counts['thematic']})")
    return index_map


def is_bad_index(symbol, index_name):
    symbol = str(symbol).upper()
    index_name = str(index_name).upper()
    if symbol in MANUAL_BLACKLIST:
        return True
    return any(k in symbol or k in index_name for k in BAD_KEYWORDS)


# ─────────────────────────────────────────────
# R2 Upload
# ─────────────────────────────────────────────

async def r2_upload(client, filename, data):
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=300,
    )
    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed")


# ─────────────────────────────────────────────
# Index Master
# ─────────────────────────────────────────────

async def fetch_index_master(client):
    url = f"{FINEDGE_BASE}/index/master"
    params = {"token": FINEDGE_TOKEN}
    r = await fetch_with_retry(client, url, params)
    return r.json()


def parse_index_master(rows):
    output = {}
    skipped = 0
    for row in rows:
        raw_symbol = str(row.get("index_symbol", "")).strip()
        symbol = normalize_index_symbol(raw_symbol)
        if not symbol:
            skipped += 1; continue
        index_name     = str(row.get("index_name", ""))
        index_sub_type = str(row.get("index_sub_type", ""))
        constituents   = row.get("constituents") or []
        if is_bad_index(symbol, index_name):
            skipped += 1; continue
        if index_sub_type in BAD_TYPES:
            skipped += 1; continue
        if not constituents or len(constituents) < 5:
            skipped += 1; continue
        output[symbol] = {
            "api_symbol" : raw_symbol,
            "name"       : row.get("index_name"),
            "type"       : index_sub_type,
            "index_type" : row.get("index_type"),
            "exchange"   : row.get("exchange"),
            "description": row.get("description"),
            "constituents": constituents,
        }
    print(f"✓ Clean indices: {len(output)}")
    print(f"✓ Removed noisy indices: {skipped}")
    return output


# ─────────────────────────────────────────────
# Daily Feed
# ─────────────────────────────────────────────

async def fetch_index_daily(client):
    url = f"{FINEDGE_BASE}/index/market-price/daily-feed"
    params = {"token": FINEDGE_TOKEN}
    r = await fetch_with_retry(client, url, params)
    return r.json()


def parse_index_daily(rows, valid_symbols):
    output = {}
    skipped = 0
    for row in rows:
        symbol = normalize_index_symbol(row.get("index_symbol"))
        if symbol not in valid_symbols:
            skipped += 1; continue
        output[symbol] = {
            "name"         : row.get("index_name"),
            "close"        : row.get("close_price"),
            "open"         : row.get("open_price"),
            "high"         : row.get("high_price"),
            "low"          : row.get("low_price"),
            "change_pct"   : row.get("change_pct"),
            "points_change": row.get("points_change"),
            "volume"       : row.get("volume"),
            "turnover"     : row.get("turnover"),
            "market_cap"   : row.get("market_cap"),
            "pe"           : row.get("pe"),
            "pb"           : row.get("pb"),
            "div_yield"    : row.get("div_yield"),
        }
    print(f"✓ Daily feed indices: {len(output)}")
    print(f"✓ Skipped noisy daily feed: {skipped}")
    return output


# ─────────────────────────────────────────────
# Index Returns
# ─────────────────────────────────────────────

async def fetch_index_returns(client):
    url = f"{FINEDGE_BASE}/index/price-returns"
    params = {"token": FINEDGE_TOKEN}
    r = await fetch_with_retry(client, url, params)
    return r.json()


def parse_index_returns(rows, valid_symbols, weekly_map=None):
    """
    Finedge ne fix kar diya sign issue.
    3Y/5Y/7Y/10Y = CAGR → absolute convert karo.
    Structure: { "1M": {"v": 1.04, "d": "2026-05-13"}, ... }
    1W is NOT from Finedge — it's computed locally (see compute_weekly_return)
    since Finedge's price-returns API has no weekly period at all.
    """
    weekly_map = weekly_map or {}
    CAGR = {"3Y": 3, "5Y": 5, "7Y": 7, "10Y": 10}
    ASIS = {"1M", "3M", "6M", "1Y"}
    ALL  = list(ASIS) + list(CAGR)
    output = {}
    skipped = 0
    for row in rows:
        symbol = normalize_index_symbol(row.get("index_symbol"))
        if symbol not in valid_symbols:
            skipped += 1; continue
        dates = row.get("dates") or {}
        ret = {}
        for p in ALL:
            raw = row.get(p)
            if raw is None:
                continue
            if p in CAGR:
                v = round((pow(1 + raw / 100, CAGR[p]) - 1) * 100, 2)
            else:
                v = round(raw, 2)
            ret[p] = {"v": v, "d": dates.get(p) or None}
        if weekly_map.get(symbol):
            ret["1W"] = weekly_map[symbol]
        ret["last_date"] = dates.get("last_date") or None
        output[symbol] = ret
    print(f"✓ Returns indices: {len(output)}")
    print(f"✓ Skipped noisy returns: {skipped}")
    return output


# ─────────────────────────────────────────────
# Historical
# ─────────────────────────────────────────────

async def fetch_index_history_one(client, api_symbol, symbol):
    # Every symbol now gets a 3-year lookback by default (DEFAULT_HISTORY_DAYS).
    # EXTENDED_HISTORY_DAYS is only for a symbol that ever needs a lookback
    # LONGER than this baseline — currently empty since 3Y is the baseline.
    lookback_days = EXTENDED_HISTORY_DAYS.get(symbol, DEFAULT_HISTORY_DAYS)
    today     = datetime.now().date()
    from_date = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    url = f"{FINEDGE_BASE}/index/market-price/historical"
    params = {
        "index_symbol": api_symbol,
        "from_date"   : from_date,
        "to_date"     : to_date,
        "token"       : FINEDGE_TOKEN,
    }
    try:
        r = await fetch_with_retry(client, url, params, retries=4, base_delay=3)
        return r.json().get("rows") or []
    except Exception:
        return []


def parse_index_history(rows):
    if not rows:
        return []
    return [{
        "date"         : r.get("quote_date"),
        "open"         : r.get("open_price"),
        "high"         : r.get("high_price"),
        "low"          : r.get("low_price"),
        "close"        : r.get("close_price"),
        "change_pct"   : r.get("change_pct"),
        "points_change": r.get("points_change"),
        "volume"       : r.get("volume"),
        "turnover"     : r.get("turnover"),
    } for r in rows]


def build_ohlc_arrays(parsed):
    """
    Convert a symbol's parsed history (list of {date,open,high,low,close,
    volume,...} row-dicts) into parallel arrays {d,o,h,l,c,v} — the exact
    shape the frontend's stock ohlc_N.json chunks already use (see
    index.html's mergeStocksIntoCache). Feeding index_ohlc.json in this
    same shape means the frontend reuses its existing merge code unchanged
    for indices too.
    """
    return {
        "d": [r.get("date") for r in parsed],
        "o": [r.get("open") for r in parsed],
        "h": [r.get("high") for r in parsed],
        "l": [r.get("low") for r in parsed],
        "c": [r.get("close") for r in parsed],
        "v": [r.get("volume") for r in parsed],
    }


def _num(v):
    """Finedge sends prices as strings in history and (sometimes) numbers in the
    daily feed — compare them as numbers, never as raw values."""
    try:
        return round(float(v), 4)
    except (TypeError, ValueError):
        return None


def _same_ohlc(a, b):
    return all(_num(a.get(k)) == _num(b.get(k)) for k in ("open", "high", "low", "close")) \
        and _num(b.get("close")) is not None


def drop_stale_tail(parsed):
    """
    Remove trailing candles that only repeat the previous candle's OHLC.
    When Finedge stops updating an index (12 BSE indices froze on 2026-08-14),
    its daily feed keeps returning that last session; stamped with a later
    date it drew a flat line across the chart. Returns (parsed, dropped).
    """
    dropped = 0
    while len(parsed) >= 2 and _same_ohlc(parsed[-2], parsed[-1]):
        parsed.pop()
        dropped += 1
    return parsed, dropped


# Indices whose newest real candle is older than this are reported at the end
# of the run (Finedge has stopped updating them).
STALE_AFTER_DAYS = 7
STALE_INDICES: dict = {}
# Indices with no new candle for longer than this are marked "stale": true in
# index_master.json, and the Index Monitor hides them. When data comes back,
# the next run drops the flag and the index shows again.
HIDE_AFTER_DAYS = 30
LAST_REAL_DATE: dict = {}


def merge_daily_into_history(parsed, daily_row):
    """
    Merge today's daily-feed row into the historical candle list, so
    index_history/{symbol}.json always reflects the latest price even on
    days when Finedge's historical endpoint hasn't caught up yet.

    - If the last historical row is already dated today, REPLACE it with
      the daily-feed row (daily-feed is the more current/live source).
    - If today isn't present yet, APPEND a new row built from daily-feed.
    - No-op if there's no daily-feed row for this symbol, or if the
      history is empty (nothing to anchor the merge to).

    - Skipped on a weekend / NSE holiday: the daily feed then still holds
      the last session's prices, and appending them under today's date
      created a duplicate candle.
    - Skipped when the feed's OHLC is identical to the last historical
      candle (a stale feed that has not rolled over yet).

    Returns (parsed, status) where status is one of:
      "appended" | "replaced" | "no-daily-row" | "empty-history" |
      "holiday-skip" | "stale-skip"
    """
    if not parsed:
        return parsed, "empty-history"
    if not daily_row:
        return parsed, "no-daily-row"

    today_str = today_ist()
    if not is_trading_day(today_str):
        return parsed, "holiday-skip"
    merged_row = {
        "date"         : today_str,
        "open"         : daily_row.get("open"),
        "high"         : daily_row.get("high"),
        "low"          : daily_row.get("low"),
        "close"        : daily_row.get("close"),
        "change_pct"   : daily_row.get("change_pct"),
        "points_change": daily_row.get("points_change"),
        "volume"       : daily_row.get("volume"),
        "turnover"     : daily_row.get("turnover"),
    }

    last_date = str(parsed[-1].get("date", ""))[:10]
    if last_date != today_str:
        # numeric compare: history holds strings ("38038.02"), the feed may hold
        # numbers — the old raw == never matched, so stale rows were appended
        if _same_ohlc(parsed[-1], merged_row):
            return parsed, "stale-skip"
    if last_date == today_str:
        parsed[-1] = merged_row
        status = "replaced"
    else:
        parsed.append(merged_row)
        status = "appended"
    return parsed, status


# ─────────────────────────────────────────────
# Upstox helpers
# ─────────────────────────────────────────────

def _upstox_headers():
    return {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_TOKEN}"}


def _index_name_key(name):
    """'BSE Focused IT' / 'S&P BSE FOCUSED IT' / 'FOCUSED IT' → 'FOCUSEDIT'."""
    n = str(name or "").upper().replace("S&P", " ")
    n = "".join(ch for ch in n if ch.isalnum() or ch == " ")
    words = [w for w in n.split() if w not in ("BSE", "INDEX", "TRI")]
    return "".join(words)


async def load_upstox_bse_indices(client):
    """{name_key: instrument_key} for every BSE index in Upstox's instrument file."""
    import gzip
    if not UPSTOX_TOKEN:
        print("⚠ UPSTOX_TOKEN not set — BSE indices use Finedge only")
        return {}
    try:
        r = await fetch_with_retry(client, UPSTOX_BSE_BOD, None, retries=3, base_delay=2, timeout=120)
        instruments = json.loads(gzip.decompress(r.content))
    except Exception as e:
        print(f"⚠ Upstox BSE instrument file failed ({e}) — BSE indices use Finedge only")
        return {}
    out = {}
    UPSTOX_BSE_LIST.clear()
    for inst in instruments:
        if inst.get("segment") != "BSE_INDEX" or not inst.get("instrument_key"):
            continue
        UPSTOX_BSE_LIST.append((inst.get("name") or "", inst.get("trading_symbol") or "", inst["instrument_key"]))
        for label in (inst.get("name"), inst.get("trading_symbol")):
            # two keys per label: words like "BSE" removed ("FOCUSEDIT"), and the
            # label squeezed as it is ("BSEIT", "SMLCAP") — Upstox trading
            # symbols are BSE's index codes, which match Finedge's SNSX<code>
            for k in (_index_name_key(label), _compact(label)):
                if k and k not in out:
                    out[k] = inst["instrument_key"]
    print(f"✓ Upstox BSE indices: {len(UPSTOX_BSE_LIST)} instruments")
    return out


UPSTOX_BSE_LIST: list = []   # (name, trading_symbol, instrument_key), for log hints


def _compact(label):
    return "".join(ch for ch in str(label or "").upper() if ch.isalnum())


def upstox_hints(name, limit=3):
    """Upstox BSE indices that share a word with name — printed for unmatched indices."""
    words = [w for w in _compact_words(name) if len(w) >= 3 and w not in ("BSE", "INDEX", "S&P")]
    hits = [x for x in UPSTOX_BSE_LIST if any(w in (x[0] + " " + x[1]).upper() for w in words)]
    return hits[:limit]


def _compact_words(name):
    return "".join(ch if ch.isalnum() else " " for ch in str(name or "").upper()).split()


def upstox_key_for(symbol, meta, upstox_keys):
    if symbol in UPSTOX_INDEX_OVERRIDES:
        return UPSTOX_INDEX_OVERRIDES[symbol]
    is_bse = str(meta.get("exchange") or "").upper() == "BSE" or symbol.startswith("SNSX")
    if not is_bse or not upstox_keys:
        return None
    for k in (_index_name_key(meta.get("name")), _compact(symbol[4:] if symbol.startswith("SNSX") else symbol),
              _compact(meta.get("name"))):
        if k and k in upstox_keys:
            return upstox_keys[k]
    return None


async def fetch_upstox_index_history(client, instrument_key, symbol):
    """Daily candles from Upstox for the same window as Finedge, as history rows."""
    lookback_days = EXTENDED_HISTORY_DAYS.get(symbol, DEFAULT_HISTORY_DAYS)
    t = datetime.now(ZoneInfo("Asia/Kolkata")).date()
    f = t - timedelta(days=lookback_days)
    rows = {}
    cur = f
    while cur <= t:  # Upstox gives at most one year of daily candles per call
        chunk_to = min(date(cur.year, 12, 31), t)
        url = f"{UPSTOX_BASE}/historical-candle/{instrument_key}/day/{chunk_to.isoformat()}/{cur.isoformat()}"
        try:
            r = await client.get(url, headers=_upstox_headers(), timeout=30)
            if r.status_code == 429:
                await asyncio.sleep(20)
                r = await client.get(url, headers=_upstox_headers(), timeout=30)
            if r.status_code == 200:
                for c in ((r.json().get("data") or {}).get("candles") or []):
                    d = str(c[0])[:10]
                    rows[d] = {"date": d, "open": c[1], "high": c[2], "low": c[3], "close": c[4],
                               "change_pct": None, "points_change": None,
                               "volume": c[5] if len(c) > 5 else None, "turnover": None}
            elif r.status_code == 401:
                print("  ✗ UPSTOX_TOKEN rejected (401)")
                return []
        except httpx.HTTPError as e:
            print(f"  ⚠ Upstox {symbol} {cur.year}: {e.__class__.__name__}")
        cur = date(cur.year + 1, 1, 1)
    return [rows[d] for d in sorted(rows)]


def upstox_mismatch(parsed, upstox_rows):
    """Median % gap between Finedge and Upstox closes on shared dates, or None
    when they share fewer than 5 dates. A big gap means a wrong name match."""
    fin = {str(r.get("date", ""))[:10]: _num(r.get("close")) for r in parsed}
    diffs = sorted(abs(_num(u["close"]) / fin[u["date"]] - 1) * 100
                   for u in upstox_rows if fin.get(u["date"]) and _num(u.get("close")))
    return diffs[len(diffs) // 2] if len(diffs) >= 5 else None


def fill_from_upstox(parsed, upstox_rows):
    """Add Upstox candles for dates Finedge does not have. Returns (rows, added)."""
    have = {str(r.get("date", ""))[:10] for r in parsed}
    extra = [r for r in upstox_rows if r["date"] not in have and r.get("close") is not None]
    if not extra:
        return parsed, 0
    merged = sorted(parsed + extra, key=lambda r: str(r.get("date", ""))[:10])
    return merged, len(extra)


UPSTOX_STATS = {"matched": 0, "filled": 0, "candles": 0, "unmatched": [], "rejected": []}


async def fetch_parse_upload_one_history(client, sem, i, total, symbol, meta, daily_row=None,
                                         upstox_key=None, upstox_sem=None):
    async with sem:
        rows = await fetch_index_history_one(client, meta["api_symbol"], symbol)
        parsed = parse_index_history(rows)
        # Finedge returns history newest-first. Everything below (daily merge,
        # stale-tail check, Upstox fill, last-date) needs oldest-first, so sort
        # here and put the original order back just before upload.
        newest_first = len(parsed) > 1 and str(parsed[0].get("date", ""))[:10] > str(parsed[-1].get("date", ""))[:10]
        parsed.sort(key=lambda r: str(r.get("date", ""))[:10])
        up_rows = []
        if upstox_key:
            async with upstox_sem:
                up_rows = await fetch_upstox_index_history(client, upstox_key, symbol)
        if not parsed and up_rows:
            print(f"[{i}/{total}]   {symbol}: no Finedge history — using Upstox")
        if not parsed and not up_rows:
            print(f"[{i}/{total}] ✗ {symbol} | no data")
            return symbol, None, None, False, "no-history", None
        if parsed:
            parsed, merge_status = merge_daily_into_history(parsed, daily_row)
            # a frozen Finedge tail must go before Upstox fills the dates after it
            parsed, _ = drop_stale_tail(parsed)
        else:
            merge_status = "empty-history"
        if up_rows and parsed:
            gap = upstox_mismatch(parsed, up_rows)
            if gap is not None and gap > 1.5:
                print(f"[{i}/{total}]   {symbol}: Upstox prices differ {gap:.1f}% from Finedge — "
                      f"wrong match ({upstox_key}), not used")
                UPSTOX_STATS["rejected"].append((symbol, upstox_key, round(gap, 1)))
                up_rows = []
        if up_rows:
            parsed, added = fill_from_upstox(parsed, up_rows)
            if added:
                UPSTOX_STATS["filled"] += 1
                UPSTOX_STATS["candles"] += added
                print(f"[{i}/{total}]   {symbol}: +{added} candle(s) from Upstox")
        if DROP_DATES:
            before = len(parsed)
            parsed = [r for r in parsed if str(r.get("date", ""))[:10] not in DROP_DATES]
            if len(parsed) != before:
                print(f"[{i}/{total}]   {symbol}: dropped {before - len(parsed)} candle(s) dated {', '.join(sorted(DROP_DATES))}")
            if not parsed:
                print(f"[{i}/{total}] ✗ {symbol} | no data left after --drop-date")
                return symbol, None, None, False, "no-history", None
        parsed, n_stale = drop_stale_tail(parsed)
        if n_stale:
            print(f"[{i}/{total}]   {symbol}: dropped {n_stale} repeated candle(s) at the end (feed not updating)")
        last_real = max(str(r.get("date", ""))[:10] for r in parsed)
        LAST_REAL_DATE[symbol] = last_real
        try:
            if (date.fromisoformat(today_ist()) - date.fromisoformat(last_real)).days > STALE_AFTER_DAYS:
                STALE_INDICES[symbol] = last_real
        except ValueError:
            pass
        weekly = compute_weekly_return(parsed)
        msw = compute_index_mswing(parsed)
        if newest_first:
            parsed = parsed[::-1]
        ohlc_arrays = build_ohlc_arrays(parsed)
        await upload_with_manifest(client, r2_upload, f"index_history/{symbol}.json", parsed,
                                    schema_v=1, extra_meta={"candle_count": len(parsed)})
        years_note = f" ({EXTENDED_HISTORY_DAYS[symbol] // 365}Y)" if symbol in EXTENDED_HISTORY_DAYS else ""
        print(f"[{i}/{total}] ✓ {symbol} | {len(parsed)} candles{years_note} | daily-merge: {merge_status}")
        return symbol, weekly, msw, True, merge_status, ohlc_arrays


def compute_weekly_return(history):
    """
    1-week return — Finedge's price-returns API has no 1W period at all,
    so it's derived locally. Uses the SAME convention as the rest of this
    codebase's "weekly" logic (pipeline.py's _resample_weekly / Weekly IB /
    Weekly NR7 detectors): group daily candles by ISO calendar week
    (year, week_number) and take the LAST trading day's close within each
    week as that week's close.

    Return = latest close vs. the close of the last trading day in the
    PREVIOUS completed ISO week — i.e. "this week so far" vs "last week".

    This is naturally holiday-safe: a short week (e.g. Friday off) simply
    ends on whichever day actually traded last (Thursday) — there's no
    fixed "7 days" or "5 trading days" assumption anywhere.
    """
    if not history:
        return None
    rows = sorted(
        (r for r in history if r.get("date") and r.get("close") is not None),
        key=lambda r: r["date"]
    )
    if len(rows) < 2:
        return None

    week_last = {}  # (iso_year, iso_week) -> {"close":..., "date":...}
    for r in rows:
        try:
            d = date.fromisoformat(r["date"][:10])
        except (ValueError, TypeError):
            continue
        week_last[d.isocalendar()[:2]] = {"close": r["close"], "date": r["date"]}

    weeks_sorted = sorted(week_last.keys())
    if len(weeks_sorted) < 2:
        return None

    latest = rows[-1]
    base = week_last[weeks_sorted[-2]]  # last trading day of the previous completed week
    if not base.get("close"):
        return None
    pct = round((latest["close"] - base["close"]) / base["close"] * 100, 2)
    return {"v": pct, "d": base["date"]}


# ─────────────────────────────────────────────
# Index MSwing (daily / weekly / previous-week)
# Same tiered formula as pipeline.py's stock _calculate_mswing:
#   full history -> 20 & 50 bars, else 20 & 10, else 5 & 10, else None.
# Weekly uses ISO-week resampled closes (same convention as
# compute_weekly_return). Previous-week = weekly series minus the
# current (in-progress) week.
# ─────────────────────────────────────────────

def _index_weekly_closes(history):
    rows = sorted(
        (r for r in history if r.get("date") and r.get("close") is not None),
        key=lambda r: r["date"]
    )
    wk = {}  # (iso_year, iso_week) -> last close of that week
    for r in rows:
        try:
            d = date.fromisoformat(r["date"][:10])
        except (ValueError, TypeError):
            continue
        wk[d.isocalendar()[:2]] = r["close"]
    return [wk[k] for k in sorted(wk.keys())]


def _mswing_latest(closes):
    """Tiered mswing (20/50 -> 20/10 -> 5/10) on a chronological close list; latest value or None."""
    n = len(closes)
    if n < 11:
        return None
    idx = n - 1
    c_now = closes[idx]
    if c_now is None:
        return None

    def _c(k):
        j = idx - k
        v = closes[j] if j >= 0 else None
        return v if v else None

    c5, c10, c20, c50 = _c(5), _c(10), _c(20), _c(50)
    try:
        if c50 and c20:
            return round((c_now - c20) / c20 * 100 / 20 + (c_now - c50) / c50 * 100 / 50, 4)
        if c20 and c10:
            return round((c_now - c10) / c10 * 100 / 10 + (c_now - c20) / c20 * 100 / 20, 4)
        if c10 and c5:
            return round((c_now - c5) / c5 * 100 / 5 + (c_now - c10) / c10 * 100 / 10, 4)
    except ZeroDivisionError:
        return None
    return None


def compute_index_mswing(history):
    """{'d': daily, 'w': weekly, 'wprev': previous-week} tiered mswing from an index's daily history."""
    rows = sorted(
        (r for r in history if r.get("date") and r.get("close") is not None),
        key=lambda r: r["date"]
    )
    daily_closes = [r["close"] for r in rows]
    weekly_closes = _index_weekly_closes(history)
    return {
        "d"    : _mswing_latest(daily_closes),
        "w"    : _mswing_latest(weekly_closes),
        "wprev": _mswing_latest(weekly_closes[:-1]) if len(weekly_closes) >= 2 else None,
    }


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

async def main():
    async with httpx.AsyncClient() as client:

        print("\n=================================")
        print(" INDEX PIPELINE STARTED")
        print("=================================\n")
        _today = today_ist()
        print(f"Date (IST): {_today} — {'trading day' if is_trading_day(_today) else 'NOT a trading day: daily candle will not be added'}")
        if DROP_DATES:
            print(f"Dropping candles dated: {', '.join(sorted(DROP_DATES))}")
        print()

        print("=== INDEX MASTER ===")
        master_rows = await fetch_index_master(client)
        if isinstance(master_rows, dict):
            master_rows = master_rows.get("data", [])
        master_parsed = parse_index_master(master_rows)
        valid_symbols = set(master_parsed.keys())
        await upload_with_manifest(client, r2_upload, "index_master.json", master_parsed,
                                    schema_v=1, extra_meta={"index_count": len(master_parsed)})
        print("✅ index_master.json uploaded\n")

        index_map = build_index_map(master_parsed)
        await upload_with_manifest(client, r2_upload, "index_map.json", index_map,
                                    schema_v=1, extra_meta={"index_count": len(index_map)})
        print(f"✅ index_map.json uploaded ({len(index_map)} indices)\n")

        print("=== INDEX DAILY FEED ===")
        daily_rows   = await fetch_index_daily(client)
        daily_parsed = parse_index_daily(daily_rows, valid_symbols)
        await upload_with_manifest(client, r2_upload, "index_daily.json", daily_parsed,
                                    schema_v=1, extra_meta={"index_count": len(daily_parsed)})
        print("✅ index_daily.json uploaded\n")

        print("=== INDEX RETURNS ===")
        # If this permanently fails after retries, don't crash the whole
        # run — the historical loop below (119 symbols) is far more
        # valuable and completely unrelated to this endpoint failing.
        try:
            returns_rows = await fetch_index_returns(client)
            print(f"  fetched {len(returns_rows)} raw rows\n")
        except Exception as e:
            print(f"  ✗ index/price-returns permanently failed after retries: {e}")
            print("  → continuing pipeline without returns data for this run\n")
            returns_rows = None

        print("=== INDEX HISTORICAL ===")
        symbols = sorted(master_parsed.items())
        total = len(symbols)
        sem = asyncio.Semaphore(HISTORY_CONCURRENCY)
        upstox_sem = asyncio.Semaphore(UPSTOX_CONCURRENCY)
        upstox_keys = await load_upstox_bse_indices(client)
        upstox_for = {}
        for symbol, meta in symbols:
            k = upstox_key_for(symbol, meta, upstox_keys)
            if k:
                upstox_for[symbol] = k
            elif upstox_keys and (str(meta.get("exchange") or "").upper() == "BSE" or symbol.startswith("SNSX")):
                UPSTOX_STATS["unmatched"].append((symbol, meta.get("name")))
        UPSTOX_STATS["matched"] = len(upstox_for)
        if upstox_keys:
            print(f"✓ Upstox matched {len(upstox_for)} BSE indices, unmatched {len(UPSTOX_STATS['unmatched'])}\n")

        tasks = [
            fetch_parse_upload_one_history(
                client, sem, i, total, symbol, meta,
                daily_row=daily_parsed.get(symbol),
                upstox_key=upstox_for.get(symbol), upstox_sem=upstox_sem,
            )
            for i, (symbol, meta) in enumerate(symbols, 1)
        ]
        results = await asyncio.gather(*tasks)

        success = failed = 0
        weekly_map = {}
        mswing_map = {}
        index_ohlc_map = {}
        merge_counts = {"appended": 0, "replaced": 0, "holiday-skip": 0, "stale-skip": 0, "no-daily-row": 0, "empty-history": 0, "no-history": 0}
        for symbol, weekly, msw, ok, merge_status, ohlc_arrays in results:
            merge_counts[merge_status] = merge_counts.get(merge_status, 0) + 1
            if ok:
                success += 1
                if weekly:
                    weekly_map[symbol] = weekly
                if msw:
                    mswing_map[symbol] = msw
                if ohlc_arrays:
                    index_ohlc_map[symbol] = ohlc_arrays
            else:
                failed += 1

        # Mark long-stale indices in index_master.json (uploaded again with the flag)
        hidden = []
        for symbol, last in LAST_REAL_DATE.items():
            meta = master_parsed.get(symbol)
            if meta is None:
                continue
            try:
                age = (date.fromisoformat(_today) - date.fromisoformat(last)).days
            except ValueError:
                continue
            if age > HIDE_AFTER_DAYS:
                meta["stale"] = True
                meta["last_date"] = last
                hidden.append((symbol, last))
            else:
                meta.pop("stale", None)
                meta.pop("last_date", None)
        if len(hidden) > len(LAST_REAL_DATE) // 4:
            # that many dead indices at once means a bug or a feed outage, not
            # discontinued indices — don't hide anything this run
            print(f"⚠ {len(hidden)} of {len(LAST_REAL_DATE)} indices look stale — too many, flags NOT applied")
            for symbol, _ in hidden:
                master_parsed[symbol].pop("stale", None)
                master_parsed[symbol].pop("last_date", None)
            hidden = []
        await upload_with_manifest(client, r2_upload, "index_master.json", master_parsed,
                                    schema_v=1, extra_meta={"index_count": len(master_parsed),
                                                            "stale_count": len(hidden)})
        print(f"✅ index_master.json uploaded again — {len(hidden)} stale index(es) flagged "
              f"(no data for over {HIDE_AFTER_DAYS} days, hidden on the site)")
        for symbol, last in sorted(hidden, key=lambda x: x[1]):
            print(f"   {symbol:<16} last candle {last}")
        print()

        # index_mswing.json — { symbol: {d, w, wprev} }. Independent of the
        # returns endpoint (only needs the history already fetched above),
        # so it's emitted even if index/price-returns failed this run.
        await upload_with_manifest(client, r2_upload, "index_mswing.json", mswing_map,
                                    schema_v=1, extra_meta={"index_count": len(mswing_map)})
        print(f"✅ index_mswing.json uploaded ({len(mswing_map)} indices)\n")

        # index_ohlc.json — { symbol: {d,o,h,l,c,v} }, single combined file
        # in the SAME array shape the frontend's stock ohlc_N.json chunks
        # use. Lets the chart page merge indices into its existing symbol
        # pool with one fetch instead of 119 separate index_history/*.json
        # requests. Built from history already fetched above — no extra
        # Finedge calls.
        await upload_with_manifest(client, r2_upload, "index_ohlc.json", index_ohlc_map,
                                    schema_v=1, extra_meta={"index_count": len(index_ohlc_map)})
        print(f"✅ index_ohlc.json uploaded ({len(index_ohlc_map)} indices)\n")

        if returns_rows is not None:
            returns_parsed = parse_index_returns(returns_rows, valid_symbols, weekly_map)
            await upload_with_manifest(client, r2_upload, "index_returns.json", returns_parsed,
                                        schema_v=1, extra_meta={"index_count": len(returns_parsed), "weekly_count": len(weekly_map)})
            print("✅ index_returns.json uploaded\n")
        else:
            print("⏭️  index_returns.json skipped this run (upstream endpoint failed)\n")

        print("\n=================================")
        print(" INDEX PIPELINE COMPLETED")
        print("=================================")
        print(f"\n📅 Daily-merge into history — appended: {merge_counts['appended']} | "
              f"replaced: {merge_counts['replaced']} | holiday skip: {merge_counts['holiday-skip']} | "
              f"stale skip: {merge_counts['stale-skip']} | no daily row: {merge_counts['no-daily-row']} | "
              f"empty history: {merge_counts['empty-history']} | no history at all: {merge_counts['no-history']}")
        if UPSTOX_STATS["matched"]:
            print(f"\n🔗 Upstox: {UPSTOX_STATS['matched']} BSE indices matched, "
                  f"{UPSTOX_STATS['filled']} got {UPSTOX_STATS['candles']} missing candle(s)")
        if UPSTOX_STATS["rejected"]:
            print("   Matched but prices did not agree (fix in UPSTOX_INDEX_OVERRIDES):")
            for sym, key, gap in UPSTOX_STATS["rejected"]:
                print(f"     {sym:<16} {key}  ({gap}% apart)")
        if UPSTOX_STATS["unmatched"]:
            print("   Not found on Upstox (add to UPSTOX_INDEX_OVERRIDES if needed):")
            for sym, nm in UPSTOX_STATS["unmatched"]:
                hint = "; ".join(f"{n or t} = {k}" for n, t, k in upstox_hints(nm))
                print(f"     {sym:<16} {nm}" + (f"   → maybe: {hint}" if hint else ""))
        if STALE_INDICES:
            print(f"\n⚠ {len(STALE_INDICES)} indices have no new data for over {STALE_AFTER_DAYS} days "
                  f"(Finedge not updating them):")
            for sym, last in sorted(STALE_INDICES.items(), key=lambda x: x[1]):
                print(f"   {sym:<16} last candle {last}")
        print(f"\n✅ Success: {success}")
        print(f"❌ Failed : {failed}")
        print(f"📦 Total  : {total}\n")


if __name__ == "__main__":
    asyncio.run(main())
