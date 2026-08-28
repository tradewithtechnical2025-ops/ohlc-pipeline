#!/usr/bin/env python3

import asyncio
import json
import os
import random
from datetime import datetime, timedelta, date

import httpx
from r2_manifest import upload_with_manifest

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

# How many index-history requests to run in parallel. Sequential (1) was
# safe but slow. 8 was tried and caused Finedge to throw 503s on nearly
# every request (44% failure rate even with retries) — their server
# clearly can't handle that concurrency. Keep this low; raise cautiously
# and only after confirming failure rate stays near zero.
HISTORY_CONCURRENCY = 3

# Symbols that need a longer historical lookback than the default 365 days.
# Add entries here if another symbol ever needs extended history — no
# other code changes required.
EXTENDED_HISTORY_DAYS = {
    "NIFMID400": 365 * 3,
    "NIFTY50":   365 * 3,
}

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
    # Most symbols get the default 365-day window. A small set of symbols
    # (see EXTENDED_HISTORY_DAYS) need a longer lookback — everything else
    # is unaffected.
    lookback_days = EXTENDED_HISTORY_DAYS.get(symbol, 365)
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

    Returns (parsed, status) where status is one of:
      "appended" | "replaced" | "no-daily-row" | "empty-history"
    """
    if not parsed:
        return parsed, "empty-history"
    if not daily_row:
        return parsed, "no-daily-row"

    today_str = date.today().isoformat()
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
    if last_date == today_str:
        parsed[-1] = merged_row
        status = "replaced"
    else:
        parsed.append(merged_row)
        status = "appended"
    return parsed, status


async def fetch_parse_upload_one_history(client, sem, i, total, symbol, meta, daily_row=None):
    async with sem:
        rows = await fetch_index_history_one(client, meta["api_symbol"], symbol)
        parsed = parse_index_history(rows)
        if not parsed:
            print(f"[{i}/{total}] ✗ {symbol} | no data")
            return symbol, None, None, False, "no-history"
        parsed, merge_status = merge_daily_into_history(parsed, daily_row)
        weekly = compute_weekly_return(parsed)
        msw = compute_index_mswing(parsed)
        await upload_with_manifest(client, r2_upload, f"index_history/{symbol}.json", parsed,
                                    schema_v=1, extra_meta={"candle_count": len(parsed)})
        years_note = f" ({EXTENDED_HISTORY_DAYS[symbol] // 365}Y)" if symbol in EXTENDED_HISTORY_DAYS else ""
        print(f"[{i}/{total}] ✓ {symbol} | {len(parsed)} candles{years_note} | daily-merge: {merge_status}")
        return symbol, weekly, msw, True, merge_status


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

        tasks = [
            fetch_parse_upload_one_history(
                client, sem, i, total, symbol, meta,
                daily_row=daily_parsed.get(symbol),
            )
            for i, (symbol, meta) in enumerate(symbols, 1)
        ]
        results = await asyncio.gather(*tasks)

        success = failed = 0
        weekly_map = {}
        mswing_map = {}
        merge_counts = {"appended": 0, "replaced": 0, "no-daily-row": 0, "empty-history": 0, "no-history": 0}
        for symbol, weekly, msw, ok, merge_status in results:
            merge_counts[merge_status] = merge_counts.get(merge_status, 0) + 1
            if ok:
                success += 1
                if weekly:
                    weekly_map[symbol] = weekly
                if msw:
                    mswing_map[symbol] = msw
            else:
                failed += 1

        # index_mswing.json — { symbol: {d, w, wprev} }. Independent of the
        # returns endpoint (only needs the history already fetched above),
        # so it's emitted even if index/price-returns failed this run.
        await upload_with_manifest(client, r2_upload, "index_mswing.json", mswing_map,
                                    schema_v=1, extra_meta={"index_count": len(mswing_map)})
        print(f"✅ index_mswing.json uploaded ({len(mswing_map)} indices)\n")

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
              f"replaced: {merge_counts['replaced']} | no daily row: {merge_counts['no-daily-row']} | "
              f"empty history: {merge_counts['empty-history']} | no history at all: {merge_counts['no-history']}")
        print(f"\n✅ Success: {success}")
        print(f"❌ Failed : {failed}")
        print(f"📦 Total  : {total}\n")


if __name__ == "__main__":
    asyncio.run(main())
