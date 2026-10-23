"""US company metadata, paced Yahoo requests and lossless field-by-field merges.

python pipeline_us_meta.py                  # missing/stale profiles (7-day cache)
python pipeline_us_meta.py --force --limit 5 # small diagnostic run
python pipeline_us_meta.py --refresh-universe

Existing us_company_meta.json is never replaced when all attempted requests fail.
Environment: existing WORKER_URL/WORKER_TOKEN; optional US_META_WORKERS (2),
US_META_REQUEST_GAP (1.0 seconds), US_META_MAX_AGE_DAYS (7), US_META_RETRIES (1).
"""
import argparse
import asyncio
import logging
import math
import os
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib.metadata import version, PackageNotFoundError

import httpx
import yfinance as yf
import pipeline_us as p
from pipeline_us import r2_upload, r2_download, now_iso, EXCHANGE_NAMES

log = logging.getLogger("meta")
# pipeline_us silences Yahoo logs for bulk OHLC. Metadata needs its warnings.
logging.getLogger("yfinance").setLevel(logging.WARNING)
META_FILE = "us_company_meta.json"
LOG_EVERY = 50


def settings():
    workers = int(os.environ.get("US_META_WORKERS", "2"))
    gap = float(os.environ.get("US_META_REQUEST_GAP", "1.0"))
    age = float(os.environ.get("US_META_MAX_AGE_DAYS", "7"))
    retries = int(os.environ.get("US_META_RETRIES", "1"))
    if not 1 <= workers <= 4 or not math.isfinite(gap) or gap < .25 or not math.isfinite(age) or age <= 0 or not 0 <= retries <= 3:
        raise ValueError("Invalid US_META settings: workers 1–4, request gap >= .25s, age > 0, retries 0–3")
    return workers, gap, age, retries


def error_text(error):
    text = str(error).replace("\n", " ")
    text = re.sub(r"(?i)(crumb|cookie|authorization|token)([=:]\s*)[^\s&;,]+", r"\1\2[redacted]", text)
    return text[:350]


def classify_error(exc):
    text = (type(exc).__name__ + " " + str(exc)).lower()
    if "429" in text or "ratelimit" in text or "rate limit" in text or "too many requests" in text:
        return "rate_limit"
    if any(s in text for s in ("401", "403", "unauthorized", "forbidden", "invalid crumb")):
        return "authorization"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if any(s in text for s in ("ssl", "certificate", "curl", "connection", "resolve host")):
        return "network"
    return "request_error"


def finite_number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def clean_text(value):
    return value.strip() if isinstance(value, str) and value.strip() else None


def normalize_info(info, fallback_name=None):
    if not isinstance(info, dict) or not info:
        raise ValueError("Yahoo returned empty metadata")
    # A lone price/PEG value is not a company profile. Do not count it as success.
    name = clean_text(info.get("longName")) or clean_text(info.get("shortName"))
    meaningful = name or any(info.get(k) is not None for k in ("sector", "industry", "marketCap", "totalAssets", "country", "website"))
    if not meaningful:
        raise ValueError("Yahoo returned no company profile; available keys=" + ",".join(sorted(str(k) for k in info)[:15]))
    name = name or clean_text(fallback_name)
    if not name:
        raise ValueError("Yahoo profile has no company name")
    exch = clean_text(info.get("exchange"))
    first = finite_number(info.get("firstTradeDateEpochUtc"))
    start_date = None
    if first:
        try:
            start_date = datetime.fromtimestamp(first, timezone.utc).date().isoformat()
        except (ValueError, OverflowError, OSError):
            pass
    annual_div = finite_number(info.get("dividendRate"))
    price = finite_number(info.get("regularMarketPrice")) or finite_number(info.get("currentPrice"))
    # Use annual dividend / price when available: avoids guessing fraction vs percent.
    div_yield = annual_div / price * 100 if annual_div is not None and price and price > 0 else None
    if div_yield is None:
        trailing = finite_number(info.get("trailingAnnualDividendYield"))
        div_yield = trailing * 100 if trailing is not None else None
    return {
        "name": name, "sector": clean_text(info.get("sector")), "industry": clean_text(info.get("industry")),
        "marketCap": finite_number(info.get("marketCap")) or finite_number(info.get("totalAssets")),
        "exchange": exch, "exchangeName": EXCHANGE_NAMES.get(exch, exch),
        "country": clean_text(info.get("country")), "website": clean_text(info.get("website")),
        "employees": finite_number(info.get("fullTimeEmployees")), "beta": finite_number(info.get("beta")),
        "pe": finite_number(info.get("trailingPE")), "fwdPe": finite_number(info.get("forwardPE")),
        "divYield": round(div_yield, 4) if div_yield is not None else None,
        "high52": finite_number(info.get("fiftyTwoWeekHigh")), "low52": finite_number(info.get("fiftyTwoWeekLow")),
        "avgVolume": finite_number(info.get("averageVolume")), "sharesOut": finite_number(info.get("sharesOutstanding")),
        "description": clean_text(info.get("longBusinessSummary")), "startDate": start_date,
        "_metaUpdated": now_iso(),
        "_profileUpdated": now_iso() if clean_text(info.get("sector")) and clean_text(info.get("industry")) else None,
    }


@dataclass
class FetchResult:
    symbol: str
    data: dict = field(default_factory=dict)
    reason: str = ""
    detail: str = ""


class RequestPacer:
    """One shared start-rate limiter across workers; no per-thread burst."""
    def __init__(self, gap):
        self.gap, self.last, self.lock = gap, None, threading.Lock()

    def wait(self):
        with self.lock:
            if self.last is not None:
                time.sleep(max(0, self.gap - (time.monotonic() - self.last)))
            self.last = time.monotonic()


def fetch_one(symbol, pacer=None):
    if pacer:
        pacer.wait()
    try:
        info = yf.Ticker(symbol).info
        data = normalize_info(info, p.UNIVERSE.get(symbol, {}).get("name"))
        return FetchResult(symbol, data=data)
    except Exception as exc:
        reason = "empty_profile" if isinstance(exc, ValueError) and str(exc).startswith("Yahoo") else classify_error(exc)
        return FetchResult(symbol, reason=reason, detail=type(exc).__name__ + ": " + error_text(exc))


def fetch_all(symbols):
    workers, gap, _, retry_passes = settings()
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        return {}
    results, failures = {}, Counter()
    pacer = RequestPacer(gap)

    def record(result):
        if result.data:
            results[result.symbol] = result.data
        else:
            failures[result.reason] += 1
            if failures[result.reason] <= 5:
                log.warning("  %s [%s]: %s", result.symbol, result.reason, result.detail)

    # Check real requested names, favouring widely-covered tickers when present.
    ordered = [s for s in ("AAPL", "MSFT", "SPY") if s in symbols]
    ordered += [s for s in symbols if s not in ordered]
    probes = ordered[:min(3, len(ordered))]
    log.info("  Yahoo preflight: %s", ", ".join(probes))
    for cycle in range(2):
        preflight = [fetch_one(sym, pacer) for sym in probes]
        for result in preflight:
            record(result)
        if any(r.data for r in preflight):
            break
        if any(r.reason in {"rate_limit", "authorization"} for r in preflight):
            log.error("  Yahoo preflight blocked (%s). Stopping; no full-universe requests.", dict(failures))
            return results
        if cycle == 0:
            log.warning("  All preflight profiles failed; retrying only these %d names after 15s", len(probes))
            time.sleep(15)
    if not results:
        log.error("  Yahoo preflight failed twice: %s. Check warnings above; metadata remains untouched.", dict(failures))
        return {}

    pending = [s for s in symbols if s not in results]
    consecutive = 0
    stopped = False
    for attempt in range(1 + retry_passes):
        if not pending or stopped:
            break
        if attempt:
            log.info("  Retry pass %d: %d transient/profile failures; waiting 20s", attempt, len(pending))
            time.sleep(20)
        current = list(pending)
        processed = 0
        # Bounded batches: a circuit breaker never leaves thousands of queued futures.
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for start in range(0, len(current), workers):
                batch = list(executor.map(lambda sym: fetch_one(sym, pacer), current[start:start+workers]))
                for result in batch:
                    record(result)
                    processed += 1
                    consecutive = 0 if result.data else consecutive + 1
                blocked = any(r.reason in {"rate_limit", "authorization"} for r in batch)
                if blocked or consecutive >= 20:
                    log.error("  Stopping metadata requests: %s; %d consecutive failures. Keeping %d successful profiles.",
                              "Yahoo block/rate limit" if blocked else "widespread failure", consecutive, len(results))
                    stopped = True
                    break
                if processed % LOG_EVERY < workers or processed == len(current):
                    log.info("  info %d/%d (updated so far: %d)", processed, len(current), len(results))
        pending = [s for s in symbols if s not in results]
    log.info("  Yahoo result: %d/%d successful; failure categories: %s", len(results), len(symbols), dict(failures))
    return results


def profile_is_fresh(row, max_age_days):
    if not isinstance(row, dict) or not row.get("name") or not (row.get("sector") and row.get("industry")):
        return False
    try:
        stamp = datetime.fromisoformat(row["_profileUpdated"].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - stamp).total_seconds()
        return 0 <= age < max_age_days * 86400
    except (KeyError, TypeError, ValueError):
        return False


def merge_metadata(existing, symbols, fresh):
    out = {**existing, "stocks": {s: dict(v) for s, v in existing.get("stocks", {}).items()}}
    for sym in symbols:
        merged = dict(out["stocks"].get(sym, {}))
        merged.update({k: v for k, v in fresh.get(sym, {}).items() if v is not None and v != ""})
        merged["indices"] = p.UNIVERSE[sym]["indices"]
        if not merged.get("name"):
            merged["name"] = p.UNIVERSE[sym].get("name")
        if not merged.get("exchangeName") and p.UNIVERSE[sym].get("exchange"):
            merged["exchangeName"] = p.UNIVERSE[sym]["exchange"]
        out["stocks"][sym] = merged
    for sym in out["stocks"]:
        if sym not in p.UNIVERSE:
            out["stocks"][sym]["indices"] = []
    out.update(updated=now_iso(), count=len(out["stocks"]))
    return out


async def main(args=None):
    args = args or argparse.Namespace(force=False, limit=None, refresh_universe=False)
    _, _, max_age, _ = settings()
    for name in ("yfinance", "curl_cffi", "pandas"):
        try:
            log.info("Dependency %s=%s", name, version(name))
        except PackageNotFoundError:
            log.info("Dependency %s: not installed", name)
    async with httpx.AsyncClient() as client:
        # The OHLC/universe job owns broad liquidity scanning. Do not repeat it
        # before thousands of profile requests unless explicitly requested.
        await p.init_universe(client, rebuild=args.refresh_universe)
        symbols = list(p.US_SYMBOLS)
        if not symbols:
            log.error("Universe empty; metadata left untouched")
            return 1
        try:
            existing = await r2_download(client, META_FILE) or {}
            if not isinstance(existing, dict) or not isinstance(existing.get("stocks", {}), dict):
                raise ValueError("Unexpected metadata schema")
        except Exception as exc:
            log.error("Couldn't read existing metadata (%s); refusing to replace it", error_text(exc))
            return 1
        todo = [s for s in symbols if args.force or not profile_is_fresh(existing.get("stocks", {}).get(s), max_age)]
        if args.limit is not None:
            todo = todo[:args.limit]
        log.info("Meta: %d tracked, %d profiles to fetch, %d skipped/not scheduled", len(symbols), len(todo), len(symbols)-len(todo))
        fresh = await asyncio.to_thread(fetch_all, todo) if todo else {}
        if todo and not fresh:
            log.error("Nothing fetched; existing %s left untouched. Actual Yahoo failure is logged above.", META_FILE)
            return 1
        stocks = merge_metadata(existing, symbols, fresh)
        await r2_upload(client, META_FILE, stocks)
    failed = [s for s in todo if s not in fresh]
    log.info("Meta done: %d updated, %d attempted-but-failed (previous values preserved), %d stored", len(fresh), len(failed), stocks["count"])
    if failed:
        log.warning("Failed-symbol sample: %s", ", ".join(failed[:30]))
    # LIQUID is an optional expansion group, not a required index.
    empty = [k for k, v in p.UNIVERSE_INDICES.items() if k != "LIQUID" and not v.get("count")]
    if empty:
        log.error("Required indices have no constituents: %s", ", ".join(empty))
    return 1 if empty or (todo and len(failed) > len(todo)*.2) else 0


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Refresh cached profiles too")
    parser.add_argument("--limit", type=int, help="Fetch at most this many profiles (diagnostic run)")
    parser.add_argument("--refresh-universe", action="store_true", help="Rebuild universe before fetching metadata")
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be >= 1")
    return asyncio.run(main(args))


if __name__ == "__main__":
    sys.exit(cli())
