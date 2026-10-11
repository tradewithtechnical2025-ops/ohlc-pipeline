#!/usr/bin/env python3
"""
Daily fundamentals master  →  R2: fundamentals_master.json (+ .manifest.json)
==============================================================================
The ONE fundamentals file the website reads. Runs every trading day at
16:30 IST. Each run:

  1. classification.json           → the stock universe (NSE+BSE, ETFs out)
  2. FinEdge daily-price-ratios    → pe / pb / ps / pfcf / ptb, one call per
                                     stock on the fundamentals basis
                                     (consolidated, else standalone); a second
                                     call only when that basis comes back empty
  3. fundamentals_rich.json        → quarters / annual / BS / CF / ratios /
                                     growth   (pipeline_fundamentals_prod.py)
     shareholding.json             → shareholding history (pipeline_shareholding.py)
     screener_feed.json            → last price, for market cap / EV
  4. merge + derived values (mcap, EV, EV/EBITDA, earnings yield,
     shareholding changes) + sanity checks
  5. upload fundamentals_master.json and its manifest

Failure rules — the website must never get an empty or half file:
  * a stock whose ratio fetch fails keeps the previous run's ratios, marked
    stale;
  * every fetch failing, or the stock count dropping below 90% of the
    previous file, aborts the run and leaves the R2 file untouched.

Units (also written into the file under "units"):
  money → ₹ crore;  per-share → ₹;  ratios / margins / growth / holdings → %
  Every history list is newest first, stored as rows whose column names are
  given once at the top of the file under "cols".

Usage
  python pipeline_daily_master.py                    # normal daily run
  python pipeline_daily_master.py --force            # also on weekend/holiday
  python pipeline_daily_master.py --symbols ITC,TCS  # refresh just these, keep the rest
  python pipeline_daily_master.py --no-upload        # build locally only
  python pipeline_daily_master.py --local-dir DIR --no-fetch --no-upload
        # offline test: reads classification.json, fundamentals_rich.json,
        # shareholding.json, screener_feed.json, price_ratios_daily.json and
        # fundamentals_master.json (previous, optional) from DIR
"""

import argparse
import asyncio
import json
import logging
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import httpx

LOG = logging.getLogger("daily_master")
IST = ZoneInfo("Asia/Kolkata")
FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"
HERE = Path(__file__).parent

OUT_KEY = "fundamentals_master.json"
SCHEMA = 1
RATIO_FIELDS = ("pe", "pb", "ps", "pfcf", "ptb")
SH_QUARTERS = 16
MIN_KEEP_RATIO = 0.90          # abort if the universe shrinks below this vs last file

# Same ETF rule as pipeline_fundamentals_prod.py / pipeline_shareholding.py.
ETF_ENDSWITH = ("ETF", "BEES", "LIQUID", "GILT", "IETF", "MMQS", "TOTAL")
ETF_CONTAINS = ("NIFTY", "BANKEX", "MSCIN")

# Column order of every history table. The website maps rows back with these.
COLS = {
    "q": ["period", "period_end", "sales", "ebitda", "opm", "other_income", "interest", "depreciation",
          "exceptional", "pbt", "tax", "pat", "pat_attr", "eps", "npm", "sales_yoy", "ebitda_yoy",
          "pat_yoy", "eps_yoy"],
    "q_bank": ["period", "period_end", "sales", "nii", "ppop", "provisions", "other_income", "interest",
               "exceptional", "pbt", "tax", "pat", "pat_attr", "eps", "npm", "gnpa_pct", "nnpa_pct",
               "cet1", "sales_yoy", "pat_yoy", "eps_yoy"],
    "a": ["period", "period_end", "sales", "ebitda", "opm", "other_income", "interest", "depreciation",
          "exceptional", "pbt", "tax", "pat", "pat_attr", "eps", "npm", "dividend_payout", "sales_yoy",
          "pat_yoy", "eps_yoy"],
    "a_bank": ["period", "period_end", "sales", "nii", "ppop", "provisions", "other_income", "interest",
               "exceptional", "pbt", "tax", "pat", "pat_attr", "eps", "npm", "gnpa_pct", "nnpa_pct",
               "cet1", "dividend_payout", "sales_yoy", "pat_yoy", "eps_yoy"],
    "bs": ["period", "period_end", "equity_capital", "reserves", "shareholders_equity", "total_equity",
           "borrowings", "cash", "investments", "fixed_assets", "cwip", "total_assets", "current_assets",
           "current_liabilities", "inventories", "receivables", "payables"],
    "bs_bank": ["period", "period_end", "equity_capital", "reserves", "shareholders_equity", "total_equity",
                "borrowings", "cash", "investments", "fixed_assets", "total_assets", "deposits", "advances"],
    "cf": ["period", "period_end", "cfo", "cfi", "cff", "capex", "fcf", "dividends_paid"],
    "sh": ["quarter", "promoter", "fii", "dii", "govt", "public"],
}


# ══════════════════════════════════════════════════════════════
# small helpers
# ══════════════════════════════════════════════════════════════

def is_etf(sym):
    return sym.endswith(ETF_ENDSWITH) or any(k in sym for k in ETF_CONTAINS)


def num(v):
    if isinstance(v, bool) or v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def r2(v):
    v = num(v)
    return round(v, 2) if v is not None else None


def finite(o):
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: finite(v) for k, v in o.items()}
    if isinstance(o, list):
        return [finite(v) for v in o]
    return o


def rows(records, cols):
    return [[rec.get(c) for c in cols] for rec in (records or [])]


def load_holidays():
    p = HERE / "nse_holidays.json"
    try:
        return set(json.loads(p.read_text())) if p.exists() else set()
    except Exception:
        return set()


def previous_trading_day(d, holidays):
    d = d - timedelta(days=1)
    while d.weekday() >= 5 or d.isoformat() in holidays:
        d -= timedelta(days=1)
    return d


# ══════════════════════════════════════════════════════════════
# I/O — R2 via the worker, or a local folder for offline runs
# ══════════════════════════════════════════════════════════════

class Store:
    def __init__(self, client, local_dir=None):
        self.client = client
        self.local_dir = Path(local_dir) if local_dir else None
        if not self.local_dir:
            self.worker = os.environ["WORKER_URL"].rstrip("/")
            self.headers = {"X-Secret-Token": os.environ["WORKER_TOKEN"]}

    async def read(self, name):
        if self.local_dir:
            p = self.local_dir / name
            return json.loads(p.read_text()) if p.exists() else None
        r = await self.client.get(f"{self.worker}/{quote(name, safe='/')}", headers=self.headers, timeout=180)
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise RuntimeError(f"R2 read {name}: HTTP {r.status_code}")
        LOG.info("  ↓ %s (%.0f KB)", name, len(r.content) / 1024)
        return r.json()

    async def write(self, name, text):
        r = await self.client.post(self.worker, params={"file": name},
                                   headers={**self.headers, "Content-Type": "application/json"},
                                   content=text.encode(), timeout=300)
        if r.status_code != 200:
            raise RuntimeError(f"R2 upload {name}: HTTP {r.status_code}")
        LOG.info("  ↑ %s (%.0f KB)", name, len(text) / 1024)


# ══════════════════════════════════════════════════════════════
# FinEdge daily-price-ratios
# ══════════════════════════════════════════════════════════════

def latest_ratio_row(payload, today):
    if not isinstance(payload, dict) or not isinstance(payload.get("price_ratios"), list):
        raise ValueError("bad payload")
    best = None
    for row in payload["price_ratios"]:
        if not isinstance(row, dict):
            continue
        try:
            d = date.fromisoformat(str(row.get("quote_date", ""))[:10])
        except ValueError:
            continue
        if d > today:
            continue
        if best is None or d > best[0]:
            best = (d, row)
    if not best:
        return None
    d, raw = best
    out = {"date": d.isoformat()}
    for f in RATIO_FIELDS:
        out[f] = r2(raw.get(f))
    return out


class RatioFetcher:
    def __init__(self, client, concurrency=4, delay=0.25):
        self.client = client
        self.token = os.environ["FINEDGE_TOKEN"]
        self.sem = asyncio.Semaphore(concurrency)
        self.lock = asyncio.Lock()
        self.delay = delay
        self.last = 0.0
        self.calls = 0

    async def _get(self, sym, basis, from_year, today):
        async with self.sem:
            for attempt in range(5):
                async with self.lock:
                    loop = asyncio.get_running_loop()
                    await asyncio.sleep(max(0.0, self.delay - (loop.time() - self.last)))
                    self.last = loop.time()
                self.calls += 1
                try:
                    r = await self.client.get(f"{FINEDGE_BASE}/daily-price-ratios/{quote(sym, safe='')}",
                                              params={"token": self.token, "statement_type": basis,
                                                      "from": from_year}, timeout=45)
                except httpx.RequestError:
                    # never log the request — the token is in the query string
                    if attempt == 4:
                        raise RuntimeError("network")
                    await asyncio.sleep(2 ** attempt)
                    continue
                if r.status_code in (401, 403):
                    raise PermissionError("FinEdge rejected the token")
                if r.status_code == 429 or r.status_code >= 500:
                    if attempt == 4:
                        raise RuntimeError(f"HTTP {r.status_code}")
                    await asyncio.sleep(20 if r.status_code == 429 else 2 ** attempt)
                    continue
                if r.status_code == 404:
                    return None
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}")
                try:
                    return latest_ratio_row(r.json(), today)
                except (ValueError, TypeError):
                    raise RuntimeError("bad response")

    async def fetch(self, sym, preferred, from_year, today):
        """→ (row or None, basis used, status)"""
        first = preferred if preferred in ("c", "s") else "c"
        order = (first, "s" if first == "c" else "c")
        errors = 0
        for basis in order:
            try:
                row = await self._get(sym, basis, from_year, today)
            except PermissionError:
                raise
            except RuntimeError:
                errors += 1
                continue
            if row and any(row.get(f) is not None for f in RATIO_FIELDS):
                return row, basis, "ok"
        return None, None, ("failed" if errors == len(order) else "empty")


# ══════════════════════════════════════════════════════════════
# per-stock record
# ══════════════════════════════════════════════════════════════

def shareholding_block(sh_entry):
    """shareholding.json keeps quarters oldest → newest; flip, keep 16."""
    data = (sh_entry or {}).get("data") or []
    hist = list(reversed(data))[:SH_QUARTERS]
    if not hist:
        return None, []
    latest = hist[0]

    def ch(key, back):
        if len(hist) <= back:
            return None
        a, b = num(hist[0].get(key)), num(hist[back].get(key))
        return round(a - b, 2) if a is not None and b is not None else None

    block = {"quarter": latest.get("quarter")}
    for k in ("promoter", "fii", "dii", "govt", "public"):
        block[k] = r2(latest.get(k))
    for k in ("promoter", "fii", "dii"):
        block[f"{k}_ch_1q"] = ch(k, 1)
        block[f"{k}_ch_4q"] = ch(k, 4)
    return block, hist


def build_record(sym, cls, rich, sh_entry, ratio, price):
    rich = rich or {}
    kind = rich.get("company_type") or "other"
    bank = kind == "bank"
    ttm = rich.get("ttm") or {}
    ratios = rich.get("ratios") or {}
    flags = []

    # ── valuation ───────────────────────────────────────────────────────
    val = {f: None for f in RATIO_FIELDS}
    val.update({"date": None, "basis": None, "stale": True})
    if ratio:
        val.update({k: ratio.get(k) for k in RATIO_FIELDS})
        val["date"] = ratio.get("date")
        val["basis"] = ratio.get("basis")
        val["stale"] = bool(ratio.get("stale"))
    ltp = num((price or {}).get("ltp"))
    shares = num(rich.get("diluted_shares"))
    mcap = round(ltp * shares / 1e7, 2) if ltp and shares else None
    val["ltp"] = ltp
    val["price_date"] = (price or {}).get("date")
    val["mcap"] = mcap
    pe = val.get("pe")
    val["earnings_yield"] = round(100 / pe, 2) if pe and pe > 0 else None
    ev = ev_ebitda = None
    if mcap is not None and not bank:
        bs0 = (rich.get("bs_annual") or [{}])[0]
        net_debt = ratios.get("net_debt")
        if net_debt is None and bs0:
            net_debt = (num(bs0.get("borrowings")) or 0) - (num(bs0.get("cash")) or 0)
        ev = round(mcap + (net_debt or 0), 2)
        e = num(ttm.get("ebitda"))
        ev_ebitda = round(ev / e, 2) if e and e > 0 else None
    val["ev"] = ev
    val["ev_ebitda"] = ev_ebitda

    # ── shareholding ────────────────────────────────────────────────────
    sh, sh_hist = shareholding_block(sh_entry)

    if not rich:
        flags.append("no_fundamentals")
    if not sh:
        flags.append("no_shareholding")
    if not ratio:
        flags.append("no_ratios")
    elif val["stale"]:
        flags.append("ratios_stale")
    if ltp is None:
        flags.append("no_price")
    for w in rich.get("warnings") or []:
        flags.append(w)

    rec = {
        "name": cls.get("name") or rich.get("name"),
        "exchange": cls.get("exchange") or rich.get("exchange"),
        "trading_symbol": cls.get("trading_symbol"),
        "sector_group": cls.get("sector_group") or rich.get("sector_group"),
        "industry": cls.get("display_industry") or rich.get("display_industry") or rich.get("industry"),
        "company_type": kind,
        "basis": rich.get("basis"),
        "last_result_date": rich.get("last_result_date"),
        "diluted_shares": shares,
        "val": val,
        "ttm": {k: ttm.get(k) for k in ("period", "sales", "ebitda", "opm", "pbt", "pat", "pat_attr", "eps",
                                         "npm", "nii", "ppop")} if ttm else None,
        "ratios": ratios or None,
        "growth": rich.get("growth") or None,
        "sh": sh,
        "q": rows(rich.get("quarters"), COLS["q_bank" if bank else "q"]),
        "a": rows(rich.get("annual"), COLS["a_bank" if bank else "a"]),
        "bs": rows(rich.get("bs_annual"), COLS["bs_bank" if bank else "bs"]),
        "cf": rows(rich.get("cf_annual"), COLS["cf"]),
        "sh_hist": rows(sh_hist, COLS["sh"]),
        "flags": flags,
    }
    # quarterly growth shortcuts used by filters
    if rich.get("quarters"):
        q0 = rich["quarters"][0]
        rec["q_latest"] = {k: q0.get(k) for k in ("period", "sales_yoy", "pat_yoy", "eps_yoy", "ebitda_yoy",
                                                    "sales_qoq", "pat_qoq", "opm")}
    else:
        rec["q_latest"] = None
    return finite(rec)


# ══════════════════════════════════════════════════════════════
# run
# ══════════════════════════════════════════════════════════════

def universe_from(classification):
    out = {}
    for row in classification or []:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol", "")).strip().upper()
        exch = str(row.get("exchange", "")).strip().upper()
        if sym and exch in ("NSE", "BSE") and not is_etf(sym):
            out[sym] = {"name": row.get("name"), "exchange": exch, "trading_symbol": row.get("trading_symbol"),
                        "sector_group": row.get("sector_group"), "display_industry": row.get("display_industry")}
    return out


def ratios_from_daily_file(daily, sym):
    """Offline test only — read a stock's ratios from an old
    price_ratios_daily.json instead of calling FinEdge."""
    e = ((daily or {}).get("stocks") or {}).get(sym) or {}
    latest = e.get("latest")
    if not latest:
        return None
    row = {"date": latest.get("quote_date"), "basis": e.get("statement_type")}
    row.update({f: r2(latest.get(f)) for f in RATIO_FIELDS})
    return row


async def run(args):
    now = datetime.now(IST)
    today = now.date()
    holidays = load_holidays()
    if not args.force and (today.weekday() >= 5 or today.isoformat() in holidays):
        LOG.info("Weekend/holiday — skipped (use --force to run anyway)")
        return
    # 16:30 run: FinEdge's latest close is normally yesterday's. Anything
    # older than the previous trading day counts as stale.
    fresh_from = previous_trading_day(today, holidays).isoformat()

    async with httpx.AsyncClient() as client:
        store = Store(client, args.local_dir)
        classification = await store.read("classification.json")
        universe = universe_from(classification)
        if not universe:
            raise RuntimeError("classification.json missing/empty — nothing to build")

        rich_file = await store.read("fundamentals_rich.json") or {}
        rich = rich_file.get("stocks") or {}
        sh_file = await store.read("shareholding.json") or {}
        feed = await store.read("screener_feed.json") or []
        prev_file = await store.read(OUT_KEY) or {}
        prev = prev_file.get("stocks") or {}
        daily_file = await store.read("price_ratios_daily.json") if args.no_fetch else None

        feed_list = feed if isinstance(feed, list) else (feed.get("stocks") or feed.get("data") or [])
        price = {}
        for row in feed_list:
            if isinstance(row, dict) and row.get("symbol"):
                price[str(row["symbol"]).upper()] = {"ltp": row.get("ltp"),
                                                     "date": row.get("as_of_date") or row.get("date")}

        targets = sorted(universe)
        if args.symbols:
            wanted = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}
            unknown = wanted - set(universe)
            if unknown:
                raise ValueError("Not in classification universe: " + ", ".join(sorted(unknown)))
            targets = sorted(wanted)

        LOG.info("Universe %d | rich %d | shareholding %d | prices %d | previous master %d",
                 len(universe), len(rich), len(sh_file), len(price), len(prev))

        # ── 1. ratios ───────────────────────────────────────────────────
        ratios, status = {}, {"ok": 0, "empty": 0, "failed": 0, "kept_previous": 0}
        if args.no_fetch:
            for sym in targets:
                row = ratios_from_daily_file(daily_file, sym)
                if row:
                    ratios[sym] = row
                    status["ok"] += 1
                else:
                    status["empty"] += 1
        else:
            fetcher = RatioFetcher(client, args.concurrency)
            from_year = today.year if today.month > 1 else today.year - 1

            async def one(sym):
                pref = (rich.get(sym) or {}).get("basis") or "c"
                row, basis, st = await fetcher.fetch(sym, pref, from_year, today)
                return sym, row, basis, st

            for i in range(0, len(targets), 40):
                for sym, row, basis, st in await asyncio.gather(*(one(s) for s in targets[i:i + 40])):
                    status[st] += 1
                    if row:
                        row["basis"] = basis
                        ratios[sym] = row
                LOG.info("  ratios %d/%d  ok:%d empty:%d failed:%d", min(i + 40, len(targets)), len(targets),
                         status["ok"], status["empty"], status["failed"])
            LOG.info("FinEdge calls: %d", fetcher.calls)
            if targets and status["failed"] == len(targets):
                raise RuntimeError("Every ratio fetch failed — R2 file left untouched")

        # keep yesterday's ratios for stocks that failed / came back empty
        for sym in targets:
            if sym in ratios:
                continue
            old = ((prev.get(sym) or {}).get("val")) or {}
            if old.get("date"):
                ratios[sym] = {**{f: old.get(f) for f in RATIO_FIELDS}, "date": old["date"],
                               "basis": old.get("basis")}
                status["kept_previous"] += 1
        for row in ratios.values():
            row["stale"] = not row.get("date") or row["date"] < fresh_from

        # ── 2. records ──────────────────────────────────────────────────
        stocks = {} if not args.symbols else {s: v for s, v in prev.items() if s in universe}
        for sym in targets:
            stocks[sym] = build_record(sym, universe[sym], rich.get(sym), sh_file.get(sym),
                                       ratios.get(sym), price.get(sym))
        # a --symbols run on a fresh file still needs the other stocks
        if args.symbols and not prev:
            LOG.warning("No previous master to merge into — file will hold only the requested symbols")

        # ── 3. sanity ───────────────────────────────────────────────────
        if prev and not args.symbols and len(stocks) < MIN_KEEP_RATIO * len(prev):
            raise RuntimeError(f"Stock count fell {len(prev)} → {len(stocks)} (<{MIN_KEEP_RATIO:.0%}) — "
                               "not uploading; check classification.json")
        counts = {
            "stocks": len(stocks),
            "with_fundamentals": sum(1 for v in stocks.values() if "no_fundamentals" not in v["flags"]),
            "with_shareholding": sum(1 for v in stocks.values() if v.get("sh")),
            "with_ratios": sum(1 for v in stocks.values() if v["val"].get("date")),
            "ratios_fresh": sum(1 for v in stocks.values() if v["val"].get("date") and not v["val"]["stale"]),
            "with_price": sum(1 for v in stocks.values() if v["val"].get("ltp") is not None),
        }
        payload = {
            "schema_version": SCHEMA,
            "generated_at": datetime.now(IST).isoformat(timespec="seconds"),
            "as_of": {
                "ratios_fresh_from": fresh_from,
                "fundamentals_updated": rich_file.get("updated"),
                "shareholding_updated": max((v.get("updated") or "" for v in sh_file.values()
                                             if isinstance(v, dict)), default=None) or None,
            },
            "units": {"money": "crore", "per_share": "rupee", "ratios": "percent",
                      "valuation": "x (pe, pb, ps, pfcf, ptb, ev_ebitda)", "order": "newest_first"},
            "cols": COLS,
            "counts": counts,
            "run": {"ratio_status": status, "symbols": args.symbols or "all"},
            "stocks": stocks,
        }
        text = json.dumps(payload, separators=(",", ":"), allow_nan=False, ensure_ascii=False)
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text)
        LOG.info("Built %s — %.1f MB | %s | ratios %s", out, len(text.encode()) / 1e6, counts, status)

        if args.no_upload or args.local_dir:
            LOG.info("Upload skipped")
            return
        from r2_manifest import upload_str_with_manifest

        async def up(c, name, body):
            await store.write(name, body)

        manifest = await upload_str_with_manifest(client, up, OUT_KEY, text, schema_v=SCHEMA,
                                                  extra_meta={"stock_count": len(stocks)})
        LOG.info("✅ %s uploaded (hash=%s)", OUT_KEY, manifest["hash"])


def main():
    ap = argparse.ArgumentParser(description="Build fundamentals_master.json")
    ap.add_argument("--symbols", default="", help="comma-separated subset; the rest is kept from the last file")
    ap.add_argument("--force", action="store_true", help="run on weekend/holiday too")
    ap.add_argument("--no-upload", action="store_true")
    ap.add_argument("--no-fetch", action="store_true", help="offline: ratios from price_ratios_daily.json")
    ap.add_argument("--local-dir", help="offline: read inputs from this folder instead of R2")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--output", default="output/fundamentals_master.json")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(run(args))
    except Exception as exc:
        LOG.error("Run failed: %s: %s", type(exc).__name__, exc)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
