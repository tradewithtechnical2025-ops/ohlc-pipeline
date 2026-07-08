"""
pipeline_nse.py
Fetches from NSE archives:
  - sec_list_DDMMYYYY.csv           → today's circuit bands (EQ+BE only)
  - eq_band_changes_DDMMYYYY.csv    → next trading day's changes (EQ+BE only)
  - bulk.csv                        → latest bulk deals
  - block.csv                       → latest block deals
  - fao_participant_oi_DDMMYYYY.csv → participant-wise F&O OI (FII/DII/Client/Pro)
  - fiidiiTradeReact?csv=true       → FII/DII cash provisional (buy/sell/net in Rs Cr)
  - sec_bhavdata_full_DDMMYYYY.csv  → delivery qty/% (EQ+BE only)

Saves to R2 via Worker (FLAT keys — no slashes, worker slash-rejection safe):
  nse_bands.json              → EQ+BE symbols with current band + next-day change if any
                                 (+ deliv_qty/deliv_per merged in when same-day data available)
  nse_bulk.json               → latest bulk deals
  nse_block.json              → latest block deals
  nse_bulk_history.json       → symbol-wise accumulated bulk history
  nse_block_history.json      → symbol-wise accumulated block history
  nse_band_changes_history.json → symbol-wise accumulated circuit band-change history
  nse_participant_oi.json     → latest single-day participant OI snapshot
  nse_participant_oi_hist.json → rolling 60-day participant OI history (sorted asc)
  nse_fii_dii.json             → latest FII/DII cash provisional snapshot (Rs Cr)
  nse_fii_dii_hist.json        → rolling 180-day FII/DII cash history (sorted asc)
  nse_deliv.json                → latest delivery qty/% snapshot (all EQ+BE symbols)
  nse_deliv_hist.json           → rolling 30-day delivery qty/% history (sorted asc)
"""

import asyncio
import csv
import io
import json
import os
import re
import time
from datetime import date, datetime, timedelta

import httpx
from r2_manifest import upload_with_manifest

# ── Telegram notify ──
try:
    from telegram_notify import PipelineStatus, send_message
except ImportError:
    class PipelineStatus:
        def __init__(self, name): self.name = name
        def set(self, *a, **k): pass
        def success(self, *a, **k): pass
        def failure(self, exc, reraise=True, **k):
            if reraise: raise exc
    def send_message(text, silent=False): pass

from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_qty(v):
    try:
        return int(float(str(v).replace(",", "")))
    except:
        return 0


def build_bulk_summary(deals):
    agg = defaultdict(lambda: {"buy_qty": 0, "sell_qty": 0})
    for d in deals:
        key = (d["symbol"], d["client"])
        qty = parse_qty(d["qty"])
        if d["side"].upper() == "BUY":
            agg[key]["buy_qty"] += qty
        else:
            agg[key]["sell_qty"] += qty

    out = []
    for (symbol, client), v in agg.items():
        buy_qty  = v["buy_qty"]
        sell_qty = v["sell_qty"]
        net_qty  = buy_qty - sell_qty
        gross    = max(buy_qty, sell_qty)
        if gross == 0:
            continue
        if abs(net_qty) <= gross * 0.10:
            signal = "TRADING_ACTIVITY"
        elif net_qty > 0:
            signal = "ACCUMULATION"
        else:
            signal = "DISTRIBUTION"
        out.append({
            "symbol":   symbol,
            "client":   client,
            "buy_qty":  buy_qty,
            "sell_qty": sell_qty,
            "net_qty":  net_qty,
            "signal":   signal,
        })
    return sorted(out, key=lambda x: abs(x["net_qty"]), reverse=True)


def build_block_summary(deals):
    by_symbol = defaultdict(lambda: {"buyers": [], "sellers": [], "buy_qty": 0, "sell_qty": 0})
    for d in deals:
        sym = d["symbol"]
        qty = parse_qty(d["qty"])
        row = {"client": d["client"], "qty": qty, "price": d["price"]}
        if d["side"].upper() == "BUY":
            by_symbol[sym]["buyers"].append(row)
            by_symbol[sym]["buy_qty"] += qty
        else:
            by_symbol[sym]["sellers"].append(row)
            by_symbol[sym]["sell_qty"] += qty

    out = []
    for sym, v in by_symbol.items():
        out.append({
            "symbol":   sym,
            "buy_qty":  v["buy_qty"],
            "sell_qty": v["sell_qty"],
            "buyers":   v["buyers"],
            "sellers":  v["sellers"],
            "signal":   "INSTITUTIONAL_TRANSFER",
        })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

DL_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UP_HEADERS = {"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"}

NSE_BASE   = "https://archives.nseindia.com/content/equities"
NSCCL_BASE = "https://archives.nseindia.com/content/nsccl"
BHAV_BASE  = "https://archives.nseindia.com/products/content"
NSE_API_BASE = "https://www.nseindia.com/api"

NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

OI_HISTORY_DAYS = 60         # rolling window for participant OI
FII_DII_HISTORY_DAYS = 180   # rolling window for FII/DII cash trend
DELIV_HISTORY_DAYS = 30      # rolling window for delivery qty/%

# ── NSE Holidays 2026 ─────────────────────────────────────────────────────────
NSE_HOLIDAYS = {
    date(2026, 1, 26),
    date(2026, 3, 25),
    date(2026, 4, 14),
    date(2026, 4, 18),
    date(2026, 5,  1),
    date(2026, 8, 15),
    date(2026, 10, 2),
    date(2026, 11, 4),
    date(2026, 12, 25),
}


def is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in NSE_HOLIDAYS


def next_trading_day(d: date) -> date:
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


def prev_trading_day(d: date) -> date:
    prv = d - timedelta(days=1)
    while not is_trading_day(prv):
        prv -= timedelta(days=1)
    return prv


def fmt(d: date) -> str:
    return d.strftime("%d%m%Y")


# ─────────────────────────────────────────────────────────────────────────────
# NSE fetch helpers
# ─────────────────────────────────────────────────────────────────────────────

def make_nse_session() -> httpx.Client:
    client = httpx.Client(headers=NSE_HEADERS, follow_redirects=True, timeout=30)
    try:
        client.get("https://www.nseindia.com")
        time.sleep(1)
    except Exception:
        pass
    return client


def fetch_csv_url(session: httpx.Client, url: str) -> list[dict] | None:
    """Returns rows or None on 404. Raises on other errors."""
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        text = r.content.decode("utf-8-sig").strip()
        if "NO RECORDS" in text.splitlines()[:3]:
            return []
        rows = list(csv.DictReader(io.StringIO(text)))
        print(f"  ✓ {url.split('/')[-1]} → {len(rows)} rows")
        return rows
    except httpx.HTTPStatusError:
        raise
    except Exception as e:
        raise RuntimeError(str(e))


def fetch_with_fallback(
    session: httpx.Client,
    tpl: str,
    start: date,
    direction: str = "prev",
    max_tries: int = 5,
) -> tuple[list[dict], date]:
    cur = start
    for i in range(max_tries):
        url  = tpl.format(fmt(cur))
        rows = fetch_csv_url(session, url)
        if rows is not None:
            if i > 0:
                print(f"  ⚠ Fell back to {cur}")
            return rows, cur
        print(f"  404 for {cur}, trying {'previous' if direction=='prev' else 'next'} trading day...")
        cur = prev_trading_day(cur) if direction == "prev" else next_trading_day(cur)
    raise RuntimeError(f"Could not fetch after {max_tries} attempts: {tpl}")


# ─────────────────────────────────────────────────────────────────────────────
# Participant OI CSV fetch — handles NSE title row + trailing-space headers

def fetch_oi_csv(session: httpx.Client, url: str) -> list[dict] | None:
    """
    NSE fao_participant_oi CSV has a title row as line 0, then the real header,
    then data. Standard fetch_csv_url would use the title as DictReader header.
    This function strips the title row and normalises column header whitespace.
    Returns rows or None on 404.
    """
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        text = r.content.decode("utf-8-sig").strip()
        lines = text.splitlines()
        # Drop any leading lines that are not the real header
        # Real header starts with "Client Type"
        start = next(
            (i for i, l in enumerate(lines) if l.strip().startswith("Client Type")),
            None,
        )
        if start is None:
            print(f"  ⚠ fao_participant_oi: real header not found in {url.split('/')[-1]}")
            return []
        # Strip trailing spaces from each header field
        header_fields = [f.strip() for f in lines[start].split(",")]
        lines[start] = ",".join(header_fields)
        clean_text = "\n".join(lines[start:])
        rows = list(csv.DictReader(io.StringIO(clean_text)))
        print(f"  ✓ {url.split('/')[-1]} → {len(rows)} rows")
        return rows
    except httpx.HTTPStatusError:
        raise
    except Exception as e:
        raise RuntimeError(str(e))


def fetch_oi_with_fallback(
    session: httpx.Client, start: date, max_tries: int = 5
) -> tuple[list[dict], date]:
    cur = start
    for i in range(max_tries):
        url  = f"{NSCCL_BASE}/fao_participant_oi_{fmt(cur)}.csv"
        rows = fetch_oi_csv(session, url)
        if rows is not None:
            if i > 0:
                print(f"  ⚠ Fell back to {cur}")
            return rows, cur
        print(f"  404 for {cur}, trying previous trading day...")
        cur = prev_trading_day(cur)
    raise RuntimeError(f"Could not fetch fao_participant_oi after {max_tries} attempts")


# ─────────────────────────────────────────────────────────────────────────────
# Participant OI parser
# ─────────────────────────────────────────────────────────────────────────────

_OI_PARTICIPANTS = {"Client", "DII", "FII", "Pro"}

def parse_participant_oi(rows: list[dict], as_of: date) -> dict:
    """
    Expects rows already cleaned by fetch_oi_csv:
      - title row stripped, real header is row 0 of DictReader
      - column header whitespace already normalised
    """
    if not rows:
        return {"date": as_of.isoformat(), "participants": {}}

    def _int(row, col):
        raw = row.get(col, "0").strip().replace(",", "")
        try:
            return int(float(raw))
        except:
            return 0

    participants = {}
    for row in rows:
        ptype = row.get("Client Type", "").strip()
        if ptype not in _OI_PARTICIPANTS:
            continue

        fut_idx_long   = _int(row, "Future Index Long")
        fut_idx_short  = _int(row, "Future Index Short")
        fut_stk_long   = _int(row, "Future Stock Long")
        fut_stk_short  = _int(row, "Future Stock Short")
        oi_call_long   = _int(row, "Option Index Call Long")
        oi_put_long    = _int(row, "Option Index Put Long")
        oi_call_short  = _int(row, "Option Index Call Short")
        oi_put_short   = _int(row, "Option Index Put Short")
        os_call_long   = _int(row, "Option Stock Call Long")
        os_put_long    = _int(row, "Option Stock Put Long")
        os_call_short  = _int(row, "Option Stock Call Short")
        os_put_short   = _int(row, "Option Stock Put Short")
        total_long     = _int(row, "Total Long Contracts")
        total_short    = _int(row, "Total Short Contracts")

        participants[ptype] = {
            "fut_idx_long":   fut_idx_long,
            "fut_idx_short":  fut_idx_short,
            "fut_stk_long":   fut_stk_long,
            "fut_stk_short":  fut_stk_short,
            "oi_call_long":   oi_call_long,
            "oi_put_long":    oi_put_long,
            "oi_call_short":  oi_call_short,
            "oi_put_short":   oi_put_short,
            "os_call_long":   os_call_long,
            "os_put_long":    os_put_long,
            "os_call_short":  os_call_short,
            "os_put_short":   os_put_short,
            "total_long":     total_long,
            "total_short":    total_short,
            "net":            total_long - total_short,
            "fut_net":        (fut_idx_long + fut_stk_long) - (fut_idx_short + fut_stk_short),
            "fut_idx_net":    fut_idx_long - fut_idx_short,
        }

    return {
        "date":         as_of.isoformat(),
        "participants": participants,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Delivery % (sec_bhavdata_full) parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_bhavdata_deliv(rows: list[dict]) -> dict:
    """
    Parses sec_bhavdata_full_DDMMYYYY.csv rows into {symbol: {deliv_qty, deliv_per}}
    for EQ + BE series only.
    Header fields come with a leading space (e.g. ' SERIES', ' DELIV_PER') because
    NSE's CSV uses ", " as the delimiter — strip both keys and values.
    BE series shows '-' for delivery fields (mandatory delivery, not separately
    tracked) — treated as None rather than 0.
    """
    out = {}
    for row in rows:
        r = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        series = r.get("SERIES", "")
        if series not in ("EQ", "BE"):
            continue
        sym = r.get("SYMBOL", "")
        if not sym:
            continue

        qty_raw = r.get("DELIV_QTY", "-")
        per_raw = r.get("DELIV_PER", "-")

        deliv_qty = None
        if qty_raw not in ("-", "", None):
            try:
                deliv_qty = int(float(qty_raw.replace(",", "")))
            except ValueError:
                deliv_qty = None

        deliv_per = None
        if per_raw not in ("-", "", None):
            try:
                deliv_per = float(per_raw)
            except ValueError:
                deliv_per = None

        out[sym] = {"deliv_qty": deliv_qty, "deliv_per": deliv_per}
    return out


# ─────────────────────────────────────────────────────────────────────────────
# FII/DII cash provisional fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fii_dii(session: httpx.Client) -> list[dict]:
    """
    Fetches daily FII/DII cash-market provisional figures (Rs Crores) from
    NSE's API. Returns raw CSV rows as dicts — column names from NSE vary in
    casing/spacing (category/Category, buyValue/'Buy Value' etc.), so
    parse_fii_dii() below normalises them rather than assuming exact keys.
    """
    url = f"{NSE_API_BASE}/fiidiiTradeReact?csv=true"
    headers = {**NSE_HEADERS, "Accept": "text/csv,application/json,text/plain,*/*"}
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig").strip()
    if not text:
        return []
    rows = list(csv.DictReader(io.StringIO(text)))
    print(f"  ✓ fiidiiTradeReact → {len(rows)} rows")
    return rows


def _norm_key(k: str) -> str:
    """
    NSE's CSV headers come as e.g. 'BUY VALUE \n(₹ Crores)' — strip everything
    except lowercase a-z/0-9 so 'BUY VALUE \n(₹ Crores)' → 'buyvaluecrores'.
    """
    return re.sub(r"[^a-z0-9]", "", k.lower())


def _find_value(norm: dict, needle: str):
    """Returns the value of the first normalised key containing `needle`."""
    for k, v in norm.items():
        if needle in k:
            return v
    return None


def _parse_nse_date(s: str) -> str:
    """Converts NSE's date string (e.g. '17-Jun-26' or '17-Jun-2026') to ISO YYYY-MM-DD."""
    s = s.strip()
    for fmt_str in ("%d-%b-%y", "%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt_str).date().isoformat()
        except ValueError:
            continue
    return s  # fallback: leave as-is if format is unrecognised


def parse_fii_dii(rows: list[dict]) -> dict:
    """
    Normalises NSE's FII/DII cash CSV into a clean snapshot:
      {"date": "YYYY-MM-DD", "fii": {buy, sell, net}, "dii": {buy, sell, net}}
    Values are in Rs Crores. Robust to NSE's multi-line/unit-suffixed headers,
    e.g. 'CATEGORY \n', 'BUY VALUE \n(₹ Crores)', dates like '17-Jun-26'.
    """
    out = {"date": None, "fii": None, "dii": None}

    def _num(v):
        try:
            return float(str(v).replace(",", "").strip())
        except:
            return 0.0

    for row in rows:
        norm = {_norm_key(k): v for k, v in row.items()}

        category = (_find_value(norm, "category") or "").strip().upper()
        row_date = (_find_value(norm, "date") or "").strip()

        buy  = _num(_find_value(norm, "buyvalue"))
        sell = _num(_find_value(norm, "sellvalue"))
        net  = _num(_find_value(norm, "netvalue"))
        if net == 0.0 and (buy or sell):
            net = buy - sell

        entry = {"buy": buy, "sell": sell, "net": net}

        if row_date and not out["date"]:
            out["date"] = _parse_nse_date(row_date)

        if category.startswith("FII") or category.startswith("FPI"):
            out["fii"] = entry
        elif category.startswith("DII") or category.startswith("MF"):
            out["dii"] = entry

    return out


# ─────────────────────────────────────────────────────────────────────────────
# Worker R2 helpers
# ─────────────────────────────────────────────────────────────────────────────

async def r2_get(client: httpx.AsyncClient, filename: str):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


async def r2_put(client: httpx.AsyncClient, filename: str, data):
    body = json.dumps(data, ensure_ascii=False).encode()
    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers=UP_HEADERS,
        content=body,
        timeout=120,
    )
    r.raise_for_status()
    print(f"  ↑ {filename} ({len(body)/1024:.1f} KB)")


# ─────────────────────────────────────────────────────────────────────────────
# History helpers
# ─────────────────────────────────────────────────────────────────────────────

def merge_oi_history(hist: list, new_entry: dict, max_days: int = OI_HISTORY_DAYS) -> list:
    """
    hist: list of daily dicts sorted ascending by ISO "date" key
    new_entry: parsed dict for today (has "date" key, ISO format)
    Returns updated list with:
      - duplicate dates replaced (idempotent re-runs)
      - oldest entries pruned so only max_days remain
    Generic — reused for participant-OI, FII/DII cash, and delivery % history.
    """
    target_date = new_entry["date"]

    existing_dates = [e["date"] for e in hist]
    if target_date in existing_dates:
        idx = existing_dates.index(target_date)
        hist[idx] = new_entry
        print(f"  history: updated existing entry for {target_date}")
    else:
        hist.append(new_entry)
        print(f"  history: added new entry for {target_date}")

    hist.sort(key=lambda x: x["date"])

    if len(hist) > max_days:
        removed = len(hist) - max_days
        hist = hist[-max_days:]
        print(f"  history: pruned {removed} old entries, keeping {len(hist)} days")

    return hist


def merge_deals_history(hist: dict, deals: list) -> int:
    added = 0
    for d in deals:
        sym = d["symbol"]
        if sym not in hist:
            hist[sym] = []
        key = f"{d['date']}|{d['client']}|{d['side']}|{d['qty']}"
        existing = {f"{x['date']}|{x['client']}|{x['side']}|{x['qty']}" for x in hist[sym]}
        if key not in existing:
            hist[sym].append(d)
            added += 1
    return added


def merge_band_changes_history(hist: dict, changes: dict, effective_date: str) -> int:
    """
    hist: {symbol: [ {effective_date, series, from, to, direction}, ... ]}
          each per-symbol list kept ascending sorted by effective_date.
    changes: {symbol: {"from":.., "to":.., "series":..}} for this run.
    effective_date: ISO date string (next_trading_day) the change applies from.
    Idempotent — skips symbol+effective_date pairs already present.
    Returns count of new entries added.
    """
    added = 0
    for sym, chg in changes.items():
        if sym not in hist:
            hist[sym] = []
        existing_dates = {e["effective_date"] for e in hist[sym]}
        if effective_date in existing_dates:
            continue
        hist[sym].append({
            "effective_date": effective_date,
            "series":         chg.get("series", "EQ"),
            "from":           chg.get("from"),
            "to":             chg.get("to"),
            "direction":      _band_dir(chg),
        })
        hist[sym].sort(key=lambda x: x["effective_date"])
        added += 1
    return added


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def _band_dir(chg: dict) -> str:
    try:
        return "up" if int(chg.get("to", 0)) > int(chg.get("from", 0)) else "down"
    except (ValueError, TypeError):
        return "down"


async def run():
    status = PipelineStatus("pipeline_nse")
    try:
        today    = date.today()
        next_day = next_trading_day(today)
        print(f"Today: {today}  |  Next trading day: {next_day}")

        nse = make_nse_session()

        # ── 1. sec_list — today's circuit bands ───────────────────────────────────
        print("\n[1] sec_list (today's bands)...")
        sec_rows, sec_date = fetch_with_fallback(
            nse, f"{NSE_BASE}/sec_list_{{}}.csv", today, direction="prev"
        )

        # ── 1b. sec_bhavdata_full — delivery % (same-day companion to sec_list) ──
        print("\n[1b] sec_bhavdata_full (delivery %)...")
        try:
            bhav_rows, bhav_date = fetch_with_fallback(
                nse, f"{BHAV_BASE}/sec_bhavdata_full_{{}}.csv", today, direction="prev"
            )
            deliv_map = parse_bhavdata_deliv(bhav_rows)
            print(f"  Delivery data: {len(deliv_map)} symbols  ({bhav_date})")
        except RuntimeError:
            print("  ⚠ sec_bhavdata_full not available — skipping delivery %")
            deliv_map, bhav_date = {}, None

        # ── 2. eq_band_changes — optional, uploaded ~8 PM IST ────────────────────
        print(f"\n[2] eq_band_changes (next day: {next_day})...")
        try:
            chg_rows, chg_date = fetch_with_fallback(
                nse, f"{NSE_BASE}/eq_band_changes_{{}}.csv", next_day, direction="next", max_tries=2
            )
        except RuntimeError:
            print("  ⚠ eq_band_changes not available yet — skipping")
            chg_rows, chg_date = [], next_day

        # ── 3. bulk deals ─────────────────────────────────────────────────────────
        print("\n[3] bulk.csv...")
        bulk_rows = fetch_csv_url(nse, f"{NSE_BASE}/bulk.csv") or []

        # ── 4. block deals ────────────────────────────────────────────────────────
        print("\n[4] block.csv...")
        block_rows = fetch_csv_url(nse, f"{NSE_BASE}/block.csv") or []

        # ── 5. Participant OI — fallback up to 5 previous trading days ────────────
        print(f"\n[5] fao_participant_oi (today: {today})...")
        try:
            oi_rows, oi_date = fetch_oi_with_fallback(nse, today, max_tries=5)
            print(f"  Participant OI date resolved to: {oi_date}")
        except RuntimeError:
            print("  ⚠ fao_participant_oi not available — skipping OI update")
            oi_rows, oi_date = [], today

        # ── 6. FII/DII cash provisional ────────────────────────────────────────────
        print("\n[6] fiidiiTradeReact (FII/DII cash)...")
        fii_dii_snapshot = None
        try:
            fii_dii_rows = fetch_fii_dii(nse)
            if fii_dii_rows:
                fii_dii_snapshot = parse_fii_dii(fii_dii_rows)
                if fii_dii_snapshot.get("date") and (fii_dii_snapshot.get("fii") or fii_dii_snapshot.get("dii")):
                    fii = fii_dii_snapshot.get("fii") or {}
                    dii = fii_dii_snapshot.get("dii") or {}
                    print(f"  FII net: {fii.get('net', 0):+,.2f} Cr   "
                          f"DII net: {dii.get('net', 0):+,.2f} Cr   ({fii_dii_snapshot['date']})")
                else:
                    print(f"  ⚠ fii_dii: couldn't parse category/values. "
                          f"Raw columns: {list(fii_dii_rows[0].keys())}")
                    fii_dii_snapshot = None
            else:
                print("  ⚠ fii_dii: empty response — skipping")
        except Exception as e:
            print(f"  ⚠ fii_dii fetch failed: {e} — skipping")
            fii_dii_snapshot = None

        nse.close()

        # ── Process bands ─────────────────────────────────────────────────────────
        bands = {}
        for row in sec_rows:
            series = row.get("Series", "").strip()
            if series not in ("EQ", "BE"):
                continue
            sym  = row.get("Symbol", "").strip()
            band = row.get("Band", "").strip()
            if sym:
                bands[sym] = {
                    "series":  series,
                    "circuit": int(band) if band.isdigit() else band,
                }

        changes = {}
        for row in chg_rows:
            series = row.get("Series", "").strip()
            if series not in ("EQ", "BE"):
                continue
            sym = row.get("Symbol", "").strip()
            frm = row.get("From", "").strip()
            to  = row.get("To", "").strip()
            if sym:
                changes[sym] = {
                    "from": int(frm) if frm.isdigit() else frm,
                    "to":   int(to)  if to.isdigit()  else to,
                    "series": series,
                }

        for sym, chg in changes.items():
            if sym in bands:
                bands[sym]["change"] = chg
            else:
                bands[sym] = {"series": "?", "circuit": chg["from"], "change": chg}

        # ── Merge delivery % into bands — only when bhavdata is for the same
        #    trading day as sec_list, to avoid mixing stale delivery data with
        #    a fresh circuit-band snapshot.
        deliv_merged = False
        if deliv_map and bhav_date == sec_date:
            for sym, dv in deliv_map.items():
                if sym in bands:
                    bands[sym]["deliv_qty"] = dv["deliv_qty"]
                    bands[sym]["deliv_per"] = dv["deliv_per"]
            deliv_merged = True
        elif deliv_map:
            print(f"  ⚠ deliv data date ({bhav_date}) != bands date ({sec_date}) — "
                  f"not merging into bands this run, stored separately below")

        bands_out = {
            "date":             sec_date.isoformat(),
            "next_trading_day": chg_date.isoformat(),
            "data":             [{"symbol": s, **v} for s, v in sorted(bands.items())],
        }
        print(f"\n  Bands: {len(bands)} symbols  |  Changes: {len(changes)}  |  "
              f"Deliv merged: {deliv_merged}")

        # ── Process bulk/block ────────────────────────────────────────────────────
        def clean_deals(rows, has_remarks=False):
            out = []
            for r in rows:
                sym = r.get("Symbol", "").strip()
                if not sym or sym.upper() in ("NO RECORDS", "SYMBOL", "-"):
                    continue
                d = {
                    "date":   r.get("Date", "").strip(),
                    "symbol": sym,
                    "name":   r.get("Security Name", "").strip(),
                    "client": r.get("Client Name", "").strip(),
                    "side":   r.get("Buy/Sell", "").strip(),
                    "qty":    r.get("Quantity Traded", "").strip(),
                    "price":  r.get("Trade Price / Wght. Avg. Price", "").strip(),
                }
                if has_remarks:
                    d["remarks"] = r.get("Remarks", "").strip()
                out.append(d)
            return out

        bulk_clean   = clean_deals(bulk_rows, has_remarks=True)
        block_clean  = clean_deals(block_rows)
        bulk_summary  = build_bulk_summary(bulk_clean)
        block_summary = build_block_summary(block_clean)
        print(f"  Bulk: {len(bulk_clean)}  Block: {len(block_clean)}")

        # ── Process participant OI ─────────────────────────────────────────────────
        oi_snapshot = None
        if oi_rows:
            oi_snapshot = parse_participant_oi(oi_rows, oi_date)
            fii = oi_snapshot["participants"].get("FII", {})
            dii = oi_snapshot["participants"].get("DII", {})
            print(f"  OI parsed — FII net: {fii.get('net', 0):+,}  DII net: {dii.get('net', 0):+,}")
        else:
            print("  ⚠ No OI rows — skipping OI upload")

        # ── Process delivery % snapshot ────────────────────────────────────────────
        deliv_snapshot = None
        if deliv_map and bhav_date:
            deliv_snapshot = {"date": bhav_date.isoformat(), "data": deliv_map}

        # ── Upload to R2 ──────────────────────────────────────────────────────────
        print("\n[7] Uploading to R2...")
        async with httpx.AsyncClient() as client:

            # ── Fetch all existing snapshots + histories in one parallel round-trip
            (
                existing_bands,
                existing_bulk,
                existing_block,
                bulk_hist,
                block_hist,
                band_changes_hist,
                oi_hist,
                fii_dii_hist,
                deliv_hist,
            ) = await asyncio.gather(
                r2_get(client, "nse_bands.json"),
                r2_get(client, "nse_bulk.json"),
                r2_get(client, "nse_block.json"),
                r2_get(client, "nse_bulk_history.json"),
                r2_get(client, "nse_block_history.json"),
                r2_get(client, "nse_band_changes_history.json"),
                r2_get(client, "nse_participant_oi_hist.json"),
                r2_get(client, "nse_fii_dii_hist.json"),
                r2_get(client, "nse_deliv_hist.json"),
            )

            bulk_hist          = bulk_hist          or {}
            block_hist         = block_hist         or {}
            band_changes_hist  = band_changes_hist  or {}
            oi_hist            = oi_hist            or []
            fii_dii_hist       = fii_dii_hist       or []
            deliv_hist         = deliv_hist         or []

            upload_tasks = []

            # ── Bands ─────────────────────────────────────────────────────────────
            bands_date_str = sec_date.isoformat()
            if existing_bands and existing_bands.get("date") == bands_date_str:
                print(f"  ✓ bands: {bands_date_str} already current — skipping")
                print(f"    Showing: {existing_bands['date']}  ({len(existing_bands.get('data', []))} symbols)")
            else:
                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_bands.json", bands_out,
                                                           schema_v=1, extra_meta={"symbol_count": len(bands)},
                                                           ensure_ascii=False))

            # ── Bulk snapshot + summary + history ─────────────────────────────────
            bulk_date_str       = bulk_clean[0]["date"] if bulk_clean else None
            existing_bulk_date  = existing_bulk[0]["date"] if existing_bulk else None

            if bulk_date_str and bulk_date_str == existing_bulk_date:
                print(f"  ✓ bulk: {bulk_date_str} already current — skipping snapshot + history")
            else:
                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_bulk.json", bulk_clean,
                                                           schema_v=1, extra_meta={"deal_count": len(bulk_clean)},
                                                           ensure_ascii=False))
                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_bulk_summary.json", bulk_summary,
                                                           schema_v=1, extra_meta={"row_count": len(bulk_summary)},
                                                           ensure_ascii=False))
                b1 = merge_deals_history(bulk_hist, bulk_clean)
                print(f"  Bulk history: +{b1} new deals")
                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_bulk_history.json", bulk_hist,
                                                           schema_v=1, ensure_ascii=False))

            # ── Block snapshot + summary + history ────────────────────────────────
            block_date_str      = block_clean[0]["date"] if block_clean else None
            existing_block_date = existing_block[0]["date"] if existing_block else None

            if not block_clean:
                print("  ✓ block: no deals today — skipping")
            elif block_date_str and block_date_str == existing_block_date:
                print(f"  ✓ block: {block_date_str} already current — skipping snapshot + history")
            else:
                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_block.json", block_clean,
                                                           schema_v=1, extra_meta={"deal_count": len(block_clean)},
                                                           ensure_ascii=False))
                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_block_summary.json", block_summary,
                                                           schema_v=1, extra_meta={"row_count": len(block_summary)},
                                                           ensure_ascii=False))
                b2 = merge_deals_history(block_hist, block_clean)
                print(f"  Block history: +{b2} new deals")
                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_block_history.json", block_hist,
                                                           schema_v=1, ensure_ascii=False))

            # ── Band-change history ─────────────────────────────────────────────────
            if changes:
                effective_date_str = chg_date.isoformat()
                b3 = merge_band_changes_history(band_changes_hist, changes, effective_date_str)
                if b3:
                    print(f"  Band-change history: +{b3} new entries ({effective_date_str})")
                    upload_tasks.append(upload_with_manifest(
                        client, r2_put, "nse_band_changes_history.json", band_changes_hist,
                        schema_v=1, extra_meta={"symbol_count": len(band_changes_hist)},
                        ensure_ascii=False))
                else:
                    print(f"  ✓ Band-change history: {effective_date_str} already present — unchanged")
            else:
                print("  ✓ Band-change history: no changes today — skipping")

            # ── Participant OI snapshot + history ─────────────────────────────────
            if oi_snapshot:
                oi_date_str = oi_snapshot["date"]

                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_participant_oi.json", oi_snapshot,
                                                           schema_v=1, ensure_ascii=False))

                existing_oi_dates = {e["date"] for e in oi_hist}
                if oi_date_str in existing_oi_dates:
                    print(f"  ✓ OI history: {oi_date_str} already present — history unchanged")
                    if oi_hist:
                        print(f"    Data available: {len(oi_hist)} days  "
                              f"({oi_hist[0]['date']} → {oi_hist[-1]['date']})")
                else:
                    oi_hist = merge_oi_history(oi_hist, oi_snapshot, max_days=OI_HISTORY_DAYS)
                    upload_tasks.append(upload_with_manifest(client, r2_put, "nse_participant_oi_hist.json", oi_hist,
                                                               schema_v=1, extra_meta={"day_count": len(oi_hist)},
                                                               ensure_ascii=False))
            else:
                print("  ⚠ No OI snapshot — skipping OI uploads")

            # ── FII/DII cash snapshot + history ─────────────────────────────────────
            if fii_dii_snapshot and fii_dii_snapshot.get("date"):
                fd_date_str = fii_dii_snapshot["date"]

                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_fii_dii.json", fii_dii_snapshot,
                                                           schema_v=1, ensure_ascii=False))

                existing_fd_dates = {e["date"] for e in fii_dii_hist}
                if fd_date_str in existing_fd_dates:
                    print(f"  ✓ FII/DII history: {fd_date_str} already present — history unchanged")
                    if fii_dii_hist:
                        print(f"    Data available: {len(fii_dii_hist)} days  "
                              f"({fii_dii_hist[0]['date']} → {fii_dii_hist[-1]['date']})")
                else:
                    fii_dii_hist = merge_oi_history(fii_dii_hist, fii_dii_snapshot, max_days=FII_DII_HISTORY_DAYS)
                    upload_tasks.append(upload_with_manifest(client, r2_put, "nse_fii_dii_hist.json", fii_dii_hist,
                                                               schema_v=1, extra_meta={"day_count": len(fii_dii_hist)},
                                                               ensure_ascii=False))
            else:
                print("  ⚠ No FII/DII snapshot — skipping FII/DII uploads")

            # ── Delivery % snapshot + rolling 30-day history ────────────────────────
            if deliv_snapshot and deliv_snapshot.get("date"):
                dv_date_str = deliv_snapshot["date"]

                upload_tasks.append(upload_with_manifest(client, r2_put, "nse_deliv.json", deliv_snapshot,
                                                           schema_v=1, extra_meta={"symbol_count": len(deliv_map)},
                                                           ensure_ascii=False))

                existing_dv_dates = {e["date"] for e in deliv_hist}
                if dv_date_str in existing_dv_dates:
                    print(f"  ✓ Deliv history: {dv_date_str} already present — history unchanged")
                    if deliv_hist:
                        print(f"    Data available: {len(deliv_hist)} days  "
                              f"({deliv_hist[0]['date']} → {deliv_hist[-1]['date']})")
                else:
                    deliv_hist = merge_oi_history(deliv_hist, deliv_snapshot, max_days=DELIV_HISTORY_DAYS)
                    upload_tasks.append(upload_with_manifest(client, r2_put, "nse_deliv_hist.json", deliv_hist,
                                                               schema_v=1, extra_meta={"day_count": len(deliv_hist)},
                                                               ensure_ascii=False))
            else:
                print("  ⚠ No delivery snapshot — skipping delivery uploads")

            # ── Fire all pending uploads in parallel ──────────────────────────────
            if upload_tasks:
                await asyncio.gather(*upload_tasks)
            else:
                print("  ✓ All data already current — nothing to upload")

        # ── Circuit change Telegram alert ──────────────────────────────
        if changes:
            increased = {s: c for s, c in changes.items() if _band_dir(c) == "up"}
            decreased = {s: c for s, c in changes.items() if _band_dir(c) == "down"}
            msg_lines = ["🔔 <b>NSE Circuit Changes</b>", f"Next day: <code>{chg_date.isoformat()}</code>"]
            if increased:
                msg_lines.append("")
                msg_lines.append("🟢 <b>Band Increased</b>")
                for sym, chg in sorted(increased.items(), key=lambda x: int(x[1].get("to", 0)), reverse=True):
                    msg_lines.append(f"<code>{sym}</code>: {chg.get('from')}% → {chg.get('to')}%")
            if decreased:
                msg_lines.append("")
                msg_lines.append("🔴 <b>Band Decreased</b>")
                for sym, chg in sorted(decreased.items(), key=lambda x: int(x[1].get("to", 0)), reverse=True):
                    msg_lines.append(f"<code>{sym}</code>: {chg.get('from')}% → {chg.get('to')}%")
            send_message("\n".join(msg_lines))

        status.set("bands", len(bands))
        status.set("circuit_changes", len(changes))
        status.set("bulk_deals", len(bulk_clean))
        status.set("block_deals", len(block_clean))
        status.set("deliv_symbols", len(deliv_map))
        status.success()
        print("\n✅ pipeline_nse.py complete")
    except Exception as e:
        status.failure(e)



if __name__ == "__main__":
    asyncio.run(run())
