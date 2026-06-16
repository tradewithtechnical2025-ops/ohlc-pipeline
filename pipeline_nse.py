"""
pipeline_nse.py
Fetches from NSE archives:
  - sec_list_DDMMYYYY.csv           → today's circuit bands (EQ+BE only)
  - eq_band_changes_DDMMYYYY.csv    → next trading day's changes (EQ+BE only)
  - bulk.csv                        → latest bulk deals
  - block.csv                       → latest block deals
  - fao_participant_oi_DDMMYYYY.csv → participant-wise F&O OI (FII/DII/Client/Pro)

Saves to R2 via Worker (FLAT keys — no slashes, worker slash-rejection safe):
  nse_bands.json              → EQ+BE symbols with current band + next-day change if any
  nse_bulk.json               → latest bulk deals
  nse_block.json              → latest block deals
  nse_bulk_history.json       → symbol-wise accumulated bulk history
  nse_block_history.json      → symbol-wise accumulated block history
  nse_participant_oi.json     → latest single-day participant OI snapshot
  nse_participant_oi_hist.json → rolling 60-day participant OI history (sorted asc)
"""

import asyncio
import csv
import io
import json
import os
import time
from datetime import date, timedelta

import httpx

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

NSE_BASE  = "https://archives.nseindia.com/content/equities"
NSCCL_BASE = "https://archives.nseindia.com/content/nsccl"

NSE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Referer":         "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

OI_HISTORY_DAYS = 60   # rolling window

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
# Participant OI parser
# ─────────────────────────────────────────────────────────────────────────────

# Column order in the CSV (after stripping whitespace from header):
#   Client Type,
#   Future Index Long, Future Index Short,
#   Future Stock Long, Future Stock Short,
#   Option Index Call Long, Option Index Put Long,
#   Option Index Call Short, Option Index Put Short,
#   Option Stock Call Long, Option Stock Put Long,
#   Option Stock Call Short, Option Stock Put Short,
#   Total Long Contracts, Total Short Contracts

_OI_PARTICIPANTS = {"Client", "DII", "FII", "Pro"}

def parse_participant_oi(rows: list[dict], as_of: date) -> dict:
    """
    Returns a dict ready to store:
    {
      "date": "2026-06-15",
      "participants": {
        "FII":    { "fut_idx_long": ..., "fut_idx_short": ..., ... , "total_long": ..., "total_short": ..., "net": ... },
        "DII":    { ... },
        "Client": { ... },
        "Pro":    { ... },
      }
    }
    """
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
            # Futures
            "fut_idx_long":   fut_idx_long,
            "fut_idx_short":  fut_idx_short,
            "fut_stk_long":   fut_stk_long,
            "fut_stk_short":  fut_stk_short,
            # Index options
            "oi_call_long":   oi_call_long,
            "oi_put_long":    oi_put_long,
            "oi_call_short":  oi_call_short,
            "oi_put_short":   oi_put_short,
            # Stock options
            "os_call_long":   os_call_long,
            "os_put_long":    os_put_long,
            "os_call_short":  os_call_short,
            "os_put_short":   os_put_short,
            # Totals
            "total_long":     total_long,
            "total_short":    total_short,
            "net":            total_long - total_short,
            # Derived: futures-only net (cleaner directional signal)
            "fut_net":        (fut_idx_long + fut_stk_long) - (fut_idx_short + fut_stk_short),
            # Index futures net (most watched)
            "fut_idx_net":    fut_idx_long - fut_idx_short,
        }

    return {
        "date":         as_of.isoformat(),
        "participants": participants,
    }


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
    hist: list of daily OI dicts sorted ascending by date
    new_entry: parsed dict for today (has "date" key)
    Returns updated list with:
      - duplicate dates replaced (idempotent re-runs)
      - oldest entries pruned so only max_days remain
    """
    target_date = new_entry["date"]

    # Replace existing entry for same date, or append
    existing_dates = [e["date"] for e in hist]
    if target_date in existing_dates:
        idx = existing_dates.index(target_date)
        hist[idx] = new_entry
        print(f"  OI history: updated existing entry for {target_date}")
    else:
        hist.append(new_entry)
        print(f"  OI history: added new entry for {target_date}")

    # Sort ascending
    hist.sort(key=lambda x: x["date"])

    # Prune to rolling window
    if len(hist) > max_days:
        removed = len(hist) - max_days
        hist = hist[-max_days:]
        print(f"  OI history: pruned {removed} old entries, keeping {len(hist)} days")

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


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def run():
    today    = date.today()
    next_day = next_trading_day(today)
    print(f"Today: {today}  |  Next trading day: {next_day}")

    nse = make_nse_session()

    # ── 1. sec_list — today's circuit bands ───────────────────────────────────
    print("\n[1] sec_list (today's bands)...")
    sec_rows, sec_date = fetch_with_fallback(
        nse, f"{NSE_BASE}/sec_list_{{}}.csv", today, direction="prev"
    )

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
        oi_rows, oi_date = fetch_with_fallback(
            nse,
            f"{NSCCL_BASE}/fao_participant_oi_{{}}.csv",
            today,
            direction="prev",
            max_tries=5,
        )
        print(f"  Participant OI date resolved to: {oi_date}")
    except RuntimeError:
        print("  ⚠ fao_participant_oi not available — skipping OI update")
        oi_rows, oi_date = [], today

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
            }

    for sym, chg in changes.items():
        if sym in bands:
            bands[sym]["change"] = chg
        else:
            bands[sym] = {"series": "?", "circuit": chg["from"], "change": chg}

    bands_out = {
        "date":             sec_date.isoformat(),
        "next_trading_day": chg_date.isoformat(),
        "data":             [{"symbol": s, **v} for s, v in sorted(bands.items())],
    }
    print(f"\n  Bands: {len(bands)} symbols  |  Changes: {len(changes)}")

    # ── Process bulk/block ────────────────────────────────────────────────────
    def clean_deals(rows, has_remarks=False):
        out = []
        for r in rows:
            sym = r.get("Symbol", "").strip()
            if not sym or sym == "NO RECORDS":
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

    # ── Upload to R2 ──────────────────────────────────────────────────────────
    print("\n[6] Uploading to R2...")
    async with httpx.AsyncClient() as client:

        # ── Fetch all existing snapshots + histories in one parallel round-trip
        (
            existing_bands,
            existing_bulk,
            existing_block,
            bulk_hist,
            block_hist,
            oi_hist,
        ) = await asyncio.gather(
            r2_get(client, "nse_bands.json"),
            r2_get(client, "nse_bulk.json"),
            r2_get(client, "nse_block.json"),
            r2_get(client, "nse_bulk_history.json"),
            r2_get(client, "nse_block_history.json"),
            r2_get(client, "nse_participant_oi_hist.json"),
        )

        bulk_hist  = bulk_hist  or {}
        block_hist = block_hist or {}
        oi_hist    = oi_hist    or []

        upload_tasks = []

        # ── Bands ─────────────────────────────────────────────────────────────
        bands_date_str = sec_date.isoformat()
        if existing_bands and existing_bands.get("date") == bands_date_str:
            print(f"  ✓ bands: {bands_date_str} already current — skipping")
            print(f"    Showing: {existing_bands['date']}  ({len(existing_bands.get('data', []))} symbols)")
        else:
            upload_tasks.append(r2_put(client, "nse_bands.json", bands_out))

        # ── Bulk snapshot + summary + history ─────────────────────────────────
        # bulk.csv carries date inside each row; use first row's date
        bulk_date_str       = bulk_clean[0]["date"] if bulk_clean else None
        existing_bulk_date  = existing_bulk[0]["date"] if existing_bulk else None

        if bulk_date_str and bulk_date_str == existing_bulk_date:
            print(f"  ✓ bulk: {bulk_date_str} already current — skipping snapshot + history")
        else:
            upload_tasks.append(r2_put(client, "nse_bulk.json",         bulk_clean))
            upload_tasks.append(r2_put(client, "nse_bulk_summary.json", bulk_summary))
            b1 = merge_deals_history(bulk_hist, bulk_clean)
            print(f"  Bulk history: +{b1} new deals")
            upload_tasks.append(r2_put(client, "nse_bulk_history.json", bulk_hist))

        # ── Block snapshot + summary + history ────────────────────────────────
        block_date_str      = block_clean[0]["date"] if block_clean else None
        existing_block_date = existing_block[0]["date"] if existing_block else None

        if block_date_str and block_date_str == existing_block_date:
            print(f"  ✓ block: {block_date_str} already current — skipping snapshot + history")
        else:
            upload_tasks.append(r2_put(client, "nse_block.json",         block_clean))
            upload_tasks.append(r2_put(client, "nse_block_summary.json", block_summary))
            b2 = merge_deals_history(block_hist, block_clean)
            print(f"  Block history: +{b2} new deals")
            upload_tasks.append(r2_put(client, "nse_block_history.json", block_hist))

        # ── Participant OI snapshot + history ─────────────────────────────────
        if oi_snapshot:
            oi_date_str = oi_snapshot["date"]

            # Snapshot always uploaded so frontend shows latest available date
            upload_tasks.append(r2_put(client, "nse_participant_oi.json", oi_snapshot))

            # History — only append if genuinely new trading day
            existing_oi_dates = {e["date"] for e in oi_hist}
            if oi_date_str in existing_oi_dates:
                print(f"  ✓ OI history: {oi_date_str} already present — history unchanged")
                if oi_hist:
                    print(f"    Data available: {len(oi_hist)} days  "
                          f"({oi_hist[0]['date']} → {oi_hist[-1]['date']})")
            else:
                oi_hist = merge_oi_history(oi_hist, oi_snapshot, max_days=OI_HISTORY_DAYS)
                upload_tasks.append(r2_put(client, "nse_participant_oi_hist.json", oi_hist))
        else:
            print("  ⚠ No OI snapshot — skipping OI uploads")

        # ── Fire all pending uploads in parallel ──────────────────────────────
        if upload_tasks:
            await asyncio.gather(*upload_tasks)
        else:
            print("  ✓ All data already current — nothing to upload")

    print("\n✅ pipeline_nse.py complete")


if __name__ == "__main__":
    asyncio.run(run())
