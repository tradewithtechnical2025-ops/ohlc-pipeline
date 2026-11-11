"""
backfill_vwap.py — one-time backfill of the yearly VWAP history (vwap_hist_YYYY.json)
from NSE bhavcopies (sec_bhavdata_full_DDMMYYYY.csv).

Per trading day and NSE stock it stores [AVG_PRICE, CLOSE_PRICE] — both RAW prices.
pipeline.py and pipeline_chart.py turn them into adjusted VWAP (see vwap_store.py).
From now on pipeline_master.py adds each new day from Upstox.

- NSE stocks only (BSE-only stocks are not in the NSE file).
- Only stocks in master.json are kept.
- Resumable: days already in a year file are skipped, so a re-run continues where it stopped.
- The day is taken from the file's own DATE1 column; a file NSE serves under a holiday date
  (a copy of the previous session) is skipped.

Run (needs WORKER_URL, WORKER_TOKEN; from the folder with pipeline_nse.py and vwap_store.py):
    BACKFILL_YEARS=5 python backfill_vwap.py
"""

import asyncio
import json
import os
from datetime import date, datetime, timedelta, timezone

import httpx

from pipeline_nse import BHAV_BASE, fetch_csv_url, fmt, make_nse_session
from vwap_store import year_file, years_back_floor

WORKER_URL     = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN   = os.environ["WORKER_TOKEN"]
MASTER_FILE    = "master.json"
BACKFILL_YEARS = int(os.environ.get("BACKFILL_YEARS", "5"))
SAVE_EVERY     = 60          # upload the year file every N new days (checkpoint)
IST            = timezone(timedelta(hours=5, minutes=30))


def parse_bhav(rows: list[dict], universe: set[str]) -> tuple[str | None, dict]:
    """-> (ISO date from DATE1, {SYMBOL: [avg_price, close_price]}) for EQ/BE rows.
    NSE's header has a leading space (' AVG_PRICE'), so keys and values are stripped."""
    out, day = {}, None
    for row in rows:
        r = {str(k).strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        if r.get("SERIES") not in ("EQ", "BE"):
            continue
        if day is None and r.get("DATE1"):
            for f in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
                try:
                    day = datetime.strptime(r["DATE1"], f).date().isoformat()
                    break
                except ValueError:
                    pass
        sym = (r.get("SYMBOL") or "").upper()
        if not sym or (universe and sym not in universe):
            continue
        try:
            avg = float(str(r.get("AVG_PRICE") or "0").replace(",", ""))
            cls = float(str(r.get("CLOSE_PRICE") or "0").replace(",", ""))
        except ValueError:
            continue
        if avg > 0 and cls > 0:
            out[sym] = [round(avg, 2), round(cls, 2)]
    return day, out


async def r2_get(client, name):
    r = await client.get(f"{WORKER_URL}/{name}", headers={"X-Secret-Token": WORKER_TOKEN}, timeout=180)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


async def r2_put(client, name, data):
    r = await client.post(f"{WORKER_URL}?file={name}",
                          headers={"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"},
                          content=json.dumps(data, separators=(",", ":")).encode(), timeout=300)
    r.raise_for_status()


async def save_year(client, year: int, days: dict):
    await r2_put(client, year_file(year), {"updated": datetime.now(IST).isoformat(timespec="seconds"),
                                           "fields": ["vwap", "close"], "days": dict(sorted(days.items()))})
    print(f"  ↑ {year_file(year)}: {len(days)} days")


async def main():
    async with httpx.AsyncClient() as client:
        master = await r2_get(client, MASTER_FILE) or []
        universe = {str(s.get("symbol") or "").upper() for s in master if s.get("symbol")}
        print(f"Universe from {MASTER_FILE}: {len(universe)} NSE symbols")

        now = datetime.now(IST)
        today = now.date()
        last = today if now.hour >= 21 else today - timedelta(days=1)   # today's file comes ~8 pm
        floor = date.fromisoformat(years_back_floor(today.isoformat(), BACKFILL_YEARS))
        print(f"Backfill {floor} → {last}")

        nse = make_nse_session()
        total_added = total_skipped = total_nofile = 0
        for year in range(last.year, floor.year - 1, -1):          # newest year first
            j = await r2_get(client, year_file(year))
            days = (j or {}).get("days") if isinstance(j, dict) else None
            days = days if isinstance(days, dict) else {}
            start = max(floor, date(year, 1, 1)); end = min(last, date(year, 12, 31))
            added = 0; cur = end
            print(f"\n{year}: {len(days)} day(s) already saved")
            while cur >= start:
                key = cur.isoformat()
                if cur.weekday() >= 5:
                    pass
                elif key in days:
                    total_skipped += 1
                else:
                    try:
                        rows = fetch_csv_url(nse, f"{BHAV_BASE}/sec_bhavdata_full_{fmt(cur)}.csv", retries=2)
                    except Exception as e:
                        rows = None
                        print(f"  ⚠ {key}: {e}")
                    if rows:
                        day, data = parse_bhav(rows, universe)
                        if day == key and data:
                            days[key] = data; added += 1; total_added += 1
                            if added % SAVE_EVERY == 0:
                                await save_year(client, year, days)
                        else:
                            total_nofile += 1
                            print(f"  · {key}: file is for {day} — holiday copy, skipped")
                    else:
                        total_nofile += 1
                    await asyncio.sleep(0.4)
                cur -= timedelta(days=1)
            if added:
                await save_year(client, year, days)
            print(f"{year}: +{added} day(s), now {len(days)}")
        nse.close()
        print(f"\n✅ VWAP history: +{total_added} day(s) from NSE bhavcopy, "
              f"{total_skipped} already present, {total_nofile} holidays / no file")


if __name__ == "__main__":
    asyncio.run(main())
