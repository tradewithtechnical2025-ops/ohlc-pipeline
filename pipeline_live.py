"""
pipeline_live.py
Fetches live OHLC from Upstox v3 for all stocks in master.json
- OHLC endpoint: open, high, low, volume (live_ohlc)
- LTP endpoint:  last_price + cp (prev close) → Change% ke liye
- Pre-open window (9:00–9:08 AM IST): captures indicative LTP vs prev close
  as a frozen "preopen_gap" % for gap-up/gap-down screening.
Uploads result as live_ohlc.json to R2 via Cloudflare Worker
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

UPSTOX_TOKEN = os.environ["UPSTOX_TOKEN"]
WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

WORKER_HEADERS   = {"X-Secret-Token": WORKER_TOKEN}
UPSTOX_OHLC_URL  = "https://api.upstox.com/v3/market-quote/ohlc"
UPSTOX_LTP_URL   = "https://api.upstox.com/v3/market-quote/ltp"
BATCH_SIZE = 500
INTERVAL   = "1d"
IST        = ZoneInfo("Asia/Kolkata")

# Pre-open call auction window (IST), expressed as minutes-since-midnight.
# NSE pre-open mechanics: 09:00-09:08 order entry, 09:08-09:12 order
# matching, 09:12-09:15 buffer — price stays at the settled pre-open
# equilibrium the WHOLE time until continuous trading starts at 09:15.
# So it's safe (and correct) to keep this window open through 09:14,
# which also gives plenty of slack for a GitHub Actions cron run that
# fires a few minutes late.
PREOPEN_START_MIN = 8 * 60 + 58    # 08:58 — a little early buffer
PREOPEN_END_MIN   = 9 * 60 + 14    # 09:14 — right up to continuous trading open


# ── R2 helpers ────────────────────────────────────────────────────────────────

async def r2_download(client, filename):
    r = await client.get(
        f"{WORKER_URL}/{filename}",
        headers=WORKER_HEADERS,
        timeout=120,
    )
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"Download failed {filename}: HTTP {r.status_code}")
    return r.json()


async def r2_upload(client, filename, data):
    payload = json.dumps(data, separators=(",", ":")).encode()
    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers={**WORKER_HEADERS, "Content-Type": "application/json"},
        content=payload,
        timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Upload failed {filename}: HTTP {r.status_code}")
    log.info(f"  ↑ {filename} ({len(payload)/1024:.1f} KB)")


# ── Upstox API batch fetch ────────────────────────────────────────────────────

async def fetch_batch(client, url, ikeys: list[str], extra_params: dict = {}) -> dict:
    params  = {"instrument_key": ",".join(ikeys), **extra_params}
    headers = {
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
        "Accept": "application/json",
    }
    r = await client.get(url, params=params, headers=headers, timeout=30)

    if r.status_code == 401:
        log.error("❌ UPSTOX_TOKEN invalid")
        raise SystemExit(1)
    if r.status_code == 429:
        log.warning("  429 — sleeping 10s")
        await asyncio.sleep(10)
        return {}
    if r.status_code != 200:
        log.warning(f"  HTTP {r.status_code} from {url}")
        return {}

    return r.json().get("data", {})


def in_preopen_window(ts_str: str, fallback: bool) -> bool:
    """
    Prefer the exchange-side quote timestamp (accurate, immune to our own
    script's scheduling/network delay) over the script's wall-clock time.
    Falls back to the wall-clock check if the timestamp is missing/unparseable.
    """
    if not ts_str:
        return fallback
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        dt_ist = dt.astimezone(IST)
        mins = dt_ist.hour * 60 + dt_ist.minute
        return PREOPEN_START_MIN <= mins < PREOPEN_END_MIN
    except Exception:
        return fallback


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    now_ist = datetime.now(IST)
    today   = now_ist.strftime("%Y-%m-%d")
    now_min = now_ist.hour * 60 + now_ist.minute
    in_preopen = PREOPEN_START_MIN <= now_min < PREOPEN_END_MIN
    log.info(f"━━━ Live OHLC Pipeline — {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')} ━━━")
    if in_preopen:
        log.info("  🔔 Pre-open window active (08:58–09:14) — capturing gap snapshot")

    async with httpx.AsyncClient() as client:

        # 1. Load master + ikey_map + old live_ohlc (parallel)
        log.info("Downloading master.json, ikey_map.json, live_ohlc.json…")
        master_raw, ikey_map_raw, old_ohlc = await asyncio.gather(
            r2_download(client, "master.json"),
            r2_download(client, "ikey_map.json"),
            r2_download(client, "live_ohlc.json"),
        )

        master  = master_raw or []
        nse_map = (ikey_map_raw or {}).get("nse", {})
        bse_map = (ikey_map_raw or {}).get("bse", {})
        log.info(f"  {len(master)} stocks in master.json")

        # 2. Prev close fallback from old live_ohlc (2-day persistence)
        old_data   = (old_ohlc or {}).get("data", {})
        old_date   = (old_ohlc or {}).get("date", "")
        is_new_day = bool(old_date and old_date != today)

        prev_close_map = {}
        preopen_map    = {}  # sym → {"preopen_price":.., "preopen_gap":..} frozen snapshot
        for sym, d in old_data.items():
            prev_close_map[sym] = d.get("c") if is_new_day else d.get("pc")
            # Pre-open snapshot only carries forward within the SAME trading day.
            # On a new day it resets (no stale gap % from yesterday's pre-open).
            if not is_new_day and d.get("preopen_gap") is not None:
                preopen_map[sym] = {
                    "preopen_price": d.get("preopen_price"),
                    "preopen_gap"  : d.get("preopen_gap"),
                }

        if is_new_day:
            log.info(f"  New day (prev={old_date}) → {len(prev_close_map)} prev closes carried")
        elif old_date:
            log.info(f"  Same day refresh → {len(prev_close_map)} pc values carried, "
                      f"{len(preopen_map)} pre-open gaps carried")

        # 3. symbol → instrument_key
        sym_to_ikey = {}
        for stock in master:
            sym  = stock.get("symbol", "")
            ikey = nse_map.get(sym) or bse_map.get(sym)
            if sym and ikey:
                sym_to_ikey[sym] = ikey

        log.info(f"  Resolved {len(sym_to_ikey)}/{len(master)} symbols")

        # ikey (colon format) → symbol reverse map
        ikey_to_sym = {v.replace("|", ":"): k for k, v in sym_to_ikey.items()}
        ikeys = list(sym_to_ikey.values())

        # 4. Batch fetch OHLC + LTP together per batch
        result = {}
        total_batches = (len(ikeys) + BATCH_SIZE - 1) // BATCH_SIZE
        preopen_captured = 0

        for i in range(0, len(ikeys), BATCH_SIZE):
            batch     = ikeys[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            log.info(f"  Batch {batch_num}/{total_batches} — {len(batch)} stocks…")

            # Fetch OHLC and LTP in parallel for this batch
            ohlc_raw, ltp_raw = await asyncio.gather(
                fetch_batch(client, UPSTOX_OHLC_URL, batch, {"interval": INTERVAL}),
                fetch_batch(client, UPSTOX_LTP_URL,  batch),
            )

            # Build ltp lookup: ikey_colon → ltp_data
            ltp_lookup = {}
            for resp_key, ltp_data in ltp_raw.items():
                itoken = ltp_data.get("instrument_token", "").replace("|", ":")
                ltp_lookup[itoken] = ltp_data

            for resp_key, ohlc_data in ohlc_raw.items():
                itoken = ohlc_data.get("instrument_token", "").replace("|", ":")
                sym    = ikey_to_sym.get(itoken)
                if not sym:
                    continue

                live = ohlc_data.get("live_ohlc") or {}
                ltp_info = ltp_lookup.get(itoken, {})

                # cp from LTP endpoint = previous day close ✅
                # fallback = our persisted prev_close from yesterday
                pc = ltp_info.get("cp") or prev_close_map.get(sym)

                ltp_price = ltp_info.get("last_price") or ohlc_data.get("last_price")

                # ── Pre-open gap snapshot ──────────────────────────────────
                # Use this specific quote's exchange timestamp to decide if
                # it's a pre-open snapshot — more reliable than the script's
                # own wall-clock time, which can drift if this run's cron
                # trigger or network fetch was delayed. Falls back to the
                # wall-clock check (in_preopen) if the timestamp is missing.
                ts_val = ohlc_data.get("timestamp", "")
                stock_in_preopen = in_preopen_window(ts_val, in_preopen)

                # During the window, the call-auction indicative price keeps
                # refining every run — we take the latest one each cycle.
                # After the window, we freeze whatever was last captured
                # (don't let it drift with regular-session LTP).
                if stock_in_preopen and ltp_price and pc:
                    preopen_price = ltp_price
                    preopen_gap   = round((preopen_price - pc) / pc * 100, 2)
                    preopen_captured += 1
                else:
                    prev = preopen_map.get(sym, {})
                    preopen_price = prev.get("preopen_price")
                    preopen_gap   = prev.get("preopen_gap")

                result[sym] = {
                    "o"  : live.get("open"),
                    "h"  : live.get("high"),
                    "l"  : live.get("low"),
                    "c"  : ltp_price,
                    "pc" : pc,
                    "vol": live.get("volume"),
                    "ts" : ohlc_data.get("timestamp", ""),
                    "preopen_price": preopen_price,
                    "preopen_gap"  : preopen_gap,
                }

            if i + BATCH_SIZE < len(ikeys):
                await asyncio.sleep(1)

        pc_filled = sum(1 for d in result.values() if d.get("pc"))
        gap_filled = sum(1 for d in result.values() if d.get("preopen_gap") is not None)
        log.info(f"  Fetched {len(result)} stocks  (pc available: {pc_filled}, "
                  f"preopen_gap available: {gap_filled})")
        if in_preopen:
            log.info(f"  🔔 Captured pre-open gap for {preopen_captured} stocks this run")

        # 5. Upload
        payload = {
            "updated_at": now_ist.isoformat(),
            "date"      : today,
            "count"     : len(result),
            "data"      : result,
        }
        await r2_upload(client, "live_ohlc.json", payload)

    log.info("✅ live_ohlc.json uploaded")
    log.info("━━━ Done ━━━")


if __name__ == "__main__":
    asyncio.run(run())
