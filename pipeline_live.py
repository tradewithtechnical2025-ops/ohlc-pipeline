"""
pipeline_live.py
Fetches live OHLC from Upstox v3 for all stocks in master.json
- OHLC endpoint: open, high, low, volume (live_ohlc)
- LTP endpoint:  last_price + cp (prev close) → Change% ke liye
- Pre-open window (8:58–9:14 AM IST): captures indicative LTP vs prev close
  as a frozen "preopen_gap" % for gap-up/gap-down screening.
Uploads result as live_ohlc.json to R2 via Cloudflare Worker.

Pre-open gap snapshot is ALSO persisted separately as preopen_gaps.json —
this makes it immune to any partial/transient failure in the main
live_ohlc.json fetch/upload cycle (e.g. one Upstox batch failing mid-day
no longer wipes the whole day's gap data for the rest of the session).
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

PREOPEN_FILE = "preopen_gaps.json"   # separate, resilient store for pre-open snapshot

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

        # 1. Load master + ikey_map + old live_ohlc + old preopen snapshot (parallel)
        log.info("Downloading master.json, ikey_map.json, live_ohlc.json, preopen_gaps.json…")
        master_raw, ikey_map_raw, old_ohlc, old_preopen = await asyncio.gather(
            r2_download(client, "master.json"),
            r2_download(client, "ikey_map.json"),
            r2_download(client, "live_ohlc.json"),
            r2_download(client, PREOPEN_FILE),
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
        for sym, d in old_data.items():
            prev_close_map[sym] = d.get("c") if is_new_day else d.get("pc")

        if is_new_day:
            log.info(f"  New day (prev={old_date}) → {len(prev_close_map)} prev closes carried")
        elif old_date:
            log.info(f"  Same day refresh → {len(prev_close_map)} pc values carried")

        # 2b. Pre-open snapshot — sourced from its OWN file, independent of
        #     whatever happened to live_ohlc.json in any given run. This is
        #     the fix: a partial/failed live_ohlc fetch no longer wipes the
        #     whole day's frozen gap data.
        old_preopen_data = (old_preopen or {}).get("data", {})
        old_preopen_date = (old_preopen or {}).get("date", "")
        preopen_is_new_day = bool(old_preopen_date and old_preopen_date != today)

        # preopen_map is the running truth: starts as a copy of whatever we
        # already had for today, and only gets overwritten per-symbol below
        # when this run actually captures/confirms a value. Symbols not
        # touched this run (e.g. dropped from a failed Upstox batch) simply
        # keep their previously known snapshot.
        preopen_map = {} if preopen_is_new_day else dict(old_preopen_data)

        if preopen_is_new_day:
            log.info(f"  New day → pre-open snapshot reset (prev={old_preopen_date})")
        elif old_preopen_date:
            log.info(f"  Pre-open snapshot loaded → {len(preopen_map)} gaps carried from store")

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
                    preopen_map[sym] = {
                        "preopen_price": preopen_price,
                        "preopen_gap"  : preopen_gap,
                    }
                else:
                    prev = preopen_map.get(sym, {})
                    preopen_price = prev.get("preopen_price")
                    preopen_gap   = prev.get("preopen_gap")
                    # NOTE: we deliberately do NOT delete/overwrite preopen_map[sym]
                    # here even if prev is empty — there's nothing to carry, and
                    # leaving it absent is correct (no stale/fabricated value).

                gap_source = "preopen" if preopen_gap is not None else None

                # ── Fallback: derive gap from today's open vs prev close ────
                # If the true pre-open snapshot is unavailable for any reason
                # (store lost/reset, symbol missed the window, etc.), fall
                # back to (open - pc)/pc once the day's open print exists.
                # This is a slightly different number in principle — it's the
                # actual 09:15 opening trade, not the pre-open call-auction
                # equilibrium — but in practice they're almost always equal
                # since the auction sets the opening price. Only used when the
                # real pre-open value is missing, and flagged via gap_source
                # so the frontend can tell the two apart if it wants to.
                open_px = live.get("open")
                if preopen_gap is None and open_px and pc:
                    preopen_price = open_px
                    preopen_gap   = round((open_px - pc) / pc * 100, 2)
                    gap_source    = "open_fallback"

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
                    "gap_source"   : gap_source,
                }

            if i + BATCH_SIZE < len(ikeys):
                await asyncio.sleep(1)

        pc_filled = sum(1 for d in result.values() if d.get("pc"))
        gap_filled = sum(1 for d in result.values() if d.get("preopen_gap") is not None)
        log.info(f"  Fetched {len(result)} stocks  (pc available: {pc_filled}, "
                  f"preopen_gap available: {gap_filled})")
        if in_preopen:
            log.info(f"  🔔 Captured pre-open gap for {preopen_captured} stocks this run")

        # 5. Upload live_ohlc.json
        payload = {
            "updated_at": now_ist.isoformat(),
            "date"      : today,
            "count"     : len(result),
            "data"      : result,
        }
        await r2_upload(client, "live_ohlc.json", payload)

        # 5b. Upload preopen_gaps.json — the resilient, independent store.
        # Written every run (cheap, small file) so it self-heals: even if a
        # batch dropped some symbols from `result` this run, their entries
        # in preopen_map (carried from the previous store load) are untouched
        # and get re-persisted here regardless.
        preopen_payload = {
            "updated_at": now_ist.isoformat(),
            "date"      : today,
            "data"      : preopen_map,
        }
        await r2_upload(client, PREOPEN_FILE, preopen_payload)

    log.info("✅ live_ohlc.json + preopen_gaps.json uploaded")
    log.info("━━━ Done ━━━")


if __name__ == "__main__":
    asyncio.run(run())
