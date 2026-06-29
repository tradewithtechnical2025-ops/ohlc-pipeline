"""
pipeline_live.py
Fetches live/today's OHLC from Upstox for all stocks in master.json
Uploads result as live_ohlc.json to R2 via Cloudflare Worker
Run every 3-5 min during market hours via GitHub Actions
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

WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}
UPSTOX_OHLC_URL = "https://api.upstox.com/v2/market-quote/ohlc"
BATCH_SIZE = 500
INTERVAL   = "1d"
IST        = ZoneInfo("Asia/Kolkata")


# ── R2 helpers ────────────────────────────────────────────────────────────────

async def r2_download(client, filename):
    r = await client.get(
        f"{WORKER_URL}/{filename}",
        headers=WORKER_HEADERS,
        timeout=120,
    )
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


# ── Upstox OHLC ───────────────────────────────────────────────────────────────

async def fetch_ohlc_batch(client, ikeys: list[str]) -> dict:
    params  = {"instrument_key": ",".join(ikeys), "interval": INTERVAL}
    headers = {
        "Authorization": f"Bearer {UPSTOX_TOKEN}",
        "Accept": "application/json",
    }
    r = await client.get(UPSTOX_OHLC_URL, params=params, headers=headers, timeout=30)

    if r.status_code == 401:
        log.error("❌ UPSTOX_TOKEN invalid")
        raise SystemExit(1)

    if r.status_code == 429:
        log.warning("  429 — sleeping 10s")
        await asyncio.sleep(10)
        return {}

    if r.status_code != 200:
        log.warning(f"  OHLC batch HTTP {r.status_code}")
        return {}

    return r.json().get("data", {})


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    now_ist = datetime.now(IST)
    log.info(f"━━━ Live OHLC Pipeline — {now_ist.strftime('%Y-%m-%d %H:%M:%S IST')} ━━━")

    async with httpx.AsyncClient() as client:

        # 1. Load master.json
        log.info("Downloading master.json…")
        master = await r2_download(client, "master.json")
        log.info(f"  {len(master)} stocks")

        # 2. Load ikey_map.json
        log.info("Downloading ikey_map.json…")
        ikey_map_raw = await r2_download(client, "ikey_map.json")
        nse_map = ikey_map_raw.get("nse", {})
        bse_map = ikey_map_raw.get("bse", {})

        # 3. symbol → instrument_key (prefer NSE)
        sym_to_ikey = {}
        for stock in master:
            sym = stock.get("symbol", "")
            if not sym:
                continue
            ikey = nse_map.get(sym) or bse_map.get(sym)
            if ikey:
                sym_to_ikey[sym] = ikey

        log.info(f"  Resolved {len(sym_to_ikey)}/{len(master)} symbols")

        # reverse map
        ikey_to_sym = {v: k for k, v in sym_to_ikey.items()}
        ikeys  = list(sym_to_ikey.values())
        result = {}

        # 4. Batch fetch
        total_batches = (len(ikeys) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(ikeys), BATCH_SIZE):
            batch      = ikeys[i : i + BATCH_SIZE]
            batch_num  = i // BATCH_SIZE + 1
            log.info(f"  Batch {batch_num}/{total_batches} — {len(batch)} stocks…")

            raw = await fetch_ohlc_batch(client, batch)

            for ikey, ohlc_data in raw.items():
                sym = ikey_to_sym.get(ikey)
                if not sym:
                    continue
                ohlc = ohlc_data.get("ohlc", {})
                result[sym] = {
                    "o" : ohlc.get("open"),
                    "h" : ohlc.get("high"),
                    "l" : ohlc.get("low"),
                    "c" : ohlc_data.get("last_price"),  # live LTP
                    "pc": ohlc.get("close"),             # prev close
                    "ts": ohlc_data.get("timestamp", ""),
                }

            if i + BATCH_SIZE < len(ikeys):
                await asyncio.sleep(1)

        log.info(f"  Fetched OHLC for {len(result)} stocks")

        # 5. Upload
        payload = {
            "updated_at": now_ist.isoformat(),
            "count": len(result),
            "data": result,
        }
        await r2_upload(client, "live_ohlc.json", payload)

    log.info("✅ live_ohlc.json uploaded")
    log.info("━━━ Done ━━━")


if __name__ == "__main__":
    asyncio.run(run())
