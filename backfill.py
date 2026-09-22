"""
backfill.py — combines two backfill jobs against nse_results_detailed.json
into one script, one run, one upload:

  1. MARKET SESSION BACKFILL (recover BH/IH/AH for records parsed before
     pipeline_news.py started capturing filed_at_ts). Recovers the
     original filing timestamp by matching each record's `link` against
     nse_results_pdf_feed.json / bse_results_pdf_feed.json — the PDF-
     candidate accumulators, which keep an item's published_ts even after
     it's been parsed, capped only by recency (500 each). A link already
     evicted by that cap has no recoverable timestamp; its market_session
     stays None ("unknown") permanently — reported, not an error.
     Idempotent: only ever touches records still missing market_session,
     so running this every day (not just once) costs nothing extra once
     the initial backlog is caught up — there's no need to schedule it
     separately from the gap backfill below.

  2. GAP / PRICE-REACTION BACKFILL. For every result within
     MAX_RESULT_AGE_DAYS, computes (1) result-day move + volume spike —
     fillable the same evening the result's own EOD candle lands, no
     need to wait a day; (2) next-trading-day move + volume spike and
     (3) next-trading-day gap %/direction — these genuinely do wait for
     T+1; and (4) return from result date to today. Uses pipeline.py's
     OHLC chunks (ohlc_1..8.json), so this script should run on a
     schedule AFTER pipeline.py's daily OHLC job finishes (see the
     matching GitHub Actions workflow's cron comment).
     (1) and (2)/(3) are each frozen once computed, independently of one
     another — a missing T+1 no longer holds back (1), which used to be
     bundled with (2)/(3) behind the same "wait for T+1" gate even
     though it never needed T+1 at all. (4) is a moving target and is
     re-evaluated every run for any record still in the retention window.

Both parts read nse_results_detailed.json once at the top and write it
back once at the bottom (if anything changed) — not two separate
read-modify-write cycles, which would double the race-window risk
against pipeline_news.py's own 10-min writes to the same file.
"""
import asyncio
import json
import os
from datetime import date, datetime, timezone, timedelta

import httpx

WORKER_URL = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
WORKER_HEADERS = {"X-Secret-Token": WORKER_TOKEN}

_IST = timezone(timedelta(hours=5, minutes=30))
R2_CHUNKS = 8            # must match pipeline.py's R2_CHUNKS — ohlc_1.json..ohlc_8.json
MAX_RESULT_AGE_DAYS = 365
VOLUME_LOOKBACK = 20     # trading days of pre-result average volume, matches pipeline.py's _detect_post_result_thrust default


async def r2_get(client: httpx.AsyncClient, filename: str):
    r = await client.get(f"{WORKER_URL}/{filename}", headers=WORKER_HEADERS, timeout=90)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


async def r2_put(client: httpx.AsyncClient, filename: str, data: dict):
    body = json.dumps(data).encode()
    r = await client.post(f"{WORKER_URL}?file={filename}",
                           headers={**WORKER_HEADERS, "Content-Type": "application/json"},
                           content=body, timeout=90)
    r.raise_for_status()
    print(f"  ↑ {filename} ({len(body)/1024:.1f} KB)")


# ═══════════════════════════════════════════════════════════════════
# PART 1 — market session backfill
# ═══════════════════════════════════════════════════════════════════

def classify_market_session(filed_at_ts):
    """Identical logic to pipeline_news.py's _classify_market_session:
    'BH' before 09:15 IST, 'IH' 09:15-15:30, 'AH' after. (None, None) if
    filed_at_ts is falsy."""
    if not filed_at_ts:
        return None, None
    dt = datetime.fromtimestamp(filed_at_ts, tz=_IST)
    minutes = dt.hour * 60 + dt.minute
    if minutes < 9 * 60 + 15:
        session = "BH"
    elif minutes <= 15 * 60 + 30:
        session = "IH"
    else:
        session = "AH"
    return session, dt.isoformat()


async def run_market_session_backfill(client: httpx.AsyncClient, items: list) -> int:
    print("Part 1 — market session backfill...")
    nse_feed, bse_feed = await asyncio.gather(
        r2_get(client, "nse_results_pdf_feed.json"),
        r2_get(client, "bse_results_pdf_feed.json"),
    )
    link_to_ts = {}
    for feed in (nse_feed, bse_feed):
        for it in (feed or {}).get("items", []):
            link, ts = it.get("link"), it.get("published_ts")
            if link and ts and link not in link_to_ts:
                link_to_ts[link] = ts
    print(f"  ✓ {len(link_to_ts)} link -> timestamp mapping(s) available")

    filled, already_had, unrecoverable = 0, 0, 0
    for it in items:
        meta = it.get("meta") or {}
        if meta.get("market_session"):
            already_had += 1
            continue
        ts = link_to_ts.get(it.get("link"))
        if not ts:
            unrecoverable += 1
            continue
        session, iso = classify_market_session(ts)
        meta["filed_at_ts"] = ts
        meta["filed_at_ist"] = iso
        meta["market_session"] = session
        it["meta"] = meta
        filled += 1

    print(f"  ✓ Filled {filled} | {already_had} already had it | {unrecoverable} unrecoverable (link evicted from accumulators)")
    return filled


# ═══════════════════════════════════════════════════════════════════
# PART 2 — gap / price-reaction backfill
# ═══════════════════════════════════════════════════════════════════

async def load_ohlc_all(client: httpx.AsyncClient) -> dict:
    tasks = [r2_get(client, f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_data = {}
    for i, res in enumerate(results):
        if isinstance(res, Exception) or not res:
            print(f"  ⚠ ohlc_{i+1}.json unavailable: {res if isinstance(res, Exception) else 'empty'}")
            continue
        stocks = res.get("stocks", res) if isinstance(res, dict) else {}
        all_data.update(stocks)
    print(f"  ✓ OHLC loaded: {len(all_data)} symbols across {R2_CHUNKS} chunks")
    return all_data


def compute_result_day(sym: str, board_meeting_date: str, all_data: dict):
    """(1) result_day_ch / result_day_vol_x — needs ONLY the result day's
    own OHLC (T), not T+1. Fillable the very same evening the result's
    EOD candle lands, no need to wait a day. Kept as its own function
    (not bundled with compute_t1_fields below) specifically so a missing
    T+1 never blocks this from being computed — that bundling was a bug:
    the result day's own move was being held back for a day it never
    needed to wait for."""
    s = all_data.get(sym)
    if not s:
        return None
    dates, closes, volumes = s.get("d"), s.get("c"), s.get("v")
    if not dates or not closes:
        return None
    ri_list = [i for i, d in enumerate(dates) if d == board_meeting_date]
    if not ri_list:
        return None  # today's own EOD candle isn't in OHLC yet
    ri = ri_list[-1]
    if ri == 0 or not closes[ri - 1]:
        return None  # no prior close to compare against (first day in series)
    t_close = closes[ri]
    if not t_close:
        return None
    result_day_ch = round((t_close - closes[ri - 1]) / closes[ri - 1] * 100, 2)

    result_day_vol_x = None
    if volumes and ri < len(volumes) and volumes[ri] is not None:
        lookback = min(VOLUME_LOOKBACK, ri)
        if lookback > 0:
            avg_vol = sum(volumes[ri - lookback:ri]) / lookback
            if avg_vol:
                result_day_vol_x = round(volumes[ri] / avg_vol, 2)

    return {"result_day_ch": result_day_ch, "result_day_vol_x": result_day_vol_x}


def compute_t1_fields(sym: str, board_meeting_date: str, all_data: dict):
    """(2) t1_ch_pct/vol_x, (3) gap_pct/direction — needs T+1, so this
    genuinely waits a day (unlike compute_result_day above). Mirrors
    pipeline.py's _detect_post_result_thrust ri/ti indexing, but
    unconditional — no move-size/volume/close-position filtering."""
    s = all_data.get(sym)
    if not s:
        return None
    dates, opens, highs, lows, closes, volumes = s.get("d"), s.get("o"), s.get("h"), s.get("l"), s.get("c"), s.get("v")
    if not dates or not opens or not closes:
        return None
    ri_list = [i for i, d in enumerate(dates) if d == board_meeting_date]
    if not ri_list:
        return None
    ri = ri_list[-1]
    ti = ri + 1
    if ti >= len(dates):
        return None
    t_close = closes[ri]
    if not t_close:
        return None
    t1_date, t1_open, t1_high, t1_low, t1_close = dates[ti], opens[ti], highs[ti], lows[ti], closes[ti]
    if t1_open is None:
        return None

    gap_pct = round((t1_open - t_close) / t_close * 100, 2)
    t1_ch_pct = round((t1_close - t_close) / t_close * 100, 2) if t1_close else None

    t1_vol_x = None
    if volumes and ti < len(volumes) and volumes[ti] is not None:
        lookback = min(VOLUME_LOOKBACK, ri)
        if lookback > 0:
            avg_vol = sum(volumes[ri - lookback:ri]) / lookback
            if avg_vol:
                t1_vol_x = round(volumes[ti] / avg_vol, 2)

    return {
        "next_trading_date": t1_date,
        "t_close": round(t_close, 2),
        "t1_open": round(t1_open, 2) if t1_open is not None else None,
        "t1_high": round(t1_high, 2) if t1_high is not None else None,
        "t1_low": round(t1_low, 2) if t1_low is not None else None,
        "t1_close": round(t1_close, 2) if t1_close is not None else None,
        "t1_ch_pct": t1_ch_pct,
        "t1_vol_x": t1_vol_x,
        "gap_pct": gap_pct,
        "gap_direction": "up" if gap_pct > 0 else ("down" if gap_pct < 0 else "flat"),
    }


def compute_return_since_result(sym: str, board_meeting_date: str, all_data: dict):
    """(4) result date -> today's move. Recomputed every run (unlike the
    frozen fields above) since "today" advances daily. Needs only the
    result date itself to be in OHLC — T+1 not required."""
    s = all_data.get(sym)
    if not s:
        return None
    dates, closes = s.get("d"), s.get("c")
    if not dates or not closes:
        return None
    ri_list = [i for i, d in enumerate(dates) if d == board_meeting_date]
    if not ri_list:
        return None
    t_close = closes[ri_list[-1]]
    if not t_close:
        return None
    as_of_date, as_of_close = dates[-1], closes[-1]
    if as_of_close is None:
        return None
    return {
        "as_of_date": as_of_date,
        "as_of_close": round(as_of_close, 2),
        "ret_since_result": round((as_of_close - t_close) / t_close * 100, 2),
    }


async def run_gap_backfill(client: httpx.AsyncClient, items: list) -> int:
    print("Part 2 — gap / price-reaction backfill...")
    all_data = await load_ohlc_all(client)
    if not all_data:
        print("  ✗ No OHLC data available at all — skipping this part")
        return 0

    cutoff = (date.today() - timedelta(days=MAX_RESULT_AGE_DAYS)).isoformat()
    filled_rd, already_had_rd, waiting_on_rd = 0, 0, 0
    filled_t1, already_had_t1, waiting_on_t1 = 0, 0, 0
    updated_ret, no_ret_data = 0, 0
    no_match, too_old, changed = 0, 0, 0

    for it in items:
        meta = it.get("meta") or {}
        sym, bmd = meta.get("symbol"), meta.get("board_meeting_date")
        if not sym or not bmd:
            no_match += 1
            continue
        if bmd < cutoff:
            too_old += 1
            continue

        pr = it.get("price_reaction") or {}
        item_changed = False

        if "result_day_ch" in pr:
            already_had_rd += 1
        else:
            rd = compute_result_day(sym, bmd, all_data)
            if rd:
                pr.update(rd)
                filled_rd += 1
                item_changed = True
            else:
                waiting_on_rd += 1

        if "gap_pct" in pr:
            already_had_t1 += 1
        else:
            t1 = compute_t1_fields(sym, bmd, all_data)
            if t1:
                pr.update(t1)
                filled_t1 += 1
                item_changed = True
            else:
                waiting_on_t1 += 1

        ret = compute_return_since_result(sym, bmd, all_data)
        if ret:
            if pr.get("ret_since_result") != ret["ret_since_result"]:
                updated_ret += 1
                item_changed = True
            pr.update(ret)
        else:
            no_ret_data += 1

        if pr:
            it["price_reaction"] = pr
        if item_changed:
            changed += 1

    print(f"  ✓ (1) result day: {filled_rd} newly filled | {already_had_rd} already had it | {waiting_on_rd} waiting on today's own EOD candle")
    print(f"  ✓ (2)(3) next day: {filled_t1} newly filled | {already_had_t1} already had it | {waiting_on_t1} waiting on T+1 data")
    print(f"  ✓ (4) return-since-result: {updated_ret} updated this run | {no_ret_data} no OHLC match")
    print(f"    {no_match} unmatched (no symbol/date) | {too_old} beyond {MAX_RESULT_AGE_DAYS}d cutoff")
    return changed


# ═══════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════

async def run():
    async with httpx.AsyncClient() as client:
        print("Loading nse_results_detailed.json...")
        detailed = await r2_get(client, "nse_results_detailed.json")
        items = (detailed or {}).get("items", [])
        if not items:
            print("  ✗ nse_results_detailed.json empty/unavailable — nothing to backfill")
            return

        session_filled = await run_market_session_backfill(client, items)
        reaction_changed = await run_gap_backfill(client, items)

        if session_filled or reaction_changed:
            detailed["updated_at"] = datetime.now(timezone.utc).isoformat()
            await r2_put(client, "nse_results_detailed.json", detailed)
        else:
            print("Nothing changed in either part — skipping upload")

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
