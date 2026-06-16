"""
pipeline_rrg.py
Computes JdK RS Ratio + RS Momentum for NSE sector indices vs NIFTY50
Uploads rrg_data.json to Cloudflare R2

Modified JdK parameters for ~1yr daily data:
  - Short EMA  : 10 weeks
  - Long EMA   : 52 weeks  (original=125, scaled for data availability)
  - Momentum   : 10 weeks EMA of RS Ratio
"""

import json
import os
import httpx
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────

WORKER_URL     = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN   = os.environ["WORKER_TOKEN"]
FINEDGE_TOKEN  = os.environ["FINEDGE_TOKEN"]

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}

FINEDGE_BASE  = "https://data.finedgeapi.com/api/v1"
R2_PREFIX     = "index_history"

BENCHMARK_SYM = "NIFTY50"

# Copied from pipeline_indices.py — sectoral + thematic only
INDEX_CATEGORIES = {
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

# Sectoral + Thematic indices for RRG
RRG_SECTORS = {
    sym: meta[1]
    for sym, meta in INDEX_CATEGORIES.items()
    if meta[0] in ("sectoral", "thematic")
}

# JdK parameters (modified for ~1yr data)
SHORT_EMA_WEEKS = 10
LONG_EMA_WEEKS  = 52
MOM_EMA_WEEKS   = 10
TAIL_WEEKS      = 12   # weeks of history to store for chart tails

# Multiple benchmark support
BENCHMARKS = {
    "NIFTY50": "Nifty 50",
    "NIF500":  "Nifty 500",
    "NIF200":  "Nifty 200",
}

# ── Finedge Fetch ─────────────────────────────────────────────────────────────

async def fetch_history_from_finedge(client, symbol):
    """Fetch 18 months of daily history from Finedge API using normalized symbol."""
    today     = datetime.now().date()
    from_date = (today - timedelta(days=548)).strftime("%Y-%m-%d")
    to_date   = today.strftime("%Y-%m-%d")
    url    = f"{FINEDGE_BASE}/index/market-price/historical"
    params = {
        "index_symbol": symbol,
        "from_date":    from_date,
        "to_date":      to_date,
        "token":        FINEDGE_TOKEN,
    }
    try:
        r = await client.get(url, params=params, timeout=300)
        if r.status_code != 200:
            return []
        rows = r.json().get("rows") or []
        return [{
            "date":  row.get("quote_date"),
            "close": row.get("close_price"),
        } for row in rows if row.get("close_price")]
    except Exception as e:
        print(f"    [WARN] Finedge fetch failed for {symbol}: {e}")
        return []

# ── Worker Upload ─────────────────────────────────────────────────────────────

async def upload_json_to_r2(client, key, data):
    """Upload dict as JSON to R2 via Worker."""
    url  = f"{WORKER_URL}?file={key}"
    body = json.dumps(data).encode()
    r    = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=body,
        timeout=300,
    )
    if r.status_code != 200:
        raise RuntimeError(f"{key} upload failed: {r.status_code} {r.text}")
    print(f"  [OK] Uploaded {key} ({len(body)/1024:.1f} KB)")

# ── Data Helpers ──────────────────────────────────────────────────────────────

def parse_daily_to_weekly(raw_data):
    """
    Convert daily OHLCV list → weekly Friday-close Series.
    Handles holidays automatically via .last() — picks last
    available trading day of each week.
    """
    if not raw_data:
        return None

    df = pd.DataFrame(raw_data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")
    df = df[["close"]].dropna()

    # Resample to weekly — last close of each week ending Friday
    weekly = df["close"].resample("W-FRI").last().dropna()
    return weekly

# ── JdK RS Computation ────────────────────────────────────────────────────────

def compute_ema(series, span):
    """Standard EMA with min_periods=1 so warmup doesn't produce NaN."""
    return series.ewm(span=span, adjust=False, min_periods=1).mean()

def compute_jdk_rs(sector_weekly, benchmark_weekly,
                   short_ema=SHORT_EMA_WEEKS,
                   long_ema=LONG_EMA_WEEKS,
                   mom_ema=MOM_EMA_WEEKS):
    """
    Modified JdK RS Ratio + RS Momentum.

    RS Ratio:
      1. raw_rs       = sector_close / benchmark_close
      2. smoothed     = EMA(raw_rs, short_ema)          ← noise filter
      3. rs_ratio     = (smoothed / EMA(smoothed, long_ema) - 1) * 100 + 100
                        center=100, >100 outperforming

    RS Momentum:
      4. rs_momentum  = (rs_ratio / EMA(rs_ratio, mom_ema) - 1) * 100 + 100
                        center=100, >100 accelerating

    Returns DataFrame with columns: rs_ratio, rs_momentum
    Indexed by date (weekly).
    """
    # Align both series on same dates
    combined = pd.DataFrame({
        "sector":    sector_weekly,
        "benchmark": benchmark_weekly,
    }).dropna()

    if len(combined) < short_ema + 5:
        print(f"  [WARN] Not enough data: {len(combined)} weeks")
        return None

    raw_rs   = combined["sector"] / combined["benchmark"]
    smoothed = compute_ema(raw_rs, short_ema)
    long_avg = compute_ema(smoothed, long_ema)

    rs_ratio    = (smoothed / long_avg - 1) * 100 + 100
    rs_mom_base = compute_ema(rs_ratio, mom_ema)
    rs_momentum = (rs_ratio / rs_mom_base - 1) * 100 + 100

    result = pd.DataFrame({
        "rs_ratio":    rs_ratio.round(4),
        "rs_momentum": rs_momentum.round(4),
    })
    return result

# ── Build Output ──────────────────────────────────────────────────────────────

def build_tail(rs_df, tail_weeks=TAIL_WEEKS):
    """
    Extract last N weeks as list of {date, rs_ratio, rs_momentum}
    for RRG tail rendering on frontend.
    """
    tail = rs_df.tail(tail_weeks).copy()
    return [
        {
            "date": str(idx.date()),
            "rs":   row["rs_ratio"],
            "rm":   row["rs_momentum"],
        }
        for idx, row in tail.iterrows()
    ]

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print("=" * 60)
    print(f"RRG Pipeline  —  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    output = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "benchmarks":   {},
    }

    async with httpx.AsyncClient() as client:

        # ── Process each benchmark ────────────────────────────────────────────
        for bench_sym, bench_name in BENCHMARKS.items():
            print(f"\nBenchmark: {bench_name} ({bench_sym})")

            bench_raw = await fetch_history_from_finedge(client, bench_sym)
            if not bench_raw:
                print(f"  [SKIP] Benchmark {bench_sym} — no data from Finedge")
                continue

            bench_weekly = parse_daily_to_weekly(bench_raw)
            if bench_weekly is None or len(bench_weekly) < 50:
                print(f"  [SKIP] Not enough benchmark data: {len(bench_weekly) if bench_weekly is not None else 0} weeks")
                continue

            print(f"  Benchmark weeks available: {len(bench_weekly)}")

            sectors_out = []

            # ── Process each sector ───────────────────────────────────────────
            for sym, display_name in RRG_SECTORS.items():
                print(f"  Processing {display_name} ({sym})...")

                sector_raw = await fetch_history_from_finedge(client, sym)
                if not sector_raw:
                    print(f"    [SKIP] No data for {sym}")
                    continue

                sector_weekly = parse_daily_to_weekly(sector_raw)
                if sector_weekly is None or len(sector_weekly) < 20:
                    print(f"    [SKIP] Not enough data: {len(sector_weekly) if sector_weekly is not None else 0} weeks")
                    continue

                rs_df = compute_jdk_rs(sector_weekly, bench_weekly)
                if rs_df is None:
                    continue

                tail = build_tail(rs_df, TAIL_WEEKS)
                if not tail:
                    continue

                latest = tail[-1]

                sectors_out.append({
                    "sym":   sym,
                    "name":  display_name,
                    "rs":    latest["rs"],
                    "rm":    latest["rm"],
                    "tail":  tail,
                    "weeks": len(rs_df),
                })

                print(f"    RS={latest['rs']:.2f}  RM={latest['rm']:.2f}  Weeks={len(rs_df)}")

            output["benchmarks"][bench_sym] = {
                "name":    bench_name,
                "sectors": sectors_out,
            }
            print(f"  Done — {len(sectors_out)} sectors computed")

        # ── Upload ────────────────────────────────────────────────────────────
        print("\nUploading rrg_data.json to R2...")
        await upload_json_to_r2(client, "rrg_data.json", output)
        print("\nRRG Pipeline complete.")


if __name__ == "__main__":
    asyncio.run(main())
