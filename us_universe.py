"""
us_universe.py
Builds the list of US symbols to track + which index each one belongs to.

Index membership isn't available from Yahoo (Yahoo only knows the EXCHANGE, e.g. NASDAQ/NYSE),
so constituents are scraped from Wikipedia, which is kept current when indices rebalance.

Output on R2: us_universe.json
  {
    "updated": "...Z",
    "indices": {"SP500": {"name": "S&P 500", "count": 503, "source": "wikipedia"}, ...},
    "symbols": {"AAPL": {"name": "Apple Inc.", "indices": ["DJI", "NDX", "SP500"]}, ...}
  }

Safety: each index's scraped list is validated by row count. If a source fails or looks wrong,
that index keeps its members from the previous us_universe.json — a bad scrape never shrinks the universe.
"""

import io
import re
import logging

import httpx
import pandas as pd

log = logging.getLogger("universe")

UNIVERSE_FILE = "us_universe.json"
UA = {"User-Agent": "tradewithtech-pipeline/1.0 (https://tradewithtech.in)"}

# key -> display name, Wikipedia page, valid member-count range, enabled
# Turn on SP400 / SP600 to add mid caps and small caps (~1000 more symbols).
INDICES = {
    "SP500": ("S&P 500",            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", (480, 520), True),
    "NDX":   ("Nasdaq-100",         "https://en.wikipedia.org/wiki/Nasdaq-100",                  (95, 110),  True),
    "DJI":   ("Dow Jones 30",       "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average", (28, 32),   True),
    "SP400": ("S&P MidCap 400",     "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies", (380, 420), False),
    "SP600": ("S&P SmallCap 600",   "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies", (570, 630), False),
}

# Fallback for S&P 500 if Wikipedia fails (GitHub-hosted dataset)
SP500_CSV = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"

# Always tracked, tagged "ETF" (index trackers — handy for charts/benchmarks)
EXTRA_SYMBOLS = {"SPY": "SPDR S&P 500 ETF", "QQQ": "Invesco QQQ Trust", "DIA": "SPDR Dow Jones ETF"}

SYMBOL_RE = re.compile(r"^[A-Z]{1,5}(-[A-Z]{1,2})?$")


def clean_symbol(s) -> str | None:
    s = re.sub(r"\[.*?\]", "", str(s))            # footnotes like [1]
    s = s.split(":")[-1]                          # "NYSE: MMM" -> "MMM"
    s = s.strip().upper().replace(".", "-")        # BRK.B -> BRK-B (Yahoo format)
    return s if SYMBOL_RE.match(s) else None


def _flat(col) -> str:
    return " ".join(str(c) for c in col) if isinstance(col, tuple) else str(col)


def parse_members(html: str, lo: int, hi: int) -> dict:
    """Find the constituents table: has a Symbol/Ticker column and a member count within [lo, hi]."""
    for df in pd.read_html(io.StringIO(html)):
        cols = [_flat(c) for c in df.columns]
        low = [c.lower() for c in cols]
        if any("added" in c or "removed" in c for c in low):     # skip "index changes" tables
            continue
        sym_i = next((i for i, c in enumerate(low) if c.strip() in ("symbol", "ticker", "ticker symbol")), None)
        if sym_i is None:
            continue
        name_i = next((i for i, c in enumerate(low) if c.strip() in ("security", "company", "name", "company name")), None)
        members = {}
        for _, row in df.iterrows():
            sym = clean_symbol(row.iloc[sym_i])
            if sym:
                members[sym] = str(row.iloc[name_i]).strip() if name_i is not None else None
        if lo <= len(members) <= hi:
            return members
    return {}


async def scrape_index(client, key) -> tuple[dict, str]:
    _, url, (lo, hi), _ = INDICES[key]
    try:
        r = await client.get(url, headers=UA, timeout=30, follow_redirects=True)
        r.raise_for_status()
        members = parse_members(r.text, lo, hi)
        if members:
            return members, "wikipedia"
        log.warning(f"  {key}: no valid table on Wikipedia (expected {lo}-{hi} rows)")
    except Exception as e:
        log.warning(f"  {key}: Wikipedia failed ({e})")

    if key == "SP500":
        try:
            r = await client.get(SP500_CSV, timeout=30)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text))
            members = {s: n for s, n in ((clean_symbol(a), b) for a, b in zip(df["Symbol"], df["Security"])) if s}
            if lo <= len(members) <= hi:
                return members, "github-datasets"
        except Exception as e:
            log.warning(f"  SP500: GitHub fallback failed ({e})")
    return {}, "failed"


async def build_universe(client, r2_download, r2_upload, now_iso) -> dict:
    """Scrape all enabled indices, merge with previous universe on failure, upload, return universe."""
    try:
        prev = await r2_download(client, UNIVERSE_FILE) or {}
    except Exception:
        prev = {}
    prev_syms = prev.get("symbols", {})

    members_by_index, index_info = {}, {}
    for key, (name, _, _, enabled) in INDICES.items():
        if not enabled:
            continue
        members, source = await scrape_index(client, key)
        if not members:
            members = {s: v.get("name") for s, v in prev_syms.items() if key in v.get("indices", [])}
            source = "previous" if members else "missing"
            log.warning(f"  {key}: using {source} list ({len(members)} symbols)")
        members_by_index[key] = members
        index_info[key] = {"name": name, "count": len(members), "source": source}
        log.info(f"  {key}: {len(members)} members ({source})")

    symbols = {}
    for key, members in members_by_index.items():
        for sym, nm in members.items():
            entry = symbols.setdefault(sym, {"name": nm, "indices": []})
            entry["name"] = entry["name"] or nm
            entry["indices"].append(key)
    for sym, nm in EXTRA_SYMBOLS.items():
        symbols.setdefault(sym, {"name": nm, "indices": []})["indices"].append("ETF")
    for v in symbols.values():
        v["indices"].sort()

    universe = {"updated": now_iso(), "indices": index_info, "symbols": dict(sorted(symbols.items()))}

    added = sorted(set(symbols) - set(prev_syms))
    removed = sorted(set(prev_syms) - set(symbols))
    if prev_syms and (added or removed):
        log.info(f"  universe changes: +{added or '-'}  -{removed or '-'}")

    await r2_upload(client, UNIVERSE_FILE, universe)
    log.info(f"  universe: {len(symbols)} unique symbols uploaded")
    return universe


async def load_universe(client, r2_download) -> dict:
    """Read the universe built by the meta job. Returns {} if missing."""
    try:
        return await r2_download(client, UNIVERSE_FILE) or {}
    except Exception as e:
        log.warning(f"  couldn't load {UNIVERSE_FILE}: {e}")
        return {}
