"""
us_universe.py
Builds the list of US symbols to track + which index each one belongs to.

Index membership isn't available from Yahoo (Yahoo only knows the EXCHANGE, e.g. NASDAQ/NYSE).
Sources, tried in order per index:
  S&P 500 / 400 / 600 and Dow 30 -> official daily holdings of the SPDR ETF tracking it (SPY / MDY / SLY / DIA)
  Nasdaq-100                     -> Nasdaq's own list API
  then Wikipedia, then (S&P 500) a GitHub-hosted CSV.

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
WIKI_UA = {"User-Agent": "tradewithtech-pipeline/1.0 (https://tradewithtech.in) python-httpx"}
BROWSER_UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

SSGA = "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-{}.xlsx"
WIKI = "https://en.wikipedia.org/w/api.php"

# key -> display name, valid member-count range, enabled, [sources tried in order]
# A source is (kind, arg): "ssga" = SPDR ETF daily holdings file, "nasdaq" = Nasdaq API list,
# "wiki" = Wikipedia page, "csv" = GitHub CSV with Symbol/Security columns.
# Turn on SP400 / SP600 to add mid caps and small caps (~1000 more symbols).
INDICES = {
    "SP500": ("S&P 500",          (480, 520), True,  [("ssga", "spy"), ("wiki", "List_of_S&P_500_companies"),
                                                      ("csv", "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv")]),
    "NDX":   ("Nasdaq-100",       (95, 110),  True,  [("nasdaq", "nasdaq100"), ("wiki", "Nasdaq-100")]),
    "DJI":   ("Dow Jones 30",     (28, 32),   True,  [("ssga", "dia"), ("wiki", "Dow_Jones_Industrial_Average")]),
    "SP400": ("S&P MidCap 400",   (380, 420), False, [("ssga", "mdy"), ("wiki", "List_of_S&P_400_companies")]),
    "SP600": ("S&P SmallCap 600", (570, 630), False, [("ssga", "sly"), ("wiki", "List_of_S&P_600_companies")]),
}

# Always tracked, tagged "ETF" (index trackers — handy for charts/benchmarks)
EXTRA_SYMBOLS = {"SPY": "SPDR S&P 500 ETF", "QQQ": "Invesco QQQ Trust", "DIA": "SPDR Dow Jones ETF"}

SYMBOL_RE = re.compile(r"^[A-Z]{1,5}(-[A-Z]{1,2})?$")


def clean_symbol(s) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = re.sub(r"\[.*?\]", "", str(s))            # footnotes like [1]
    s = s.split(":")[-1]                          # "NYSE: MMM" -> "MMM"
    s = s.strip().upper().replace(".", "-").replace("/", "-")   # BRK.B / BRK/B -> BRK-B (Yahoo format)
    return s if SYMBOL_RE.match(s) else None


def _flat(col) -> str:
    return " ".join(str(c) for c in col) if isinstance(col, tuple) else str(col)


def _members_from_columns(df, sym_names, name_names) -> dict:
    low = [_flat(c).lower().strip() for c in df.columns]
    sym_i = next((i for i, c in enumerate(low) if c in sym_names), None)
    if sym_i is None:
        return {}
    name_i = next((i for i, c in enumerate(low) if c in name_names), None)
    out = {}
    for _, row in df.iterrows():
        sym = clean_symbol(row.iloc[sym_i])
        if sym:
            out[sym] = str(row.iloc[name_i]).strip() if name_i is not None else None
    return out


def parse_wiki_tables(html: str, lo: int, hi: int) -> tuple[dict, str]:
    """Find the constituents table. Returns (members, diagnostic string)."""
    tables = pd.read_html(io.StringIO(html))
    diag = []
    for df in tables:
        cols = [_flat(c) for c in df.columns]
        diag.append(f"{len(df)}x{cols[:4]}")
        if any("added" in c.lower() or "removed" in c.lower() for c in cols):   # index-changes tables
            continue
        members = _members_from_columns(df, ("symbol", "ticker", "ticker symbol"),
                                        ("security", "company", "name", "company name"))
        if lo <= len(members) <= hi:
            return members, ""
    return {}, f"{len(tables)} tables: " + "; ".join(diag[:6])


def parse_ssga_xlsx(content: bytes) -> dict:
    """SPDR daily holdings: a few metadata rows, then a header row with 'Ticker', then holdings, then disclaimers."""
    raw = pd.read_excel(io.BytesIO(content), header=None)
    hdr = next(i for i, row in raw.iterrows() if any(str(v).strip().lower() == "ticker" for v in row.values))
    df = raw.iloc[hdr + 1:].copy()
    df.columns = [str(v).strip() for v in raw.iloc[hdr].values]
    return _members_from_columns(df, ("ticker",), ("name",))


def parse_nasdaq_json(j: dict) -> dict:
    d = j.get("data") or {}
    rows = (d.get("data") or {}).get("rows") or d.get("rows") or []
    out = {}
    for r in rows:
        sym = clean_symbol(r.get("symbol", ""))
        if sym:
            out[sym] = r.get("companyName")
    return out


async def _try_source(client, key, kind, arg, lo, hi) -> dict:
    if kind == "ssga":
        r = await client.get(SSGA.format(arg), headers=BROWSER_UA, timeout=60, follow_redirects=True)
        r.raise_for_status()
        return parse_ssga_xlsx(r.content)
    if kind == "nasdaq":
        r = await client.get(f"https://api.nasdaq.com/api/quote/list-type/{arg}",
                             headers={**BROWSER_UA, "Origin": "https://www.nasdaq.com", "Referer": "https://www.nasdaq.com/"},
                             timeout=30, follow_redirects=True)
        r.raise_for_status()
        return parse_nasdaq_json(r.json())
    if kind == "wiki":
        params = {"action": "parse", "page": arg, "prop": "text", "format": "json", "formatversion": "2", "redirects": "1"}
        r = await client.get(WIKI, params=params, headers=WIKI_UA, timeout=30, follow_redirects=True)
        r.raise_for_status()
        j = r.json()
        if "parse" not in j:
            raise RuntimeError(f"API error: {str(j)[:200]}")
        members, diag = parse_wiki_tables(j["parse"]["text"], lo, hi)
        if not members:
            log.warning(f"  {key}: wiki page had no constituents table — {diag}")
        return members
    if kind == "csv":
        r = await client.get(arg, timeout=30, follow_redirects=True)
        r.raise_for_status()
        return _members_from_columns(pd.read_csv(io.StringIO(r.text)), ("symbol",), ("security", "name"))
    raise ValueError(kind)


async def scrape_index(client, key) -> tuple[dict, str]:
    _, (lo, hi), _, sources = INDICES[key]
    for kind, arg in sources:
        try:
            members = await _try_source(client, key, kind, arg, lo, hi)
        except Exception as e:
            log.warning(f"  {key}: {kind} failed ({type(e).__name__}: {str(e)[:200]})")
            continue
        if lo <= len(members) <= hi:
            return members, kind
        if members:
            log.warning(f"  {key}: {kind} gave {len(members)} symbols, expected {lo}-{hi}; trying next source")
    return {}, "failed"


async def build_universe(client, r2_download, r2_upload, now_iso) -> dict:
    """Scrape all enabled indices, merge with previous universe on failure, upload, return universe."""
    try:
        prev = await r2_download(client, UNIVERSE_FILE) or {}
    except Exception:
        prev = {}
    prev_syms = prev.get("symbols", {})

    members_by_index, index_info = {}, {}
    for key, (name, _, enabled, _) in INDICES.items():
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
