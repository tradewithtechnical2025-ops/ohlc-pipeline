"""
nse_catalysts.py
Pulls NSE corporate announcements, keeps only the ones that can explain a gap
(orders, deals, results, news verification, negative events, price-movement
clarifications), and stores them per symbol so the EP scanner can show WHY an
EP happened.

Sources, in order:
  1. www.nseindia.com/api/corporate-announcements  (JSON, has the symbol, supports
     a date range so missed runs are back-filled; needs the primed NSE session)
  2. Online_announcements.xml RSS                   (latest ~day only, no symbol;
     company name is mapped to a symbol via EQUITY_L.csv)

Output (R2, flat key):
  nse_catalysts.json → {"updated": iso, "data": {SYMBOL: [item, ...]}}
  item = {id, dt, react_date, session, category, subject, text, link}
    react_date = the trading day on which the market could first react
                 (same day before 15:30 IST, next trading day after it).
    session    = BH (before 09:15) | IH (market hours) | AH (after 15:30 or holiday)
                 same codes ep.html already uses for results.
"""

import csv
import io
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, time as dtime, timedelta

API_URL   = "https://www.nseindia.com/api/corporate-announcements"
RSS_URL   = "https://www.nseindia.com/content/RSS/Online_announcements.xml"
EQUITY_L  = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

HISTORY_DAYS = 90          # keep ~4 months: 20-session tracking window + pre-EP news
TEXT_MAX     = 300         # exchange summary is enough; the PDF link has the rest

MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

# ─────────────────────────────────────────────────────────────────────────────
# Classification
# Checked top to bottom; first match wins. SUBJECT is matched first because it
# is the exchange's own category; TEXT (the one-line summary) is only used for
# vague subjects like "Press Release" / "General Updates" / "Updates".
# ─────────────────────────────────────────────────────────────────────────────

_IGNORE_SUBJECT = re.compile(
    r"trading window|declaration of nav|shareholders meeting|newspaper publication|"
    r"change in director|change in management|change in company secretary|appointment|"
    r"resignation|cessation|redemption|payment of interest|record date|esop|esos|"
    r"investor presentation|analyst|institutional investor|board meeting intimation|"
    r"regulation 51|regulation 57|amendment to aoa|notice of shareholders|credit rating|"
    r"change in auditor|movement in units|noc/no dues|takeover regulations|corrigendum",
    re.I)

_NEGATIVE = re.compile(
    r"insolvency|\bcirp\b|default in interest|default in principal|show cause|"
    r"pendency of any litigation|pendency of litigation|actions? (initiated|taken)|"
    r"orders? passed|fire incident|\bfire\b|penalty|search and seizure|\braid\b|"
    r"fraud|suspension of",
    re.I)

_DEBT = re.compile(r"non.?convertible|debenture|\bncds?\b|commercial paper|\bbonds?\b|\bisin\b", re.I)

_CLARIFICATION = re.compile(r"price movement|spurt in volume|movement in (the )?price", re.I)

_RESULTS = re.compile(r"financial results?|audited results|unaudited results", re.I)

_ORDER = re.compile(
    r"orders?/contracts?|awarding of order|bagging|receiving of order|letter of (intent|award)|"
    r"\bloi\b|work order|purchase order|notification of award|order wins?|"
    r"\border (of|for|from|worth|valued)\b|\bmandate\b|deals? worth|contract (of|for|from|worth)",
    re.I)

_DEAL = re.compile(
    r"acquisition|acquire|sale or disposal|disposal|scheme of arrangement|merger|"
    r"demerger|amalgamation|joint venture|\bjv\b|collaborat|partner(ship)?|agreement|"
    r"charter|\bqip\b|qualified institutional|fund rais|preferential issue|buy ?back|"
    r"bonus|split|restructuring|stake",
    re.I)

_NEWS = re.compile(r"news verification", re.I)

_VAGUE_SUBJECT = re.compile(r"press release|general updates|^updates$|disclosure of material issue", re.I)


def classify(subject: str, text: str) -> str | None:
    """Returns a catalyst category, or None for noise that should be dropped."""
    subject = (subject or "").strip()
    text = (text or "").strip()
    both = f"{subject} {text}"

    if _IGNORE_SUBJECT.search(subject) and not _RESULTS.search(text):
        return None
    if _NEGATIVE.search(both):
        return "Negative"
    if _DEBT.search(both):
        return None             # NCD / CP / bond issuance and redemption: not an equity catalyst
    if _CLARIFICATION.search(both):
        return "Clarification"
    if _NEWS.search(subject):
        return "News"
    if _RESULTS.search(subject) or (re.search(r"outcome of board meeting", subject, re.I)
                                    and _RESULTS.search(text)):
        return "Results"
    if _ORDER.search(both):
        return "Order"
    if _DEAL.search(both):
        return "Corporate action"
    if _VAGUE_SUBJECT.search(subject):
        return "Other"          # kept for manual review; the text says what it is
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Timing: which session could first react to the filing
# ─────────────────────────────────────────────────────────────────────────────

def react_info(dt: datetime, is_trading_day, next_trading_day) -> tuple[str, str]:
    d, t = dt.date(), dt.time()
    if not is_trading_day(d):
        return next_trading_day(d).isoformat(), "AH"
    if t < MARKET_OPEN:
        return d.isoformat(), "BH"
    if t < MARKET_CLOSE:
        return d.isoformat(), "IH"
    return next_trading_day(d).isoformat(), "AH"


def _parse_dt(s: str) -> datetime | None:
    s = (s or "").strip()
    for f in ("%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    return None


def _make_item(symbol, dt, subject, text, link, is_trading_day, next_trading_day):
    category = classify(subject, text)
    if not category or not symbol or not dt:
        return None
    rd, sess = react_info(dt, is_trading_day, next_trading_day)
    text = re.sub(r"\s+", " ", text or "").strip()
    return {
        "id":         f"{dt.strftime('%Y%m%d%H%M%S')}|{subject[:40]}|{(link or '')[-60:]}",
        "dt":         dt.isoformat(timespec="seconds"),
        "react_date": rd,
        "session":    sess,
        "category":   category,
        "subject":    subject.strip(),
        "text":       text[:TEXT_MAX],
        "link":       link or "",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Source 1: NSE JSON API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_api(session, start: date, end: date, is_trading_day, next_trading_day) -> dict:
    """
    Returns {symbol: [items]}. Raises on HTTP / parse failure so the caller can
    fall back to RSS. Field names are read defensively: NSE has renamed keys
    before (desc / subject, attchmntText / text).
    """
    params = {"index": "equities",
              "from_date": start.strftime("%d-%m-%Y"),
              "to_date":   end.strftime("%d-%m-%Y")}
    headers = {"Accept": "application/json, text/plain, */*",
               "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements"}
    r = session.get(API_URL, params=params, headers=headers, timeout=45)
    r.raise_for_status()
    payload = r.json()
    rows = payload.get("data", []) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected API payload type: {type(rows).__name__}")

    out, kept = {}, 0
    for row in rows:
        sym  = (row.get("symbol") or "").strip().upper()
        subj = row.get("desc") or row.get("subject") or ""
        text = row.get("attchmntText") or row.get("text") or ""
        dt   = _parse_dt(row.get("an_dt") or row.get("sort_date") or row.get("dt") or "")
        link = row.get("attchmntFile") or ""
        item = _make_item(sym, dt, subj, text, link, is_trading_day, next_trading_day)
        if item:
            out.setdefault(sym, []).append(item)
            kept += 1
    print(f"  ✓ corporate-announcements API → {len(rows)} rows, {kept} catalyst items "
          f"({start} → {end})")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Source 2: RSS + EQUITY_L name → symbol map
# ─────────────────────────────────────────────────────────────────────────────

_NAME_STOP = re.compile(r"\b(the|limited|ltd|india|private|pvt|company|co|corporation|corp)\b")

def norm_name(name: str) -> str:
    n = (name or "").lower().replace("&", " and ")
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    n = _NAME_STOP.sub(" ", n)
    return re.sub(r"\s+", " ", n).strip()


def fetch_name_map(session) -> dict:
    r = session.get(EQUITY_L, timeout=30)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig")
    m = {}
    for row in csv.DictReader(io.StringIO(text)):
        row = {k.strip().upper(): (v or "").strip() for k, v in row.items() if k}
        sym, name = row.get("SYMBOL"), row.get("NAME OF COMPANY")
        if sym and name:
            m[norm_name(name)] = sym
    print(f"  ✓ EQUITY_L.csv → {len(m)} names")
    return m


def parse_rss(xml_text: str, name_map: dict, is_trading_day, next_trading_day) -> tuple[dict, int]:
    """Returns ({symbol: [items]}, unmapped_count)."""
    root = ET.fromstring(xml_text)
    out, unmapped = {}, 0
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        desc  = (it.findtext("description") or "").strip()
        link  = (it.findtext("link") or "").strip()
        dt    = _parse_dt(it.findtext("pubDate") or "")
        text, _, subj = desc.partition("|SUBJECT:")
        if not subj:
            subj, text = desc, ""
        if classify(subj, text) is None:
            continue
        sym = name_map.get(norm_name(title))
        if not sym:
            unmapped += 1       # mutual-fund NAVs, debt-only issuers, SME names etc.
            continue
        item = _make_item(sym, dt, subj, text, link, is_trading_day, next_trading_day)
        if item:
            out.setdefault(sym, []).append(item)
    return out, unmapped


def fetch_rss(session, is_trading_day, next_trading_day) -> dict:
    name_map = fetch_name_map(session)
    r = session.get(RSS_URL, headers={"Accept": "application/rss+xml, application/xml, */*"}, timeout=30)
    r.raise_for_status()
    out, unmapped = parse_rss(r.text, name_map, is_trading_day, next_trading_day)
    n = sum(len(v) for v in out.values())
    print(f"  ✓ RSS → {n} catalyst items ({unmapped} catalyst-looking items had no symbol match)")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Entry point + history merge
# ─────────────────────────────────────────────────────────────────────────────

def fetch_catalysts(session, today: date, lookback_days: int,
                    is_trading_day, next_trading_day) -> tuple[dict, str]:
    """Tries the API first, then RSS. Returns (items_by_symbol, source_name)."""
    start = today - timedelta(days=lookback_days)
    try:
        return fetch_api(session, start, today, is_trading_day, next_trading_day), "api"
    except Exception as e:
        print(f"  ⚠ corporate-announcements API failed ({e}) — falling back to RSS")
    return fetch_rss(session, is_trading_day, next_trading_day), "rss"


def merge_catalysts(hist: dict, new: dict, today: date, keep_days: int = HISTORY_DAYS) -> int:
    """
    hist/new: {symbol: [items]}. Dedupes on item id, keeps each symbol's list
    newest-first, drops items older than keep_days. Returns count added.
    Manual edits (items with "manual": true) are never dropped or overwritten.
    """
    cutoff = (today - timedelta(days=keep_days)).isoformat()
    added = 0
    for sym, items in new.items():
        cur = hist.setdefault(sym, [])
        ids = {x.get("id") for x in cur}
        for it in items:
            if it["id"] not in ids:
                cur.append(it)
                ids.add(it["id"])
                added += 1
    for sym in list(hist):
        hist[sym] = [x for x in hist[sym] if x.get("manual") or x.get("dt", "")[:10] >= cutoff]
        hist[sym].sort(key=lambda x: x.get("dt", ""), reverse=True)
        if not hist[sym]:
            del hist[sym]
    return added
