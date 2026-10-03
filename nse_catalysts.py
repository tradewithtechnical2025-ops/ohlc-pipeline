"""
nse_catalysts.py
Pulls NSE corporate announcements, keeps only the ones that can explain a gap
(orders, deals, news verification, negative events, price-movement
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

    # Results are handled by the dedicated results pipeline, not catalysts.
    if _RESULTS.search(both) or re.search(r"outcome of board meeting", subject, re.I) and _RESULTS.search(text):
        return None
    if _IGNORE_SUBJECT.search(subject):
        return None
    if _NEGATIVE.search(both):
        return "Negative"
    if _DEBT.search(both):
        return None             # NCD / CP / bond issuance and redemption: not an equity catalyst
    if _CLARIFICATION.search(both):
        return "Clarification"
    if _NEWS.search(subject):
        return "News"
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
# Local PDF enrichment (NO AI)
# Only new Order catalysts are opened. Extraction is deliberately conservative:
# if a reliable order-value phrase is not found, the item is left unchanged.
# ─────────────────────────────────────────────────────────────────────────────

_MONEY_RE = re.compile(
    r"(?:₹|rs\.?|inr)\s*([0-9][0-9,]*(?:\.\d+)?)\s*"
    r"(crores?|cr\.?|lakhs?|lacs?|millions?|mn\.?|billions?|bn\.?)",
    re.I,
)
_ORDER_CONTEXT_RE = re.compile(
    r"order|contract|letter of award|letter of intent|\bloa?\b|work order|purchase order|"
    r"notification of award|mandate|awarded|bagged|won|wins",
    re.I,
)


def _money_to_cr(number: str, unit: str) -> float | None:
    try:
        value = float(number.replace(",", ""))
    except (TypeError, ValueError):
        return None
    u = unit.lower().replace(".", "")
    if u.startswith("cr") or u.startswith("crore"):
        return round(value, 4)
    if u.startswith("lakh") or u.startswith("lac"):
        return round(value / 100.0, 4)
    if u.startswith("million") or u == "mn":
        return round(value / 10.0, 4)
    if u.startswith("billion") or u == "bn":
        return round(value * 100.0, 4)
    return None


def _extract_pdf_text(session, url: str) -> str:
    if not url or url == "-" or not url.lower().split("?", 1)[0].endswith(".pdf"):
        return ""
    try:
        from pypdf import PdfReader
        r = session.get(url, timeout=40)
        r.raise_for_status()
        reader = PdfReader(io.BytesIO(r.content))
        # Order details are normally near the beginning; cap work at 12 pages.
        return "\n".join((p.extract_text() or "") for p in reader.pages[:12])
    except Exception as e:
        print(f"  ⚠ PDF detail extraction failed for {url.rsplit('/', 1)[-1]} ({e})")
        return ""


def _extract_order_details(text: str) -> dict:
    clean = re.sub(r"\s+", " ", text or " ").strip()
    if not clean:
        return {}

    candidates = []
    for m in _MONEY_RE.finditer(clean):
        lo, hi = max(0, m.start() - 220), min(len(clean), m.end() + 220)
        context = clean[lo:hi]
        if not _ORDER_CONTEXT_RE.search(context):
            continue
        value_cr = _money_to_cr(m.group(1), m.group(2))
        if value_cr is not None and value_cr > 0:
            candidates.append((value_cr, m.group(0).strip(), context))

    if not candidates:
        return {}

    # Prefer the largest order-context amount. This avoids many small incidental
    # figures, while remaining conservative because an order keyword is required.
    value_cr, raw, context = max(candidates, key=lambda x: x[0])
    return {
        "order_value_cr": value_cr,
        "order_value_text": raw,
        "detail_source": "pdf_local",
        "detail_excerpt": context[:500],
    }


def enrich_new_orders_local(session, new_items: dict, existing_ids: set[str]) -> tuple[int, int]:
    """Enrich only genuinely new Order items; never uses AI."""
    checked = enriched = 0
    for items in new_items.values():
        for it in items:
            if it.get("category") != "Order" or it.get("id") in existing_ids:
                continue
            checked += 1
            text = _extract_pdf_text(session, it.get("link", ""))
            details = _extract_order_details(text)
            if details:
                it.update(details)
                enriched += 1
    if checked:
        print(f"  ✓ Local order-PDF enrichment → checked={checked}, value_found={enriched}, AI=0")
    return checked, enriched


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

# ─────────────────────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────────────────────

def _build_session():
    """Create and prime a browser-like NSE session."""
    import requests
    s = requests.Session()
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
    })
    # NSE's API normally expects cookies from the home page. Failure to prime
    # is non-fatal because fetch_catalysts() can still fall back to RSS.
    try:
        s.get("https://www.nseindia.com/", timeout=20)
    except Exception as e:
        print(f"  ⚠ NSE session prime failed ({e}) — API will be tried anyway")
    return s


def _load_nse_holidays(session, year: int) -> set[date]:
    """Best-effort NSE CM holiday calendar; weekdays are the safe fallback."""
    holidays = set()
    try:
        r = session.get(
            "https://www.nseindia.com/api/holiday-master?type=trading",
            headers={"Accept": "application/json, text/plain, */*",
                     "Referer": "https://www.nseindia.com/resources/exchange-communication-holidays"},
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("CM", []) if isinstance(payload, dict) else []
        for row in rows:
            raw = str(row.get("tradingDate") or row.get("date") or "").strip()
            for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
                try:
                    d = datetime.strptime(raw, fmt).date()
                    if d.year in (year, year + 1):
                        holidays.add(d)
                    break
                except ValueError:
                    pass
        print(f"  ✓ NSE holiday calendar → {len(holidays)} CM holiday(s) loaded")
    except Exception as e:
        print(f"  ⚠ NSE holiday calendar unavailable ({e}) — using Mon-Fri fallback")
    return holidays


def _r2_get_json(session, filename: str):
    import os
    import time
    worker_url = os.environ["WORKER_URL"].rstrip("/")
    token = os.environ["WORKER_TOKEN"]
    try:
        sep = "&" if "?" in filename else "?"
        r = session.get(
            f"{worker_url}/{filename}{sep}v={int(time.time())}",
            headers={"X-Secret-Token": token, "Cache-Control": "no-cache"},
            timeout=30,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ⚠ R2 read {filename} failed ({e}) — starting with empty history")
        return None


def _r2_put_json(session, filename: str, payload: dict):
    import json
    import os
    worker_url = os.environ["WORKER_URL"].rstrip("/")
    token = os.environ["WORKER_TOKEN"]
    r = session.post(
        f"{worker_url}?file={filename}",
        headers={"X-Secret-Token": token, "Content-Type": "application/json"},
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        timeout=120,
    )
    r.raise_for_status()
    print(f"  ✓ Uploaded {filename}")


def main():
    import os

    # Fail clearly in GitHub Actions instead of silently doing nothing.
    for key in ("WORKER_URL", "WORKER_TOKEN"):
        if not os.environ.get(key):
            raise RuntimeError(f"Missing required environment variable: {key}")

    today = date.today()
    print(f"Catalyst scan starting for {today.isoformat()}...")

    nse_session = _build_session()
    holidays = _load_nse_holidays(nse_session, today.year)

    def is_trading_day(d: date) -> bool:
        return d.weekday() < 5 and d not in holidays

    def next_trading_day(d: date) -> date:
        x = d + timedelta(days=1)
        while not is_trading_day(x):
            x += timedelta(days=1)
        return x

    # Existing R2 payload shape: {"updated": ..., "data": {SYMBOL: [...]}}
    # Accept a bare symbol->items dict too, so an older/manual file is not lost.
    r2_session = _build_session()
    old_payload = _r2_get_json(r2_session, "nse_catalysts.json") or {}
    if isinstance(old_payload, dict) and isinstance(old_payload.get("data"), dict):
        history = old_payload["data"]
    elif isinstance(old_payload, dict):
        history = old_payload
    else:
        history = {}

    # Remove historical Results entries too; results are maintained by the
    # dedicated results pipeline and should not duplicate catalyst storage.
    removed_results = 0
    for sym in list(history):
        before = len(history[sym])
        history[sym] = [x for x in history[sym] if x.get("category") != "Results"]
        removed_results += before - len(history[sym])
        if not history[sym]:
            del history[sym]
    if removed_results:
        print(f"  🗑 Removed {removed_results} old Results item(s) from catalyst history")

    existing_ids = {x.get("id") for items in history.values() for x in items if x.get("id")}

    new_items, source = fetch_catalysts(
        nse_session, today, HISTORY_DAYS, is_trading_day, next_trading_day
    )
    fetched = sum(len(v) for v in new_items.values())

    # No AI: only genuinely new Order PDFs are inspected locally for order value.
    enrich_new_orders_local(nse_session, new_items, existing_ids)

    added = merge_catalysts(history, new_items, today, HISTORY_DAYS)
    total = sum(len(v) for v in history.values())

    payload = {
        "updated": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": source,
        "data": history,
    }
    _r2_put_json(r2_session, "nse_catalysts.json", payload)

    print(f"  ✓ Catalyst scan complete: source={source}, fetched={fetched}, "
          f"new={added}, symbols={len(history)}, stored={total}")


if __name__ == "__main__":
    main()
