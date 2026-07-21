import asyncio
import calendar
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
import feedparser
import httpx

# ── Telegram notify ──
try:
    from telegram_notify import send_message
except ImportError:
    def send_message(text, silent=False, chat_id=""): pass

# Separate channel for financial-results alerts, so they don't mix with
# pipeline status notifications in the main TELEGRAM_CHAT_ID channel.
# Boss needs to create this channel and set the secret once.
TELEGRAM_RESULTS_CHAT_ID = os.environ.get("TELEGRAM_RESULTS_CHAT_ID", "")

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]
UP_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json"
}
DL_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}

# Feed definitions: (source_key, label, rss_url)
FEEDS = [
    # NSE Official
    ("nse_results",       "NSE Financial Results",  "https://nsearchives.nseindia.com/content/RSS/Integrated_Filing_Financials.xml"),
    ("nse_announcements", "NSE Announcements",       "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml"),
    ("nse_board",         "NSE Board Meetings",      "https://nsearchives.nseindia.com/content/RSS/Board_Meetings.xml"),
    ("nse_corp_actions",  "NSE Corporate Actions",   "https://nsearchives.nseindia.com/content/RSS/Corporate_action.xml"),
    # Market News
    ("et_markets",   "Economic Times Markets", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("mint_markets", "LiveMint Markets",        "https://www.livemint.com/rss/markets"),
]

# source_key(s) -> R2 output file
# Single key = individual file, list = merged file
OUTPUT_MAP = {
    "nse_results_feed.json":   ["nse_results"],
    "nse_announcements.json":  ["nse_announcements"],
    "nse_board_meetings.json": ["nse_board"],
    "nse_corp_actions.json":   ["nse_corp_actions"],
    "market_news.json":        ["et_markets", "mint_markets"],
}


# Summary patterns to drop (routine regulatory noise, not news)
NOISE_PATTERNS = [
    "Net Asset Value",
]

# |SUBJECT: tag values to drop — routine compliance/regulatory boilerplate,
# not actionable for trading. Matched case-insensitively against the exact
# subject text (regex so "Disclosure"/"Intimation" prefix variants both hit).
NOISE_SUBJECT_PATTERNS = [
    r"^updates$",
    r"^general updates$",
    r"^copy of newspaper publication$",
    r"^certificate under sebi \(depositories and participants\) regulations, 2018$",
    r"^quarterly compliance report on corporate governance",
    r"^structural digital database$",
    r"^(disclosure|intimation) under regulation (27\(2\)|13\(3\)|7\(1\)|6\(1\)|50\(1\)|51|52\(4\))$",
    r"^board meeting intimation$",  # future-dated notice only; "Outcome of Board Meeting" kept (actual results)
    r"^shareholders meeting$",      # AGM/EGM/postal ballot voting outcomes — not trading-actionable
    r"^allotment of securities$",   # routine NCD/ESOP allotment filings
]
_NOISE_SUBJECT_RE = re.compile("|".join(NOISE_SUBJECT_PATTERNS), re.IGNORECASE)

_SUBJECT_TAG_RE = re.compile(r"\|SUBJECT:\s*(.+)$")

def is_noise(item: dict) -> bool:
    summary = item.get("summary", "")
    if any(p in summary for p in NOISE_PATTERNS):
        return True
    m = _SUBJECT_TAG_RE.search(summary)
    if m and _NOISE_SUBJECT_RE.match(m.group(1).strip()):
        return True
    return False


def dedup_items(items: list[dict]) -> list[dict]:
    """
    Dedup by link + title + summary, NOT published.
    NSE re-publishes the same announcement with updated timestamps (NTPC type)
    — those are duplicates. But NAV updates share one generic link with
    different summaries — those are distinct and must be kept.
    Items must be sorted newest-first before calling, so latest published wins.
    """
    seen = set()
    out = []
    for it in items:
        key = (it.get("link", ""), it.get("title", ""), it.get("summary", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


async def fetch_feed(client: httpx.AsyncClient, source_key: str, label: str, url: str, retries_per_domain: int = 2) -> tuple[str, list[dict], bool]:
    # Fallback to the legacy archives.nseindia.com domain if the primary
    # nsearchives.nseindia.com domain fails all its attempts — GitHub Actions
    # runner IPs have been seen getting ReadTimeout consistently on the
    # primary domain while working fine from a regular browser, suggesting
    # IP-level throttling/WAF specific to that subdomain. Same URL path is
    # assumed to exist on the legacy domain.
    urls_to_try = [url]
    if "nsearchives.nseindia.com" in url:
        urls_to_try.append(url.replace("nsearchives.nseindia.com", "archives.nseindia.com"))

    last_exc = None
    got_empty_after_all_retries = False
    v = int(time.time() // 300)  # 5-min cache-buster bucket

    for domain_idx, base_url in enumerate(urls_to_try):
        sep = "&" if "?" in base_url else "?"
        cache_busted_url = f"{base_url}{sep}v={v}"
        domain_label = base_url.split("/")[2]
        is_last_domain = domain_idx == len(urls_to_try) - 1

        for attempt in range(retries_per_domain):
            is_last_attempt = is_last_domain and attempt == retries_per_domain - 1
            try:
                r = await client.get(cache_busted_url, headers=BROWSER_HEADERS, timeout=20, follow_redirects=True)
                r.raise_for_status()
                feed = feedparser.parse(r.content)
                items = []
                for entry in feed.entries:

                    # Epoch timestamp for reliable cross-source sorting
                    ts = 0
                    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                    if parsed:
                        try:
                            ts = calendar.timegm(parsed)
                        except Exception:
                            ts = 0

                    items.append({
                        "source":       label,
                        "source_key":   source_key,
                        "title":        entry.get("title", "").strip(),
                        "link":         entry.get("link", ""),
                        "published":    entry.get("published", ""),
                        "published_ts": ts,
                        "summary":      entry.get("summary", entry.get("description", "")).strip()[:300],
                    })

                # NSE occasionally serves a transient empty-but-200 response
                # (confirmed: same feed returned 0 items one run, 20 the next,
                # no other change) — retry before accepting zero as final.
                if not items:
                    if not is_last_attempt:
                        print(f"  ⚠ {label} ({domain_label}): got 0 items, retry {attempt+1}/{retries_per_domain} in {2**attempt}s")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    # Exhausted every attempt on every domain and still empty.
                    # For these high-volume feeds a genuine zero is implausible
                    # — treat as failure (not success) so callers preserve
                    # existing R2 data rather than overwrite it with [].
                    got_empty_after_all_retries = True
                    break

                if domain_idx > 0:
                    print(f"  ⚠ {label}: fell back to {domain_label}")
                print(f"  ✓ {label}: {len(items)} items")
                return source_key, items, True
            except Exception as e:
                last_exc = e
                if not is_last_attempt:
                    print(f"  ⚠ {label} ({domain_label}): {type(e).__name__}: {e or '(no message)'}, retry {attempt+1}/{retries_per_domain} in {2**attempt}s")
                    await asyncio.sleep(2 ** attempt)
                    continue
                print(f"  ⚠ {label} ({domain_label}): exhausted retries — {type(e).__name__}: {e or '(no message)'}")

    if got_empty_after_all_retries:
        print(f"  ✗ {label}: got 0 items on every attempt across {len(urls_to_try)} domain(s) — "
              f"treating as failure (implausible for this feed), keeping existing data")
    else:
        print(f"  ✗ {label}: {type(last_exc).__name__ if last_exc else 'unknown'}: {last_exc or '(no message)'} (tried {len(urls_to_try)} domain(s))")
    return source_key, [], False


async def r2_get(client: httpx.AsyncClient, filename: str):
    try:
        r = await client.get(f"{WORKER_URL}/{filename}", headers=DL_HEADERS, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ⚠ r2_get({filename}) failed: {e}")
        return None


async def r2_put(client: httpx.AsyncClient, filename: str, data: dict):
    body = json.dumps(data, ensure_ascii=False).encode()
    r = await client.post(
        f"{WORKER_URL}?file={filename}",
        headers=UP_HEADERS,
        content=body,
        timeout=120
    )
    r.raise_for_status()
    print(f"✓ Uploaded {filename}")


def make_payload(items: list[dict]) -> dict:
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items
    }


# ─────────────────────────────────────────────────────────────────────────
# Financial Results XBRL parsing (in-capmkt / IFIndAs taxonomy)
#
# Context IDs (e.g. "OneD", "FourD") are NOT standardized across filers —
# they're arbitrary labels chosen by whatever software generated the filing.
# We classify every context by its actual period span instead of trusting
# the ID: ~80-100 days -> quarter, ~350-380 days -> year, instant -> balance
# sheet date. Contexts with a dimensional <scenario> (related-party tables,
# other-expenses breakdowns etc.) are skipped — those aren't primary P&L
# figures. If a filing lacks annual or YoY-comparison data, we simply don't
# populate that field rather than guessing.
# ─────────────────────────────────────────────────────────────────────────

XBRL_LINK_RE = re.compile(r"/corporate/xbrl/.*\.xml$", re.IGNORECASE)

_XBRL_FIELD_MAP = {
    "RevenueFromOperations":                                              "revenue",
    "OtherIncome":                                                        "other_income",
    "Income":                                                             "total_income",
    "Expenses":                                                           "total_expenses",
    "ProfitBeforeExceptionalItemsAndTax":                                 "pbt_before_exceptional",
    "ExceptionalItemsBeforeTax":                                          "exceptional_items",
    "ProfitBeforeTax":                                                    "pbt",
    "CurrentTax":                                                         "current_tax",
    "DeferredTax":                                                        "deferred_tax",
    "TaxExpense":                                                         "tax_expense",
    "ProfitLossForPeriod":                                                "pat",
    "ComprehensiveIncomeForThePeriod":                                    "comprehensive_income",
    "PaidUpValueOfEquityShareCapital":                                    "paidup_equity_capital",
    "FaceValueOfEquityShareCapital":                                      "face_value",
    "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations":   "eps_basic",
    "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "eps_diluted",
    "DisclosureOfNotesOnFinancialResultsExplanatoryTextBlock":            "notes_raw",
}

# Phrases NSE filers commonly use to flag that this period isn't a fair
# YoY comparison (business transfers, discontinued ops, restructuring,
# scheme of arrangement, etc). Matched case-insensitively against the
# filing's own notes text — if the company itself says it, we surface it
# rather than silently showing a misleading % change.
_NOT_COMPARABLE_RE = re.compile(
    r"not\s+compar(e|able)|not\s+directly\s+compar|results?\s+(are|is)\s+not\s+compar",
    re.IGNORECASE,
)

_XBRL_META_TAGS = {
    "ScripCode":                                          "scrip_code",
    "Symbol":                                             "symbol",
    "NameOfTheCompany":                                   "company_name",
    "DateOfBoardMeetingWhenFinancialResultsWereApproved": "board_meeting_date",
    "TypeOfReportingPeriod":                               "period_type",
    "ReportingQuarter":                                    "quarter_label",
    "WhetherResultsAreAuditedOrUnaudited":                 "audited",
    "NatureOfReportStandaloneConsolidated":                "standalone_consolidated",
}


def _xbrl_localname(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _xbrl_parse_date(s):
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except (ValueError, AttributeError, TypeError):
        return None


def _xbrl_classify_contexts(root) -> dict:
    ctx_info = {}
    for ctx in root.iter():
        if _xbrl_localname(ctx.tag) != "context":
            continue
        cid = ctx.get("id")
        has_scenario = any(_xbrl_localname(child.tag) == "scenario" for child in ctx)

        period = next((c for c in ctx if _xbrl_localname(c.tag) == "period"), None)
        if period is None:
            continue

        instant_el = start_el = end_el = None
        for p in period:
            ln = _xbrl_localname(p.tag)
            if ln == "instant":
                instant_el = p
            elif ln == "startDate":
                start_el = p
            elif ln == "endDate":
                end_el = p

        if instant_el is not None:
            d = _xbrl_parse_date(instant_el.text)
            ctx_info[cid] = {"type": "instant", "start": None, "end": d,
                              "days": None, "has_scenario": has_scenario}
        elif start_el is not None and end_el is not None:
            s, e = _xbrl_parse_date(start_el.text), _xbrl_parse_date(end_el.text)
            days = (e - s).days if (s and e) else None
            ctx_info[cid] = {"type": "duration", "start": s, "end": e,
                              "days": days, "has_scenario": has_scenario}
    return ctx_info


def _xbrl_bucket(days):
    if days is None:
        return None
    if 75 <= days <= 100:
        return "quarter"
    if 175 <= days <= 190:
        return "half_year"
    if 350 <= days <= 380:
        return "year"
    return None


def _process_notes(period_dict: dict, max_notes_chars: int = 600) -> None:
    """
    Mutates period_dict in place: pops the raw notes text, cleans it, checks
    for a company-stated "not comparable" caveat (common when a business
    segment was transferred/discontinued — e.g. Paytm's Q1 FY27 standalone
    revenue after moving its offline merchant business to a subsidiary),
    and stores a short excerpt + boolean flag plus a truncated general note.
    Scans the FULL text for the caveat before truncating, so a disclaimer
    buried deep in a long notes block isn't missed.
    """
    raw = period_dict.pop("notes_raw", None)
    if not raw or not isinstance(raw, str):
        return

    cleaned = re.sub(r"<br\s*/?>", " ", raw)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    m = _NOT_COMPARABLE_RE.search(cleaned)
    if m:
        # grab the sentence containing the match for a short, useful excerpt
        start = cleaned.rfind(".", 0, m.start()) + 1
        end = cleaned.find(".", m.end())
        end = end + 1 if end != -1 else min(len(cleaned), m.end() + 200)
        excerpt = cleaned[start:end].strip()
        period_dict["yoy_caution"] = True
        period_dict["yoy_caution_note"] = excerpt[:400]

    if cleaned:
        period_dict["notes"] = cleaned[:max_notes_chars] + ("…" if len(cleaned) > max_notes_chars else "")


def _compute_opm(period_dict: dict) -> None:
    """
    Mutates period_dict in place, adding 'opm' as a decimal fraction (e.g.
    0.241 = 24.1%) using the same formula pipeline_fundamentals_prod.py's
    _compute_opm() uses for fundamentals_summary.json ((sales-expenses)/
    sales) — matching methodology is what makes the QoQ/YoY OPM comparison
    against fundamentals data meaningful rather than comparing two
    differently-defined margins.
    """
    revenue = period_dict.get("revenue")
    expenses = period_dict.get("total_expenses")
    if revenue and expenses is not None and revenue != 0:
        period_dict["opm"] = round((revenue - expenses) / revenue, 4)


def parse_financial_results_xbrl(xml_bytes: bytes) -> dict:
    """Parses raw XBRL bytes into {meta, quarter, year, yoy_comparison}."""
    from xml.etree import ElementTree as ET

    root = ET.fromstring(xml_bytes)
    ctx_info = _xbrl_classify_contexts(root)

    buckets = {"quarter": [], "half_year": [], "year": [], "instant": []}
    for cid, info in ctx_info.items():
        if info["has_scenario"]:
            continue
        if info["type"] == "instant":
            buckets["instant"].append(cid)
        else:
            b = _xbrl_bucket(info["days"])
            if b:
                buckets[b].append(cid)

    for b in ("quarter", "half_year", "year", "instant"):
        buckets[b].sort(key=lambda cid: ctx_info[cid]["end"], reverse=True)

    facts_by_ctx = {}
    for el in root.iter():
        ln = _xbrl_localname(el.tag)
        cref = el.get("contextRef")
        if cref is None:
            continue
        facts_by_ctx.setdefault(cref, {})[ln] = el.text

    def extract(cid, tag_map):
        if cid is None or cid not in facts_by_ctx:
            return {}
        raw = facts_by_ctx[cid]
        out = {}
        for xbrl_tag, field in tag_map.items():
            if xbrl_tag in raw and raw[xbrl_tag] is not None:
                val = raw[xbrl_tag]
                try:
                    out[field] = float(val)
                except ValueError:
                    out[field] = val
        return out

    meta_cid = buckets["quarter"][0] if buckets["quarter"] else (
        buckets["year"][0] if buckets["year"] else None)
    result = {"meta": extract(meta_cid, _XBRL_META_TAGS)}

    if buckets["quarter"]:
        cur_q = buckets["quarter"][0]
        result["quarter"] = extract(cur_q, _XBRL_FIELD_MAP)
        result["quarter"]["period_end"] = ctx_info[cur_q]["end"].isoformat()
        result["quarter"]["period_start"] = ctx_info[cur_q]["start"].isoformat()
        _process_notes(result["quarter"])
        _compute_opm(result["quarter"])

        cur_start = ctx_info[cur_q]["start"]
        for cid in buckets["quarter"][1:]:
            other_start = ctx_info[cid]["start"]
            if other_start and cur_start and abs((cur_start - other_start).days - 365) <= 20:
                yoy = extract(cid, _XBRL_FIELD_MAP)
                if yoy:
                    yoy["period_end"] = ctx_info[cid]["end"].isoformat()
                    _compute_opm(yoy)
                    result["yoy_comparison"] = yoy
                break

    if buckets["year"]:
        cur_y = buckets["year"][0]
        result["year"] = extract(cur_y, _XBRL_FIELD_MAP)
        result["year"]["period_end"] = ctx_info[cur_y]["end"].isoformat()
        result["year"]["period_start"] = ctx_info[cur_y]["start"].isoformat()
        _process_notes(result["year"])
        _compute_opm(result["year"])

    return result


FUNDAMENTALS_FILE = "fundamentals_summary.json"


def _quarter_header(iso_date: str):
    """'2026-06-30' -> 'Jun 2026' (matches fundamentals_summary.json's quarter header format)."""
    try:
        d = datetime.strptime(iso_date, "%Y-%m-%d")
        return d.strftime("%b %Y")
    except (ValueError, TypeError):
        return None


def _fundamentals_basis(symbol: str, xbrl_nature: str, fundamentals: dict):
    """Returns (stock_dict, basis_label) if fundamentals_summary.json's stype
    for this symbol matches the XBRL filing's own standalone/consolidated
    nature, else (None, None) — see _compare_to_fundamentals docstring for
    why we refuse to guess across a basis mismatch.

    Checks the primary stype first, then the dual-tracked alt series
    (quarters_alt/stype_alt — added July 2026 to pipeline_fundamentals_prod.py)
    before giving up. The primary pick is a strict-recency tie-break that
    favours Consolidated on a tie even when Standalone is equally current,
    so a Standalone XBRL filing would otherwise never match even though the
    data exists in fundamentals — quarters_alt is where fundamentals stores
    that "lost" tie-break series.
    """
    if not fundamentals or not symbol:
        return None, None
    stock = fundamentals.get(symbol.upper())
    if not stock:
        return None, None
    basis_map = {"c": "consolidated", "s": "standalone"}
    nature = (xbrl_nature or "").strip().lower()

    stype = (stock.get("stype") or "").strip().lower()
    if stype in basis_map and basis_map[stype] == nature:
        return stock, basis_map[stype]

    stype_alt = (stock.get("stype_alt") or "").strip().lower()
    if stype_alt in basis_map and basis_map[stype_alt] == nature and stock.get("quarters_alt"):
        # Shim: reuse _compare_to_fundamentals' existing stock["quarters"]
        # lookup by presenting quarters_alt under that same key.
        alt_stock = dict(stock)
        alt_stock["quarters"] = stock["quarters_alt"]
        return alt_stock, basis_map[stype_alt]

    return None, None


def _compare_to_fundamentals(stock: dict, basis: str, xbrl_quarter: dict, prior_header: str, suffix: str):
    """
    Shared comparison logic for both YoY and QoQ: looks up `prior_header`
    in the stock's fundamentals quarters, and computes % change for
    Revenue/PAT/EPS against the XBRL-parsed current quarter (xbrl_quarter)
    — not against fundamentals' own current-quarter figure, which usually
    isn't there yet (fundamentals lags the live XBRL feed).

    suffix distinguishes the output field names ("yoy" -> sales_yoy_pct,
    "qoq" -> sales_qoq_pct) so both can coexist in the same result dict.
    """
    if not xbrl_quarter or not prior_header:
        return None
    quarters = stock.get("quarters") or []
    by_header = {q.get("header"): q for q in quarters if q.get("header")}
    prior_q = by_header.get(prior_header)
    if not prior_q:
        return None

    out = {"basis": basis, "basis_verified": True, "prior_header": prior_header}
    field_map = {"revenue": "sales", "pat": "pat", "eps_basic": "eps"}
    got_any = False
    for xbrl_field, fund_field in field_map.items():
        cur_v = xbrl_quarter.get(xbrl_field)
        prior_v = prior_q.get(fund_field)
        if cur_v is not None and prior_v is not None and prior_v != 0:
            out[f"{fund_field}_prior"] = prior_v
            out[f"{fund_field}_{suffix}_pct"] = round((cur_v - prior_v) / abs(prior_v) * 100, 2)
            got_any = True

    # OPM — percentage-POINT change, not relative % change. A margin is
    # already a percentage, so "OPM 24.1% (+1.8pp)" is what's meaningful,
    # not "OPM changed by +8.1%" (relative change of a percentage is
    # confusing to read). fundamentals' own 'opm' field is a decimal
    # fraction (e.g. 0.223), same convention as xbrl_quarter['opm'].
    cur_opm = xbrl_quarter.get("opm")
    prior_opm = prior_q.get("opm")
    if cur_opm is not None and prior_opm is not None:
        out["opm_prior"] = round(prior_opm * 100, 2)
        out[f"opm_{suffix}_pp"] = round((cur_opm - prior_opm) * 100, 2)
        got_any = True

    return out if got_any else None


def _yoy_fundamentals(symbol: str, period_end_iso: str, xbrl_quarter: dict, xbrl_nature: str, fundamentals: dict):
    """
    Fallback YoY using the fundamentals database when the XBRL filing itself
    didn't tag a prior-year-same-quarter context (common — many filers only
    tag the current period). Only needs fundamentals' PRIOR-year quarter —
    the current quarter's figures come from the XBRL we already parsed.

    BASIS CHECK: fundamentals_summary.json tags each stock's series with
    `stype` ("c"=Consolidated, "s"=Standalone). We only compute YoY when
    this matches the XBRL filing's own NatureOfReportStandaloneConsolidated
    — Standalone vs Consolidated PAT/Revenue can differ by 15-20%+ for the
    same company/quarter (seen directly: Paytm standalone PAT ₹185cr vs
    consolidated ₹220cr, same quarter), so comparing across a basis
    mismatch would produce a misleading % change. On mismatch or missing
    stype, we skip rather than guess.
    """
    stock, basis = _fundamentals_basis(symbol, xbrl_nature, fundamentals)
    if not stock:
        return None
    cur_header = _quarter_header(period_end_iso)
    if not cur_header:
        return None
    try:
        cur_month, cur_year = cur_header.split()
        prior_header = f"{cur_month} {int(cur_year) - 1}"
    except ValueError:
        return None
    return _compare_to_fundamentals(stock, basis, xbrl_quarter, prior_header, "yoy")


def _qoq_fundamentals(symbol: str, xbrl_quarter: dict, xbrl_nature: str, fundamentals: dict):
    """
    QoQ (immediately-preceding quarter) comparison. XBRL filings essentially
    never tag the prior quarter as a context (unlike prior-year, which some
    filers do), so this is fundamentals-only — no XBRL-native equivalent to
    check first, unlike YoY. Prior quarter is derived from the current
    quarter's own period_start (one day earlier = prior quarter's end date),
    which is exact rather than assuming a fixed calendar-quarter cycle.
    """
    if not xbrl_quarter:
        return None
    stock, basis = _fundamentals_basis(symbol, xbrl_nature, fundamentals)
    if not stock:
        return None
    period_start = xbrl_quarter.get("period_start")
    if not period_start:
        return None
    try:
        start_date = datetime.strptime(period_start, "%Y-%m-%d").date()
    except ValueError:
        return None
    prior_end = start_date - timedelta(days=1)
    prior_header = prior_end.strftime("%b %Y")
    return _compare_to_fundamentals(stock, basis, xbrl_quarter, prior_header, "qoq")


async def fetch_xbrl_bytes(client: httpx.AsyncClient, url: str, retries: int = 3):
    """Fetch raw XBRL bytes with backoff on 502/503/504/network errors —
    same flakiness profile as NSE's other archive endpoints."""
    for attempt in range(retries):
        try:
            r = await client.get(url, headers=BROWSER_HEADERS, timeout=30, follow_redirects=True)
            if r.status_code == 404:
                return None
            if r.status_code in (502, 503, 504):
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                r.raise_for_status()
            r.raise_for_status()
            return r.content
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            raise RuntimeError(str(e))
    return None


def _fmt_cr(val):
    """Formats a raw rupee value as ₹X.XX Cr for Telegram messages."""
    if val is None:
        return "—"
    try:
        return f"₹{val / 1e7:,.2f} Cr"
    except (TypeError, ZeroDivisionError):
        return "—"


def _telegram_basis_block(parsed: dict) -> list:
    """Builds the Current Qtr / QoQ / YoY lines for ONE basis (Standalone or
    Consolidated). No header/company-name lines — those are built once by
    the caller so two bases for the same company share a single message."""
    q = parsed.get("quarter", {})
    revenue = q.get("revenue")
    pat = q.get("pat")
    pat_emoji = "🟢" if (pat is not None and pat >= 0) else ("🔴" if pat is not None else "")
    cur_header = _quarter_header(q.get("period_end")) or ""

    lines = [f"<b>Current Qtr{' (' + cur_header + ')' if cur_header else ''}</b>"]
    lines.append(f"Rev: <b>{_fmt_cr(revenue)}</b>")
    lines.append(f"PAT: {pat_emoji} <b>{_fmt_cr(pat)}</b>")
    if q.get("eps_basic") is not None:
        lines.append(f"EPS: <b>₹{q['eps_basic']}</b>")

    def _pct(cur_v, prior_v):
        if cur_v is None or prior_v is None or prior_v == 0:
            return None
        return (cur_v - prior_v) / abs(prior_v) * 100

    def _section(title, prior_header, cur_rev, cur_pat, rev_pct, pat_pct, opm_current_pct=None, opm_pp=None, prefix=""):
        sec = ["", f"<b>{title}{' (vs ' + prior_header + ')' if prior_header else ''}</b>"]
        if cur_rev is not None and rev_pct is not None:
            sec.append(f"Rev: {_fmt_cr(cur_rev)} ({prefix}{'+' if rev_pct >= 0 else ''}{rev_pct:.1f}%)")
        if cur_pat is not None and pat_pct is not None:
            sec.append(f"PAT: {_fmt_cr(cur_pat)} ({prefix}{'+' if pat_pct >= 0 else ''}{pat_pct:.1f}%)")
        if opm_current_pct is not None and opm_pp is not None:
            sec.append(f"OPM: {opm_current_pct}% ({prefix}{'+' if opm_pp >= 0 else ''}{opm_pp:.1f}pp)")
        return sec if len(sec) > 2 else []

    cur_opm_pct = round(q["opm"] * 100, 2) if q.get("opm") is not None else None

    qf = parsed.get("qoq_fundamentals")
    if qf:
        prefix = "" if qf.get("basis_verified") else "~"
        lines += _section("QoQ", qf.get("prior_header"), revenue, pat,
                           qf.get("sales_qoq_pct"), qf.get("pat_qoq_pct"),
                           cur_opm_pct, qf.get("opm_qoq_pp"), prefix)

    yoy = parsed.get("yoy_comparison")
    yf = parsed.get("yoy_fundamentals")
    if yoy:
        rev_pct = _pct(revenue, yoy.get("revenue"))
        pat_pct = _pct(pat, yoy.get("pat"))
        yoy_header = _quarter_header(yoy.get("period_end"))
        opm_pp = round((q["opm"] - yoy["opm"]) * 100, 2) if q.get("opm") is not None and yoy.get("opm") is not None else None
        lines += _section("YoY", yoy_header, revenue, pat, rev_pct, pat_pct, cur_opm_pct, opm_pp)
    elif yf:
        prefix = "" if yf.get("basis_verified") else "~"
        lines += _section("YoY", yf.get("prior_header"), revenue, pat,
                           yf.get("sales_yoy_pct"), yf.get("pat_yoy_pct"),
                           cur_opm_pct, yf.get("opm_yoy_pp"), prefix)

    if q.get("yoy_caution"):
        lines.append("")
        lines.append("⚠️ Company notes: results may not be YoY comparable")

    return lines


def _telegram_result_message(group) -> str:
    """
    Builds ONE Telegram message for a company's result. `group` is either a
    single parsed dict (one basis filed) or a list of 1-2 parsed dicts
    (Standalone + Consolidated for the same company/quarter) — grouped by
    _group_parsed_results() before this is called, so the two bases always
    arrive in the same message instead of as separate messages that other
    companies' results can get interleaved between.
    """
    items = group if isinstance(group, list) else [group]
    items = sorted(items, key=lambda p: 0 if (p.get("meta", {}).get("standalone_consolidated") == "Consolidated") else 1)

    first_meta = items[0].get("meta", {})
    company = first_meta.get("company_name") or items[0].get("title") or "Unknown"
    quarter_label = first_meta.get("quarter_label") or ""
    audited = first_meta.get("audited") or ""
    board_date = first_meta.get("board_meeting_date")

    lines = [f"📊 <b>{company}</b>"]
    tag_bits = [b for b in (quarter_label, audited) if b]
    if tag_bits:
        lines.append(" · ".join(tag_bits))
    if board_date:
        lines.append(f"Result Date: {board_date}")

    for i, parsed in enumerate(items):
        nature = parsed.get("meta", {}).get("standalone_consolidated") or ""
        lines.append("")
        if nature:
            lines.append(f"━━ <b>{nature.upper()}</b> ━━")
        lines += _telegram_basis_block(parsed)

    return "\n".join(lines)


def _group_parsed_results(parsed_new: list) -> list:
    """
    Groups newly-parsed results by company+quarter (scrip_code +
    board_meeting_date + quarter period_end) so Standalone and Consolidated
    filings for the same result — which arrive as two separate XBRL files —
    get sent as ONE Telegram message instead of two, which previously let
    other companies' messages land in between them.
    """
    groups = {}
    order = []
    for p in parsed_new:
        meta = p.get("meta", {})
        q = p.get("quarter", {})
        key = (meta.get("scrip_code"), meta.get("board_meeting_date"), q.get("period_end"))
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(p)
    return [groups[k] for k in order]


async def build_results_detailed(client: httpx.AsyncClient, results_items: list[dict], fundamentals: dict | None) -> dict | None:
    """
    For nse_results_feed.json items whose link points to an XBRL file,
    fetch + parse P&L figures and merge into nse_results_detailed.json.
    Only processes links not already present (idempotent across runs —
    avoids re-fetching ~150+ XBRL files every poll).
    """
    xbrl_items = [it for it in results_items if XBRL_LINK_RE.search(it.get("link", ""))]
    if not xbrl_items:
        print("  ⚠ No XBRL-linked results items — skipping detail parse")
        return None

    existing = await r2_get(client, "nse_results_detailed.json")
    existing_items = (existing or {}).get("items", [])
    existing_links = {it.get("link") for it in existing_items}

    new_items = [it for it in xbrl_items if it["link"] not in existing_links]
    if not new_items:
        print("  ✓ nse_results_detailed: no new XBRL filings to parse")
        return None

    print(f"  Parsing {len(new_items)} new XBRL result filing(s)...")
    sem = asyncio.Semaphore(5)  # be polite to nsearchives.nseindia.com

    async def process(it):
        async with sem:
            try:
                content = await fetch_xbrl_bytes(client, it["link"])
                if not content:
                    return None
                parsed = parse_financial_results_xbrl(content)
                if not parsed.get("quarter") and not parsed.get("year"):
                    return None  # not a financial-results XBRL (or empty) — skip silently
                parsed["link"] = it["link"]
                parsed["title"] = it.get("title", "")
                parsed["published"] = it.get("published", "")
                parsed["published_ts"] = it.get("published_ts", 0)

                if "yoy_comparison" not in parsed and parsed.get("quarter", {}).get("period_end"):
                    symbol = parsed.get("meta", {}).get("symbol")
                    nature = parsed.get("meta", {}).get("standalone_consolidated")
                    yoy_fund = _yoy_fundamentals(symbol, parsed["quarter"]["period_end"], parsed["quarter"], nature, fundamentals)
                    if yoy_fund:
                        parsed["yoy_fundamentals"] = yoy_fund

                if parsed.get("quarter"):
                    symbol = parsed.get("meta", {}).get("symbol")
                    nature = parsed.get("meta", {}).get("standalone_consolidated")
                    qoq_fund = _qoq_fundamentals(symbol, parsed["quarter"], nature, fundamentals)
                    if qoq_fund:
                        parsed["qoq_fundamentals"] = qoq_fund

                return parsed
            except Exception as e:
                print(f"  ⚠ XBRL parse failed for {it['link'].split('/')[-1]}: {e}")
                return None

    results = await asyncio.gather(*(process(it) for it in new_items))
    parsed_new = [r for r in results if r]
    print(f"  ✓ Parsed {len(parsed_new)}/{len(new_items)} successfully")

    if parsed_new:
        groups = _group_parsed_results(parsed_new)
        print(f"  Sending {len(groups)} Telegram message(s) ({len(parsed_new)} filings grouped)...")
        if not TELEGRAM_RESULTS_CHAT_ID:
            print("  ⚠ TELEGRAM_RESULTS_CHAT_ID not set — results going to the main "
                  "TELEGRAM_CHAT_ID channel (will mix with pipeline status alerts). "
                  "Set TELEGRAM_RESULTS_CHAT_ID to send these to a separate channel.")
        # Sequential with a small delay — Telegram's per-chat flood limit is
        # roughly ~1 msg/sec sustained; sending a batch of ~20 all at once
        # risks 429s. Individual send failures are swallowed (not fatal to
        # the pipeline — results are still saved to R2 either way).
        for group in groups:
            try:
                send_message(_telegram_result_message(group), chat_id=TELEGRAM_RESULTS_CHAT_ID)
            except Exception as e:
                sym = group[0].get("meta", {}).get("symbol") if group else "?"
                print(f"  ⚠ Telegram send failed for {sym}: {e}")
            await asyncio.sleep(1)

    merged = existing_items + parsed_new
    merged.sort(key=lambda x: x.get("published_ts", 0), reverse=True)
    merged = merged[:1000]  # cap file size — keep most recent 1000 filings

    return make_payload(merged)


async def run():
    now = datetime.now(timezone.utc).isoformat()
    print(f"Fetching all feeds... [{now}]")

    async with httpx.AsyncClient() as client:
        # Fetch all feeds concurrently
        tasks = [fetch_feed(client, sk, label, url) for sk, label, url in FEEDS]
        results = await asyncio.gather(*tasks)
        result_map  = {sk: items for sk, items, ok in results}
        success_map = {sk: ok    for sk, items, ok in results}

        uploads = []
        results_feed_items = []

        for filename, source_keys in OUTPUT_MAP.items():

            failed_sources = [sk for sk in source_keys if not success_map.get(sk, False)]
            if failed_sources:
                print(f"  ⚠ {filename}: skipping upload — fetch failed for {failed_sources}, "
                      f"keeping existing R2 data untouched")
                continue

            items = []
            for sk in source_keys:
                items.extend(result_map.get(sk, []))

            # Newest first (merged sources ke liye zaroori, aur dedup
            # latest published wala instance rakhta hai)
            items.sort(key=lambda x: x.get("published_ts", 0), reverse=True)

            before = len(items)
            items = [it for it in items if not is_noise(it)]
            dropped_noise = before - len(items)

            before_dedup = len(items)
            items = dedup_items(items)
            dropped_dup = before_dedup - len(items)

            if dropped_noise or dropped_dup:
                print(f"  {filename}: -{dropped_noise} noise, -{dropped_dup} dup → {len(items)}")

            if filename == "nse_results_feed.json":
                results_feed_items = items

            uploads.append((filename, make_payload(items)))

        # Upload all concurrently
        print("\nUploading to R2...")
        upload_tasks = [r2_put(client, fname, payload) for fname, payload in uploads]
        await asyncio.gather(*upload_tasks)

        # ── Financial results detail (P&L from XBRL) ────────────────────
        print("\nParsing financial results XBRL...")
        fundamentals = await r2_get(client, FUNDAMENTALS_FILE)
        fundamentals_stocks = (fundamentals or {}).get("stocks")
        if not fundamentals_stocks:
            print(f"  ⚠ {FUNDAMENTALS_FILE} unavailable — YoY fallback via fundamentals disabled this run")
        detailed_payload = await build_results_detailed(client, results_feed_items, fundamentals_stocks)
        if detailed_payload:
            await r2_put(client, "nse_results_detailed.json", detailed_payload)

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
