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
    "Cache-Control": "no-cache",
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
    r"^(notice of )?shareholders? meetings?(-xbrl)?$",  # AGM/EGM/postal ballot voting outcomes — not trading-actionable (covers both the plain-feed and XBRL-tagged variants)
    r"^allotment of securities$",   # routine NCD/ESOP allotment filings
    r"^change in directors?/kmp/smp/auditor/rta$",  # routine KMP/auditor/RTA administrative changes
    r"^change in director\(s\)$",                   # routine board-composition filings (not MD/CEO-level)
    r"^appointment$",                                # generic appointment notices (KMP/company secretary level)
    r"^cessation$",                                  # generic cessation notices (KMP/director resignations)
    r"^options to purchase securities$",             # ESOP/stock benefit grants — compliance filing, not trading-actionable
    r"^analysts?/institutional investor meet/con\. call updates$",  # analyst meet schedule/outcome/transcript — routine, very high frequency
    r"^analyst/investor meet para a-xbrl$",                          # XBRL-tagged variant of the same analyst-meet noise
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
                IST = timezone(timedelta(hours=5, minutes=30))
                for entry in feed.entries:

                    # Epoch timestamp for reliable cross-source sorting
                    ts = 0
                    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
                    if parsed:
                        try:
                            ts = calendar.timegm(parsed)
                        except Exception:
                            ts = 0
                    if not ts:
                        # NSE's own feeds (results, board meetings, corp actions)
                        # use a non-standard "DD-Mon-YYYY HH:MM:SS" IST string
                        # with no weekday/timezone, which feedparser's RFC822/
                        # ISO parsers silently fail on (published_parsed stays
                        # None) — parse it manually instead of falling back to 0,
                        # which broke newest-first sort/1000-cap truncation and
                        # made every item look equally "new".
                        raw = entry.get("published", "") or entry.get("updated", "")
                        try:
                            dt = datetime.strptime(raw.strip(), "%d-%b-%Y %H:%M:%S").replace(tzinfo=IST)
                            ts = int(dt.astimezone(timezone.utc).timestamp())
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
    # Cache-bust every call. Without this, the Worker/Cloudflare edge can
    # serve a stale cached response for this exact URL — which silently
    # breaks the new-vs-already-processed dedup in build_results_detailed()
    # (a stale/empty read makes every filing look "new" again on the very
    # next run, even minutes after the previous run's upload succeeded).
    try:
        v = int(time.time())
        sep = "&" if "?" in filename else "?"
        r = await client.get(f"{WORKER_URL}/{filename}{sep}v={v}", headers=DL_HEADERS, timeout=30)
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

# XBRL filenames embed a DDMMYYYYHHMMSS submission timestamp, e.g.
# INTEGRATED_FILING_INDAS_1699505_22072026062423_WEB.xml -> 22072026062423.
# This is far more reliable than the RSS entry's published_ts (which has
# been observed to come through as 0 for this feed) for deciding which of
# two filings for the same symbol+quarter+nature is the newer one.
_XBRL_FILENAME_TS_RE = re.compile(r"_(\d{14})_WEB\.xml$", re.IGNORECASE)
# Generic fallback: NSE embeds a DDMMYYYYHHMMSS submission timestamp in
# virtually every corporate filing filename regardless of file type
# (XBRL .xml, PDF outcome letters, etc) — e.g. "KAYA_03082026160711_..." or
# "BLUEJET_03082026124851_FinalUpload.pdf". Used when the file isn't XBRL.
_GENERIC_FILENAME_TS_RE = re.compile(r"_(\d{14})_")


def _filing_ts(link: str) -> str:
    m = _XBRL_FILENAME_TS_RE.search(link or "")
    if m:
        return m.group(1)
    m = _GENERIC_FILENAME_TS_RE.search(link or "")
    return m.group(1) if m else ""


_IST = timezone(timedelta(hours=5, minutes=30))


def _effective_ts(it: dict) -> int:
    """Sort key for merging/capping nse_results_detailed.json. Prefers the
    RSS published_ts, but falls back to the filename-embedded submission
    timestamp when published_ts is 0 — which every record parsed before the
    published_ts date-parsing fix has. Without this fallback, a large batch
    of same-valued (0) timestamps makes the merge sort a no-op, so newly
    parsed+notified items can get silently dropped by the 1000-item cap
    truncation before ever being persisted (they were already sent to
    Telegram, but never actually saved) — causing the exact same filings to
    look "new" again on the next run and get re-notified forever."""
    ts = it.get("published_ts", 0)
    if ts:
        return ts
    fts = _filing_ts(it.get("link", ""))
    if fts:
        try:
            dt = datetime.strptime(fts, "%d%m%Y%H%M%S").replace(tzinfo=_IST)
            return int(dt.astimezone(timezone.utc).timestamp())
        except ValueError:
            pass
    return 0

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

# ── PDF fast-path parsing (Outcome of Board Meeting) ────────────────────
# NSE's XBRL filing for a result often lands noticeably later than the
# "Outcome of Board Meeting" PDF for the same result (the PDF is filed the
# moment the board approves it; XBRL is a separate, slower submission).
# This is a best-effort text/regex parser — PDFs aren't a standardized
# machine-readable format the way XBRL is, so it targets only the core
# line items needed for the Telegram alert. When the XBRL filing for the
# same symbol+quarter+nature shows up later, build_results_detailed's
# "refiled" handling silently supersedes this record with the authoritative
# XBRL data — so an imperfect PDF parse just gets corrected, it never
# blocks or duplicates the real notification.

_PDF_SUBJECT_RE = re.compile(r"outcome of board meeting", re.IGNORECASE)
_PDF_STATEMENT_HEADING_RE = re.compile(
    r"Statement of (Standalone|Consolidated)[\s\S]{0,80}?Financial Results", re.IGNORECASE
)
# Fallback for filings that don't use the "Statement of ..." prefix at all —
# e.g. just "STANDALONE UNAUDITED FINANCIAL RESULTS FOR THE QUARTER ENDED...".
# Tried only if the primary pattern above finds nothing.
_PDF_STATEMENT_HEADING_FALLBACK_RE = re.compile(
    r"(Standalone|Consolidated)[\s\S]{0,40}?Financial Results", re.IGNORECASE
)
_PDF_FILENAME_TS_RE = re.compile(r"^([A-Z0-9&\-]+)_(\d{2})(\d{2})(\d{4})\d{6}_", re.IGNORECASE)
_PDF_FORMULA_REF_RE = re.compile(r"\(\s*\d+\s*[+\-]\s*\d+\s*\)")
_PDF_NUM_TOKEN_RE = r"\(?-?[\d,]+\.?\d*\)?|-|—"


def _is_board_outcome_pdf(it: dict) -> bool:
    link = it.get("link", "")
    if not link.lower().endswith(".pdf"):
        return False
    m = _SUBJECT_TAG_RE.search(it.get("summary", ""))
    return bool(m and _PDF_SUBJECT_RE.search(m.group(1)))


def _pdf_parse_num(tok: str):
    tok = tok.strip()
    if tok in ("-", "—", ""):
        return 0.0
    neg = tok.startswith("(") and tok.endswith(")")
    tok = tok.strip("()").replace(",", "").strip()
    if not tok:
        return None
    try:
        v = float(tok)
        return -v if neg else v
    except ValueError:
        return None


def _pdf_numbers_after(section: str, label_pattern: str, max_cols: int = 4):
    """Finds the label, then reads up to max_cols numeric tokens on the same
    line — the standard NSE quarterly-result row layout is
    [current quarter, immediately-preceding quarter, same quarter last year,
    full year], all on one line. Returns a list padded with None to
    max_cols. Skips formula-reference parentheticals like "(3 - 4)"."""
    m = re.search(label_pattern, section, re.IGNORECASE)
    if not m:
        return [None] * max_cols
    nl = section.find("\n", m.end())
    tail = section[m.end(): nl if nl != -1 else m.end() + 400]
    tail = _PDF_FORMULA_REF_RE.sub(" ", tail)
    vals = [_pdf_parse_num(t) for t in re.findall(_PDF_NUM_TOKEN_RE, tail)]
    vals = vals[:max_cols]
    return vals + [None] * (max_cols - len(vals))


def _pdf_number_after(section: str, label_pattern: str):
    """Current-quarter (first column) convenience wrapper around
    _pdf_numbers_after."""
    return _pdf_numbers_after(section, label_pattern, max_cols=1)[0]


def _pdf_header_dates(section: str):
    """Extracts the column header dates from the results table's
    'Particulars <date1> <date2> ...' row, e.g. ['30 June 2026',
    '31 March 2026', '30 June 2025', '31 March 2026']. Returns ISO dates,
    padded with None to 4 columns."""
    m = re.search(r"Particulars\s+((?:\d{1,2}\s+[A-Za-z]+\s+\d{4}\s*){2,4})", section)
    if not m:
        return [None] * 4
    raw_dates = re.findall(r"\d{1,2}\s+[A-Za-z]+\s+\d{4}", m.group(1))
    iso_dates = []
    for d in raw_dates[:4]:
        try:
            iso_dates.append(datetime.strptime(d.strip(), "%d %B %Y").strftime("%Y-%m-%d"))
        except ValueError:
            iso_dates.append(None)
    return iso_dates + [None] * (4 - len(iso_dates))


def _pdf_comparison(cur: dict, prior: dict, prior_header, suffix: str):
    """Builds a comparison dict in the same shape _compare_to_fundamentals()
    produces (sales_prior/sales_{suffix}_pct, pat_..., eps_..., opm_...),
    but computed directly from the PDF's own comparative column instead of
    a separate fundamentals lookup — this is the filing's own reported
    comparative figure, which is more precise than a database join.
    basis="reported" flags this as sourced from the filing itself."""
    if not prior_header or not any(v is not None for v in prior.values()):
        return None
    out = {"basis": "reported", "basis_verified": True, "prior_header": prior_header}
    field_map = {"revenue": "sales", "pat": "pat", "eps_basic": "eps"}
    got_any = False
    for cur_field, out_field in field_map.items():
        cur_v, prior_v = cur.get(cur_field), prior.get(cur_field)
        if cur_v is not None and prior_v is not None and prior_v != 0:
            out[f"{out_field}_prior"] = prior_v
            out[f"{out_field}_{suffix}_pct"] = round((cur_v - prior_v) / abs(prior_v) * 100, 2)
            got_any = True
    prior_rev, prior_exp = prior.get("revenue"), prior.get("total_expenses")
    prior_opm = (prior_rev - prior_exp) / prior_rev if (prior_rev and prior_exp is not None and prior_rev != 0) else None
    cur_opm = cur.get("opm")
    if cur_opm is not None and prior_opm is not None:
        out["opm_prior"] = round(prior_opm * 100, 2)
        out[f"opm_{suffix}_pp"] = round((cur_opm - prior_opm) * 100, 2)
        got_any = True
    return out if got_any else None


def _pdf_quarter_label(period_end_iso: str):
    try:
        d = datetime.strptime(period_end_iso, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None
    if d.month in (4, 5, 6):
        q, fy_end = 1, d.year + 1
    elif d.month in (7, 8, 9):
        q, fy_end = 2, d.year + 1
    elif d.month in (10, 11, 12):
        q, fy_end = 3, d.year + 1
    else:
        q, fy_end = 4, d.year
    return f"Q{q} FY{str(fy_end)[-2:]}"


def parse_financial_results_pdf(content: bytes, link: str):
    """Best-effort parse of an 'Outcome of Board Meeting' PDF into the same
    {meta, quarter} shape parse_financial_results_xbrl() produces, so it can
    flow through the same grouping/dedup/Telegram code. Prefers the
    Consolidated results table over Standalone when a PDF contains both.
    Returns None if this isn't a financial-results outcome (e.g. a PDF only
    about KMP appointments) or the core numbers can't be found."""
    from pypdf import PdfReader
    import io as _io

    fname_dbg = link.rsplit("/", 1)[-1]

    try:
        reader = PdfReader(_io.BytesIO(content))
        text = "\n".join((p.extract_text() or "") for p in reader.pages)
    except Exception as e:
        print(f"    · [{fname_dbg}] PdfReader/extract_text raised: {type(e).__name__}: {e}")
        return None
    if not text.strip():
        print(f"    · [{fname_dbg}] extracted text is empty (likely a scanned/image-only PDF)")
        return None

    headings = list(_PDF_STATEMENT_HEADING_RE.finditer(text))
    used_fallback = False
    if not headings:
        headings = list(_PDF_STATEMENT_HEADING_FALLBACK_RE.finditer(text))
        used_fallback = True
    if not headings:
        snippet = ""
        m_fr = re.search(r"financial results", text, re.IGNORECASE)
        if m_fr:
            start = max(0, m_fr.start() - 60)
            snippet = text[start:m_fr.end() + 20].replace("\n", "⏎")
        print(f"    · [{fname_dbg}] no 'Standalone/Consolidated ... Financial Results' "
              f"heading found (tried both primary and fallback patterns) — not a results table "
              f"(governance/KMP-only outcome PDF), or heading wording differs further from expected"
              + (f" | nearby text: ...{snippet}..." if snippet else " | 'financial results' not found in text at all"))
        return None  # no results table in this PDF (pure governance/KMP outcome)
    if used_fallback:
        print(f"    · [{fname_dbg}] matched via fallback heading pattern (primary 'Statement of ...' pattern missed it)")

    chosen = next((h for h in headings if h.group(1).lower() == "consolidated"), headings[0])
    nature = "Consolidated" if chosen.group(1).lower() == "consolidated" else "Standalone"
    start = chosen.end()
    later = [h.start() for h in headings if h.start() > start]
    section = text[start: min(later) if later else len(text)]

    revenue_c        = _pdf_numbers_after(section, r"Revenue from operations")
    other_income_c   = _pdf_numbers_after(section, r"Other income")
    total_income_c   = _pdf_numbers_after(section, r"Total income")
    total_expenses_c = _pdf_numbers_after(section, r"Total expenses")
    pbt_c            = _pdf_numbers_after(section, r"\)\s*before tax")
    tax_expense_c    = _pdf_numbers_after(section, r"Total tax expense")
    pat_c            = _pdf_numbers_after(section, r"Net (?:profit|loss|\(loss\)|profit/\(loss\)).*?for the period")
    comprehensive_c  = _pdf_numbers_after(section, r"Total comprehensive.*?for the period")
    eps_basic_c      = _pdf_numbers_after(section, r"\(a\)\s*Basic")
    eps_diluted_c    = _pdf_numbers_after(section, r"\(b\)\s*Diluted")

    revenue, other_income, total_income, total_expenses = revenue_c[0], other_income_c[0], total_income_c[0], total_expenses_c[0]
    pbt, tax_expense, pat, comprehensive = pbt_c[0], tax_expense_c[0], pat_c[0], comprehensive_c[0]
    eps_basic, eps_diluted = eps_basic_c[0], eps_diluted_c[0]

    if revenue is None and pat is None:
        print(f"    · [{fname_dbg}] found a results heading but couldn't extract Revenue or PAT numbers "
              f"— label regex likely didn't match this PDF's exact wording/layout")
        return None  # couldn't find the table's actual numbers — don't fabricate a record

    m_qend = re.search(r"quarter ended\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text, re.IGNORECASE)
    period_end = None
    if m_qend:
        try:
            period_end = datetime.strptime(m_qend.group(1).strip(), "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            period_end = None
    if not period_end:
        print(f"    · [{fname_dbg}] Revenue/PAT found but no 'quarter ended <date>' phrase matched "
              f"— can't build the dedup key without it")
        return None  # can't build a reliable dedup key without the quarter

    fname = link.rsplit("/", 1)[-1]
    m_fn = _PDF_FILENAME_TS_RE.match(fname)
    if not m_fn:
        print(f"    · [{fname_dbg}] all numbers found but filename doesn't match the expected "
              f"'PREFIX_DDMMYYYYHHMMSS_...' timestamp pattern — can't derive board_meeting_date")
        return None
    board_meeting_date = f"{m_fn.group(4)}-{m_fn.group(3)}-{m_fn.group(2)}"

    # The filename prefix is often an internal/uploader ID, NOT the real NSE
    # trading symbol (e.g. "GLAXO1924_...", "NOCIL1961_...", "ESCORTS2_...",
    # "TATACHEMYS_..." for GLAXO/NOCIL/ESCORTS/TATACHEM respectively) — using
    # it as the dedup key's symbol would silently break the match against
    # the later XBRL's authoritative symbol, causing a duplicate Telegram
    # send instead of a quiet update. Pull the real symbol from the PDF's
    # own "NSE Symbol: XXXX" line (present in every NSE outcome letter);
    # fall back to the filename prefix only if that's not found.
    m_sym = re.search(r"NSE\s+Symbol\s*:?\s*\n?\s*([A-Z0-9&]+)", text, re.IGNORECASE)
    symbol = m_sym.group(1).upper() if m_sym else m_fn.group(1)

    m_aud = re.search(r"\((Unaudited|Audited)\)", section, re.IGNORECASE)
    audited = m_aud.group(1).capitalize() if m_aud else None

    first_line = text.strip().split("\n", 1)[0].strip()
    company_name = first_line if first_line and len(first_line) < 80 else symbol

    quarter = {
        "revenue": revenue,
        "other_income": other_income,
        "total_income": total_income,
        "total_expenses": total_expenses,
        "pbt": pbt,
        "tax_expense": tax_expense,
        "pat": pat,
        "comprehensive_income": comprehensive,
        "eps_basic": eps_basic,
        "eps_diluted": eps_diluted,
        "period_end": period_end,
    }
    _compute_opm(quarter)

    result = {
        "meta": {
            "symbol": symbol,
            "company_name": company_name,
            "board_meeting_date": board_meeting_date,
            "standalone_consolidated": nature,
            "audited": audited,
            "quarter_label": _pdf_quarter_label(period_end),
            "scrip_code": None,
            "source": "pdf",
        },
        "quarter": quarter,
    }

    # QoQ (col 1) and YoY (col 2) comparisons, straight from the PDF's own
    # comparative columns — same standard 4-column layout as SEBI Reg. 33
    # quarterly disclosures: [current, immediately-preceding qtr,
    # same qtr last year, full year]. More precise than the fundamentals-
    # database fallback since it's the filing's own reported figures.
    header_dates = _pdf_header_dates(section)
    qoq_prior = {
        "revenue": revenue_c[1], "pat": pat_c[1], "eps_basic": eps_basic_c[1], "total_expenses": total_expenses_c[1],
    }
    yoy_prior = {
        "revenue": revenue_c[2], "pat": pat_c[2], "eps_basic": eps_basic_c[2], "total_expenses": total_expenses_c[2],
    }
    qoq_header = _quarter_header(header_dates[1]) if header_dates[1] else None
    yoy_header = _quarter_header(header_dates[2]) if header_dates[2] else None
    qoq_fund = _pdf_comparison(quarter, qoq_prior, qoq_header, "qoq")
    yoy_fund = _pdf_comparison(quarter, yoy_prior, yoy_header, "yoy")
    if qoq_fund:
        result["qoq_fundamentals"] = qoq_fund
    if yoy_fund:
        result["yoy_fundamentals"] = yoy_fund

    return result


async def fetch_pdf_bytes(client: httpx.AsyncClient, url: str, retries: int = 4):
    """Same retry/backoff/cache-bust profile as fetch_xbrl_bytes — NSE's
    archive host shows the same flakiness for PDFs as for XBRL."""
    sep = "&" if "?" in url else "?"
    for attempt in range(retries):
        fetch_url = url if attempt == 0 else f"{url}{sep}_cb={int(time.time() * 1000)}{attempt}"
        try:
            r = await client.get(fetch_url, headers=BROWSER_HEADERS, timeout=30, follow_redirects=True)
            if r.status_code == 404:
                return None
            if r.status_code in (403, 429, 502, 503, 504):
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt + 1)
                    continue
                r.raise_for_status()
            r.raise_for_status()
            return r.content
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt + 1)
                continue
            raise RuntimeError(str(e))
    return None


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


XBRL_HEADERS = {
    **BROWSER_HEADERS,
    "Accept": "application/xml, text/xml, */*",
    "Referer": "https://www.nseindia.com/",
}


async def fetch_xbrl_bytes(client: httpx.AsyncClient, url: str, retries: int = 4):
    """Fetch raw XBRL bytes with backoff on 403/502/503/504/network errors.

    403s on this host tend to be a CDN-edge-cached negative response tied to
    the exact URL (the file itself is fine — a request from a different
    edge/POP returns 200), not a real per-IP block. So after the first 403
    we retry with a cache-busting query param so the CDN can't serve the
    same cached 403 again — it's forced to treat it as a fresh URL."""
    sep = "&" if "?" in url else "?"
    for attempt in range(retries):
        fetch_url = url if attempt == 0 else f"{url}{sep}_cb={int(time.time() * 1000)}{attempt}"
        try:
            r = await client.get(fetch_url, headers=XBRL_HEADERS, timeout=30, follow_redirects=True)
            if r.status_code == 404:
                return None
            if r.status_code in (403, 429, 502, 503, 504):
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt + 1)
                    continue
                r.raise_for_status()
            r.raise_for_status()
            return r.content
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt + 1)
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
        key = (meta.get("scrip_code") or meta.get("symbol"), meta.get("board_meeting_date"), q.get("period_end"))
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(p)
    return [groups[k] for k in order]


async def build_results_detailed(client: httpx.AsyncClient, results_items: list[dict], board_items: list[dict], fundamentals: dict | None) -> dict | None:
    """
    Builds/updates nse_results_detailed.json from two sources:
      - XBRL filings (results_items, nse_results_feed.json) — authoritative,
        full-detail, but often published well after the board meeting.
      - "Outcome of Board Meeting" PDFs (board_items, nse_board_meetings.json)
        — a fast-path: usually available immediately, core numbers only,
        best-effort regex parse (see parse_financial_results_pdf).
    Both feed the same symbol+quarter+nature dedup key, so if a PDF result
    was already notified, the later XBRL for the same result just updates
    the record silently (see the "refiled" handling below) instead of
    sending a second Telegram message.
    Only processes links not already present (idempotent across runs —
    avoids re-fetching ~150+ files every poll).
    """
    xbrl_items = [it for it in results_items if XBRL_LINK_RE.search(it.get("link", ""))]
    pdf_items = [it for it in board_items if _is_board_outcome_pdf(it)]
    if not xbrl_items and not pdf_items:
        print("  ⚠ No XBRL or board-outcome-PDF results items — skipping detail parse")
        return None

    existing = await r2_get(client, "nse_results_detailed.json")
    existing_items = (existing or {}).get("items", [])
    existing_links = {it.get("link") for it in existing_items}

    def _result_key(it):
        """Business key for a result: same company + same quarter + same
        standalone/consolidated nature = the same underlying result, even if
        NSE re-files it under a brand-new XBRL link (corrections, resubmissions,
        or just a re-publish — same root cause as the NTPC-type re-publishing
        the general feed dedup already works around), or if it was first seen
        as a fast-path PDF and is now confirmed by the authoritative XBRL."""
        meta = it.get("meta", {}) or {}
        quarter = it.get("quarter", {}) or {}
        return (meta.get("symbol"), quarter.get("period_end"), meta.get("standalone_consolidated"))

    # index existing items by business key so a re-filed result (or a later
    # XBRL confirming an earlier fast-path PDF) updates the existing record
    # in place instead of appending a lookalike duplicate
    existing_by_key = {_result_key(it): idx for idx, it in enumerate(existing_items) if _result_key(it)[0]}

    # ⚠️ TEMPORARY: XBRL processing disabled to isolate-test the PDF fast-path.
    # Set back to False (or remove this block) once PDF testing is done.
    DISABLE_XBRL_FOR_TESTING = True
    if DISABLE_XBRL_FOR_TESTING:
        print("  ⚠ XBRL processing disabled for testing — PDF-only this run")
        xbrl_items = []

    new_xbrl = [it for it in xbrl_items if it["link"] not in existing_links]
    new_pdf = [it for it in pdf_items if it["link"] not in existing_links]

    # Give-up tracking: NSE's WAF blocks some specific filing URLs
    # persistently for GitHub Actions' IP/pattern (confirmed: the same file
    # is fetchable from elsewhere, so this isn't a transient/cache issue —
    # it just never succeeds from this runner). Without this, an
    # unfetchable filing gets retried every single run forever since a
    # failure never lands it in existing_links. GIVE_UP_ATTEMPTS caps that:
    # after ~7.5h of retrying (15 runs x 30min), stop hammering it and flag
    # it for manual attention instead.
    GIVE_UP_ATTEMPTS = 15
    failures_payload = await r2_get(client, "nse_xbrl_failures.json")
    failures = (failures_payload or {}).get("links", {})
    given_up_links = {link for link, e in failures.items() if e.get("attempts", 0) >= GIVE_UP_ATTEMPTS}
    if given_up_links:
        before_xbrl, before_pdf = len(new_xbrl), len(new_pdf)
        new_xbrl = [it for it in new_xbrl if it["link"] not in given_up_links]
        new_pdf = [it for it in new_pdf if it["link"] not in given_up_links]
        skipped = (before_xbrl - len(new_xbrl)) + (before_pdf - len(new_pdf))
        if skipped:
            print(f"  ⏭ Skipping {skipped} filing(s) given up after {GIVE_UP_ATTEMPTS}+ failed "
                  f"attempts (see nse_xbrl_failures.json)")

    if not new_xbrl and not new_pdf:
        print("  ✓ nse_results_detailed: no new filings to parse")
        return None

    print(f"  Parsing {len(new_xbrl)} new XBRL + {len(new_pdf)} new PDF result filing(s)...")
    sem = asyncio.Semaphore(3)  # be polite to nsearchives.nseindia.com
    failed_links = []

    def _attach_fundamentals(parsed):
        if "yoy_comparison" not in parsed and "yoy_fundamentals" not in parsed and parsed.get("quarter", {}).get("period_end"):
            symbol = parsed.get("meta", {}).get("symbol")
            nature = parsed.get("meta", {}).get("standalone_consolidated")
            yoy_fund = _yoy_fundamentals(symbol, parsed["quarter"]["period_end"], parsed["quarter"], nature, fundamentals)
            if yoy_fund:
                parsed["yoy_fundamentals"] = yoy_fund
        if "qoq_fundamentals" not in parsed and parsed.get("quarter"):
            symbol = parsed.get("meta", {}).get("symbol")
            nature = parsed.get("meta", {}).get("standalone_consolidated")
            qoq_fund = _qoq_fundamentals(symbol, parsed["quarter"], nature, fundamentals)
            if qoq_fund:
                parsed["qoq_fundamentals"] = qoq_fund

    async def process_xbrl(it):
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
                _attach_fundamentals(parsed)
                return parsed
            except Exception as e:
                print(f"  ⚠ XBRL parse failed for {it['link'].split('/')[-1]}: {e}")
                failed_links.append(it["link"])
                return None

    async def process_pdf(it):
        async with sem:
            fname = it["link"].split("/")[-1]
            try:
                content = await fetch_pdf_bytes(client, it["link"])
                if not content:
                    print(f"  ⚠ PDF fetch returned empty for {fname}")
                    failed_links.append(it["link"])
                    return None
                parsed = parse_financial_results_pdf(content, it["link"])
                if not parsed:
                    print(f"  ⚠ PDF parse returned None for {fname} "
                          f"(no results table / no statement heading / missing quarter-end match — "
                          f"see parse_financial_results_pdf's early-return points)")
                    failed_links.append(it["link"])  # not a results PDF — no point refetching forever
                    return None
                parsed["link"] = it["link"]
                parsed["title"] = it.get("title", "")
                parsed["published"] = it.get("published", "")
                parsed["published_ts"] = it.get("published_ts", 0)
                _attach_fundamentals(parsed)
                return parsed
            except Exception as e:
                print(f"  ⚠ PDF parse failed for {it['link'].split('/')[-1]}: {e}")
                failed_links.append(it["link"])
                return None

    xbrl_results, pdf_results = await asyncio.gather(
        asyncio.gather(*(process_xbrl(it) for it in new_xbrl)),
        asyncio.gather(*(process_pdf(it) for it in new_pdf)),
    )
    parsed_all = [r for r in xbrl_results if r] + [r for r in pdf_results if r]
    print(f"  ✓ Parsed {len(parsed_all)}/{len(new_xbrl) + len(new_pdf)} successfully")

    if failed_links:
        now_iso = datetime.now(timezone.utc).isoformat()
        for link in failed_links:
            entry = failures.get(link, {"first_failed": now_iso, "attempts": 0})
            entry["attempts"] += 1
            entry["last_failed"] = now_iso
            failures[link] = entry
        newly_given_up = [link for link in failed_links
                          if failures[link]["attempts"] == GIVE_UP_ATTEMPTS]
        if newly_given_up:
            print(f"  ⚠ {len(newly_given_up)} filing(s) just crossed {GIVE_UP_ATTEMPTS} failed "
                  f"attempts — giving up on them going forward (nse_xbrl_failures.json)")
        await r2_put(client, "nse_xbrl_failures.json", {"updated_at": now_iso, "links": failures})

    # Intra-batch dedup: NSE sometimes files the same symbol+quarter+nature
    # twice within minutes (correction/resubmission) — both can land as
    # "new" in the SAME run, so the cross-run existing_by_key check below
    # (built before this run started) can't catch them against each other.
    # Keep only the latest per key, using the XBRL filename's embedded
    # submission timestamp (published_ts has been observed as unreliable/0
    # for this feed).
    latest_by_key = {}
    unkeyed = []
    for r in parsed_all:
        key = _result_key(r)
        if not key[0]:
            unkeyed.append(r)
            continue
        prior = latest_by_key.get(key)
        if prior is None or _filing_ts(r.get("link", "")) >= _filing_ts(prior.get("link", "")):
            latest_by_key[key] = r
    superseded_count = len(parsed_all) - len(latest_by_key) - len(unkeyed)
    parsed_all = list(latest_by_key.values()) + unkeyed
    if superseded_count > 0:
        print(f"  ↺ {superseded_count} superseded within this batch (same-run resubmission) — kept latest only")

    # Split out re-filed results (same symbol+quarter+nature already notified
    # under a different link) — refresh their data but don't spam Telegram again.
    parsed_new = []
    refiled = []
    for r in parsed_all:
        key = _result_key(r)
        if key[0] and key in existing_by_key:
            refiled.append(r)
        else:
            parsed_new.append(r)
    if refiled:
        print(f"  ↻ {len(refiled)} re-filed (already notified earlier) — updating record, skipping Telegram")
        for r in refiled:
            existing_items[existing_by_key[_result_key(r)]] = r

    if parsed_new:
        groups = _group_parsed_results(parsed_new)
        print(f"  Sending {len(groups)} Telegram message(s) ({len(parsed_new)} filings grouped)...")
        if not TELEGRAM_RESULTS_CHAT_ID:
            print("  ⚠ TELEGRAM_RESULTS_CHAT_ID not set — results going to the main "
                  "TELEGRAM_CHAT_ID channel (will mix with pipeline status alerts). "
                  "Set TELEGRAM_RESULTS_CHAT_ID to send these to a separate channel.")
        # Sequential with a delay between sends — Telegram's per-chat flood
        # limit is roughly ~1 msg/sec sustained, but real-world timing
        # jitter means even a strict 1s gap can trigger 429s during a
        # heavy burst (e.g. 60+ companies reporting the same evening).
        # On a 429, back off and retry a few times rather than dropping the
        # message — a dropped send here is a PERMANENTLY missed
        # notification, since the filing is still recorded as "already
        # processed" in nse_results_detailed.json regardless of whether
        # the Telegram send succeeded.
        SEND_RETRIES = 4
        for group in groups:
            sym = group[0].get("meta", {}).get("symbol") if group else "?"
            msg = _telegram_result_message(group)
            for attempt in range(SEND_RETRIES):
                try:
                    send_message(msg, chat_id=TELEGRAM_RESULTS_CHAT_ID)
                    break
                except Exception as e:
                    is_last = attempt == SEND_RETRIES - 1
                    is_rate_limit = "429" in str(e)
                    if is_last:
                        print(f"  ⚠ Telegram send failed for {sym} after {SEND_RETRIES} attempts: {e}")
                        break
                    wait = (10 if is_rate_limit else 3) * (attempt + 1)
                    print(f"  ⚠ Telegram send for {sym} failed ({e}), retrying in {wait}s "
                          f"(attempt {attempt+1}/{SEND_RETRIES})...")
                    await asyncio.sleep(wait)
            await asyncio.sleep(2)

    merged = existing_items + parsed_new
    merged.sort(key=_effective_ts, reverse=True)
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
        board_meeting_items = []

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
                # NSE's RSS feed itself only ever shows its latest ~20 items
                # (confirmed: consistently exactly 20 across runs) — if we
                # just re-upload that snapshot each time, results scroll out
                # of the feed (and off the frontend's Results tab) faster
                # than they can be viewed, especially during results season
                # when 100+ companies file in an evening. Accumulate against
                # the existing R2 file instead, same pattern already used
                # for nse_results_detailed.json.
                existing_feed = await r2_get(client, "nse_results_feed.json")
                existing_feed_items = (existing_feed or {}).get("items", [])
                merged_feed = dedup_items(items + existing_feed_items)
                merged_feed.sort(key=_effective_ts, reverse=True)
                merged_feed = merged_feed[:1000]  # same cap as nse_results_detailed.json
                added = len(merged_feed) - len(existing_feed_items)
                print(f"  nse_results_feed.json: {len(existing_feed_items)} existing + "
                      f"{max(added, 0)} new = {len(merged_feed)} (capped at 1000)")
                items = merged_feed
                results_feed_items = items

            if filename == "nse_board_meetings.json":
                board_meeting_items = items

            uploads.append((filename, make_payload(items)))

        # Upload all concurrently
        print("\nUploading to R2...")
        upload_tasks = [r2_put(client, fname, payload) for fname, payload in uploads]
        await asyncio.gather(*upload_tasks)

        # nse_board_meetings.json is just a rolling ~300-item snapshot of the
        # RSS feed (not accumulated, unlike nse_results_feed.json) — a PDF
        # "Outcome of Board Meeting" filed early in the day can scroll out
        # of that window by evening once enough other announcements (KMP
        # changes, press releases, etc.) push it out, well before it's ever
        # been detail-parsed. Keep a small dedicated accumulator so PDF
        # fast-path candidates aren't lost to feed churn, same fix already
        # applied to nse_results_feed.json for the same underlying reason.
        # NSE doesn't consistently route "Outcome of Board Meeting" PDFs
        # through Board_Meetings.xml — some land only in
        # Online_announcements.xml instead (confirmed: e.g. UNO Minda's
        # outcome PDF appeared only in nse_announcements this run, with
        # board_meeting_items empty of it). Use the RAW pre-noise-filtered
        # fetch of both feeds so a future noise-pattern tweak can't
        # accidentally hide a real results PDF from this detector.
        pdf_source_items = result_map.get("nse_board", []) + result_map.get("nse_announcements", [])
        seen_pdf_links = set()
        pdf_candidates_now = []
        for it in pdf_source_items:
            if not _is_board_outcome_pdf(it):
                continue
            link = it.get("link", "")
            if link in seen_pdf_links:
                continue
            seen_pdf_links.add(link)
            pdf_candidates_now.append(it)
        existing_pdf_feed = await r2_get(client, "nse_results_pdf_feed.json")
        existing_pdf_items = (existing_pdf_feed or {}).get("items", [])
        merged_pdf_feed = dedup_items(pdf_candidates_now + existing_pdf_items)
        merged_pdf_feed.sort(key=_effective_ts, reverse=True)
        merged_pdf_feed = merged_pdf_feed[:500]
        print(f"  nse_results_pdf_feed.json: {len(existing_pdf_items)} existing + "
              f"{max(len(merged_pdf_feed) - len(existing_pdf_items), 0)} new = {len(merged_pdf_feed)} (capped at 500)")
        await r2_put(client, "nse_results_pdf_feed.json", make_payload(merged_pdf_feed))

        # ── Financial results detail (P&L from XBRL) ────────────────────
        print("\nParsing financial results XBRL...")
        fundamentals = await r2_get(client, FUNDAMENTALS_FILE)
        fundamentals_stocks = (fundamentals or {}).get("stocks")
        if not fundamentals_stocks:
            print(f"  ⚠ {FUNDAMENTALS_FILE} unavailable — YoY fallback via fundamentals disabled this run")
        detailed_payload = await build_results_detailed(client, results_feed_items, merged_pdf_feed, fundamentals_stocks)
        if detailed_payload:
            await r2_put(client, "nse_results_detailed.json", detailed_payload)

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
