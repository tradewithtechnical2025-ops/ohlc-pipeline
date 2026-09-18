import asyncio
import base64
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

# Used for AI-assisted PDF financial-results extraction. This is now the
# ONLY extraction path for PDFs (regex fallback removed) — if this isn't
# set, PDF result parsing simply doesn't run (see parse_financial_results_pdf).
# Gemini instead of Claude specifically because the free tier needs no card
# on file (vs Anthropic billing, which hit setup friction) — same system
# prompt/schema either way, this just swaps which API answers it. Model name
# drifts periodically as Google renames/retires versions (already hit once
# this project) — check https://aistudio.google.com/app/apikey if this
# starts 404ing.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
AI_PDF_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")

# XBRL processing — enabled, but ONLY as a fallback for results that have
# NO PDF-sourced record at all (see the refiled/parsed_new split below).
# Re-enabling this outright once caused two real problems: (1) XBRL
# OVERWRITES a PDF/AI-parsed record for the same symbol+quarter with a
# plain XBRL-only one — XBRL never carries key_highlights/management_
# commentary/segment_breakup (AI-only concepts), so the richer card
# silently lost that content whenever XBRL "caught up" to a result
# already covered by the PDF fast-path; (2) a backlog of previously-
# unparsed XBRL announcements got treated as "new" the moment this flag
# flipped on, flooding Telegram with already-covered results at once.
# The fallback-only logic below (never overwrite an existing PDF record;
# only fill in results the PDF path never covered at all) plus a recency
# guard on Telegram sends fixes both without losing XBRL-only coverage.
DISABLE_XBRL_FOR_TESTING = False
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
    ("bs_finance",   "Business Standard Finance", "https://www.business-standard.com/rss/finance-103.rss"),
]

# source_key(s) -> R2 output file
# Single key = individual file, list = merged file
OUTPUT_MAP = {
    "nse_results_feed.json":   ["nse_results"],
    "nse_announcements.json":  ["nse_announcements"],
    "nse_board_meetings.json": ["nse_board"],
    "nse_corp_actions.json":   ["nse_corp_actions"],
    "market_news.json":        ["et_markets", "mint_markets", "bs_finance"],
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
                        # Optional richer fields — only Business Standard's
                        # feed populates these right now (media:content for
                        # an article thumbnail, bs:source for the wire/
                        # agency attribution e.g. "Press Trust of India" or
                        # "Bloomberg"). Other sources simply leave these
                        # blank; the frontend treats them as optional.
                        "image":        (entry.get("media_content") or [{}])[0].get("url", ""),
                        "author":       entry.get("bs_source", "") or entry.get("author", ""),
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
#
# Extraction is AI-only (Gemini) — see parse_financial_results_pdf. A cheap
# regex heading pre-check (still using the pattern set below) decides
# whether a PDF is even worth sending to the AI at all, to avoid burning an
# API call on the majority of "Outcome of Board Meeting" PDFs that are
# actually governance/KMP-only notices with no results table.

# SUBJECT tag phrasings NSE/filers use for a PDF that MIGHT contain a
# results table. "Outcome of Board Meeting" is the most common, but some
# filings are tagged directly with a results-flavoured subject instead
# (e.g. "Financial Results", "Results for the Quarter", "Un-Audited
# Financial Results") — without matching those too, such a PDF would never
# even get downloaded, let alone reach the heading/AI check. Being broad
# here is safe: a false-positive match still has to clear the in-PDF
# heading check (_pdf_find_heading_candidates) before an AI call is made,
# so casting a wider net at this stage costs at most a wasted PDF fetch,
# never a wasted AI call.
_PDF_SUBJECT_PATTERNS = [
    r"outcome of board meeting",
    r"financial results?",
    r"results? for the (?:quarter|year|half.?year)",
    r"(?:un-?)?audited financial results?",
    r"integrated filing[\s\S]{0,20}financial",
]
_PDF_SUBJECT_RE = re.compile("|".join(_PDF_SUBJECT_PATTERNS), re.IGNORECASE)
# Candidate heading patterns, tried in this priority order:
#   1. "Statement of Standalone/Consolidated ... Financial Results" (most specific)
#   2. "Standalone/Consolidated ... Financial Results" without the "Statement of" prefix
#   3. Bare "(Un)Audited Financial Results for the Quarter/Year" with NO
#      Standalone/Consolidated qualifier at all — some single-entity filers
#      (no subsidiaries) omit it entirely (confirmed: MSWIL's actual table
#      heading was "UNAUDITED FINANCIAL RESULTS FOR THE QUARTER ENDED..."
#      with no qualifier word anywhere nearby). Defaults to "Standalone".
# All three commonly ALSO match inside the auditor's review-report cover
# letter ("...reviewed the accompanying Statement of Standalone unaudited
# financial results...") which precedes the real table in these PDFs —
# that boilerplate sentence is excluded via _PDF_BOILERPLATE_PRECEDE_RE
# rather than by pattern alone, since the wording is otherwise identical.
_PDF_HEADING_PATTERNS = [
    re.compile(r"Statement of (Standalone|Consolidated)[\s\S]{0,80}?Financial Results", re.IGNORECASE),
    # Order A: "...Standalone Unaudited Financial Results..." (qualifier before audited-word)
    re.compile(r"(Standalone|Consolidated)[\s\S]{0,20}?(?:Un-?)?[Aa]udited[\s\S]{0,10}?Financial Results", re.IGNORECASE),
    # Order B: "...Unaudited Standalone Financial Results..." (audited-word before qualifier —
    # NSE's actual real-world ordering, confirmed from a live filing: "STATEMENT OF UNAUDITED
    # STANDALONE FINANCIAL RESULTS..."). Order A above does NOT catch this — the audited-word
    # comes before, not after, Standalone/Consolidated, so this tier is required separately.
    re.compile(r"(?:Un-?)?[Aa]udited[\s\S]{0,10}?(Standalone|Consolidated)[\s\S]{0,20}?Financial Results", re.IGNORECASE),
    re.compile(r"(?:Un-?)?[Aa]udited Financial Results\s+for\s+the\s+(?:Quarter|Year)", re.IGNORECASE),
]
# Boilerplate lead-in phrases that precede a heading-like match inside the
# auditor's review report cover letter rather than the actual results
# table — e.g. "...reviewed the accompanying Statement of unaudited
# financial results..." or "...the accompanying Statement of Standalone...".
# Checked against the ~40 chars immediately before the match.
_PDF_BOILERPLATE_PRECEDE_RE = re.compile(r"(accompanying|reviewed)[\s\S]{0,15}$", re.IGNORECASE)
_PDF_FILENAME_TS_RE = re.compile(r"^([A-Z0-9&\-]+)_(\d{2})(\d{2})(\d{4})\d{6}_", re.IGNORECASE)


def _pdf_find_heading_candidates(text: str):
    """Returns [(start, end, nature)] for every non-boilerplate heading-like
    match across all three pattern tiers, sorted by position. `nature` is
    "Standalone" or "Consolidated" (defaulting to "Standalone" when the
    matched pattern has no qualifier group, i.e. tier 3).

    Used as a cheap pre-check before calling the AI — if this returns
    empty, the PDF is (almost certainly) a governance/KMP-only outcome
    letter with no actual results table, so we skip the AI call entirely."""
    candidates = []
    for pat in _PDF_HEADING_PATTERNS:
        for m in pat.finditer(text):
            pre = text[max(0, m.start() - 40):m.start()]
            if _PDF_BOILERPLATE_PRECEDE_RE.search(pre):
                continue
            nature = "Standalone"
            if m.groups() and m.group(1) and m.group(1).lower() in ("standalone", "consolidated"):
                nature = m.group(1).capitalize()
            candidates.append((m.start(), m.end(), nature))
    # de-dup near-identical positions across pattern tiers (same real
    # heading can match more than one tier's pattern)
    candidates.sort(key=lambda c: c[0])
    deduped = []
    for c in candidates:
        if deduped and c[0] - deduped[-1][0] < 20:
            continue
        deduped.append(c)
    return deduped


# Some "Outcome of Board Meeting" PDFs are about something OTHER than
# quarterly financial results entirely (NCD/debenture issuance, other
# fundraising, share allotment) — NSE tags these with the exact same
# generic SUBJECT as a genuine results filing, so the subject tag alone
# can't tell them apart. These phrases in the free-text part of the
# summary (before the |SUBJECT: tag) are strong signals the underlying
# PDF is NOT a results filing, so we exclude them from ever entering
# nse_results_pdf_feed.json — without this, they show up as bare
# "RESULT" cards with no financial data (their PDF genuinely has no
# results table for the heading-check/AI to find, so they'd always stay
# unparsed clutter in the Results tab).
_PDF_NON_RESULT_PATTERNS = [
    r"non.?convertible debentures?",
    r"\bNCDs?\b",
    r"issuance of .*(debentures?|securities)",
    r"allotment of (equity )?shares?",
    r"raising funds? through",
]
_PDF_NON_RESULT_RE = re.compile("|".join(_PDF_NON_RESULT_PATTERNS), re.IGNORECASE)


def _extract_filename_symbol(link: str) -> str:
    """Best-effort NSE symbol/scrip-code guess from a filing's own filename
    prefix (e.g. 'SKYWAYS_17092026...pdf' -> 'SKYWAYS') — used to cross-
    check a candidate announcement against result_calendar.json without
    needing to download and parse the PDF itself just to find out."""
    fname = link.rsplit("/", 1)[-1]
    m = _PDF_FILENAME_TS_RE.match(fname)
    if m:
        return m.group(1).upper()
    return ""


def _in_result_calendar(symbol: str, calendar: dict, item_link: str) -> bool:
    """True if `symbol` appears in result_calendar.json for the filing's
    own embedded date, or the day before/after (NSE's predicted calendar
    date and the actual filing date can be a day off). Fails OPEN — if the
    calendar is empty/unavailable, or we can't confidently extract a
    symbol or date from this item, the item is allowed through rather than
    silently hidden, since a broken filter is worse than an occasional
    false positive."""
    if not calendar or not symbol:
        return True
    fts = _filing_ts(item_link)
    if not fts:
        return True
    try:
        d = datetime.strptime(fts, "%d%m%Y%H%M%S").date()
    except ValueError:
        return True
    for delta in (-1, 0, 1):
        day = (d + timedelta(days=delta)).isoformat()
        if symbol in (calendar.get(day) or []):
            return True
    return False


def _is_board_outcome_pdf(it: dict) -> bool:
    link = it.get("link", "")
    if not link.lower().endswith(".pdf"):
        return False
    summary = it.get("summary", "")
    m = _SUBJECT_TAG_RE.search(summary)
    if not (m and _PDF_SUBJECT_RE.search(m.group(1))):
        return False
    free_text = summary.split("|SUBJECT:")[0]
    if _PDF_NON_RESULT_RE.search(free_text):
        return False
    return True


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


_AI_EXTRACT_SYSTEM_PROMPT = """You extract structured financial data from an NSE-listed Indian company's quarterly results outcome PDF. You are given the extracted plain text of the document AND, usually, the actual PDF document itself.

The extracted text can be genuinely UNRELIABLE for the numbers table specifically — for scanned or lower-quality PDFs, text extraction has been observed to corrupt digits outright (e.g. "406.95" extracted as "40695" with the decimal point silently dropped, or "436.33" garbled into an unrelated "13635"), not just misalign columns. When the PDF document itself is provided, treat it as the authoritative source for every number in the main results table — read the table directly from the PDF the way a person would, rather than trusting the extracted text's digits. Use the extracted text mainly for things that are awkward to re-derive from the PDF alone (confirming labels, locating which page has the table) and as a fallback only when no PDF is provided at all.

Your job:
1. Determine if this document contains an actual quarterly financial results TABLE (the "Statement of Standalone/Consolidated Financial Results" with line items like Revenue, Expenses, Profit, EPS). If it's only a cover letter, merger intimation, KMP change notice, AGM notice, or similar with no such table, set is_results_table to false and leave other fields null.
2. Indian results filings very often show BOTH Standalone and Consolidated tables — usually as two SEPARATE tables further apart in the document, not side by side. SEARCH THE ENTIRE TEXT for a table explicitly labeled "Consolidated" before concluding only Standalone exists — don't stop at the first table you see. If a genuine Consolidated table exists, use it throughout (every field below, don't mix bases). Only use Standalone if no Consolidated table is present at all. Record which one you used in "basis" — this field is REQUIRED, never omit it.
3. Extract values ONLY from the MAIN results table's own rows — never from a subsidiary/joint-venture footnote, a segment-wise breakdown table, or the auditor's report's boilerplate sentences, even if they mention similar words ("total income", "net profit") with numbers nearby. The main table is the one with the full standard line-item structure (Revenue, Expenses, Profit before tax, Tax expense, Profit for the period, EPS).
4. Use the CURRENT quarter column only (the most recent quarter, i.e. the first/leftmost data column — NOT a prior-year or prior-quarter comparative column) for the "current" object.
5. Report the unit the table itself states (look for "₹ in Crore", "Rs in Crores", "₹ in Million", "₹ in Lakh"/"₹ in Lakhs"/"Rs. in Lacs"/"Rs. in Lac" — "Lac"/"Lacs" is a very common alternate spelling of Lakh in Indian filings, treat it identically — or similar near the table header) — if genuinely no unit statement exists anywhere, use "Crore" as the default (NSE's most common convention).
6. Ignore any numbers inside formula references like "(3+4)" or "[3-4]" next to line-item labels — those are row-number citations, not data.
7. Also extract the prior-quarter (immediately preceding quarter, "QoQ") and same-quarter-last-year ("YoY") values for revenue, total_income, total_expenses, PAT, EPS, finance_costs and depreciation if visible as separate columns in the same main table, plus each comparison column's period-end date. Revenue, total_income and total_expenses comparative columns get missed more often than PAT/EPS — the standard NSE quarterly table always has ALL FOUR columns (current quarter, immediately-preceding quarter, same quarter last year, full year) on the SAME rows as the current-quarter figures, so for EVERY row where you found a current-quarter value, actively look at that same row's other columns for the comparative figures too rather than only checking for PAT/EPS comparatives. total_expenses specifically matters even though it isn't shown in the final display on its own — it's what the operating-profit and operating-margin comparisons are computed from downstream, so a missing prior-period total_expenses silently blanks out those comparisons even when revenue/PAT/EPS comparatives are otherwise complete.
8. Extract the quarter-end date (the date this result is FOR, e.g. "quarter ended June 30, 2026" -> "2026-06-30").
9. "pat" MUST be the figure the filing's own reported EPS is actually derived from (usually "Profit attributable to Owners/Shareholders of the Company" — NOT a larger "total" figure that also includes non-controlling/minority interest, if the filing distinguishes between the two). Cross-check: PAT divided by shares outstanding should roughly reconcile to the reported EPS.
10. finance_costs and depreciation are separate P&L line items (usually "Finance Costs" and "Depreciation and Amortisation Expense") — extract them if the table shows them; the caller computes EBITDA from these, don't compute it yourself.
11. Segment-wise revenue is usually in its own table/note (often titled "Segment Information" or "Segment Revenue") — look for it actively rather than only checking the main P&L; most listed operating companies with multiple business lines report this. Omit segment_breakup entirely if the company doesn't report segments.
12. management_commentary: 1-3 sentence summary of any outlook/commentary/guidance mentioned in the document (not the standard boilerplate disclaimers), or null if there's none.
13. key_highlights: 2-5 short, specific, numbers-first strings on the most notable things about this result (big beats/misses, one-off items, margin changes, notable segment performance) — omit if nothing stands out beyond the raw numbers already captured.
14. board_meeting_outcome: brief note on any OTHER board decisions mentioned (dividend, bonus, other corporate actions), or null if there's nothing beyond the results approval itself.

Return ONLY valid JSON (no markdown fences, no other text) matching exactly this schema:
{
  "is_results_table": true or false,
  "basis": "Standalone" or "Consolidated" or null,
  "unit": "Crore" or "Million" or "Lakh" or null,
  "period_end": "YYYY-MM-DD" or null,
  "current": {
    "revenue": number or null,
    "other_income": number or null,
    "total_income": number or null,
    "total_expenses": number or null,
    "finance_costs": number or null,
    "depreciation": number or null,
    "pbt": number or null,
    "tax_expense": number or null,
    "pat": number or null,
    "comprehensive_income": number or null,
    "eps_basic": number or null,
    "eps_diluted": number or null
  },
  "qoq_prior": {"period_end": "YYYY-MM-DD" or null, "revenue": number or null, "total_income": number or null, "pat": number or null, "eps_basic": number or null, "total_expenses": number or null, "finance_costs": number or null, "depreciation": number or null},
  "yoy_prior": {"period_end": "YYYY-MM-DD" or null, "revenue": number or null, "total_income": number or null, "pat": number or null, "eps_basic": number or null, "total_expenses": number or null, "finance_costs": number or null, "depreciation": number or null},
  "segment_breakup": [{"segment": string, "revenue": number}] or omitted,
  "management_commentary": string or null,
  "key_highlights": [string, ...] or omitted,
  "board_meeting_outcome": string or null
}

All numeric values must be in the unit you reported (do NOT convert to rupees yourself — the caller handles that). EPS values are per-share rupee amounts regardless of the table's unit — never scale EPS. Use only information present in the document. Do not invent numbers — use null or omit the key when something genuinely isn't there."""


async def _ai_extract_financials(client: httpx.AsyncClient, text: str, fname_dbg: str, pdf_bytes: bytes = None):
    """Calls Gemini to extract structured financial data from the PDF's
    extracted text AND (when provided) the raw PDF itself, sent as a
    native document part — Gemini reads PDFs directly (including
    rendering each page internally), so there's no need to pre-render
    pages to images ourselves; the raw bytes are simpler and, for a
    normal-sized results PDF, a smaller payload too. Returns the parsed
    JSON dict, or None if the API isn't configured, the call fails, or
    the response doesn't parse as valid JSON. Caller is responsible for
    unit-scaling and sanity checks.

    The PDF matters because pdfplumber's text extraction can come out
    genuinely GARBLED for scanned/image-quality filings — not just
    misaligned columns, but wrong digits entirely (confirmed directly:
    one filing's "406.95" extracted as "40695" with the decimal point
    gone, "436.33" as "13635", "Unaudited" as "Cnaudited"). No amount of
    prompt tuning fixes an AI reading from already-corrupted input text —
    giving it the actual PDF lets it read the table visually, the same
    way a person would, sidestepping that text-layer corruption entirely
    for the numbers that actually matter.

    Same _AI_EXTRACT_SYSTEM_PROMPT and same output schema as the old
    Claude-based version — _build_result_from_ai() downstream needs no
    changes. Gemini has no separate system-prompt slot in this endpoint,
    so the instructions are prepended to the single user turn instead."""
    if not GEMINI_API_KEY:
        return None
    try:
        parts = [{"text": _AI_EXTRACT_SYSTEM_PROMPT + "\n\n" + text[:60000]}]
        if pdf_bytes:
            parts.append({"inline_data": {"mime_type": "application/pdf",
                                           "data": base64.b64encode(pdf_bytes).decode()}})

        r = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{AI_PDF_MODEL}:generateContent?key={GEMINI_API_KEY}",
            json={
                # Matches the known-working browser-based RHP extractor's
                # request shape exactly (same model, same generationConfig,
                # same 60000-char text cap) — that tool reliably gets clean
                # JSON back from this model. An earlier attempt here added
                # a "thinkingConfig": {"thinkingBudget": 0} field that
                # isn't present in the working reference at all; every
                # response after adding it came back truncated a few
                # hundred characters into the JSON regardless of how high
                # maxOutputTokens was raised, so that field (not the token
                # budget) was almost certainly the actual cause — removed.
                "contents": [{"parts": parts}],
                "generationConfig": {
                    "temperature": 0.05,
                    "maxOutputTokens": 8192,
                    "responseMimeType": "application/json",
                },
            },
            timeout=90,
        )
        if r.status_code == 429:
            print(f"    · [{fname_dbg}] AI extraction skipped: Gemini quota/rate limit hit")
            return None
        r.raise_for_status()
        data = r.json()
        candidates = data.get("candidates") or []
        if not candidates or "content" not in candidates[0]:
            print(f"    · [{fname_dbg}] AI extraction: unexpected Gemini response shape")
            return None
        finish_reason = candidates[0].get("finishReason", "")
        parts = candidates[0]["content"].get("parts") or []
        raw_text = "".join(p.get("text", "") for p in parts).strip()
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw_text, flags=re.MULTILINE).strip()
        try:
            # strict=False allows literal control characters (unescaped raw
            # newlines/tabs) inside JSON string values without raising —
            # Gemini occasionally emits a raw newline inside a multi-line
            # text field (e.g. management_commentary) instead of the
            # JSON-escaped \n, which under strict (default) parsing
            # surfaces as a confusing "Unterminated string starting at..."
            # error even though the response is otherwise well-formed.
            parsed = json.loads(cleaned, strict=False)
        except json.JSONDecodeError as je:
            # Surface finishReason on a parse failure — "MAX_TOKENS" here
            # means the response was genuinely cut off mid-JSON (budget
            # exhausted, likely by internal thinking tokens), vs "STOP"
            # meaning the model finished normally but emitted malformed
            # JSON — the two need different fixes, so don't conflate them.
            print(f"    · [{fname_dbg}] AI JSON parse failed ({je}); finishReason={finish_reason or 'unknown'}, "
                  f"response length={len(raw_text)} chars")
            return None
        return parsed
    except Exception as e:
        print(f"    · [{fname_dbg}] AI extraction failed: {type(e).__name__}: {e}")
        return None


def _build_result_from_ai(ai: dict, text: str, link: str, fname_dbg: str, rss_title: str = ""):
    """Converts the AI extraction's JSON into the {meta, quarter,
    qoq_fundamentals, yoy_fundamentals} shape used throughout the pipeline —
    plus segment_breakup / management_commentary / key_highlights /
    board_meeting_outcome as additional top-level keys when the AI found
    them (not every filing has these; XBRL parsing never populates them,
    so downstream code that doesn't know about them just won't see the
    keys — no other function needs to change).
    Applies unit scaling and a total_income reconciliation sanity check.
    Returns None if the AI result fails basic validation (missing
    revenue+PAT, bad date, unmatched filename)."""
    cur = ai.get("current") or {}
    nature = ai.get("basis") or "Standalone"
    unit_word = (ai.get("unit") or "Crore").lower()
    if unit_word.startswith("million"):
        unit_multiplier = 1e6
    elif unit_word.startswith("lakh"):
        unit_multiplier = 1e5
    else:
        unit_multiplier = 1e7  # Crore, NSE's default convention

    def scale(v):
        return v * unit_multiplier if isinstance(v, (int, float)) else None

    revenue = scale(cur.get("revenue"))
    other_income = scale(cur.get("other_income"))
    total_income = scale(cur.get("total_income"))
    total_expenses = scale(cur.get("total_expenses"))
    finance_costs = scale(cur.get("finance_costs"))
    depreciation = scale(cur.get("depreciation"))
    pbt = scale(cur.get("pbt"))
    tax_expense = scale(cur.get("tax_expense"))
    pat = scale(cur.get("pat"))
    comprehensive = scale(cur.get("comprehensive_income"))
    eps_basic = cur.get("eps_basic")      # per-share rupee amount — never scaled
    eps_diluted = cur.get("eps_diluted")

    # EBITDA = PBT + Finance Costs + Depreciation (no Other Income subtraction,
    # matching the convention validated against several real filings' own stated
    # EBITDA — not company-defined "Adjusted EBITDA", which can differ). Only
    # computed when the filing's table actually broke out both line items.
    ebitda = (pbt + finance_costs + depreciation) if (pbt is not None and finance_costs is not None and depreciation is not None) else None

    if revenue is None and pat is None:
        print(f"    · [{fname_dbg}] AI returned is_results_table=true but no revenue/PAT — treating as invalid")
        return None

    # Total Income must equal Revenue + Other Income by definition. Even AI
    # extraction can occasionally pick up a stray number from a notes/
    # segment sub-table, so this stays as defense-in-depth.
    if revenue is not None and other_income is not None and total_income is not None:
        expected = revenue + other_income
        if abs(expected - total_income) > max(1e7, 0.02 * abs(expected)):
            print(f"    · [{fname_dbg}] AI total_income sanity check failed: ₹{total_income/1e7:.2f} Cr, "
                  f"but revenue+other_income = ₹{expected/1e7:.2f} Cr — using computed value")
            total_income = round(expected, 2)

    period_end = ai.get("period_end")
    if not period_end:
        print(f"    · [{fname_dbg}] AI result missing period_end — can't build dedup key")
        return None
    try:
        datetime.strptime(period_end, "%Y-%m-%d")
    except (ValueError, TypeError):
        print(f"    · [{fname_dbg}] AI returned invalid period_end format: {period_end!r}")
        return None

    fname = link.rsplit("/", 1)[-1]
    m_fn = _PDF_FILENAME_TS_RE.match(fname)
    if not m_fn:
        print(f"    · [{fname_dbg}] AI result parsed but filename doesn't match expected timestamp pattern")
        return None
    board_meeting_date = f"{m_fn.group(4)}-{m_fn.group(3)}-{m_fn.group(2)}"

    m_sym = re.search(r"NSE\s+Symbol\s*:?\s*\n?\s*([A-Z0-9&]+)", text, re.IGNORECASE)
    symbol = m_sym.group(1).upper() if m_sym else m_fn.group(1)

    m_aud = re.search(r"\((Unaudited|Audited)\)", text, re.IGNORECASE)
    audited = m_aud.group(1).capitalize() if m_aud else None

    first_line = text.strip().split("\n", 1)[0].strip()
    company_name = rss_title.strip() if rss_title and rss_title.strip() else (
        first_line if first_line and len(first_line) < 80 else symbol)

    quarter = {
        "revenue": revenue, "other_income": other_income, "total_income": total_income,
        "total_expenses": total_expenses, "finance_costs": finance_costs, "depreciation": depreciation,
        "pbt": pbt, "tax_expense": tax_expense, "pat": pat, "ebitda": ebitda,
        "comprehensive_income": comprehensive, "eps_basic": eps_basic, "eps_diluted": eps_diluted,
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
            "extraction_method": "ai",
        },
        "quarter": quarter,
    }

    # Narrative/extra fields the AI schema captures that XBRL doesn't —
    # attached only when present so callers that don't know about them yet
    # (Telegram formatting, R2 schema) are unaffected.
    if ai.get("segment_breakup"):
        # Scale each segment's revenue the same way every other rupee
        # figure above is scaled (Crore/Million/Lakh -> raw rupees) — the
        # AI reports these in the filing's stated unit just like revenue/
        # PAT/etc, so leaving them unscaled would make segment numbers
        # ~1e5-1e7x smaller than everything else downstream expects
        # (_fmt_cr divides by 1e7 assuming raw rupees).
        result["segment_breakup"] = [
            {"segment": s.get("segment"), "revenue": scale(s.get("revenue"))}
            for s in ai["segment_breakup"] if isinstance(s, dict) and s.get("segment")
        ]
    if ai.get("management_commentary"):
        result["management_commentary"] = ai["management_commentary"]
    if ai.get("key_highlights"):
        result["key_highlights"] = ai["key_highlights"]
    if ai.get("board_meeting_outcome"):
        result["board_meeting_outcome"] = ai["board_meeting_outcome"]

    qoq = ai.get("qoq_prior") or {}
    yoy = ai.get("yoy_prior") or {}
    qoq_prior = {
        "revenue": scale(qoq.get("revenue")), "total_income": scale(qoq.get("total_income")),
        "pat": scale(qoq.get("pat")), "eps_basic": qoq.get("eps_basic"),
        "total_expenses": scale(qoq.get("total_expenses")),
    }
    yoy_prior = {
        "revenue": scale(yoy.get("revenue")), "total_income": scale(yoy.get("total_income")),
        "pat": scale(yoy.get("pat")), "eps_basic": yoy.get("eps_basic"),
        "total_expenses": scale(yoy.get("total_expenses")),
    }
    qoq_header = _quarter_header(qoq.get("period_end")) if qoq.get("period_end") else None
    yoy_header = _quarter_header(yoy.get("period_end")) if yoy.get("period_end") else None
    qoq_fund = _pdf_comparison(quarter, qoq_prior, qoq_header, "qoq")
    yoy_fund = _pdf_comparison(quarter, yoy_prior, yoy_header, "yoy")
    if qoq_fund:
        result["qoq_fundamentals"] = qoq_fund
    if yoy_fund:
        result["yoy_fundamentals"] = yoy_fund

    return result


def _pdf_comparison(cur: dict, prior: dict, prior_header, suffix: str):
    """Builds a comparison dict (sales_prior/sales_{suffix}_pct, pat_...,
    eps_..., opm_...) computed directly from the PDF's own comparative
    column via the AI extraction — this is the filing's own reported
    comparative figure, which is more precise than a separate fundamentals
    database lookup. basis="reported" flags this as sourced from the
    filing itself."""
    if not prior_header or not any(v is not None for v in prior.values()):
        return None
    out = {"basis": "reported", "basis_verified": True, "prior_header": prior_header}
    field_map = {"revenue": "sales", "total_income": "total_income", "pat": "pat", "eps_basic": "eps"}
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


async def parse_financial_results_pdf(client: httpx.AsyncClient, content: bytes, link: str, rss_title: str = ""):
    """Best-effort parse of an 'Outcome of Board Meeting' PDF into the same
    {meta, quarter} shape parse_financial_results_xbrl() produces, so it can
    flow through the same grouping/dedup/Telegram code.

    AI-ONLY extraction (no regex fallback):
      1. Extract text via pdfplumber.
      2. Cheap regex heading pre-check (_pdf_find_heading_candidates) — if
         no non-boilerplate 'Financial Results' heading is found at all,
         this is (almost certainly) a governance/KMP-only outcome letter
         with no results table, so we skip the AI call entirely rather than
         spending an API call on it.
      3. Only if a heading candidate exists do we call Gemini
         (_ai_extract_financials) to actually extract the numbers. If the
         AI call is unavailable (no GEMINI_API_KEY), fails, says
         is_results_table=false, or its result fails validation, we
         return None — there is no regex-based numeric fallback anymore.

    rss_title is the company name straight from the NSE RSS feed's own
    <title> element (e.g. "Uno Minda Limited") — used as company_name
    instead of guessing from the PDF's first line, which was confirmed
    unreliable (grabbed dates, website URLs, reference numbers, or
    name+address as if they were the company name).
    """
    import pdfplumber
    import io as _io

    fname_dbg = link.rsplit("/", 1)[-1]

    try:
        with pdfplumber.open(_io.BytesIO(content)) as pdf:
            text = "\n".join((p.extract_text(layout=True) or "") for p in pdf.pages)
    except Exception as e:
        print(f"    · [{fname_dbg}] pdfplumber open/extract_text raised: {type(e).__name__}: {e}")
        return None
    if not text.strip():
        print(f"    · [{fname_dbg}] extracted text is empty (likely a scanned/image-only PDF)")
        return None

    # ── Cheap pre-check BEFORE spending an AI call ──
    # Most "Outcome of Board Meeting" PDFs are governance/KMP-only (no
    # results table) — no point burning a Gemini call on those.
    if not _pdf_find_heading_candidates(text):
        print(f"    · [{fname_dbg}] no 'Financial Results' heading found — not a results PDF, skipping AI call")
        return None

    # ── AI extraction (sole extraction path — no regex fallback) ──
    # Send the FULL extracted PDF text (not a truncated head-of-document
    # slice) — Gemini flash's context window comfortably fits an entire
    # results PDF, and truncating to a fixed prefix was clipping the actual
    # table on filings with a long cover letter/auditor's report ahead of
    # it. _ai_extract_financials still applies its own generous safety cap
    # for the rare pathologically long document. The raw PDF (`content`,
    # already in memory from the fetch) is sent alongside the text and
    # takes priority for the actual numbers — Gemini reads PDFs natively,
    # so there's no need to pre-render pages to images ourselves.
    ai = await _ai_extract_financials(client, text, fname_dbg, content)
    if not ai:
        if not GEMINI_API_KEY:
            print(f"    · [{fname_dbg}] skipping — GEMINI_API_KEY not set")
        else:
            print(f"    · [{fname_dbg}] AI extraction failed or returned unparseable data — skipping")
        return None
    if not ai.get("is_results_table"):
        print(f"    · [{fname_dbg}] AI says this isn't a results table — skipping")
        return None

    result = _build_result_from_ai(ai, text, link, fname_dbg, rss_title)
    if not result:
        print(f"    · [{fname_dbg}] AI result failed validation (missing revenue/PAT, bad date, or filename mismatch) — skipping")
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


def _shorten_quarter_header(h):
    """'Mar 2026' -> \"Mar'26\" — compact month+2-digit-year label so the
    monospace comparison table's column headers stay narrow, matching the
    abbreviated style financial-data aggregator sites commonly use."""
    if not h:
        return None
    parts = h.split()
    if len(parts) != 2 or len(parts[1]) < 2:
        return h
    mon, yr = parts
    return f"{mon}'{yr[-2:]}"


def _fmt_table_num(val, decimals=1):
    """Compact number for a monospace table cell — no ₹ symbol (keeps
    columns narrow enough to line up on a phone screen), '-' for missing."""
    if val is None:
        return "-"
    try:
        return f"{val:,.{decimals}f}"
    except (TypeError, ValueError):
        return "-"


def _fmt_table_pct(val, decimals=1):
    if val is None:
        return "-"
    try:
        return f"{'+' if val >= 0 else ''}{val:.{decimals}f}%"
    except (TypeError, ValueError):
        return "-"


def _fmt_table_pp(val, decimals=1):
    """Percentage-POINT delta for the OPM row — a margin is already a
    percentage, so its change should read as '+1.8pp', not a relative %
    change of the percentage itself."""
    if val is None:
        return "-"
    try:
        return f"{'+' if val >= 0 else ''}{val:.{decimals}f}pp"
    except (TypeError, ValueError):
        return "-"


def _telegram_fin_table(parsed: dict) -> list:
    """Builds a compact multi-column comparison table — Metric rows
    (Sales/PAT/EPS/OPM%) x QoQ/YoY/Current/QoQ-prior/YoY-prior columns —
    as a Telegram <pre> monospace block. Mirrors the row-x-column layout
    financial-data aggregator apps (e.g. Earnings Pulse) use; Telegram
    messages can't render a real HTML table, so this is plain fixed-width
    text instead. Returns [] if there's no prior-period data to compare
    against at all (nothing to build a table from)."""
    q = parsed.get("quarter", {})
    revenue = q.get("revenue")
    total_income = q.get("total_income")
    cur_rev = total_income if total_income is not None else revenue
    cur_pat = q.get("pat")
    cur_eps = q.get("eps_basic")
    cur_opm = round(q["opm"] * 100, 1) if q.get("opm") is not None else None
    cur_header = _shorten_quarter_header(_quarter_header(q.get("period_end"))) or "Cur"

    qf = parsed.get("qoq_fundamentals") or {}
    yoy_native = parsed.get("yoy_comparison")
    yf = parsed.get("yoy_fundamentals") or {}

    def _pick(ti_key, sales_key, source):
        ti = source.get(ti_key)
        return ti if ti is not None else source.get(sales_key)

    qoq_rev = _pick("total_income_prior", "sales_prior", qf)
    qoq_rev_pct = qf.get("total_income_qoq_pct")
    if qoq_rev_pct is None:
        qoq_rev_pct = qf.get("sales_qoq_pct")
    qoq_pat = qf.get("pat_prior")
    qoq_pat_pct = qf.get("pat_qoq_pct")
    qoq_eps = qf.get("eps_prior")
    qoq_opm = qf.get("opm_prior")
    qoq_opm_pp = qf.get("opm_qoq_pp")
    qoq_header = _shorten_quarter_header(qf.get("prior_header"))

    if yoy_native:
        # Native XBRL-tagged prior-year context — doesn't carry a
        # comparative EPS figure, unlike the AI/fundamentals paths.
        yoy_rev = yoy_native.get("total_income")
        if yoy_rev is None:
            yoy_rev = yoy_native.get("revenue")
        yoy_pat = yoy_native.get("pat")
        yoy_opm = round(yoy_native["opm"] * 100, 1) if yoy_native.get("opm") is not None else None
        yoy_opm_pp = round((cur_opm - yoy_opm), 1) if (cur_opm is not None and yoy_opm is not None) else None
        yoy_eps = None
        yoy_rev_pct = yoy_pat_pct = None  # computed generically below from the raw values
        yoy_header = _shorten_quarter_header(_quarter_header(yoy_native.get("period_end")))
    else:
        yoy_rev = _pick("total_income_prior", "sales_prior", yf)
        yoy_rev_pct = yf.get("total_income_yoy_pct")
        if yoy_rev_pct is None:
            yoy_rev_pct = yf.get("sales_yoy_pct")
        yoy_pat = yf.get("pat_prior")
        yoy_pat_pct = yf.get("pat_yoy_pct")
        yoy_eps = yf.get("eps_prior")
        yoy_opm = yf.get("opm_prior")
        yoy_opm_pp = yf.get("opm_yoy_pp")
        yoy_header = _shorten_quarter_header(yf.get("prior_header"))

    if not qf and not yf and not yoy_native:
        return []  # nothing to compare against — a table would be all dashes

    # Sales/PAT are raw rupees on the `quarter`/fundamentals dicts — scale
    # to ₹Cr (÷1e7) for the table, same convention _fmt_cr uses everywhere
    # else. EPS/OPM are already in their natural display units.
    def _cr(v):
        return v / 1e7 if v is not None else None

    rows = [
        ("Sales", _cr(cur_rev), _cr(qoq_rev), qoq_rev_pct, _cr(yoy_rev), yoy_rev_pct, 1, "pct"),
        ("PAT",   _cr(cur_pat), _cr(qoq_pat), qoq_pat_pct, _cr(yoy_pat), yoy_pat_pct, 1, "pct"),
        ("EPS",   cur_eps,      qoq_eps,      None,        yoy_eps,      None,        2, "pct"),
        ("OPM%",  cur_opm,      qoq_opm,      qoq_opm_pp,  yoy_opm,      yoy_opm_pp,  1, "pp"),
    ]

    hdr_cur = cur_header or "Cur"
    hdr_qoq = qoq_header or "-"
    hdr_yoy = yoy_header or "-"
    header = f"{'Metric':<7}{'QoQ':>8}{'YoY':>8}{hdr_cur:>9}{hdr_qoq:>9}{hdr_yoy:>9}"
    lines = [f"<pre>{header}"]
    for label, cur, qprior, qdelta, yprior, ydelta, dec, delta_kind in rows:
        # Sales/PAT/OPM already have a precomputed delta; EPS doesn't carry
        # one upstream, so derive a plain % change here from the prior value.
        if delta_kind == "pct":
            if qdelta is None and cur is not None and qprior is not None and qprior != 0:
                qdelta = (cur - qprior) / abs(qprior) * 100
            if ydelta is None and cur is not None and yprior is not None and yprior != 0:
                ydelta = (cur - yprior) / abs(yprior) * 100
            fmt_delta = _fmt_table_pct
        else:
            fmt_delta = _fmt_table_pp
        row = (f"{label:<7}{fmt_delta(qdelta):>8}{fmt_delta(ydelta):>8}"
               f"{_fmt_table_num(cur, dec):>9}{_fmt_table_num(qprior, dec):>9}{_fmt_table_num(yprior, dec):>9}")
        lines.append(row)
    lines.append("</pre>")
    lines.append("<i>Sales/PAT in ₹Cr</i>")
    return lines


def _telegram_basis_block(parsed: dict) -> list:
    """Builds the financial comparison block for ONE basis (Standalone or
    Consolidated). No header/company-name lines — those are built once by
    the caller so two bases for the same company share a single message."""
    q = parsed.get("quarter", {})
    revenue = q.get("revenue")
    total_income = q.get("total_income")
    rev_display = total_income if total_income is not None else revenue
    pat = q.get("pat")
    pat_emoji = "🟢" if (pat is not None and pat >= 0) else ("🔴" if pat is not None else "")
    cur_header = _quarter_header(q.get("period_end")) or ""

    table = _telegram_fin_table(parsed)
    if table:
        lines = list(table)
    else:
        # No prior-period data at all (e.g. a company's first-ever result,
        # or fundamentals lookup failed) — fall back to a plain current-
        # quarter summary rather than sending an empty/dash-only table.
        lines = [f"<b>Current Qtr{' (' + cur_header + ')' if cur_header else ''}</b>"]
        lines.append(f"Rev: <b>{_fmt_cr(rev_display)}</b>")
        lines.append(f"PAT: {pat_emoji} <b>{_fmt_cr(pat)}</b>")
        if q.get("eps_basic") is not None:
            lines.append(f"EPS: <b>₹{q['eps_basic']}</b>")

    if q.get("yoy_caution"):
        lines.append("")
        lines.append("⚠️ Company notes: results may not be YoY comparable")

    # ── AI-extracted narrative fields (segment breakup, commentary,
    # highlights, other board decisions) — these come only from the AI
    # PDF-extraction path (XBRL parsing never populates them), so most
    # existing/XBRL-sourced records simply won't have these keys and these
    # blocks are silently skipped for them.
    segs = parsed.get("segment_breakup")
    if segs:
        lines.append("")
        lines.append("<b>📦 Segment Revenue</b>")
        for s in segs:
            seg_name = s.get("segment")
            seg_rev = s.get("revenue")
            if seg_name and seg_rev is not None:
                lines.append(f"{seg_name}: {_fmt_cr(seg_rev)}")

    highlights = parsed.get("key_highlights")
    if highlights:
        lines.append("")
        lines.append("<b>✨ Key Highlights</b>")
        for h in highlights:
            lines.append(f"• {h}")

    commentary = parsed.get("management_commentary")
    if commentary:
        lines.append("")
        lines.append("<b>🗣️ Management Commentary</b>")
        lines.append(commentary)

    board_outcome = parsed.get("board_meeting_outcome")
    if board_outcome:
        lines.append("")
        lines.append("<b>🏛️ Other Board Decisions</b>")
        lines.append(board_outcome)

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

    msg = "\n".join(lines)
    # Telegram's hard cap is 4096 chars per message. The narrative fields
    # (segment breakup, highlights, commentary, board outcome — especially
    # doubled up across Standalone + Consolidated in one message) can push
    # past that on a verbose filing. Truncate defensively rather than let
    # the send fail outright; the full data is still in
    # nse_results_detailed.json regardless of what fits in the alert.
    TELEGRAM_MAX_CHARS = 4000
    if len(msg) > TELEGRAM_MAX_CHARS:
        msg = msg[:TELEGRAM_MAX_CHARS].rsplit("\n", 1)[0] + "\n\n…(truncated, see full data on the site)"
    return msg


def _merge_xbrl_into_pdf_record(existing: dict, xbrl_parsed: dict) -> dict:
    """When XBRL data arrives for a result the PDF fast-path already
    covered, update just the NUMERIC fields with XBRL's officially-tagged
    figures (more authoritative than an AI read of the PDF), while
    preserving every narrative field the PDF/AI extraction found
    (key_highlights, management_commentary, segment_breakup,
    board_meeting_outcome) — XBRL parsing never produces those at all, so
    a blanket overwrite (the earlier design) would silently delete them.
    No Telegram notification follows this: the person was already
    notified when the PDF-based result first came in — this just quietly
    corrects/confirms the numbers in place."""
    merged = dict(existing)
    merged_quarter = dict(existing.get("quarter") or {})
    xbrl_quarter = xbrl_parsed.get("quarter") or {}

    NUMERIC_FIELDS = ("revenue", "other_income", "total_income", "total_expenses",
                       "pbt", "tax_expense", "pat", "comprehensive_income",
                       "eps_basic", "eps_diluted")
    changed_fields = []
    for f in NUMERIC_FIELDS:
        xv = xbrl_quarter.get(f)
        if xv is not None and xv != merged_quarter.get(f):
            changed_fields.append(f)
            merged_quarter[f] = xv

    if changed_fields:
        _compute_opm(merged_quarter)  # keep opm consistent with any updated revenue/expenses
    merged["quarter"] = merged_quarter

    # If XBRL tagged its own prior-year context, refresh yoy_fundamentals'
    # absolute figures from it (more authoritative than the AI's read of
    # the PDF's own comparative column) — never touches qoq_fundamentals
    # (XBRL essentially never tags QoQ) or any narrative field.
    yoy_native = xbrl_parsed.get("yoy_comparison")
    if yoy_native:
        yf = dict(existing.get("yoy_fundamentals") or {})
        cur = merged_quarter
        prior_ti = yoy_native.get("total_income")
        if prior_ti is not None:
            yf["total_income_prior"] = prior_ti
            if cur.get("total_income") is not None and prior_ti != 0:
                yf["total_income_yoy_pct"] = round((cur["total_income"] - prior_ti) / abs(prior_ti) * 100, 2)
        prior_pat = yoy_native.get("pat")
        if prior_pat is not None:
            yf["pat_prior"] = prior_pat
            if cur.get("pat") is not None and prior_pat != 0:
                yf["pat_yoy_pct"] = round((cur["pat"] - prior_pat) / abs(prior_pat) * 100, 2)
        prior_eps = yoy_native.get("eps_basic")
        if prior_eps is not None:
            yf["eps_prior"] = prior_eps
            if cur.get("eps_basic") is not None and prior_eps != 0:
                yf["eps_yoy_pct"] = round((cur["eps_basic"] - prior_eps) / abs(prior_eps) * 100, 2)
        if yoy_native.get("opm") is not None and cur.get("opm") is not None:
            yf["opm_prior"] = round(yoy_native["opm"] * 100, 2)
            yf["opm_yoy_pp"] = round((cur["opm"] - yoy_native["opm"]) * 100, 2)
        yf["basis"] = "xbrl_tagged"
        yf["basis_verified"] = True
        merged["yoy_fundamentals"] = yf

    if changed_fields:
        sym = (existing.get("meta", {}) or {}).get("symbol")
        print(f"    · XBRL confirmed/updated {len(changed_fields)} field(s) for {sym}: "
              f"{', '.join(changed_fields)} (narrative preserved, no Telegram resend)")
    return merged


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


async def _update_results_by_symbol(client: httpx.AsyncClient, parsed_all: list, quarters_to_keep: int = 8):
    """Maintains a per-symbol store of each company's most recent quarters
    of parsed results (Standalone and Consolidated tracked separately),
    independent of nse_results_detailed.json's global 1000-item rolling
    cap. That cap is shared across EVERY company combined — during a busy
    results season, a company's own 2nd/3rd/4th-most-recent quarter can
    get evicted by the sheer volume of OTHER companies filing, well before
    4 quarters have actually passed for that company. This file keeps at
    least `quarters_to_keep` quarters per symbol+nature no matter how much
    unrelated filing volume happens elsewhere, so a stock's own quarterly
    history/AI-summary stays reliably available (e.g. for a per-stock
    "past 4 quarters" view on the frontend)."""
    if not parsed_all:
        return
    existing = await r2_get(client, "nse_results_by_symbol.json")
    store = (existing or {}).get("symbols", {})

    touched = set()
    for r in parsed_all:
        meta = r.get("meta", {}) or {}
        symbol = meta.get("symbol")
        nature = meta.get("standalone_consolidated") or "Standalone"
        period_end = (r.get("quarter") or {}).get("period_end")
        if not symbol or not period_end:
            continue
        touched.add(symbol)
        sym_entry = store.setdefault(symbol, {})
        nature_list = sym_entry.setdefault(nature, [])
        # Replace any existing entry for the same quarter (a refiled/
        # updated result) rather than duplicating it, then keep only the
        # most recent `quarters_to_keep` by period_end.
        nature_list[:] = [q for q in nature_list if (q.get("quarter") or {}).get("period_end") != period_end]
        nature_list.append(r)
        nature_list.sort(key=lambda q: (q.get("quarter") or {}).get("period_end") or "", reverse=True)
        sym_entry[nature] = nature_list[:quarters_to_keep]

    if touched:
        payload = {"updated_at": datetime.now(timezone.utc).isoformat(), "symbols": store}
        await r2_put(client, "nse_results_by_symbol.json", payload)
        print(f"  ✓ nse_results_by_symbol.json: updated {len(touched)} symbol(s), "
              f"keeping up to {quarters_to_keep} quarters each")


async def build_results_detailed(client: httpx.AsyncClient, results_items: list[dict], board_items: list[dict], fundamentals: dict | None) -> dict | None:
    """
    Builds/updates nse_results_detailed.json from two sources:
      - XBRL filings (results_items, nse_results_feed.json) — authoritative,
        full-detail, but often published well after the board meeting.
      - "Outcome of Board Meeting" PDFs (board_items, nse_board_meetings.json)
        — a fast-path: usually available immediately, core numbers only,
        AI-extracted (see parse_financial_results_pdf).
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

    def _basis_key(it):
        """(symbol, period_end) — ignores standalone/consolidated nature,
        used to find the Standalone/Consolidated counterpart of a result."""
        meta = it.get("meta", {}) or {}
        quarter = it.get("quarter", {}) or {}
        return (meta.get("symbol"), quarter.get("period_end"))

    # Consolidated preferred over Standalone: retroactive cleanup. NSE
    # often files Standalone and Consolidated as two SEPARATE PDF documents
    # (different filenames/links, sometimes even different runs) rather
    # than two tables in one PDF, so they can end up stored as two
    # independent records for the same symbol+quarter. Consolidated is
    # what's wanted; Standalone should only ever persist as a fallback when
    # no Consolidated result exists at all for that company+quarter — drop
    # any Standalone record that already has a Consolidated counterpart
    # sitting in the existing data, regardless of whether anything new is
    # being parsed this run.
    _existing_consolidated_keys = {
        _basis_key(it) for it in existing_items
        if (it.get("meta", {}).get("standalone_consolidated") or "").strip().lower() == "consolidated"
        and _basis_key(it)[0]
    }
    if _existing_consolidated_keys:
        _before = len(existing_items)
        existing_items = [
            it for it in existing_items
            if not ((it.get("meta", {}).get("standalone_consolidated") or "").strip().lower() == "standalone"
                    and _basis_key(it) in _existing_consolidated_keys)
        ]
        _removed = _before - len(existing_items)
        if _removed:
            print(f"  🗑 Removed {_removed} previously-stored Standalone record(s) already superseded "
                  f"by an existing Consolidated result (Consolidated preferred)")

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
    # (module-level DISABLE_XBRL_FOR_TESTING — see top of file)
    if DISABLE_XBRL_FOR_TESTING:
        print("  ⚠ XBRL processing disabled for testing — PDF-only this run")
        xbrl_items = []

    # Existing PDF-sourced records are treated as reprocess-eligible (even
    # though their link is already present) when they show signs of being
    # stale/wrong rather than genuinely complete — this lets extraction
    # improvements (new label regex, the AI extractor, unit-scaling fixes)
    # go back and correct records already sitting on R2 with bad data.
    # Confirmed real cases that motivated each check below:
    #   - GODREJPROP/DDEL/MSWIL: pat/pbt were null (missing entirely)
    #   - GODREJPROP/DDEL/UNOMINDA: pat/pbt were non-null but UNSCALED
    #     (e.g. revenue=506.17 instead of ~5,061,700,000) — a null-only
    #     check would never catch these, since they "look" complete
    #   - UNOMINDA: eps_basic=511.0 (should be ~5.11) — a decimal/scale bug
    #     unrelated to the Crore-vs-rupee issue but equally implausible
    # Only applies to source=="pdf" — XBRL-sourced records are authoritative.
    def _looks_stale_or_wrong(it):
        meta = it.get("meta", {}) or {}
        if meta.get("source") != "pdf":
            return False
        q = it.get("quarter", {}) or {}
        pat, pbt = q.get("pat"), q.get("pbt")
        if pat is None and pbt is None:
            return True
        revenue = q.get("revenue")
        if revenue is not None and 0 < abs(revenue) < 1e6:
            return True  # implausibly small for raw rupees — almost certainly unscaled Crore/Million data
        for eps_field in ("eps_basic", "eps_diluted"):
            eps_v = q.get(eps_field)
            if eps_v is not None and abs(eps_v) > 1000:
                return True  # no real NSE-listed company's quarterly EPS is in the thousands
        if "extraction_method" not in meta:
            return True  # pre-dates this tracking — provenance/quality unknown, worth a fresh attempt
        # QoQ-prior and YoY-prior total_income/revenue being EXACTLY equal is
        # a strong signal of a column-misalignment extraction bug — the AI
        # duplicated one comparison column's value into both slots instead of
        # reading Mar-quarter and same-quarter-last-year separately (seen
        # directly: Skyways' qoq/yoy total_income_prior both landed on the
        # same figure). A real company's QoQ and YoY prior periods are
        # different quarters and essentially never report the identical
        # revenue figure to the rupee, so treat an exact match as implausible
        # rather than coincidental.
        qf = it.get("qoq_fundamentals") or {}
        yf = it.get("yoy_fundamentals") or {}
        for field in ("total_income_prior", "sales_prior"):
            qv, yv = qf.get(field), yf.get(field)
            if qv is not None and yv is not None and qv == yv:
                return True
        return False

    incomplete_pdf_links = {it.get("link") for it in existing_items if _looks_stale_or_wrong(it)}
    if incomplete_pdf_links:
        print(f"  ↻ {len(incomplete_pdf_links)} existing PDF-sourced record(s) look stale/incomplete/implausible — "
              f"will retry parsing them")

    new_xbrl = [it for it in xbrl_items if it["link"] not in existing_links]
    new_pdf = [it for it in pdf_items if it["link"] not in existing_links or it["link"] in incomplete_pdf_links]

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
                # Require an actual "quarter" bucket — our whole system
                # (dedup, QoQ/YoY comparisons, the Results tab's card
                # layout) is built around quarterly figures. A filing that
                # only has "year" data (some non-Ind-AS taxonomies report
                # annually with no quarter context) can't be meaningfully
                # displayed or compared, and previously slipped through as
                # a symbol-less, quarter-less junk record (seen directly:
                # Synoptics Technologies' NONINDAS filing).
                if not parsed.get("quarter"):
                    return None  # no quarterly data — not useful for this feed, skip silently
                if not parsed.get("meta", {}).get("symbol"):
                    return None  # can't be identified/deduped without a symbol — skip
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
                parsed = await parse_financial_results_pdf(client, content, it["link"], it.get("title", ""))
                if not parsed:
                    print(f"  ⚠ PDF parse returned None for {fname} "
                          f"(no results heading / AI unavailable / AI said not a results table / "
                          f"AI result failed validation — see parse_financial_results_pdf)")
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
    xbrl_merged_records = []
    for r in parsed_all:
        key = _result_key(r)
        r_is_xbrl = (r.get("meta", {}) or {}).get("source") != "pdf"  # XBRL path never sets meta.source
        matched = key[0] and key in existing_by_key
        if matched:
            idx = existing_by_key[key]
            existing_rec = existing_items[idx]
            existing_is_pdf = (existing_rec.get("meta", {}) or {}).get("source") == "pdf"
            if r_is_xbrl and existing_is_pdf:
                # XBRL "catching up" to a result the PDF fast-path already
                # covered — selectively merge just the numeric fields in
                # place (see _merge_xbrl_into_pdf_record) rather than a
                # blanket overwrite, so the AI-extracted narrative content
                # survives. No Telegram resend — the person was already
                # notified when the PDF result first came in.
                merged_rec = _merge_xbrl_into_pdf_record(existing_rec, r)
                existing_items[idx] = merged_rec
                xbrl_merged_records.append(merged_rec)
                continue
            refiled.append(r)
        else:
            parsed_new.append(r)
    if xbrl_merged_records:
        print(f"  🔗 {len(xbrl_merged_records)} XBRL result(s) merged into existing PDF record(s) — numbers refreshed, narrative kept, no Telegram resend")
    if refiled:
        print(f"  ↻ {len(refiled)} re-filed (already notified earlier) — updating record, skipping Telegram: "
              f"{', '.join((r.get('meta', {}) or {}).get('symbol') or '?' for r in refiled)}")
        for r in refiled:
            existing_items[existing_by_key[_result_key(r)]] = r

    # Consolidated preferred over Standalone: forward-looking filter.
    # Covers both (a) Consolidated already sitting in existing_items while
    # a new Standalone filing arrives this run, and (b) Standalone and
    # Consolidated both arriving fresh in the SAME run's batch (the common
    # case — NSE frequently files both PDFs for the same board meeting
    # within minutes of each other). Either way, drop the Standalone
    # record before it can be stored or sent to Telegram — Standalone only
    # ever survives as a fallback when no Consolidated result exists at
    # all for that symbol+quarter.
    #
    # IMPORTANT: this must run BEFORE the XBRL recency-guard block below,
    # not after. It used to run after, which meant parsed_new_for_telegram
    # got built from the pre-filter parsed_new — so a Standalone entry
    # correctly got dropped from parsed_new/storage here, but the ALREADY-
    # built parsed_new_for_telegram snapshot still had it, and it kept
    # getting sent to Telegram on every single run (NSE re-files the same
    # Standalone XBRL fresh each day, so it never matched an existing key
    # and never aged out of the 3-day recency guard either) even though it
    # was being correctly dropped from storage. Confirmed directly via
    # diagnostic logging: SKYWAYS/ESSARSHPNG/INDLMETER/TEMPSENS all showed
    # computed_key=(...,'Standalone') with only a 'Consolidated' key on
    # file — genuinely new-by-key every run, correctly dropped by this
    # filter, yet still Telegram-sent because of the stale snapshot.
    consolidated_available = {
        _basis_key(it) for it in existing_items + parsed_new
        if (it.get("meta", {}).get("standalone_consolidated") or "").strip().lower() == "consolidated"
        and _basis_key(it)[0]
    }
    if consolidated_available:
        before_new = len(parsed_new)
        dropped_syms = [
            (r.get("meta", {}) or {}).get("symbol")
            for r in parsed_new
            if (r.get("meta", {}).get("standalone_consolidated") or "").strip().lower() == "standalone"
            and _basis_key(r) in consolidated_available
        ]
        parsed_new = [
            r for r in parsed_new
            if not ((r.get("meta", {}).get("standalone_consolidated") or "").strip().lower() == "standalone"
                    and _basis_key(r) in consolidated_available)
        ]
        dropped_new_standalone = before_new - len(parsed_new)
        if dropped_new_standalone:
            print(f"  ⏸ Dropped {dropped_new_standalone} new Standalone result(s) — Consolidated "
                  f"already available/arriving for the same symbol+quarter (Consolidated preferred): "
                  f"{', '.join(dropped_syms)}")

        before_existing = len(existing_items)
        removed_syms = [
            (it.get("meta", {}) or {}).get("symbol")
            for it in existing_items
            if (it.get("meta", {}).get("standalone_consolidated") or "").strip().lower() == "standalone"
            and _basis_key(it) in consolidated_available
        ]
        existing_items = [
            it for it in existing_items
            if not ((it.get("meta", {}).get("standalone_consolidated") or "").strip().lower() == "standalone"
                    and _basis_key(it) in consolidated_available)
        ]
        dropped_existing_standalone = before_existing - len(existing_items)
        if dropped_existing_standalone:
            print(f"  🗑 Removed {dropped_existing_standalone} previously-stored Standalone record(s) "
                  f"now superseded by a Consolidated result arriving this run: {', '.join(removed_syms)}")

    # XBRL-sourced NEW results (no PDF record existed at all for this
    # symbol+quarter — the sole scenario XBRL is meant to fill in) still
    # get a recency guard before Telegram: re-enabling XBRL after it was
    # off for a while means whatever backlog of old, never-covered XBRL
    # filings accumulated in the meantime would otherwise all fire at
    # once, flooding the channel with results that are old news by now.
    # Genuinely fresh XBRL-only results (published in roughly the last
    # couple of days) still notify normally. Runs on the ALREADY
    # Consolidated-preference-filtered parsed_new (see above) so a
    # dropped Standalone duplicate can never sneak into this snapshot.
    XBRL_TELEGRAM_MAX_AGE_SECONDS = 3 * 24 * 60 * 60  # 3 days
    now_ts = datetime.now(timezone.utc).timestamp()
    xbrl_backlog_silenced = []
    still_notify = []
    for r in parsed_new:
        is_xbrl = (r.get("meta", {}) or {}).get("source") != "pdf"
        if is_xbrl and (now_ts - _effective_ts(r)) > XBRL_TELEGRAM_MAX_AGE_SECONDS:
            xbrl_backlog_silenced.append((r.get("meta", {}) or {}).get("symbol") or "?")
            continue
        still_notify.append(r)
    if xbrl_backlog_silenced:
        print(f"  🔇 {len(xbrl_backlog_silenced)} XBRL-only result(s) older than 3 days — storing data, "
              f"skipping Telegram (backlog, not fresh news): {', '.join(xbrl_backlog_silenced)}")
    parsed_new_for_telegram = still_notify

    if parsed_new_for_telegram:
        groups = _group_parsed_results(parsed_new_for_telegram)
        print(f"  Sending {len(groups)} Telegram message(s) ({len(parsed_new_for_telegram)} filings grouped)...")
        telegram_syms = [(g[0].get("meta", {}) or {}).get("symbol") for g in groups if g]
        print(f"    symbols: {', '.join(s for s in telegram_syms if s)}")
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

    # Guarantee every item actually parsed/notified THIS run survives the
    # cap, regardless of its timestamp quality — a bad or zero
    # published_ts (a known issue, especially for XBRL items) could
    # otherwise sort a freshly-added record to the bottom and truncate it
    # out before it's ever persisted, making it look "new" again next run
    # and re-notifying Telegram forever. Only the OLDER, already-persisted
    # portion gets trimmed to make room, never this run's new items.
    new_links = {it.get("link") for it in parsed_new}
    older_existing = [it for it in existing_items if it.get("link") not in new_links]
    older_existing.sort(key=_effective_ts, reverse=True)
    keep_older = max(0, 1000 - len(parsed_new))
    merged = parsed_new + older_existing[:keep_older]
    merged.sort(key=_effective_ts, reverse=True)

    # Include the freshly XBRL-merged records too, so the per-symbol
    # store's copy of this quarter also gets the corrected numbers rather
    # than staying stale.
    await _update_results_by_symbol(client, refiled + parsed_new + xbrl_merged_records)

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

            # While XBRL processing is disabled, nse_results_feed.json's
            # accumulated XBRL announcements can never get an AI/XBRL-
            # parsed detail record (build_results_detailed skips XBRL
            # entirely), and the frontend has separately been told to hide
            # this feed from the Results tab too — nothing reads or
            # benefits from it right now. Skip fetching/accumulating/
            # uploading it entirely rather than doing that work for a file
            # nothing consumes. Existing R2 data is left untouched (not
            # deleted) so re-enabling XBRL later picks up right where it
            # left off.
            if filename == "nse_results_feed.json" and DISABLE_XBRL_FOR_TESTING:
                print(f"  ⏭ {filename}: skipping fetch/accumulate — XBRL processing disabled, nothing consumes this file right now")
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

        # Cross-check against result_calendar.json — NSE's own schedule of
        # which symbols are actually due to declare results on which date.
        # Many "Outcome of Board Meeting" PDFs are about something other
        # than quarterly results (NCD issuance, KMP changes, etc.) with a
        # generic boilerplate summary giving no textual clue either way
        # (confirmed directly: Anupam Rasayan/Shalibhadra/Shivalic Power/
        # Pelatro all showed up as bare "RESULT" cards with no financial
        # data — none of their symbols were actually on the calendar for
        # that date). This filter is ground-truth rather than a text
        # heuristic, so it catches cases the earlier NCD/debenture wording
        # filter can't. Applied to the FULL candidate set (new + already-
        # stored) so previously-admitted non-results self-heal out on each
        # run, not just prevented going forward.
        calendar_payload = await r2_get(client, "result_calendar.json")
        if not calendar_payload:
            print("  ⚠ result_calendar.json unavailable — skipping calendar cross-check this run")
        existing_pdf_feed = await r2_get(client, "nse_results_pdf_feed.json")
        existing_pdf_items = (existing_pdf_feed or {}).get("items", [])
        all_pdf_candidates = dedup_items(pdf_candidates_now + existing_pdf_items)

        before_cal = len(all_pdf_candidates)
        if calendar_payload:
            all_pdf_candidates = [
                it for it in all_pdf_candidates
                if _in_result_calendar(_extract_filename_symbol(it.get("link", "")), calendar_payload, it.get("link", ""))
            ]
            dropped_cal = before_cal - len(all_pdf_candidates)
            if dropped_cal:
                print(f"  🗑 {dropped_cal} announcement(s) dropped — symbol not on result_calendar.json for that date "
                      f"(likely not an actual results filing despite the 'Outcome of Board Meeting' subject)")

        merged_pdf_feed = all_pdf_candidates
        merged_pdf_feed.sort(key=_effective_ts, reverse=True)
        merged_pdf_feed = merged_pdf_feed[:500]
        print(f"  nse_results_pdf_feed.json: {len(existing_pdf_items)} existing + "
              f"{max(len(merged_pdf_feed) - len(existing_pdf_items), 0)} new = {len(merged_pdf_feed)} (capped at 500)")
        await r2_put(client, "nse_results_pdf_feed.json", make_payload(merged_pdf_feed))

        # ── Financial results detail (P&L from XBRL / AI-extracted PDF) ──
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
