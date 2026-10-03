"""
nse_catalysts.py
Pulls NSE corporate announcements, keeps a trader-focused set of material
events, drops routine exchange/compliance noise at the backend, and stores them
per symbol so the frontend can show WHY an event may matter.

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
import os
import json
import base64
import xml.etree.ElementTree as ET
from datetime import date, datetime, time as dtime, timedelta

API_URL   = "https://www.nseindia.com/api/corporate-announcements"
RSS_URL   = "https://www.nseindia.com/content/RSS/Online_announcements.xml"
EQUITY_L  = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

HISTORY_DAYS = 20          # TEST MODE: keep/fetch only the last 20 calendar days
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
    r"investor presentation|analyst|institutional investor|board meeting intimation|"
    r"regulation 51|regulation 57|amendment to aoa|notice of shareholders|credit rating|"
    r"movement in units|noc/no dues|corrigendum",
    re.I)

# Hard-noise filters: do not store these in nse_catalysts.json and never send
# them to Gemini. These are exchange surveillance / routine compliance items.
_BACKEND_NOISE = re.compile(
    r"news verification|exchange has sought clarification|clarification.*(?:price|volume)|"
    r"spurt in (?:price|volume)|significant movement in (?:the )?price|movement in (?:the )?price|"
    r"inter[- ]se transfer.*promoter|promoter.*inter[- ]se transfer|regulation 10\(6\)|"
    r"appointment|re-appointment|reappointment|statutory auditor|secretarial auditor|"
    r"internal auditor|cost auditor|scrutinizer",
    re.I)

# Routine completion/allotment after an already announced fund raise is not a
# catalyst for this feed. New/proposed fund raises remain eligible below.
_ROUTINE_ALLOTMENT = re.compile(
    r"allotment of (?:equity shares|shares|securities).*pursuant to (?:a )?(?:preferential|rights|qip)|"
    r"allotted .*securities.*preferential issue",
    re.I)

_NEGATIVE = re.compile(
    r"insolvency|\bcirp\b|default in interest|default in principal|show cause|"
    r"pendency of any litigation|pendency of litigation|actions? (initiated|taken)|"
    r"orders? passed|fire incident|\bfire\b|penalty|search and seizure|\braid\b|"
    r"fraud|suspension of",
    re.I)

_DEBT = re.compile(r"non.?convertible|debenture|\bncds?\b|commercial paper|\bbonds?\b|\bisin\b", re.I)
_RESULTS = re.compile(r"financial results?|audited results|unaudited results", re.I)

_ORDER = re.compile(
    r"orders?/contracts?|awarding of order|bagging|receiv(?:e|ed|ing) (?:an? )?order|"
    r"supply order|work order|purchase order|order (?:received|awarded|secured)|"
    r"letter of (?:intent|award|acceptance)|\bloa\b|\bloi\b|notification of award|"
    r"order wins?|\border (?:of|for|from|worth|valued)\b|\bmandate\b|deals? worth|"
    r"contract (?:award|awarded|of|for|from|worth)|\bl1\b|first lowest|lowest bidder",
    re.I)

_ACQUISITION = re.compile(
    r"acquisition|acquir(?:e|ed|ing)|purchase of .*stake|stake acquisition|"
    r"completion of acquisition|become .*wholly[- ]owned subsidiary",
    re.I)
_DIVESTMENT = re.compile(
    r"sale or disposal|divestment|disinvestment|sale of .*stake|sale of .*shareholding|"
    r"transfer of (?:the )?entire equity|ceased to be .*subsidiary|sale of surplus land|"
    r"asset monetisation|asset monetization",
    re.I)
_SCHEME = re.compile(
    r"scheme of arrangement|amalgamation|merger|demerger|scheme .*implemented|"
    r"restructuring pursuant to .*scheme",
    re.I)
_STRATEGIC_AGREEMENT = re.compile(
    r"intellectual property license|licen[cs]e agreement|strategic (?:agreement|partnership|collaboration)|"
    r"joint venture|\bjv\b|memorandum of understanding|\bmou\b|technical collaboration|"
    r"manufacturing agreement|distribution agreement|technology agreement|"
    r"execution of .*agreement|signing of .*agreement",
    re.I)
_CORP_ACTION = re.compile(
    r"buy ?back|bonus|stock split|sub-division|rights issue|qualified institutional|\bqip\b|"
    r"fund rais|preferential issue|dividend",
    re.I)

_VAGUE_SUBJECT = re.compile(r"press release|general updates|^updates$|disclosure of material issue|agreements?", re.I)


def classify(subject: str, text: str) -> str | None:
    """Trader-focused catalyst category, or None when the event should not be stored."""
    subject = (subject or "").strip()
    text = (text or "").strip()
    both = f"{subject} {text}"

    # Results are handled by the dedicated results pipeline.
    if _RESULTS.search(both) or (re.search(r"outcome of board meeting", subject, re.I) and _RESULTS.search(text)):
        return None

    # Drop obvious exchange/routine noise before any PDF/AI work.
    if _BACKEND_NOISE.search(both) or _ROUTINE_ALLOTMENT.search(both):
        return None
    if _IGNORE_SUBJECT.search(subject):
        return None
    if _DEBT.search(both):
        return None

    # Material event rules. Generic NSE subjects such as General Updates are
    # deliberately reclassified from their text before being discarded.
    if _NEGATIVE.search(both):
        return "Negative"
    if _ORDER.search(both):
        return "Order"
    if _ACQUISITION.search(both):
        return "Acquisition"
    if _DIVESTMENT.search(both):
        return "Divestment"
    if _SCHEME.search(both):
        return "Scheme of Arrangement"
    if _STRATEGIC_AGREEMENT.search(both):
        return "Strategic Agreement"
    if _CORP_ACTION.search(both):
        return "Corporate Action"

    # Unresolved General Updates / Updates / Agreements are not stored merely
    # for manual review anymore. This is what keeps the backend file controlled.
    if _VAGUE_SUBJECT.search(subject):
        return None
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
# Order PDF enrichment: Gemini first, local parser as fallback
# Only genuinely new Order catalysts are opened. On a clean/rebuild run, only
# today's Order PDFs are enriched so historical backfill remains fast. Gemini is
# never called for non-Order catalysts. Market cap is intentionally NOT requested
# from Gemini because it is market data, not a filing fact.
# ─────────────────────────────────────────────────────────────────────────────

_MONEY_RE = re.compile(
    r"(?:(?:₹|rs\.?|inr)\s*)?([0-9][0-9,]*(?:\.\d+)?)\s*"
    r"(crores?|cr\.?|lakhs?|lacs?|millions?|mn\.?|billions?|bn\.?)",
    re.I,
)
_ORDER_CONTEXT_RE = re.compile(
    r"order|contract|letter of award|letter of acceptance|letter of intent|\bloa?\b|"
    r"work order|purchase order|supply order|notification of award|mandate|awarded|bagged|won|wins",
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
        return "\n".join((p.extract_text() or "") for p in reader.pages[:12])
    except Exception as e:
        print(f"  ⚠ PDF detail extraction failed for {url.rsplit('/', 1)[-1]} ({e})")
        return ""


def _normalize_pdf_text(text: str) -> str:
    """Normalize common pypdf spacing artifacts without using OCR/AI."""
    s = re.sub(r"\s+", " ", text or " ").strip()
    # CFF/Type1 PDFs can emit decimals one glyph at a time: "75 . 9 6".
    s = re.sub(r"(?<=\d)\s*\.\s*(?=\d)", ".", s)
    s = re.sub(r"(?<=\d)\s+(?=\d)", "", s)
    # Clean punctuation/hyphen spacing created by line-oriented extraction.
    s = re.sub(r"\s+([,.;:])", r"\1", s)
    s = re.sub(r"\bN\s*-\s*Type\b", "N-Type", s, flags=re.I)
    s = re.sub(r"\bGlass\s*-\s*to\s*-\s*Glass\b", "Glass-to-Glass", s, flags=re.I)
    return s


def _clean_field(v: str, max_len: int = 350) -> str:
    v = re.sub(r"\s+", " ", v or "").strip(" :-;|\t\r\n")
    v = re.sub(r"\s+([,.;:])", r"\1", v)
    # Never let signature/footer prose leak into a table value.
    v = re.split(r"\b(?:This is for your information|Yours faithfully|Thanking you)\b", v,
                 maxsplit=1, flags=re.I)[0]
    return v[:max_len].strip(" :-;|")


def _table_items(clean: str) -> dict[int, str]:
    """Return numbered SEBI disclosure rows (1..9), bounded by the next row."""
    hits = list(re.finditer(r"(?<!\d)\b([1-9])\.\s+", clean))
    rows = {}
    for i, m in enumerate(hits):
        n = int(m.group(1))
        if n in rows:
            continue
        end = hits[i + 1].start() if i + 1 < len(hits) else len(clean)
        rows[n] = clean[m.end():end].strip()
    return rows


def _row_answer(row: str, label_pattern: str, max_len: int = 400) -> str:
    if not row:
        return ""
    m = re.search(label_pattern, row, re.I)
    if not m:
        return ""
    return _clean_field(row[m.end():], max_len)


def _extract_order_details(text: str) -> dict:
    clean = _normalize_pdf_text(text)
    if not clean:
        return {}

    rows = _table_items(clean)

    entity = _row_answer(
        rows.get(1, ""),
        r"name of (?:the )?entity awarding (?:the )?order\(s\)/\s*contract\(s\)",
    )
    terms = _row_answer(
        rows.get(2, ""),
        r"significant terms and conditions of order\(s\)/\s*contract\(s\) awarded in brief",
    )
    nature = _row_answer(
        rows.get(4, ""),
        r"nature of order\(s\)\s*/\s*contract\(s\)",
    )
    # Prefer a descriptive terms row, but ignore generic boilerplate.
    if terms and not re.fullmatch(r"as per (?:the )?terms of (?:the )?order", terms, re.I):
        purpose = terms
    else:
        purpose = nature

    execution = _row_answer(
        rows.get(6, ""),
        r"time period by which (?:the )?order\(s\)\s*/?\s*contract\(s\) is to be executed",
    )

    order_type = ""
    # Row 5 is the cleanest standardized domestic/international field.
    row5 = rows.get(5, "")
    tm = re.search(r"whether domestic or international\s+(domestic|international)\b", row5, re.I)
    if not tm:
        tm = re.search(r"\b(domestic|international)\b", row5, re.I)
    if not tm:
        row3 = rows.get(3, "")
        tm = re.search(r"\b(domestic|international)(?:\s+entit(?:y|ies))?\b\s*$", row3, re.I)
    if tm:
        order_type = tm.group(1).title()

    # Row 9 specifically answers the related-party question. Do not search the
    # whole PDF because unrelated Yes/No answers create false positives.
    related_party = None
    row9 = rows.get(9, "")
    if row9:
        yn = re.search(r"\b(Yes|No)\b\s*$", row9, re.I)
        if not yn:
            answers = re.findall(r"\b(Yes|No)\b", row9, re.I)
            if answers:
                yn = type("_M", (), {"group": lambda self, _n: answers[-1]})()
        if yn:
            related_party = yn.group(1).lower() == "yes"

    # Prefer row 7 (Broad consideration / size) for monetary value. This avoids
    # unrelated amounts elsewhere in the filing. Fall back to contextual scan.
    money_scopes = []
    if rows.get(7):
        money_scopes.append((rows[7], 2))
    money_scopes.append((clean, 0))
    candidates = []
    for scope, scope_score in money_scopes:
        for m in _MONEY_RE.finditer(scope):
            lo, hi = max(0, m.start() - 220), min(len(scope), m.end() + 220)
            context = scope[lo:hi]
            strong = bool(re.search(
                r"order value|total order value|broad consideration|size of (?:the )?order|"
                r"order\(s\)/contract\(s\)|contract value|value of (?:the )?(?:order|contract)",
                context, re.I,
            ))
            if scope_score == 0 and not strong and not _ORDER_CONTEXT_RE.search(context):
                continue
            value_cr = _money_to_cr(m.group(1), m.group(2))
            if value_cr is not None and value_cr > 0:
                candidates.append((scope_score + (1 if strong else 0), value_cr,
                                   m.group(0).strip(), context))
        if candidates and scope_score == 2:
            break

    out = {}
    if candidates:
        # Highest-confidence scope first; largest amount only breaks ties.
        _, value_cr, raw, context = max(candidates, key=lambda x: (x[0], x[1]))
        out.update({
            "order_value_cr": value_cr,
            "order_value_text": raw,
            "detail_excerpt": _clean_field(context, 500),
        })
    if entity:
        out["order_from"] = entity
    if purpose:
        out["order_purpose"] = purpose
    if order_type:
        out["order_type"] = order_type
    if execution:
        out["execution_period"] = execution
    if related_party is not None:
        out["related_party"] = related_party
    if out:
        out["detail_source"] = "pdf_local"
    return out

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_ORDER_MODEL = os.environ.get("GEMINI_ORDER_MODEL", os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")).strip()

_ORDER_AI_PROMPT = r"""
You are extracting factual details from an Indian listed company's order/contract announcement PDF.
Read the ENTIRE PDF carefully, including tables, annexures and footnotes.

Return ONLY one valid JSON object. Do not use markdown. Do not infer facts that are not disclosed.
Use null for unavailable fields and [] for unavailable lists.

Schema:
{
  "order_value_cr": number|null,
  "order_value_text": string|null,
  "customer": string|null,
  "order_scope": string|null,
  "order_type": "Domestic"|"International"|null,
  "execution_period": string|null,
  "quantity_or_capacity": string|null,
  "project_location": string|null,
  "related_party": boolean|null,
  "promoter_interest": boolean|null,
  "tax_inclusion": string|null,
  "currency": string|null,
  "company_share_of_order_cr": number|null,
  "other_material_details": [string],
  "summary": string|null
}

Rules:
- order_value_cr must be the disclosed order/contract value converted to INR crore when the PDF permits a reliable conversion.
- Preserve the original disclosed amount in order_value_text.
- If the announcement covers multiple orders, use the disclosed aggregate total when available and explain the split in other_material_details.
- customer is the awarding entity/client. Do not include table headings or row numbers.
- order_scope is a concise description of the actual goods/services/project scope.
- Capture MW/MWp, units, kilometres, tonnes, project capacity or other meaningful quantity in quantity_or_capacity when disclosed.
- Capture project/site/geography in project_location when disclosed.
- related_party must come from the specific related-party disclosure, not an unrelated Yes/No elsewhere.
- promoter_interest must come from the promoter/promoter-group interest disclosure.
- tax_inclusion should say whether the stated order value includes/excludes taxes if explicitly disclosed.
- company_share_of_order_cr is only for consortium/JV orders where the company's own share is explicitly disclosed or directly calculable from disclosed figures.
- other_material_details should contain only decision-useful factual details from the PDF, such as consortium share, repeat order, tender/LOA status, milestone, special terms, or customer/project specifics. Avoid boilerplate.
- summary must be one concise factual sentence. Do not call the order bullish/bearish, good/bad, material/immaterial, or predict stock-price impact.
- Do NOT provide market cap, revenue, valuation, order-to-market-cap ratio, or any fact not contained in this PDF.
""".strip()


def _download_pdf_bytes(session, url: str) -> bytes:
    if not url or url == "-" or not url.lower().split("?", 1)[0].endswith(".pdf"):
        return b""
    try:
        r = session.get(url, timeout=45)
        r.raise_for_status()
        data = r.content
        if not data.startswith(b"%PDF"):
            return b""
        return data
    except Exception as e:
        print(f"  ⚠ PDF download failed for {url.rsplit('/', 1)[-1]} ({e})")
        return b""


def _extract_pdf_text_bytes(pdf_bytes: bytes) -> str:
    if not pdf_bytes:
        return ""
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        return "\n".join((p.extract_text() or "") for p in reader.pages[:12])
    except Exception:
        return ""


def _gemini_order_details(session, pdf_bytes: bytes, filename: str = "") -> dict:
    if not GEMINI_API_KEY or not pdf_bytes:
        return {}
    try:
        payload = {
            "contents": [{"parts": [
                {"text": _ORDER_AI_PROMPT},
                {"inline_data": {"mime_type": "application/pdf", "data": base64.b64encode(pdf_bytes).decode("ascii")}},
            ]}],
            "generationConfig": {
                "temperature": 0.05,
                "maxOutputTokens": 4096,
                "responseMimeType": "application/json",
            },
        }
        r = session.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_ORDER_MODEL}:generateContent?key={GEMINI_API_KEY}",
            json=payload, timeout=120,
        )
        if r.status_code == 429:
            print(f"    · [{filename}] Gemini skipped: quota/rate limit")
            return {}
        r.raise_for_status()
        data = r.json()
        candidates = data.get("candidates") or []
        if not candidates:
            return {}
        raw = "".join(p.get("text", "") for p in candidates[0].get("content", {}).get("parts", []))
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.I | re.M).strip()
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            return {}

        out = {}
        # Map AI schema to existing catalyst keys so the frontend remains compatible.
        mapping = {
            "order_value_cr": "order_value_cr",
            "order_value_text": "order_value_text",
            "customer": "order_from",
            "order_scope": "order_purpose",
            "order_type": "order_type",
            "execution_period": "execution_period",
            "quantity_or_capacity": "quantity_or_capacity",
            "project_location": "project_location",
            "related_party": "related_party",
            "promoter_interest": "promoter_interest",
            "tax_inclusion": "tax_inclusion",
            "currency": "currency",
            "company_share_of_order_cr": "company_share_of_order_cr",
            "other_material_details": "other_material_details",
            "summary": "order_summary",
        }
        for src, dst in mapping.items():
            v = obj.get(src)
            if v is not None and v != "" and v != []:
                out[dst] = v

        # Basic type/sanity guards. Bad AI fields are dropped, allowing local fallback.
        if "order_value_cr" in out:
            try:
                v = float(out["order_value_cr"])
                if not (0 < v < 10_000_000):
                    raise ValueError
                out["order_value_cr"] = round(v, 4)
            except (TypeError, ValueError):
                out.pop("order_value_cr", None)
        if "company_share_of_order_cr" in out:
            try:
                v = float(out["company_share_of_order_cr"])
                if v <= 0:
                    raise ValueError
                out["company_share_of_order_cr"] = round(v, 4)
            except (TypeError, ValueError):
                out.pop("company_share_of_order_cr", None)
        for k in ("related_party", "promoter_interest"):
            if k in out and not isinstance(out[k], bool):
                out.pop(k, None)
        if out:
            out["detail_source"] = "gemini_pdf"
        return out
    except Exception as e:
        print(f"    · [{filename}] Gemini extraction failed ({e})")
        return {}


def enrich_new_orders(session, new_items: dict, existing_ids: set[str],
                      today: date, initial_build: bool = False,
                      market_cap_map: dict | None = None,
                      ttm_sales_map: dict | None = None) -> tuple[int, int]:
    """Gemini-first enrichment for genuinely new Order PDFs; local parser fallback."""
    checked = enriched = values_found = gemini_ok = local_fallback = 0
    for items in new_items.values():
        for it in items:
            if it.get("category") != "Order" or it.get("id") in existing_ids:
                continue
            if initial_build and str(it.get("dt", ""))[:10] != today.isoformat():
                continue

            checked += 1
            url = it.get("link", "")
            fname = url.rsplit("/", 1)[-1] if url else "order.pdf"
            pdf_bytes = _download_pdf_bytes(session, url)
            if not pdf_bytes:
                continue

            details = _gemini_order_details(session, pdf_bytes, fname)
            if details:
                gemini_ok += 1
            else:
                text = _extract_pdf_text_bytes(pdf_bytes)
                details = _extract_order_details(text)
                if details:
                    local_fallback += 1

            if details:
                it.update(details)
                enriched += 1
                order_value = details.get("order_value_cr")
                if order_value is not None:
                    values_found += 1
                    try:
                        order_value = float(order_value)
                        symbol = str(it.get("symbol") or "").strip().upper()
                        # Items are grouped by symbol, but _make_item does not store it.
                        # The caller stamps _lookup_symbol temporarily before enrichment.
                        symbol = str(it.get("_lookup_symbol") or symbol).strip().upper()
                        mcap = (market_cap_map or {}).get(symbol)
                        ttm = (ttm_sales_map or {}).get(symbol)
                        if mcap is not None and float(mcap) > 0:
                            it["order_to_market_cap_pct"] = round(order_value / float(mcap) * 100.0, 2)
                        if ttm is not None and float(ttm) > 0:
                            it["order_to_ttm_sales_pct"] = round(order_value / float(ttm) * 100.0, 2)
                    except (TypeError, ValueError, ZeroDivisionError):
                        pass

    if initial_build:
        print(f"  ⚡ Initial/rebuild mode → historical Order PDFs skipped; only {today.isoformat()} orders enriched")
    if checked:
        print(f"  ✓ Order-PDF enrichment → checked={checked}, details_found={enriched}, "
              f"value_found={values_found}, Gemini={gemini_ok}, local_fallback={local_fallback}")
    elif initial_build:
        print("  ✓ Order-PDF enrichment → checked=0")
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


def _build_materiality_maps(classification_payload, fundamentals_payload):
    """Return symbol -> market cap (₹ cr) and symbol -> TTM sales (₹ cr)."""
    market_cap_map = {}
    ttm_sales_map = {}

    # classification.json may be a list, {"data": [...]}, {"stocks": [...]},
    # or a symbol-keyed dict. Prefer nse_code, then symbol.
    rows = classification_payload
    if isinstance(rows, dict):
        if isinstance(rows.get("data"), list):
            rows = rows["data"]
        elif isinstance(rows.get("stocks"), list):
            rows = rows["stocks"]
        elif all(isinstance(v, dict) for v in rows.values()):
            rows = list(rows.values())
        else:
            rows = []
    if isinstance(rows, list):
        for x in rows:
            if not isinstance(x, dict):
                continue
            sym = str(x.get("nse_code") or x.get("symbol") or "").strip().upper()
            try:
                mcap = float(x.get("market_cap_cr"))
                if sym and mcap > 0:
                    market_cap_map[sym] = mcap
            except (TypeError, ValueError):
                pass

    # fundamentals_summary.json shape: {"updated": ..., "stocks": {SYMBOL: {...}}}
    stocks = fundamentals_payload.get("stocks", {}) if isinstance(fundamentals_payload, dict) else {}
    if isinstance(stocks, dict):
        for key, x in stocks.items():
            if not isinstance(x, dict):
                continue
            sym = str(x.get("symbol") or key or "").strip().upper()
            quarters = x.get("quarters") or []
            vals = []
            for q in quarters[:4]:
                if not isinstance(q, dict):
                    continue
                try:
                    sales = float(q.get("sales"))
                    if sales >= 0:
                        vals.append(sales)
                except (TypeError, ValueError):
                    pass
            # Require four reported quarters. Source sales are rupees; 1 crore = 1e7 rupees.
            if sym and len(vals) == 4:
                ttm_sales_map[sym] = sum(vals) / 1e7

    return market_cap_map, ttm_sales_map

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

    # Materiality inputs are read from the same R2 store. They are used only
    # to calculate the two ratios below; raw market cap / TTM sales are not
    # duplicated into nse_catalysts.json.
    classification_payload = _r2_get_json(r2_session, "classification.json") or {}
    fundamentals_payload = _r2_get_json(r2_session, "fundamentals_summary.json") or {}
    market_cap_map, ttm_sales_map = _build_materiality_maps(
        classification_payload, fundamentals_payload
    )
    print(f"  ✓ Materiality lookup → market_cap={len(market_cap_map)} symbols, "
          f"ttm_sales={len(ttm_sales_map)} symbols")

    # Re-apply today's backend rules to historical rows as well. This removes
    # old Results, Other, Clarification/News Verification, routine allotments,
    # auditor appointments, promoter inter-se transfers and other noise already
    # present in R2. Manual rows are preserved.
    removed_noise = 0
    reclassified = 0
    for sym in list(history):
        cleaned = []
        for x in history[sym]:
            if x.get("manual"):
                cleaned.append(x)
                continue
            cat = classify(x.get("subject", ""), x.get("text", ""))
            if not cat:
                removed_noise += 1
                continue
            if x.get("category") != cat:
                x["category"] = cat
                reclassified += 1
            cleaned.append(x)
        history[sym] = cleaned
        if not history[sym]:
            del history[sym]
    if removed_noise or reclassified:
        print(f"  🧹 Historical cleanup → removed={removed_noise}, reclassified={reclassified}")

    existing_ids = {x.get("id") for items in history.values() for x in items if x.get("id")}
    initial_build = not bool(existing_ids)

    new_items, source = fetch_catalysts(
        nse_session, today, HISTORY_DAYS, is_trading_day, next_trading_day
    )
    fetched = sum(len(v) for v in new_items.values())

    # Stamp the group symbol temporarily so enrichment can join to the R2
    # classification/fundamentals lookups without changing the stored schema.
    for _sym, _items in new_items.items():
        for _it in _items:
            _it["_lookup_symbol"] = str(_sym).strip().upper()

    # Gemini reads only genuinely new Order PDFs; local parser is the fallback.
    # Non-Order catalysts never reach Gemini.
    enrich_new_orders(
        nse_session, new_items, existing_ids, today, initial_build,
        market_cap_map, ttm_sales_map
    )

    # Internal join key must never be persisted.
    for _items in new_items.values():
        for _it in _items:
            _it.pop("_lookup_symbol", None)

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
