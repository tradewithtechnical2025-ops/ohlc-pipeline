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

# Hard noise: reject before any PDF/AI work.
_BACKEND_NOISE = re.compile(
    r"news verification|exchange has sought clarification|clarification.*(?:price|volume)|"
    r"spurt in (?:price|volume)|significant movement in (?:the )?price|movement in (?:the )?price|"
    r"inter[- ]se transfer.*promoter|promoter.*inter[- ]se transfer|regulation 10\(6\)",
    re.I)

# Professional-service appointments are routine. Do NOT use a blanket
# 'appointment/resignation' rule: CEO/MD/CFO/WTD/director changes may be material.
_ROUTINE_PROFESSIONAL = re.compile(
    r"(?:appointment|re-appointment|reappointment).*"
    r"(?:statutory auditor|secretarial auditor|internal auditor|cost auditor|scrutinizer|"
    r"chartered accountant|\bca firm\b)|"
    r"(?:statutory auditor|secretarial auditor|internal auditor|cost auditor|scrutinizer).*"
    r"(?:appointment|re-appointment|reappointment)", re.I)

# SAST/promoter-shareholding disclosures are not acquisitions by the listed company.
_SAST_NOISE = re.compile(
    r"disclosure.*regulation\s*(?:29|31)\s*\(?[12]?\)?|"
    r"regulation\s*29\s*\(?2\)?|regulation\s*31|"
    r"substantial acquisition of shares and takeovers regulations|\bsast\b", re.I)
_PROMOTER_MPS_SALE = re.compile(
    r"sale of (?:equity )?shares by (?:a )?promoter.*(?:open market|minimum public shareholding)|"
    r"promoter.*(?:minimum public shareholding|\bmps\b)", re.I)

# Incorporating/funding one's own subsidiary is not an external acquisition catalyst.
_SUBSIDIARY_INCORPORATION = re.compile(
    r"incorporation of (?:a |an )?(?:wholly owned |step[- ]down )?subsidiar|"
    r"incorporat(?:e|ed|ion).*\b(?:wos|wholly owned subsidiary|step[- ]down subsidiary)\b", re.I)
_INTERNAL_SUB_INVESTMENT = re.compile(
    r"(?:additional )?investment.*(?:wholly owned subsidiary|\bwos\b)|"
    r"subscription.*(?:rights issue|equity shares).*?(?:wholly owned subsidiary|\bwos\b)|"
    r"(?:wholly owned subsidiary|\bwos\b).*?(?:rights issue|additional investment|capital infusion)", re.I)

# Routine completion/allotment after an already-announced raise is not a new catalyst.
_ROUTINE_ALLOTMENT = re.compile(
    r"allotment of (?:equity shares|shares|securities).*pursuant to (?:a )?(?:preferential|rights|qip)|"
    r"allotted .*securities.*preferential issue|conversion of .*warrants.*(?:equity shares|preferential)",
    re.I)

# Administrative dividend/buyback paperwork and duplicate communications.
_ROUTINE_CORP_ACTION = re.compile(
    r"(?:tds|kyc|non[- ]?compliant).*dividend|withholding of .*dividend|"
    r"dividend.*(?:tds|kyc|non[- ]?compliant)|non[- ]credit of dividend|"
    r"(?:agm|annual general meeting).*approval of dividend|approval of dividend.*(?:agm|annual general meeting)|"
    r"clarification.*valuation methodology.*preferential issue|"
    r"dispatch.*(?:buyback|rights)|trading approval.*(?:bonus|split|rights|preferential)", re.I)

# Insolvency/proceeding steps that do not change the economic state of the case.
_CIRP_PROCEDURAL = re.compile(
    r"(?:prior |post[- ]facto )?intimation.*(?:(?:coc|committee of creditors).*meeting|meeting.*(?:coc|committee of creditors))|"
    r"(?:outcome|voting results?).*(?:(?:coc|committee of creditors).*meeting|meeting.*(?:coc|committee of creditors))|"
    r"appointment of (?:the )?(?:irp|rp|resolution professional)|"
    r"interim resolution professional.*(?:performing|functions)|"
    r"cirp.*trading window|trading window.*cirp", re.I)

# Some NSE CIRP summaries are completely generic; the filing filename/link carries
# the procedural meaning.  These patterns are intentionally narrow and are used
# only for Negative/CIRP rows, never as a global subject filter.
_CIRP_PROCEDURAL_ITEM = re.compile(
    r"(?:coc|committee[ _-]?of[ _-]?creditors).{0,40}(?:meeting|outcome|voting)|"
    r"(?:meeting|outcome|voting).{0,40}(?:coc|committee[ _-]?of[ _-]?creditors)|"
    r"appointment.{0,30}(?:irp|rp|resolution[ _-]?professional)|"
    r"(?:irp|rp|resolution[ _-]?professional).{0,30}appointment|"
    r"trading[ _-]?window|vacation[ _-]?(?:of[ _-]?)?(?:office|director)", re.I)

_CIRP_MATERIAL = re.compile(
    r"(?:cirp|insolvency).*(?:admit(?:ted|sion)?|initiat(?:ed|ion)|commenc(?:ed|ement))|"
    r"resolution plan.*(?:approved|rejected|dismissed|accepted)|"
    r"(?:nclt|nclat).*(?:approved|rejected|sanctioned|dismissed|liquidation)|"
    r"liquidation.*(?:ordered|order|approved)|"
    r"(?:settlement|withdrawal|termination).*(?:cirp|insolvency)|"
    r"(?:cirp|insolvency).*(?:settlement|withdrawal|termination)", re.I)

def _is_cirp_procedural_item(item: dict) -> bool:
    """Drop only clearly administrative CIRP filings; material case milestones win."""
    subject = str(item.get("subject") or "")
    text = str(item.get("text") or "")
    link = str(item.get("link") or "")
    both = f"{subject} {text} {link}"
    if not re.search(r"corporate insolvency resolution process|\bcirp\b|insolvency", both, re.I):
        return False
    if _CIRP_MATERIAL.search(both):
        return False
    return bool(_CIRP_PROCEDURAL.search(both) or _CIRP_PROCEDURAL_ITEM.search(both))

# Scheme notices/reports are procedural; retain approvals, NCLT orders, effective dates,
# record dates and implementation/completion milestones.
_SCHEME_PROCEDURAL = re.compile(
    r"board meeting.*(?:scheduled|to consider).*(?:scheme|merger|demerger|amalgamation)|"
    r"(?:audit committee|independent directors?).*report.*(?:scheme|merger|demerger|amalgamation)|"
    r"(?:notice|convening).*(?:shareholders?|creditors?).*meeting.*(?:scheme|merger|demerger)|"
    r"(?:hearing date|date of hearing).*?(?:scheme|merger|demerger|amalgamation)|"
    r"(?:petition|second motion petition).*(?:admitted|admission)", re.I)

# Only explicit senior executive changes are catalysts. NSE's generic subjects such as
# "Resignation of Director/KMP/SMP" are intentionally NOT enough on their own;
# otherwise hundreds of routine personnel filings enter the feed.
_MANAGEMENT_CHANGE = re.compile(
    r"(?:appointment|appointed|resignation|resigned|cessation|retirement|vacation of office).*?"
    r"(?:chief executive officer|\bceo\b|managing director|\bmd\b|chief financial officer|\bcfo\b|"
    r"whole[- ]time director|whole time director)|"
    r"(?:chief executive officer|\bceo\b|managing director|\bmd\b|chief financial officer|\bcfo\b|"
    r"whole[- ]time director|whole time director).*?"
    r"(?:appointment|appointed|resignation|resigned|cessation|retirement|vacation of office)", re.I)
_REGULATORY_GRANT = re.compile(
    r"(?:grant|receipt|received|obtained|renewal).*?(?:licen[cs]e|registration|regulatory approval|certificate of registration)|"
    r"(?:licen[cs]e|registration|regulatory approval|certificate of registration).*?(?:granted|received|obtained|renewed)", re.I)

_NEGATIVE = re.compile(
    r"insolvency|\bcirp\b|default in interest|default in principal|show cause|"
    r"pendency of any litigation|pendency of litigation|actions? (initiated|taken)|"
    r"orders? passed|fire incident|\bfire\b|penalty|search and seizure|\braid\b|"
    r"fraud|suspension of|liquidation|resolution plan (?:rejected|dismissed)", re.I)
_ADVERSE_TAX_ORDER = re.compile(
    r"(?:receipt of |received )?(?:an? )?order from .*?(?:income tax|gst|tax authority)|"
    r"(?:income tax|gst|tax authority).*?(?:demand|penalty|order|show cause)", re.I)

_DEBT = re.compile(r"non.?convertible|debenture|\bncds?\b|commercial paper|\bbonds?\b|\bisin\b", re.I)
_RESULTS = re.compile(r"financial results?|audited results|unaudited results", re.I)

_ORDER = re.compile(
    r"orders?/contracts?|awarding of order|bagging|receiv(?:e|ed|ing) (?:an? )?order|"
    r"supply order|work order|purchase order|order (?:received|awarded|secured)|"
    r"letter of (?:intent|award|acceptance)|\bloa\b|\bloi\b|notification of award|"
    r"order wins?|\border (?:of|for|from|worth|valued)\b|\bmandate\b|deals? worth|"
    r"contract (?:award|awarded|of|for|from|worth)|\bl1\b|first lowest|lowest bidder|preferred bidder",
    re.I)
_ORDER_PRE_BID = re.compile(
    r"bid submitted|submission of bid|tender participation|participat(?:e|ion).*tender|"
    r"expression of interest|\beoi\b|pre[- ]qualification|technical bid qualified", re.I)
_ORDER_CANCEL = re.compile(r"(?:order|contract).*(?:cancelled|canceled|terminated)|(?:cancellation|termination).*(?:order|contract)", re.I)

_ACQUISITION = re.compile(
    r"acquisition|acquir(?:e|ed|ing)|purchase of .*stake|purchase of .*business|"
    r"purchase of .*assets?|stake acquisition|completion of acquisition|"
    r"purchase .*equity shares|definitive agreement.*acquir", re.I)
_DIVESTMENT = re.compile(
    r"sale or disposal|divestment|disinvestment|sale of .*stake|sale of .*shareholding|"
    r"transfer of (?:the )?entire equity|ceased to be .*subsidiary|sale of surplus land|"
    r"asset monetisation|asset monetization|business sale|sale of undertaking", re.I)
_SCHEME = re.compile(
    r"scheme of arrangement|amalgamation|merger|demerger|scheme .*implemented|"
    r"restructuring pursuant to .*scheme", re.I)
_STRATEGIC_AGREEMENT = re.compile(
    r"intellectual property license|licen[cs]e agreement|strategic (?:agreement|partnership|collaboration)|"
    r"joint venture|\bjv\b|memorandum of understanding|\bmou\b|technical collaboration|"
    r"manufacturing agreement|distribution agreement|technology agreement|"
    r"port operations agreement|hotel management agreement", re.I)
_CORP_ACTION = re.compile(
    r"buy ?back|bonus|stock split|sub-division|rights issue|qualified institutional|\bqip\b|"
    r"fund rais|preferential issue|dividend", re.I)

_VAGUE_SUBJECT = re.compile(r"press release|general updates|^updates$|disclosure of material issue|agreements?", re.I)


def _event_meta(subject: str, text: str, category: str) -> dict:
    """Cheap deterministic event type/stage hints for frontend and dedupe work."""
    both = f"{subject or ''} {text or ''}"
    out = {}
    if category == "Order":
        if re.search(r"\bl1\b|first lowest|lowest bidder|preferred bidder", both, re.I):
            out.update(event_type="L1 Bidder", stage="L1 / Awaiting Award")
        elif re.search(r"letter of award|letter of acceptance|\bloa\b|awarded|bagging/receiving|order received|work order|purchase order|supply order|contract win", both, re.I):
            out.update(event_type="Order Award", stage="Awarded")
    elif category == "Acquisition":
        out["event_type"] = "Acquisition"
        if re.search(r"completed|completion|acquired", both, re.I): out["stage"] = "Completed"
        elif re.search(r"definitive agreement|agreement signed|entered into.*agreement", both, re.I): out["stage"] = "Agreement Signed"
        elif re.search(r"approved|board.*approval", both, re.I): out["stage"] = "Approved"
        else: out["stage"] = "Announced"
    elif category == "Divestment":
        out["event_type"] = "Divestment"
        if re.search(r"extension|extended|delay", both, re.I): out["stage"] = "Completion Delayed/Extended"
        elif re.search(r"completed|completion|ceased to be", both, re.I): out["stage"] = "Completed"
        elif re.search(r"agreement|approved", both, re.I): out["stage"] = "Approved / Agreement"
        else: out["stage"] = "Announced"
    elif category == "Scheme of Arrangement":
        if re.search(r"demerger|hive[- ]?off", both, re.I): out["event_type"] = "Demerger"
        elif re.search(r"merger|amalgamation", both, re.I): out["event_type"] = "Merger"
        else: out["event_type"] = "Scheme of Arrangement"
        if re.search(r"effective date|became effective|implemented|fully implemented|completed", both, re.I): out["stage"] = "Effective / Completed"
        elif re.search(r"nclt.*(?:approved|approval)|(?:approved|sanctioned).*nclt", both, re.I): out["stage"] = "NCLT Approved"
        elif re.search(r"record date", both, re.I): out["stage"] = "Record Date"
        elif re.search(r"observation letter|no[- ]?objection|\bnoc\b", both, re.I): out["stage"] = "Exchange NOC"
        elif re.search(r"approved|outcome of board meeting", both, re.I): out["stage"] = "Board Approved"
    elif category == "Strategic Agreement":
        if re.search(r"joint venture|\bjv\b", both, re.I): out["event_type"] = "Joint Venture"
        elif re.search(r"licen[cs]e|intellectual property", both, re.I): out["event_type"] = "IP / Licence Agreement"
        else: out["event_type"] = "Strategic Agreement"
        if re.search(r"non[- ]binding", both, re.I): out["stage"] = "Non-Binding MoU"
        elif re.search(r"definitive|executed|entered into|signed", both, re.I): out["stage"] = "Definitive / Signed"
        elif re.search(r"memorandum of understanding|\bmou\b", both, re.I): out["stage"] = "MoU"
    return out


def classify(subject: str, text: str) -> str | None:
    """Trader-focused catalyst category, or None when the event should not be stored."""
    subject = (subject or "").strip()
    text = (text or "").strip()
    both = f"{subject} {text}"

    if _RESULTS.search(both) or (re.search(r"outcome of board meeting", subject, re.I) and _RESULTS.search(text)):
        return None
    if (_BACKEND_NOISE.search(both) or _ROUTINE_PROFESSIONAL.search(both) or
            _ROUTINE_ALLOTMENT.search(both) or _ROUTINE_CORP_ACTION.search(both) or
            _SAST_NOISE.search(both) or _PROMOTER_MPS_SALE.search(both) or
            _SUBSIDIARY_INCORPORATION.search(both) or _INTERNAL_SUB_INVESTMENT.search(both) or
            _CIRP_PROCEDURAL.search(both) or _SCHEME_PROCEDURAL.search(both)):
        return None
    if _IGNORE_SUBJECT.search(subject) or _DEBT.search(both):
        return None

    # Positive regulatory grants must be resolved before the broad NSE subject
    # "granting/withdrawal/.../suspension" can trigger the Negative regex.
    if _REGULATORY_GRANT.search(both):
        return "Regulatory Approval"
    if _MANAGEMENT_CHANGE.search(both):
        return "Management Change"

    # Cancellation/termination of an order is adverse, never a fresh Order win.
    if _ORDER_CANCEL.search(both) or _ADVERSE_TAX_ORDER.search(both):
        return "Negative"
    if _NEGATIVE.search(both):
        return "Negative"

    # Do not promote mere tender participation into an Order catalyst.
    if _ORDER_PRE_BID.search(both):
        return None
    if _ORDER.search(both):
        return "Order"

    # Divestment is checked before acquisition so JV dilution/business-sale text
    # containing the counterparty's word 'acquire' is not mislabeled Acquisition.
    if _DIVESTMENT.search(both):
        return "Divestment"
    if _SCHEME.search(both):
        return "Scheme of Arrangement"
    if _STRATEGIC_AGREEMENT.search(both):
        return "Strategic Agreement"
    if _ACQUISITION.search(both):
        return "Acquisition"
    if _CORP_ACTION.search(both):
        return "Corporate Action"

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
    item = {
        "id":         f"{dt.strftime('%Y%m%d%H%M%S')}|{subject[:40]}|{(link or '')[-60:]}",
        "dt":         dt.isoformat(timespec="seconds"),
        "react_date": rd,
        "session":    sess,
        "category":   category,
        "subject":    subject.strip(),
        "text":       text[:TEXT_MAX],
        "link":       link or "",
    }
    item.update(_event_meta(subject, text, category))
    return item


# ─────────────────────────────────────────────────────────────────────────────
# Order PDF enrichment: local parser first, Gemini only when local extraction is insufficient
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

# Local money extraction must be conservative.  These phrases commonly occur in
# explanatory classification tables and are NOT transaction values.
_MONEY_RANGE_NOISE_RE = re.compile(
    r"(?:up to|above|below|less than|more than|between)\s*(?:₹|rs\.?|inr)?\s*[0-9][0-9,.]*\s*(?:crores?|cr\.?)|"
    r"[0-9][0-9,.]*\s*(?:crores?|cr\.?)\s*(?:to|[-–—])\s*(?:₹|rs\.?|inr)?\s*[0-9][0-9,.]*\s*(?:crores?|cr\.?)|"
    r"project classification|significant orders|large orders|mega orders|ultra[- ]?mega orders",
    re.I,
)

_STRONG_MONEY_CONTEXT_RE = re.compile(
    r"consideration|purchase price|transaction value|order value|contract value|broad consideration|"
    r"issue size|fund ?raise|penalty|fine|demand|claim amount|amount payable|investment of|project cost|"
    r"aggregate consideration|total consideration|sale value|buyback size",
    re.I,
)

def _has_inr_marker(raw: str, context: str) -> bool:
    """True only when the amount is explicitly INR/Rupee denominated nearby."""
    raw = raw or ""
    context = context or ""
    if re.search(r"(?:₹|\brs\.?\b|\binr\b|rupees?)", raw, re.I):
        return True
    # Allow a currency heading immediately around a tabular value.
    return bool(re.search(r"(?:₹|\brs\.?\b|\binr\b|rupees?).{0,45}$", context[:max(0, len(context)//2)], re.I))

def _money_candidate_is_safe(raw: str, context: str, *, standardized_row: bool = False) -> bool:
    if _MONEY_RANGE_NOISE_RE.search(context or ""):
        return False
    unit_m = re.search(r"(crores?|cr\.?|lakhs?|lacs?|millions?|mn\.?|billions?|bn\.?)", raw or "", re.I)
    unit = (unit_m.group(1).lower().replace('.', '') if unit_m else "")
    # Never convert a bare million/billion amount: currency may be USD/EUR/etc.
    if unit.startswith(("million", "billion")) or unit in {"mn", "bn"}:
        return _has_inr_marker(raw, context) and bool(_STRONG_MONEY_CONTEXT_RE.search(context or ""))
    # Generic catalyst parsing requires explicit rupee/INR denomination.
    if not standardized_row and not _has_inr_marker(raw, context):
        return False
    # Even with currency, reject isolated amounts without transaction context.
    return standardized_row or bool(_STRONG_MONEY_CONTEXT_RE.search(context or ""))


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


def _order_amount_role_score(raw: str, context: str) -> tuple[int, str]:
    """Rank an INR amount by semantic role: headline total > component."""
    c = (context or "").lower()
    # Component labels are usually immediately adjacent to their amount. Check
    # them first in this deliberately tight context window.
    if re.search(r"transferable\s+development\s+rights|\btdr\b|land\s+premium|free\s+sale\s+land|"
                 r"security\s+deposit|performance\s+(?:bank\s+)?guarantee|\bpbg\b|advance\s+payment|"
                 r"retention\s+money|liquidated\s+damages|component|portion|tranche", c):
        return -5, "component"
    if re.search(r"total\s+(?:development|project|contract|order)\s+(?:cost|value)|"
                 r"aggregate\s+(?:order|contract|project)\s+value|total\s+consideration|"
                 r"overall\s+(?:project|order|contract)\s+(?:cost|value)|broad\s+consideration|"
                 r"size\s+of\s+(?:the\s+)?(?:order|contract)", c):
        return 8, "total"
    if re.search(r"order\s+value|contract\s+value|value\s+of\s+(?:the\s+)?(?:order|contract)|"
                 r"project\s+cost|work\s+order\s+value|loa\s+value", c):
        return 6, "headline"
    return 1, "unspecified"


def _nearest_role_context(scope: str, start: int, end: int, radius: int = 70) -> str:
    """Use a tighter window for role detection so a nearby component label does
    not inherit 'total project value' language from another amount in the row."""
    lo, hi = max(0, start - radius), min(len(scope), end + radius)
    return scope[lo:hi]


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
            raw = m.group(0).strip()
            # Row 7 is NSE's standardized consideration/size row, so a bare
            # "Crore" value is acceptable there. Elsewhere require explicit
            # INR/Rupee denomination + strong order-value context.
            if not _money_candidate_is_safe(raw, context, standardized_row=(scope_score == 2)):
                continue
            value_cr = _money_to_cr(m.group(1), m.group(2))
            if value_cr is not None and value_cr > 0:
                role_context = _nearest_role_context(scope, m.start(), m.end())
                role_score, role = _order_amount_role_score(raw, role_context)
                # v4.1: component economics (TDR, land premium, PBG, advance,
                # tranche, etc.) must never populate the headline order_value_cr.
                # If no total/headline candidate exists, leave order_value_cr blank.
                if role == "component":
                    continue
                candidates.append((scope_score, role_score, (1 if strong else 0), value_cr, raw, context, role))
        if candidates and scope_score == 2:
            # Row 7 is authoritative, but it can contain both a total and its
            # components. Keep all row-7 candidates and rank by semantic role.
            break

    out = {}
    if candidates:
        # Standardized row first, then semantic role, then strong context.
        # Amount size is only the final tie-breaker.
        _, _, _, value_cr, raw, context, role = max(candidates, key=lambda x: (x[0], x[1], x[2], x[3]))
        out.update({
            "order_value_cr": value_cr,
            "order_value_text": raw,
            "order_value_role": role,
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


def _extract_money_candidates(clean: str) -> list[tuple[float, str, str]]:
    """Return plausible disclosed monetary amounts as (crore, raw, context)."""
    out = []
    for m in _MONEY_RE.finditer(clean or ""):
        value = _money_to_cr(m.group(1), m.group(2))
        if value is None or value <= 0:
            continue
        lo, hi = max(0, m.start() - 180), min(len(clean), m.end() + 220)
        raw = m.group(0).strip()
        context = _clean_field(clean[lo:hi], 520)
        if not _money_candidate_is_safe(raw, context, standardized_row=False):
            continue
        out.append((value, raw, context))
    return out


def _best_money(clean: str, context_re: str = "") -> tuple[float | None, str, str]:
    candidates = _extract_money_candidates(clean)
    if not candidates:
        return None, "", ""
    if context_re:
        contextual = [x for x in candidates if re.search(context_re, x[2], re.I)]
        if not contextual:
            return None, "", ""
        candidates = contextual
    # Largest amount is used only after currency + context validation.
    value, raw, context = max(candidates, key=lambda x: x[0])
    return value, raw, context


def _extract_local_catalyst_details(category: str, text: str) -> dict:
    """Conservative non-AI parser for material catalyst PDFs.

    Only stores fields that can be recovered directly from machine-readable text.
    Missing/ambiguous facts are intentionally left blank.
    """
    clean = _normalize_pdf_text(text)
    if not clean:
        return {}
    out = {}

    if category == "Order":
        return _extract_order_details(clean)

    if category == "Acquisition":
        value, raw, _ = _best_money(clean, r"consideration|purchase price|transaction value|acquisition|acquir")
        if value is not None:
            out["transaction_value_cr"] = value
            out["transaction_value_text"] = raw
        pct = re.search(r"(?:acquir(?:e|ed|ing)|purchase|stake|shareholding)[^.%]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*%", clean, re.I)
        if not pct:
            pct = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%[^.]{0,100}?(?:stake|shareholding|equity)", clean, re.I)
        if pct:
            out["stake_acquired_pct"] = float(pct.group(1))
        post = re.search(r"(?:post[- ]?(?:acquisition|transaction)|after (?:the )?acquisition)[^.%]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*%", clean, re.I)
        if post:
            out["post_transaction_stake_pct"] = float(post.group(1))
        target = re.search(r"(?:acquisition of|acquire(?:d|s|ing)?(?: up to)?(?: an?)?(?: additional)?(?: \d+(?:\.\d+)?\s*%)?(?: equity shares? in| stake in)?)\s+([A-Z][A-Za-z0-9&.,'()\- ]{2,100}?)(?=\s+(?:for|from|through|by|at|pursuant|vide|which|,|\())", clean)
        if target:
            out["target"] = _clean_field(target.group(1), 120)

    elif category == "Divestment":
        value, raw, _ = _best_money(clean, r"consideration|sale value|transaction value|sale|disposal|divest")
        if value is not None:
            out["transaction_value_cr"] = value
            out["transaction_value_text"] = raw
        pct = re.search(r"(?:sale|sell|sold|disposal|divestment|transfer)[^.%]{0,130}?([0-9]+(?:\.[0-9]+)?)\s*%", clean, re.I)
        if pct:
            out["stake_sold_pct"] = float(pct.group(1))
        buyer = re.search(r"(?:buyer|purchaser|transferee)\s*[:\-]?\s*([A-Z][A-Za-z0-9&.,'()\- ]{2,120}?)(?=\s{2,}|\.|;|,\s*(?:for|at|pursuant))", clean, re.I)
        if buyer:
            out["buyer"] = _clean_field(buyer.group(1), 120)

    elif category == "Negative":
        # Negative disclosures need type/stage even when no reliable amount exists.
        # This remains fully local: machine-readable PDF text + deterministic regex.
        if re.search(r"\b(?:sfio|serious fraud investigation office)\b", clean, re.I):
            out["negative_type"] = "SFIO Investigation"
            out["negative_stage"] = "Investigation / Notice"
        elif re.search(r"\b(?:enforcement directorate|\bed\b|cbi|central bureau of investigation)\b", clean, re.I):
            out["negative_type"] = "Regulatory Investigation"
            out["negative_stage"] = "Investigation / Notice"
        elif re.search(r"show[ -]?cause", clean, re.I):
            out["negative_type"] = "Show Cause Notice"
            out["negative_stage"] = "Notice"
        elif re.search(r"\b(?:gst|income tax|tax authority|tax demand)\b", clean, re.I):
            out["negative_type"] = "Tax / GST"
            out["negative_stage"] = "Demand / Order" if re.search(r"demand|order", clean, re.I) else "Notice"
        elif re.search(r"penalty|\bfine\b", clean, re.I):
            out["negative_type"] = "Penalty / Fine"
            out["negative_stage"] = "Order / Penalty"
        elif re.search(r"litigation|dispute|court|tribunal|arbitration", clean, re.I):
            out["negative_type"] = "Litigation / Dispute"
            out["negative_stage"] = "Update"
        elif re.search(r"search and seizure|\braid\b", clean, re.I):
            out["negative_type"] = "Search / Raid"
            out["negative_stage"] = "Investigation"
        elif re.search(r"fire|accident|shutdown|plant closure", clean, re.I):
            out["negative_type"] = "Operational Incident"
            out["negative_stage"] = "Incident"
        elif re.search(r"liquidation", clean, re.I):
            out["negative_type"] = "Insolvency / Liquidation"
            out["negative_stage"] = "Liquidation"
        elif re.search(r"resolution plan", clean, re.I):
            out["negative_type"] = "Insolvency / Resolution Plan"
            out["negative_stage"] = "Decision"
        elif re.search(r"\bcirp\b|insolvency", clean, re.I):
            out["negative_type"] = "Insolvency / CIRP"
            out["negative_stage"] = "Material Update"

        value, raw, context = _best_money(clean, r"penalty|demand|fine|tax|claim|litigation|show cause|order")
        if value is not None:
            out["amount_cr"] = value
            out["amount_text"] = raw
            out["amount_context"] = context
        authority = re.search(r"(?:authority|regulator|department|issued by|order (?:passed|received) from)\s*[:\-]?\s*([A-Z][A-Za-z0-9&.,'()\-/ ]{3,120}?)(?=\.|;|\n| dated | vide )", clean, re.I)
        if authority:
            out["authority"] = _clean_field(authority.group(1), 120)

    elif category == "Strategic Agreement":
        value, raw, _ = _best_money(clean, r"investment|project|agreement|consideration|contract|value")
        if value is not None:
            out["agreement_value_cr"] = value
            out["agreement_value_text"] = raw
        if re.search(r"non[- ]binding", clean, re.I):
            out["binding_status"] = "Non-Binding"
        elif re.search(r"definitive agreement|binding agreement|executed.*agreement|agreement.*executed", clean, re.I):
            out["binding_status"] = "Binding / Definitive"
        cp = re.search(r"(?:agreement|mou|memorandum of understanding|collaboration|partnership)\s+(?:with|between)\s+([A-Z][A-Za-z0-9&.,'()\- ]{2,120}?)(?=\.|;|,\s*(?:for|to|and))", clean, re.I)
        if cp:
            out["counterparty"] = _clean_field(cp.group(1), 120)

    elif category == "Scheme of Arrangement":
        if re.search(r"demerger|hive[- ]?off", clean, re.I):
            out["scheme_type"] = "Demerger"
        elif re.search(r"merger|amalgamation", clean, re.I):
            out["scheme_type"] = "Merger / Amalgamation"
        eff = re.search(r"(?:effective date|appointed date|record date)\s*(?:is|shall be|:|-)?\s*([0-3]?\d[\-/ ][A-Za-z0-9\-/ ]{4,20})", clean, re.I)
        if eff:
            out["scheme_date_text"] = _clean_field(eff.group(1), 40)

    elif category == "Corporate Action":
        # A dividend/bonus/split PDF can contain large unrelated rupee figures
        # (paid-up capital, turnover, reserves, etc.).  Only fund-raise / buyback
        # style actions are allowed to populate issue_value_* fields.
        ca_head = clean[:2500]
        value_action = bool(re.search(
            r"\b(?:qip|qualified institutional placement|preferential issue|rights issue|buyback|fund ?raise|fundraising)\b",
            ca_head, re.I
        ))
        if value_action:
            value, raw, _ = _best_money(clean, r"issue size|fund raise|fundraise|qip|preferential|buyback|rights issue|consideration")
            if value is not None:
                out["issue_value_cr"] = value
                out["issue_value_text"] = raw
        ratio = re.search(r"(?:bonus|ratio|rights)[^\d]{0,50}(\d+)\s*[:/]\s*(\d+)", clean, re.I)
        if ratio:
            out["ratio"] = f"{ratio.group(1)}:{ratio.group(2)}"
        price = re.search(r"(?:issue price|floor price|buyback price)[^₹RsINR0-9]{0,30}(?:₹|Rs\.?|INR)?\s*([0-9][0-9,]*(?:\.\d+)?)", clean, re.I)
        if price:
            out["price_per_security"] = float(price.group(1).replace(',', ''))

    if out:
        out["detail_source"] = "local_pdf"
    return out


def _apply_materiality_ratios(it: dict, category: str, details: dict,
                               market_cap_map: dict | None, ttm_sales_map: dict | None) -> None:
    symbol = str(it.get("_lookup_symbol") or it.get("symbol") or "").strip().upper()
    if not symbol:
        return
    mcap = (market_cap_map or {}).get(symbol)
    ttm = (ttm_sales_map or {}).get(symbol)
    value_key = {
        "Order": "order_value_cr",
        "Acquisition": "transaction_value_cr",
        "Divestment": "transaction_value_cr",
        "Negative": "amount_cr",
        "Strategic Agreement": "agreement_value_cr",
        "Corporate Action": "issue_value_cr",
    }.get(category)
    if not value_key or details.get(value_key) is None:
        return
    try:
        value = float(details[value_key])
        prefix = {
            "Order": "order", "Acquisition": "transaction", "Divestment": "transaction",
            "Negative": "amount", "Strategic Agreement": "agreement", "Corporate Action": "issue"
        }[category]
        if mcap is not None and float(mcap) > 0:
            it[f"{prefix}_to_market_cap_pct"] = round(value / float(mcap) * 100.0, 2)
        if category == "Order" and ttm is not None and float(ttm) > 0:
            it["order_to_ttm_sales_pct"] = round(value / float(ttm) * 100.0, 2)
    except (TypeError, ValueError, ZeroDivisionError):
        pass


def enrich_local_pdfs(session, new_items: dict, existing_ids: set[str], today: date,
                       initial_build: bool = False, market_cap_map: dict | None = None,
                       ttm_sales_map: dict | None = None) -> tuple[int, int]:
    """PDF enrichment with NO AI/API calls.

    New catalysts are checked immediately. Existing history can be backfilled in
    controlled batches by the caller; failed/scan-only PDFs are marked checked so
    they are not downloaded repeatedly.
    """
    supported = {"Order", "Acquisition", "Divestment", "Negative",
                 "Strategic Agreement", "Scheme of Arrangement", "Corporate Action"}
    checked = enriched = values_found = 0
    for items in new_items.values():
        for it in items:
            cat = it.get("category")
            if cat not in supported or it.get("id") in existing_ids:
                continue
            if initial_build and str(it.get("dt", ""))[:10] != today.isoformat():
                continue
            checked += 1
            pdf_bytes = _download_pdf_bytes(session, it.get("link", ""))
            if not pdf_bytes:
                it["local_pdf_checked"] = True
                continue
            pdf_text = _extract_pdf_text_bytes(pdf_bytes)
            details = _extract_local_catalyst_details(cat, pdf_text)
            it["local_pdf_checked"] = True
            it["local_parser_version"] = 4.1
            if details:
                it.update(details)
                _apply_materiality_ratios(it, cat, details, market_cap_map, ttm_sales_map)
                enriched += 1
                if any(k.endswith("_cr") for k in details):
                    values_found += 1
    if initial_build:
        print(f"  ⚡ Initial/rebuild mode → historical PDFs skipped; only {today.isoformat()} catalysts enriched")
    print(f"  ✓ Local PDF enrichment v4.1 (AI disabled) → checked={checked}, details_found={enriched}, value_found={values_found}")
    return checked, enriched



_LOCAL_ENRICHMENT_FIELDS = {
    "order_value_cr", "order_value_text", "order_value_role", "company_share_of_order_cr",
    "transaction_value_cr", "transaction_value_text", "stake_acquired_pct",
    "post_transaction_stake_pct", "target", "stake_sold_pct", "buyer",
    "amount_cr", "amount_text", "amount_context", "authority",
    "negative_type", "negative_stage",
    "agreement_value_cr", "agreement_value_text", "binding_status", "counterparty",
    "scheme_type", "scheme_date_text", "issue_value_cr", "issue_value_text",
    "ratio", "price_per_security", "detail_excerpt",
    "order_to_market_cap_pct", "order_to_ttm_sales_pct",
    "transaction_to_market_cap_pct", "amount_to_market_cap_pct",
    "agreement_to_market_cap_pct", "issue_to_market_cap_pct",
}


def _clear_local_enrichment(item: dict) -> None:
    """Remove only fields owned by the local parser; preserve NSE/event metadata."""
    for key in _LOCAL_ENRICHMENT_FIELDS:
        item.pop(key, None)
    if item.get("detail_source") in {"local_pdf", "pdf_local"}:
        item.pop("detail_source", None)


def revalidate_local_history(session, history: dict, market_cap_map: dict | None = None,
                             ttm_sales_map: dict | None = None) -> tuple[int, int, int]:
    """One-time v4.1 reparse/normalization of local-PDF enrichment.

    Old v1/v2 values may have been merged forward even after the parser became
    stricter.  Every locally enriched historical row is therefore reparsed once
    with v4.1.  The version marker prevents repeat downloads on later runs.
    Gemini/manual enrichment is never touched.
    """
    checked = changed = values = 0
    supported = {"Order", "Acquisition", "Divestment", "Negative",
                 "Strategic Agreement", "Scheme of Arrangement", "Corporate Action"}
    for sym, items in history.items():
        for it in items:
            if it.get("manual") or it.get("category") not in supported:
                continue
            source = it.get("detail_source")
            if source not in {None, "local_pdf", "pdf_local"}:
                continue
            if not it.get("local_pdf_checked") and source not in {"local_pdf", "pdf_local"}:
                continue
            if float(it.get("local_parser_version") or 0) >= 4.1:
                continue

            checked += 1
            before = {k: it.get(k) for k in _LOCAL_ENRICHMENT_FIELDS if k in it}
            pdf_bytes = _download_pdf_bytes(session, it.get("link", ""))
            _clear_local_enrichment(it)
            it["local_pdf_checked"] = True
            it["local_parser_version"] = 4.1

            if pdf_bytes:
                pdf_text = _extract_pdf_text_bytes(pdf_bytes)
                details = _extract_local_catalyst_details(it.get("category"), pdf_text)
                if details:
                    it.update(details)
                    it["_lookup_symbol"] = str(sym).strip().upper()
                    _apply_materiality_ratios(it, it.get("category"), details,
                                              market_cap_map, ttm_sales_map)
                    it.pop("_lookup_symbol", None)
                    if any(k.endswith("_cr") for k in details):
                        values += 1

            after = {k: it.get(k) for k in _LOCAL_ENRICHMENT_FIELDS if k in it}
            if before != after:
                changed += 1

    if checked:
        print(f"  ♻ Local PDF v4.1 history revalidation → checked={checked}, changed={changed}, value_found={values}")
    return checked, changed, values


def revalidate_negative_history(session, history: dict, market_cap_map: dict | None = None) -> tuple[int, int, int]:
    """One-time local Negative refresh. Other v4.1 category parsers stay frozen."""
    checked = changed = values = 0
    for sym, items in history.items():
        for it in items:
            if it.get("manual") or it.get("category") != "Negative":
                continue
            if int(it.get("negative_parser_version") or 0) >= 1:
                continue
            checked += 1
            before = {k: it.get(k) for k in ("amount_cr", "amount_text", "amount_context", "authority", "negative_type", "negative_stage") if k in it}
            pdf_bytes = _download_pdf_bytes(session, it.get("link", ""))
            it["negative_parser_version"] = 1
            if not pdf_bytes:
                continue
            details = _extract_local_catalyst_details("Negative", _extract_pdf_text_bytes(pdf_bytes))
            if details:
                # Replace only Negative-owned local fields; do not touch other category enrichment.
                for k in ("amount_cr", "amount_text", "amount_context", "authority", "negative_type", "negative_stage", "amount_to_market_cap_pct"):
                    it.pop(k, None)
                it.update(details)
                it["_lookup_symbol"] = str(sym).strip().upper()
                _apply_materiality_ratios(it, "Negative", details, market_cap_map, None)
                it.pop("_lookup_symbol", None)
                if details.get("amount_cr") is not None:
                    values += 1
            after = {k: it.get(k) for k in ("amount_cr", "amount_text", "amount_context", "authority", "negative_type", "negative_stage") if k in it}
            if before != after:
                changed += 1
    if checked:
        print(f"  ♻ Negative local history v1 → checked={checked}, changed={changed}, value_found={values}")
    return checked, changed, values


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




# ─────────────────────────────────────────────────────────────────────────────
# Conservative lifecycle consolidation
# ─────────────────────────────────────────────────────────────────────────────

_LIFECYCLE_CATEGORIES = {
    "Order", "Acquisition", "Divestment", "Scheme of Arrangement",
    "Strategic Agreement", "Corporate Action",
}

_LIFECYCLE_STOP = {
    "limited", "company", "exchange", "informed", "regarding", "about", "under",
    "pursuant", "regulation", "sebi", "listing", "obligations", "disclosure",
    "requirements", "general", "updates", "update", "press", "release", "outcome",
    "board", "meeting", "held", "dated", "the", "and", "for", "with", "from",
    "that", "this", "has", "have", "its", "their", "private", "ltd",
}

def _life_tokens(item: dict) -> set[str]:
    raw = f"{item.get('subject','')} {item.get('text','')}".lower()
    raw = re.sub(r"https?://\S+", " ", raw)
    toks = set(re.findall(r"[a-z][a-z0-9]{2,}", raw))
    return {t for t in toks if t not in _LIFECYCLE_STOP}

def _money_markers(item: dict) -> set[str]:
    raw = f"{item.get('text','')} {item.get('order_value_text','')}".lower().replace(",", "")
    vals = set()
    for m in re.finditer(r"(?:rs\.?|₹|inr)?\s*(\d+(?:\.\d+)?)\s*(?:crore|crores|cr\b)", raw, re.I):
        try:
            vals.add(f"{float(m.group(1)):.2f}")
        except Exception:
            pass
    if item.get("order_value_cr") is not None:
        try: vals.add(f"{float(item['order_value_cr']):.2f}")
        except Exception: pass
    return vals

def _life_similarity(a: dict, b: dict) -> float:
    ta, tb = _life_tokens(a), _life_tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(1, len(ta | tb))

def _same_lifecycle(a: dict, b: dict) -> bool:
    cat = a.get("category")
    if cat != b.get("category") or cat not in _LIFECYCLE_CATEGORIES:
        return False
    try:
        da = datetime.fromisoformat(str(a.get("dt", "")).replace("Z", "+00:00"))
        db = datetime.fromisoformat(str(b.get("dt", "")).replace("Z", "+00:00"))
        gap = abs((da.date() - db.date()).days)
    except Exception:
        gap = 999
    if gap > 14:
        return False

    sim = _life_similarity(a, b)
    ma, mb = _money_markers(a), _money_markers(b)

    # Orders are especially collision-prone: same value (L1 -> award) or very
    # strong project/customer wording is required. Different order values never merge.
    if cat == "Order":
        if ma and mb and not (ma & mb):
            return False
        return bool(ma & mb) or sim >= 0.62

    # Scheme filings often use generic exchange boilerplate; same-symbol filings
    # close in time are consolidated only when their meaningful wording overlaps.
    if cat == "Scheme of Arrangement":
        return sim >= 0.30

    # Capital-action duplicates (Outcome + QIP/Preferential/Buyback etc.) are
    # normally filed minutes apart. Restrict the looser rule to the same day.
    if cat == "Corporate Action":
        return (gap == 0 and sim >= 0.22) or sim >= 0.58

    # Acquisition/divestment/strategic-agreement lifecycle updates normally repeat
    # the target/counterparty/project name, so require meaningful token overlap.
    return sim >= 0.42

_STAGE_RANK = {
    "L1 / Awaiting Award": 10, "Announced": 10, "MoU": 10, "Non-Binding MoU": 5,
    "Approved": 20, "Board Approved": 20, "Approved / Agreement": 25,
    "Agreement Signed": 30, "Definitive / Signed": 30, "Exchange NOC": 35,
    "Awarded": 40, "NCLT Approved": 45, "Record Date": 50,
    "Completion Delayed/Extended": 55, "Effective / Completed": 60, "Completed": 60,
}

def _lifecycle_winner(a: dict, b: dict) -> tuple[dict, dict]:
    ra = _STAGE_RANK.get(a.get("stage"), 0)
    rb = _STAGE_RANK.get(b.get("stage"), 0)
    if ra != rb:
        return (a, b) if ra > rb else (b, a)
    return (a, b) if str(a.get("dt", "")) >= str(b.get("dt", "")) else (b, a)

def _carry_enrichment(winner: dict, loser: dict) -> None:
    """Keep useful structured facts when a later lifecycle filing is terse."""
    protected = {"id", "dt", "react_date", "session", "category", "subject", "text", "link", "event_type", "stage"}
    for k, v in loser.items():
        if k not in protected and k not in winner and v not in (None, "", [], {}):
            winner[k] = v

def consolidate_lifecycles(data: dict) -> int:
    """Conservatively collapse duplicate stages of the same underlying event per symbol.

    Returns the number of cards removed. Manual rows are never consolidated.
    """
    removed = 0
    for sym in list(data):
        items = sorted(data[sym], key=lambda x: x.get("dt", ""), reverse=True)
        kept = []
        for item in items:
            if item.get("manual") or item.get("category") not in _LIFECYCLE_CATEGORIES:
                kept.append(item)
                continue
            hit = None
            for i, prev in enumerate(kept):
                if prev.get("manual"):
                    continue
                if _same_lifecycle(item, prev):
                    hit = i
                    break
            if hit is None:
                kept.append(item)
                continue
            winner, loser = _lifecycle_winner(item, kept[hit])
            _carry_enrichment(winner, loser)
            kept[hit] = winner
            removed += 1
        kept.sort(key=lambda x: x.get("dt", ""), reverse=True)
        data[sym] = kept
        if not kept:
            del data[sym]
    return removed


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

    # Reparse legacy local-PDF fields once with the current strict rules.
    # This removes stale v1/v2 false values that would otherwise survive merge.
    revalidate_local_history(r2_session, history, market_cap_map, ttm_sales_map)

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
            if not cat or _is_cirp_procedural_item(x):
                removed_noise += 1
                continue
            if x.get("category") != cat:
                x["category"] = cat
                reclassified += 1
            # Refresh deterministic stage/type metadata on retained history.
            for k in ("event_type", "stage"):
                x.pop(k, None)
            x.update(_event_meta(x.get("subject", ""), x.get("text", ""), cat))
            cleaned.append(x)
        history[sym] = cleaned
        if not history[sym]:
            del history[sym]
    if removed_noise or reclassified:
        print(f"  🧹 Historical cleanup → removed={removed_noise}, reclassified={reclassified}")

    # Negative-only local refresh. This does not alter the frozen v4.1 parsers
    # for Orders, Acquisition, Corporate Action, etc.
    revalidate_negative_history(r2_session, history, market_cap_map)

    existing_ids = {x.get("id") for items in history.values() for x in items if x.get("id")}
    initial_build = not bool(existing_ids)

    new_items, source = fetch_catalysts(
        nse_session, today, HISTORY_DAYS, is_trading_day, next_trading_day
    )
    fetched = sum(len(v) for v in new_items.values())

    # NSE often gives CIRP filings a generic summary, so use the PDF filename/link
    # as an additional deterministic signal for clearly procedural fresh rows.
    fresh_procedural_removed = 0
    for _sym in list(new_items):
        _kept = []
        for _it in new_items[_sym]:
            if _it.get("category") == "Negative" and _is_cirp_procedural_item(_it):
                fresh_procedural_removed += 1
                continue
            _kept.append(_it)
        if _kept:
            new_items[_sym] = _kept
        else:
            del new_items[_sym]
    if fresh_procedural_removed:
        print(f"  🧹 Fresh CIRP procedural cleanup → removed={fresh_procedural_removed}")

    # Stamp the group symbol temporarily so enrichment can join to the R2
    # classification/fundamentals lookups without changing the stored schema.
    for _sym, _items in new_items.items():
        for _it in _items:
            _it["_lookup_symbol"] = str(_sym).strip().upper()

    # Local-only PDF enrichment. No Gemini/AI request is made anywhere in this path.
    enrich_local_pdfs(
        nse_session, new_items, existing_ids, today, initial_build,
        market_cap_map, ttm_sales_map
    )
    for _items in new_items.values():
        for _it in _items:
            if _it.get("category") == "Negative" and _it.get("local_pdf_checked"):
                _it["negative_parser_version"] = 1

    # Internal join key must never be persisted.
    for _items in new_items.values():
        for _it in _items:
            _it.pop("_lookup_symbol", None)

    added = merge_catalysts(history, new_items, today, HISTORY_DAYS)

    # Collapse duplicate lifecycle filings only after old + fresh rows are merged,
    # so L1 -> award, announced -> completed, and scheme stage updates can meet.
    lifecycle_removed = consolidate_lifecycles(history)
    if lifecycle_removed:
        print(f"  🔗 Lifecycle consolidation → removed={lifecycle_removed} duplicate stage card(s)")

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
