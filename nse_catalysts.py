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
BACKFILL_PDF_BATCH = 10    # max never-opened history PDFs parsed per run (local parser, no AI)
BACKFILL_MAX_ATTEMPTS = 3  # failed downloads retried on later runs before giving up

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
    r"disclosure.*regulation\s*(?:10\s*\(?[56]\)?|29|31)\s*\(?[12]?\)?|"
    r"regulation\s*(?:10\s*\(?[56]\)?|29\s*\(?2\)?|31)|"
    r"substantial acquisition of shares and takeovers[^.]{0,35}regulations|\bsast\b", re.I)
_PROMOTER_MPS_SALE = re.compile(
    r"sale of (?:equity )?shares by (?:a )?promoter.*(?:open market|minimum public shareholding)|"
    r"promoter.*(?:minimum public shareholding|\bmps\b)", re.I)

# Incorporating/funding one's own subsidiary is not an external acquisition catalyst.
_SUBSIDIARY_INCORPORATION = re.compile(
    r"incorporation of (?:a |an |one or more |[a-z0-9 -]+ )?(?:wholly owned |step[- ]down )?subsidiar(?:y|ies)|"
    r"incorporat(?:e|ed|ion).*\b(?:wos|wholly[- ]owned subsidiar(?:y|ies)|step[- ]down subsidiar(?:y|ies))\b", re.I)
_INTERNAL_SUB_INVESTMENT = re.compile(
    r"(?:additional )?(?:investment|invested).*?(?:wholly[- ]owned subsidiar(?:y|ies)|\bwos\b)|"
    r"(?:subscription|subscribe|subscribed).*?(?:rights issue|equity shares|share capital|preference shares|warrants).*?(?:wholly[- ]owned subsidiar(?:y|ies)|\bwos\b)|"
    r"(?:wholly[- ]owned subsidiar(?:y|ies)|\bwos\b).*?(?:rights issue|additional investment|capital infusion|subscription|subscribe|subscribed)|"
    r"(?:investment|invested) (?:in|into)?.*?(?:wholly[- ]owned subsidiar(?:y|ies)|\bwos\b)", re.I)

# Batch-1 precision rules: acquisition/divestment/strategic agreement.
# These are deliberately summary-text rules so obvious exchange disclosures are
# resolved before any PDF enrichment.
_ACQ_ROUTINE_INTERNAL = re.compile(
    r"(?:acquisition|acquire|subscription|investment).*?(?:equity shares|share capital|rights issue).*?"
    r"(?:wholly[- ]owned subsidiary|\bwos\b)|"
    r"(?:wholly[- ]owned subsidiary|\bwos\b).*?(?:acquisition|acquire|subscription|investment).*?"
    r"(?:equity shares|share capital|rights issue)|"
    r"apportionment of (?:the )?cost of acquisition", re.I)
_ACQ_DILUTION = re.compile(
    r"dilution of (?:the )?(?:company['’ ]s )?shareholding|non[- ]participation in (?:the )?rights issue|"
    r"shareholding.*(?:has been|is|was|will be|stands)\s+(?:reduced|diluted)|ceased to be .*subsidiary", re.I)
_STRATEGIC_TO_ORDER = re.compile(
    r"\bdeal win\b|(?:agreement|partnership).*?(?:customer contract|customer win|order awarded)", re.I)
_STRATEGIC_TO_DIVEST = re.compile(
    r"(?:mou|agreement).*?(?:sale of entire|sale of .*stake|sale of .*shareholding|disinvestment)|"
    r"extinguishment of .*shares.*(?:buyback|joint venture)", re.I)
_JV_STRATEGIC = re.compile(
    r"(?:investment|equity investment|subscription).*?(?:joint venture|\bjv\b)|"
    r"(?:joint venture|\bjv\b).*?(?:investment|equity investment|subscription|incorporation)", re.I)

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
    r"dispatch.*(?:buyback|rights)|trading approval.*(?:bonus|split|rights|preferential)|"
    r"record date.*dividend|dividend.*record date|"
    r"(?:payment|credit|remittance).*dividend|dividend.*(?:payment|credit|remittance)|"
    # PSU press releases about handing the dividend cheque to the Government.
    r"\bpa(?:ys|id|ying)\b.{0,60}dividend.{0,40}(?:government|\bgoi\b|president of india|ministry)|"
    r"dividend (?:cheque|warrant).{0,60}(?:government|\bgoi\b|minister|ministry)", re.I)

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
    r"trading[ _-]?window|vacation[ _-]?(?:of[ _-]?)?(?:office|director)|"
    # Shareholder-meeting paperwork filed under the CIRP subject (notice,
    # proceedings, voting results, e-voting, book closure, postal ballot).
    r"(?:annual|extra[ _-]?ordinary)[ _-]?general[ _-]?meeting|\b[ae]gm\b|"
    r"e[ _-]?voting|book[ _-]?closure|postal[ _-]?ballot", re.I)

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
    r"(?:petition|second motion petition).*(?:admitted|admission)|"
    r"(?:filing|submission).*?(?:petition|application).*?(?:scheme|merger|demerger|amalgamation)|"
    r"(?:scheme|merger|demerger|amalgamation).*?(?:filing|submission).*?(?:petition|application)|"
    # NSE summaries of court-convened meeting notices often omit the word "scheme"
    # and only say the meeting is being held per the NCLT's order.
    r"(?=.*(?:shareholders?|creditors?))(?:notice|convening).{0,60}meeting.*?"
    r"(?:nclt|nclat|national company law|tribunal|hon['’]?ble)", re.I)

# A court/tribunal/authority order is a legal event, never a business order win.
# Allows up to four words between "order from/of" and the forum name
# (e.g. "Order from Hon'ble Delhi High Court").
_JUDICIAL_ORDER = re.compile(
    r"\border(?:s)?\s+(?:of|from|by|passed by|issued by|dated\s+\S+\s+(?:of|from|by))\s+(?:the\s+)?"
    r"(?:[\w.'’()-]+\s+){0,4}?"
    r"(?:hon['’]?ble|honourable|nclt|nclat|national company law|"
    r"(?:high|supreme|district|commercial|sessions) court|court\b|"
    r"(?:securities )?appellate tribunal|arbitral tribunal|tribunal|\bitat\b|\bcestat\b|\bdrt\b|\bdrat\b|"
    r"consumer (?:forum|commission)|competition commission|\bcci\b)", re.I)
# A court can also be a customer (e.g. an IT/services contract from a court registry).
# Explicit procurement wording overrides the judicial reading.
_JUDICIAL_ORDER_BUSINESS = re.compile(
    r"awarding of order|bagging|letter of (?:award|acceptance|intent)|\blo[ai]\b|work order|purchase order|"
    r"supply order|\bfor (?:the )?(?:supply|design|construction|installation|development|provision|"
    r"implementation|maintenance)\b|\b(?:it|software|consulting) services\b|\bservices? contract\b", re.I)

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

# Batch-3 precision: distinguish genuine regulatory grants from adverse licence actions
# and routine exchange/administrative approvals. Adverse action always wins.
_REGULATORY_ADVERSE = re.compile(
    r"(?:suspension|suspended|cancel(?:lation|led)|revocation|revoked|withdrawal|withdrawn|surrender).*?"
    r"(?:licen[cs]e|registration|regulatory approval|certificate)|"
    r"(?:licen[cs]e|registration|regulatory approval|certificate).*?"
    r"(?:suspension|suspended|cancel(?:lation|led)|revocation|revoked|withdrawal|withdrawn|surrender)", re.I)
_REGULATORY_ROUTINE = re.compile(
    r"(?:in[- ]principle|trading|listing) approval.*?(?:shares|securities|allotment|esop|bonus|rights|preferential)|"
    r"approval.*?(?:listing|trading).*?(?:shares|securities|allotment)|"
    r"exchange approval.*?(?:allotment|listing|trading)|"
    r"cancel(?:lation|led).*?(?:employee stock options?|esop)|"
    r"(?:employee stock options?|esop).*?cancel(?:lation|led)", re.I)

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
_ORDER_CANCEL = re.compile(
    r"(?:order|contract|letter of (?:award|acceptance|intent)|\blo[ai]\b).*(?:cancelled|canceled|terminated|withdrawn|annulled|short[- ]?closed)|"
    r"(?:cancell?ation|termination|withdrawal|annulment|short[- ]?closure).*(?:order|contract|letter of (?:award|acceptance|intent)|\blo[ai]\b)", re.I)

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
        elif re.search(r"letter of intent|\bloi\b", both, re.I) and not re.search(
                r"letter of award|letter of acceptance|\bloa\b|work order|purchase order", both, re.I):
            out.update(event_type="Order Award", stage="Letter of Intent")
        elif re.search(
                r"letter of award|letter of acceptance|\bloa\b|awarded|bagging/receiving|bagging|"
                r"awarding of order|notification of award|order received|work order|purchase order|"
                r"supply order|contract win|receipt of (?:an? )?order|receiv(?:e|ed|es|ing) (?:an? )?order|"
                r"secured (?:an? |the )?(?:order|contract)|order (?:of|for|from|worth|valued)\b|"
                r"contract (?:of|for|from|worth)\b|orders? wins?", both, re.I):
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
    elif category == "Corporate Action":
        if re.search(r"buy ?back", both, re.I): out["event_type"] = "Buyback"
        elif re.search(r"bonus", both, re.I): out["event_type"] = "Bonus"
        elif re.search(r"stock split|sub-division", both, re.I): out["event_type"] = "Stock Split"
        elif re.search(r"rights issue", both, re.I): out["event_type"] = "Rights Issue"
        elif re.search(r"qualified institutional|\bqip\b", both, re.I): out["event_type"] = "QIP"
        elif re.search(r"preferential issue", both, re.I): out["event_type"] = "Preferential Issue"
        elif re.search(r"dividend", both, re.I): out["event_type"] = "Dividend"
        else: out["event_type"] = "Corporate Action"
        if re.search(r"\bclos(?:ed|ure)\b|completed|completion", both, re.I): out["stage"] = "Completed"
        elif re.search(r"record date", both, re.I): out["stage"] = "Record Date"
        elif re.search(r"allot(?:ted|ment)", both, re.I): out["stage"] = "Allotment"
        elif re.search(r"\brecommend(?:ed|s|ation)?\b|subject to (?:the )?(?:approval|consent) of (?:the )?(?:shareholders|members)", both, re.I):
            out["stage"] = "Board Recommended"     # shareholder approval still pending
        elif re.search(r"approved|approval|outcome of board meeting", both, re.I): out["stage"] = "Approved"
        else: out["stage"] = "Announced"
    elif category == "Regulatory Approval":
        out["event_type"] = "Regulatory Approval"
        if re.search(r"renewal|renewed", both, re.I): out["stage"] = "Renewed"
        elif re.search(r"grant|granted|receipt|received|obtained|certificate of registration", both, re.I): out["stage"] = "Granted / Received"
        else: out["stage"] = "Approved"
    elif category == "Management Change":
        out["event_type"] = "Management Change"
        if re.search(r"resignation|resigned|cessation|retirement|vacation of office", both, re.I): out["stage"] = "Exit"
        elif re.search(r"appointment|appointed|reappointment|re-appointed", both, re.I): out["stage"] = "Appointment"
    return out


def is_explicit_noise(subject: str, text: str) -> bool:
    """True when a backend noise rule positively matches (results, SAST, CIRP/scheme
    paperwork, routine allotments, debt, ignored subjects, ...)."""
    subject = (subject or "").strip()
    text = (text or "").strip()
    both = f"{subject} {text}"
    if _RESULTS.search(both) or (re.search(r"outcome of board meeting", subject, re.I) and _RESULTS.search(text)):
        return True
    if (_BACKEND_NOISE.search(both) or _ROUTINE_PROFESSIONAL.search(both) or
            _ROUTINE_ALLOTMENT.search(both) or _ROUTINE_CORP_ACTION.search(both) or
            _SAST_NOISE.search(both) or _PROMOTER_MPS_SALE.search(both) or
            _SUBSIDIARY_INCORPORATION.search(both) or _INTERNAL_SUB_INVESTMENT.search(both) or
            _CIRP_PROCEDURAL.search(both) or _SCHEME_PROCEDURAL.search(both) or
            _REGULATORY_ROUTINE.search(both)):
        return True
    return bool(_IGNORE_SUBJECT.search(subject) or _DEBT.search(both))


def classify(subject: str, text: str) -> str | None:
    """Trader-focused catalyst category, or None when the event should not be stored."""
    subject = (subject or "").strip()
    text = (text or "").strip()
    both = f"{subject} {text}"

    if is_explicit_noise(subject, text):
        return None

    # Batch-3: adverse licence/registration action must beat the broad NSE subject
    # "granting/withdrawal/surrender/cancellation/suspension". Only an explicit
    # positive grant/receipt/renewal is a Regulatory Approval catalyst.
    # NSE's subject taxonomy itself contains the words withdrawal/cancellation/
    # suspension even for a positive receipt. Prefer the actual announcement text
    # when it explicitly says a licence/registration was granted or received.
    if _REGULATORY_GRANT.search(text) and not _REGULATORY_ADVERSE.search(text):
        return "Regulatory Approval"
    if _REGULATORY_ADVERSE.search(text):
        return "Negative"
    if _REGULATORY_GRANT.search(both):
        return "Regulatory Approval"
    if _REGULATORY_ADVERSE.search(both):
        return "Negative"
    if _MANAGEMENT_CHANGE.search(both):
        return "Management Change"

    # A substantive merger/demerger/amalgamation/NCLT scheme filing must beat the
    # broad Negative phrase "order(s) passed". Procedural scheme noise was already
    # rejected above by _SCHEME_PROCEDURAL.
    if _SCHEME.search(both) and re.search(r"(?:scheme|merger|demerger|amalgamation|nclt|nclat)", both, re.I):
        return "Scheme of Arrangement"

    # A court/tribunal order (NCLAT, High Court, ITAT, ...) is a legal outcome.
    # Route it to Negative so the litigation parser can tag it adverse or relief;
    # without this, "Order from ... Tribunal" matched _ORDER as a business win.
    if _JUDICIAL_ORDER.search(both) and not _JUDICIAL_ORDER_BUSINESS.search(both):
        return "Negative"

    # Cancellation/termination of an order is adverse, never a fresh Order win.
    if _ORDER_CANCEL.search(both) or _ADVERSE_TAX_ORDER.search(both):
        return "Negative"
    if _NEGATIVE.search(both):
        return "Negative"

    # An explicit acquisition of customer contracts/business/assets remains an
    # Acquisition even though the acquired object contains the word "contract".
    if _ACQUISITION.search(both) and re.search(r"acquir(?:e|ed|ing).*?(?:customer contracts?|business|assets?)", both, re.I):
        return "Acquisition"

    # Do not promote mere tender participation into an Order catalyst.
    if _ORDER_PRE_BID.search(both):
        return None
    if _ORDER.search(both):
        return "Order"

    # Batch-1 cross-category resolution.  Economic substance beats NSE's broad
    # Acquisition/Agreement subject labels.
    if _ACQ_DILUTION.search(both) or _STRATEGIC_TO_DIVEST.search(both):
        return "Divestment"
    if _STRATEGIC_TO_ORDER.search(both):
        return "Order"
    if _JV_STRATEGIC.search(both):
        return "Strategic Agreement"
    if _ACQ_ROUTINE_INTERNAL.search(both):
        return None

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
    r"aggregate consideration|total consideration|sale value|buyback size|"
    # Narrative order totals: "orders totaling Rs 250.78 crores",
    # "wins new orders of Rs. 1,303 crores", "order worth ₹44 crore".
    # Must sit directly on the word order/contract, so "order book of Rs X"
    # or "YTD order intake of Rs X" do not qualify.
    r"(?:orders?|contracts?)\s+(?:totall?ing|aggregating(?:\s+to)?|amounting\s+to|worth|valued\s+at|"
    r"of\s+(?:approx\.?\s+|approximately\s+|about\s+)?(?:₹|rs\.?|inr))",
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
    # Join only runs of single glyph-split digits ("7 5 . 9 6" -> "75.96").
    # The old rule also glued a year to the next row number ("2027 7." -> "20277").
    s = re.sub(r"(?<![\d,.])\d(?:\s\d)+(?![\d,])", lambda m: m.group(0).replace(" ", ""), s)
    # Clean punctuation/hyphen spacing created by line-oriented extraction.
    s = re.sub(r"\s+([,.;:])", r"\1", s)
    # Thousand separators split by line wrapping inside an amount:
    # "Rs 7, 600 crores" was read as "600 crores". Only repaired right after a
    # currency marker so ordinary lists like "1, 2, 3" are untouched.
    _amt = re.compile(r"((?:₹|rs\.?|inr)\s*\d{1,3}(?:,\d{2,3})*),\s+(?=\d{2,3}\b)", re.I)
    while True:
        s2 = _amt.sub(r"\1,", s)
        if s2 == s:
            break
        s = s2
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


def _yes_no_answer(row: str) -> bool | None:
    """Answer of a SEBI Yes/No row. The question itself contains "If yes ...",
    so only the text after the question ("arm's length" / last '?') counts."""
    if not row:
        return None
    m = None
    for m in re.finditer(r"arm['’`s ]*\s*length[\"'”’.;:\s]*|\?", row, re.I):
        pass
    ans = row[m.end():] if m else row
    ans = ans.strip(" .:;-\"'”’")
    if not ans:
        return None
    if re.match(r"(?:no\b|nil\b|none\b|n\.?\s?a\.?\b|not applicable|not interested|does not|do not|is not|are not|not a\b|not fall)", ans, re.I):
        return False
    if re.match(r"yes\b", ans, re.I):
        return True
    if re.search(r"\b(?:does|do|is|are|shall|would) not\b.{0,40}related party|not (?:a )?related party|not fall", ans, re.I):
        return False
    return None


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
    _generic = re.compile(r"(?:as per\b.*|general (?:contract|condition)s?\b.*|standard (?:terms|conditions)\b.*|"
                          r"one[- ]time|letter of (?:award|acceptance|intent)|n\.?a\.?|not applicable|epc|supply|works?)\.?", re.I)
    if terms and not _generic.fullmatch(terms.strip()):
        purpose = terms
    elif nature and not _generic.fullmatch(nature.strip()):
        purpose = nature
    else:
        purpose = ""

    execution = _row_answer(
        rows.get(6, ""),
        r"time period by which (?:the )?order\(s\)\s*/?\s*contract\(s\) is to be executed",
    )

    if execution:
        # Stop at the next SEBI row if the table boundary was missed.
        execution = re.split(r"\s*(?:\b\d\.\s*)?(?:broad (?:commercial )?consideration|whether the|"
                             r"name of the entity)", execution, maxsplit=1, flags=re.I)[0].strip(" .;")
        if re.search(r"\b(?:19|20)\d{3,}\b", execution) or len(execution) < 3:
            execution = ""

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
    related_party = _yes_no_answer(rows.get(9, ""))

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
# Extraction needs little reasoning; thinking tokens bill at the output rate.
# Set GEMINI_THINKING_LEVEL="" to send no thinking config at all.
GEMINI_THINKING_LEVEL = os.environ.get("GEMINI_THINKING_LEVEL", "low").strip()
AI_ORDER_BATCH = int(os.environ.get("AI_ORDER_BATCH", "10"))     # max Gemini calls per run
AI_ORDER_MAX_ATTEMPTS = 2       # failed AI calls per row before giving up
AI_AGREE_TOL = 0.05             # local vs AI within 5% = confirmed
_GEMINI_RATE_LIMITED = False

_ORDER_AI_PROMPT = r"""
Read this Indian listed company's order/contract announcement PDF. Return ONLY this JSON, nothing else:
{"value_cr": number|null, "value_basis": "total"|"annual"|"not_disclosed", "tenure_years": number|null, "customer": string|null, "summary": string|null}
Rules:
- value_cr: the order/contract value in INR crore (convert rupees/lakh). If only a foreign currency is given, use the INR equivalent stated in the PDF, else null. If several orders, use the stated aggregate.
- Ignore order book, YTD order intake, revenue, turnover and market cap figures.
- value_basis "annual" when the PDF gives a yearly revenue/tariff for a multi-year period; then value_cr is the yearly figure and tenure_years the period. Otherwise tenure_years is null.
- customer: name of the entity that awarded the order; null if withheld.
- summary: one factual sentence under 25 words saying what work and for whom. No opinions.
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
                "maxOutputTokens": 1024,   # JSON is ~100 tokens; cap includes thinking
                "responseMimeType": "application/json",
            },
        }
        global _GEMINI_RATE_LIMITED
        url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
               f"{GEMINI_ORDER_MODEL}:generateContent?key={GEMINI_API_KEY}")
        if GEMINI_THINKING_LEVEL:
            payload["generationConfig"]["thinkingConfig"] = {"thinkingLevel": GEMINI_THINKING_LEVEL}
        r = session.post(url, json=payload, timeout=120)
        if r.status_code == 400 and "thinkingConfig" in payload["generationConfig"]:
            # Model does not accept this thinking setting; retry with its default.
            payload["generationConfig"].pop("thinkingConfig", None)
            r = session.post(url, json=payload, timeout=120)
        if r.status_code == 429:
            _GEMINI_RATE_LIMITED = True
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
        try:
            v = obj.get("value_cr")
            v = float(v) if v is not None else None
            if v is not None and 0 < v < 10_000_000:
                out["order_value_cr"] = round(v, 4)
        except (TypeError, ValueError):
            pass
        basis = str(obj.get("value_basis") or "").lower()
        if basis in {"total", "annual", "not_disclosed"}:
            out["value_basis"] = basis
        try:
            t = obj.get("tenure_years")
            t = float(t) if t is not None else None
            if t and 0 < t <= 50:
                out["tenure_years"] = t
        except (TypeError, ValueError):
            pass
        for src, dst in (("customer", "order_from"), ("summary", "order_summary")):
            v = obj.get(src)
            if isinstance(v, str) and v.strip() and v.strip().lower() not in {"null", "none", "n/a"}:
                out[dst] = v.strip()[:300]
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
        # Negative parser v1.3: precision-first.  Determine event semantics before
        # selecting money so project values / facilities / historical references
        # cannot become the primary adverse amount.
        relief = bool(re.search(
            r"(?:set aside|quashed|demand (?:has been |was )?(?:deleted|dropped|withdrawn)|"
            r"appeal (?:has been )?allowed|in favour of (?:the )?company|no (?:further )?liability|"
            r"proceedings? (?:has been |were )?(?:dropped|closed)|penalty (?:has been )?(?:waived|deleted))",
            clean, re.I))
        # "Set aside" is only a relief when what was set aside was AGAINST the company.
        # If an award in its favour was set aside, or the company says it will
        # appeal / challenge, the outcome is adverse.
        adverse = bool(re.search(
            r"set aside (?:the |an? )?(?:arbitral |arbitration )?award|award[^.]{0,120}?(?:is|was|has been|were|stands?) set aside|"
            r"strong case to challenge|(?:preferring|prefer|filing|file) an? (?:appeal|appropriate petition|review petition|special leave)|"
            r"intends? to (?:appeal|challenge)|taking appropriate legal steps",
            clean, re.I))
        if adverse:
            relief = False
        tax_ctx = re.search(r"\bgst\b(?!\s*(?:in|no\.?|number|:)?\s*\d)|income[- ]tax|tax demand|taxation|\bitat\b|assessment order|customs duty", clean, re.I)

        if re.search(r"\b(?:sfio|serious fraud investigation office)\b", clean, re.I):
            out["negative_type"] = "SFIO Investigation"
            out["negative_stage"] = "Investigation / Notice"
        elif re.search(r"\b(?:enforcement directorate|\bed\b|cbi|central bureau of investigation)\b", clean, re.I):
            out["negative_type"] = "Regulatory Investigation"
            out["negative_stage"] = "Investigation / Notice"
        elif relief and tax_ctx:
            out["negative_type"] = "Tax / Litigation Relief"
            out["negative_stage"] = "Relief / Set Aside"
        elif relief and re.search(r"litigation|dispute|court|tribunal|arbitrat", clean, re.I):
            out["negative_type"] = "Litigation Relief"
            out["negative_stage"] = "Relief / Favourable Outcome"
        elif re.search(r"arbitrat(?:ion|or|ral)|arbitral award", clean, re.I):
            out["negative_type"] = "Litigation / Arbitration"
            out["negative_stage"] = ("Adverse Order" if adverse else
                                     "Award / Order" if re.search(r"award|order", clean, re.I) else "Update")
        elif re.search(r"show[ -]?cause", clean, re.I):
            out["negative_type"] = "Show Cause Notice"
            out["negative_stage"] = "Notice"
        elif re.search(r"\b(?:gst|income tax|tax authority|tax demand|assessment order)\b", clean, re.I):
            out["negative_type"] = "Tax / GST"
            out["negative_stage"] = "Demand / Order" if re.search(r"demand|order", clean, re.I) else "Notice"
        elif re.search(r"penalty|\bfine\b", clean, re.I):
            out["negative_type"] = "Penalty / Fine"
            out["negative_stage"] = "Order / Penalty"
        elif re.search(r"litigation|dispute|court|tribunal", clean, re.I):
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

        # Role-aware money extraction.  A candidate is accepted only when its local
        # context describes an adverse monetary role; generic project/facility/
        # transaction values are ignored.
        candidates = _extract_money_candidates(clean)
        reject_role = re.compile(r"project (?:cost|value)|development cost|contract value|order value|"
                                 r"credit facility|loan facility|secured facility|charge (?:created|amount)|"
                                 r"turnover|revenue|net worth|share capital|consideration", re.I)
        role_hits = {"tax_demand": [], "penalty": [], "interest": [], "award": [], "claim": []}

        # v1.2: assign a monetary role only when the role is explicitly attached
        # to that amount. This prevents one figure from being copied into tax,
        # penalty and interest merely because those words occur later in a paragraph.
        def _explicit_money_role(raw: str, context: str, role: str) -> bool:
            eraw = re.escape(raw)
            if role == "tax_demand":
                pats = [
                    rf"(?:tax (?:demand|liability)|demand(?:ed|ing)? (?:tax|liability)|demand raised)[^.;:]{{0,55}}{eraw}",
                    rf"{eraw}[^.;:]{{0,28}}(?:tax )?demand\b",
                ]
            elif role == "penalty":
                pats = [
                    rf"(?:penalty|fine|penal amount)(?:\s+(?:of|amounting to|aggregating to|is|:|-))?[^.;:]{{0,35}}{eraw}",
                ]
            elif role == "interest":
                pats = [
                    rf"interest(?:\s+(?:of|amounting to|aggregating to|is|:|-))[^.;:]{{0,30}}{eraw}",
                ]
            elif role == "award":
                pats = [
                    rf"(?:arbitral award|arbitration|compensation|damages|award(?:ed)? amount)[^.;:]{{0,90}}{eraw}",
                    rf"{eraw}[^.;:]{{0,90}}(?:arbitral award|awarded by|compensation|damages)",
                ]
            else:  # claim / general litigation exposure
                pats = [
                    rf"(?:claim|dispute|litigation|show[ -]?cause|notice)[^.;:]{{0,90}}{eraw}",
                    rf"{eraw}[^.;:]{{0,55}}(?:claim|dispute|litigation)",
                ]
            return any(re.search(pat, context, re.I | re.S) for pat in pats)

        for cand in candidates:
            value, raw, context = cand
            if reject_role.search(context) and not re.search(r"penalty|tax demand|demand raised|arbitral award|compensation|damages", context, re.I):
                continue
            for role in role_hits:
                if _explicit_money_role(raw, context, role):
                    role_hits[role].append(cand)

        def _pick(role):
            vals = role_hits.get(role) or []
            return max(vals, key=lambda x: x[0]) if vals else None

        tax = _pick("tax_demand")
        pen = _pick("penalty")
        intr = _pick("interest")
        award = _pick("award")
        claim = _pick("claim")
        if tax:
            out["tax_demand_cr"], out["tax_demand_text"] = tax[0], tax[1]
        if pen:
            out["penalty_cr"], out["penalty_text"] = pen[0], pen[1]
        if intr:
            out["interest_cr"], out["interest_text"] = intr[0], intr[1]
        if award:
            out["award_amount_cr"], out["award_amount_text"] = award[0], award[1]

        # v1.3: a favourable order may refer to the historical demand/claim that
        # has just been set aside. Keep that amount only as a lifecycle reference;
        # it is not a current adverse exposure and must not populate amount_cr.
        if relief and candidates:
            relief_candidates = [c for c in candidates if re.search(
                r"set aside|quashed|deleted|dropped|withdrawn|appeal.{0,30}allowed|previously upheld|tax demand|demand",
                c[2], re.I | re.S)]
            if relief_candidates:
                ref = max(relief_candidates, key=lambda x: x[0])
                out["relief_reference_cr"] = ref[0]
                out["relief_reference_text"] = ref[1]

        # Primary amount represents the principal adverse exposure, not a sum that
        # could double-count overlapping disclosures. Keep separate role fields too.
        primary = None
        if out.get("negative_type") == "Litigation / Arbitration":
            primary = award or claim
        elif out.get("negative_type") in {"Tax / GST", "Show Cause Notice"}:
            primary = tax or pen or claim
        elif out.get("negative_type") == "Penalty / Fine":
            primary = pen or tax or claim
        elif out.get("negative_type") not in {"Tax / Litigation Relief", "Litigation Relief", "Insolvency / CIRP", "Insolvency / Resolution Plan", "Insolvency / Liquidation"}:
            primary = claim or pen or tax or award
        if primary:
            out["amount_cr"], out["amount_text"], out["amount_context"] = primary

        # Total exposure is emitted only when distinct tax + penalty amounts can be
        # identified. Interest is intentionally excluded because it is often open-ended.
        if tax and pen and abs(tax[0] - pen[0]) > 1e-9:
            out["total_exposure_cr"] = round(tax[0] + pen[0], 6)
        elif tax and pen and re.search(r"tax liability.{0,80}penalty|demand.{0,80}penalty", clean, re.I | re.S):
            # Same numerical amount can legitimately apply once as tax and once as penalty.
            out["total_exposure_cr"] = round(tax[0] + pen[0], 6)

        # Authority is whitelist/context based.  Never save fragments such as
        # 'Transcript', 'Letter', 'BSE Limited P', or arbitrary prose.
        auth_patterns = [
            r"Serious Fraud Investigation Office(?: \(SFIO\))?", r"Enforcement Directorate(?: \(ED\))?",
            r"Central Bureau of Investigation(?: \(CBI\))?", r"Securities and Exchange Board of India(?: \(SEBI\))?",
            r"Reserve Bank of India(?: \(RBI\))?", r"National Stock Exchange of India(?: Limited)?", r"BSE Limited",
            r"Income Tax Appellate Tribunal(?: \(ITAT\))?", r"Income Tax Department", r"National Faceless Assessment (?:Unit|Centre)",
            r"(?:Additional |Assistant |Joint |Deputy )?Commissioner(?: of Income Tax|,? CGST(?: & Central Excise)?| of GST)?",
            r"(?:CGST|SGST|GST) (?:Department|Authority|Officer|Commissionerate)",
            r"National Company Law Tribunal(?: \(NCLT\))?", r"National Company Law Appellate Tribunal(?: \(NCLAT\))?",
            r"Regional Provident Fund Commissioner(?: \(RPFC\))?",
        ]
        # Every filing names SEBI's regulations and is addressed to NSE/BSE; those
        # mentions are boilerplate, not the authority that acted.
        auth_text = re.sub(
            r"securities and exchange board of india\s*\((?:listing|prohibition|substantial|issue|share based|delisting)[^)]{0,120}\)\s*(?:regulations?)?(?:,?\s*\d{4})?|"
            r"sebi\s*\((?:listing|lodr|prohibition|substantial|issue|pit)[^)]{0,120}\)\s*(?:regulations?)?(?:,?\s*\d{4})?|"
            r"sebi (?:master )?circular[^.;]{0,120}", " ", clean, flags=re.I)
        exch_action = re.compile(
            r"(?:penalty|fine\b|show cause notice)[^.]{0,40}?(?:imposed|levied|issued)?\s*by\s+(?:the\s+)?"
            r"(?:national stock exchange|\bnse\b|bse limited|\bbse\b)|"
            r"(?:national stock exchange|\bnse\b|bse limited|\bbse\b)[^.]{0,60}"
            r"(?:imposed|levied|has fined|issued|initiated|suspended)", re.I)
        found_auth = []
        for pat in auth_patterns:
            if re.search(r"Stock Exchange|BSE", pat) and not exch_action.search(auth_text):
                continue
            m = re.search(pat, auth_text, re.I)
            if m:
                val = _clean_field(m.group(0), 120)
                if val.lower() not in {x.lower() for x in found_auth}:
                    found_auth.append(val)
        if found_auth:
            out["authority"] = "; ".join(found_auth[:2])

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


def _is_non_binding_mou(it: dict, details: dict | None = None) -> bool:
    status = str((details or {}).get("binding_status") or it.get("binding_status") or "")
    if re.search(r"non[- ]?binding", status, re.I):
        return True
    both = f"{it.get('subject', '')} {it.get('text', '')}"
    return bool(re.search(r"memorandum of understanding|\bmou\b", both, re.I)
                and not re.search(r"definitive|binding agreement", both, re.I))


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
    if category == "Strategic Agreement" and _is_non_binding_mou(it, details):
        # A non-binding MoU value (often the company's own capex pledge to a
        # state government) is not revenue; a % of market cap misleads.
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
    print(f"  ✓ Local PDF enrichment v4.1 → checked={checked}, details_found={enriched}, value_found={values_found}")
    return checked, enriched



def backfill_local_history(session, history: dict, market_cap_map: dict | None = None,
                           ttm_sales_map: dict | None = None,
                           limit: int = BACKFILL_PDF_BATCH) -> tuple[int, int, int]:
    """Open PDFs of stored catalysts that were never checked, newest first.

    enrich_local_pdfs() only sees fresh rows (and only today's rows on a
    rebuild), and revalidate_local_history() only reparses rows that were
    already checked. Rows that slipped past both would stay unenriched for
    ever; this drains that backlog `limit` PDFs per run with the local parser.
    Gemini, manual and already-enriched rows are never touched.
    """
    supported = {"Order", "Acquisition", "Divestment", "Negative",
                 "Strategic Agreement", "Scheme of Arrangement", "Corporate Action"}
    pending = []
    for sym, items in history.items():
        for it in items:
            if (it.get("manual") or it.get("local_pdf_checked") or it.get("detail_source")
                    or it.get("category") not in supported):
                continue
            pending.append((it.get("dt", ""), sym, it))
    if not pending:
        return 0, 0, 0
    # Never-tried rows first (newest first), then earlier failures, so one
    # unreachable PDF cannot block the queue for BACKFILL_MAX_ATTEMPTS runs.
    pending.sort(key=lambda t: t[0], reverse=True)
    pending.sort(key=lambda t: int(t[2].get("local_pdf_attempts") or 0))

    checked = enriched = values = 0
    consecutive_fail = 0
    for _dt, sym, it in pending[:max(0, limit)]:
        if consecutive_fail >= 3:
            # Session is probably blocked/throttled; stop and try again next run.
            print("  ⚠ History PDF backfill paused after 3 consecutive download failures")
            break
        cat = it["category"]
        link = it.get("link", "")
        checked += 1
        pdf_bytes = _download_pdf_bytes(session, link)
        if not pdf_bytes:
            is_pdf_link = bool(link) and link.lower().split("?", 1)[0].endswith(".pdf")
            attempts = int(it.get("local_pdf_attempts") or 0) + 1
            if not is_pdf_link or attempts >= BACKFILL_MAX_ATTEMPTS:
                # Nothing to open, or repeatedly unreachable: stop retrying.
                it.pop("local_pdf_attempts", None)
                it["local_pdf_checked"] = True
            else:
                it["local_pdf_attempts"] = attempts
            if is_pdf_link:
                consecutive_fail += 1
            continue
        consecutive_fail = 0
        details = _extract_local_catalyst_details(cat, _extract_pdf_text_bytes(pdf_bytes))
        it.pop("local_pdf_attempts", None)
        it["local_pdf_checked"] = True
        it["local_parser_version"] = 4.1
        if cat == "Negative":
            it["negative_parser_version"] = NEG_PARSER_VERSION
        if details:
            it.update(details)
            it["_lookup_symbol"] = str(sym).strip().upper()
            _apply_materiality_ratios(it, cat, details, market_cap_map, ttm_sales_map)
            it.pop("_lookup_symbol", None)
            enriched += 1
            if any(k.endswith("_cr") and v is not None for k, v in details.items()):
                values += 1
    remaining = sum(1 for _d, _s, x in pending if not x.get("local_pdf_checked"))
    print(f"  📥 History PDF backfill → checked={checked}, details_found={enriched}, "
          f"value_found={values}, remaining={remaining}")
    return checked, enriched, values


_LOCAL_ENRICHMENT_FIELDS = {
    "order_value_cr", "order_value_text", "order_value_role", "company_share_of_order_cr",
    "transaction_value_cr", "transaction_value_text", "stake_acquired_pct",
    "post_transaction_stake_pct", "target", "stake_sold_pct", "buyer",
    "amount_cr", "amount_text", "amount_context", "authority",
    "negative_type", "negative_stage", "tax_demand_cr", "tax_demand_text",
    "penalty_cr", "penalty_text", "interest_cr", "interest_text",
    "award_amount_cr", "award_amount_text", "total_exposure_cr",
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
    supported = {"Order", "Acquisition", "Divestment",
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


NEG_PARSER_VERSION = 1.5        # 1.5: adverse set-aside (award against company), GSTIN not tax, exchange authority only when it acted
NEG_REVALIDATE_BATCH = int(os.environ.get("NEG_REVALIDATE_BATCH", "40"))


def revalidate_negative_history(session, history: dict, market_cap_map: dict | None = None) -> tuple[int, int, int]:
    """One-time local Negative v1.3 refresh. Other v4.1 category parsers stay frozen."""
    checked = changed = values = 0
    for sym, items in history.items():
        for it in items:
            if it.get("manual") or it.get("category") != "Negative":
                continue
            try:
                neg_ver = float(it.get("negative_parser_version") or 0)
            except (TypeError, ValueError):
                neg_ver = 0.0
            if neg_ver >= NEG_PARSER_VERSION:
                continue
            if it.get("negative_type") == "Order Cancellation" or it.get("cancels") or it.get("category_override") == "Negative":
                it["negative_parser_version"] = NEG_PARSER_VERSION   # set by the order check
                continue
            if checked >= NEG_REVALIDATE_BATCH:
                break
            checked += 1
            neg_fields = ("amount_cr", "amount_text", "amount_context", "authority", "negative_type", "negative_stage",
                          "tax_demand_cr", "tax_demand_text", "penalty_cr", "penalty_text",
                          "interest_cr", "interest_text", "award_amount_cr", "award_amount_text",
                          "total_exposure_cr", "relief_reference_cr", "relief_reference_text", "amount_to_market_cap_pct")
            before = {k: it.get(k) for k in neg_fields if k in it}
            pdf_bytes = _download_pdf_bytes(session, it.get("link", ""))
            it["negative_parser_version"] = NEG_PARSER_VERSION
            if not pdf_bytes:
                continue
            details = _extract_local_catalyst_details("Negative", _extract_pdf_text_bytes(pdf_bytes))
            if details:
                # Replace only Negative-owned local fields; do not touch other category enrichment.
                for k in neg_fields:
                    it.pop(k, None)
                it.update(details)
                it["_lookup_symbol"] = str(sym).strip().upper()
                _apply_materiality_ratios(it, "Negative", details, market_cap_map, None)
                it.pop("_lookup_symbol", None)
                if details.get("amount_cr") is not None:
                    values += 1
            after = {k: it.get(k) for k in neg_fields if k in it}
            if before != after:
                changed += 1
    if checked:
        print(f"  ♻ Negative local history v{NEG_PARSER_VERSION} → checked={checked}, changed={changed}, value_found={values}")
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
# Negative metadata hygiene (v1.3)
# ─────────────────────────────────────────────────────────────────────────────

_NEGATIVE_OWNED_FIELDS = (
    "negative_parser_version", "negative_type", "negative_stage",
    "tax_demand_cr", "tax_demand_text", "penalty_cr", "penalty_text",
    "interest_cr", "interest_text", "award_amount_cr", "award_amount_text",
    "total_exposure_cr", "relief_reference_cr", "relief_reference_text",
    "amount_cr", "amount_text", "amount_context", "amount_to_market_cap_pct", "authority",
)

def scrub_negative_metadata_from_nonnegative(data: dict) -> int:
    """Remove Negative-owned parser fields from every non-Negative row, including
    rows that were reclassified on an earlier run and therefore do not change
    category during today's historical cleanup.
    """
    changed = 0
    for items in data.values():
        for it in items:
            if it.get("category") == "Negative":
                continue
            touched = False
            for key in _NEGATIVE_OWNED_FIELDS:
                if key in it:
                    it.pop(key, None)
                    touched = True
            if touched and it.get("detail_source") in {"local_pdf", "pdf_local"}:
                it.pop("detail_source", None)
            if touched:
                changed += 1
    return changed

# ─────────────────────────────────────────────────────────────────────────────
# Negative adverse -> relief lifecycle consolidation (v1.3)
# ─────────────────────────────────────────────────────────────────────────────

def _negative_money_markers(item: dict) -> set[str]:
    vals = set()
    for key in ("amount_cr", "tax_demand_cr", "penalty_cr", "award_amount_cr", "relief_reference_cr"):
        try:
            if item.get(key) is not None:
                vals.add(f"{float(item[key]):.3f}")
        except Exception:
            pass
    raw = f"{item.get('text','')} {item.get('amount_text','')} {item.get('tax_demand_text','')}".replace(",", "")
    for m in re.finditer(r"(?:rs\.?|₹|inr)\s*(\d+(?:\.\d+)?)\s*(crore|crores|cr\b|million|mn\b|lakh|lakhs)", raw, re.I):
        try:
            unit = m.group(2).lower()
            v = float(m.group(1))
            if unit.startswith("million") or unit == "mn": v /= 10.0
            elif unit.startswith("lakh"): v /= 100.0
            vals.add(f"{v:.3f}")
        except Exception:
            pass
    return vals

def consolidate_negative_relief_lifecycles(data: dict) -> int:
    """Collapse an earlier adverse tax/litigation card when a later filing
    clearly records relief/set-aside for the same monetary matter. Conservative:
    same symbol, <=20 days, and a shared explicit monetary marker are required.
    """
    removed = 0
    relief_types = {"Tax / Litigation Relief", "Litigation Relief"}
    for sym in list(data):
        items = sorted(data[sym], key=lambda x: x.get("dt", ""), reverse=True)
        drop = set()
        for i, newer in enumerate(items):
            if newer.get("category") != "Negative" or newer.get("negative_type") not in relief_types:
                continue
            nm = _negative_money_markers(newer)
            if not nm:
                continue
            try:
                nd = datetime.fromisoformat(str(newer.get("dt", "")).replace("Z", "+00:00")).date()
            except Exception:
                continue
            for j in range(i + 1, len(items)):
                older = items[j]
                if older.get("category") != "Negative" or older.get("negative_type") in relief_types:
                    continue
                try:
                    od = datetime.fromisoformat(str(older.get("dt", "")).replace("Z", "+00:00")).date()
                except Exception:
                    continue
                if (nd - od).days > 20:
                    break
                if nm & _negative_money_markers(older):
                    drop.add(j)
        if drop:
            data[sym] = [x for j, x in enumerate(items) if j not in drop]
            removed += len(drop)
    return removed

# ─────────────────────────────────────────────────────────────────────────────
# Conservative lifecycle consolidation
# ─────────────────────────────────────────────────────────────────────────────

_LIFECYCLE_CATEGORIES = {
    "Order", "Acquisition", "Divestment", "Scheme of Arrangement",
    "Strategic Agreement", "Corporate Action", "Regulatory Approval",
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

    # Regulatory grants/renewals can be duplicated by exchange filings, but do not
    # merge different licences merely because the company is the same.
    if cat == "Regulatory Approval":
        return (gap == 0 and sim >= 0.45) or sim >= 0.70

    # Acquisition/divestment/strategic-agreement lifecycle updates normally repeat
    # the target/counterparty/project name, so require meaningful token overlap.
    return sim >= 0.42

_STAGE_RANK = {
    "L1 / Awaiting Award": 10, "Announced": 10, "MoU": 10, "Non-Binding MoU": 5,
    "Approved": 20, "Board Approved": 20, "Approved / Agreement": 25, "Allotment": 35,
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


# ─────────────────────────────────────────────────────────────────────────────
# NSE headline cross-check (no AI)
# The exchange summary often states the value outright ("wins orders of Rs 1,303
# crores"). It fills gaps where the PDF parser found nothing, and overrides PDF
# values that are clearly the wrong number (YTD intake, combined announcements,
# only the first of several work orders).
# ─────────────────────────────────────────────────────────────────────────────

_HEADLINE_AMOUNT_RE = re.compile(
    r"(?:₹|rs\.?|inr)\s*([0-9][0-9,]*(?:\.\d+)?)\s*(lakh crores?|crores?|cr\b\.?|lakhs?|lacs?)", re.I)
# Amounts in these contexts are not the value of this filing.
_HEADLINE_SKIP_CONTEXT = re.compile(
    r"order ?book|order intake|\bytd\b|year[- ]to[- ]date|till date|so far|cumulative|"
    r"turnover|revenue|market cap|net worth|paid[- ]up|authori[sz]ed capital|dividend", re.I)
_HEADLINE_VALUE_KEY = {
    "Order": ("order_value_cr", "order_value_text", "order_value_role"),
    "Acquisition": ("transaction_value_cr", "transaction_value_text", None),
    "Divestment": ("transaction_value_cr", "transaction_value_text", None),
    "Strategic Agreement": ("agreement_value_cr", "agreement_value_text", None),
    "Corporate Action": ("issue_value_cr", "issue_value_text", None),
}
# Only fund-raising corporate actions have an issue size worth showing.
_HEADLINE_CA_TYPES = {"QIP", "Rights Issue", "Preferential Issue", "Buyback", "Corporate Action"}
HEADLINE_OVERRIDE_DIFF = 0.15      # >15% apart = different number, not rounding/GST
NOMINAL_ACQ_MAX_CR = 1.0           # shell/SPV purchases for a few lakh are not catalysts
NOMINAL_ACQ_MAX_MCAP_PCT = 0.1


def _headline_value(text: str) -> tuple[float | None, str]:
    """First clean ₹ amount in the NSE summary, in crore."""
    import html as _html
    t = _html.unescape(text or "")
    for m in _HEADLINE_AMOUNT_RE.finditer(t):
        before = t[max(0, m.start() - 45):m.start()]
        if _HEADLINE_SKIP_CONTEXT.search(before):
            continue
        try:
            n = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        u = m.group(2).lower()
        cr = n * 1e5 if u.startswith("lakh cr") else n if u.startswith("cr") else n / 100.0
        if cr <= 0:
            continue
        return round(cr, 4), m.group(0).strip()
    return None, ""


def apply_headline_quality(history: dict, market_cap_map: dict | None = None,
                           ttm_sales_map: dict | None = None) -> tuple[int, int, int]:
    """Fill/override values from the NSE headline and drop nominal acquisitions.

    Idempotent: once a row carries the headline value, later runs see no
    difference. The replaced PDF figure is kept as pdf_value_cr for audit.
    Manual rows are never touched; Gemini values are filled but not overridden.
    """
    filled = overridden = dropped = 0
    for sym in list(history):
        kept = []
        for it in history[sym]:
            if it.get("manual"):
                kept.append(it)
                continue
            cat = it.get("category")
            # Re-strip ratios that should not exist (e.g. non-binding MoU).
            if cat == "Strategic Agreement" and _is_non_binding_mou(it):
                it.pop("agreement_to_market_cap_pct", None)

            keys = _HEADLINE_VALUE_KEY.get(cat)
            if keys and not (cat == "Corporate Action" and it.get("event_type") not in _HEADLINE_CA_TYPES):
                vkey, tkey, rkey = keys
                hv, htxt = _headline_value(it.get("text", ""))
                cur = it.get(vkey)
                if hv is not None:
                    change = False
                    if cur is None:
                        change = True
                        filled += 1
                    elif (it.get("detail_source") not in {"gemini_pdf", "gemini"}
                          and it.get("value_source") != "gemini_pdf"):
                        try:
                            curf = float(cur)
                        except (TypeError, ValueError):
                            curf = None
                        if curf and abs(hv - curf) / max(hv, curf) > HEADLINE_OVERRIDE_DIFF:
                            role = str(it.get(rkey) or "") if rkey else ""
                            # PDF "total" rows are trusted unless the headline is the
                            # bigger aggregate (several work orders in one filing).
                            if role != "total" or hv > curf:
                                change = True
                                overridden += 1
                                it.setdefault("pdf_value_cr", curf)
                    if change:
                        it[vkey] = hv
                        it[tkey] = htxt
                        if rkey:
                            it[rkey] = "headline"
                        it["value_source"] = "nse_headline"
                        for k in list(it):
                            if k.endswith("_to_market_cap_pct") or k == "order_to_ttm_sales_pct":
                                it.pop(k, None)
                        it["_lookup_symbol"] = str(sym).strip().upper()
                        _apply_materiality_ratios(it, cat, {vkey: hv, "binding_status": it.get("binding_status")},
                                                  market_cap_map, ttm_sales_map)
                        it.pop("_lookup_symbol", None)

            if cat == "Acquisition":
                v = it.get("transaction_value_cr")
                pct = it.get("transaction_to_market_cap_pct")
                if (isinstance(v, (int, float)) and v < NOMINAL_ACQ_MAX_CR and
                        (pct is None or pct < NOMINAL_ACQ_MAX_MCAP_PCT)):
                    dropped += 1
                    continue
            kept.append(it)
        if kept:
            history[sym] = kept
        else:
            del history[sym]
    if filled or overridden or dropped:
        print(f"  🧾 Headline cross-check → filled={filled}, overridden={overridden}, "
              f"nominal_acquisitions_dropped={dropped}")
    return filled, overridden, dropped


# ─────────────────────────────────────────────────────────────────────────────
# Order check pipeline (heading → PDF type check → AI only when needed)
#   1. Value from the heading (NSE summary or the PDF "Sub:" line) — free.
#   2. Local PDF check: is this really an order RECEIVED? Placed orders go to
#      Capex, cancellations to Negative, L1 / Preferred Bidder / LoI fix stage.
#   3. AI only for confirmed orders whose value is not in the heading but is
#      disclosed somewhere in the PDF. AI returns 5 small fields.
# ─────────────────────────────────────────────────────────────────────────────

_ANNEXURE_VALUE_ROW = re.compile(
    r"broad (?:commercial )?consideration|size of the order|value of (?:the )?(?:order|contract)|contract value|"
    r"converted value in inr", re.I)
ORDER_CHECK_BATCH = int(os.environ.get("ORDER_CHECK_BATCH", "25"))   # PDFs opened per run
ORDER_CHECK_VERSION = 1
MINING_LEASE_CATEGORY = "Order"    # set to "Capex" to move mining-lease bids out of Orders

_SUBJECT_LINE_RE = re.compile(
    r"\bsub(?:ject)?\s*[:.\-–]\s*(.{10,600}?)(?=\bdear\b|\bref(?:erence)?\s*[:.]|\brespected\b|\bpursuant\b|\bin accordance\b|$)",
    re.I)
_HEAD_REGION_CHARS = 2500
_CANCEL_RE = re.compile(
    r"(?:cancell?ation|cancell?ed|terminat(?:ion|ed)|withdraw(?:al|n)|annul(?:led|ment)|short[- ]?clos(?:ure|ed))"
    r"\W+(?:\w+\W+){0,6}?(?:letter of (?:award|acceptance|intent)|\blo[ai]\b|work order|purchase order|order|contract)|"
    r"(?:letter of (?:award|acceptance|intent)|\blo[ai]\b|work order|purchase order|order|contract)"
    r"\W+(?:\w+\W+){0,6}?(?:cancell?ed|terminated|withdrawn|annulled|short[- ]?closed)", re.I)
_PLACED_STRONG_RE = re.compile(
    r"placement of (?:the )?(?:purchase |work )?order on|(?:placed|awarded) (?:the |an? )?(?:purchase |work )?(?:order|contract) (?:on|to) m/?s|"
    r"approv\w* (?:for |the )?(?:placing|placement|award) of (?:the )?(?:purchase |work )?order", re.I)
_PLACED_RE = re.compile(
    r"name of (?:the )?entity to (?:which|whom) (?:the )?order|placement of (?:the )?(?:purchase |work )?order on|"
    r"(?:placed|awarded) (?:the |an? )?(?:purchase |work )?(?:order|contract) (?:on|to) m/?s", re.I)
_L1_RE = re.compile(
    r"(?:declared|emerged|stood|ranked|been|is|as)\s+(?:as\s+)?(?:the\s+)?(?:l[- ]?1|lowest)\b(?!\s*(?:position|pipeline|order\s*book))|"
    r"\bl[- ]?1\s*(?:bidder|stage|bid)\b|lowest (?:evaluated )?bidder|first lowest", re.I)
_PREF_BIDDER_RE = re.compile(r"preferred bidder", re.I)
_MINING_RE = re.compile(r"mining lease|mineral block|composite licen[cs]e|limestone block|coal block", re.I)
_LOI_RE = re.compile(r"letter of intent|\bloi\b", re.I)
_FIRM_AWARD_RE = re.compile(r"letter of (?:award|acceptance)|\bloa\b|work order|purchase order|agreement (?:signed|executed)|contract (?:signed|executed)", re.I)
_ANY_AMOUNT_RE = re.compile(
    r"(?:₹|rs\.?|inr|usd|us\$|eur|€|sgd|aed|gbp|£|\$)\s*[0-9]|[0-9][0-9,.]*\s*(?:crores?|lakhs?|lacs?)\b|"
    r"[0-9][0-9,.]*\s*(?:million|billion)\s*(?:us\s*dollars?|usd|dollars?)", re.I)
_QTY_RE = re.compile(r"\b\d[\d,.]*\s*(?:GW|MWh|MWp|MW|TPH)\b(?:\s*/\s*\d[\d,.]*\s*(?:GWh|MWh))?")
_BAND_RE = re.compile(
    r"(?:major|significant|large|mega|big|sizeable|ultra[- ]mega)\W{0,3}(?:order)?\W{0,3}(?:indicates?|means?|denotes?|refers? to|is defined as|classification)"
    r"[^.]{0,80}?(?:(?:over|above|exceeding|more than|greater than|upwards of)\s*(?:₹|rs\.?|inr)\s*([\d,]+(?:\.\d+)?)\s*(?:crores?|cr)"
    r"|(?:between|from|range of)\s*(?:₹|rs\.?|inr)\s*([\d,]+(?:\.\d+)?)\s*(?:crores?|cr)?\s*(?:and|to|-|–)\s*(?:₹|rs\.?|inr)?\s*([\d,]+(?:\.\d+)?)\s*(?:crores?|cr))",
    re.I)
_ANNUAL_RE = re.compile(r"per annum|yearly revenue|annual(?:ly)? revenue|revenue per year|per year for|p\.a\.", re.I)
_VENDOR_RE = re.compile(r"entity to (?:which|whom).{0,60}?awarded\s*[;:]?\s*(.{3,90}?)(?=\s+(?:b\s*\.|2\s*\.|\(ii\)|whether)(?:\s|$))", re.I)
_CANCEL_PARTY_RE = re.compile(r"(?:received |awarded |issued )?(?:from|by)\s+((?:[A-Z][\w&.,'()-]*\s?){1,6})", re.M)


def _pdf_subject(text: str) -> str:
    clean = _normalize_pdf_text(text)
    m = _SUBJECT_LINE_RE.search(clean[:4000])
    return m.group(1).strip() if m else ""


def _order_doc_check(pdf_text: str, nse_text: str) -> dict:
    """Free, local classification of an order filing. Never calls AI."""
    clean = _normalize_pdf_text(pdf_text or "")
    subject = _pdf_subject(pdf_text or "")
    head = f"{nse_text or ''} {subject} {clean[:_HEAD_REGION_CHARS]}"
    out = {"subject": subject}
    if _CANCEL_RE.search(f"{nse_text or ''} {subject}") or _CANCEL_RE.search(clean[:1200]):
        out["doc_type"] = "cancellation"
    elif _PLACED_STRONG_RE.search(head) or (
            # Annexure label alone is ambiguous (some recipients name themselves there),
            # so it needs a named vendor and must not be a "Bagging/Receiving" filing.
            _PLACED_RE.search(clean) and _VENDOR_RE.search(clean)
            and not re.search(r"bagging|receiv", nse_text or "", re.I)):
        out["doc_type"] = "placed"
    else:
        out["doc_type"] = "received"
        if _PREF_BIDDER_RE.search(head):
            out["stage"] = "Preferred Bidder"
            out["event_type"] = "Mining Lease" if _MINING_RE.search(clean) else "Order Award"
        elif _L1_RE.search(head):
            out["stage"], out["event_type"] = "L1 / Awaiting Award", "L1 Bidder"
        elif _LOI_RE.search(head) and not _FIRM_AWARD_RE.search(head):
            out["stage"], out["event_type"] = "Letter of Intent", "Order Award"
    out["value_disclosed"] = bool(_ANY_AMOUNT_RE.search(clean))
    q = _QTY_RE.search(head) or _QTY_RE.search(clean)
    if q:
        out["quantity"] = q.group(0)
    out["annual"] = bool(_ANNUAL_RE.search(clean))
    b = _BAND_RE.search(clean)
    if b:
        lo = b.group(1) or b.group(2)
        out["band_min"] = float(lo.replace(",", ""))
        if b.group(3):
            out["band_max"] = float(b.group(3).replace(",", ""))
    out["_head"] = head[:1500]
    return out


def _apply_band(it: dict, chk: dict) -> bool:
    """Company only disclosed a size band: show it as a band, never as an exact value."""
    lo = chk.get("band_min")
    if lo is None:
        return False
    v = it.get("order_value_cr")
    if v is not None and not (abs(v - lo) < 0.01 or (chk.get("band_max") and abs(v - chk["band_max"]) < 0.01)):
        return False          # an exact value was disclosed elsewhere; keep it
    it["order_value_cr"] = lo
    it["order_value_role"] = "band_min"
    hi = chk.get("band_max")
    it["order_value_text"] = (f"₹{lo:,.0f}–{hi:,.0f} Cr (company band)" if hi else f"Over ₹{lo:,.0f} Cr (company band)")
    return True


def _ai_value_in_head(av: float, chk: dict) -> bool:
    """True when the AI's figure is printed in the title/subject area, i.e. the filing's headline number."""
    head = chk.get("_head", "")
    for m in re.finditer(r"(?:₹|rs\.?|inr)\s*([\d,]+(?:\.\d+)?)\s*(?:crores?|cr)", head, re.I):
        try:
            if abs(float(m.group(1).replace(",", "")) - av) < 0.01:
                return True
        except ValueError:
            pass
    return False


def _set_ratio(it: dict, sym: str, category: str, key: str, value: float,
               market_cap_map: dict | None, ttm_sales_map: dict | None) -> None:
    for k in [k for k in it if k.endswith("_to_market_cap_pct") or k == "order_to_ttm_sales_pct"]:
        it.pop(k, None)
    it["_lookup_symbol"] = str(sym).strip().upper()
    _apply_materiality_ratios(it, category, {key: value}, market_cap_map, ttm_sales_map)
    it.pop("_lookup_symbol", None)


_ORDER_OWNED = ("order_value_cr", "order_value_text", "order_value_role", "order_from", "order_purpose",
                "order_type", "execution_period", "related_party", "order_summary", "detail_excerpt",
                "order_to_market_cap_pct", "order_to_ttm_sales_pct", "company_share_of_order_cr")


def _to_capex(it: dict, sym: str, chk: dict, local: dict, hv: float | None, htxt: str,
              market_cap_map: dict | None) -> None:
    clean_vendor = ""
    m = _VENDOR_RE.search(_normalize_pdf_text(chk.get("_text", "")))
    if m:
        clean_vendor = _clean_field(m.group(1), 120)
    value = hv if hv is not None else local.get("order_value_cr", it.get("order_value_cr"))
    vtxt = htxt or local.get("order_value_text") or it.get("order_value_text")
    purpose = local.get("order_purpose") or it.get("order_purpose")
    for k in _ORDER_OWNED:
        it.pop(k, None)
    it["category_override"] = "Capex"
    it["category"] = "Capex"
    it["event_type_override"] = it["event_type"] = "Order Placed"
    it["stage_override"] = it["stage"] = ("Board Approved" if re.search(r"board of directors|board meeting|approval of the board", chk.get("subject", "") + " " + _normalize_pdf_text(chk.get("_text", ""))[:1500], re.I)
                                          else "Placed")
    if value is not None:
        it["capex_value_cr"] = value
        if vtxt:
            it["capex_value_text"] = vtxt
        mcap = (market_cap_map or {}).get(str(sym).strip().upper())
        try:
            if mcap and float(mcap) > 0:
                it["capex_to_market_cap_pct"] = round(float(value) / float(mcap) * 100.0, 2)
        except (TypeError, ValueError):
            pass
    if clean_vendor:
        it["vendor"] = clean_vendor
    if purpose:
        it["capex_purpose"] = purpose


def _to_cancellation(it: dict, history_items: list, hv: float | None, htxt: str) -> None:
    for k in _ORDER_OWNED:
        it.pop(k, None)
    it["category_override"] = it["category"] = "Negative"
    it["negative_type"] = "Order Cancellation"
    it["negative_stage"] = "Cancelled"
    it["negative_parser_version"] = NEG_PARSER_VERSION   # keep the Negative PDF parser from relabelling it
    it.pop("event_type", None); it.pop("stage", None)
    if hv is not None:
        it["amount_cr"], it["amount_text"] = hv, htxt
    # Link to the award it cancels: same symbol, earlier Order card, same counterparty.
    m = _CANCEL_PARTY_RE.search(it.get("text", ""))
    party = m.group(1).strip(" .,") if m else ""
    if len(party) < 4:
        return
    cands = [x for x in history_items if x is not it and x.get("category") == "Order"
             and x.get("dt", "") < it.get("dt", "")
             and party.lower() in f"{x.get('text', '')} {x.get('order_from', '')}".lower()]
    if len(cands) == 1:
        c = cands[0]
        c["stage_override"] = c["stage"] = "Cancelled"
        c["cancelled_by"] = it.get("id")
        it["cancels"] = c.get("id")
        if it.get("amount_cr") is None and c.get("order_value_cr") is not None:
            it["amount_cr"] = c["order_value_cr"]
    elif len(cands) > 1:
        it["needs_review"] = True
        it["review_reason"] = f"cancellation matches {len(cands)} earlier orders from {party}"


STAGE_CHECK_VERSION = 3


def recheck_order_stages(session, history: dict, limit: int = 30) -> None:
    """Re-run only the PDF type check on rows whose stage/category the first
    order-check version changed (L1 / LoI / Preferred Bidder / Capex)."""
    checked = changed = 0
    for sym, items in history.items():
        for it in items:
            if checked >= limit:
                break
            if it.get("manual") or int(it.get("stage_check_v") or 0) >= STAGE_CHECK_VERSION:
                continue
            was_capex = it.get("category_override") == "Capex"
            if not (was_capex or it.get("stage_override") in {"L1 / Awaiting Award", "Letter of Intent", "Preferred Bidder"}
                    or it.get("value_source") == "gemini_pdf" or it.get("needs_review")):
                continue
            pdf = _download_pdf_bytes(session, it.get("link", ""))
            if not pdf:
                continue
            checked += 1
            chk = _order_doc_check(_extract_pdf_text_bytes(pdf), it.get("text", ""))
            if was_capex and chk["doc_type"] != "placed":
                # Back to a received order; value/customer come from the next order check.
                for k in ("category_override", "event_type_override", "stage_override", "capex_value_cr",
                          "capex_value_text", "capex_to_market_cap_pct", "vendor", "capex_purpose", "order_check_v"):
                    it.pop(k, None)
                it["category"] = "Order"
                it["event_type"], it["stage"] = "Order Award", "Awarded"
                changed += 1
            if not was_capex and it.get("category") == "Order":
                if _apply_band(it, chk):
                    changed += 1
                av, lv = it.get("order_value_cr"), it.get("local_value_cr")
                if it.get("needs_review") and av is not None and (
                        _ai_value_in_head(float(av), chk)
                        or (lv is not None and any(x is not it and x.get("order_value_cr") is not None
                                                   and abs(float(x["order_value_cr"]) - float(lv)) < 0.01
                                                   for x in items))):
                    it.pop("needs_review", None); it.pop("review_reason", None)
                    changed += 1
            if was_capex and chk["doc_type"] == "placed":
                pass
            elif not was_capex:
                new_stage = chk.get("stage") or "Awarded"
                new_type = chk.get("event_type") or "Order Award"
                if new_stage != it.get("stage"):
                    it["stage_override"] = it["stage"] = new_stage
                    it["event_type_override"] = it["event_type"] = new_type
                    changed += 1
            it["stage_check_v"] = STAGE_CHECK_VERSION
    if checked:
        print(f"  🔁 Stage re-check → checked={checked}, changed={changed}")


def process_orders(session, history: dict, market_cap_map: dict | None = None,
                   ttm_sales_map: dict | None = None, pdf_limit: int = ORDER_CHECK_BATCH,
                   ai_limit: int = AI_ORDER_BATCH) -> dict:
    """Run the order check once per row (marker order_check_v)."""
    global _GEMINI_RATE_LIMITED
    _GEMINI_RATE_LIMITED = False
    stats = dict(checked=0, heading=0, ai=0, capex=0, cancelled=0, not_disclosed=0, stage_fixed=0, queued=0)
    queue = []
    for sym, items in history.items():
        for it in items:
            if it.get("manual") or int(it.get("order_check_v") or 0) >= ORDER_CHECK_VERSION:
                continue
            cat = it.get("category")
            is_cancel_neg = cat == "Negative" and _ORDER_CANCEL.search(f"{it.get('subject', '')} {it.get('text', '')}")
            if cat == "Order" or is_cancel_neg:
                queue.append((it.get("dt", ""), sym, it))
    queue.sort(key=lambda t: t[0], reverse=True)
    ai_sent = 0
    for _dt, sym, it in queue:
        if stats["checked"] >= pdf_limit:
            break
        pdf_bytes = _download_pdf_bytes(session, it.get("link", ""))
        if not pdf_bytes:
            continue      # retried next run; unreachable links are handled by backfill
        stats["checked"] += 1
        text = _extract_pdf_text_bytes(pdf_bytes)
        chk = _order_doc_check(text, it.get("text", ""))
        chk["_text"] = text
        if it.get("category") == "Negative":
            chk["doc_type"] = "cancellation"

        # Heading value: NSE summary first, then the PDF "Sub:" line.
        hv, htxt = _headline_value(it.get("text", ""))
        hsrc = "nse_headline"
        if hv is None:
            hv, htxt = _headline_value(chk.get("subject", ""))
            hsrc = "pdf_subject"

        # Fresh local parse with the fixed parser (gemini/manual values are kept).
        local = {}
        if it.get("detail_source") not in {"gemini_pdf", "gemini"}:
            local = _extract_order_details(text)

        if chk["doc_type"] == "cancellation":
            _to_cancellation(it, history.get(sym, []), hv, htxt)
            stats["cancelled"] += 1
        elif chk["doc_type"] == "placed":
            _to_capex(it, sym, chk, local, hv, htxt, market_cap_map)
            stats["capex"] += 1
        else:
            if local:
                for k in ("order_from", "order_purpose", "order_type", "execution_period", "related_party"):
                    it.pop(k, None)
                for k in ("order_from", "order_purpose", "order_type", "execution_period", "related_party"):
                    if local.get(k) not in (None, ""):
                        it[k] = local[k]
            if chk.get("stage"):
                it["stage_override"] = it["stage"] = chk["stage"]
                it["event_type_override"] = it["event_type"] = chk["event_type"]
                if chk.get("event_type") == "Mining Lease" and MINING_LEASE_CATEGORY != "Order":
                    it["category_override"] = it["category"] = MINING_LEASE_CATEGORY
                stats["stage_fixed"] += 1
            if not it.get("event_type"):
                # Generic press-release wording left it untagged; the PDF says it is an order.
                it["event_type_override"] = it["event_type"] = "Order Award"
                it["stage_override"] = it["stage"] = it.get("stage") or "Awarded"
            if chk.get("quantity") and not it.get("quantity_or_capacity"):
                it["quantity_or_capacity"] = chk["quantity"]

            if hv is not None and not chk["annual"]:
                it["order_value_cr"], it["order_value_text"] = hv, htxt
                it["order_value_role"] = "headline"
                it["value_source"] = hsrc
                lv = local.get("order_value_cr")
                if lv and abs(lv - hv) / max(lv, hv) > HEADLINE_OVERRIDE_DIFF:
                    it["local_value_cr"] = lv      # audit only; heading wins
                _set_ratio(it, sym, "Order", "order_value_cr", hv, market_cap_map, ttm_sales_map)
                stats["heading"] += 1
            elif (local.get("order_value_cr") is not None and not chk["annual"]
                  and (local.get("order_value_role") == "total"
                       or re.search(r"converted value in inr|inr equivalent", local.get("detail_excerpt") or "", re.I))
                  and _ANNEXURE_VALUE_ROW.search(local.get("detail_excerpt") or "")):
                # Value read from the SEBI annexure consideration row: reliable, no AI needed.
                lv = local["order_value_cr"]
                it["order_value_cr"], it["order_value_text"] = lv, local.get("order_value_text")
                it["order_value_role"] = "total"
                it["value_source"] = "pdf_annexure"
                _set_ratio(it, sym, "Order", "order_value_cr", lv, market_cap_map, ttm_sales_map)
                stats["annexure"] = stats.get("annexure", 0) + 1
            elif not chk["value_disclosed"]:
                for k in ("order_value_cr", "order_value_text", "order_value_role",
                          "order_to_market_cap_pct", "order_to_ttm_sales_pct"):
                    it.pop(k, None)
                it["value_disclosed"] = False
                stats["not_disclosed"] += 1
            else:
                # Value is somewhere in the PDF but not in the heading -> AI.
                if not GEMINI_API_KEY or ai_sent >= ai_limit or _GEMINI_RATE_LIMITED:
                    if local.get("order_value_cr") is not None and it.get("order_value_cr") is None:
                        it["order_value_cr"] = local["order_value_cr"]
                        it["order_value_text"] = local.get("order_value_text")
                        it["order_value_role"] = local.get("order_value_role")
                        _set_ratio(it, sym, "Order", "order_value_cr", local["order_value_cr"],
                                   market_cap_map, ttm_sales_map)
                    stats["queued"] += 1
                    continue          # no marker: AI retried on a later run
                ai_sent += 1
                ai = _gemini_order_details(session, pdf_bytes, it.get("link", "").rsplit("/", 1)[-1])
                if _GEMINI_RATE_LIMITED:
                    stats["queued"] += 1
                    continue
                stats["ai"] += 1
                it["ai_checked"] = True
                for k in ("order_from", "order_summary"):
                    if ai.get(k):
                        it[k] = ai[k]
                av, basis = ai.get("order_value_cr"), ai.get("value_basis")
                if basis == "not_disclosed" or av is None:
                    if av is None and local.get("order_value_cr") is not None:
                        av, basis = local["order_value_cr"], "total"
                    else:
                        it["value_disclosed"] = False
                if av is not None:
                    lv = local.get("order_value_cr")
                    if basis == "annual":
                        tenure = ai.get("tenure_years")
                        it["annual_value_cr"] = av
                        if tenure:
                            it["tenure_years"] = tenure
                            it["order_value_cr"] = round(av * tenure, 2)
                            it["order_value_role"] = "annual_x_tenure"
                        else:
                            it["order_value_cr"] = av
                            it["order_value_role"] = "annual"
                        it["order_value_text"] = f"₹{av} Cr per year" + (f" × {tenure:g} yrs" if tenure else "")
                    else:
                        it["order_value_cr"], it["order_value_role"] = av, "ai"
                        it["order_value_text"] = f"₹{av} Cr"
                        if lv and abs(lv - av) / max(lv, av) > AI_AGREE_TOL:
                            it["local_value_cr"] = lv
                            other_card = any(x is not it and x.get("order_value_cr") is not None
                                             and abs(float(x["order_value_cr"]) - lv) < 0.01
                                             for x in history.get(sym, []))
                            # Local picked an earlier order's figure ("in continuation to our letter…")
                            # or a YTD/order-book number while AI matches the headline: AI is right.
                            if not (other_card or _ai_value_in_head(av, chk)):
                                it["needs_review"] = True
                                it["review_reason"] = "AI and PDF table disagree"
                    it["value_source"] = "gemini_pdf"
                    _apply_band(it, chk)
                    _set_ratio(it, sym, "Order", "order_value_cr", it["order_value_cr"],
                               market_cap_map, ttm_sales_map)
                    if basis == "annual" and ttm_sales_map:
                        ttm = ttm_sales_map.get(str(sym).strip().upper())
                        try:
                            if ttm and float(ttm) > 0:     # annuity: yearly revenue vs yearly sales
                                it["order_to_ttm_sales_pct"] = round(av / float(ttm) * 100.0, 2)
                        except (TypeError, ValueError):
                            pass
        it["order_check_v"] = ORDER_CHECK_VERSION
        it["stage_check_v"] = STAGE_CHECK_VERSION
    stats["queued"] += max(0, len(queue) - stats["checked"])
    print("  🔎 Order check → " + ", ".join(f"{k}={v}" for k, v in stats.items()))
    return stats


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


def _r2_get_json_strict(session, filename: str) -> tuple[bool, object]:
    """(ok, data). 404 -> (True, None). Any other failure -> (False, None), so callers
    never mistake a network glitch for an empty file and overwrite real history."""
    import time
    worker_url = os.environ["WORKER_URL"].rstrip("/")
    token = os.environ["WORKER_TOKEN"]
    try:
        r = session.get(f"{worker_url}/{filename}?v={int(time.time())}",
                        headers={"X-Secret-Token": token, "Cache-Control": "no-cache"}, timeout=30)
        if r.status_code == 404:
            return True, None
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        print(f"  ⚠ R2 read {filename} failed ({e})")
        return False, None


def _r2_put_json(session, filename: str, payload: dict, quiet: bool = False):
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
    if not quiet:
        print(f"  ✓ Uploaded {filename}")


# ─────────────────────────────────────────────────────────────────────────────
# Corporate Action PDF check (no AI)
# NSE's summary often says "approved" while the PDF says the board only
# RECOMMENDED it, subject to shareholder approval (postal ballot / EGM). The
# stage comes from the PDF; credit and record dates are captured when stated.
# ─────────────────────────────────────────────────────────────────────────────

CA_CHECK_BATCH = int(os.environ.get("CA_CHECK_BATCH", "20"))
CA_CHECK_VERSION = 1
_DATE_TXT = r"(?:[A-Z][a-z]+\.?\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4}|\d{1,2}(?:st|nd|rd|th)?\s+[A-Z][a-z]+,?\s+\d{4}|\d{1,2}[./-]\d{1,2}[./-]\d{2,4})"
_CA_SH_APPROVED = re.compile(r"(?:approved|passed)\s+by\s+(?:the\s+)?(?:shareholders|members)|requisite majority|result[s]? of (?:the )?(?:postal ballot|e-?voting|egm)", re.I)
_CA_SH_PENDING = re.compile(r"subject to (?:the )?(?:approval|consent) of (?:the )?(?:shareholders|members)|\brecommend(?:ed|s)?\b[^.]{0,80}(?:bonus|issu|split|sub-division)|draft postal ballot notice|convening (?:an? )?(?:extra[- ]?ordinary general meeting|egm)", re.I)
_CA_CREDIT = re.compile(r"(?:credited|dispatched|allot(?:ted|ment)).{0,200}?on or before\s+(" + _DATE_TXT + r")", re.I)
_CA_RECORD = re.compile(r"record date[^.]{0,60}?(?:is|as|fixed as|fixed|be|:)\s*(?:on\s+)?(?:[A-Z][a-z]+day,?\s+)?(" + _DATE_TXT + r")", re.I)


def process_corp_actions(session, history: dict, limit: int = CA_CHECK_BATCH) -> None:
    checked = changed = 0
    for sym, items in history.items():
        for it in items:
            if checked >= limit:
                break
            if (it.get("manual") or it.get("category") != "Corporate Action"
                    or int(it.get("ca_check_v") or 0) >= CA_CHECK_VERSION
                    or it.get("event_type") in {"Dividend"}):
                continue
            pdf = _download_pdf_bytes(session, it.get("link", ""))
            if not pdf:
                continue
            checked += 1
            clean = _normalize_pdf_text(_extract_pdf_text_bytes(pdf))
            before = (it.get("stage"), it.get("credit_by"), it.get("record_date"))
            m = _CA_RECORD.search(clean)
            if m:
                it["record_date"] = m.group(1)
            m = _CA_CREDIT.search(clean)
            if m:
                it["credit_by"] = m.group(1)
            # Never move a later stage (record date / allotment / completed) backwards.
            if it.get("stage") in {None, "", "Announced", "Approved", "Board Recommended"}:
                if _CA_SH_APPROVED.search(clean):
                    it["stage_override"] = it["stage"] = "Shareholders Approved"
                elif _CA_SH_PENDING.search(clean):
                    it["stage_override"] = it["stage"] = "Board Recommended"
                if it.get("record_date"):
                    it["stage_override"] = it["stage"] = "Record Date"
            it["ca_check_v"] = CA_CHECK_VERSION
            if (it.get("stage"), it.get("credit_by"), it.get("record_date")) != before:
                changed += 1
    if checked:
        print(f"  🏷 Corporate action check → checked={checked}, changed={changed}")


# ─────────────────────────────────────────────────────────────────────────────
# Per-symbol 3-year event archive (for chart markers)
#   cat_hist_<SYMBOL>.json  → {"symbol", "updated", "events": [compact event, ...]}
#   cat_hist__index.json    → {SYMBOL: {"h": hash of its 20-day window, "n", "last"}}
# The rolling 20-day file stays small; charts fetch one symbol's archive on demand.
# Inside the 20-day window the archive mirrors the live file (so fixes, merges and
# removals carry over); older events are frozen and kept for ARCHIVE_YEARS.
# ─────────────────────────────────────────────────────────────────────────────

ARCHIVE_YEARS = 3
ARCHIVE_PREFIX = "cat_hist_"
ARCHIVE_INDEX = "cat_hist__index.json"
ARCHIVE_BATCH = int(os.environ.get("ARCHIVE_BATCH", "200"))   # symbol files written per run


def archive_key(symbol: str) -> str:
    return ARCHIVE_PREFIX + re.sub(r"[^A-Z0-9]", "_", str(symbol).upper()) + ".json"


def _compact_event(it: dict) -> dict:
    """Only what a chart marker and its tooltip need."""
    cat = it.get("category")
    relief = cat == "Negative" and re.search(r"relief|favourable|set aside|quashed|in favour",
                                              f"{it.get('negative_type', '')} {it.get('negative_stage', '')}", re.I)
    value = next((it.get(k) for k in ("order_value_cr", "capex_value_cr", "transaction_value_cr", "issue_value_cr",
                                      "total_exposure_cr", "amount_cr", "agreement_value_cr")
                  if it.get(k) is not None), None)
    mcap = next((it.get(k) for k in ("order_to_market_cap_pct", "capex_to_market_cap_pct",
                                     "transaction_to_market_cap_pct", "amount_to_market_cap_pct",
                                     "agreement_to_market_cap_pct", "issue_to_market_cap_pct")
                 if it.get(k) is not None), None)
    summary = it.get("order_summary") or it.get("event_summary") or re.sub(
        r"^.{0,120}?\bhas\s+informed\s+the\s+exchange\s+(?:about|regarding|that)\s+", "", it.get("text") or "", flags=re.I)
    out = {
        "id": it.get("id"), "dt": it.get("dt"), "react_date": it.get("react_date"), "session": it.get("session"),
        "category": "Relief" if relief else cat,
        "type": it.get("negative_type") if cat == "Negative" else it.get("event_type"),
        "stage": it.get("negative_stage") if cat == "Negative" else it.get("stage"),
        "value_cr": value, "mcap_pct": mcap, "ttm_pct": it.get("order_to_ttm_sales_pct"),
        "ratio": it.get("ratio"), "customer": it.get("order_from") or it.get("vendor"),
        "record_date": it.get("record_date"), "credit_by": it.get("credit_by"),
        "summary": (summary or "")[:220], "link": it.get("link"),
    }
    return {k: v for k, v in out.items() if v not in (None, "")}


def update_symbol_archives(session, history: dict, suppressed: dict, today: date) -> None:
    import hashlib
    ok, index = _r2_get_json_strict(session, ARCHIVE_INDEX)
    if not ok:
        print("  ⚠ Archive skipped this run (index unreadable)")
        return
    index = index if isinstance(index, dict) else {}
    win_cut = (today - timedelta(days=HISTORY_DAYS)).isoformat()
    arch_cut = (today - timedelta(days=365 * ARCHIVE_YEARS + 1)).isoformat()
    written = failed = pending = 0
    for sym in sorted(history):
        compact = sorted((_compact_event(x) for x in history[sym] if x.get("id")),
                         key=lambda e: e.get("dt", ""), reverse=True)
        h = hashlib.sha1(json.dumps(compact, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
        if (index.get(sym) or {}).get("h") == h:
            continue
        if written >= ARCHIVE_BATCH:
            pending += 1
            continue
        key = archive_key(sym)
        ok, arch = _r2_get_json_strict(session, key)
        if not ok:
            failed += 1
            continue
        cur_ids = {e["id"] for e in compact}
        old = (arch or {}).get("events") or []
        # Keep frozen history (older than the live window, younger than 3 years);
        # inside the window the live file is the truth.
        kept = [e for e in old if e.get("id") not in cur_ids and e.get("id") not in suppressed
                and arch_cut <= str(e.get("dt", ""))[:10] < win_cut]
        events = sorted(kept + compact, key=lambda e: e.get("dt", ""), reverse=True)
        try:
            _r2_put_json(session, key, {"symbol": sym, "updated": datetime.now().astimezone().isoformat(timespec="seconds"),
                                        "events": events}, quiet=True)
        except Exception as e:
            print(f"  ⚠ Archive write {key} failed ({e})")
            failed += 1
            continue
        index[sym] = {"h": h, "n": len(events), "last": events[0].get("dt", "") if events else ""}
        written += 1
    if written:
        _r2_put_json(session, ARCHIVE_INDEX, index, quiet=True)
    print(f"  📚 Symbol archive → written={written}, pending={pending}, failed={failed}, symbols={len(index)}")


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
    # Cards removed on purpose (lifecycle duplicates, superseded adverse cards,
    # nominal acquisitions). Without this list the API re-delivers them every run,
    # their PDFs are re-downloaded and they can even consume AI calls again.
    suppressed = {}
    if isinstance(old_payload, dict) and isinstance(old_payload.get("suppressed"), dict):
        _sup_cutoff = (today - timedelta(days=HISTORY_DAYS + 2)).isoformat()
        suppressed = {k: v for k, v in old_payload["suppressed"].items() if str(v)[:10] >= _sup_cutoff}
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
            if (not cat and x.get("category") and len(x.get("text") or "") >= TEXT_MAX
                    and not is_explicit_noise(x.get("subject", ""), x.get("text", ""))):
                # Stored text is cut at TEXT_MAX; the keyword that classified this
                # row at fetch time may lie past the cut. Keep the fetch-time
                # category unless a noise rule positively matches.
                cat = x["category"]
            if x.get("category_override"):
                cat = x["category_override"]     # set from the PDF, outranks summary text
            if not cat or _is_cirp_procedural_item(x):
                removed_noise += 1
                continue
            if x.get("category") != cat:
                old_cat = x.get("category")
                x["category"] = cat
                reclassified += 1
                # v1.2: Negative parser metadata must never leak into a row that
                # has been deterministically reclassified to another category.
                if old_cat == "Negative" and cat != "Negative":
                    for _k in ("negative_parser_version", "negative_type", "negative_stage",
                               "tax_demand_cr", "tax_demand_text", "penalty_cr", "penalty_text",
                               "interest_cr", "interest_text", "award_amount_cr", "award_amount_text",
                               "total_exposure_cr", "amount_cr", "amount_text", "amount_context",
                               "amount_to_market_cap_pct", "authority"):
                        x.pop(_k, None)
                    if x.get("detail_source") in {"local_pdf", "pdf_local"}:
                        x.pop("detail_source", None)
            # Refresh deterministic stage/type metadata on retained history.
            for k in ("event_type", "stage"):
                x.pop(k, None)
            x.update(_event_meta(x.get("subject", ""), x.get("text", ""), cat))
            if cat == "Negative" and (x.get("cancels") or (x.get("category_override") == "Negative"
                                                         and _ORDER_CANCEL.search(f"{x.get('subject', '')} {x.get('text', '')}"))):
                x["negative_type"], x["negative_stage"] = "Order Cancellation", "Cancelled"
                x.pop("authority", None)
                if x.get("amount_cr") is None and x.get("cancels"):
                    _o = next((y for y in history.get(sym, []) if y.get("id") == x["cancels"]), None)
                    if _o and _o.get("order_value_cr") is not None:
                        x["amount_cr"] = _o["order_value_cr"]
            # Decisions made by the PDF order check survive the daily re-tagging.
            if x.get("event_type_override"):
                x["event_type"] = x["event_type_override"]
            if x.get("stage_override"):
                x["stage"] = x["stage_override"]
            cleaned.append(x)
        history[sym] = cleaned
        if not history[sym]:
            del history[sym]
    if removed_noise or reclassified:
        print(f"  🧹 Historical cleanup → removed={removed_noise}, reclassified={reclassified}")

    stale_negative_scrubbed = scrub_negative_metadata_from_nonnegative(history)
    if stale_negative_scrubbed:
        print(f"  🧽 Negative metadata scrub → cleaned={stale_negative_scrubbed} non-Negative card(s)")

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

    suppressed_skipped = 0
    if suppressed:
        for _sym in list(new_items):
            _kept = [it for it in new_items[_sym] if it.get("id") not in suppressed]
            suppressed_skipped += len(new_items[_sym]) - len(_kept)
            if _kept:
                new_items[_sym] = _kept
            else:
                del new_items[_sym]
    if suppressed_skipped:
        print(f"  🚫 Previously removed cards skipped → {suppressed_skipped}")

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
                _it["negative_parser_version"] = NEG_PARSER_VERSION

    # Internal join key must never be persisted.
    for _items in new_items.values():
        for _it in _items:
            _it.pop("_lookup_symbol", None)

    added = merge_catalysts(history, new_items, today, HISTORY_DAYS)
    _post_merge = {x.get("id"): x.get("dt", "") for items in history.values() for x in items
                   if x.get("id") and not x.get("manual")}

    # Drain the never-opened backlog (rows stored before their PDF was checked).
    # Runs after merge so expired rows are already gone and fresh rows that a
    # rebuild skipped are included. Uses the primed NSE session like fresh enrichment.
    backfill_local_history(nse_session, history, market_cap_map, ttm_sales_map)

    # Cross-check values against the NSE headline before lifecycle consolidation,
    # so merged cards carry the corrected figure.
    apply_headline_quality(history, market_cap_map, ttm_sales_map)

    # One-time re-check of stages/categories set by the first order-check version.
    recheck_order_stages(nse_session, history)

    # Order check: heading value → PDF type check → AI only when needed.
    process_orders(nse_session, history, market_cap_map, ttm_sales_map)

    # Corporate actions: stage (recommended vs shareholder-approved) and dates from the PDF.
    process_corp_actions(nse_session, history)

    # Defensive final hygiene after merge: no Negative-only metadata may survive
    # on a card whose final category is something else.
    scrub_negative_metadata_from_nonnegative(history)

    # Negative v1.3: when a later filing explicitly sets aside / grants relief
    # on the same monetary matter, suppress the older adverse card.
    negative_relief_removed = consolidate_negative_relief_lifecycles(history)
    if negative_relief_removed:
        print(f"  🔗 Negative relief lifecycle → removed={negative_relief_removed} superseded adverse card(s)")

    # Collapse duplicate lifecycle filings only after old + fresh rows are merged,
    # so L1 -> award, announced -> completed, and scheme stage updates can meet.
    lifecycle_removed = consolidate_lifecycles(history)
    if lifecycle_removed:
        print(f"  🔗 Lifecycle consolidation → removed={lifecycle_removed} duplicate stage card(s)")

    total = sum(len(v) for v in history.values())

    _final_ids = {x.get("id") for items in history.values() for x in items}
    _newly_suppressed = {k: v for k, v in _post_merge.items() if k not in _final_ids}
    suppressed.update(_newly_suppressed)
    if _newly_suppressed:
        print(f"  🗂 Suppressed for future runs → +{len(_newly_suppressed)} (total {len(suppressed)})")

    payload = {
        "suppressed": suppressed,
        "updated": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source": source,
        "data": history,
    }
    _r2_put_json(r2_session, "nse_catalysts.json", payload)

    # Per-symbol 3-year archive for chart markers. Runs after the main upload so
    # an archive problem can never block the live file.
    try:
        update_symbol_archives(r2_session, history, suppressed, today)
    except Exception as e:
        print(f"  ⚠ Symbol archive step failed ({e})")

    print(f"  ✓ Catalyst scan complete: source={source}, fetched={fetched}, "
          f"new={added}, symbols={len(history)}, stored={total}")


if __name__ == "__main__":
    main()
