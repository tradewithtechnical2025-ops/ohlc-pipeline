#!/usr/bin/env python3
"""
RHP Summary Pipeline — GitHub Actions
Reads ipo_data.json — either a local copy if pipeline_ipo.py wrote one in
this same job, or fetched directly from R2 otherwise — downloads the RHP/DRHP
PDF for each IPO that has one, extracts key sections, summarizes via the
Gemini API (free tier — no card required), and uploads the resulting
structured JSON to R2 at ipo_summaries/{id}.json.

Can run either as a step right after pipeline_ipo.py in the same job (fast
path: reads the local file, no extra network round-trip) or entirely on its
own schedule (falls back to GET {WORKER_URL}/ipo_data.json with the same
X-Secret-Token header used for uploads — the same pattern already proven in
vcp_test_scanner.py's download_all_chunks(), i.e. this worker's GET path
takes the service token directly; no Firebase user auth needed here).

Progress is tracked in a small manifest file (rhp_manifest.json) that this
script commits back to the repo, so already-processed IPOs are skipped on
future runs.

Required environment variables:
  GEMINI_API_KEY   — from https://aistudio.google.com/app/apikey (free, no card)
  WORKER_URL       — same R2 upload Worker used by pipeline_ipo.py
  WORKER_TOKEN     — same secret token used by pipeline_ipo.py

Optional:
  GEMINI_MODEL     — defaults to "gemini-3.6-flash". Google renames/retires
                     these periodically; if you start seeing 404 errors,
                     check https://aistudio.google.com/app/apikey for the
                     current free-tier model name and set this env var.
  IPO_DATA_PATH    — defaults to "ipo_data.json". If present locally, used
                     as-is; otherwise fetched fresh from R2 under this same
                     filename.
  MANIFEST_PATH    — defaults to "rhp_manifest.json"
  MAX_PER_RUN      — cap how many new PDFs to process in one run (default 8,
                     to keep job duration and API usage predictable)

Usage:
  python pipeline_rhp_summary.py
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime

import httpx
import pdfplumber
import io

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")
WORKER_URL     = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN   = os.environ["WORKER_TOKEN"]

IPO_DATA_PATH  = os.environ.get("IPO_DATA_PATH", "ipo_data.json")
MANIFEST_PATH  = os.environ.get("MANIFEST_PATH", "rhp_manifest.json")
MAX_PER_RUN    = int(os.environ.get("MAX_PER_RUN", "8"))

GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
}
GEMINI_DELAY_SEC = 4  # stay well within free-tier RPM limits


# ══════════════════════════════════════════════════════════════
# MANIFEST (tracks which IPO ids have already been processed)
# ══════════════════════════════════════════════════════════════

def load_manifest() -> dict:
    if os.path.exists(MANIFEST_PATH):
        try:
            return json.load(open(MANIFEST_PATH))
        except Exception:
            log.warning("Manifest file unreadable, starting fresh")
    return {"processed": {}}


def save_manifest(manifest: dict):
    json.dump(manifest, open(MANIFEST_PATH, "w"), indent=2)


# ══════════════════════════════════════════════════════════════
# SECTION LOCATION (ported + validated from the browser tool)
# ══════════════════════════════════════════════════════════════

def looks_like_toc(page_text: str) -> bool:
    """Table of Contents pages are dense with 'dot leader' patterns like
    '....... 123'. A real section heading page essentially never has more
    than one or two — this lets us skip the TOC even when it runs longer
    than any fixed page-offset guess would cover."""
    return len(re.findall(r"\.{4,}\s*\d+", page_text)) >= 3


def find_page(pages: list[str], patterns: list[re.Pattern], from_idx: int = 0) -> int:
    for i in range(from_idx, len(pages)):
        if looks_like_toc(pages[i]):
            continue
        for p in patterns:
            if p.search(pages[i]):
                return i
    return -1


def locate_sections(pages: list[str]) -> dict:
    """Returns page-index ranges for each section. Patterns are deliberately
    case-SENSITIVE: real headings are always ALL CAPS in these documents,
    while the same phrase constantly appears in ordinary lowercase prose
    elsewhere ('...based on the restated financial information...') —
    case-insensitive matching was grabbing those prose mentions instead."""
    risk_idx = find_page(pages, [
        re.compile(r"SECTION\s*II[\s\S]{0,40}RISK FACTORS"),
        re.compile(r"^RISK FACTORS$", re.M),
    ], 5)

    obj_idx = find_page(pages, [re.compile(r"OBJECTS OF THE (OFFER|ISSUE)")],
                         risk_idx + 18 if risk_idx >= 0 else 0)

    fin_from = risk_idx + 18 if risk_idx >= 0 else 10
    fin_idx = find_page(pages, [
        re.compile(r"SUMMARY OF (OUR )?FINANCIAL (INFORMATION|STATEMENTS)"),
        re.compile(r"RESTATED (CONSOLIDATED )?(STATEMENT OF ASSETS|FINANCIAL INFORMATION)"),
        re.compile(r"RESTATED STATEMENT OF PROFIT AND LOSS"),
    ], fin_from)

    biz_idx = find_page(pages, [re.compile(r"OUR BUSINESS"), re.compile(r"^BUSINESS OVERVIEW", re.M)])

    return {"risk": risk_idx, "objects": obj_idx, "financials": fin_idx, "business": biz_idx}


def join_range(pages: list[str], start: int, count: int) -> str:
    if start < 0:
        return ""
    return "\n\n".join(pages[start:start + count])


# ══════════════════════════════════════════════════════════════
# PDF PROCESSING
# ══════════════════════════════════════════════════════════════

def extract_pdf_sections(pdf_bytes: bytes) -> dict:
    """Two-pass extraction: a fast plain-text pass across every page to
    locate section boundaries, then a slower layout-preserving re-extraction
    of just the ~30 pages we actually need (layout=True keeps table columns
    associated with their row labels — critical for financial tables)."""
    quick_pages = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            quick_pages.append(p.extract_text() or "")
            p.flush_cache()

    idx = locate_sections(quick_pages)
    log.info(f"  section pages: {idx}")

    needed_ranges = []
    if idx["risk"] >= 0: needed_ranges.append((idx["risk"], idx["risk"] + 18))
    if idx["objects"] >= 0: needed_ranges.append((idx["objects"], idx["objects"] + 4))
    if idx["financials"] >= 0: needed_ranges.append((idx["financials"], idx["financials"] + 10))
    if idx["business"] >= 0: needed_ranges.append((idx["business"], idx["business"] + 3))
    needed_ranges.append((0, 4))  # cover pages / offer details

    needed_page_nums = set()
    for start, end in needed_ranges:
        for n in range(start, min(end, len(quick_pages))):
            needed_page_nums.add(n)

    layout_pages = {}
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for n in sorted(needed_page_nums):
            layout_pages[n] = pdf.pages[n].extract_text(layout=True) or ""
            pdf.pages[n].flush_cache()

    def render(start, count):
        if start < 0:
            return ""
        return "\n\n".join(layout_pages.get(n, "") for n in range(start, min(start + count, len(quick_pages))))

    return {
        "risk_factors": render(idx["risk"], 18),
        "objects": render(idx["objects"], 4),
        "financials": render(idx["financials"], 10),
        "business": render(idx["business"], 3),
        "offer_details": render(0, 4),
    }


# ══════════════════════════════════════════════════════════════
# PROMPT + GEMINI
# ══════════════════════════════════════════════════════════════

def build_prompt(sections: dict, company_name: str) -> str:
    return f"""You are analyzing an Indian IPO's Red Herring Prospectus (RHP) for a retail-investor-facing summary card. Extract and compute the following from the excerpts below, and respond with ONLY a single valid JSON object (no markdown fences, no commentary) matching EXACTLY this schema:

{{
  "company_name": string,
  "business_summary": "2-4 sentence plain-English description of what the company does",
  "objects_of_issue": {{ "status": "extracted", "note": "one sentence on what the fresh issue proceeds will fund" }},
  "key_risks": [array of 5-8 strings — the most material, specific, quantified risk factors, each 1-2 sentences, prioritizing ones with numbers/percentages over generic boilerplate risks],
  "financials_lakhs": {{
    "currency_unit": "INR Lakhs",
    "revenue_from_operations": {{"FY<year>": number, ...}},
    "pat": {{"FY<year>": number, ...}},
    "ebitda": {{"FY<year>": number, ...}} (COMPUTE as PBT + Finance Costs + Depreciation - Other Income for each year if a P&L table is present; omit this field entirely if you cannot compute it),
    "operating_cash_flow": {{"FY<year>": number, ...}} (omit if not found),
    "note": "one sentence flagging anything unusual (e.g. one-off income, declining margins) or noting what wasn't found"
  }},
  "customer_concentration_pct": {{"top_1": number, "top_5": number, "top_10": number, "period": "FY<year>"}} (include only the keys you actually found; omit entire field if not mentioned),
  "supplier_concentration_pct": {{"top_10": number, "period": "FY<year>"}} (omit entire field if not mentioned),
  "issue_structure": {{
    "fresh_pct": number or null, "ofs_pct": number or null,
    "fresh_issue_cr": number or null, "ofs_cr": number or null,
    "fresh_issue_shares": number or null, "ofs_shares": number or null,
    "selling_shareholders": [array of strings, empty if 100% fresh issue],
    "note": "one sentence describing the fresh/OFS split and whether any selling shareholder has a notably low cost of acquisition (WACA)"
  }},
  "promoters": [array of promoter names],
  "registrar": string, "lead_manager": string, "listing_exchange": "BSE and NSE" or as applicable
}}

Rules:
- The FINANCIAL SUMMARY excerpt below may contain a Restated Statement of Assets & Liabilities (balance sheet — ignore for financials_lakhs) followed by a Restated Statement of Profit and Loss (this is what you need). Columns are usually ordered most-recent-year first. Numbers may be in ₹ Millions, ₹ Lakhs, or ₹ Crores — convert everything to Lakhs (1 million = 10 lakhs, 1 crore = 100 lakhs).
- Before deciding a financial figure is unavailable, look carefully for the P&L table even if interleaved with balance sheet data or spanning a page break.
- Use only information present in the excerpts below. Do not invent numbers. If a field genuinely cannot be found, use null or omit the key, and mention the gap in the relevant "note" field.

Company name (if known): {company_name or '(not provided — extract from the document)'}

=== EXCERPT: OFFER DETAILS / COVER PAGES ===
{sections['offer_details'][:8000]}

=== EXCERPT: RISK FACTORS ===
{sections['risk_factors'][:20000]}

=== EXCERPT: OBJECTS OF THE OFFER ===
{sections['objects'][:6000]}

=== EXCERPT: FINANCIAL SUMMARY / RESTATED FINANCIALS ===
{sections['financials'][:20000]}

=== EXCERPT: BUSINESS OVERVIEW ===
{sections['business'][:4000]}
"""


async def call_gemini(client: httpx.AsyncClient, prompt: str) -> str:
    url = f"{GEMINI_URL}?key={GEMINI_API_KEY}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.15, "maxOutputTokens": 8192, "responseMimeType": "application/json"},
    }
    r = await client.post(url, json=body, timeout=90)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini API error {r.status_code}: {r.text[:400]}")
    data = r.json()
    candidates = data.get("candidates") or []
    if not candidates or "content" not in candidates[0]:
        raise RuntimeError(f"Unexpected Gemini response shape: {json.dumps(data)[:400]}")
    parts = candidates[0]["content"].get("parts") or []
    return "".join(p.get("text", "") for p in parts)


def parse_json_response(raw_text: str) -> dict:
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        cleaned = re.sub(r"^```json\s*", "", raw_text.strip(), flags=re.I)
        cleaned = re.sub(r"```\s*$", "", cleaned).strip()
        return json.loads(cleaned)


# ══════════════════════════════════════════════════════════════
# R2 UPLOAD  (same pattern as pipeline_ipo.py)
# ══════════════════════════════════════════════════════════════

async def r2_upload(client: httpx.AsyncClient, filename: str, data: dict):
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode()
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(url, headers={"X-Secret-Token": WORKER_TOKEN, "Content-Type": "application/json"},
                           content=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"R2 upload failed for {filename}: HTTP {r.status_code} — {r.text[:200]}")
    log.info(f"  ↑ {filename} ({len(payload)/1024:.1f} KB)")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

async def process_one_ipo(client: httpx.AsyncClient, entry: dict) -> dict | None:
    ipo_id = entry["id"]
    name = entry.get("name", "")
    doc_url = entry.get("rhp_url") or entry.get("drhp_url")
    if not doc_url:
        return None

    log.info(f"→ {name} ({ipo_id})")
    log.info(f"  downloading {doc_url}")
    try:
        resp = await client.get(doc_url, headers=DOWNLOAD_HEADERS, timeout=60, follow_redirects=True)
        resp.raise_for_status()
        pdf_bytes = resp.content
        if len(pdf_bytes) < 5000 or not pdf_bytes.startswith(b"%PDF"):
            raise RuntimeError(f"response doesn't look like a PDF ({len(pdf_bytes)} bytes)")
    except Exception as e:
        log.warning(f"  ✗ download failed: {e}")
        return {"ipo_id": ipo_id, "status": "download_failed", "error": str(e)}

    try:
        sections = extract_pdf_sections(pdf_bytes)
        if not sections["risk_factors"] and not sections["financials"]:
            log.warning("  WARNING: could not locate main sections — results may be thin")

        prompt = build_prompt(sections, name)
        raw_text = await call_gemini(client, prompt)
        parsed = parse_json_response(raw_text)
    except Exception as e:
        log.warning(f"  ✗ processing failed: {e}")
        return {"ipo_id": ipo_id, "status": "processing_failed", "error": str(e)}

    parsed["ipo_id"] = ipo_id
    parsed["rhp_source"] = doc_url
    parsed["generated_at"] = datetime.now().strftime("%Y-%m-%d")

    try:
        await r2_upload(client, f"ipo_summaries/{ipo_id}.json", parsed)
    except Exception as e:
        log.warning(f"  ✗ upload failed: {e}")
        return {"ipo_id": ipo_id, "status": "upload_failed", "error": str(e)}

    log.info(f"  ✅ done: {ipo_id}")
    return {"ipo_id": ipo_id, "status": "done"}


async def load_ipo_data(client: httpx.AsyncClient) -> dict:
    """Prefer a local ipo_data.json if pipeline_ipo.py happens to have written
    one in this job; otherwise fetch it from R2 directly, using the same
    GET-with-X-Secret-Token pattern already proven in vcp_test_scanner.py's
    download_all_chunks() — this worker's GET path takes the secret token
    the same way its POST (upload) path does, no Firebase auth needed for
    server-to-server calls."""
    if os.path.exists(IPO_DATA_PATH):
        log.info(f"Found local {IPO_DATA_PATH}, using it")
        return json.load(open(IPO_DATA_PATH))

    log.info(f"No local {IPO_DATA_PATH} — fetching from R2 instead")
    r = await client.get(f"{WORKER_URL}/ipo_data.json", headers={"X-Secret-Token": WORKER_TOKEN}, timeout=90)
    if r.status_code != 200:
        log.error(f"❌ Could not fetch ipo_data.json from R2 either: HTTP {r.status_code} — {r.text[:200]}")
        sys.exit(1)
    return r.json()


async def run():
    log.info("━━━ RHP Summary Pipeline ━━━")

    async with httpx.AsyncClient() as client:
        ipo_data = await load_ipo_data(client)
        ipos = ipo_data.get("ipos", [])

        manifest = load_manifest()
        processed = manifest["processed"]

        candidates = [x for x in ipos if (x.get("rhp_url") or x.get("drhp_url")) and x["id"] not in processed]
        log.info(f"{len(candidates)} IPOs have a document and are not yet processed "
                 f"(of {len(ipos)} total, {len(processed)} already done)")

        todo = candidates[:MAX_PER_RUN]
        if len(candidates) > MAX_PER_RUN:
            log.info(f"Processing {MAX_PER_RUN} this run (MAX_PER_RUN cap); {len(candidates) - MAX_PER_RUN} remain for next run")

        if not todo:
            log.info("Nothing to do.")
            return

        for i, entry in enumerate(todo):
            result = await process_one_ipo(client, entry)
            if result:
                processed[result["ipo_id"]] = {
                    "status": result["status"],
                    "at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                }
                if result["status"] != "done":
                    processed[result["ipo_id"]]["error"] = result.get("error", "")
                save_manifest(manifest)  # save after every item so partial progress isn't lost
            if i < len(todo) - 1:
                await asyncio.sleep(GEMINI_DELAY_SEC)

    done = sum(1 for v in processed.values() if v.get("status") == "done")
    log.info(f"━━━ Pipeline complete. Manifest: {done} done / {len(processed)} tracked total ━━━")


if __name__ == "__main__":
    asyncio.run(run())
