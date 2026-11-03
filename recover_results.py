"""recover_results.py — one-time rebuild of nse_results_detailed.json from results_ai_checkpoint.json.

The results file lost its history (a failed read was taken as "no file"). Every PDF the pipeline sent
to Gemini is still in results_ai_checkpoint.json (filename -> AI response). This script turns those
responses back into result records with the pipeline's own _build_result_from_ai(), using the
candidate feeds (nse/bse_results_pdf_feed.json) for each file's link, title, date and BSE code, then
merges them with what is stored now. No PDF downloads, no Gemini calls, no Telegram.

Results older than the checkpoint (before 28 Oct 2026) are then filled from fundamentals_rich.json
(pipeline_fundamentals_prod.py): every stock whose latest quarter is the target quarter and is not
in the file yet gets a record with the full P&L (Sales, Other income, OP, Interest, Depreciation,
PBT, Tax, PAT, EPS) and QoQ / YoY priors. These records have no PDF link and no AI highlights;
price_reaction is added by backfill.py's next run. Turn this step off with --no-fundamentals.

    WORKER_URL=... WORKER_TOKEN=... python recover_results.py            # dry run: prints what it would add
    WORKER_URL=... WORKER_TOKEN=... python recover_results.py --upload   # saves the merged file to R2
Offline test (local copies of the four files): python recover_results.py --local DIR
"""
import asyncio
import json
import os
import sys

import httpx

import pipeline_news as pn

TARGET_PERIOD = os.environ.get("PERIOD_END", "2026-09-30")     # quarter to fill from fundamentals


def _fy_label(iso):
    y, m = int(iso[:4]), int(iso[5:7])
    q = {6: 1, 9: 2, 12: 3, 3: 4}.get(m)
    fy = y + 1 if m >= 4 else y
    return f"Q{q} FY{fy % 100:02d}" if q else ""


def _pe_iso(pe):
    s = str(pe or "")
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 and s.isdigit() else None


def _fund_record(sym, e):
    """One result record (pipeline_news shape) from a fundamentals_rich.json entry, or None."""
    qs = e.get("quarters") or []
    if not qs or _pe_iso(qs[0].get("period_end")) != TARGET_PERIOD:
        return None
    rd = e.get("last_result_date")
    if not rd or rd <= TARGET_PERIOD:          # a result can only be declared after the quarter ends
        return None
    cur = qs[0]
    by_end = {x.get("period_end"): x for x in qs}
    prev = qs[1] if len(qs) > 1 else None
    pe = cur.get("period_end")
    yago = by_end.get(pe - 10000) if isinstance(pe, int) else None
    bank = e.get("company_type") == "bank"
    R = lambda v: None if v is None else v * 1e7            # crore -> rupees, as pipeline_news stores
    pct = lambda c, p: None if (c is None or p in (None, 0)) else round((c - p) / abs(p) * 100, 2)

    q = {"revenue": R(cur.get("sales")), "other_income": R(cur.get("other_income")),
         "finance_costs": R(cur.get("interest")), "depreciation": R(cur.get("depreciation")),
         "exceptional_items": R(cur.get("exceptional")), "pbt": R(cur.get("pbt")),
         "tax_expense": R(cur.get("tax")), "pat": R(cur.get("pat_attr") if cur.get("pat_attr") is not None else cur.get("pat")),
         "eps_basic": cur.get("eps"), "period_end": TARGET_PERIOD}
    if q["revenue"] is not None and q["other_income"] is not None:
        q["total_income"] = q["revenue"] + q["other_income"]
    if bank:
        q.update({"financial_sector": "bank", "nii": R(cur.get("nii")), "ppop": R(cur.get("ppop"))})
    else:
        q["operating_profit"] = R(cur.get("ebitda"))
        q["ebitda"] = q["operating_profit"]
        if cur.get("opm") is not None:
            q["opm"] = round(cur["opm"] / 100, 4)

    def prior(p, tag):
        if not p:
            return None
        P = lambda k: p.get("pat_attr") if (k == "pat" and p.get("pat_attr") is not None) else p.get(k)
        f = {"basis": "fundamentals", "basis_verified": True, "prior_header": p.get("period") or "",
             "sales_prior": R(p.get("sales")), f"sales_{tag}_pct": pct(cur.get("sales"), p.get("sales")),
             "pat_prior": R(P("pat")), f"pat_{tag}_pct": pct(q["pat"] and q["pat"] / 1e7, P("pat")),
             "eps_prior": p.get("eps"), f"eps_{tag}_pct": pct(cur.get("eps"), p.get("eps"))}
        if p.get("sales") is not None and p.get("other_income") is not None:
            f["total_income_prior"] = R(p["sales"] + p["other_income"])
        if bank:
            for k in ("nii", "ppop"):
                f[f"{k}_prior"] = R(p.get(k)); f[f"{k}_{tag}_pct"] = pct(cur.get(k), p.get(k))
        else:
            f["operating_profit_prior"] = R(p.get("ebitda"))
            f[f"operating_profit_{tag}_pct"] = pct(cur.get("ebitda"), p.get("ebitda"))
            f["opm_prior"] = p.get("opm")
            if cur.get("opm") is not None and p.get("opm") is not None:
                f[f"opm_{tag}_pp"] = round(cur["opm"] - p["opm"], 2)
        return {k: v for k, v in f.items() if v is not None}

    rec = {"meta": {"symbol": sym, "company_name": e.get("name") or sym, "board_meeting_date": rd,
                    "standalone_consolidated": "Consolidated" if e.get("basis") == "c" else "Standalone",
                    "audited": None, "quarter_label": _fy_label(TARGET_PERIOD), "scrip_code": e.get("bse_code"),
                    "exchange": e.get("exchange") or ("NSE" if e.get("nse_code") else "BSE"),
                    "source": "fundamentals", "extraction_method": "fundamentals"},
           "quarter": {k: v for k, v in q.items() if v is not None},
           "title": e.get("name") or sym, "published": "", "published_ts": 0}
    qf, yf = prior(prev, "qoq"), prior(yago, "yoy")
    if qf: rec["qoq_fundamentals"] = qf
    if yf: rec["yoy_fundamentals"] = yf
    if q.get("revenue") is None and q.get("pat") is None:
        return None
    return rec


async def _get(client, name, local):
    if local:
        p = os.path.join(local, name)
        return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else None
    return await pn.r2_get(client, name)


async def main():
    upload = "--upload" in sys.argv
    local = sys.argv[sys.argv.index("--local") + 1] if "--local" in sys.argv else None
    async with httpx.AsyncClient() as client:
        if local:
            current = await _get(client, "nse_results_detailed.json", local)
        else:
            current = await pn._load_existing_results(client)   # strict: stops on a failed read
        ckpt = await _get(client, "results_ai_checkpoint.json", local) or {}
        feeds = []
        for name in ("nse_results_pdf_feed.json", "bse_results_pdf_feed.json"):
            feeds += ((await _get(client, name, local)) or {}).get("items", [])
        rich = None if "--no-fundamentals" in sys.argv else await _get(client, "fundamentals_rich.json", local)
        cls = await _get(client, "classification.json", local)
        bse_map = {}
        rows = cls if isinstance(cls, list) else (cls or {}).get("items") or []
        for row in rows:
            code, sym = (row.get("bse_code") or "").strip(), row.get("symbol")
            if code and sym and code not in bse_map:
                bse_map[code] = sym

    items = list((current or {}).get("items") or [])
    print(f"stored now: {len(items)} result(s); checkpoint: {len((ckpt.get('responses') or {}))} response(s); "
          f"feeds: {len(feeds)} filing(s); BSE codes mapped: {len(bse_map)}")
    by_file = {}
    for it in feeds:
        by_file.setdefault(it["link"].rsplit("/", 1)[-1], it)

    def key(r):
        m, q = r.get("meta") or {}, r.get("quarter") or {}
        return (str(m.get("symbol") or "").upper(), q.get("period_end"), (m.get("standalone_consolidated") or "").lower())

    have_links = {r.get("link") for r in items}
    have_keys = {key(r) for r in items}
    rebuilt, skipped = [], {"not a results statement": 0, "no feed entry": 0, "already stored": 0,
                             "older quarter": 0, "failed validation": 0}
    for v in (ckpt.get("responses") or {}).values():
        ai, fname = v.get("response") or {}, v.get("filename") or ""
        if not isinstance(ai, dict) or not ai.get("is_results_table") or not ai.get("current"):
            skipped["not a results statement"] += 1; continue
        it = by_file.get(fname)
        if not it:
            skipped["no feed entry"] += 1; continue
        if it["link"] in have_links:
            skipped["already stored"] += 1; continue
        bse = it.get("_exchange") == "BSE" or "bseindia.com" in it["link"]
        fdate = pn._bse_fallback_date(it.get("published", ""))
        try:
            from datetime import date as _d
            if pn.STALE_PERIOD_GATE and fdate and pn._is_stale_period(ai.get("period_end"), _d.fromisoformat(fdate)):
                skipped["older quarter"] += 1; continue
        except Exception:
            pass
        try:
            if bse:
                code = it.get("scripcode", "")
                r = pn._build_result_from_ai(ai, "", it["link"], fname, it.get("title", ""), scrip_code=code,
                                             fallback_board_meeting_date=fdate,
                                             symbol_override=bse_map.get(code, code), exchange="BSE")
            else:
                r = pn._build_result_from_ai(ai, "", it["link"], fname, it.get("title", ""),
                                             fallback_board_meeting_date=fdate)
        except Exception as e:
            print(f"  ⚠ {fname}: {type(e).__name__}: {e}")
            r = None
        if not r:
            skipped["failed validation"] += 1; continue
        r["link"] = it["link"]; r["title"] = it.get("title", "")
        r["published"] = it.get("published", ""); r["published_ts"] = it.get("published_ts", 0)
        if key(r) in have_keys:
            skipped["already stored"] += 1; continue
        have_keys.add(key(r)); have_links.add(it["link"])
        rebuilt.append(r)

    # Older results (before the checkpoint) from fundamentals_rich.json: one record per stock and quarter
    fund_added, fund_skip = [], {"already in file": 0, "same company stored under its BSE code": 0,
                                 "not this quarter / no result date": 0}
    have_sym_q = {(k[0], k[1]) for k in map(key, items + rebuilt)}
    # a company stored from a BSE filing may carry its BSE code (or the code as scrip_code) instead of the NSE symbol
    have_code_q = set()
    for r in items + rebuilt:
        m, pe = r.get("meta") or {}, (r.get("quarter") or {}).get("period_end")
        for c in (m.get("scrip_code"), m.get("symbol")):
            if c and str(c).isdigit():
                have_code_q.add((str(c), pe))
    for sym, e in ((rich or {}).get("stocks") or {}).items():
        r = _fund_record(sym, e or {})
        if not r:
            fund_skip["not this quarter / no result date"] += 1; continue
        k = key(r)
        if (k[0], k[1]) in have_sym_q:
            fund_skip["already in file"] += 1; continue
        code = str((e or {}).get("bse_code") or "")
        if code and (code, k[1]) in have_code_q:
            fund_skip["same company stored under its BSE code"] += 1; continue
        have_sym_q.add((k[0], k[1]))
        fund_added.append(r)
    if rich is not None:
        print(f"from fundamentals_rich.json: {len(fund_added)} older result(s) added; skipped: "
              + ", ".join(f"{k} {n}" for k, n in fund_skip.items() if n))

    # Consolidated preferred: drop a Standalone record when the same symbol + quarter has a Consolidated one
    allr = items + rebuilt + fund_added
    cons = {(k[0], k[1]) for k in map(key, allr) if k[2] == "consolidated"}
    merged = [r for r in allr if not (key(r)[2] == "standalone" and (key(r)[0], key(r)[1]) in cons)]
    for r in merged:
        pn._apply_grade(r)
    merged.sort(key=lambda r: (str((r.get("meta") or {}).get("board_meeting_date") or ""), pn._effective_ts(r)), reverse=True)
    days = {}
    for r in merged:
        d = str((r.get("meta") or {}).get("board_meeting_date") or "")[:10]
        days[d] = days.get(d, 0) + 1
    print(f"rebuilt {len(rebuilt)} result(s); skipped: " + ", ".join(f"{k} {n}" for k, n in skipped.items() if n))
    print(f"merged file: {len(merged)} result(s)")
    for d in sorted(days, reverse=True)[:40]:
        print(f"  {d}: {days[d]}")

    payload = pn.make_payload(merged)
    links = set((current or {}).get("processed_result_links") or []) | {r.get("link") for r in merged if r.get("link")}
    payload["processed_result_links"] = sorted(links)
    out = os.path.join(local or ".", "nse_results_detailed.recovered.json")
    json.dump(payload, open(out, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"written {out}")
    if upload:
        async with httpx.AsyncClient() as client:
            await pn.r2_put(client, "nse_results_detailed.json", payload)
        print("uploaded nse_results_detailed.json")
    else:
        print("dry run — nothing uploaded (add --upload to save it to R2)")


if __name__ == "__main__":
    asyncio.run(main())
