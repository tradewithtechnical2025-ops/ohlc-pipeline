"""recover_results.py — one-time rebuild of nse_results_detailed.json from results_ai_checkpoint.json.

The results file lost its history (a failed read was taken as "no file"). Every PDF the pipeline sent
to Gemini is still in results_ai_checkpoint.json (filename -> AI response). This script turns those
responses back into result records with the pipeline's own _build_result_from_ai(), using the
candidate feeds (nse/bse_results_pdf_feed.json) for each file's link, title, date and BSE code, then
merges them with what is stored now. No PDF downloads, no Gemini calls, no Telegram.

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

    # Consolidated preferred: drop a Standalone record when the same symbol + quarter has a Consolidated one
    allr = items + rebuilt
    cons = {(k[0], k[1]) for k in map(key, allr) if k[2] == "consolidated"}
    merged = [r for r in allr if not (key(r)[2] == "standalone" and (key(r)[0], key(r)[1]) in cons)]
    for r in merged:
        pn._apply_grade(r)
    merged.sort(key=pn._effective_ts, reverse=True)
    merged = merged[:1000]
    days = {}
    for r in merged:
        d = str((r.get("meta") or {}).get("board_meeting_date") or "")[:10]
        days[d] = days.get(d, 0) + 1
    print(f"rebuilt {len(rebuilt)} result(s); skipped: " + ", ".join(f"{k} {n}" for k, n in skipped.items() if n))
    print(f"merged file: {len(merged)} result(s)")
    for d in sorted(days, reverse=True)[:12]:
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
