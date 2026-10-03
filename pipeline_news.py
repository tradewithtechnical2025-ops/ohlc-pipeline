                seen_pdf_links.add(link)
                pdf_candidates_now.append(it)
                cal_fb += 1
            if cal_fb:
                print(f"  + {cal_fb} NSE PDF(s) added via result_calendar.json (no results wording in subject/text)")
        existing_pdf_feed = await r2_get(client, "nse_results_pdf_feed.json")
        existing_pdf_items = (existing_pdf_feed or {}).get("items", [])
        all_pdf_candidates = dedup_items(pdf_candidates_now + existing_pdf_items)

        before_cal = len(all_pdf_candidates)
        if calendar_payload:
            kept_pdf_candidates = []
            explicit_result_bypass = 0
            for it in all_pdf_candidates:
                # Explicit NSE wording is stronger evidence than the archive
                # filename prefix. The prefix can be an old/internal company
                # code rather than the live NSE symbol (PCPL -> PRANAV), so do
                # not let that mismatch suppress a confirmed results filing.
                if _has_explicit_financial_results_text(it):
                    kept_pdf_candidates.append(it)
                    explicit_result_bypass += 1
                    continue

                # Generic board outcomes still need the calendar guard; this
                # preserves the existing protection against NCD/KMP/fundraise
                # PDFs that share the same generic NSE subject.
                if _in_result_calendar(
                    _extract_filename_symbol(it.get("link", "")),
                    calendar_payload,
                    it.get("link", ""),
                ):
                    kept_pdf_candidates.append(it)

            all_pdf_candidates = kept_pdf_candidates
            dropped_cal = before_cal - len(all_pdf_candidates)
            if explicit_result_bypass:
                print(f"  ✓ {explicit_result_bypass} announcement(s) kept by explicit financial-results text "
                      f"(calendar symbol check bypassed; handles NSE filename/symbol mismatches)")
            if dropped_cal:
                print(f"  🗑 {dropped_cal} announcement(s) dropped — no explicit financial-results text and "
                      f"filename symbol not on result_calendar.json for that date")

        merged_pdf_feed = all_pdf_candidates
        merged_pdf_feed.sort(key=_effective_ts, reverse=True)
        merged_pdf_feed = merged_pdf_feed[:500]
        print(f"  nse_results_pdf_feed.json: {len(existing_pdf_items)} existing + "
              f"{max(len(merged_pdf_feed) - len(existing_pdf_items), 0)} new = {len(merged_pdf_feed)} (capped at 500)")
        await r2_put(client, "nse_results_pdf_feed.json", make_payload(merged_pdf_feed))

        # ── BSE results-PDF candidates ──
        # Same rolling-accumulator pattern as nse_results_pdf_feed.json
        # above (BSE's feed only ever shows its latest snapshot too), now
        # with the same result_calendar.json cross-check NSE gets — this
        # is what catches filings like Infrastructure Leasing & Financial
        # Services' "revised audited standalone financial results for FY
        # 2018-19" (a stale NCLT-resolution correction, not a current
        # quarter, but one that still matches _is_bse_results_pdf's text
        # pattern): the symbol just isn't on the calendar for that date,
        # so it's dropped here rather than reaching Telegram/storage as if
        # it were today's result. bse_symbol_map is loaded first (moved up
        # from just before build_results_detailed) since resolving each
        # item's probable symbol is needed for the calendar lookup itself,
        # not only for later parsing.
        bse_symbol_map = await _load_bse_symbol_map(client)
        bse_sorted = _dedup_bse_by_link(
            sorted(result_map.get("bse_announcements", []), key=lambda x: x.get("published_ts", 0), reverse=True)
        )
        bse_candidates_now = [it for it in bse_sorted if _is_bse_results_pdf(it)]
        # Extra candidates (calendar-listed symbol, or generic board outcome). They
        # bypass the calendar gate below on purpose - the PDF heading check decides.
        bse_extra = []
        for it in bse_sorted:
            if _is_bse_results_pdf(it):
                continue
            kind = _bse_extra_candidate_kind(it, calendar_payload, bse_symbol_map)
            if kind == "calendar":
                it["_cal_fallback"] = True
            elif kind == "loose":
                it["_loose"] = True
            else:
                continue
            bse_extra.append(it)
        if bse_extra:
            print(f"  + {len(bse_extra)} BSE PDF(s) added as extra candidates "
                  f"({sum(1 for i in bse_extra if i.get('_cal_fallback'))} via calendar, "
                  f"{sum(1 for i in bse_extra if i.get('_loose'))} generic board-outcome)")
        if calendar_payload:
            before_bse_cal = len(bse_candidates_now)
            bse_candidates_now = [
                it for it in bse_candidates_now
                if _in_result_calendar(_pdf_probable_symbol(it, bse_symbol_map), calendar_payload, it.get("link", ""),
                                        explicit_date=_bse_fallback_date(it.get("published", "")))
            ]
            dropped_bse_cal = before_bse_cal - len(bse_candidates_now)
            if dropped_bse_cal:
                print(f"  🗑 {dropped_bse_cal} BSE announcement(s) dropped — symbol not on result_calendar.json "
                      f"for that date (likely a stale/non-current-quarter filing despite matching the results text pattern)")
        bse_candidates_now = bse_candidates_now + bse_extra
        existing_bse_feed = await r2_get(client, "bse_results_pdf_feed.json")
        existing_bse_items = (existing_bse_feed or {}).get("items", [])
        merged_bse_feed = _dedup_bse_by_link(dedup_items(bse_candidates_now + existing_bse_items))
        merged_bse_feed.sort(key=_effective_ts, reverse=True)
        merged_bse_feed = merged_bse_feed[:BSE_PDF_FEED_CAP]
        print(f"  bse_results_pdf_feed.json: {len(existing_bse_items)} existing + "
              f"{max(len(merged_bse_feed) - len(existing_bse_items), 0)} new = {len(merged_bse_feed)} (capped at {BSE_PDF_FEED_CAP})")
        await r2_put(client, "bse_results_pdf_feed.json", make_payload(merged_bse_feed))

        # ── Financial results detail (P&L from XBRL / AI-extracted PDF) ──
        print("\nParsing financial results (PDF-only)...")
        fundamentals = await r2_get(client, FUNDAMENTALS_FILE)
        fundamentals_stocks = (fundamentals or {}).get("stocks")
        if not fundamentals_stocks:
            print(f"  ⚠ {FUNDAMENTALS_FILE} unavailable — YoY fallback via fundamentals disabled this run")
        detailed_payload = await build_results_detailed(client, results_feed_items, merged_pdf_feed, fundamentals_stocks,
                                                          bse_pdf_items=merged_bse_feed, bse_symbol_map=bse_symbol_map)
        if detailed_payload:
            await r2_put(client, "nse_results_detailed.json", detailed_payload)

    print("✅ Done")


if __name__ == "__main__":
    asyncio.run(run())
