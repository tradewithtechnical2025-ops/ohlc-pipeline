# ══════════════════════════════════════════════════════════════
# PATCH — pipeline.py mein run_ep_scan() ko isse REPLACE karein
#
# Bug: EP/PR signals ke ltp, rs, sector, sales_ch, eps_ch screener.json
#      (legacy sheet export) se aate the — stale prices (VENUSREM ltp=1280
#      vs last_close=1715), empty ltp naye stocks ke liye, galat rs values
#      (AGARIND rs=-0.5).
#
# Fix: ltp = fresh OHLC last_close, rs = calculated rs_calc, sector =
#      classification.json, sales/eps = fundamentals.json, patterns =
#      aaj ke screener_feed se. Sheet sirf fallback.
# ══════════════════════════════════════════════════════════════

async def run_ep_scan() -> None:
    today=today_ist()
    log.info(f"━━━ EP + Post-Result + RS Scan  {today} ━━━")
    async with httpx.AsyncClient() as client:
        global ISIN_MAP,BSE_ISIN_MAP,BSE_META
        ISIN_MAP,BSE_ISIN_MAP,BSE_META=await build_isin_map(client)
        today_symbols=await get_result_symbols_finedge(client)
        if today_symbols: await save_result_calendar(client,today_symbols,today)
        ohlc_tasks=[r2_download(client,f"ohlc_{i+1}.json") for i in range(R2_CHUNKS)]
        (ohlc_results,screener_raw,fund_raw,cal_raw,classification,
         idx_hist_n50,idx_hist_n500,idx_hist_sm400,idx_daily,sheet_raw,
         hlr_raw,pb_raw,pat_raw)=await asyncio.gather(
            asyncio.gather(*ohlc_tasks,return_exceptions=True),
            r2_download(client,"screener.json"),r2_download_fund(client),
            r2_download(client,"result_calendar.json"),r2_download(client,"classification.json"),
            r2_download(client,f"index_history/{INDEX_SYMBOLS['nifty50']}.json"),
            r2_download(client,f"index_history/{INDEX_SYMBOLS['nifty500']}.json"),
            r2_download(client,f"index_history/{INDEX_SYMBOLS['smallmid400']}.json"),
            r2_download(client,"index_daily.json"),r2_download(client,"sheet_data.json"),
            r2_download(client,"hlr_signals.json"),r2_download(client,"pullback_signals.json"),
            r2_download(client,"pattern_signals.json"),
        )
        all_data={}
        for i,res in enumerate(ohlc_results):
            if isinstance(res,Exception): log.warning(f"  ohlc_{i+1}.json error: {res}")
            elif res and "stocks" in res: all_data.update(res["stocks"])
        log.info(f"Loaded {len(all_data)} stocks")
        screener={}
        if isinstance(screener_raw,list):
            for row in screener_raw:
                sym=(row.get("Stocks","") or "").strip()
                if not sym: continue
                try: sc=float(row.get("SALES CH%",0))*100; sales_ch=f"+{sc:.1f}%" if sc>=0 else f"{sc:.1f}%"
                except: sales_ch=""
                try: ec=float(row.get("EPS CHANGE",0))*100; eps_ch=f"+{ec:.1f}%" if ec>=0 else f"{ec:.1f}%"
                except: eps_ch=""
                pat_cols=["NR7","WIB","DIB","MCP","W-MCP","HVQ","VD","PullBack","ATR Tightness","Volume footprint","Launchpad","HLR","BS","GAPUP","PP","HPBC","TL/HL BO","3WTC"]
                combined=set()
                for p in (row.get("Patterns","") or "").split("||"):
                    p=p.strip()
                    if p: combined.add(p)
                for col in pat_cols:
                    v=row.get(col,"")
                    if v and v not in ("",None,0,"No"): combined.add(v if isinstance(v,str) else col)
                screener[sym]={"sales_ch":sales_ch,"eps_ch":eps_ch,"patterns":"||".join(sorted(combined)),"sector":row.get("SECTOR",""),"rs":row.get("RS Rating",""),"ltp":row.get("LTP","")}
        fund_lookup={}
        if isinstance(fund_raw,dict): fund_lookup=fund_raw
        elif isinstance(fund_raw,list): fund_lookup={d["symbol"]:d for d in fund_raw if d.get("symbol")}
        result_calendar=cal_raw if isinstance(cal_raw,dict) else {}
        classification=classification or []
        sheet_data={}
        if isinstance(sheet_raw,list):
            for row in sheet_raw:
                sym=row.get("symbol") or row.get("Stocks","")
                if sym: sheet_data[sym]={"circuit":row.get("Circuit") or row.get("circuit"),"tv_code":row.get("TV CODE") or row.get("tv_code",""),"hpbc":row.get("HPBC") or row.get("hpbc",""),"tl_hl_bo":row.get("TL/HL BO") or row.get("tl_hl_bo","")}
        elif isinstance(sheet_raw,dict): sheet_data=sheet_raw
        hlr_map={}
        if isinstance(hlr_raw,dict):
            for sig in (hlr_raw.get("signals") or []):
                sym=sig.get("symbol")
                if sym:
                    if sym not in hlr_map or sig.get("touches",0)>hlr_map[sym].get("touches",0): hlr_map[sym]=sig
        pb_map={}
        if isinstance(pb_raw,dict):
            for sig in (pb_raw.get("signals") or []):
                sym=sig.get("symbol")
                if sym: pb_map[sym]=sig
        pat_map={}
        if isinstance(pat_raw,dict):
            for sig in (pat_raw.get("signals") or []):
                sym=sig.get("symbol"); pat=sig.get("pattern")
                if sym and pat: pat_map.setdefault(sym,set()).add(pat)

        # ─── FIX: fresh enrichment helpers ───
        # Classification map — sector ke liye (sheet ki jagah)
        cls_map_ep={}
        for x in (classification or []):
            sym0=x.get("symbol") or x.get("nse_code")
            if sym0: cls_map_ep[sym0]=x

        def _fund_chg(fund):
            """fundamentals.json se q_name + YoY sales/eps change strings."""
            pl=fund.get("pl_quarterly",[])
            q_name=pl[0].get("header","") if pl else ""
            sales_ch=eps_ch=""
            if pl and len(pl)>=5:
                s0=pl[0].get("sales"); s4=pl[4].get("sales")
                if s0 and s4:
                    v=round((s0-s4)/s4*100,1); sales_ch=f"+{v}%" if v>=0 else f"{v}%"
                e0=pl[0].get("eps"); e4=pl[4].get("eps")
                if e0 and e4:
                    v=round((e0-e4)/e4*100,1); eps_ch=f"+{v}%" if v>=0 else f"{v}%"
            return q_name,sales_ch,eps_ch

        signals=_detect_ep(all_data)
        signals.sort(key=lambda x:(x["ep_date"],x["gap_pct"]),reverse=True)
        for sig in signals:
            sym=sig["symbol"]; sc=screener.get(sym,{}); ci=cls_map_ep.get(sym,{}); fund=fund_lookup.get(sym,{})
            q_name,sales_ch,eps_ch=_fund_chg(fund)
            sig.update({
                "sales_ch":sales_ch or sc.get("sales_ch",""),
                "eps_ch":eps_ch or sc.get("eps_ch",""),
                "patterns":sc.get("patterns",""),
                "sector":ci.get("sector_group") or sc.get("sector",""),
                "ltp":sig["last_close"],
                "q_name":q_name,
            })
            vol_x=sig.pop("vol_spike_x",1); sig["vol_pct"]=f"+{round((vol_x-1)*100)}%"
        pr_signals=[]
        if result_calendar:
            pr_signals=_detect_post_result_thrust(all_data,result_calendar)
            for sig in pr_signals:
                sym=sig["symbol"]; sc=screener.get(sym,{}); ci=cls_map_ep.get(sym,{}); fund=fund_lookup.get(sym,{})
                q_name,sales_ch,eps_ch=_fund_chg(fund)
                fresh_ltp=None
                if sym in all_data:
                    fresh_ltp=next((v for v in reversed(all_data[sym]["c"]) if v is not None),None)
                sig.update({
                    "sales_ch":sales_ch or sc.get("sales_ch",""),
                    "eps_ch":eps_ch or sc.get("eps_ch",""),
                    "patterns":sc.get("patterns",""),
                    "sector":ci.get("sector_group") or sc.get("sector",""),
                    "ltp":round(fresh_ltp,2) if fresh_ltp is not None else sc.get("ltp",""),
                    "q_name":q_name,
                })
        rs_data=_calculate_rs(all_data,history_days=90)
        rs_history_list=_build_rs_history_json(all_data,rs_data)
        # ─── FIX: rs bhi fresh calculated value se (sheet ka stale rs nahi) ───
        for sig in signals:
            rc=rs_data.get(sig["symbol"],{}).get("rs")
            sig["rs_calc"]=rc
            sig["rs"]=rc if rc is not None else sig.get("rs","")
        for sig in pr_signals:
            rc=rs_data.get(sig["symbol"],{}).get("rs")
            sig["rs_calc"]=rc
            sig["rs"]=rc if rc is not None else sig.get("rs","")
        idx_daily=idx_daily or {}
        index_maps={
            "nifty50":_build_index_close_map(idx_hist_n50,idx_daily.get(INDEX_SYMBOLS["nifty50"],{}).get("close"),today),
            "nifty500":_build_index_close_map(idx_hist_n500,idx_daily.get(INDEX_SYMBOLS["nifty500"],{}).get("close"),today),
            "smallmid400":_build_index_close_map(idx_hist_sm400,idx_daily.get(INDEX_SYMBOLS["smallmid400"],{}).get("close"),today),
        }
        mansfield=_calculate_mansfield_rs(all_data,index_maps)
        for sym in rs_data:
            m=mansfield.get(sym,{})
            for idx_key,metrics in m.items():
                for k,v in metrics.items(): rs_data[sym][f"{k}_{idx_key}"]=v
        sector_group_rs_history=_build_group_rs_history(classification,rs_history_list,"sector_group")
        industry_rs_history=_build_group_rs_history(classification,rs_history_list,"display_industry")
        mswing_data=_calculate_mswing(all_data,history_days=ROLLING_DAYS-50)
        mswing_list=_build_mswing_json(all_data,mswing_data)
        for sig in signals:
            sym=sig["symbol"]; sig["mswing"]=mswing_data.get(sym,{}).get("mswing"); sig["mswing_avg9"]=mswing_data.get(sym,{}).get("mswing_avg9")
        for sig in pr_signals:
            sym=sig["symbol"]; sig["mswing"]=mswing_data.get(sym,{}).get("mswing"); sig["mswing_avg9"]=mswing_data.get(sym,{}).get("mswing_avg9")
        screener_feed=_build_screener_feed(all_data,classification,rs_data,mswing_data,result_calendar,sheet_data,today,hlr_map=hlr_map,pb_map=pb_map,pat_map=pat_map)
        ep_pat_map={}
        for sig in signals: ep_pat_map.setdefault(sig["symbol"],set()).add("EP")
        for row in screener_feed:
            sym=row["symbol"]; sc=screener.get(sym,{}); fund=fund_lookup.get(sym,{})
            pl=fund.get("pl_quarterly",[]); row["q_name"]=pl[0].get("header","") if pl else ""
            if pl and len(pl)>=5:
                s0=pl[0].get("sales"); s4=pl[4].get("sales")
                row["sales_ch"]=round((s0-s4)/s4*100,1) if s0 and s4 else None
                e0=pl[0].get("eps"); e4=pl[4].get("eps")
                row["eps_ch"]=round((e0-e4)/e4*100,1) if e0 and e4 else None
            else: row["sales_ch"]=None; row["eps_ch"]=None
            pats=set()
            for flag,label in [("vd","VD"),("hvq","HVQ"),("hvm","HVM"),("hvy","HVY"),("lvq","LVQ"),("lvm","LVM"),("lvy","LVY"),("vol_footprint","Volume Footprint"),("atr_tightness","ATR Tightness"),("bs","BS"),("pp","PP"),("mcp","MCP"),("launchpad","Launchpad"),("ib","IB"),("dib","DIB"),("nr7","NR7"),("wib","WIB"),("w_dib","W-DIB"),("w_nr7","W-NR7"),("w_3tc","3WTC"),("pullback","PullBack"),("tl_hl_bo","TL/HL BO"),("hpbc","HPBC")]:
                if row.get(flag): pats.add(label)
            if row.get("gap_fill"): pats.add(row["gap_fill"])
            if row.get("hlr_state"): pats.add(row["hlr_state"])
            if sym in ep_pat_map: pats|=ep_pat_map[sym]
            row["patterns"]="||".join(sorted(pats))
        # ─── FIX: EP/PR signals mein aaj ke fresh patterns (sheet ke purane nahi) ───
        feed_pat={row["symbol"]:row["patterns"] for row in screener_feed}
        for sig in signals:
            if sig["symbol"] in feed_pat: sig["patterns"]=feed_pat[sig["symbol"]]
        for sig in pr_signals:
            if sig["symbol"] in feed_pat: sig["patterns"]=feed_pat[sig["symbol"]]
        await asyncio.gather(
            r2_upload(client,"ep_signals.json",json.dumps({"updated":today,"count":len(signals),"signals":signals})),
            r2_upload(client,"rs_ratings.json",json.dumps({"updated":today,"count":len(rs_data),"stocks":rs_data})),
            r2_upload(client,"rs_history.json",json.dumps(rs_history_list)),
            r2_upload(client,"mswing.json",json.dumps(mswing_list)),
            r2_upload(client,"post_result_signals.json",json.dumps({"updated":today,"count":len(pr_signals),"ah_count":sum(1 for s in pr_signals if "AH" in s["reaction_type"]),"ih_count":sum(1 for s in pr_signals if "IH" in s["reaction_type"]),"signals":pr_signals})),
            r2_upload(client,"sector_group_rs_history.json",json.dumps(sector_group_rs_history)),
            r2_upload(client,"industry_rs_history.json",json.dumps(industry_rs_history)),
            r2_upload(client,"screener_feed.json",json.dumps(screener_feed)),
            backup_pattern_history(client,screener_feed,today),
        )
        log.info(f"✅ EP:{len(signals)}  PostResult:{len(pr_signals)}  RS:{len(rs_data)}")
    log.info("━━━ EP + Post-Result + RS Scan complete ━━━")
