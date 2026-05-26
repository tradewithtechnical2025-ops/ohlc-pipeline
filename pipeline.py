# ══════════════════════════════════════════════════════════════
# COMPLETE FIX — Replace these functions in pipeline.py
# Issues fixed:
#   1. header empty          → _fmt_period_end() se derive
#   2. eps_diluted null      → pl_quarterly[0].eps se
#   3. book_value_ps null    → BS se compute
#   4. net_debt null         → BS se compute
#   5. CF sab null           → correct field names
#   6. dividend_payout null  → basic-financials se (by year)
#   7. promoter null         → shareholding dict format + category names fix
# ══════════════════════════════════════════════════════════════

# ── HELPER ────────────────────────────────────────────────────

def _fmt_period_end(period_end) -> str:
    """20260331 (int) → 'Mar 2026'"""
    if not period_end:
        return ""
    MONTHS = ["","Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]
    s = str(int(period_end))
    if len(s) == 8:
        m = int(s[4:6])
        return f"{MONTHS[m]} {s[:4]}" if 1 <= m <= 12 else s
    return str(period_end)


# ── SHAREHOLDING — dict format + new category names ───────────

async def _finedge_shareholding(client, sem, sym) -> dict | None:
    d = await _finedge_get(client, sem, f"shareholdings/pattern/{sym}", {"period": "quarterly"})
    if not d:
        return None
    columns = d.get("columns", [])
    rows    = d.get("rows", [])
    if not columns or not rows:
        return None

    n_qtrs = min(8, len(columns))
    qtrs   = columns[:n_qtrs]

    def get_row(*names) -> list:
        """Category name (partial, case-insensitive) se row dhundho.
        Data dict format {"Jun 2023": 41.93, ...} ya list dono handle karta hai."""
        for name in names:
            r = next(
                (x for x in rows if name.lower() in x.get("category", "").lower()),
                None
            )
            if r is None:
                continue
            data = r.get("data", {})
            if isinstance(data, dict):
                return [data.get(q) for q in qtrs]
            elif isinstance(data, list):
                return list(data[:n_qtrs])
        return []

    # Finedge category names (as reported)
    fii    = get_row("institutionsforeign", "foreign", "fii")
    dii    = get_row("institutionsdomestic", "domestic", "dii")
    public = get_row("noninstitutions", "public", "retail")
    govt   = get_row("goverment", "government")          # Finedge typo: "Goverments"
    promoter = get_row("promoter")

    # ITC jaise stocks mein Promoter category nahi hoti
    # Compute karo: 100 - FII - DII - Public - Govt
    if not any(v is not None for v in promoter):
        promoter_computed = []
        for i in range(n_qtrs):
            vals = [
                fii[i]    if i < len(fii)    else None,
                dii[i]    if i < len(dii)    else None,
                public[i] if i < len(public) else None,
                govt[i]   if i < len(govt)   else 0,
            ]
            if all(v is not None for v in vals[:3]):
                promoter_computed.append(round(100 - sum(v or 0 for v in vals), 2))
            else:
                promoter_computed.append(None)
        promoter = promoter_computed

    def _first(lst):
        return next((v for v in lst if v is not None), None)

    return {
        "sh_quarters" : qtrs,
        "sh_promoter" : promoter,
        "sh_fii"      : fii,
        "sh_dii"      : dii,
        "sh_public"   : public,
        "promoter"    : _first(promoter),
        "fii"         : _first(fii),
        "dii"         : _first(dii),
        "public"      : _first(public),
        "promoter_ch" : (
            round(promoter[0] - promoter[1], 2)
            if len(promoter) >= 2
            and promoter[0] is not None
            and promoter[1] is not None
            else None
        ),
    }


# ── MAIN FUNDAMENTAL FETCHER ──────────────────────────────────

async def fetch_one_fundamental(
    client,
    sem,
    sym: str,
    isin: str = "",
) -> tuple[str, dict | None]:
    pl_qtr, pl_ann, bs_ann, cf_ann, basic, prof_ratios, sh, profile = await asyncio.gather(
        _finedge_financials(client, sem, sym, "pl", "quarterly"),
        _finedge_financials(client, sem, sym, "pl", "annual"),
        _finedge_financials(client, sem, sym, "bs", "annual"),
        _finedge_financials(client, sem, sym, "cf", "annual"),
        _finedge_basic(client, sem, sym),
        _finedge_ratios_pr(client, sem, sym),
        _finedge_shareholding(client, sem, sym),
        _finedge_profile(client, sem, sym),
    )

    if not any([pl_qtr, pl_ann, bs_ann, cf_ann]):
        return sym, None

    obj: dict = {"symbol": sym, "updated": today_ist(), "source": "finedge"}

    # ── Company Profile ────────────────────────────────────────
    if profile:
        obj.update({
            "name"        : profile.get("name", ""),
            "sector"      : profile.get("sector", ""),
            "industry"    : profile.get("industry", ""),
            "sub_industry": profile.get("sub_industry", ""),
            "macro_sector": profile.get("macro_sector", ""),
            "market_cap"  : profile.get("market_cap"),
            "bse_code"    : profile.get("bse_code", ""),
            "description" : profile.get("description", ""),
            "website"     : profile.get("website", ""),
        })

    # ── Basic Financials TTM ───────────────────────────────────
    # FIX: eps_diluted, book_value_ps, net_debt basic-financials mein nahi hain
    #      — baad mein PL/BS se derive karenge
    if basic:
        ttm = basic[0] if basic else {}
        obj.update({
            "ebit"              : ttm.get("ebit"),
            "ebitda"            : ttm.get("ebitda"),
            "operating_revenue" : ttm.get("operatingRevenue"),
            "operating_profit"  : ttm.get("operatingProfit"),
            "shares_outstanding": ttm.get("dilutedSharesOutstanding"),
        })

    # ── dividend_payout lookup (basic-financials mein, PL mein nahi) ──
    div_payout_by_year: dict = {}
    if basic:
        for row in basic:
            yr = row.get("year")
            dp = row.get("dividendPayout")
            if yr is not None and dp is not None:
                div_payout_by_year[yr] = dp

    # ── P&L Quarterly — 12 quarters ───────────────────────────
    # FIX: header = _fmt_period_end(period_end)
    if pl_qtr:
        obj["pl_quarterly"] = [
            {
                "header"       : _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "period_end"   : q.get("period_end"),
                "sales"        : q.get("revenueFromOperations"),
                "expenses"     : q.get("expenses"),
                "pbt"          : q.get("profitBeforeTax"),
                "pat"          : q.get("profitLossForPeriod"),
                "eps"          : q.get("eps"),
                "depreciation" : q.get("depreciationAndAmortisation"),
                "finance_costs": q.get("financeCosts"),
                "tax"          : q.get("taxExpense"),
                "other_income" : q.get("otherIncome"),
            }
            for q in pl_qtr[:12]
        ]

    # ── P&L Annual — 5 years ──────────────────────────────────
    # FIX: header = _fmt_period_end(period_end)
    # FIX: dividend_payout = div_payout_by_year lookup (PL mein field nahi)
    if pl_ann:
        obj["pl_annual"] = [
            {
                "header"          : _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "year"            : q.get("year"),
                "sales"           : q.get("revenueFromOperations"),
                "expenses"        : q.get("expenses"),
                "pbt"             : q.get("profitBeforeTax"),
                "pat"             : q.get("profitLossForPeriod"),
                "eps"             : q.get("eps"),
                "depreciation"    : q.get("depreciationAndAmortisation"),
                "finance_costs"   : q.get("financeCosts"),
                "dividend_payout" : div_payout_by_year.get(q.get("year")),
                "other_income"    : q.get("otherIncome"),
            }
            for q in pl_ann[:5]
        ]

    # ── Balance Sheet Annual — 5 years ────────────────────────
    # FIX: header = _fmt_period_end(period_end)
    if bs_ann:
        obj["bs_annual"] = [
            {
                "header"               : _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "year"                 : q.get("year"),
                "total_assets"         : q.get("assets"),
                "equity_capital"       : q.get("equityCapital"),
                "reserves"             : q.get("reserves"),
                "borrowings_current"   : q.get("borrowingsCurrent"),
                "borrowings_noncurrent": q.get("borrowingsNoncurrent"),
                "borrowings_total"     : (q.get("borrowingsCurrent") or 0) + (q.get("borrowingsNoncurrent") or 0),
                "cash"                 : q.get("cashAndCashEquivalents"),
                "current_assets"       : q.get("currentAssets"),
                "current_liabilities"  : q.get("currentLiabilities"),
                "fixed_assets"         : q.get("propertyPlantAndEquipmentNet"),
                "investments"          : q.get("investments"),
            }
            for q in bs_ann[:5]
        ]

    # ── Cash Flow Annual — 5 years ────────────────────────────
    # FIX: correct field names from Finedge CF response
    if cf_ann:
        obj["cf_annual"] = [
            {
                "header": _fmt_period_end(q.get("period_end")) or q.get("header", ""),
                "year"  : q.get("year"),
                "cfo"   : q.get("cashFlowsFromOperatingActivities"),
                "cfi"   : q.get("cashFlowsFromInvestingActivities"),
                "cff"   : q.get("cashFlowsFromFinancingActivities"),
                "net_cf": q.get("netCashFlow"),
                "capex" : q.get("purchaseOfPPEClassifiedAsInvesting"),
                "fcf"   : (
                    (q.get("cashFlowsFromOperatingActivities") or 0) +
                    (q.get("purchaseOfPPEClassifiedAsInvesting") or 0)
                ) if q.get("cashFlowsFromOperatingActivities") is not None else None,
            }
            for q in cf_ann[:5]
        ]

    # ── Profitability Ratios — 5 years ────────────────────────
    if prof_ratios:
        obj["ratios_annual"] = [
            {
                "header"          : r.get("header", ""),
                "year"            : r.get("year"),
                "gross_margin"    : r.get("grossMargin"),
                "ebit_margin"     : r.get("ebitMargin"),
                "ebitda_margin"   : r.get("ebitdaMargin"),
                "net_margin"      : r.get("netMargin"),
                "operating_margin": r.get("operatingMargin"),
                "roe"             : r.get("returnOnEquity"),
                "roa"             : r.get("returnOnAsset"),
                "roce"            : r.get("returnOnCapital"),
                "pretax_margin"   : r.get("preTaxMargin"),
                "tax_rate"        : r.get("effectiveTaxRate"),
            }
            for r in prof_ratios[:5]
        ]

    # ── Shareholding — 8 quarters ─────────────────────────────
    if sh:
        obj.update({
            "promoter"    : sh.get("promoter"),
            "fii"         : sh.get("fii"),
            "dii"         : sh.get("dii"),
            "public"      : sh.get("public"),
            "promoter_ch" : sh.get("promoter_ch"),
            "sh_quarters" : sh.get("sh_quarters", []),
            "sh_promoter" : sh.get("sh_promoter", []),
            "sh_fii"      : sh.get("sh_fii", []),
            "sh_dii"      : sh.get("sh_dii", []),
            "sh_public"   : sh.get("sh_public", []),
        })

    # ── Post-processing: BS se derive karo ────────────────────
    # FIX: eps_diluted → latest quarterly eps
    if obj.get("pl_quarterly"):
        obj["eps_diluted"] = obj["pl_quarterly"][0].get("eps")

    # FIX: book_value_ps → (equity_capital + reserves) / shares
    # FIX: net_debt → borrowings_total − cash
    if obj.get("bs_annual"):
        bs0    = obj["bs_annual"][0]
        eq_cap = bs0.get("equity_capital") or 0
        res    = bs0.get("reserves") or 0
        borr   = bs0.get("borrowings_total") or 0
        cash   = bs0.get("cash") or 0
        shares = obj.get("shares_outstanding") or 0

        obj["book_value_ps"] = round((eq_cap + res) / shares, 2) if shares else None
        obj["net_debt"]      = round(borr - cash) if (borr or cash) else None

    return sym, obj
