"""result_card.py — a quarterly result as a PNG card for Telegram (sendPhoto).

    from result_card import render_result_card
    png = render_result_card({
        "name": "WeWork India Management Limited", "symbol": "WEWORK",
        "quarter": "Q2 FY27", "audit": "Unaudited", "date": "2026-10-26",
        "sections": [{
            "title": "Consolidated",
            "cols": ["Sep'26", "Jun'26", "Sep'25"],          # current, previous quarter, year ago
            "rows": [  # cur / prev / yago values; qoq / yoy: precomputed change or None (worked out here)
                {"name": "Sales", "cur": 767.8, "prev": 683.8, "yago": 574.7, "dec": 1, "kind": "pct"},
                {"name": "PAT",   "cur": 20.3,  "prev": -4.3,  "yago": 6.3,   "dec": 1, "kind": "pct"},
                {"name": "OPM %", "cur": 63.8,  "prev": 64.0,  "yago": 66.1,  "dec": 1, "kind": "pp"},
            ],
            "note": "Sales / OP / PAT in ₹ Cr · OP = EBITDA excl. other income",
        }],
    })
Only Pillow is needed. Fonts: DejaVu (on GitHub's Ubuntu runners), Inter when installed.
Changes: kind "pct" = % change (from a loss, measured on the size of the loss, so + means better);
kind "pp" = change in points (margins). A turn from / into a loss is spelled out in a note under
the table, e.g. "PAT QoQ: loss to profit (-4.3 → 20.3)".
"""
import io
import os
from PIL import Image, ImageDraw, ImageFont

_FONT_DIRS = ["/usr/share/fonts/truetype/dejavu", "/usr/share/fonts/opentype/inter", "/usr/share/fonts/truetype/inter"]
_FC = {}


def _font(bold, size):
    k = (bold, size)
    if k in _FC:
        return _FC[k]
    names = (["Inter-Bold.otf", "DejaVuSans-Bold.ttf"] if bold else ["Inter-Regular.otf", "DejaVuSans.ttf"])
    f = None
    for d in _FONT_DIRS:
        for n in names:
            p = os.path.join(d, n)
            if os.path.exists(p):
                f = ImageFont.truetype(p, size); break
        if f: break
    if f is None:                                       # no DejaVu / Inter on the machine
        try: f = ImageFont.load_default(size=size)      # Pillow 10.1+: scalable built-in font
        except TypeError: f = ImageFont.load_default()
        print("result_card: DejaVu font not found — using Pillow's built-in font")
    _FC[k] = f
    return _FC[k]


# Light theme (darker green / red / accent than the old dark theme so they read on white)
BG, CARD, LINE = (243, 246, 250), (255, 255, 255), (222, 228, 236)
TXT, MUT, ACC = (15, 23, 42), (100, 116, 139), (2, 132, 199)
GRN, RED, AMB = (22, 163, 74), (220, 38, 38), (180, 83, 9)
TAG_BG = (224, 242, 254)    # symbol pill background


def _num(v, dec=1, kind="pct"):
    if v is None:
        return "—"
    try:
        s = f"{v:,.{dec}f}"
    except (TypeError, ValueError):
        return "—"
    return s + "%" if kind == "pp" else s


def _delta(cur, old, given, kind):
    """(text, colour). `given` = change worked out upstream; else from the values."""
    d = given
    if d is None and cur is not None and old is not None:
        try:
            if kind == "pp":
                d = cur - old
            elif old != 0:
                d = (cur - old) / abs(old) * 100
        except TypeError:
            d = None
    if d is None:
        return "—", MUT
    c = GRN if d > 0 else RED if d < 0 else MUT
    return (f"{d:+.1f}pp" if kind == "pp" else f"{d:+.1f}%"), c


def _turn(cur, old):
    if cur is None or old is None or old == 0:
        return None
    try:
        if old < 0 < cur: return "loss to profit"
        if old > 0 > cur: return "profit to loss"
        if old < 0 and cur < 0: return "loss narrowed" if cur > old else "loss widened"
    except TypeError:
        return None
    return None


def render_result_card(d, scale=2):
    S = lambda x: int(round(x * scale))
    W = S(560)
    pad, rh = S(22), S(34)
    xs = [pad, S(150), S(250), S(360), S(455), W - pad]   # right edges: current | QoQ | YoY | prev | year ago
    f_title = _font(True, S(20))
    f_sub, f_h, f_cell, f_cellb = _font(False, S(12.5)), _font(True, S(11)), _font(False, S(14)), _font(True, S(14))
    f_note, f_tag, f_sec = _font(False, S(10.5)), _font(True, S(10.5)), _font(True, S(11.5))

    secs = []
    for sec in d.get("sections") or []:
        rows = sec.get("rows") or []
        notes = []
        for r in rows:
            if r.get("kind") == "pp": continue
            for old, lbl in ((r.get("prev"), "QoQ"), (r.get("yago"), "YoY")):
                t = _turn(r.get("cur"), old)
                if t: notes.append(f"{r['name']} {lbl}: {t} ({_num(old, r.get('dec', 1))} → {_num(r.get('cur'), r.get('dec', 1))})")
        secs.append((sec, rows, notes))

    head_h = S(84)
    body_h = 0
    for sec, rows, notes in secs:
        # table box (rows + header + 10) + 10 gap below it + notes + 14 — same steps as the drawing loop
        body_h += (S(24) if sec.get("title") else 0) + rh * (len(rows) + 1) + S(20) + S(17) * len(notes) + S(14)
    foot_notes = []
    for sec, _, _ in secs:
        n = sec.get("note")
        if n and n not in foot_notes: foot_notes.append(n)
    H = head_h + body_h + S(16) * len(foot_notes) + S(34)
    img = Image.new("RGB", (W, H), BG)
    g = ImageDraw.Draw(img)

    # ── header
    sym = d.get("symbol") or ""
    room = W - 2 * pad - (g.textlength(sym, font=f_tag) + S(30) if sym else 0)
    name, sz = str(d.get("name") or ""), 20
    while sz > 13 and g.textlength(name, font=f_title) > room:
        sz -= 1; f_title = _font(True, S(sz))
    while len(name) > 4 and g.textlength(name, font=f_title) > room:
        name = name.rstrip("…")[:-1].rstrip() + "…"
    g.text((pad, S(18) + S(20 - sz) // 2), name, font=f_title, fill=TXT)
    if sym:
        tw = g.textlength(sym, font=f_tag)
        g.rounded_rectangle([W - pad - tw - S(16), S(20), W - pad, S(42)], radius=S(6), fill=TAG_BG)
        g.text((W - pad - tw - S(8), S(24)), sym, font=f_tag, fill=ACC)
    sub = " · ".join(str(x) for x in [d.get("quarter"), d.get("audit"), f"Result {d['date']}" if d.get("date") else None] if x)
    g.text((pad, S(50)), sub, font=f_sub, fill=MUT)

    # ── sections
    y = head_h
    for sec, rows, notes in secs:
        if sec.get("title"):
            g.text((pad, y), str(sec["title"]).upper(), font=f_sec, fill=ACC)
            y += S(24)
        cols = list(sec.get("cols") or []) + ["", "", ""]
        th = rh * (len(rows) + 1) + S(10)
        g.rounded_rectangle([S(12), y, W - S(12), y + th], radius=S(10), fill=CARD, outline=LINE, width=max(1, S(1)))
        hy = y + S(10)
        for i, h in enumerate(["", cols[0] or "Now", "QoQ", "YoY", cols[1], cols[2]]):
            if not h: continue
            tw = g.textlength(h, font=f_h)
            g.text((xs[i] - tw, hy), h, font=f_h, fill=ACC if i == 1 else MUT)
        ry = y + rh + S(4)
        g.line([S(22), ry - S(6), W - S(22), ry - S(6)], fill=LINE, width=max(1, S(1)))
        for r in rows:
            dec, kind = r.get("dec", 1), r.get("kind", "pct")
            q, qc = _delta(r.get("cur"), r.get("prev"), r.get("qoq"), kind)
            a, ac = _delta(r.get("cur"), r.get("yago"), r.get("yoy"), kind)
            g.text((pad, ry + S(4)), str(r.get("name", "")), font=f_cellb, fill=TXT)
            for i, (t, c, f) in enumerate([(_num(r.get("cur"), dec, kind), TXT, f_cellb), (q, qc, f_cell), (a, ac, f_cell),
                                           (_num(r.get("prev"), dec, kind), MUT, f_cell), (_num(r.get("yago"), dec, kind), MUT, f_cell)], start=1):
                tw = g.textlength(t, font=f)
                g.text((xs[i] - tw, ry + S(4)), t, font=f, fill=c)
            ry += rh
        y += th + S(10)
        for n in notes:
            g.text((pad, y), "• " + n, font=f_note, fill=AMB)
            y += S(17)
        y += S(14)

    # ── footer
    for n in foot_notes:
        g.text((pad, y), n, font=f_note, fill=MUT)
        y += S(16)
    g.text((pad, H - S(26)), "tradewithtech.in", font=f_tag, fill=ACC)
    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


if __name__ == "__main__":
    R = lambda n, c, p, y, dec=1, kind="pct": {"name": n, "cur": c, "prev": p, "yago": y, "dec": dec, "kind": kind}
    png = render_result_card({
        "name": "WeWork India Management Limited", "symbol": "WEWORK",
        "quarter": "Q2 FY27", "audit": "Unaudited", "date": "2026-10-26",
        "sections": [{"title": "Consolidated", "cols": ["Sep'26", "Jun'26", "Sep'25"],
                      "rows": [R("Sales", 767.8, 683.8, 574.7), R("OP", 490.1, 438.0, 380.1), R("PAT", 20.3, -4.3, 6.3),
                               R("EPS", 1.46, -0.31, 0.47, 2), R("OPM%", 63.8, 64.0, 66.1, 1, "pp")],
                      "note": "Sales / OP / PAT in ₹ Cr · OP = EBITDA excl. other income"},
                     {"title": "Standalone", "cols": ["Sep'26", "Jun'26", "Sep'25"],
                      "rows": [R("Sales", 701.2, 640.0, 530.1), R("PAT", 15.1, 3.2, -2.0), R("OPM%", 62.0, 63.1, 65.0, 1, "pp")],
                      "note": "Sales / OP / PAT in ₹ Cr · OP = EBITDA excl. other income"}],
    })
    open("result_card_sample.png", "wb").write(png)
    print(len(png) // 1024, "KB")
