"""fii_card.py — the daily FII / DII cash activity as a PNG card for Telegram (sendPhoto).

    from fii_card import render_fii_card
    png = render_fii_card({
        "date": "Mon 02 Nov 2026",
        "fii": {"buy": 16499, "sell": 13385, "net": 3115},
        "dii": {"buy": 14988, "sell": 15381, "net": -392},
        "last5": [("27 Oct", 4240, -1250), ..., ("02 Nov", 3115, -392)],   # (label, FII net, DII net)
        "mtd": {"month": "Nov", "fii": 3115, "dii": -392},                  # optional
        "fut": {"long": 31191, "short": 264390},                            # optional, FII index futures
    })
Same light style as result_card.py. Only Pillow is needed. Values are ₹ Cr.
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
    if f is None:
        try: f = ImageFont.load_default(size=size)
        except TypeError: f = ImageFont.load_default()
    _FC[k] = f
    return f


BG, CARD, LINE = (243, 246, 250), (255, 255, 255), (222, 228, 236)
TXT, MUT, ACC = (15, 23, 42), (100, 116, 139), (2, 132, 199)
GRN, RED = (22, 163, 74), (220, 38, 38)
GRN_BG, RED_BG = (220, 252, 231), (254, 226, 226)
TAG_BG = (224, 242, 254)


def _cr(v):
    return "—" if v is None else f"{v:+,.0f}"


def render_fii_card(d, scale=2):
    S = lambda x: int(round(x * scale))
    W, pad = S(560), S(22)
    f_title, f_sub, f_tag = _font(True, S(20)), _font(False, S(12.5)), _font(True, S(10.5))
    f_h, f_sec, f_big = _font(True, S(11)), _font(True, S(11.5)), _font(True, S(22))
    f_cell, f_cellb, f_note = _font(False, S(13.5)), _font(True, S(13.5)), _font(False, S(10.5))
    last5 = list(d.get("last5") or [])[-5:]
    mtd, fut = d.get("mtd") or None, d.get("fut") or None
    fut_ok = bool(fut and (fut.get("long") or 0) + (fut.get("short") or 0))

    rh = S(34)
    H = S(80) + S(92) + S(16)                                          # header + tiles
    if last5:
        H += S(22) + rh * (len(last5) + 1) + S(10) + S(14)
    if mtd or fut_ok:
        H += S(40)
    if fut_ok:
        H += S(22)
    H += S(40)
    img = Image.new("RGB", (W, H), BG)
    g = ImageDraw.Draw(img)

    # ── header
    g.text((pad, S(18)), "FII / DII Activity", font=f_title, fill=TXT)
    dt = str(d.get("date") or "")
    if dt:
        tw = g.textlength(dt, font=f_tag)
        g.rounded_rectangle([W - pad - tw - S(16), S(20), W - pad, S(42)], radius=S(6), fill=TAG_BG)
        g.text((W - pad - tw - S(8), S(24)), dt, font=f_tag, fill=ACC)
    g.text((pad, S(50)), "Cash market · ₹ Cr · provisional", font=f_sub, fill=MUT)

    # ── FII / DII net tiles
    y = S(80)
    tw2 = (W - 2 * S(12) - S(10)) // 2
    for i, key in enumerate(("fii", "dii")):
        e = d.get(key) or {}
        x = S(12) + i * (tw2 + S(10))
        n = e.get("net")
        c = GRN if (n or 0) > 0 else RED if (n or 0) < 0 else MUT
        g.rounded_rectangle([x, y, x + tw2, y + S(92)], radius=S(10), fill=CARD, outline=LINE, width=max(1, S(1)))
        g.text((x + S(14), y + S(12)), f"{key.upper()} NET", font=f_h, fill=MUT)
        g.text((x + S(14), y + S(28)), _cr(n), font=f_big, fill=c)
        if e.get("buy") is not None or e.get("sell") is not None:
            g.text((x + S(14), y + S(62)), f"Buy {e.get('buy') or 0:,.0f}  ·  Sell {e.get('sell') or 0:,.0f}",
                   font=f_note, fill=MUT)
    y += S(92) + S(16)

    # ── last 5 sessions: value + bar from a centre line
    if last5:
        g.text((pad, y), "LAST 5 SESSIONS (NET)", font=f_sec, fill=ACC)
        y += S(22)
        th = rh * (len(last5) + 1) + S(10)
        g.rounded_rectangle([S(12), y, W - S(12), y + th], radius=S(10), fill=CARD, outline=LINE, width=max(1, S(1)))
        cols = [("FII", S(120), S(320)), ("DII", S(330), S(530))]
        mx = max([abs(v) for _, a, b in last5 for v in (a, b) if v is not None] or [1]) or 1
        for name, x0, x1 in cols:
            g.text(((x0 + x1) // 2 - g.textlength(name, font=f_h) // 2, y + S(10)), name, font=f_h, fill=MUT)
        ry = y + S(38)
        for k, (lbl, a, b) in enumerate(last5):
            last = k == len(last5) - 1
            g.text((pad, ry + S(4)), str(lbl), font=f_cellb if last else f_cell, fill=TXT)
            for (name, x0, x1), v in zip(cols, (a, b)):
                mid = (x0 + x1) // 2
                g.line([mid, ry, mid, ry + S(28)], fill=LINE, width=max(1, S(1)))
                if v is None:
                    continue
                half = (x1 - x0) // 2 - S(4)
                w = max(S(2), int(abs(v) / mx * half * 0.55))
                if v > 0:
                    g.rounded_rectangle([mid, ry + S(6), mid + w, ry + S(22)], radius=S(3), fill=GRN_BG)
                elif v < 0:
                    g.rounded_rectangle([mid - w, ry + S(6), mid, ry + S(22)], radius=S(3), fill=RED_BG)
                t = _cr(v)
                c = GRN if v > 0 else RED if v < 0 else MUT
                tx = mid + S(6) if v < 0 else mid - S(6) - g.textlength(t, font=f_cellb)
                g.text((tx, ry + S(4)), t, font=f_cellb, fill=c)
            ry += rh
        y += th + S(14)

    # ── month to date + FII index futures
    if mtd or fut_ok:
        x = S(12)

        def chip(x, parts):
            w = S(20) + sum(g.textlength(t, font=f) for t, f, _ in parts) + S(6) * (len(parts) - 1)
            g.rounded_rectangle([x, y, x + w, y + S(30)], radius=S(8), fill=CARD, outline=LINE, width=max(1, S(1)))
            cx = x + S(10)
            for t, f, c in parts:
                g.text((cx, y + (S(8) if f is f_note else S(6))), t, font=f, fill=c)
                cx += g.textlength(t, font=f) + S(6)
            return x + w + S(8)

        if mtd:
            fm, dm = mtd.get("fii"), mtd.get("dii")
            x = chip(x, [(f"{mtd.get('month', '')} MTD  FII", f_note, MUT),
                         (_cr(fm), f_cellb, GRN if (fm or 0) > 0 else RED if (fm or 0) < 0 else MUT),
                         ("DII", f_note, MUT),
                         (_cr(dm), f_cellb, GRN if (dm or 0) > 0 else RED if (dm or 0) < 0 else MUT)])
        if fut_ok:
            lo, sh = fut.get("long") or 0, fut.get("short") or 0
            chip(x, [("FII index futures", f_note, MUT), (f"{lo / (lo + sh) * 100:.0f}% long", f_cellb, TXT)])
        y += S(40)
        if fut_ok:
            g.text((pad, y), f"Index futures: {lo:,} long / {sh:,} short contracts", font=f_note, fill=MUT)
            y += S(22)

    g.text((pad, H - S(28)), "tradewithtech.in", font=f_tag, fill=ACC)
    buf = io.BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


if __name__ == "__main__":
    png = render_fii_card({
        "date": "Mon 02 Nov 2026",
        "fii": {"buy": 16499, "sell": 13385, "net": 3115},
        "dii": {"buy": 14988, "sell": 15381, "net": -392},
        "last5": [("27 Oct", 4240, -1250), ("28 Oct", 872, 643), ("29 Oct", -944, 1924),
                  ("30 Oct", 3599, -818), ("02 Nov", 3115, -392)],
        "mtd": {"month": "Nov", "fii": 3115, "dii": -392},
        "fut": {"long": 31191, "short": 264390},
    })
    open("fii_card_sample.png", "wb").write(png)
    print(len(png) // 1024, "KB")
