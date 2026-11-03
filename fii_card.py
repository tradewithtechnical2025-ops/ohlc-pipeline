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
    # Narrow card, large type: Telegram shows the photo at the chat width, so on a phone a wide
    # card's text came out tiny. 440 wide with 13-30 px type reads like a normal message.
    W, pad = S(440), S(18)
    f_title, f_sub, f_tag = _font(True, S(22)), _font(False, S(14)), _font(True, S(12.5))
    f_h, f_sec, f_big = _font(True, S(13)), _font(True, S(13)), _font(True, S(30))
    f_cell, f_cellb, f_note = _font(False, S(15.5)), _font(True, S(15.5)), _font(False, S(13))
    last5 = list(d.get("last5") or [])[-5:]
    mtd, fut = d.get("mtd") or None, d.get("fut") or None
    fut_ok = bool(fut and (fut.get("long") or 0) + (fut.get("short") or 0))

    rh = S(38)
    TILE_H = S(108)
    H = S(84) + TILE_H + S(16)                                         # header + tiles
    if last5:
        H += S(22) + rh * (len(last5) + 1) + S(10) + S(14)
    if mtd:
        H += S(44)
    if fut_ok:
        H += S(44) + S(24)
    H += S(40)
    img = Image.new("RGB", (W, H), BG)
    g = ImageDraw.Draw(img)

    # ── header
    g.text((pad, S(16)), "FII / DII Activity", font=f_title, fill=TXT)
    dt = str(d.get("date") or "")
    if dt:
        tw = g.textlength(dt, font=f_tag)
        g.rounded_rectangle([W - pad - tw - S(16), S(19), W - pad, S(43)], radius=S(6), fill=TAG_BG)
        g.text((W - pad - tw - S(8), S(23)), dt, font=f_tag, fill=ACC)
    g.text((pad, S(52)), "Cash market · ₹ Cr · provisional", font=f_sub, fill=MUT)

    # ── FII / DII net tiles
    y = S(84)
    tw2 = (W - 2 * S(12) - S(10)) // 2
    for i, key in enumerate(("fii", "dii")):
        e = d.get(key) or {}
        x = S(12) + i * (tw2 + S(10))
        n = e.get("net")
        c = GRN if (n or 0) > 0 else RED if (n or 0) < 0 else MUT
        g.rounded_rectangle([x, y, x + tw2, y + TILE_H], radius=S(10), fill=CARD, outline=LINE, width=max(1, S(1)))
        g.text((x + S(12), y + S(10)), f"{key.upper()} NET", font=f_h, fill=MUT)
        g.text((x + S(12), y + S(28)), _cr(n), font=f_big, fill=c)
        if e.get("buy") is not None or e.get("sell") is not None:   # buy and sell on their own lines: room for big numbers
            g.text((x + S(12), y + S(68)), f"Buy {e.get('buy') or 0:,.0f}", font=f_note, fill=MUT)
            g.text((x + S(12), y + S(86)), f"Sell {e.get('sell') or 0:,.0f}", font=f_note, fill=MUT)
    y += TILE_H + S(16)

    # ── last 5 sessions: value + bar from a centre line
    if last5:
        g.text((pad, y), "LAST 5 SESSIONS (NET)", font=f_sec, fill=ACC)
        y += S(22)
        th = rh * (len(last5) + 1) + S(10)
        g.rounded_rectangle([S(12), y, W - S(12), y + th], radius=S(10), fill=CARD, outline=LINE, width=max(1, S(1)))
        cols = [("FII", S(96), S(262)), ("DII", S(266), S(432))]
        mx = max([abs(v) for _, a, b in last5 for v in (a, b) if v is not None] or [1]) or 1
        for name, x0, x1 in cols:
            g.text(((x0 + x1) // 2 - g.textlength(name, font=f_h) // 2, y + S(10)), name, font=f_h, fill=MUT)
        ry = y + S(40)
        for k, (lbl, a, b) in enumerate(last5):
            last = k == len(last5) - 1
            g.text((pad, ry + S(4)), str(lbl), font=f_cellb if last else f_cell, fill=TXT)
            for (name, x0, x1), v in zip(cols, (a, b)):
                mid = (x0 + x1) // 2
                g.line([mid, ry, mid, ry + S(32)], fill=LINE, width=max(1, S(1)))
                if v is None:
                    continue
                half = (x1 - x0) // 2 - S(4)
                w = max(S(2), int(abs(v) / mx * half * 0.45))
                if v > 0:
                    g.rounded_rectangle([mid, ry + S(6), mid + w, ry + S(26)], radius=S(3), fill=GRN_BG)
                elif v < 0:
                    g.rounded_rectangle([mid - w, ry + S(6), mid, ry + S(26)], radius=S(3), fill=RED_BG)
                t = _cr(v)
                c = GRN if v > 0 else RED if v < 0 else MUT
                tx = mid + S(6) if v < 0 else mid - S(6) - g.textlength(t, font=f_cellb)
                g.text((tx, ry + S(4)), t, font=f_cellb, fill=c)
            ry += rh
        y += th + S(14)

    # ── month to date + FII index futures
    if mtd or fut_ok:
        x = S(12)

        def chip(yy, parts):                            # full-width row: label left, values right
            g.rounded_rectangle([S(12), yy, W - S(12), yy + S(36)], radius=S(8), fill=CARD, outline=LINE, width=max(1, S(1)))
            g.text((S(24), yy + S(10)), parts[0][0], font=parts[0][1], fill=parts[0][2])
            cx = W - S(24)
            for t, f, c in reversed(parts[1:]):
                cx -= g.textlength(t, font=f)
                g.text((cx, yy + (S(10) if f is f_note else S(8))), t, font=f, fill=c)
                cx -= S(8)

        if mtd:
            fm, dm = mtd.get("fii"), mtd.get("dii")
            chip(y, [(f"{mtd.get('month', '')} month to date", f_note, MUT),
                     ("FII", f_note, MUT), (_cr(fm), f_cellb, GRN if (fm or 0) > 0 else RED if (fm or 0) < 0 else MUT),
                     ("DII", f_note, MUT), (_cr(dm), f_cellb, GRN if (dm or 0) > 0 else RED if (dm or 0) < 0 else MUT)])
            y += S(44)
        if fut_ok:
            lo, sh = fut.get("long") or 0, fut.get("short") or 0
            chip(y, [("FII index futures", f_note, MUT), (f"{lo / (lo + sh) * 100:.0f}% long", f_cellb, TXT)])
            y += S(44)
            g.text((pad, y), f"{lo:,} long / {sh:,} short contracts", font=f_note, fill=MUT)
            y += S(24)

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
