#!/usr/bin/env python3
"""
Иш — branded headline card generator
=====================================
Turns one digest (dict) into a 1080x1350 PNG for Facebook/Instagram.

Design signature: a vertical "citation rail" down the left edge — the
name Иш means "to cite a source", so every card reads as a cited thing.
The rail takes the story's category color.

Usage (standalone test):
    python card.py            # renders sample_card.png from demo data

Usage (from the agent):
    from card import make_card
    path = make_card(digest_dict, out_dir="cards")
"""

import os
import textwrap
from PIL import Image, ImageDraw, ImageFont

# ── Canvas ────────────────────────────────────────────────────
W, H = 1080, 1350
MARGIN = 90
RAIL_X = 60          # left citation rail position
RAIL_W = 12

# ── Palette ───────────────────────────────────────────────────
INK      = (22, 28, 38)       # near-black headline
PAPER    = (244, 246, 245)    # cool paper background
SLATE    = (92, 107, 122)     # muted captions
WHITE    = (255, 255, 255)
TENGRI   = (36, 86, 166)      # Иш brand blue
SAFFRON  = (185, 126, 0)      # "why it matters"
SAFFRONBG= (251, 243, 221)

# Category → rail color
CAT_COLORS = {
    "Улс төр":    (176, 58, 46),    # red
    "Эдийн засаг":(31, 122, 77),    # green
    "Нийгэм":     (36, 86, 166),    # blue
    "Технологи":  (109, 76, 178),   # purple
    "Спорт":      (192, 108, 20),   # orange
    "Дэлхий":     (38, 110, 124),   # teal
}
DEFAULT_CAT_COLOR = TENGRI

# ── Fonts (DejaVu ships on Linux/GitHub Actions; override later
#    with a Mongolian-optimized face for production) ────────────
def _font_paths():
    # Headlines previously used a serif (Georgia), but Windows Georgia
    # drops some Mongolian glyphs (ү renders as a box). The sans family
    # (Arial / DejaVuSans) renders ALL Mongolian letters correctly, so
    # we use it for everything. Correct letters > serif styling.
    candidates = {
        "serif_bold": [   # kept name for compatibility; now points to sans bold
            "C:/Windows/Fonts/arialbd.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ],
        "sans": [
            "C:/Windows/Fonts/arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ],
        "sans_bold": [
            "C:/Windows/Fonts/arialbd.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ],
    }
    resolved = {}
    for role, paths in candidates.items():
        resolved[role] = next((p for p in paths if os.path.exists(p)), None)
    return resolved


def _supports_mongolian(font_path):
    """Check a font renders ө and ү (not as missing-glyph boxes)."""
    if not font_path:
        return False
    try:
        from PIL import ImageFont
        f = ImageFont.truetype(font_path, 40)
        return f.getmask("өүӨҮ").size[0] > 0
    except Exception:
        return False

_FP = _font_paths()

# Safety: if the chosen headline (serif) font can't render ө/ү, fall
# back to a sans font that can — a correct headline beats a pretty one.
if not _supports_mongolian(_FP.get("serif_bold")):
    if _supports_mongolian(_FP.get("sans_bold")):
        print("[cards] headline font lacks Mongolian glyphs; "
              "using sans bold instead")
        _FP["serif_bold"] = _FP["sans_bold"]

def _f(role, size):
    path = _FP.get(role)
    if path:
        return ImageFont.truetype(path, size)
    return ImageFont.load_default()


# ── Text helpers ──────────────────────────────────────────────
def wrap_to_width(draw, text, font, max_w):
    """Wrap text to fit a pixel width, return list of lines."""
    words = text.split()
    lines, cur = [], ""
    for w in words:
        test = (cur + " " + w).strip()
        if draw.textlength(test, font=font) <= max_w:
            cur = test
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def draw_wrapped(draw, xy, text, font, max_w, fill, line_gap=12):
    x, y = xy
    for line in wrap_to_width(draw, text, font, max_w):
        draw.text((x, y), line, font=font, fill=fill)
        bbox = draw.textbbox((0, 0), line, font=font)
        y += (bbox[3] - bbox[1]) + line_gap
    return y


def measure_wrapped(draw, text, font, max_w, line_gap=12):
    """Return the pixel height a wrapped block will occupy."""
    lines = wrap_to_width(draw, text, font, max_w)
    if not lines:
        return 0
    h = 0
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        h += (bbox[3] - bbox[1]) + line_gap
    return h


def line_height(draw, font):
    bbox = draw.textbbox((0, 0), "Ауф", font=font)
    return bbox[3] - bbox[1]


# ── Main card builder ─────────────────────────────────────────
def make_card(d, out_dir="cards", filename=None, photo_path=None):
    """
    d: digest dict with keys title, bullets, why, category, sources.
    photo_path: optional article photo — rendered as a header image
    (~42% of card, cover-cropped, with a photo-credit line). When absent
    or unusable, the classic text-only layout is used.
    Returns the path to the written PNG.
    """
    os.makedirs(out_dir, exist_ok=True)
    cat = d.get("category", "Нийгэм")
    rail = CAT_COLORS.get(cat, DEFAULT_CAT_COLOR)

    img = Image.new("RGB", (W, H), PAPER)
    dr = ImageDraw.Draw(img)

    # ── optional article photo as FULL-CARD background ──
    # Cover-fit, darkened for legibility (no credit line: the source is
    # already cited in the footer). Text switches to a light palette.
    on_photo = False
    if photo_path and os.path.exists(photo_path):
        try:
            ph = Image.open(photo_path).convert("RGB")
            scale = max(W / ph.width, H / ph.height)
            ph = ph.resize((int(ph.width * scale) or 1,
                            int(ph.height * scale) or 1))
            left = (ph.width - W) // 2
            top = (ph.height - H) // 2
            ph = ph.crop((left, top, left + W, top + H))
            overlay = Image.new("RGB", (W, H), (10, 14, 18))
            ph = Image.blend(ph, overlay, 0.62)   # strong darken: lots of text
            img.paste(ph, (0, 0))
            dr = ImageDraw.Draw(img)
            on_photo = True
        except Exception:
            on_photo = False  # unusable photo -> classic paper card

    # palette: dark-on-paper normally, light-on-photo otherwise
    if on_photo:
        pal = dict(mark=WHITE, cat=WHITE, divider=(150, 158, 166),
                   head=WHITE, btxt=(224, 230, 236),
                   whybg=(28, 36, 46), whyhead=(240, 194, 96),
                   whytxt=(238, 242, 246), src=(206, 214, 222))
    else:
        pal = dict(mark=TENGRI, cat=None,  # cat=None -> use rail color
                   divider=(225, 229, 227), head=INK, btxt=(48, 58, 70),
                   whybg=SAFFRONBG, whyhead=SAFFRON, whytxt=INK, src=SLATE)

    # citation rail (full height, as in the classic layout)
    dr.rectangle([RAIL_X, MARGIN, RAIL_X + RAIL_W, H - MARGIN], fill=rail)

    content_x = RAIL_X + RAIL_W + 46
    content_w = W - content_x - MARGIN
    y = MARGIN

    # ── header row: wordmark + category ──
    f_mark = _f("serif_bold", 54)
    dr.text((content_x, y), "Иш", font=f_mark, fill=pal["mark"])
    f_cat = _f("sans_bold", 26)
    cat_up = cat.upper()
    cat_w = dr.textlength(cat_up, font=f_cat)
    dr.text((W - MARGIN - cat_w, y + 16), cat_up, font=f_cat,
            fill=pal["cat"] or rail)
    y += 96

    # thin divider
    dr.line([content_x, y, W - MARGIN, y], fill=pal["divider"], width=2)
    y += 50
    top_y = y

    # Reserve space at the bottom for the source line (always clear)
    f_src = _f("sans_bold", 24)
    src_h = line_height(dr, f_src)
    source_band = src_h + 30           # source text + gap above it
    bottom_limit = H - MARGIN - source_band

    title = d["title"]
    bullets = [b for b in d.get("bullets", []) if b][:3]
    why = d.get("why", "")

    # Try progressively smaller "size sets" until everything fits the
    # space between top_y and bottom_limit. Each tuple:
    # (headline, bullet, why-head, why-body, gaps...)
    SIZE_SETS = [
        dict(head=60, bul=36, whyh=26, why=34, hgap=14, bgap=24, blgap=10),
        dict(head=52, bul=33, whyh=24, why=31, hgap=12, bgap=20, blgap=8),
        dict(head=46, bul=30, whyh=23, why=29, hgap=11, bgap=17, blgap=7),
        dict(head=40, bul=28, whyh=22, why=27, hgap=10, bgap=14, blgap=6),
        dict(head=36, bul=26, whyh=21, why=25, hgap=9,  bgap=12, blgap=5),
        dict(head=33, bul=24, whyh=20, why=24, hgap=8,  bgap=10, blgap=4),
    ]

    def plan_height(s, trim_bullets=False):
        """Compute total height needed for a given size set."""
        fh = _f("serif_bold", s["head"])
        fb = _f("sans", s["bul"])
        fwh = _f("sans_bold", s["whyh"])
        fw = _f("sans", s["why"])
        h = measure_wrapped(dr, title, fh, content_w, s["hgap"])
        h += 36  # gap after headline
        use_b = bullets[:2] if trim_bullets else bullets
        for b in use_b:
            h += measure_wrapped(dr, b, fb, content_w - 34, s["blgap"])
            h += s["bgap"]
        if why:
            wlines_h = measure_wrapped(dr, why, fw, content_w - 60, 14)
            h += 24 + 50 + wlines_h + 36  # gap + label + body + padding
        return h, (fh, fb, fwh, fw)

    avail = bottom_limit - top_y
    chosen = None
    for s in SIZE_SETS:
        h, fonts = plan_height(s, trim_bullets=False)
        if h <= avail:
            chosen = (s, fonts, bullets)
            break
    if chosen is None:
        # still too tall at smallest: drop to 2 bullets at smallest size
        s = SIZE_SETS[-1]
        h, fonts = plan_height(s, trim_bullets=True)
        chosen = (s, fonts, bullets[:2])

    s, (f_head, f_bul, f_whyhead, f_why), use_bullets = chosen

    # ── render headline ──
    y = draw_wrapped(dr, (content_x, y), title, f_head,
                     content_w, pal["head"], line_gap=s["hgap"])
    y += 36

    # ── render bullets ──
    dot_r = max(5, s["bul"] // 7)
    for b in use_bullets:
        dr.ellipse([content_x, y + s["bul"] // 2, content_x + dot_r * 2,
                    y + s["bul"] // 2 + dot_r * 2], fill=rail)
        y = draw_wrapped(dr, (content_x + 34, y), b, f_bul,
                         content_w - 34, pal["btxt"], line_gap=s["blgap"])
        y += s["bgap"]

    # ── render "why" box right below bullets (not floated) ──
    if why:
        y += 4
        wlines_h = measure_wrapped(dr, why, f_why, content_w - 60, 14)
        block_h = 50 + wlines_h + 30
        dr.rounded_rectangle([content_x, y, W - MARGIN, y + block_h],
                             radius=18, fill=pal["whybg"])
        dr.text((content_x + 30, y + 22), "ЯАГААД ЧУХАЛ ВЭ?",
                font=f_whyhead, fill=pal["whyhead"])
        draw_wrapped(dr, (content_x + 30, y + 56), why, f_why,
                     content_w - 60, pal["whytxt"], line_gap=14)

    # ── footer: sources (always in reserved band at bottom) ──
    sources = d.get("sources") or [d.get("source", "")]
    src_text = "Эх сурвалж: " + ", ".join(s2 for s2 in sources if s2)
    dr.text((content_x, H - MARGIN - src_h), src_text, font=f_src, fill=pal["src"])

    out = os.path.join(out_dir, filename or "card.png")
    img.save(out, "PNG")
    return out


def make_weather_card(w, bg_image=None, out_dir="cards", filename=None):
    """
    Render a morning weather card (1080x1350).
    w: dict with tmax, tmin, label, wind, precip, date.
    bg_image: optional path to a condition-matched background photo.
    The photo is darkened for text legibility; if absent, a brand-blue
    gradient background is used.
    """
    img = Image.new("RGB", (W, H), TENGRI)

    if bg_image and os.path.exists(bg_image):
        try:
            bg = Image.open(bg_image).convert("RGB")
            # cover-fit to canvas
            scale = max(W / bg.width, H / bg.height)
            bg = bg.resize((int(bg.width * scale), int(bg.height * scale)))
            left = (bg.width - W) // 2
            top = (bg.height - H) // 2
            bg = bg.crop((left, top, left + W, top + H))
            # darken for legibility
            overlay = Image.new("RGB", (W, H), (0, 0, 0))
            bg = Image.blend(bg, overlay, 0.42)
            img.paste(bg, (0, 0))
        except Exception:
            pass  # keep solid background

    draw = ImageDraw.Draw(img)

    # Иш brand mark, top-left
    draw.text((MARGIN, 70), "Иш", font=_f("serif_bold", 64), fill=WHITE)
    draw.text((W - MARGIN - 260, 92), "ЦАГ АГААР",
              font=_f("sans_bold", 34), fill=WHITE, anchor="la")

    # Big temperature (max), centered-ish
    tmax_s = f"{w['tmax']}°"
    draw.text((MARGIN, 360), tmax_s, font=_f("serif_bold", 300), fill=WHITE)

    # min temp + condition label
    draw.text((MARGIN + 8, 720),
              f"Доод {w['tmin']}°C",
              font=_f("sans_bold", 52), fill=(220, 228, 236))
    draw.text((MARGIN, 800), w["label"],
              font=_f("sans_bold", 72), fill=WHITE)

    # detail line: wind + precip (no emoji — DejaVu lacks emoji glyphs)
    details = f"Салхи {w['wind']} км/ц"
    if w.get("precip", 0) and w["precip"] >= 0.1:
        details += f"     Тунадас {w['precip']} мм"
    draw.text((MARGIN, 920), details,
              font=_f("sans", 44), fill=(220, 228, 236))

    # date footer
    draw.text((MARGIN, H - 130), w.get("date", ""),
              font=_f("sans", 40), fill=(210, 218, 226))
    draw.text((W - MARGIN, H - 130), "ish.mn",
              font=_f("sans_bold", 40), fill=(210, 218, 226), anchor="ra")

    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, filename or "weather_card.png")
    img.save(out, "PNG")
    return out


def make_currency_card(d, out_dir="cards", filename=None):
    """
    Render the morning currency card (1080x1350): Mongolbank official
    rates as a clean table in the brand style.
    d: {"rates": {code: float}, "labels": {code: mongolian}, "date": str}
    """
    img = Image.new("RGB", (W, H), PAPER)
    draw = ImageDraw.Draw(img)

    # green rail (economy color) down the left, like news cards
    rail = CAT_COLORS.get("Эдийн засаг", TENGRI)
    draw.rectangle([RAIL_X, 140, RAIL_X + RAIL_W, H - 140], fill=rail)

    draw.text((MARGIN + 40, 90), "Иш", font=_f("serif_bold", 64), fill=TENGRI)
    draw.text((W - MARGIN, 108), "ВАЛЮТЫН ХАНШ",
              font=_f("sans_bold", 34), fill=rail, anchor="ra")

    draw.text((MARGIN + 40, 230), "Монголбанкны албан ханш",
              font=_f("sans_bold", 48), fill=INK)
    draw.text((MARGIN + 40, 300), d.get("date", ""),
              font=_f("sans", 38), fill=SLATE)

    # rate rows
    order = [c for c in ("USD", "EUR", "CNY", "RUB", "JPY", "KRW")
             if c in d["rates"]]
    y = 420
    row_h = 128
    for code in order:
        val = d["rates"][code]
        label = d["labels"].get(code, "")
        val_s = f"{val:,.2f}" if val < 100 else f"{val:,.0f}"
        # subtle row divider
        draw.line([(MARGIN + 40, y + row_h - 18), (W - MARGIN, y + row_h - 18)],
                  fill=(214, 220, 218), width=2)
        draw.text((MARGIN + 40, y), code, font=_f("sans_bold", 56), fill=INK)
        draw.text((MARGIN + 220, y + 10), label, font=_f("sans", 40), fill=SLATE)
        draw.text((W - MARGIN, y), f"{val_s}₮",
                  font=_f("sans_bold", 56), fill=TENGRI, anchor="ra")
        y += row_h

    draw.text((MARGIN + 40, H - 130), "Эх сурвалж: Монголбанк",
              font=_f("sans", 38), fill=SLATE)
    draw.text((W - MARGIN, H - 130), "ish.mn",
              font=_f("sans_bold", 40), fill=SLATE, anchor="ra")

    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, filename or "currency_card.png")
    img.save(out, "PNG")
    return out


# ── Standalone test ───────────────────────────────────────────
if __name__ == "__main__":
    demo = {
        "title": "Монголбанк бодлогын хүүг 12 хувьд хэвээр үлдээлээ",
        "category": "Эдийн засаг",
        "bullets": [
            "Төв банк бодлогын хүүг өөрчлөхгүй байхаар шийдвэрлэлээ.",
            "Инфляц буурах хандлагатай ч гадаад эрсдэл өндөр хэвээр байна.",
            "Дараагийн хурал ирэх улиралд товлогдсон.",
        ],
        "why": "Таны зээл, ипотекийн хүү ойрын саруудад тогтвортой байна гэсэн үг.",
        "sources": ["MONTSAME", "ikon.mn"],
    }
    path = make_card(demo, out_dir=".", filename="sample_card.png")
    print("wrote", path)
