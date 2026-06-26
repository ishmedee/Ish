#!/usr/bin/env python3
"""
Товч — News Digest Agent
========================
Runs 3x daily (07:00 / 12:00 / 17:00 Asia/Ulaanbaatar via cron).
Pipeline: RSS feeds -> dedupe (SQLite) -> fetch article text ->
Claude summarization -> digest.json (+ optional Telegram digest).

Setup:
    pip install requests feedparser beautifulsoup4 anthropic
    export ANTHROPIC_API_KEY=sk-ant-...
    # optional Telegram delivery:
    export TELEGRAM_BOT_TOKEN=...   TELEGRAM_CHAT_ID=...

Cron (server timezone set to Asia/Ulaanbaatar):
    0 7,12,17 * * *  cd /opt/towch && /usr/bin/python3 agent.py >> agent.log 2>&1
"""

import json
import os
import re
import sqlite3
import sys
import time
import hashlib
from datetime import datetime, timezone, timedelta

import feedparser
import requests
from bs4 import BeautifulSoup
from anthropic import Anthropic

# Card generator (optional — agent still runs if Pillow/card.py missing)
try:
    from card import make_card
    CARDS_AVAILABLE = True
except Exception as _card_err:
    CARDS_AVAILABLE = False
    print(f"[cards] disabled: {_card_err}")

# Reel generator (optional — needs ffmpeg + reel.py)
try:
    from reel import make_reel
    REELS_AVAILABLE = True
except Exception as _reel_err:
    REELS_AVAILABLE = False
    print(f"[reels] disabled: {_reel_err}")

# Master switch: post Reels in addition to feed posts
POST_REELS = os.environ.get("POST_REELS", "1") == "1"

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────

UB_TZ = timezone(timedelta(hours=8))

# Two kinds of sources:
#   "rss":      a feed URL (preferred when a site has one)
#   "listing":  a normal news-list page we scrape for article links,
#               with "link_pattern" = regex that article URLs match.
SOURCES = [
    {
        "name": "ikon.mn",
        "rss": "https://ikon.mn/rss",
        "article_selector": "div.news_body, div.content, article",
    },
    {
        "name": "MONTSAME",
        "listing": "https://montsame.mn/mn/more/8",   # МОНГОЛЫН МЭДЭЭ
        "link_pattern": r"/mn/read/\d+",
        "base_url": "https://montsame.mn",
        "article_selector": "div.article-content, div.content, article",
    },
    {
        "name": "MONTSAME-eco",
        "listing": "https://montsame.mn/mn/more/10",  # ЭДИЙН ЗАСАГ
        "link_pattern": r"/mn/read/\d+",
        "base_url": "https://montsame.mn",
        "article_selector": "div.article-content, div.content, article",
    },
    # ── gogo.mn: PARKED — returns 403 to scrapers. Needs a headless
    #    browser (Playwright) to fetch, or a publisher partnership.
    #    Re-enable by uncommenting once that's built. Pattern: /r/<code>
    # {
    #     "name": "gogo.mn",
    #     "listing": "https://gogo.mn/i/2",        # Улс төр
    #     "link_pattern": r"/r/[a-z0-9]+",
    #     "base_url": "https://gogo.mn",
    #     "article_selector": "div.article-body, div.news-detail, div.content, article",
    # },
    # {
    #     "name": "gogo.mn-eco",
    #     "listing": "https://gogo.mn/i/3",        # Эдийн засаг
    #     "link_pattern": r"/r/[a-z0-9]+",
    #     "base_url": "https://gogo.mn",
    #     "article_selector": "div.article-body, div.news-detail, div.content, article",
    # },
    # {
    #     "name": "gogo.mn-society",
    #     "listing": "https://gogo.mn/i/7",        # Нийгэм
    #     "link_pattern": r"/r/[a-z0-9]+",
    #     "base_url": "https://gogo.mn",
    #     "article_selector": "div.article-body, div.news-detail, div.content, article",
    # },
]

MAX_ARTICLES_PER_RUN = 12        # cost & noise control
MAX_PER_SOURCE = 4               # balance across outlets
MIN_ARTICLE_CHARS = 400          # skip stubs/photo posts
MODEL = "claude-sonnet-4-6" # cheap + good enough for summaries
DB_PATH = "towch.db"
OUTPUT_JSON = "digest.json"      # the website reads this file
REQUEST_TIMEOUT = 15

# ── Queue-system settings ─────────────────────────────────────
MORNING_FRESH_HOUR = 9   # before this hour, poster may use yesterday's
                         # leftovers (today's 6am batch might be thin)
MAX_QUEUE_AGE_DAYS = 5   # drop unposted stories older than this (covers
                         # a Friday story staying usable through Sunday)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}

# ── Level 1: cheap pre-filter (runs BEFORE paying for AI) ──────
# If an article's URL or title trips these, we skip it for free
# instead of spending ~3 cents to have Claude summarize an ad.
AD_URL_HINTS = [
    "/zar/", "/zarlal", "/advert", "/advertorial", "/reklam",
    "/promo", "/pr/", "/sponsor", "/huudas",
]
AD_TITLE_HINTS = [
    "хямдрал", "урамшуул", "хямдарч", "хямдарлаа", "хямдхан",
    "худалдаанд гарлаа", "худалдаалж эхэл", "захиалга авч",
    "хямдралтай", "урамшуулал", "багц үнэ", "хямд үнэ",
    "sale", "promo", "% off", "хөнгөлөлттэй", "бэлэг дагалд",
]


def looks_like_ad(title, url):
    """Free first-pass ad filter — no AI cost."""
    u = (url or "").lower()
    t = (title or "").lower()
    if any(h in u for h in AD_URL_HINTS):
        return True
    if any(h in t for h in AD_TITLE_HINTS):
        return True
    return False


CATEGORIES = ["Улс төр", "Эдийн засаг", "Нийгэм", "Технологи", "Спорт", "Дэлхий"]

PROMPT = """Чи Монголын мэдээг энгийн ойлгомжтой болгодог редактор.
Доорх нийтлэлийг уншаад ЗӨВХӨН дараах JSON-оор хариул (өөр юу ч бичихгүй, markdown хэрэглэхгүй):

{{"title": "товч тодорхой гарчиг (clickbait биш)",
 "category": "{cats} — аль нэгийг сонго",
 "bullets": ["хамгийн чухал 3 баримтыг 3 товч өгүүлбэрээр", "...", "..."],
 "why": "энгийн иргэнд яагаад хамаатай болохыг 1 өгүүлбэрээр",
 "newsworthy": true/false,
 "importance": 0-100,
 "emotional": 0-100,
 "block": true/false}}

"newsworthy" дүгнэлт (ЧУХАЛ):
false бол — дараах тохиолдолд:
  • Сурталчилгаа, бүтээгдэхүүн/үйлчилгээ борлуулах далд зар (advertorial)
  • Бодит мэдээлэлгүй, зөвхөн магтаал бүхий байгууллага/компанийн PR
  • "Шинэ бараа гарлаа", "ийм дэлгүүр нээлээ" төрлийн зар
  • Засаг захиргаа/компанийн өөрийгөө магтсан, мэдээлэл агуулаагүй текст
true бол — жинхэнэ мэдээ: улс төр, эдийн засаг, нийгэм, технологи,
  түүнчлэн спорт, соёл, хүн сонирхсон зөөлөн мэдээ ч мөн true.
Эргэлзвэл true. Зорилго: зар, хоосон PR-ийг шүүх, бодит мэдээг үлдээх.

"importance" (0-100): энэ мэдээ хүмүүсийн амьдрал, мөнгө, ажил, аюулгүй
  байдалд хэр их нөлөөлөх вэ? Бодлого, хууль, эдийн засаг, томоохон
  үйл явдал = өндөр оноо.
"emotional" (0-100): энэ мэдээ хэр их анхаарал татах, сэтгэл хөдөлгөх вэ?
  Зөрчил, маргаан, гэнэтийн/гайхалтай үйл явдал, дуулиан, хүний драм =
  өндөр оноо. Уйтгартай албан мэдээ = бага оноо.
"block" (ЦӨӨХӨН тохиолдолд true): зөвхөн дараах тохиолдолд true —
  цуст/аймшигт дүрслэл, гамшиг/золгүй явдлын хохирогчийг мөлжсөн,
  баталгаагүй гүтгэлэг/нэр төр гутаах, үзэн ядалт өдөөсөн контент.
  Бусад бүх тохиолдолд false. Энэ нь зөвхөн хуудсыг хоригдохоос хамгаалах
  доод хязгаар — ердийн сэтгэл хөдөлгөм, дуулиантай мэдээг блоклохгүй.

Нийтлэл ({source}):
{text}"""


# ──────────────────────────────────────────────────────────────
# STORAGE (dedupe across runs)
# ──────────────────────────────────────────────────────────────

def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS seen (
        url TEXT PRIMARY KEY,
        first_seen TEXT
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS digests (
        url TEXT PRIMARY KEY,
        source TEXT, category TEXT, title TEXT,
        bullets TEXT, why TEXT, orig_min INTEGER,
        published TEXT, run_at TEXT,
        sources TEXT, source_count INTEGER DEFAULT 1, all_urls TEXT
    )""")
    # migrate older DBs that lack the new columns
    cols = {r[1] for r in con.execute("PRAGMA table_info(digests)")}
    for col, decl in [("sources", "TEXT"), ("source_count", "INTEGER DEFAULT 1"),
                      ("all_urls", "TEXT"),
                      # queue system columns:
                      ("posted", "INTEGER DEFAULT 0"),      # 0=pending, 1=posted to FB
                      ("collected_date", "TEXT"),            # YYYY-MM-DD it was collected
                      ("card_path", "TEXT"),                 # saved card image path
                      ("posted_at", "TEXT"),                 # when it was posted
                      ("interest_score", "INTEGER DEFAULT 50")]:  # engagement ranking
        if col not in cols:
            con.execute(f"ALTER TABLE digests ADD COLUMN {col} {decl}")
    con.commit()
    return con


def is_new(con, url):
    return con.execute("SELECT 1 FROM seen WHERE url=?", (url,)).fetchone() is None


def mark_seen(con, url):
    con.execute(
        "INSERT OR IGNORE INTO seen VALUES (?, ?)",
        (url, datetime.now(UB_TZ).isoformat()),
    )
    con.commit()


# ──────────────────────────────────────────────────────────────
# COLLECTION
# ──────────────────────────────────────────────────────────────

def collect_from_rss(src, con):
    feed = feedparser.parse(src["rss"])
    fresh = []
    for entry in feed.entries:
        url = entry.get("link", "").strip()
        if url and is_new(con, url):
            fresh.append((src, entry.get("title", ""), url))
        if len(fresh) >= MAX_PER_SOURCE:
            break
    return fresh


def collect_from_listing(src, con):
    """Scrape a normal news-list page for article links."""
    r = requests.get(src["listing"], headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    pattern = re.compile(src["link_pattern"])
    fresh, seen_here = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not pattern.search(href):
            continue
        url = href if href.startswith("http") else src["base_url"] + href
        if url in seen_here:
            continue
        seen_here.add(url)
        if is_new(con, url):
            title = a.get_text(strip=True)
            # gogo/news card layouts sometimes put the headline in a
            # child node, leaving link text empty — try the title attr,
            # then skip if still unusable.
            if len(title) < 12:
                title = (a.get("title") or "").strip() or title
            if len(title) < 12:
                continue
            fresh.append((src, title, url))
        if len(fresh) >= MAX_PER_SOURCE:
            break
    return fresh


def collect_candidates(con):
    """Pull all sources, return list of new (source, title, url) tuples."""
    candidates = []
    for src in SOURCES:
        try:
            if src.get("rss"):
                fresh = collect_from_rss(src, con)
            elif src.get("listing"):
                fresh = collect_from_listing(src, con)
            else:
                continue
            candidates.extend(fresh)
            print(f"[collect] {src['name']}: {len(fresh)} new")
        except Exception as e:
            print(f"[collect] {src['name']} FAILED: {e}")
    return candidates[:MAX_ARTICLES_PER_RUN]


def fetch_article_text(url, selector):
    """Download the article page and extract readable text."""
    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "aside", "iframe"]):
        tag.decompose()

    node = None
    for sel in selector.split(","):
        node = soup.select_one(sel.strip())
        if node:
            break
    text = (node or soup.body or soup).get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text


# ──────────────────────────────────────────────────────────────
# SUMMARIZATION
# ──────────────────────────────────────────────────────────────

def _parse_json_lenient(raw):
    """Parse Claude's JSON, repairing common truncation issues."""
    raw = raw.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        last = raw.rfind("}")
        if last != -1:
            try:
                return json.loads(raw[:last + 1])
            except json.JSONDecodeError:
                pass
        raise


def summarize(client, source_name, text):
    msg = client.messages.create(
        model=MODEL,
        max_tokens=900,
        messages=[{
            "role": "user",
            "content": PROMPT.format(
                cats="/".join(CATEGORIES),
                source=source_name,
                text=text[:8000],
            ),
        }],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text")
    return _parse_json_lenient(raw)


# ──────────────────────────────────────────────────────────────
# CROSS-SOURCE DEDUP: cluster same-story articles, then synthesize
# ──────────────────────────────────────────────────────────────

import difflib

TITLE_SIM_THRESHOLD = 0.55  # candidate grouping; Claude confirms after


def _norm(t):
    return re.sub(r"[^\w ]", "", (t or "").lower())


def cluster_candidates(client, articles):
    """
    articles: list of dicts {src, title, url, text}
    Returns list of clusters; each cluster is a list of those dicts.
    Step A: group by fuzzy title similarity (free).
    Step B: ask Claude to confirm multi-article groups are truly the
            same story (catches different-wording, splits false matches).
    """
    # Step A — greedy similarity grouping
    groups = []
    for art in articles:
        placed = False
        for g in groups:
            ratio = difflib.SequenceMatcher(
                None, _norm(art["title"]), _norm(g[0]["title"])
            ).ratio()
            if ratio >= TITLE_SIM_THRESHOLD:
                g.append(art)
                placed = True
                break
        if not placed:
            groups.append([art])

    # Step B — Claude confirmation only for groups with >1 article
    confirmed = []
    for g in groups:
        if len(g) == 1:
            confirmed.append(g)
            continue
        listing = "\n".join(f"{i+1}. [{a['src']}] {a['title']}"
                            for i, a in enumerate(g))
        try:
            msg = client.messages.create(
                model=MODEL,
                max_tokens=200,
                messages=[{"role": "user", "content":
                    "Доорх гарчгууд ИЖИЛ үйл явдлыг мэдээлж байна уу? "
                    "Зөвхөн JSON-оор хариул: бодит нэг үйл явдлыг хамтад нь "
                    "бүлэглэсэн дугаарын жагсаалт. Жишээ: [[1,3],[2]] гэвэл "
                    "1,3 ижил, 2 өөр. Гарчгууд:\n" + listing}],
            )
            raw = "".join(b.text for b in msg.content if b.type == "text")
            raw = raw.replace("```json", "").replace("```", "").strip()
            idx_groups = json.loads(raw)
            for ig in idx_groups:
                confirmed.append([g[i - 1] for i in ig if 0 < i <= len(g)])
        except Exception as e:
            print(f"[cluster] confirm failed, keeping separate: {e}")
            confirmed.extend([[a] for a in g])
    return [c for c in confirmed if c]


SYNTH_PROMPT = """Чи Монголын мэдээг энгийн ойлгомжтой болгодог редактор.
Доорх нь ОЛОН эх сурвалж НЭГ үйл явдлыг мэдээлсэн нийтлэлүүд.
Бүгдийг уншаад нэгтгэн, ЗӨВХӨН дараах JSON-оор хариул:

{{"title": "товч тодорхой гарчиг (clickbait биш)",
 "category": "{cats} — аль нэгийг сонго",
 "bullets": ["бүх эх сурвалжийн чухал баримтыг нэгтгэсэн 3 өгүүлбэр", "...", "..."],
 "why": "энгийн иргэнд яагаад хамаатайг 1 өгүүлбэрээр",
 "newsworthy": true/false,
 "importance": 0-100,
 "emotional": 0-100,
 "block": true/false}}

"importance" (0-100): хүмүүсийн амьдрал, мөнгө, ажил, аюулгүй байдалд
  хэр нөлөөлөх вэ (бодлого, хууль, эдийн засаг = өндөр).
"emotional" (0-100): хэр анхаарал татах, сэтгэл хөдөлгөх вэ (зөрчил,
  дуулиан, гэнэтийн үйл явдал = өндөр; уйтгартай албан мэдээ = бага).
"block" (зөвхөн цөөхөн): цуст/аймшигт дүрслэл, золгүй явдлын хохирогчийг
  мөлжсөн, баталгаагүй гүтгэлэг, үзэн ядалт өдөөсөн л бол true. Бусад false.

Эх сурвалжууд зөрчилтэй мэдээлэл өгвөл түүнийг тэмдэглэ.
{articles}"""


def synthesize_cluster(client, cluster):
    """One digest from multiple same-story articles."""
    blocks = []
    for a in cluster:
        blocks.append(f"--- Эх сурвалж: {a['src']} ---\n{a['text'][:4000]}")
    msg = client.messages.create(
        model=MODEL,
        max_tokens=1000,
        messages=[{"role": "user", "content": SYNTH_PROMPT.format(
            cats="/".join(CATEGORIES),
            articles="\n\n".join(blocks),
        )}],
    )
    raw = "".join(b.text for b in msg.content if b.type == "text")
    return _parse_json_lenient(raw)


# ──────────────────────────────────────────────────────────────
# FACEBOOK POSTING
# ──────────────────────────────────────────────────────────────

FB_API = "https://graph.facebook.com/v23.0"
# How many top stories to post per edition (don't flood the feed)
FB_MAX_POSTS = 3


def build_caption(item):
    """Compose the text that accompanies a card on Facebook."""
    lines = [item["title"], ""]
    for b in item["bullets"]:
        lines.append(f"• {b}")
    if item.get("why"):
        lines.append("")
        lines.append(f"💡 Яагаад чухал вэ? {item['why']}")
    lines.append("")
    srcs = ", ".join(item.get("sources", [item.get("source", "")]))
    lines.append(f"📰 Эх сурвалж: {srcs}")
    lines.append(f"🔗 {item['url']}")
    lines.append("")
    lines.append("#Иш #мэдээ")
    return "\n".join(lines)


def post_one_to_facebook(item, card_path, token, page_id):
    """Post ONE story's card as a feed post. Returns True on success."""
    if not card_path or not os.path.exists(card_path):
        print(f"[fb] no card file for: {item['title'][:40]}")
        return False
    caption = build_caption(item)
    try:
        # Step 1: upload photo unpublished
        with open(card_path, "rb") as img:
            up = requests.post(
                f"{FB_API}/{page_id}/photos",
                data={"published": "false", "access_token": token},
                files={"source": img}, timeout=60,
            )
        photo_id = up.json().get("id")
        if not photo_id:
            err = up.json().get("error", {}).get("message", up.text[:200])
            print(f"[fb] upload FAILED ({up.status_code}): {err}")
            return False
        # Step 2: create feed post with photo attached
        r = requests.post(
            f"{FB_API}/{page_id}/feed",
            data={"message": caption,
                  "attached_media[0]": json.dumps({"media_fbid": photo_id}),
                  "access_token": token},
            timeout=60,
        )
        if r.status_code == 200 and r.json().get("id"):
            print(f"[fb] posted to feed: {item['title'][:50]}")
            return True
        err = r.json().get("error", {}).get("message", r.text[:200])
        print(f"[fb] feed post FAILED ({r.status_code}): {err}")
        return False
    except Exception as e:
        print(f"[fb] error posting {item['title'][:40]}: {e}")
        return False


def post_reel_to_facebook(item, reel_path, token, page_id):
    """
    Publish a Reel to the page via the 3-phase Reels API:
      1. start  -> get video_id + upload_url
      2. upload -> POST the file binary to the upload_url
      3. finish -> publish with description
    Returns True on success (Reel accepted for processing).
    """
    if not reel_path or not os.path.exists(reel_path):
        print(f"[reel] no video file for: {item['title'][:40]}")
        return False
    caption = build_caption(item)
    reel_api = "https://graph.facebook.com/v23.0"
    try:
        # Phase 1: start upload session
        start = requests.post(
            f"{reel_api}/{page_id}/video_reels",
            data={"upload_phase": "start", "access_token": token},
            timeout=30,
        )
        sb = start.json()
        video_id = sb.get("video_id")
        upload_url = sb.get("upload_url")
        if not video_id or not upload_url:
            err = sb.get("error", {}).get("message", start.text[:200])
            print(f"[reel] start FAILED ({start.status_code}): {err}")
            return False

        # Phase 2: upload the binary to the rupload endpoint
        file_size = os.path.getsize(reel_path)
        with open(reel_path, "rb") as f:
            up = requests.post(
                upload_url,
                headers={
                    "Authorization": f"OAuth {token}",
                    "offset": "0",
                    "file_size": str(file_size),
                },
                data=f.read(),
                timeout=180,
            )
        if up.status_code != 200 or not up.json().get("success", True):
            print(f"[reel] upload FAILED ({up.status_code}): {up.text[:200]}")
            return False

        # Phase 3: finish + publish
        fin = requests.post(
            f"{reel_api}/{page_id}/video_reels",
            params={
                "upload_phase": "finish",
                "video_id": video_id,
                "video_state": "PUBLISHED",
                "description": caption,
                "access_token": token,
            },
            timeout=60,
        )
        fb = fin.json()
        if fin.status_code == 200 and fb.get("success", False):
            print(f"[reel] posted reel: {item['title'][:50]}")
            return True
        err = fb.get("error", {}).get("message", fin.text[:200])
        print(f"[reel] finish FAILED ({fin.status_code}): {err}")
        return False
    except Exception as e:
        print(f"[reel] error posting {item['title'][:40]}: {e}")
        return False


def post_to_facebook(items_with_cards):
    """
    Publish the top stories' cards to the Facebook page as proper FEED
    posts (image + text on the timeline), not album photo uploads.

    Method: upload each card unpublished (published=false) to get a photo
    id, then create a /feed post with that photo attached. This produces
    a normal timeline post with full news-feed distribution.
    Only runs if FB_PAGE_TOKEN and FB_PAGE_ID are set.
    """
    token = os.environ.get("FB_PAGE_TOKEN")
    page_id = os.environ.get("FB_PAGE_ID")
    if not token or not page_id:
        print("[fb] skipped (no FB_PAGE_TOKEN / FB_PAGE_ID set)")
        return

    posted = 0
    for item, card_path in items_with_cards[:FB_MAX_POSTS]:
        if not card_path or not os.path.exists(card_path):
            continue
        caption = build_caption(item)
        try:
            # Step 1: upload the photo UNPUBLISHED to get its id
            with open(card_path, "rb") as img:
                up = requests.post(
                    f"{FB_API}/{page_id}/photos",
                    data={"published": "false", "access_token": token},
                    files={"source": img},
                    timeout=60,
                )
            up_body = up.json()
            photo_id = up_body.get("id")
            if not photo_id:
                err = up_body.get("error", {}).get("message", up.text[:200])
                print(f"[fb] upload FAILED ({up.status_code}): {err}")
                continue

            # Step 2: create a real FEED post with the photo attached
            r = requests.post(
                f"{FB_API}/{page_id}/feed",
                data={
                    "message": caption,
                    "attached_media[0]": json.dumps({"media_fbid": photo_id}),
                    "access_token": token,
                },
                timeout=60,
            )
            body = r.json()
            if r.status_code == 200 and body.get("id"):
                posted += 1
                print(f"[fb] posted to feed: {item['title'][:50]}")
            else:
                err = body.get("error", {}).get("message", r.text[:200])
                print(f"[fb] feed post FAILED ({r.status_code}): {err}")
        except Exception as e:
            print(f"[fb] error posting {item['title'][:40]}: {e}")
        time.sleep(2)  # gentle pacing between posts
    print(f"[fb] {posted} feed post(s) published to page")


# ──────────────────────────────────────────────────────────────
# OUTPUT
# ──────────────────────────────────────────────────────────────

def edition_label(now):
    if now.hour < 10:
        return "Өглөөний дайжест"
    if now.hour < 15:
        return "Үдийн дайжест"
    return "Оройн дайжест"


def write_json(con):
    """Export today's digests for the website frontend."""
    today = datetime.now(UB_TZ).date().isoformat()
    rows = con.execute(
        "SELECT url, source, category, title, bullets, why, orig_min, "
        "published, sources, source_count, all_urls "
        "FROM digests WHERE run_at LIKE ? "
        "ORDER BY source_count DESC, published DESC",
        (today + "%",),
    ).fetchall()
    items = [{
        "url": r[0], "source": r[1], "category": r[2], "title": r[3],
        "bullets": json.loads(r[4]), "why": r[5],
        "origMin": r[6], "published": r[7],
        "sources": json.loads(r[8]) if r[8] else [r[1]],
        "sourceCount": r[9] or 1,
        "allUrls": json.loads(r[10]) if r[10] else [r[0]],
    } for r in rows]
    payload = {
        "generated": datetime.now(UB_TZ).isoformat(),
        "edition": edition_label(datetime.now(UB_TZ)),
        "items": items,
    }
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    print(f"[output] wrote {len(items)} items -> {OUTPUT_JSON}")
    return payload


def send_telegram(payload):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    new_items = payload["items"][:8]
    lines = [f"🗞 <b>Товч — {payload['edition']}</b>\n"]
    for it in new_items:
        lines.append(f"<b>{it['title']}</b>")
        for b in it["bullets"]:
            lines.append(f"  • {b}")
        lines.append(f"  💡 {it['why']}")
        srcs = ", ".join(it.get("sources", [it["source"]]))
        lines.append(f"  📰 {srcs}")
        lines.append(f"  <a href=\"{it['url']}\">унших →</a>\n")
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": "\n".join(lines),
              "parse_mode": "HTML", "disable_web_page_preview": True},
        timeout=REQUEST_TIMEOUT,
    )
    print("[output] telegram digest sent")


# ──────────────────────────────────────────────────────────────
# COLLECTOR MODE — fetch, summarize, queue (no posting)
# ──────────────────────────────────────────────────────────────

def is_duplicate_of_recent(client, con, new_title, new_bullets, days=3):
    """
    Check if a freshly-summarized story describes the SAME event as something
    already queued or recently posted. Clustering only dedupes within one
    collector run; this catches the same event appearing across different runs
    (e.g. 6:30 batch vs 11:30 batch, or two outlets worded differently).
    Returns True if it's a duplicate (should skip).
    """
    cutoff = (datetime.now(UB_TZ).date() - timedelta(days=days)).isoformat()
    rows = con.execute(
        "SELECT title FROM digests "
        "WHERE collected_date >= ? OR posted=1 "
        "ORDER BY run_at DESC LIMIT 40", (cutoff,)
    ).fetchall()
    recent = [r[0] for r in rows if r[0]]
    if not recent:
        return False

    # quick exact-ish check first (free): identical title
    if new_title in recent:
        return True

    # ask Claude: is the new story the same EVENT as any recent one?
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(recent))
    prompt = (
        "Доорх 'ШИНЭ мэдээ' нь 'ӨМНӨХ мэдээнүүд'-ийн аль нэгтэй ЯГ ИЖИЛ үйл "
        "явдлыг өгүүлж байна уу? (өөр өнцөг биш, ижил үйл явдал)\n\n"
        f"ШИНЭ мэдээ: {new_title}\n"
        f"({'; '.join(new_bullets[:2])})\n\n"
        f"ӨМНӨХ мэдээнүүд:\n{numbered}\n\n"
        "ЗӨВХӨН JSON: {\"duplicate\": true/false, \"match\": <дугаар эсвэл 0>}"
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = "".join(b.text for b in msg.content if b.type == "text")
        d = _parse_json_lenient(raw)
        return bool(d.get("duplicate", False))
    except Exception as e:
        print(f"[dedup] check failed (allowing through): {e}")
        return False


def run_collector():
    now = datetime.now(UB_TZ)
    today = now.date().isoformat()
    print(f"\n===== Иш COLLECTOR run @ {now.isoformat()} =====")

    client = Anthropic()  # uses ANTHROPIC_API_KEY env var
    con = db_init()
    print(f"[config] cards={CARDS_AVAILABLE}")

    candidates = collect_candidates(con)
    if not candidates:
        print("[collector] nothing new, exiting")
        write_json(con)
        return

    # ── Phase 1: fetch text + free ad filter ──────────────────
    articles = []
    skipped_ads = 0
    for src, title, url in candidates:
        mark_seen(con, url)
        if looks_like_ad(title, url):
            skipped_ads += 1
            print(f"[skip] ad (free filter): {title[:50]}")
            continue
        try:
            text = fetch_article_text(url, src["article_selector"])
            if len(text) < MIN_ARTICLE_CHARS:
                print(f"[skip] too short: {title[:50]}")
                continue
            articles.append({"src": src["name"], "title": title,
                             "url": url, "text": text})
        except Exception as e:
            print(f"[fail] {url}: {e}")

    if not articles:
        print("[collector] nothing usable after fetch/filter")
        write_json(con)
        return

    # ── Phase 2: cluster same-story articles ──────────────────
    clusters = cluster_candidates(client, articles)
    merged = sum(1 for c in clusters if len(c) > 1)
    print(f"[cluster] {len(articles)} articles -> {len(clusters)} stories "
          f"({merged} merged from multiple sources)")

    # ── Phase 3: summarize/synthesize + QUEUE (no posting) ────
    queued = 0
    for cluster in clusters:
        try:
            if len(cluster) == 1:
                d = summarize(client, cluster[0]["src"], cluster[0]["text"])
            else:
                d = synthesize_cluster(client, cluster)

            if not d.get("newsworthy", True):
                print(f"[skip] not newsworthy (AI filter): {cluster[0]['title'][:50]}")
                continue

            # safety floor: skip content that could get the page banned/sued
            if d.get("block", False):
                print(f"[skip] safety floor (block): {cluster[0]['title'][:50]}")
                continue

            # cross-run dedup: skip if same event already queued/recently posted
            if is_duplicate_of_recent(client, con, d["title"], d.get("bullets", [])):
                print(f"[skip] duplicate of recent: {d['title'][:50]}")
                continue

            primary = cluster[0]
            sources = sorted({a["src"] for a in cluster})
            all_urls = [a["url"] for a in cluster]
            total_words = sum(len(a["text"].split()) for a in cluster)
            orig_min = max(1, round(total_words / 180))

            # blended interest score: 50% importance + 50% emotional pull,
            # with a small boost for multi-source (already-big) stories.
            imp = max(0, min(100, int(d.get("importance", 50))))
            emo = max(0, min(100, int(d.get("emotional", 50))))
            multi_boost = min(15, (len(sources) - 1) * 5)
            interest = min(100, round(0.5 * imp + 0.5 * emo) + multi_boost)

            # render card now so the poster just uploads it later
            card_path = None
            if CARDS_AVAILABLE:
                try:
                    h = hashlib.md5(primary["url"].encode()).hexdigest()[:8]
                    fname = f"card_{queued:02d}_{h}.png"
                    card_path = make_card({**d, "sources": sources},
                                          out_dir="cards", filename=fname)
                except Exception as ce:
                    print(f"     card failed: {ce}")

            # queue it: posted=0, tagged with today's date
            con.execute(
                "INSERT OR REPLACE INTO digests "
                "(url, source, category, title, bullets, why, orig_min, "
                "published, run_at, sources, source_count, all_urls, "
                "posted, collected_date, card_path, interest_score) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (primary["url"], primary["src"], d.get("category", "Нийгэм"),
                 d["title"], json.dumps(d["bullets"], ensure_ascii=False),
                 d["why"], orig_min, now.isoformat(), now.isoformat(),
                 json.dumps(sources, ensure_ascii=False), len(sources),
                 json.dumps(all_urls, ensure_ascii=False),
                 0, today, card_path, interest),
            )
            con.commit()
            queued += 1
            tag = f" [{len(cluster)} sources]" if len(cluster) > 1 else ""
            print(f"[queued]{tag} (score {interest}) {d['title'][:50]}")
            time.sleep(1)
        except Exception as e:
            print(f"[fail] cluster {cluster[0]['title'][:40]}: {e}")

    # housekeeping: drop stale unposted stories (older than MAX_QUEUE_AGE_DAYS)
    cutoff = (now.date() - timedelta(days=MAX_QUEUE_AGE_DAYS)).isoformat()
    dropped = con.execute(
        "DELETE FROM digests WHERE posted=0 AND collected_date < ?", (cutoff,)
    ).rowcount
    con.commit()

    pending = con.execute("SELECT COUNT(*) FROM digests WHERE posted=0").fetchone()[0]
    print(f"[collector] {queued} queued ({skipped_ads} ads skipped, "
          f"{merged} multi-source); {dropped} stale dropped; "
          f"{pending} total pending in queue")
    write_json(con)


# ──────────────────────────────────────────────────────────────
# POSTER MODE — pick ONE queued story and post it
# ──────────────────────────────────────────────────────────────

def pick_story_to_post(con, now):
    """
    Selection logic:
      • Weekend (Sat/Sun): draw from the whole pending stockpile.
      • Weekday early-morning (before MORNING_FRESH_HOUR): prefer today's,
        else yesterday's leftovers (fresh batch may not be collected yet).
      • Weekday main hours: STRICTLY today's; only if today's queue is
        empty, fall back to leftovers.
    Within the allowed pool, order by importance then freshness.
    Returns (row_dict or None, mode_str).
    """
    today = now.date().isoformat()
    is_weekend = now.weekday() >= 5

    def fetch(where, params):
        return con.execute(
            "SELECT url, source, category, title, bullets, why, sources, "
            "source_count, card_path, collected_date FROM digests "
            "WHERE posted=0 AND " + where +
            " ORDER BY interest_score DESC, source_count DESC, collected_date DESC LIMIT 1",
            params,
        ).fetchone()

    if is_weekend:
        row = fetch("1=1", ())
        mode = "weekend-stockpile"
    elif now.hour < MORNING_FRESH_HOUR:
        row = fetch("collected_date = ?", (today,)) or fetch("collected_date < ?", (today,))
        mode = "morning"
    else:
        row = fetch("collected_date = ?", (today,))
        if row:
            mode = "weekday-today"
        else:
            row = fetch("collected_date < ?", (today,))
            mode = "weekday-fallback-leftover"

    if not row:
        return None, mode
    keys = ["url", "source", "category", "title", "bullets", "why",
            "sources", "source_count", "card_path", "collected_date"]
    return dict(zip(keys, row)), mode


def run_poster():
    now = datetime.now(UB_TZ)
    print(f"\n===== Иш POSTER run @ {now.isoformat()} =====")
    con = db_init()

    token = os.environ.get("FB_PAGE_TOKEN")
    page_id = os.environ.get("FB_PAGE_ID")
    if not token or not page_id:
        print("[poster] no FB credentials — abort")
        return

    story, mode = pick_story_to_post(con, now)
    if not story:
        pending = con.execute("SELECT COUNT(*) FROM digests WHERE posted=0").fetchone()[0]
        print(f"[poster] queue empty for mode '{mode}' — nothing to post "
              f"({pending} pending overall)")
        return

    print(f"[poster] mode={mode}  picking: {story['title'][:55]}")
    item = {
        "title": story["title"],
        "bullets": json.loads(story["bullets"]) if story["bullets"] else [],
        "why": story["why"] or "",
        "url": story["url"],
        "sources": json.loads(story["sources"]) if story["sources"] else [story["source"]],
        "source_count": story["source_count"] or 1,
    }

    # Regenerate the card now — the collector ran on a different machine,
    # so its card file no longer exists. Redraw from queued data (free/fast).
    card_path = None
    if CARDS_AVAILABLE:
        try:
            h = hashlib.md5(story["url"].encode()).hexdigest()[:8]
            card_path = make_card(
                {"title": item["title"], "bullets": item["bullets"],
                 "why": item["why"], "category": story["category"],
                 "sources": item["sources"]},
                out_dir="cards", filename=f"post_{h}.png",
            )
        except Exception as ce:
            print(f"[poster] card render failed: {ce}")
    else:
        card_path = story["card_path"]  # fallback to stored path

    ok = post_one_to_facebook(item, card_path, token, page_id)
    if ok:
        con.execute("UPDATE digests SET posted=1, posted_at=? WHERE url=?",
                    (now.isoformat(), story["url"]))
        con.commit()
        pending = con.execute("SELECT COUNT(*) FROM digests WHERE posted=0").fetchone()[0]
        print(f"[poster] posted ✓  ({pending} still pending)")

        # Also post a Reel using the SAME card (reuse, don't regenerate card)
        if POST_REELS and REELS_AVAILABLE and card_path:
            try:
                h = hashlib.md5(story["url"].encode()).hexdigest()[:8]
                reel_path = make_reel(card_path, out_dir="reels",
                                      filename=f"reel_{h}.mp4")
                if reel_path:
                    post_reel_to_facebook(item, reel_path, token, page_id)
                else:
                    print("[poster] reel render returned nothing")
            except Exception as re:
                print(f"[poster] reel step failed: {re}")
    else:
        print("[poster] post failed — left in queue for next hour")


# ──────────────────────────────────────────────────────────────
# ENTRYPOINT — mode dispatch via command-line argument
# ──────────────────────────────────────────────────────────────

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "collect"
    if mode == "collect":
        run_collector()
    elif mode == "post":
        run_poster()
    else:
        print(f"Unknown mode '{mode}'. Use 'collect' or 'post'.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
