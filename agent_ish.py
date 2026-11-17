#!/usr/bin/env python3
"""
Иш Тойм — Mongolian news agent (collector / poster / weather / currency /
insights modes; see CONTEXT.md for the full design and schedule).
Pipeline: sources -> title prefilter -> fetch -> cluster -> Claude
summarize + audience-stakes score -> queue (SQLite) -> poster picks the
best fresh story -> card + Reel to Facebook.

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
import socket
import sys
import time
import hashlib
import ipaddress
import io
import warnings
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse

import feedparser
import requests

# curl_cffi impersonates Chrome's TLS fingerprint to get past 403 blocks
# on sites that detect plain-Python requests (news.mn, gogo.mn, eguur.mn).
# Falls back to regular requests if unavailable.
try:
    from curl_cffi import requests as cffi_requests
    CFFI_AVAILABLE = True
except Exception as _cffi_err:
    CFFI_AVAILABLE = False
    print(f"[fetch] curl_cffi unavailable, using plain requests: {_cffi_err}")
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
        "name": "MONTSAME-pol",
        "listing": "https://montsame.mn/mn/more/18",  # УЛС ТӨР (politics)
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
    {
        "name": "tovch.mn-pol",
        "listing": "https://tovch.mn/politics",        # Улс төр (politics)
        "link_pattern": r"/n/[a-z0-9]+",
        "base_url": "https://tovch.mn",
        "article_selector": "article, div.news-detail, div.content, main",
    },
    {
        "name": "tovch.mn-soc",
        "listing": "https://tovch.mn/society",         # Нийгэм (social)
        "link_pattern": r"/n/[a-z0-9]+",
        "base_url": "https://tovch.mn",
        "article_selector": "article, div.news-detail, div.content, main",
    },
    {
        "name": "eguur.mn-pol",
        # politics category (URL-encoded 'улс-төр'); skips entertainment
        "listing": "https://eguur.mn/category/%d1%83%d0%bb%d1%81-%d1%82%d3%a9%d1%80/",
        "link_pattern": r"eguur\.mn/\d{5,}/",
        "base_url": "https://eguur.mn",
        "article_selector": "div.entry-content, article, div.content, main",
    },
    {
        "name": "eguur.mn-soc",
        # society category (URL-encoded 'нийгэм')
        "listing": "https://eguur.mn/category/%d0%bd%d0%b8%d0%b9%d0%b3%d1%8d%d0%bc/",
        "link_pattern": r"eguur\.mn/\d{5,}/",
        "base_url": "https://eguur.mn",
        "article_selector": "div.entry-content, article, div.content, main",
    },
    {
        "name": "zarig.mn-pol",
        "listing": "https://zarig.mn/politics",
        # root-level short-code articles (e.g. zarig.mn/1iqh); excludes
        # section/static slugs via length + end anchor
        "link_pattern": r"zarig\.mn/(?!busad$|live$)[a-z0-9]{3,5}$",
        "base_url": "https://zarig.mn",
        "article_selector": "div.news-detail, article, div.content, main",
    },
    {
        "name": "zarig.mn-soc",
        "listing": "https://zarig.mn/society",
        "link_pattern": r"zarig\.mn/(?!busad$|live$)[a-z0-9]{3,5}$",
        "base_url": "https://zarig.mn",
        "article_selector": "div.news-detail, article, div.content, main",
    },
    # ── gogo.mn & news.mn: BENCHED. IP-blocked from GitHub; the free-proxy
    #    route is too flaky to rely on. The proxy infrastructure (fetch_via_proxy,
    #    use_proxy flag) stays in place — just uncomment these two blocks to
    #    re-enable when we revisit with a better proxy solution.
    # {
    #     "name": "gogo.mn-pol",
    #     "listing": "https://gogo.mn/i/2",        # Улс төр
    #     "link_pattern": r"/r/[a-z0-9]+",
    #     "base_url": "https://gogo.mn",
    #     "article_selector": "div.article-body, div.news-detail, div.content, article",
    #     "use_proxy": True,
    # },
    # {
    #     "name": "news.mn",
    #     "rss": "https://news.mn/feed/",
    #     "article_selector": "div.article-body, div.entry-content, div.content, article",
    #     "use_proxy": True,
    # },
]

MAX_ARTICLES_PER_RUN = 12        # cost & noise control
MAX_PER_SOURCE = 6               # candidates per outlet per run (prefilter
                                 # is the cost gate, so a wide net is cheap)
# Per-source overrides of MAX_PER_SOURCE, set from the Aug-Nov 2026 Facebook
# data (share of Reels >10k views by source: zarig 29%, eguur/ikon 12%,
# MONTSAME 5%, tovch 2%). More slots for outlets whose stories travel,
# fewer for those whose stories rarely do. The 40-candidate round-robin cap
# still applies, so this shifts the mix without raising cost.
SOURCE_MAX = {
    "zarig.mn-pol": 8, "zarig.mn-soc": 8,
    "eguur.mn-pol": 8, "eguur.mn-soc": 8,
    "MONTSAME": 3, "MONTSAME-pol": 3, "MONTSAME-eco": 2,
    "tovch.mn-pol": 3, "tovch.mn-soc": 3,
}


def source_cap(src):
    return SOURCE_MAX.get(src.get("name", ""), MAX_PER_SOURCE)

MIN_ARTICLE_CHARS = 400          # skip stubs/photo posts
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_IMAGE_REDIRECTS = 3
MAX_IMAGE_PIXELS = 40_000_000
MAX_FETCH_ATTEMPTS = 3           # bounded cross-run retries for fetch failures
FETCH_RETRY_MAX_AGE_DAYS = 2     # never churn a dead URL beyond this age
MODEL = "claude-opus-5-5"   # thinking can't be disabled on this model, so
                            # every max_tokens below is a generous CEILING
                            # (thinking counts against it; you only pay for
                            # tokens actually used)
DB_PATH = "towch.db"
OUTPUT_JSON = "digest.json"      # the website reads this file
REQUEST_TIMEOUT = 15

# ── Queue-system settings ─────────────────────────────────────
MORNING_FRESH_HOUR = 9   # (legacy) kept for reference
FIRST_COLLECTION_HOUR = 11  # first daily collection runs at 10:15 UB, so
                            # slots before 11:00 draw on the prior day's news
MAX_QUEUE_AGE_DAYS = 5   # housekeeping: delete unposted rows older than this
MAX_POST_AGE_DAYS = 2    # FRESHNESS: never post a story collected more than
                         # 2 days ago (Aug-Nov data: 0-2 days old hit 13-16%,
                         # 3 days 6%, 5+ days 0%)

# ── Thread-fatigue cap ────────────────────────────────────────
# Follow-ups on one running story doubled to 35% of Reels in Oct-Nov and
# drew a few hundred views each after the first one or two. Allow at most
# THREAD_MAX_POSTS posts per story thread in THREAD_WINDOW_DAYS; a further
# post needs a breakout score (a real new development).
THREAD_WINDOW_DAYS = 4
THREAD_MAX_POSTS = 2
THREAD_BREAKOUT_SCORE = 75

# ── Card length limits ────────────────────────────────────────
# Cards over ~760 chars hit 5% vs 21% under ~600, inside every topic.
TITLE_MAX_CHARS = 80
BULLET_MAX_CHARS = 110
WHY_MAX_CHARS = 120
CARD_MAX_CHARS = 600     # title + bullets + why

# ── Audience-stakes scoring ───────────────────────────────────
# Topic labels assigned by the summarizer, with score adjustments set from
# the Aug-Nov 2026 hit rates (share of Reels >10k views): human drama 27%,
# confrontation/scandal 22%, smuggling/fraud 20%, personal money 18%,
# practical life 13%, other 9%, procedural/statistics 5%, courts of public
# figures 3%. Revisit monthly from the `insights` report.
TOPIC_ADJ = {
    "human_drama": 10,
    "confrontation": 8,
    "fraud": 8,
    "money": 6,
    "practical": 0,
    "other": 0,
    "procedural": -10,
    "courts": -15,
}
TOPICS = list(TOPIC_ADJ)
# Headline words that over-performed (pensions/elderly 44% hit, children
# 42%, "from date X this changes" 33%).
HEADLINE_BONUS = 5
HEADLINE_BONUS_RE = re.compile(
    r"тэтгэв|ахмад|настан|хүүх(эд|д)|сурагч|цэцэрлэг|эхлэн",
    re.IGNORECASE,
)
# (REEL_MIN_SCORE removed: at 6 posts/day every post gets a Reel.)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "mn,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

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

# Shared JSON schema + rules for summarize() and synthesize_cluster().
# Literal braces in the JSON are doubled for str.format.
OUTPUT_SCHEMA = """{{"title": "гарчиг — доорх ГАРЧИГ дүрмээр, {tmax} тэмдэгтээс хэтрэхгүй",
 "category": "{cats} — аль нэгийг сонго",
 "bullets": ["гол баримт, {bmax} тэмдэгтээс хэтрэхгүй", "...", "..."],
 "why": "энгийн хүнд яагаад хамаатайг 1 өгүүлбэрээр, {wmax} тэмдэгтээс хэтрэхгүй",
 "full_text": "Facebook пост дээр тавих ДЭЛГЭРЭНГҮЙ текст: 2-3 богино догол мөр (нийт 8-10 өгүүлбэр). Эхний догол мөрт юу болсныг гол баримтуудтай нь; хоёр дахьд ар дэвсгэр, нөхцөл байдал, оролцогч талуудын байр суурь/хариу үйлдэл; сүүлд нь энэ юунд хүргэх, дараа нь юу болох. Догол мөрүүдийг хоосон мөрөөр тусгаарла. Зөвхөн нийтлэлд байгаа баримтаар — нэмэлт таамаггүй.",
 "hashtags": ["энэ мэдээний гол сэдэв/хүн/байгууллага/газрыг илэрхийлсэн 3 монгол хаштаг", "...", "..."],
 "newsworthy": true/false,
 "topic": "human_drama | confrontation | fraud | money | practical | procedural | courts | other — аль нэг",
 "felt_stakes": 0-100,
 "conflict": 0-100,
 "novelty": 0-100,
 "mongolia_related": true/false,
 "block": true/false}}"""

SCORING_RULES = """ГАРЧИГ (хамгийн чухал — хүмүүс гарчгаар л үзэх эсэхээ шийддэг):
  • {tmax} тэмдэгтээс хэтрэхгүй (зай орно). Богино байх тусам сайн.
  • Хүмүүст ямар нөлөөтэй вэ, эсвэл ХЭН юу хийсэн бэ гэдгээр эхлүүл; гол тоог эхэнд нь.
  • "Хуралдаанаар...", "Яамнаас мэдээлснээр..." гэх мэт байгууллагын оршлоор
    бүү эхлүүл (байгууллага өөрөө гол баатар биш бол).
  • Зөвхөн баримтад тулгуурла: худал clickbait, асуулт хэлбэрийн гарчиг хэрэглэхгүй.
  Жишээ: "Нийслэлийн хотын албан татварыг 2 хувиас 1 хувь болгох саналыг НИТХ
  хэлэлцэнэ" → "Ресторан, буудалд төлөх хотын татвар хоёр дахин буурах нь".
КАРТ БОГИНО: bullets яг 3, тус бүр {bmax} тэмдэгтээс, why {wmax} тэмдэгтээс
  хэтрэхгүй. Нэг өгүүлбэрт нэг санаа. Илүү дэлгэрэнгүйг full_text-д бич.

"hashtags": ЯГ 3 ширхэг. Мэдээнд гардаг тодорхой хүн, байгууллага, газар, эсвэл
  гол сэдвийг нэрлэсэн монгол хаштаг (жишээ: УИХ, ОюуТолгой, татвар,
  Улаанбаатар). Дүрэм: # тэмдэг БИЧИХГҮЙ; зай оруулахгүй (олон үгийг залгаж
  эсвэл том үсгээр); зөвхөн монгол/латин үсэг, тоо; ерөнхий үг биш (#мэдээ,
  #Монгол гэхгүй — тэдгээр аль хэдийн нэмэгддэг).

"newsworthy": false — далд зар, бүтээгдэхүүн/үйлчилгээ борлуулах текст,
  баримтгүй байгууллагын PR, өөрийгөө магтсан хоосон мэдэгдэл. Бусад бүх
  жинхэнэ мэдээ (улс төр, нийгэм, эдийн засаг, спорт, соёл, хүний түүх) =
  true. Эргэлзвэл true.

"topic" — уншигчийн нүдээр ангил (яг нэгийг):
  human_drama   — хүний хувь заяа: сураггүй алга болсон, амь насаа алдсан,
                  осол, гал түймэр, аврах ажиллагаа, хүүхэд/ахмадын аюул
  confrontation — нэртэй албан тушаалтан/улс төрч/байгууллагыг илчилсэн,
                  буруутгасан, огцрохыг шаардсан, хурц маргаан, олны уур,
                  жагсаал; шинээр баривчлагдсан өндөр албан тушаалтан
  fraud         — хууль бус хил давуулалт, их хэмжээний залилан, хулгай,
                  хуурамч бараа, төрийн хөрөнгө завшсан
  money         — иргэдийн хармаанд шууд: тэтгэвэр, цалин, тэтгэмж, татвар,
                  үнэ, тариф, түлш, зээл, хүүхдийн мөнгө
  practical     — өдөр тутамд хэрэгтэй мэдээлэл: шинэ журам хэзээнээс
                  мөрдөх, хуваарь, зам хаалт, бүртгэл, анхааруулга
  procedural    — хурал, хэлэлцүүлэг, уулзалт, айлчлал, санамж бичиг,
                  төлөвлөгөө, статистик тайлан, ёслол, шагнал
  courts        — олны танил хүний шүүх хурал товлогдсон/хойшилсон/
                  хорих хугацаа сунгасан гэх мэт явцын мэдээ
  other         — бусад (спорт, соёл, технологи г.м.)

"felt_stakes" (0-100): энгийн уншигч "энэ надад, миний гэр бүлд хамаатай"
  эсвэл "энэ хүний хувь заяа сэтгэл хөдөлгөм" гэж мэдрэх үү? Мөнгө, аюулгүй
  байдал, эрүүл мэнд, хүүхэд, ахмад, өдөр тутмын амьдрал, хүний драм = өндөр.
  Хурал, ёслол, статистик, албан хуурай мэдээ = 0-20.
"conflict" (0-100): нэртэй хүн/байгууллагыг буруутгасан, илчилсэн, эсэргүүцсэн,
  хоорондоо маргасан, олны дургүйцэл төрүүлсэн үү? Хурц зөрчил = өндөр;
  зөрчилгүй мэдээ = 0-15.
"novelty" (0-100): өнөөдөр шинээр болсон, хүмүүс анх удаа сонсох үйл явдал уу?
  Шинэ = өндөр; хуучин сэдвийн үргэлжлэл, товлосон хурал, давтагдсан
  мэдээлэл = бага.
"mongolia_related" (true/false): Монгол Улстай ШУУД холбоотой юу? Гадаад мэдээ
  бол зөвхөн Монгол, монголчуудыг шууд хамарсан үед true.
"block" (ЦӨӨХӨН тохиолдолд true): цуст/аймшигт дүрслэл, золгүй явдлын
  хохирогчийг мөлжсөн, баталгаагүй гүтгэлэг/нэр төр гутаах, үзэн ядалт
  өдөөсөн контент. Бусад бүх тохиолдолд false — ердийн сэтгэл хөдөлгөм,
  дуулиантай мэдээг блоклохгүй.

ОГНОО: өнөөдөр {today}. Он, сар, өдрийг нийтлэлд бичсэнээр нь л бич.
Нийтлэлд он заагаагүй бол өөрөө он НЭМЖ БИЧИХГҮЙ."""

PROMPT = """Чи Монголын Facebook мэдээний хуудасны редактор: мэдээг энгийн,
ойлгомжтой, хүмүүсийн үзэж хуваалцмаар болгодог.
Доорх нийтлэлийг уншаад ЗӨВХӨН дараах JSON-оор хариул (өөр юу ч бичихгүй, markdown хэрэглэхгүй):

{schema}

{rules}

Нийтлэл ({source}):
{text}"""


def _prompt_kwargs():
    """Format args shared by PROMPT and SYNTH_PROMPT."""
    lim = {"tmax": TITLE_MAX_CHARS, "bmax": BULLET_MAX_CHARS,
           "wmax": WHY_MAX_CHARS}
    today = datetime.now(UB_TZ).date().isoformat()
    return {
        "schema": OUTPUT_SCHEMA.format(cats="/".join(CATEGORIES), **lim),
        "rules": SCORING_RULES.format(today=today, **lim),
    }


# ──────────────────────────────────────────────────────────────
# STORAGE (dedupe across runs)
# ──────────────────────────────────────────────────────────────

def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS seen (
        url TEXT PRIMARY KEY,
        first_seen TEXT
    )""")
    con.execute("""CREATE TABLE IF NOT EXISTS fetch_attempts (
        url TEXT PRIMARY KEY,
        attempts INTEGER NOT NULL,
        first_seen TEXT NOT NULL
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
                      ("interest_score", "INTEGER DEFAULT 50"),  # engagement ranking
                      ("full_text", "TEXT"),                 # elaborated caption
                      ("image_url", "TEXT"),                 # article og:image
                      ("fb_post_id", "TEXT"),                # confirmed FB feed object id
                      ("reel_posted", "INTEGER DEFAULT 0"),  # 1 only after Reel confirms
                      ("review_needed", "INTEGER DEFAULT 0"),  # ambiguous feed outcome
                      ("hashtags", "TEXT"),                  # JSON array of article-specific hashtag tokens (no '#')
                      ("topic", "TEXT"),                     # audience topic label (TOPIC_ADJ keys)
                      ("card_chars", "INTEGER"),             # title+bullets+why length on the card
                      ("reel_video_id", "TEXT"),             # FB Reel video id (for insights)
                      ("views", "INTEGER"),                  # Reel plays, from `insights` mode
                      ("avg_watch_s", "REAL"),               # avg seconds watched, from `insights`
                      ("insights_at", "TEXT")]:              # when views were last read
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


def record_fetch_failure(con, url, now):
    """Record a survivor fetch failure and decide whether it may retry."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=UB_TZ)

    row = con.execute(
        "SELECT attempts, first_seen FROM fetch_attempts WHERE url=?",
        (url,),
    ).fetchone()
    if row is None:
        attempts = 1
        first_seen_raw = now.isoformat()
        con.execute(
            "INSERT INTO fetch_attempts (url, attempts, first_seen) "
            "VALUES (?, ?, ?)",
            (url, attempts, first_seen_raw),
        )
    else:
        attempts = row[0] + 1
        first_seen_raw = row[1]
        # Deliberately update only attempts: the retry age must never reset.
        con.execute(
            "UPDATE fetch_attempts SET attempts=? WHERE url=?",
            (attempts, url),
        )

    try:
        first_seen = datetime.fromisoformat(first_seen_raw)
        if first_seen.tzinfo is None:
            first_seen = first_seen.replace(tzinfo=UB_TZ)
        age = now - first_seen
        age_days = max(0.0, age.total_seconds() / 86400)
        age_expired = age > timedelta(days=FETCH_RETRY_MAX_AGE_DAYS)
    except (TypeError, ValueError):
        # Corrupt retry state must fail closed instead of churning forever.
        age_days = float("nan")
        age_expired = True

    if attempts >= MAX_FETCH_ATTEMPTS or age_expired:
        con.execute(
            "INSERT OR IGNORE INTO seen (url, first_seen) VALUES (?, ?)",
            (url, now.isoformat()),
        )
        con.execute("DELETE FROM fetch_attempts WHERE url=?", (url,))
        con.commit()
        age_text = "unknown" if age_days != age_days else f"{age_days:.1f} days"
        print(
            f"[fetch-retry] GIVING UP {url} after {attempts} attempts / "
            f"age {age_text}"
        )
        return "giveup"

    # Only a failed prefilter survivor reaches this deletion. Rejected titles
    # never enter this helper and remain permanently marked seen.
    con.execute("DELETE FROM seen WHERE url=?", (url,))
    con.commit()
    print(
        f"[fetch-retry] transient fetch fail, will retry "
        f"(attempt {attempts}/{MAX_FETCH_ATTEMPTS}): {url}"
    )
    return "retry"


def clear_fetch_retry(con, url):
    """Clear retry state after a survivor fetch succeeds."""
    con.execute(
        "INSERT OR IGNORE INTO seen (url, first_seen) VALUES (?, ?)",
        (url, datetime.now(UB_TZ).isoformat()),
    )
    con.execute("DELETE FROM fetch_attempts WHERE url=?", (url,))
    con.commit()


# ──────────────────────────────────────────────────────────────
# COLLECTION
# ──────────────────────────────────────────────────────────────

def fetch_html(url, timeout=REQUEST_TIMEOUT, use_proxy=False):
    """
    Fetch a page. Normal sources use curl_cffi with Chrome TLS impersonation.
    IP-blocked sources (use_proxy=True) route through free proxies first,
    then fall back to a direct attempt.
    """
    if use_proxy:
        r = fetch_via_proxy(url, timeout=min(timeout, 12))
        if r is not None:
            return r
        # fall through to a direct attempt (usually fails, but harmless)
    if CFFI_AVAILABLE:
        try:
            return cffi_requests.get(
                url, headers=HEADERS, timeout=timeout, impersonate="chrome"
            )
        except Exception as e:
            print(f"[fetch] curl_cffi failed ({e}); falling back to requests")
    return requests.get(url, headers=HEADERS, timeout=timeout)


# ── Free-proxy pool (for IP-blocked sources like gogo.mn / news.mn) ──
_PROXY_LIST_URL = ("https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list"
                   "@main/proxies/protocols/http/data.json")
_proxy_pool = None   # cached per run

def get_proxy_pool(limit=8):
    """
    Fetch a small pool of high-uptime free HTTP proxies, sorted by uptime.
    Cached for the run. Returns list of 'http://ip:port' strings (maybe []).
    Free proxies are unreliable, so callers must try several and fall back.
    """
    global _proxy_pool
    if _proxy_pool is not None:
        return _proxy_pool
    urls = [
        _PROXY_LIST_URL,
        "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/all/data.json",
    ]
    data = None
    for u in urls:
        try:
            r = requests.get(u, timeout=15)
            if r.status_code == 200:
                data = r.json()
                break
        except Exception:
            continue
    if not data:
        print("[proxy] could not load proxy list")
        _proxy_pool = []
        return _proxy_pool
    try:
        # only HTTP-capable proxies for requests' http/https routing
        good = [p for p in data
                if p.get("protocol", "http") in ("http", "https")
                and (p.get("uptime_percent") or 0) >= 80
                and (p.get("latency_ms") or 9999) < 2000]
        good.sort(key=lambda p: (-(p.get("uptime_percent") or 0),
                                 p.get("latency_ms") or 9999))
        _proxy_pool = [f"http://{p['ip']}:{p['port']}" for p in good[:limit]]
        print(f"[proxy] loaded {len(_proxy_pool)} candidate proxies")
    except Exception as e:
        print(f"[proxy] parse failed: {e}")
        _proxy_pool = []
    return _proxy_pool


def fetch_via_proxy(url, timeout=12, tries=4):
    """
    Fetch a URL through free proxies, trying several until one works.
    Returns a response or None. Bounded so flaky proxies can't hang a run.
    """
    pool = get_proxy_pool()
    for proxy in pool[:tries]:
        try:
            proxies = {"http": proxy, "https": proxy}
            r = requests.get(url, headers=HEADERS, proxies=proxies,
                             timeout=timeout)
            if r.status_code == 200 and len(r.text) > 500:
                print(f"[proxy] ok via {proxy}")
                return r
        except Exception:
            continue  # dead proxy, try next
    print(f"[proxy] all proxies failed for {url[:50]}")
    return None


def collect_from_rss(src, con):
    # fetch the feed via curl_cffi (some sites block plain feedparser),
    # then hand the raw bytes to feedparser.
    try:
        r = fetch_html(src["rss"], use_proxy=src.get("use_proxy", False))
        feed = feedparser.parse(r.content)
    except Exception as e:
        print(f"[collect] {src['name']} RSS fetch failed: {e}")
        feed = feedparser.parse(src["rss"])  # last-resort direct parse
    fresh = []
    for entry in feed.entries:
        url = entry.get("link", "").strip()
        if url and is_new(con, url):
            fresh.append((src, entry.get("title", ""), url))
        if len(fresh) >= source_cap(src):
            break
    return fresh


def collect_from_listing(src, con):
    """Scrape a normal news-list page for article links."""
    r = fetch_html(src["listing"], use_proxy=src.get("use_proxy", False))
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
        if len(fresh) >= source_cap(src):
            break
    return fresh


def collect_candidates(con):
    """Pull all sources and return up to 40 source-balanced candidates."""
    source_batches = []
    for src in SOURCES:
        try:
            if src.get("rss"):
                fresh = collect_from_rss(src, con)
            elif src.get("listing"):
                fresh = collect_from_listing(src, con)
            else:
                continue
            source_batches.append(fresh)
            print(f"[collect] {src['name']}: {len(fresh)} new")
        except Exception as e:
            print(f"[collect] {src['name']} FAILED: {e}")

    # Keep the prefilter input budget at 40 without favoring sources that
    # appear early in SOURCES. Take one candidate from each source per
    # round until the cap is reached or every source batch is exhausted.
    candidates = []
    round_index = 0
    while len(candidates) < 40:
        added = False
        for batch in source_batches:
            if round_index >= len(batch):
                continue
            candidates.append(batch[round_index])
            added = True
            if len(candidates) >= 40:
                break
        if not added:
            break
        round_index += 1
    return candidates


def fetch_article_text(url, selector, use_proxy=False):
    """
    Download the article page. Returns (text, image_url) where image_url
    is the article's share image (og:image / twitter:image) or None.
    """
    r = fetch_html(url, use_proxy=use_proxy)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # article share image (extract BEFORE decomposing tags)
    image_url = None
    for prop in (("property", "og:image"), ("name", "twitter:image"),
                 ("property", "og:image:url")):
        m = soup.find("meta", attrs={prop[0]: prop[1]})
        if m and m.get("content", "").strip().startswith("http"):
            image_url = m["content"].strip()
            break

    for tag in soup(["script", "style", "nav", "footer", "aside", "iframe"]):
        tag.decompose()

    node = None
    for sel in selector.split(","):
        node = soup.select_one(sel.strip())
        if node:
            break
    text = (node or soup.body or soup).get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text, image_url


# ──────────────────────────────────────────────────────────────
# SUMMARIZATION
# ──────────────────────────────────────────────────────────────

def _parse_json_lenient(raw):
    """Parse Claude's JSON, repairing common truncation issues."""
    raw = raw.replace("```json", "").replace("```", "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # 1) try trimming to the last complete object
    last = raw.rfind("}")
    if last != -1:
        try:
            return json.loads(raw[:last + 1])
        except json.JSONDecodeError:
            pass
    # 2) truncated mid-string (e.g. full_text cut off): close the open
    #    string, arrays, and object so the remaining fields stay usable.
    try:
        repaired = raw
        if repaired.count('"') % 2 == 1:
            repaired += '"'
        repaired += "]" * max(0, repaired.count("[") - repaired.count("]"))
        repaired += "}" * max(0, repaired.count("{") - repaired.count("}"))
        return json.loads(repaired)
    except json.JSONDecodeError:
        raise


def summarize(client, source_name, text):
    msg = client.messages.create(
        model=MODEL,
        max_tokens=8000,   # thinking + 2-3 paragraph full_text JSON
                           # (truncation kills the whole story's JSON)
        messages=[{
            "role": "user",
            "content": PROMPT.format(
                source=source_name,
                text=text[:8000],
                **_prompt_kwargs(),
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
                max_tokens=2000,   # thinking + tiny grouping array
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


SYNTH_PROMPT = """Чи Монголын Facebook мэдээний хуудасны редактор: мэдээг энгийн,
ойлгомжтой, хүмүүсийн үзэж хуваалцмаар болгодог.
Доорх нь ОЛОН эх сурвалж НЭГ үйл явдлыг мэдээлсэн нийтлэлүүд. Бүгдийг
уншаад нэгтгэн (bullets, full_text-д бүх эх сурвалжийн баримтыг нэгтгэ),
ЗӨВХӨН дараах JSON-оор хариул (өөр юу ч бичихгүй, markdown хэрэглэхгүй):

{schema}

{rules}
Эх сурвалжууд зөрчилтэй мэдээлэл өгвөл түүнийг тэмдэглэ.

{articles}"""


def synthesize_cluster(client, cluster):
    """One digest from multiple same-story articles."""
    blocks = []
    for a in cluster:
        blocks.append(f"--- Эх сурвалж: {a['src']} ---\n{a['text'][:4000]}")
    msg = client.messages.create(
        model=MODEL,
        max_tokens=8000,   # thinking + 2-3 paragraph full_text JSON
        messages=[{"role": "user", "content": SYNTH_PROMPT.format(
            articles="\n\n".join(blocks),
            **_prompt_kwargs(),
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


def clean_source_names(sources):
    """'tovch.mn-pol','tovch.mn-soc' -> 'tovch.mn' (deduped, order kept)."""
    out = []
    for s in sources or []:
        s = re.sub(r"-(pol|soc|eco)$", "", str(s).strip())
        if s and s not in out:
            out.append(s)
    return out


# Fixed, human-checked wording (not AI-generated) so there's no grammar risk.
# Reels carrying a prompt averaged 1.54 comments vs 0.87 without (Oct-Nov).
COMMENT_PROMPTS = [
    "Та энэ талаар юу гэж бодож байна? Сэтгэгдлээ үлдээгээрэй.",
    "Энэ асуудлыг та хэрхэн харж байна вэ?",
    "Санал бодлоо сэтгэгдэл хэсэгт хуваалцаарай.",
]
COMMENT_PROMPT_MIN_SCORE = 55   # roughly the top half of the queue
COMMENT_PROMPT_SKIP_TOPICS = {"human_drama"}   # no "what do you think?"
                                               # under a death/missing story


# ── Hashtags ──────────────────────────────────────────────────
# Every caption (feed post AND reel, since both call build_caption) ends
# with 6 hashtags: 3 FIXED brand/site tags + up to 3 ARTICLE-specific tags.
# The article tags are generated by the summarizer, sanitized at collect
# time, and stored in the `hashtags` column. This is a reach lever: FB
# hashtags are a minor signal, but free to add.
#
# FIXED_HASHTAGS is editorial — change these three words freely.
FIXED_HASHTAGS = ["#ИшТойм", "#Монгол", "#мэдээ"]

# Fallback article tags by category, used only when the AI returns fewer
# than 3 usable tags (or for old queued items with no stored hashtags).
# Keep these DISTINCT from FIXED_HASHTAGS (no "Монгол"/"мэдээ") so dedup
# never shrinks the line below 6.
CATEGORY_HASHTAGS = {
    "Улс төр":     ["улстөр", "улсТөр", "Монголынулстөр"],
    "Эдийн засаг": ["эдийнзасаг", "эдийнЗасаг", "эдийнзасагМэдээ"],
    "Нийгэм":      ["нийгэм", "нийгмийнмэдээ", "нийгэмМонгол"],
    "Технологи":   ["технологи", "технологиМэдээ", "шинжлэхухаан"],
    "Спорт":       ["спорт", "монголспорт", "тамирчид"],
    "Дэлхий":      ["дэлхий", "олонулс", "дэлхийнмэдээ"],
}


def _sanitize_hashtag(raw):
    """One raw AI tag → a clean bare token (no '#'), or None if unusable.
    Strips '#', removes spaces/punctuation (joining multi-word tags), keeps
    only word characters (Cyrillic/Latin letters, digits, underscore)."""
    if not isinstance(raw, str):
        return None
    tok = re.sub(r"[^\w]", "", raw.strip().lstrip("#"), flags=re.UNICODE)
    # FB ignores all-numeric / leading-digit tags; drop too-short/long.
    if not tok or tok[0].isdigit() or len(tok) < 2 or len(tok) > 40:
        return None
    return tok


def _clean_hashtags(raw_list, category):
    """AI's raw hashtag list → up to 3 unique bare tokens, padded from the
    category fallback if the AI returned fewer than 3 usable tags. Tokens
    colliding with FIXED_HASHTAGS are skipped so the composed line always
    reaches 6 distinct tags."""
    out = []
    seen = {t.lower().lstrip("#") for t in FIXED_HASHTAGS}

    def _add(tok):
        low = tok.lower()
        if low in seen:
            return
        seen.add(low)
        out.append(tok)

    for raw in (raw_list or []):
        tok = _sanitize_hashtag(raw)
        if tok:
            _add(tok)
        if len(out) >= 3:
            return out[:3]
    for tok in CATEGORY_HASHTAGS.get(category, []):
        if len(out) >= 3:
            break
        _add(tok)
    return out[:3]


def _compose_hashtag_line(item):
    """FIXED_HASHTAGS + up to 3 article tags, deduped, capped at 6.
    Article tags come from the stored `hashtags` list; if absent (old
    items), fall back to the category tags."""
    tags = list(FIXED_HASHTAGS)
    seen = {t.lower().lstrip("#") for t in tags}
    article = item.get("hashtags") or CATEGORY_HASHTAGS.get(item.get("category", ""), [])
    for t in article:
        tok = str(t).strip().lstrip("#")
        if not tok or tok.lower() in seen:
            continue
        seen.add(tok.lower())
        tags.append("#" + tok)
        if len(tags) >= 6:
            break
    return " ".join(tags)


def build_caption(item):
    """
    Compose the Facebook post text (caption above the card / Reel).
    full_text depth, bullets, why-line, an optional comment prompt,
    clean source names, then 3 fixed + 3 article hashtags.
    The CARD image itself is unchanged — this only affects post text.
    """
    lines = [item["title"], ""]

    # Elaborated write-up (the new richer text). Falls back gracefully
    # to bullets-only if an older queued item lacks full_text.
    full = (item.get("full_text") or "").strip()
    if full:
        lines.append(full)
        lines.append("")

    # Key points as bullets (kept — they scan well on mobile)
    for b in item["bullets"]:
        lines.append(f"• {b}")

    if item.get("why"):
        lines.append("")
        lines.append(f"💡 Яагаад чухал вэ? {item['why']}")

    if ((item.get("interest_score") or 0) >= COMMENT_PROMPT_MIN_SCORE
            and item.get("topic") not in COMMENT_PROMPT_SKIP_TOPICS):
        h = int(hashlib.md5(item["url"].encode()).hexdigest(), 16)
        lines.append("")
        lines.append(f"💬 {COMMENT_PROMPTS[h % len(COMMENT_PROMPTS)]}")

    lines.append("")
    srcs = ", ".join(clean_source_names(
        item.get("sources") or [item.get("source", "")]))
    lines.append(f"📰 Эх сурвалж: {srcs}")
    lines.append(f"🔗 {item['url']}")
    lines.append("")
    lines.append(_compose_hashtag_line(item))
    return "\n".join(lines)


def post_one_to_facebook(item, card_path, token, page_id):
    """
    Post ONE story's card as a feed post.

    Returns {"status": "success|clean_failure|ambiguous_failure",
             "fb_post_id": str|None}.
    Only the public /feed request can be ambiguous; an unpublished-photo
    upload failure is clean because no feed post was attempted.
    """
    if not card_path or not os.path.exists(card_path):
        print(f"[fb] no card file for: {item['title'][:40]}")
        return {"status": "clean_failure", "fb_post_id": None}
    caption = build_caption(item)

    # Step 1: upload photo unpublished. Failure here cannot create a public
    # feed post, so it is always safe for the queue to retry naturally.
    try:
        with open(card_path, "rb") as img:
            up = requests.post(
                f"{FB_API}/{page_id}/photos",
                data={"published": "false", "access_token": token},
                files={"source": img}, timeout=60,
            )
        try:
            up_body = up.json()
        except ValueError:
            up_body = {}
        photo_id = up_body.get("id")
        if not photo_id:
            err = up_body.get("error", {}).get("message", up.text[:200])
            print(f"[fb] upload FAILED ({up.status_code}): {err}")
            return {"status": "clean_failure", "fb_post_id": None}
    except Exception as e:
        print(f"[fb] unpublished photo upload FAILED: {e}")
        return {"status": "clean_failure", "fb_post_id": None}

    # Step 2: create the public feed post. A timeout or lost connection after
    # sending is ambiguous: Facebook may have accepted it despite no response.
    try:
        r = requests.post(
            f"{FB_API}/{page_id}/feed",
            data={"message": caption,
                  "attached_media[0]": json.dumps({"media_fbid": photo_id}),
                  "access_token": token},
            timeout=60,
        )
    except requests.exceptions.Timeout as e:
        print(f"[fb] AMBIGUOUS feed timeout — REVIEW REQUIRED: {e}")
        return {"status": "ambiguous_failure", "fb_post_id": None}
    except requests.exceptions.ConnectionError as e:
        message = str(e).lower()
        clean_markers = (
            "connection refused",
            "failed to establish a new connection",
            "getaddrinfo failed",
            "name or service not known",
            "network is unreachable",
        )
        if any(marker in message for marker in clean_markers):
            print(f"[fb] feed connection rejected before response: {e}")
            return {"status": "clean_failure", "fb_post_id": None}
        print(f"[fb] AMBIGUOUS feed connection loss — REVIEW REQUIRED: {e}")
        return {"status": "ambiguous_failure", "fb_post_id": None}
    except requests.exceptions.RequestException as e:
        print(f"[fb] AMBIGUOUS feed request failure — REVIEW REQUIRED: {e}")
        return {"status": "ambiguous_failure", "fb_post_id": None}
    except Exception as e:
        print(f"[fb] AMBIGUOUS feed failure — REVIEW REQUIRED: {e}")
        return {"status": "ambiguous_failure", "fb_post_id": None}

    try:
        body = r.json()
    except ValueError:
        body = {}
    if 200 <= r.status_code < 300:
        post_id = body.get("id")
        if post_id:
            print(f"[fb] posted to feed: {item['title'][:50]} ({post_id})")
            return {"status": "success", "fb_post_id": post_id}
        print("[fb] AMBIGUOUS successful response without post id — "
              "REVIEW REQUIRED")
        return {"status": "ambiguous_failure", "fb_post_id": None}

    err = body.get("error", {}).get("message", r.text[:200])
    print(f"[fb] feed post REJECTED ({r.status_code}): {err}")
    return {"status": "clean_failure", "fb_post_id": None}


def post_reel_to_facebook(item, reel_path, token, page_id):
    """
    Publish a Reel to the page via the 3-phase Reels API:
      1. start  -> get video_id + upload_url
      2. upload -> POST the file binary to the upload_url
      3. finish -> publish with description
    Returns the Reel's video_id (truthy) on success, else None/False.
    The id is stored so the weekly `insights` mode can read its views.
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
            print(f"[reel] posted reel: {item['title'][:50]} ({video_id})")
            return video_id
        err = fb.get("error", {}).get("message", fin.text[:200])
        print(f"[reel] finish FAILED ({fin.status_code}): {err}")
        return False
    except Exception as e:
        print(f"[reel] error posting {item['title'][:40]}: {e}")
        return False


# (Old batch post_to_facebook removed — poster posts one story
#  at a time via post_one_to_facebook.)

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
        "published, sources, source_count, all_urls, full_text, "
        "interest_score, posted, topic "
        "FROM digests WHERE run_at LIKE ? "
        "ORDER BY interest_score DESC, published DESC",
        (today + "%",),
    ).fetchall()
    items = [{
        "url": r[0], "source": r[1], "category": r[2], "title": r[3],
        "bullets": json.loads(r[4]), "why": r[5],
        "origMin": r[6], "published": r[7],
        "sources": json.loads(r[8]) if r[8] else [r[1]],
        "sourceCount": r[9] or 1,
        "allUrls": json.loads(r[10]) if r[10] else [r[0]],
        "fullText": r[11] or "",
        "interestScore": r[12],
        "posted": bool(r[13]),
        "topic": r[14],
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

def _norm_words(text):
    """Lowercase word set, stripping short/common tokens, for cheap overlap."""
    import re as _re
    words = _re.findall(r"[\w\u0400-\u04FF]+", (text or "").lower())
    # drop very short tokens (particles) that add noise
    return {w for w in words if len(w) >= 4}


def _title_similarity(a, b):
    """Jaccard word-overlap between two titles (0-1). Free, no AI."""
    wa, wb = _norm_words(a), _norm_words(b)
    if not wa or not wb:
        return 0.0
    inter = len(wa & wb)
    union = len(wa | wb)
    return inter / union if union else 0.0


def _number_fingerprint(text):
    """
    Distinctive numbers in a story (free same-event signal). Two outlets
    covering one press release word their headlines differently but quote
    the same figures (e.g. 173 cases, 2,779 victims, 211.1bn). Keeps only
    'strong' numbers: decimals, or 3+ digits — excluding years (1990-2035)
    and round figures (x00) that recur across unrelated stories.
    """
    out = set()
    for tok in re.findall(r"\d[\d,.]*\d|\d", text or ""):
        tok = tok.replace(",", "").rstrip(".")
        if not tok:
            continue
        has_dec = "." in tok
        digits = tok.replace(".", "")
        if not has_dec and len(digits) < 3:
            continue
        if not has_dec:
            try:
                v = int(digits)
            except ValueError:
                continue
            if 1990 <= v <= 2035 or v % 100 == 0:
                continue
        out.add(tok)
    return out


def _story_text(title, bullets):
    """Title + bullets as one string (bullets may be list or JSON text)."""
    if isinstance(bullets, str):
        try:
            bullets = json.loads(bullets)
        except Exception:
            bullets = [bullets]
    return " ".join([title or ""] + [str(b) for b in (bullets or [])])


def same_event_by_numbers(text_a, text_b, min_shared=2):
    """True if two stories share >= min_shared distinctive numbers."""
    return len(_number_fingerprint(text_a) & _number_fingerprint(text_b)) >= min_shared


def is_duplicate_of_recent(client, con, new_title, new_bullets, days=3):
    """
    Two-stage dedup to minimise AI cost:
      1. FREE word-overlap pre-filter finds plausible candidates. If the
         best overlap is very low, it's obviously not a dup — skip the AI
         call entirely (this is the common case, so most stories cost $0).
      2. Only when there ARE similar-looking candidates do we ask Claude,
         and we send just the top few (not all 40) to keep the prompt short.
         The check sends bullets alongside titles: rewritten titles about
         the same event emphasise different numbers/places, so titles alone
         systematically miss rewordings.
    Returns True if it's a duplicate (should skip).
    """
    cutoff = (datetime.now(UB_TZ).date() - timedelta(days=days)).isoformat()
    rows = con.execute(
        "SELECT title, bullets FROM digests "
        "WHERE collected_date >= ? OR posted=1 "
        "ORDER BY run_at DESC LIMIT 80", (cutoff,)
    ).fetchall()
    recent = [(r[0], r[1]) for r in rows if r[0]]
    if not recent:
        return False

    # exact match — free, instant
    if new_title in (t for t, _b in recent):
        return True

    # number fingerprint — free: same figures = same event, even when the
    # headlines share few words (the housing-fraud double in Sept 2026)
    new_text = _story_text(new_title, new_bullets)
    for t, b in recent:
        if same_event_by_numbers(new_text, _story_text(t, b)):
            print(f"[dedup] number-fingerprint match: {t[:50]}")
            return True

    # Stage 1: free similarity scoring
    scored = sorted(
        ((_title_similarity(new_title, t), t, b) for t, b in recent),
        key=lambda x: x[0], reverse=True,
    )
    best_sim = scored[0][0] if scored else 0.0

    # Very high overlap => almost certainly the same event; treat as dup
    # without paying for an AI call.
    if best_sim >= 0.6:
        return True
    # Very low overlap => clearly different topic; skip the AI call.
    # Floor is 0.12: AI-retitling plus Mongolian suffix morphology push
    # genuine same-event rewordings down to ~0.15 Jaccard (observed), so
    # a higher floor lets duplicates exit free without the AI check.
    if best_sim < 0.12:
        return False

    # Stage 2: ambiguous middle ground — ask Claude, but only about the
    # top candidates (short prompt), not all 40 titles. Bullets go along
    # too: they carry the concrete numbers/places that rewritten titles
    # omit, which is what lets Claude recognise a reworded same event.
    def _facts(bullets_json):
        try:
            parts = json.loads(bullets_json or "[]")
            return "; ".join(str(p) for p in parts)[:300]
        except Exception:
            return ""

    candidates = [(t, b) for sim, t, b in scored[:6] if sim >= 0.12]
    if not candidates:
        return False
    lines = []
    for i, (t, b) in enumerate(candidates):
        facts = _facts(b)
        lines.append(f"{i+1}. {t}"
                     + (f"\n   Баримт: {facts}" if facts else ""))
    numbered = "\n".join(lines)
    new_facts = "; ".join(str(p) for p in (new_bullets or []))[:300]
    prompt = (
        "Доорх ШИНЭ мэдээ нь ӨМНӨХ мэдээнүүдийн аль нэгтэй ИЖИЛ үйл явдлыг "
        "дахин мэдээлж байна уу? Өөр найруулгатай, өөр тоо/газар онцолсон, "
        "эсвэл нэг хэрэг явдлын шинэчилсэн тоо баримт мэдээлсэн ч ИЖИЛ үйл "
        "явдалд тооцно. Харин цоо шинэ хөгжил, дараагийн тусдаа үйл явдал "
        "бол ижил биш.\n\n"
        f"ШИНЭ мэдээ: {new_title}\n"
        + (f"Баримт: {new_facts}\n" if new_facts else "")
        + f"\nӨМНӨХ мэдээнүүд:\n{numbered}\n\n"
        "ЗӨВХӨН JSON: {\"duplicate\": true/false}"
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=2000,   # thinking + {"duplicate":..}
            messages=[{"role": "user", "content": prompt}],
        )
        raw = "".join(b.text for b in msg.content if b.type == "text")
        d = _parse_json_lenient(raw)
        return bool(d.get("duplicate", False))
    except Exception as e:
        print(f"[dedup] check failed (allowing through): {e}")
        return False


PREFILTER_COVERED_DAYS = 3     # "already covered" list window
PREFILTER_COVERED_MAX = 30     # titles shown to the prefilter (~1.5k tokens)


def recent_covered_titles(con, days=PREFILTER_COVERED_DAYS,
                          limit=PREFILTER_COVERED_MAX):
    """Titles already queued or posted recently, newest first. Shown to the
    prefilter so same-event re-reports are dropped BEFORE the ~4¢ summary
    call (a Nov 18 test run spent 3 of 5 summaries on stories the final
    dedup then threw away)."""
    cutoff = (datetime.now(UB_TZ).date() - timedelta(days=days)).isoformat()
    return [r[0] for r in con.execute(
        "SELECT title FROM digests WHERE title IS NOT NULL AND "
        "(collected_date >= ? OR posted_at >= ?) "
        "ORDER BY run_at DESC LIMIT ?", (cutoff, cutoff, limit))]


def prefilter_titles(client, candidates, covered=()):
    """
    Cheap batch pre-filter: rate every candidate TITLE's audience pull
    (likely reach, shares, comments, follows) in ONE Claude call (titles
    only, no article text), so we skip the expensive per-article
    summarization for stories that rarely travel.

    candidates: list of (src, title, url) tuples.
    covered: titles already queued/posted recently; a candidate that only
             re-reports one of them (no new development) is scored ~0.
    Returns: list of (src, title, url, score) kept for full processing:
             the top hot titles plus a small filler quota.
    """
    if not candidates:
        return []

    def fallback(reason):
        """Return a deterministic source-balanced subset capped at eight."""
        batches_by_source = {}
        for candidate in candidates:
            src = candidate[0]
            source_key = (
                str(src.get("name", "")),
                str(src.get("rss") or src.get("listing") or ""),
            )
            batches_by_source.setdefault(source_key, []).append(candidate)

        batches = list(batches_by_source.values())
        kept = []
        round_index = 0
        while len(kept) < 8:
            added = False
            for batch in batches:
                if round_index >= len(batch):
                    continue
                kept.append((*batch[round_index], None))
                added = True
                if len(kept) >= 8:
                    break
            if not added:
                break
            round_index += 1

        print(f"[prefilter] FALLBACK — {reason}; kept {len(kept)} of "
              f"{len(candidates)} source-balanced candidates (cap 8)")
        return kept

    titles = [t for (_s, t, _u) in candidates]
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(titles))
    covered_block = ""
    if covered:
        covered_block = (
            "АЛЬ ХЭДИЙН НИЙТЭЛСЭН/ДАРААЛАЛД БАЙГАА мэдээ (сүүлийн өдрүүд):\n"
            + "\n".join(f"- {t}" for t in covered) + "\n\n"
            "Гарчиг нь эдгээрийн аль нэгтэй ИЖИЛ үйл явдлыг (өөр эх сурвалж, "
            "өөр найруулга, ижил тоо баримт) дахин мэдээлж байвал 0-5 оноо өг. "
            "Харин тухайн сэдвийн ШИНЭ хөгжил (шинэ шийдвэр, шинэ хариу үйлдэл, "
            "шинэ баримт) бол ердийнхөөрөө үнэл.\n\n"
        )
    prompt = (
        "Чи Монголын Facebook мэдээний хуудасны редактор. Зорилго: аль мэдээ "
        "хамгийн олон хүнд хүрч, үзэлт, хуваалцалт, сэтгэгдэл, шинэ дагагч "
        "авчрах вэ. Доорх гарчиг бүрд 0-100 оноо өг.\n\n"
        "ӨНДӨР (70-100): хүний хувь заяа (сураггүй алга болсон, амь насаа "
        "алдсан, осол, гал түймэр, хүүхэд/ахмадын аюулгүй байдал); нэртэй "
        "албан тушаалтан/улс төрчийг илчилсэн, буруутгасан, хурц маргаан, "
        "олны дургүйцэл, шинээр баривчилсан; хууль бус хил давуулалт, том "
        "залилан, хулгай; иргэдийн хармаанд шууд нөлөөлөх шийдвэр (тэтгэвэр, "
        "цалин, тэтгэмж, татвар, үнэ, тариф, түлш, зээл).\n"
        "ДУНД (40-65): өдөр тутамд хэрэгтэй шинэ журам/хуваарь (хэзээнээс юу "
        "өөрчлөгдөх), нийгмийн тулгамдсан асуудал, сонирхолтой хүний түүх, "
        "монгол тамирчдын том амжилт.\n"
        "БАГА (0-25): хурал, хэлэлцүүлэг, уулзалт, айлчлал, санамж бичиг, "
        "төлөвлөгөө, статистик тайлан, ёслол, шагнал; олны танил хүний шүүх "
        "хурал товлогдсон/хойшилсон/хорих хугацаа сунгасан гэх мэт явцын "
        "мэдээ; PR, зар; Монголтой хамаагүй гадаад мэдээ.\n\n"
        + covered_block +
        f"Үнэлэх гарчгууд:\n{numbered}\n\n"
        "ЗӨВХӨН JSON массив буцаа, гарчиг тус бүрийн оноогоор дарааллаар: "
        "[оноо1, оноо2, ...] (өөр юу ч бичихгүй)."
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=4000,   # thinking + score array
            messages=[{"role": "user", "content": prompt}],
        )
        raw = "".join(b.text for b in msg.content if b.type == "text")
        scores = _parse_json_lenient(raw)
        if not isinstance(scores, list) or len(scores) != len(candidates):
            received = len(scores) if isinstance(scores, list) else "non-list"
            return fallback(
                f"wrong score count/type (expected {len(candidates)}, "
                f"received {received})"
            )
    except Exception as e:
        return fallback(f"Claude/JSON failure: {type(e).__name__}: {e}")

    scored = []
    for (src, title, url), sc in zip(candidates, scores):
        try:
            sc = max(0, min(100, int(sc)))
        except Exception:
            sc = 50
        scored.append((src, title, url, sc))

    # Keep the TOP hot titles,
    # capped so the wide candidate net doesn't inflate summarization cost,
    # plus a small filler quota for quiet days.
    hot = sorted([x for x in scored if x[3] >= 30],
                 key=lambda x: x[3], reverse=True)[:6]
    # filler excludes 0-5: that band is "already covered" re-reports
    filler = sorted([x for x in scored if 5 < x[3] < 30],
                    key=lambda x: x[3], reverse=True)[:2]
    kept = hot + filler
    dropped = len(scored) - len(kept)
    print(f"[prefilter] {len(scored)} titles -> keep {len(kept)} "
          f"({len(hot)} hot + {len(filler)} filler), "
          f"{dropped} skipped before summarizing")
    return kept


def _clamp100(v, default=0):
    try:
        return max(0, min(100, int(v)))
    except (TypeError, ValueError):
        return default


def compute_interest(d, n_sources):
    """
    Audience-stakes interest score (0-100) + normalized topic label.

    Replaces the political/emotional/importance blend, which turned
    anti-predictive in Oct-Nov 2026 (higher score = fewer views). Built on
    what the page's audience actually responded to:
      0.45 felt_stakes + 0.35 conflict + 0.20 novelty
      + TOPIC_ADJ[topic] + headline bonus + small multi-source nudge.
    """
    felt = _clamp100(d.get("felt_stakes"), 40)
    conflict = _clamp100(d.get("conflict"), 20)
    novelty = _clamp100(d.get("novelty"), 50)
    topic = str(d.get("topic") or "other").strip().lower()
    if topic not in TOPIC_ADJ:
        topic = "other"
    bonus = HEADLINE_BONUS if HEADLINE_BONUS_RE.search(d.get("title") or "") else 0
    multi = 3 if n_sources > 1 else 0
    raw = round(0.45 * felt + 0.35 * conflict + 0.20 * novelty)
    return max(0, min(100, raw + TOPIC_ADJ[topic] + bonus + multi)), topic


def card_chars(d):
    """Characters printed on the card: title + bullets + why."""
    return (len(d.get("title") or "")
            + sum(len(str(b)) for b in (d.get("bullets") or []))
            + len(d.get("why") or ""))


def _card_ok(d):
    return (card_chars(d) <= CARD_MAX_CHARS
            and len(d.get("title") or "") <= TITLE_MAX_CHARS)


def enforce_card_length(client, d):
    """
    Keep cards short (CARD_MAX_CHARS) with a short headline (TITLE_MAX_CHARS).
    1) free: drop the third bullet if that alone fixes it;
    2) otherwise one small Claude call to tighten title/bullets/why;
    3) if that fails, keep the bullet-trimmed version (never blocks a story).
    """
    if _card_ok(d):
        return d
    bullets = list(d.get("bullets") or [])
    trimmed = dict(d, bullets=bullets[:2]) if len(bullets) > 2 else d
    if _card_ok(trimmed):
        print(f"[card] dropped 3rd bullet to fit: {d.get('title', '')[:40]}")
        return trimmed
    src = {"title": d.get("title", ""), "bullets": bullets, "why": d.get("why", "")}
    prompt = (
        "Доорх мэдээний картын текстийг БОГИНОСГО. Утга, баримт, тоог "
        "өөрчлөхгүй; шинэ мэдээлэл нэмэхгүй; монгол хэлний зөв бичгийн "
        "дүрмийг баримтал.\n"
        f"Хязгаар: title ≤ {TITLE_MAX_CHARS} тэмдэгт; bullets яг 3, тус бүр "
        f"≤ {BULLET_MAX_CHARS}; why ≤ {WHY_MAX_CHARS}; нийт ≤ {CARD_MAX_CHARS}.\n"
        "ЗӨВХӨН ижил бүтэцтэй JSON буцаа: "
        '{"title": "...", "bullets": ["...", "...", "..."], "why": "..."}\n\n'
        + json.dumps(src, ensure_ascii=False)
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=3000,   # thinking + short JSON
            messages=[{"role": "user", "content": prompt}],
        )
        raw = "".join(b.text for b in msg.content if b.type == "text")
        short = _parse_json_lenient(raw)
        if (isinstance(short, dict) and short.get("title")
                and isinstance(short.get("bullets"), list) and short["bullets"]
                and short.get("why")):
            out = dict(d, title=short["title"], bullets=short["bullets"][:3],
                       why=short["why"])
            if not _card_ok(out) and len(out["bullets"]) > 2:
                out["bullets"] = out["bullets"][:2]
            print(f"[card] tightened {card_chars(d)} -> {card_chars(out)} chars")
            return out
    except Exception as e:
        print(f"[card] shorten call failed, keeping trimmed: {e}")
    return trimmed


# ── Token/cost tracking ───────────────────────────────────────
PRICE_IN, PRICE_OUT = 4.0, 20.0   # USD per 1M tokens, claude-opus-5-5
_USAGE = {"calls": 0, "in": 0, "out": 0, "truncated": 0}


def _install_usage_tracker(client):
    """Wrap client.messages.create to tally tokens and flag truncation."""
    orig = client.messages.create

    def tracked(*a, **k):
        msg = orig(*a, **k)
        try:
            _USAGE["calls"] += 1
            _USAGE["in"] += msg.usage.input_tokens
            _USAGE["out"] += msg.usage.output_tokens
            if getattr(msg, "stop_reason", "") == "max_tokens":
                _USAGE["truncated"] += 1
                print("[usage] WARNING: a call hit max_tokens (truncated)")
        except Exception:
            pass
        return msg

    client.messages.create = tracked


def print_usage_report():
    cost = (_USAGE["in"] * PRICE_IN + _USAGE["out"] * PRICE_OUT) / 1e6
    print(f"[usage] {_USAGE['calls']} calls | in {_USAGE['in']:,} tok | "
          f"out {_USAGE['out']:,} tok (incl. thinking) | "
          f"truncated {_USAGE['truncated']} | est ${cost:.3f}")


def run_collector():
    now = datetime.now(UB_TZ)
    today = now.date().isoformat()
    print(f"\n===== Иш COLLECTOR run @ {now.isoformat()} =====")

    client = Anthropic()  # uses ANTHROPIC_API_KEY env var
    _install_usage_tracker(client)
    con = db_init()
    print(f"[config] cards={CARDS_AVAILABLE}")

    candidates = collect_candidates(con)
    if not candidates:
        print("[collector] nothing new, exiting")
        write_json(con)
        return

    # Mark ALL candidates seen BEFORE prefiltering — the prefilter decision
    # is final. Otherwise dropped titles come back as "new" every run,
    # get re-prefiltered (wasted tokens), and crowd out genuinely new
    # articles in the per-source quota.
    for _s, _t, u in candidates:
        mark_seen(con, u)

    # ── Phase 0: cheap audience pre-filter (titles only) ──────
    # One batch Claude call rates all titles for audience pull; we keep
    # the top ones plus a small filler quota. This is the biggest cost
    # saver since it avoids full summarization of low-value stories.
    candidates = prefilter_titles(client, candidates,
                                  covered=recent_covered_titles(con))
    if not candidates:
        print("[collector] nothing kept after prefilter")
        write_json(con)
        return

    # ── Phase 1: fetch text + free ad filter ──────────────────
    articles = []
    skipped_ads = 0
    for src, title, url, _pol in candidates:
        if looks_like_ad(title, url):
            skipped_ads += 1
            print(f"[skip] ad (free filter): {title[:50]}")
            continue
        try:
            text, image_url = fetch_article_text(url, src["article_selector"],
                                                 use_proxy=src.get("use_proxy", False))
        except Exception as e:
            print(f"[fail] {url}: {e}")
            record_fetch_failure(con, url, now)
            continue
        if not text or len(text) < MIN_ARTICLE_CHARS:
            print(f"[skip] too short: {title[:50]}")
            record_fetch_failure(con, url, now)
            continue
        clear_fetch_retry(con, url)
        articles.append({"src": src["name"], "title": title,
                         "url": url, "text": text,
                         "image_url": image_url})

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

            # short card + headline first, so dedup compares the title that
            # actually gets stored (re-asks Claude once only if still long)
            d = enforce_card_length(client, d)

            # cross-run dedup: skip if same event already queued/recently posted
            if is_duplicate_of_recent(client, con, d["title"], d.get("bullets", [])):
                print(f"[skip] duplicate of recent: {d['title'][:50]}")
                continue

            # Foreign news only when it directly involves Mongolia.
            if not bool(d.get("mongolia_related", True)):
                print(f"[skip] foreign, not Mongolia-related: {d['title'][:45]}")
                continue

            primary = cluster[0]
            sources = sorted({a["src"] for a in cluster})
            all_urls = [a["url"] for a in cluster]
            total_words = sum(len(a["text"].split()) for a in cluster)
            orig_min = max(1, round(total_words / 180))

            chars = card_chars(d)

            # audience-stakes interest score (see compute_interest)
            interest, topic = compute_interest(d, len(sources))

            # No card render here: the poster regenerates the card on its
            # own machine at post time (collector-rendered files don't
            # survive across runs), so rendering at collect time was pure
            # wasted work with a permanently stale path.
            card_path = None

            # article-specific hashtags: sanitize the AI's list now (at
            # collect time, where the category is known for fallback padding)
            # and store bare tokens; the poster prepends the fixed tags.
            article_tags = _clean_hashtags(d.get("hashtags"),
                                           d.get("category", "Нийгэм"))

            # queue it: posted=0, tagged with today's date
            con.execute(
                # OR IGNORE (not REPLACE): if this URL already has a row,
                # leave it untouched — REPLACE would reset posted=0 and
                # cause a re-post if the URL ever slipped past `seen`.
                "INSERT OR IGNORE INTO digests "
                "(url, source, category, title, bullets, why, orig_min, "
                "published, run_at, sources, source_count, all_urls, "
                "posted, collected_date, card_path, interest_score, full_text, "
                "image_url, hashtags, topic, card_chars) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (primary["url"], primary["src"], d.get("category", "Нийгэм"),
                 d["title"], json.dumps(d["bullets"], ensure_ascii=False),
                 d["why"], orig_min, now.isoformat(), now.isoformat(),
                 json.dumps(sources, ensure_ascii=False), len(sources),
                 json.dumps(all_urls, ensure_ascii=False),
                 0, today, card_path, interest, d.get("full_text", ""),
                 primary.get("image_url"),
                 json.dumps(article_tags, ensure_ascii=False),
                 topic, chars),
            )
            con.commit()
            queued += 1
            tag = f" [{len(cluster)} sources]" if len(cluster) > 1 else ""
            print(f"[queued]{tag} (score {interest}, {topic}, "
                  f"{chars} chars) {d['title'][:50]}")
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
    print_usage_report()
    print(f"[collector] {queued} queued ({skipped_ads} ads skipped, "
          f"{merged} multi-source); {dropped} stale dropped; "
          f"{pending} total pending in queue")
    write_json(con)


# ──────────────────────────────────────────────────────────────
# POSTER MODE — pick ONE queued story and post it
# ──────────────────────────────────────────────────────────────

def pick_story_to_post(con, now, exclude=()):
    """
    Pick the best FRESH pending story.
      • Only stories collected within MAX_POST_AGE_DAYS are eligible
        (stale stories hit 0-8% vs 13-16% fresh).
      • Slots before FIRST_COLLECTION_HOUR prefer the prior day's batch
        (today's isn't collected yet); later slots prefer today's and fall
        back to recent leftovers only when today's queue is empty.
      • Within the pool: highest interest score, then multi-source, then
        newest. (The old economy tie-break is gone: economy hit 6%.)
    `exclude` = URLs already rejected by the poster's guards this run.
    Returns (row_dict or None, mode_str).
    """
    today = now.date().isoformat()
    oldest = (now.date() - timedelta(days=MAX_POST_AGE_DAYS)).isoformat()
    excl = list(exclude)
    excl_sql = (" AND url NOT IN (%s)" % ",".join("?" * len(excl))) if excl else ""

    def fetch(where, params):
        return con.execute(
            "SELECT url, source, category, title, bullets, why, sources, "
            "source_count, card_path, collected_date, full_text, interest_score, "
            "image_url, hashtags, topic "
            "FROM digests "
            "WHERE posted=0 AND COALESCE(review_needed, 0)=0 "
            "AND collected_date >= ? AND " + where + excl_sql +
            " ORDER BY interest_score DESC, source_count DESC, "
            "          collected_date DESC LIMIT 1",
            (oldest, *params, *excl),
        ).fetchone()

    if now.hour < FIRST_COLLECTION_HOUR:
        row = fetch("collected_date < ?", (today,)) or fetch("collected_date = ?", (today,))
        mode = "pre-collection-morning"
    else:
        row = fetch("collected_date = ?", (today,))
        if row:
            mode = "today"
        else:
            row = fetch("collected_date < ?", (today,))
            mode = "fallback-leftover"

    if not row:
        return None, mode
    keys = ["url", "source", "category", "title", "bullets", "why",
            "sources", "source_count", "card_path", "collected_date",
            "full_text", "interest_score", "image_url", "hashtags", "topic"]
    return dict(zip(keys, row)), mode


# ── Story-thread detection (fatigue cap) ──────────────────────
_PERSON_RE = re.compile(r"\b[А-ЯӨҮЁ]\.\s?([А-ЯӨҮЁ][а-яөүё]{2,})")
_WORD_RE = re.compile(r"[а-яөүёa-z]{6,}")


def _thread_signals(text):
    """(named people, 6-letter word stems) — the same signals the Aug-Nov
    analysis used to tag follow-up Reels."""
    t = text or ""
    people = {m.group(1).lower() for m in _PERSON_RE.finditer(t)}
    stems = {w[:6] for w in _WORD_RE.findall(t.lower())}
    return people, stems


def same_thread(text_a, text_b):
    """Two stories belong to one running thread if they share a named
    person plus a stem, or two 6-letter stems. Backtested on 523 Reels
    (Aug-Nov 2026): 1st/2nd posts in a thread hit 13-15%, 3rd+ hit 4-5%.
    (A frequency-based stoplist was tried and made separation worse — it
    removed exactly the over-covered threads' words, e.g. fuel.)"""
    pa, sa = _thread_signals(text_a)
    pb, sb = _thread_signals(text_b)
    shared_stems = sa & sb
    if pa & pb and shared_stems:
        return True
    return len(shared_stems) >= 2


def thread_post_count(con, story, now):
    """How many posts in the last THREAD_WINDOW_DAYS share this thread."""
    cutoff = (now - timedelta(days=THREAD_WINDOW_DAYS)).isoformat()
    txt = story.get("title") or ""
    n = 0
    for (t,) in con.execute(
            "SELECT title FROM digests WHERE posted=1 AND url != ? "
            "AND posted_at >= ?", (story["url"], cutoff)):
        if same_thread(txt, t or ""):
            n += 1
    return n


def is_safe_public_url(url):
    """Return True only for HTTP(S) URLs resolving entirely to public IPs."""
    try:
        parsed = urlparse(url)
        if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
            return False
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
        addresses = socket.getaddrinfo(
            parsed.hostname, port, type=socket.SOCK_STREAM
        )
        if not addresses:
            return False
        for address in addresses:
            raw_ip = address[4][0].split("%", 1)[0]
            if not ipaddress.ip_address(raw_ip).is_global:
                return False
        return True
    except (OSError, TypeError, ValueError):
        return False


def _reject_article_image(url, reason):
    print(f"[poster] ARTICLE IMAGE REJECTED {url}: {reason}")
    return None


def download_article_image(image_url, article_url, out_dir="imgs"):
    """
    Download the article's share image for the card header.
    Validates it's a real, reasonably-sized image (rejects tiny logos,
    favicons, broken files). Returns a local path or None.
    """
    del article_url  # kept in the public signature for existing callers
    current_url = image_url
    response = None
    try:
        redirects = 0
        while True:
            # This validates the initial URL and, on the final iteration, the
            # exact post-redirect URL immediately before it is fetched.
            if not is_safe_public_url(current_url):
                return _reject_article_image(
                    current_url, "unsafe, non-public, or unresolvable URL"
                )
            response = requests.get(
                current_url,
                headers=HEADERS,
                timeout=25,
                allow_redirects=False,
                stream=True,
            )
            if response.status_code in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                if not location:
                    return _reject_article_image(
                        current_url, "redirect response has no Location"
                    )
                if redirects >= MAX_IMAGE_REDIRECTS:
                    return _reject_article_image(
                        current_url,
                        f"more than {MAX_IMAGE_REDIRECTS} redirects",
                    )
                next_url = urljoin(current_url, location)
                # Validate before following the hop; requests never sees an
                # unsafe redirect target.
                if not is_safe_public_url(next_url):
                    return _reject_article_image(
                        next_url, "redirect targets a non-public URL"
                    )
                response.close()
                response = None
                current_url = next_url
                redirects += 1
                continue
            break

        if response.status_code != 200:
            return _reject_article_image(
                current_url, f"HTTP status {response.status_code}"
            )

        content_type = response.headers.get("Content-Type", "")
        content_type = content_type.split(";", 1)[0].strip().lower()
        if not content_type.startswith("image/"):
            return _reject_article_image(
                current_url, f"non-image Content-Type {content_type or 'missing'}"
            )

        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                if int(content_length) > MAX_IMAGE_BYTES:
                    return _reject_article_image(
                        current_url,
                        f"Content-Length exceeds {MAX_IMAGE_BYTES} bytes",
                    )
            except ValueError:
                pass

        body = io.BytesIO()
        byte_count = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            byte_count += len(chunk)
            if byte_count > MAX_IMAGE_BYTES:
                return _reject_article_image(
                    current_url,
                    f"stream exceeded {MAX_IMAGE_BYTES} bytes",
                )
            body.write(chunk)
        if byte_count < 8000:
            return _reject_article_image(
                current_url, f"image is only {byte_count} bytes (minimum 8000)"
            )

        from PIL import Image as _Img
        _Img.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS
        body.seek(0)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", _Img.DecompressionBombWarning)
                with _Img.open(body) as im:
                    width, height = im.size
                    if width * height > MAX_IMAGE_PIXELS:
                        return _reject_article_image(
                            current_url,
                            f"{width}x{height} exceeds {MAX_IMAGE_PIXELS} pixels",
                        )
                    if width < 400 or height < 250:
                        return _reject_article_image(
                            current_url,
                            f"{width}x{height} is below minimum 400x250",
                        )
                    im.verify()
        except (_Img.DecompressionBombError,
                _Img.DecompressionBombWarning) as e:
            return _reject_article_image(
                current_url, f"decompression bomb rejected: {e}"
            )

        os.makedirs(out_dir, exist_ok=True)
        h = hashlib.md5(image_url.encode()).hexdigest()[:10]
        path = os.path.join(out_dir, f"img_{h}.jpg")
        with open(path, "wb") as f:
            f.write(body.getvalue())
        return path
    except Exception as e:
        return _reject_article_image(image_url, f"download error: {e}")
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass


POSTED_LOG = "posted_log.txt"   # append-only url<TAB>title<TAB>date; a text
                                # backstop for posted-state that survives the
                                # binary-DB merge losses better than towch.db
                                # (committed by digest.yml's Save step)


def log_posted(url, title):
    """Append a successful post to the durable text log."""
    try:
        with open(POSTED_LOG, "a", encoding="utf-8") as f:
            f.write(f"{url}\t{title}\t{datetime.now(UB_TZ).date().isoformat()}\n")
    except Exception as e:
        print(f"[poster] posted-log append failed: {e}")


def load_posted_log(days=7):
    """Return (set_of_urls, list_of_titles) posted within the last N days."""
    urls, titles = set(), []
    cutoff = (datetime.now(UB_TZ).date() - timedelta(days=days)).isoformat()
    try:
        with open(POSTED_LOG, encoding="utf-8") as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 3 and parts[2] >= cutoff:
                    urls.add(parts[0])
                    titles.append(parts[1])
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[poster] posted-log read failed: {e}")
    return urls, titles


def sync_posted_flags_from_log(con):
    """
    Re-apply posted=1 for any URL in the log whose DB row lost its flag
    (binary towch.db merges can erase one run's changes; the text log is
    the durable record).
    """
    urls, _ = load_posted_log(days=14)
    if not urls:
        return
    fixed = 0
    for u in urls:
        cur = con.execute(
            "UPDATE digests SET posted=1 WHERE url=? AND posted=0", (u,))
        fixed += cur.rowcount
    if fixed:
        con.commit()
        print(f"[poster] restored posted=1 on {fixed} row(s) from posted_log")


def looks_already_posted(title, posted_titles, threshold=0.5):
    """True if `title` closely matches something we already posted."""
    if title in posted_titles:
        return True
    return any(_title_similarity(title, t) >= threshold for t in posted_titles)


def matches_recent_post_numbers(con, story, days=7):
    """True if the story shares distinctive figures with a recent post."""
    cutoff = (datetime.now(UB_TZ) - timedelta(days=days)).isoformat()
    txt = _story_text(story.get("title"), story.get("bullets"))
    for t, b in con.execute(
            "SELECT title, bullets FROM digests WHERE posted=1 "
            "AND url != ? AND (posted_at IS NULL OR posted_at >= ?)",
            (story["url"], cutoff)).fetchall():
        if same_event_by_numbers(txt, _story_text(t, b)):
            return True
    return False


def run_poster():
    now = datetime.now(UB_TZ)
    print(f"\n===== Иш POSTER run @ {now.isoformat()} =====")
    con = db_init()

    token = os.environ.get("FB_PAGE_TOKEN")
    page_id = os.environ.get("FB_PAGE_ID")
    if not token or not page_id:
        print("[poster] no FB credentials — abort")
        return

    # Restore any posted=1 flags the binary-DB merge may have erased, then
    # pick — skipping (a) anything matching a recent post (title or figures;
    # marked posted=2 = "skipped as already covered") and (b) a 3rd+ post on
    # a running story thread unless it scores as a breakout (left pending).
    sync_posted_flags_from_log(con)
    _, posted_titles = load_posted_log(days=7)

    story, mode, rejected = None, None, []
    for _attempt in range(10):
        story, mode = pick_story_to_post(con, now, exclude=rejected)
        if not story:
            break
        if looks_already_posted(story["title"], posted_titles) or \
                matches_recent_post_numbers(con, story):
            print(f"[poster] guard: already covered, skipping: {story['title'][:50]}")
            con.execute("UPDATE digests SET posted=2 WHERE url=? AND posted=0",
                        (story["url"],))
            con.commit()
            rejected.append(story["url"])
            story = None
            continue
        n_thread = thread_post_count(con, story, now)
        if (n_thread >= THREAD_MAX_POSTS
                and (story.get("interest_score") or 0) < THREAD_BREAKOUT_SCORE):
            print(f"[poster] thread cap: {n_thread} posts on this story in "
                  f"{THREAD_WINDOW_DAYS}d, skipping: {story['title'][:50]}")
            rejected.append(story["url"])
            story = None
            continue
        break
    if not story:
        pending = con.execute("SELECT COUNT(*) FROM digests WHERE posted=0").fetchone()[0]
        print(f"[poster] nothing eligible for mode '{mode}' — nothing to post "
              f"({pending} pending overall, {len(rejected)} skipped by guards)")
        return

    print(f"[poster] mode={mode}  picking: {story['title'][:55]}")
    item = {
        "title": story["title"],
        "bullets": json.loads(story["bullets"]) if story["bullets"] else [],
        "why": story["why"] or "",
        "url": story["url"],
        "sources": clean_source_names(
            json.loads(story["sources"]) if story["sources"] else [story["source"]]),
        "source_count": story["source_count"] or 1,
        "full_text": story.get("full_text") or "",
        "category": story.get("category") or "",
        "interest_score": story.get("interest_score") or 0,
        "topic": story.get("topic") or "",
        "hashtags": json.loads(story["hashtags"]) if story.get("hashtags") else [],
    }

    # Regenerate the card now — the collector ran on a different machine,
    # so its card file no longer exists. Redraw from queued data (free/fast).
    # If the article had a share image, download and validate it; the card
    # uses it as a photo header (falls back to text-only card on any issue).
    photo_path = None
    if story.get("image_url"):
        photo_path = download_article_image(story["image_url"], story["url"])

    card_path = None
    if CARDS_AVAILABLE:
        try:
            h = hashlib.md5(story["url"].encode()).hexdigest()[:8]
            card_path = make_card(
                {"title": item["title"], "bullets": item["bullets"],
                 "why": item["why"], "category": story["category"],
                 "sources": item["sources"]},
                out_dir="cards", filename=f"post_{h}.png",
                photo_path=photo_path,
            )
        except Exception as ce:
            print(f"[poster] card render failed: {ce}")
    else:
        card_path = story["card_path"]  # fallback to stored path

    feed_result = post_one_to_facebook(item, card_path, token, page_id)
    feed_status = feed_result.get("status")
    if feed_status == "success":
        fb_post_id = feed_result["fb_post_id"]
        # Commit confirmed feed state before any Reel work. If rendering or
        # uploading the Reel fails, the feed must never return to the queue.
        con.execute(
            "UPDATE digests "
            "SET posted=1, posted_at=?, fb_post_id=?, reel_posted=0, "
            "review_needed=0 WHERE url=?",
            (now.isoformat(), fb_post_id, story["url"]),
        )
        con.commit()
        log_posted(story["url"], story["title"])
        pending = con.execute("SELECT COUNT(*) FROM digests WHERE posted=0").fetchone()[0]
        print(f"[poster] feed posted ✓ id={fb_post_id} "
              f"({pending} still pending)")

        # Every posted story gets a Reel: at 6 posts/day (+6 reels = 12
        # actions/day) we're far under the spam threshold, so no score gate.
        # This is a single attempt: failed Reels are logged but never retried.
        if POST_REELS and REELS_AVAILABLE and card_path:
            reel_ok = False
            try:
                h = hashlib.md5(story["url"].encode()).hexdigest()[:8]
                reel_path = make_reel(card_path, out_dir="reels",
                                      filename=f"reel_{h}.mp4")
                if reel_path:
                    reel_ok = post_reel_to_facebook(
                        item, reel_path, token, page_id
                    )
                else:
                    print("[poster] REEL FAILED: render returned nothing; "
                          "no automatic retry")
            except Exception as re:
                print(f"[poster] REEL FAILED: {re}; no automatic retry")
            if reel_ok:
                con.execute(
                    "UPDATE digests SET reel_posted=1, reel_video_id=? WHERE url=?",
                    (str(reel_ok) if reel_ok is not True else None, story["url"]),
                )
                con.commit()
                print("[poster] reel state saved ✓")
            else:
                print("[poster] REEL NOT POSTED; feed remains posted and "
                      "will not be retried")
    elif feed_status == "ambiguous_failure":
        con.execute(
            "UPDATE digests SET review_needed=1, posted=0 WHERE url=?",
            (story["url"],),
        )
        con.commit()
        print("[poster] REVIEW REQUIRED: feed outcome ambiguous; story "
              "quarantined from automatic reposting")
    else:
        con.execute(
            "UPDATE digests SET review_needed=0, posted=0 WHERE url=?",
            (story["url"],),
        )
        con.commit()
        print("[poster] clean feed failure — left in queue for next run")


# ──────────────────────────────────────────────────────────────
# ENTRYPOINT — mode dispatch via command-line argument
# ──────────────────────────────────────────────────────────────

def run_weather():
    """Post the morning weather card to Facebook."""
    now = datetime.now(UB_TZ)
    print(f"\n===== Иш WEATHER run @ {now.isoformat()} =====")
    token = os.environ.get("FB_PAGE_TOKEN")
    page_id = os.environ.get("FB_PAGE_ID")
    if not token or not page_id:
        print("[weather] no FB credentials — abort")
        return
    try:
        from weather import make_weather_post
    except Exception as e:
        print(f"[weather] module unavailable: {e}")
        return
    client = Anthropic()
    card_path, caption = make_weather_post(client, out_dir="cards")
    if not card_path:
        print("[weather] could not build weather card — skipping")
        return
    # post the card image with its deterministic forecast caption
    ok = _post_card_with_caption(card_path, caption, token, page_id)
    print("[weather] posted ✓" if ok else "[weather] post failed")


def _post_card_with_caption(card_path, caption, token, page_id):
    """Post a prebuilt card + caption as a feed post. Returns True on success."""
    if not card_path or not os.path.exists(card_path):
        return False
    try:
        with open(card_path, "rb") as img:
            up = requests.post(
                f"{FB_API}/{page_id}/photos",
                data={"published": "false", "access_token": token},
                files={"source": img}, timeout=60,
            )
        photo_id = up.json().get("id")
        if not photo_id:
            print(f"[weather] upload failed: {up.text[:150]}")
            return False
        r = requests.post(
            f"{FB_API}/{page_id}/feed",
            data={"message": caption,
                  "attached_media[0]": json.dumps({"media_fbid": photo_id}),
                  "access_token": token}, timeout=60,
        )
        return r.status_code == 200 and bool(r.json().get("id"))
    except Exception as e:
        print(f"[weather] error: {e}")
        return False


def run_currency():
    """Post the morning Mongolbank rates card to Facebook."""
    now = datetime.now(UB_TZ)
    print(f"\n===== Иш CURRENCY run @ {now.isoformat()} =====")
    token = os.environ.get("FB_PAGE_TOKEN")
    page_id = os.environ.get("FB_PAGE_ID")
    if not token or not page_id:
        print("[currency] no FB credentials — abort")
        return
    try:
        from currency import make_currency_post
    except Exception as e:
        print(f"[currency] module unavailable: {e}")
        return
    card_path, caption = make_currency_post(out_dir="cards")
    if not caption:
        print("[currency] could not build rates post — skipping")
        return
    ok = _post_card_with_caption(card_path, caption, token, page_id)
    print("[currency] posted ✓" if ok else "[currency] post failed")


# ──────────────────────────────────────────────────────────────
# INSIGHTS MODE — weekly: read Reel views back into towch.db
# ──────────────────────────────────────────────────────────────
# Closes the loop: the scoring weights (TOPIC_ADJ etc.) were set from
# hand-exported Facebook reports; this pulls the same numbers automatically
# so they can be re-checked monthly. Needs a Page token with read_insights
# (+ pages_manage_engagement). Read-only on Facebook; writes only the
# views / avg_watch_s / insights_at columns.

INSIGHTS_LOOKBACK_DAYS = 14   # Reels keep gaining views for ~2 weeks
INSIGHTS_REPORT_DAYS = 28
HIT_VIEWS = 10_000            # "hit" = a Reel over 10k plays


def _page_reels_by_title(token, page_id, max_pages=6):
    """Map caption first line (= story title) -> reel video id, for posts
    made before reel_video_id was stored."""
    out, url = {}, f"{FB_API}/{page_id}/video_reels"
    params = {"fields": "id,description", "limit": 50, "access_token": token}
    for _ in range(max_pages):
        try:
            body = requests.get(url, params=params, timeout=30).json()
        except Exception as e:
            print(f"[insights] reel list failed: {e}")
            break
        if "error" in body:
            print(f"[insights] reel list error: {body['error'].get('message')}")
            break
        for v in body.get("data", []):
            first = (v.get("description") or "").split("\n", 1)[0].strip()
            if first and v.get("id"):
                out.setdefault(first, v["id"])
        url = (body.get("paging") or {}).get("next")
        params = None   # the `next` URL already carries the query
        if not url:
            break
    return out


def fetch_reel_insights(video_id, token):
    """Return {"views": int|None, "avg_watch_s": float|None} for one Reel."""
    try:
        body = requests.get(
            f"{FB_API}/{video_id}/video_insights",
            params={"metric": "blue_reels_play_count,post_video_avg_time_watched",
                    "access_token": token},
            timeout=30,
        ).json()
    except Exception as e:
        print(f"[insights] {video_id}: request failed: {e}")
        return None
    if "error" in body:
        print(f"[insights] {video_id}: {body['error'].get('message')}")
        return None
    vals = {}
    for m in body.get("data", []):
        v = (m.get("values") or [{}])[0].get("value")
        vals[m.get("name")] = v
    plays = vals.get("blue_reels_play_count")
    avg_ms = vals.get("post_video_avg_time_watched")
    return {
        "views": int(plays) if isinstance(plays, (int, float)) else None,
        "avg_watch_s": round(avg_ms / 1000, 2) if isinstance(avg_ms, (int, float)) else None,
    }


def _median(xs):
    xs = sorted(xs)
    if not xs:
        return 0
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


def print_insights_report(con, now):
    cutoff = (now - timedelta(days=INSIGHTS_REPORT_DAYS)).isoformat()
    rows = con.execute(
        "SELECT topic, interest_score, views, card_chars FROM digests "
        "WHERE posted=1 AND views IS NOT NULL AND posted_at >= ?",
        (cutoff,)).fetchall()
    if not rows:
        print("[insights] no Reels with views yet — nothing to report")
        return

    def line(label, vs):
        hits = sum(1 for v in vs if v > HIT_VIEWS)
        print(f"  {label:<16} n={len(vs):>3}  median={_median(vs):>7.0f}  "
              f"hit%={100 * hits / len(vs):5.1f}")

    print(f"[insights] last {INSIGHTS_REPORT_DAYS} days, Reels with views:")
    line("ALL", [r[2] for r in rows])
    print(" by topic:")
    for t in TOPICS + [None]:
        vs = [r[2] for r in rows if (r[0] or None) == t]
        if vs:
            line(t or "(unlabelled)", vs)
    print(" by score band:")
    for lo, hi in ((0, 40), (40, 55), (55, 70), (70, 101)):
        vs = [r[2] for r in rows if r[1] is not None and lo <= r[1] < hi]
        if vs:
            line(f"{lo}-{min(hi, 100)}", vs)
    print(" by card length:")
    for lo, hi in ((0, 600), (600, 700), (700, 10_000)):
        vs = [r[2] for r in rows if r[3] is not None and lo <= r[3] < hi]
        if vs:
            line(f"{lo}-{hi if hi < 10_000 else '+'} chars", vs)


def run_insights():
    now = datetime.now(UB_TZ)
    print(f"\n===== Иш INSIGHTS run @ {now.isoformat()} =====")
    token = os.environ.get("FB_PAGE_TOKEN")
    page_id = os.environ.get("FB_PAGE_ID")
    if not token or not page_id:
        print("[insights] no FB credentials — abort")
        return
    con = db_init()
    cutoff = (now - timedelta(days=INSIGHTS_LOOKBACK_DAYS)).isoformat()
    rows = con.execute(
        "SELECT url, title, reel_video_id FROM digests "
        "WHERE posted=1 AND posted_at >= ?", (cutoff,)).fetchall()
    by_title = None
    updated = 0
    for url, title, vid in rows:
        if not vid:
            if by_title is None:
                by_title = _page_reels_by_title(token, page_id)
            vid = by_title.get((title or "").strip())
            if vid:
                con.execute("UPDATE digests SET reel_video_id=? WHERE url=?",
                            (vid, url))
        if not vid:
            continue
        ins = fetch_reel_insights(vid, token)
        if ins and ins["views"] is not None:
            con.execute(
                "UPDATE digests SET views=?, avg_watch_s=?, insights_at=? "
                "WHERE url=?",
                (ins["views"], ins["avg_watch_s"], now.isoformat(), url))
            updated += 1
        time.sleep(0.3)
    con.commit()
    print(f"[insights] updated views on {updated}/{len(rows)} posts "
          f"from the last {INSIGHTS_LOOKBACK_DAYS} days")
    print_insights_report(con, now)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "collect"
    if mode == "collect":
        run_collector()
    elif mode == "post":
        run_poster()
    elif mode == "weather":
        run_weather()
    elif mode == "currency":
        run_currency()
    elif mode == "insights":
        run_insights()
    else:
        print(f"Unknown mode '{mode}'. Use 'collect', 'post', 'weather', "
              f"'currency', or 'insights'.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
