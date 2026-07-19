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
MIN_ARTICLE_CHARS = 400          # skip stubs/photo posts
MAX_FETCH_ATTEMPTS = 3           # bounded cross-run retries for fetch failures
FETCH_RETRY_MAX_AGE_DAYS = 2     # never churn a dead URL beyond this age
MODEL = "claude-sonnet-4-6" # cheap + good enough for summaries
DB_PATH = "towch.db"
OUTPUT_JSON = "digest.json"      # the website reads this file
REQUEST_TIMEOUT = 15

# ── Queue-system settings ─────────────────────────────────────
MORNING_FRESH_HOUR = 9   # (legacy) kept for reference
FIRST_COLLECTION_HOUR = 11  # first daily collection runs at 11:00 UB, so
                            # posts before this must use the prior day's news
MAX_QUEUE_AGE_DAYS = 5   # drop unposted stories older than this (covers
                         # a Friday story staying usable through Sunday)
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

# (Category-boost table removed: superseded by the politics-focused
#  scoring — 60% political relevance — plus the economy filler boost.)

PROMPT = """Чи Монголын мэдээг энгийн ойлгомжтой болгодог редактор.
Доорх нийтлэлийг уншаад ЗӨВХӨН дараах JSON-оор хариул (өөр юу ч бичихгүй, markdown хэрэглэхгүй):

{{"title": "товч тодорхой гарчиг (clickbait биш)",
 "category": "{cats} — аль нэгийг сонго",
 "bullets": ["хамгийн чухал 3 баримтыг 3 товч өгүүлбэрээр", "...", "..."],
 "why": "энгийн иргэнд яагаад хамаатай болохыг 1 өгүүлбэрээр",
 "full_text": "Facebook пост дээр тавих ДЭЛГЭРЭНГҮЙ текст: 2-3 богино догол мөр (нийт 8-10 өгүүлбэр). Эхний догол мөрт юу болсныг гол баримтуудтай нь; хоёр дахьд ар дэвсгэр, нөхцөл байдал, оролцогч талуудын байр суурь/хариу үйлдэл; сүүлд нь энэ юунд хүргэх, дараа нь юу болох. Догол мөрүүдийг хоосон мөрөөр тусгаарла. Зөвхөн нийтлэлд байгаа баримтаар — нэмэлт таамаггүй.",
 "newsworthy": true/false,
 "importance": 0-100,
 "emotional": 0-100,
 "political": 0-100,
 "mongolia_related": true/false,
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

"political" (0-100): энэ мэдээ МОНГОЛЫН УЛС ТӨРД хэр холбоотой вэ? Өндөр оноо:
  УИХ, Засгийн газар, Ерөнхийлөгч, сайд/албан тушаалтан, намууд, сонгууль,
  хууль/бодлого, авлига, томилгоо, улс төрийн дуулиан, жагсаал/эсэргүүцэл,
  улс төртэй холбоотой нийгмийн асуудал, Монголын гадаад харилцаа/дипломат.
  Бага оноо: спорт, зугаа цэнгээл, технологийн бүтээгдэхүүн, цэвэр бизнес,
  алдартны мэдээ — улс төртэй огт хамаагүй бол 0-10.
"mongolia_related" (true/false): энэ мэдээ Монгол Улстай ШУУД холбоотой юу?
  Гадаадын мэдээ бол зөвхөн Монголыг шууд хамарсан үед true (жишээ:
  Монгол-хятадын хэлэлцээр). Монголтой хамаагүй цэвэр гадаад мэдээ = false.

"importance" (0-100): энэ мэдээ хүмүүсийн амьдрал, мөнгө, ажил, аюулгүй
  байдалд хэр их нөлөөлөх вэ? Бодлого, хууль, эдийн засаг, томоохон
  үйл явдал = өндөр оноо.
"emotional" (0-100): нийгмийн сүлжээнд хэр их анхаарал татах вэ —
  хүмүүс хуваалцаж, сэтгэгдэл бичиж, маргалдах уу? Өндөр: дуулиан, зөрчил,
  авлига, гэмт хэрэг, осол, огцруулалт, иргэдийн мөнгөнд нөлөөлөх шийдвэр,
  гэнэтийн эргэлт, хүний хувь заяаны драм. Бага: ёслол, шагнал, форум,
  албан ёсны хуурай мэдээ.
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
                      ("review_needed", "INTEGER DEFAULT 0")]:  # ambiguous feed outcome
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
        if len(fresh) >= MAX_PER_SOURCE:
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
        if len(fresh) >= MAX_PER_SOURCE:
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
        max_tokens=2200,   # headroom for the 2-3 paragraph full_text
                           # (truncation kills the whole story's JSON)
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
 "full_text": "Facebook пост дээр тавих ДЭЛГЭРЭНГҮЙ текст: бүх эх сурвалжийг нэгтгэн 2-3 богино догол мөр (нийт 8-10 өгүүлбэр) — юу болсон, ар дэвсгэр нөхцөл, талуудын байр суурь, үр дагавар/дараагийн алхам. Догол мөрүүдийг хоосон мөрөөр тусгаарла. Зөвхөн нийтлэлүүдэд байгаа баримтаар — таамаггүй.",
 "newsworthy": true/false,
 "importance": 0-100,
 "emotional": 0-100,
 "political": 0-100,
 "mongolia_related": true/false,
 "block": true/false}}

"political" (0-100): Монголын улс төрд хэр холбоотой вэ? УИХ, Засгийн газар,
  Ерөнхийлөгч, сайд, намууд, сонгууль, хууль/бодлого, авлига, томилгоо,
  улс төрийн дуулиан, жагсаал, Монголын гадаад харилцаа = өндөр. Спорт,
  зугаа цэнгээл, цэвэр бизнес = 0-10.
"mongolia_related" (true/false): Монгол Улстай шууд холбоотой юу? Гадаад
  мэдээ бол зөвхөн Монголыг шууд хамарсан үед true, эс бол false.
"importance" (0-100): хүмүүсийн амьдрал, мөнгө, ажил, аюулгүй байдалд
  хэр нөлөөлөх вэ (бодлого, хууль, эдийн засаг = өндөр).
"emotional" (0-100): нийгмийн сүлжээнд хэр их анхаарал татах вэ
  (дуулиан, зөрчил, авлига, осол, иргэдийн мөнгөнд нөлөөлөх = өндөр;
  ёслол, форум, албан хуурай мэдээ = бага).
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
        max_tokens=2200,   # headroom for the 2-3 paragraph full_text
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
    """
    Compose the Facebook post text (caption above the card image).
    Uses the elaborated `full_text` for depth when available, then the
    key bullets, the 'why it matters' line, sources, and hashtags.
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

    lines.append("")
    srcs = ", ".join(item.get("sources", [item.get("source", "")]))
    lines.append(f"📰 Эх сурвалж: {srcs}")
    lines.append(f"🔗 {item['url']}")
    lines.append("")
    lines.append("#Иш #мэдээ #улстөр")
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


def is_duplicate_of_recent(client, con, new_title, new_bullets, days=3):
    """
    Two-stage dedup to minimise AI cost:
      1. FREE word-overlap pre-filter finds plausible candidates. If the
         best overlap is very low, it's obviously not a dup — skip the AI
         call entirely (this is the common case, so most stories cost $0).
      2. Only when there ARE similar-looking candidates do we ask Claude,
         and we send just the top few (not all 40) to keep the prompt short.
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

    # exact match — free, instant
    if new_title in recent:
        return True

    # Stage 1: free similarity scoring
    scored = sorted(
        ((_title_similarity(new_title, t), t) for t in recent),
        key=lambda x: x[0], reverse=True,
    )
    best_sim = scored[0][0] if scored else 0.0

    # Very high overlap => almost certainly the same event; treat as dup
    # without paying for an AI call.
    if best_sim >= 0.6:
        return True
    # Very low overlap => clearly different topic; skip the AI call.
    if best_sim < 0.18:
        return False

    # Stage 2: ambiguous middle ground — ask Claude, but only about the
    # top candidates (short prompt), not all 40 titles.
    candidates = [t for sim, t in scored[:6] if sim >= 0.18]
    if not candidates:
        return False
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(candidates))
    prompt = (
        "Доорх 'ШИНЭ мэдээ' нь 'ӨМНӨХ мэдээнүүд'-ийн аль нэгтэй ЯГ ИЖИЛ үйл "
        "явдлыг өгүүлж байна уу? (өөр өнцөг биш, ижил үйл явдал)\n\n"
        f"ШИНЭ мэдээ: {new_title}\n\n"
        f"ӨМНӨХ мэдээнүүд:\n{numbered}\n\n"
        "ЗӨВХӨН JSON: {\"duplicate\": true/false}"
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=30,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = "".join(b.text for b in msg.content if b.type == "text")
        d = _parse_json_lenient(raw)
        return bool(d.get("duplicate", False))
    except Exception as e:
        print(f"[dedup] check failed (allowing through): {e}")
        return False


def prefilter_political_titles(client, candidates):
    """
    Cheap batch pre-filter: rate every candidate TITLE's political relevance
    in ONE Claude call (titles only, no article text), so we can skip the
    expensive per-article summarization for obviously non-political stories.

    candidates: list of (src, title, url) tuples.
    Returns: list of (src, title, url, pol_guess) kept for full processing,
             biased to keep all political titles + a small filler quota.
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
    prompt = (
        "Чи Монголын мэдээний редактор. Доорх гарчиг бүрд 'ХАЛУУН МЭДЭЭ' "
        "оноо (0-100) өг: уншигчид хэр их анхаарал хандуулж, хуваалцаж, "
        "сэтгэгдэл бичих вэ?\n\n"
        "ӨНДӨР (70-100): улс төрийн дуулиан, авлига, огцруулалт/томилгоо, "
        "жагсаал эсэргүүцэл, гэмт хэрэг, осол гамшиг, иргэдийн мөнгөнд шууд "
        "нөлөөлөх шийдвэр (татвар, тэтгэвэр, цалин, үнэ тариф), хурц зөрчил "
        "маргаан, гэнэтийн том үйл явдал.\n"
        "ДУНД (40-65): УИХ/Засгийн газрын бодит ажил хэрэг, хууль тогтоомж, "
        "эдийн засаг банк санхүү, нийгмийн тулгамдсан асуудал (орон сууц, "
        "эрүүл мэнд, боловсрол, амьжиргаа), сонирхолтой хүний түүх.\n"
        "БАГА (0-25): ёслол хүндэтгэл, шагнал гардуулалт, форум чуулган "
        "нээлт, байгууллагын PR, ердийн урьдчилсан мэдээ, спортын хуваарь, "
        "зар сурталчилгаа.\n\n"
        f"Гарчигууд:\n{numbered}\n\n"
        "ЗӨВХӨН JSON массив буцаа, гарчиг тус бүрийн оноогоор дарааллаар: "
        "[оноо1, оноо2, ...] (өөр юу ч бичихгүй)."
    )
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=400,
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

    # Keep the TOP hot titles (politics + social drama both qualify),
    # capped so the wide candidate net doesn't inflate summarization cost,
    # plus a small filler quota for quiet days.
    hot = sorted([x for x in scored if x[3] >= 30],
                 key=lambda x: x[3], reverse=True)[:6]
    filler = sorted([x for x in scored if x[3] < 30],
                    key=lambda x: x[3], reverse=True)[:2]
    kept = hot + filler
    dropped = len(scored) - len(kept)
    print(f"[prefilter] {len(scored)} titles -> keep {len(kept)} "
          f"({len(hot)} hot + {len(filler)} filler), "
          f"{dropped} skipped before summarizing")
    return kept


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

    # Mark ALL candidates seen BEFORE prefiltering — the prefilter decision
    # is final. Otherwise dropped titles come back as "new" every run,
    # get re-prefiltered (wasted tokens), and crowd out genuinely new
    # articles in the per-source quota.
    for _s, _t, u in candidates:
        mark_seen(con, u)

    # ── Phase 0: cheap political pre-filter (titles only) ─────
    # Skip fetching + summarizing obviously non-political stories.
    # One batch Claude call rates all titles; we keep political ones
    # plus a small filler quota. This is the biggest cost saver since
    # it avoids full summarization of low-value stories.
    candidates = prefilter_political_titles(client, candidates)
    if not candidates:
        print("[collector] nothing political after prefilter")
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

            # cross-run dedup: skip if same event already queued/recently posted
            if is_duplicate_of_recent(client, con, d["title"], d.get("bullets", [])):
                print(f"[skip] duplicate of recent: {d['title'][:50]}")
                continue

            # POLITICS FOCUS: drop foreign news not tied to Mongolia.
            # Apolitical stories (sports/entertainment) are NOT dropped —
            # they enter the queue with low scores as "quiet-day filler",
            # and the poster only reaches them when politics runs dry
            # (politics always outscores them). This matches "mostly
            # politics, allow a little else to fill slots".
            pol = max(0, min(100, int(d.get("political", 0))))
            mn_related = bool(d.get("mongolia_related", True))
            if not mn_related:
                print(f"[skip] foreign, not Mongolia-related: {d['title'][:45]}")
                continue

            primary = cluster[0]
            sources = sorted({a["src"] for a in cluster})
            all_urls = [a["url"] for a in cluster]
            total_words = sum(len(a["text"].split()) for a in cluster)
            orig_min = max(1, round(total_words / 180))

            # politics-focused interest score:
            #   60% political relevance + 20% importance + 20% emotional,
            #   plus a small multi-source boost. This makes strongly political
            #   stories dominate the queue, while still allowing a bit of
            #   high-interest non-political news to fill slots on quiet days
            #   (it just scores lower and sinks below politics).
            # "hot news" interest score, weighted for maximum attention:
            # viral pull leads hard (48%), political weight second (32% —
            # the page identity), real impact third (20%). Multi-source
            # gives only a small nudge (not a preference — a story doesn't
            # need to be on many sites to be hot).
            imp = max(0, min(100, int(d.get("importance", 50))))
            emo = max(0, min(100, int(d.get("emotional", 50))))
            multi_boost = min(6, (len(sources) - 1) * 3)
            econ_boost = 8 if d.get("category") == "Эдийн засаг" else 0
            interest = min(100, round(0.48 * emo + 0.32 * pol + 0.20 * imp)
                           + multi_boost + econ_boost)

            # No card render here: the poster regenerates the card on its
            # own machine at post time (collector-rendered files don't
            # survive across runs), so rendering at collect time was pure
            # wasted work with a permanently stale path.
            card_path = None

            # queue it: posted=0, tagged with today's date
            con.execute(
                "INSERT OR REPLACE INTO digests "
                "(url, source, category, title, bullets, why, orig_min, "
                "published, run_at, sources, source_count, all_urls, "
                "posted, collected_date, card_path, interest_score, full_text, "
                "image_url) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (primary["url"], primary["src"], d.get("category", "Нийгэм"),
                 d["title"], json.dumps(d["bullets"], ensure_ascii=False),
                 d["why"], orig_min, now.isoformat(), now.isoformat(),
                 json.dumps(sources, ensure_ascii=False), len(sources),
                 json.dumps(all_urls, ensure_ascii=False),
                 0, today, card_path, interest, d.get("full_text", ""),
                 primary.get("image_url")),
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

    def fetch(where, params):
        return con.execute(
            "SELECT url, source, category, title, bullets, why, sources, "
            "source_count, card_path, collected_date, full_text, interest_score, "
            "image_url "
            "FROM digests "
            "WHERE posted=0 AND COALESCE(review_needed, 0)=0 AND " + where +
            # Primary: highest interest score (politics dominates).
            # Tie/filler preference: among similar scores, economy stories
            # (Эдийн засаг) come first — so when strong politics runs out,
            # Mongolian economy is the preferred filler over other topics.
            " ORDER BY interest_score DESC, "
            "          (CASE WHEN category='Эдийн засаг' THEN 0 ELSE 1 END), "
            "          source_count DESC, collected_date DESC LIMIT 1",
            params,
        ).fetchone()

    # Same-day posting, 7 days a week (collection now runs weekends too).
    # But the first collection is at 11:00, while posting starts at 08:00 —
    # so any slot before FIRST_COLLECTION_HOUR has no same-day news yet and
    # must draw on the most recent prior day. After that: strictly today's,
    # falling back to prior only if today's queue is somehow empty.
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
            "full_text", "interest_score", "image_url"]
    return dict(zip(keys, row)), mode


def download_article_image(image_url, article_url, out_dir="imgs"):
    """
    Download the article's share image for the card header.
    Validates it's a real, reasonably-sized image (rejects tiny logos,
    favicons, broken files). Returns a local path or None.
    """
    try:
        # find the source config to honor its proxy flag
        use_proxy = False
        for s in SOURCES:
            base = s.get("base_url", "")
            if base and article_url.startswith(base):
                use_proxy = s.get("use_proxy", False)
                break
        r = fetch_html(image_url, timeout=25, use_proxy=use_proxy)
        if r.status_code != 200 or len(r.content) < 8000:
            return None
        os.makedirs(out_dir, exist_ok=True)
        h = hashlib.md5(image_url.encode()).hexdigest()[:10]
        path = os.path.join(out_dir, f"img_{h}.jpg")
        with open(path, "wb") as f:
            f.write(r.content)
        # validate with Pillow: openable and big enough to be a news photo
        from PIL import Image as _Img
        with _Img.open(path) as im:
            im.verify()
        with _Img.open(path) as im:
            if im.width < 400 or im.height < 250:
                return None
        return path
    except Exception as e:
        print(f"[poster] article image skipped ({e})")
        return None


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
        "full_text": story.get("full_text") or "",
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
                    "UPDATE digests SET reel_posted=1 WHERE url=?",
                    (story["url"],),
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
    # post the card image only — no caption (the card is self-contained)
    ok = _post_card_with_caption(card_path, "", token, page_id)
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
    else:
        print(f"Unknown mode '{mode}'. Use 'collect', 'post', 'weather', or 'currency'.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
