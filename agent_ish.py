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
 "newsworthy": true/false}}

"newsworthy" дүгнэлт (ЧУХАЛ):
false бол — дараах тохиолдолд:
  • Сурталчилгаа, бүтээгдэхүүн/үйлчилгээ борлуулах далд зар (advertorial)
  • Бодит мэдээлэлгүй, зөвхөн магтаал бүхий байгууллага/компанийн PR
  • "Шинэ бараа гарлаа", "ийм дэлгүүр нээлээ" төрлийн зар
  • Засаг захиргаа/компанийн өөрийгөө магтсан, мэдээлэл агуулаагүй текст
true бол — жинхэнэ мэдээ: улс төр, эдийн засаг, нийгэм, технологи,
  түүнчлэн спорт, соёл, хүн сонирхсон зөөлөн мэдээ ч мөн true.
Эргэлзвэл true. Зорилго: зар, хоосон PR-ийг шүүх, бодит мэдээг үлдээх.

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
                      ("all_urls", "TEXT")]:
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
 "newsworthy": true/false}}

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
# MAIN RUN
# ──────────────────────────────────────────────────────────────

def main():
    now = datetime.now(UB_TZ)
    print(f"\n===== Товч agent run @ {now.isoformat()} =====")

    client = Anthropic()  # uses ANTHROPIC_API_KEY env var
    con = db_init()

    candidates = collect_candidates(con)
    if not candidates:
        print("[main] nothing new, exiting")
        write_json(con)
        return

    # ── Phase 1: fetch text + free ad filter ──────────────────
    articles = []
    skipped_ads = 0
    for src, title, url in candidates:
        mark_seen(con, url)  # mark even on failure so we don't retry forever

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
        print("[main] nothing usable after fetch/filter")
        write_json(con)
        return

    # ── Phase 2: cluster same-story articles across sources ───
    clusters = cluster_candidates(client, articles)
    merged = sum(1 for c in clusters if len(c) > 1)
    print(f"[cluster] {len(articles)} articles -> {len(clusters)} stories "
          f"({merged} merged from multiple sources)")

    # ── Phase 3: summarize (single) or synthesize (cluster) ───
    processed = 0
    for cluster in clusters:
        try:
            if len(cluster) == 1:
                a = cluster[0]
                d = summarize(client, a["src"], a["text"])
            else:
                d = synthesize_cluster(client, cluster)

            if not d.get("newsworthy", True):
                print(f"[skip] not newsworthy (AI filter): {cluster[0]['title'][:50]}")
                continue

            primary = cluster[0]
            sources = sorted({a["src"] for a in cluster})
            all_urls = [a["url"] for a in cluster]
            total_words = sum(len(a["text"].split()) for a in cluster)
            orig_min = max(1, round(total_words / 180))

            con.execute(
                "INSERT OR REPLACE INTO digests "
                "(url, source, category, title, bullets, why, orig_min, "
                "published, run_at, sources, source_count, all_urls) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (primary["url"], primary["src"], d.get("category", "Нийгэм"),
                 d["title"], json.dumps(d["bullets"], ensure_ascii=False),
                 d["why"], orig_min, now.isoformat(), now.isoformat(),
                 json.dumps(sources, ensure_ascii=False), len(sources),
                 json.dumps(all_urls, ensure_ascii=False)),
            )
            con.commit()
            processed += 1
            tag = f" [{len(cluster)} sources]" if len(cluster) > 1 else ""
            print(f"[ok]{tag} {d['title'][:55]}")

            # render branded card image for social posting
            if CARDS_AVAILABLE:
                try:
                    safe = re.sub(r"[^0-9]", "", primary["url"])[-10:] or str(processed)
                    card_path = make_card(
                        {**d, "sources": sources},
                        out_dir="cards",
                        filename=f"card_{safe}.png",
                    )
                    print(f"     card -> {card_path}")
                except Exception as ce:
                    print(f"     card failed: {ce}")

            time.sleep(1)
        except Exception as e:
            print(f"[fail] cluster {cluster[0]['title'][:40]}: {e}")

    print(f"[main] {processed} stories published "
          f"({skipped_ads} ads skipped free, {merged} multi-source)")
    payload = write_json(con)
    send_telegram(payload)


if __name__ == "__main__":
    sys.exit(main())
