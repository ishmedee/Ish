# Иш Тойм — Project Context (for AI coding assistant)

Automated Mongolian news service. Scrapes Mongolian news sites → Claude API filters/scores/summarizes in Mongolian → renders branded PNG cards + vertical video Reels → auto-posts to a Facebook Page. Runs unattended on cron-triggered GitHub Actions. All text output is Mongolian (Cyrillic).

## Environment / deploy
- Repo: `ishmedee/Ish` (private). Dev machine: Windows, `C:\Users\sahme\ish\`.
- Deploy: edit files → `git add . && git commit -m "..." && git pull --rebase --autostash && git push`.
- CI: GitHub Actions workflow `.github/workflows/digest.yml` (installs deps incl. `ffmpeg`, `curl_cffi`; runs one mode).
- Scheduler: **cron-job.org only** (GitHub's native cron never fires reliably for this private repo). Each job POSTs to the workflow_dispatch endpoint with `{"ref":"main","inputs":{"mode":"<mode>"}}`.
- Secrets (GitHub Actions env): `ANTHROPIC_API_KEY`, `FB_PAGE_TOKEN` (permanent, never-expiring page token), `FB_PAGE_ID`. Never hardcode.
- Meta app "Ish Poster" is in **Live mode** (required for API posts to be publicly visible; Development mode restricted their audience). Privacy policy at `privacy.html` hosted via GitHub Pages was required to go Live.
- Claude model string: `claude-sonnet-4-6`.

## Files
- `agent_ish.py` — main (~1480 lines). All modes + scraping + scoring + dedup + FB posting + proxy infra.
- `card.py` — PNG card renderers, 1080×1350. `make_card` (news), `make_weather_card`, `make_currency_card`. Uses Pillow. Handles Mongolian font detection.
- `reel.py` — `make_reel(card_path,...)`: card→10s vertical 1080×1920 MP4 (subtle zoom + music bed) via ffmpeg.
- `weather.py` — Open-Meteo fetch + `make_weather_post`.
- `currency.py` — Mongolbank rates fetch + `make_currency_post`.
- `digest.yml` — GitHub Actions workflow.
- `privacy.html` — bilingual privacy policy (GitHub Pages).
- DB: `towch.db` (SQLite), committed back to repo every run.

## Modes (CLI arg to agent_ish.py; dispatched in `main()`)
- `collect` → `run_collector()`
- `post` → `run_poster()`
- `weather` → `run_weather()`
- `currency` → `run_currency()`

## Current schedule (cron-job.org, Asia/Ulaanbaatar) — **collector & poster must NOT share a minute** (both write towch.db and commit it; same-minute = git collision)
| Job | Cron | Times |
|-----|------|-------|
| Weather | `45 6 * * *` | 06:45 daily |
| Currency | `50 6 * * 1-5` | 06:50 weekdays (rates don't change weekends) |
| Poster | `0 8,11,13,16,19,21 * * *` | 08,11,13,16,19,21 on the hour |
| Collector | `30 11,16 * * *` | 11:30, 16:30 daily |

## Operating strategy (current, final)
- **6 posts/day**, every post also gets a **Reel** (no score gate — ~12 FB actions/day, under the spam limit that was hit at ~64/day).
- **Collect 2×/day** (11:30, 16:30), **7 days/week** incl. weekends.
- **Strict same-day posting**, except the 08:00 slot (runs before first collection at 11:30) uses most-recent prior day so it's not empty. Controlled by `FIRST_COLLECTION_HOUR = 11` in `pick_story_to_post`.
- Editorial: Mongolian **politics primary**; secondary = hot social + Mongolian economy (economy is preferred filler). Foreign news dropped unless directly Mongolia-related.

## Key constants (agent_ish.py)
- `MODEL = "claude-sonnet-4-6"`
- `MAX_PER_SOURCE = 6` (candidates fetched per source per run)
- `MAX_ARTICLES_PER_RUN = 12` (legacy, unused; candidate list capped at 40 via source-balanced round-robin in `collect_candidates` — no source-order slicing; prefilter is the real gate)
- `MIN_ARTICLE_CHARS = 400` (skip stubs)
- `MAX_FETCH_ATTEMPTS = 3`; `FETCH_RETRY_MAX_AGE_DAYS = 2` (bounded cross-run retry for transient article-fetch failures)
- `MAX_IMAGE_BYTES = 10MB`; `MAX_IMAGE_REDIRECTS = 3`; `MAX_IMAGE_PIXELS = 40M` (article-image SSRF/resource guards)
- `FIRST_COLLECTION_HOUR = 11`; `MORNING_FRESH_HOUR = 9` (legacy, unused)
- `MAX_QUEUE_AGE_DAYS = 5` (drop stale unposted)
- `POST_REELS = env POST_REELS == "1"`
- `CATEGORIES = ["Улс төр","Эдийн засаг","Нийгэм","Технологи","Спорт","Дэлхий"]`
- `REEL_MIN_SCORE` — REMOVED (all posts get Reels).

## SQLite schema — table `digests`
Columns: `url` (PK), `source`, `category`, `title`, `bullets` (JSON array), `why`, `orig_min`, `published`, `run_at`, `sources` (JSON), `source_count` (int), `all_urls` (JSON), `posted` (0/1), `collected_date` (YYYY-MM-DD), `card_path`, `posted_at`, `interest_score` (int), `full_text` (elaborated caption), `image_url` (article og:image), `fb_post_id` (confirmed FB feed object id), `reel_posted` (0/1; 1 only after Reel upload confirms), `review_needed` (0/1; 1 = ambiguous feed outcome, quarantined from reposting).
Also table `seen(url)` for dedup of already-processed URLs, plus `fetch_attempts(url PK, attempts, first_seen)` for bounded cross-run retry state after transient article-fetch failures. Tables are created idempotently and `digests` columns are added via the migration loop in `db_init()`.

## Collector pipeline (`run_collector`)
1. `collect_candidates(con)` → all sources → list of `(src_dict, title, url)`, capped at 40 by source-balanced round-robin (one candidate per source per round).
2. **mark ALL candidates seen BEFORE prefilter** (critical: else rejected titles return every run, waste tokens, block per-source quota).
3. `prefilter_political_titles(client, candidates)` → ONE batch Claude call rating each title 0–100 "hot news" (халуун мэдээ). Keeps **top 6 hot (score ≥30) + 2 filler (<30)**. Returns `(src,title,url,pol_guess)` tuples. On parse error, wrong-length output, or any prefilter exception, the deterministic fallback keeps a source-balanced ≤8 candidates instead of all 40. (This is the main cost gate.)
4. For survivors: `fetch_article_text(url, selector, use_proxy)` → returns `(text, image_url)` where image_url is og:image/twitter:image. Transient fetch failures (exception, empty text, or `<MIN_ARTICLE_CHARS`) use `fetch_attempts` for bounded cross-run retry: maximum 3 attempts or 2 days from the first failure. Only failed survivors are temporarily removed from `seen`; rejected prefilter titles remain seen.
5. `cluster_candidates` groups same-event articles; `synthesize_cluster` merges multi-source into one summary.
6. `summarize` (single) or synth (multi) → JSON via `_parse_json_lenient`. Fields: `title, category, bullets[3], why, full_text, newsworthy, importance(0-100), emotional(0-100), political(0-100), mongolia_related(bool), block(bool)`. **max_tokens=2200** (was 900/1000 — caused truncation that killed stories).
7. Filters: skip not-newsworthy, skip `block`, skip foreign not `mongolia_related`.
8. `is_duplicate_of_recent` cross-run dedup (see below).
9. Score (see formula), insert into `digests` with `posted=0`, `image_url`, `full_text`, `interest_score`, `collected_date=today`.
10. `write_json(con)` dumps digest.json; DB committed by workflow.

## Scoring
Prefilter (title-only, hot-news triage): HIGH 70-100 = scandals/corruption/dismissals/appointments/protests/crime/accidents/wallet-impact decisions (tax,pension,salary,tariff)/sharp disputes/sudden events. MID 40-65 = real parliamentary work, legislation, economy/banking, pressing social issues, human-interest. LOW 0-25 = ceremonies/awards/forum openings/PR/routine advisories/sports schedules/ads.

Interest score (in `run_collector`, attention-led):
```
pol = political(0-100); imp = importance; emo = emotional (viral pull)
multi_boost = min(6, (n_sources-1)*3)      # deliberately small — NOT a multi-source preference
econ_boost  = 8 if category=="Эдийн засаг" else 0
interest = min(100, round(0.48*emo + 0.32*pol + 0.20*imp) + multi_boost + econ_boost)
```
`emotional` prompt = "will people share/comment/argue?" `political` = Mongolian-politics relevance. `importance` = real-life money/jobs/safety impact.

## Cross-run dedup (`is_duplicate_of_recent`) — 3 tiers, cost-minimizing
- Free `_title_similarity` (Jaccard word-overlap, words ≥4 chars).
- `≥0.60` → duplicate (free). `<0.18` → unique (free, common case). `0.18–0.60` → one short Claude call comparing top ~6 similar recent titles ("same event?").
- Compares against last ~40 titles from past 3 days (queued or posted). On error → allow through (safe: rare dup > lost story).

## Poster (`run_poster` + `pick_story_to_post`)
- Select single highest `interest_score` unposted story for the slot. Ordering: `interest_score DESC, (economy first among ties), source_count DESC, collected_date DESC`.
- Same-day: `now.hour < FIRST_COLLECTION_HOUR(11)` → prior day; else strictly `collected_date=today`, fallback prior only if today empty.
- Regenerate card ON poster machine (collector cards don't survive across runners — collector-side render was REMOVED as dead work).
- If `image_url`: `download_article_image` uses plain `requests` (not curl_cffi), requires HTTP(S) resolving only to public IPs, and revalidates every manual redirect. It requires `image/*`, streams with a 10MB cap, rejects over 40MP, and keeps the existing openable/≥400×250/≥8KB gates. Any rejection returns `None`, so the card and post continue without a photo; accepted photos become the **full-card darkened background** (blend 0.62, light text palette, NO photo credit line — source is in footer).
- `post_one_to_facebook` (2-step: upload photo published=false → attach to /feed with caption).
- On confirmed feed success, `run_poster` commits `posted=1`, `posted_at`, and `fb_post_id` **before** any Reel work.
- Reel state is separate: `make_reel` + `post_reel_to_facebook` gets one attempt only; confirmation sets `reel_posted=1`, while failure leaves `reel_posted=0` without raising or retrying.
- Ambiguous feed failures (timeout, lost connection after send, or 2xx without an id) leave `posted=0`, set `review_needed=1`, and are excluded by `pick_story_to_post`. Clean failures leave `review_needed=0` and stay queued for safe retry.
- `build_caption`: title + `full_text` (2-3 paragraphs) + bullets + "💡 Яагаад чухал вэ? {why}" + source + link + `#Иш #мэдээ #улстөр`. Falls back to bullets-only if no full_text (old queued items).

## Sources (10 active; list of dicts in agent_ish.py SOURCES)
Each dict: `name`, one of `rss` or `listing`, `link_pattern` (regex), `base_url`, `article_selector` (comma-sep CSS fallback chain), optional `use_proxy`.
- ikon.mn (RSS), MONTSAME ×3 (/mn/more/8 news, /more/18 politics, /more/10 economy), tovch.mn ×2 (/politics,/society; pattern `/n/[a-z0-9]+`), eguur.mn ×2 (WP category URLs, pattern `eguur\.mn/\d{5,}/`), **zarig.mn ×2** (/politics,/society; pattern `zarig\.mn/(?!busad$|live$)[a-z0-9]{3,5}$`).
- BENCHED (commented out): gogo.mn, news.mn — IP/DNS-blocked from GitHub even via curl_cffi + free proxies. `use_proxy` infra + `fetch_via_proxy`/`get_proxy_pool` remain in code (proxyscrape jsdelivr JSON list).

## Anti-blocking
- `fetch_html(url, timeout, use_proxy)`: uses **curl_cffi** `impersonate="chrome"` (TLS fingerprint — recovered eguur.mn) with full browser headers; falls back to plain `requests` if curl_cffi missing/errors. If `use_proxy`, tries free proxies first.
- RSS fetch also routed through `fetch_html` (some feeds block plain feedparser).

## Weather (`weather.py`, mode `weather`)
- `fetch_forecast()`: Open-Meteo `api.open-meteo.com/v1/forecast`, UB lat 47.92/lon 106.92, daily tmax/tmin/precip/weathercode/windmax, tz Asia/Ulaanbaatar. Free, no key.
- `_condition(code,tmax)` maps WMO code → (label, bg_key ∈ clear/clouds/rain/snow/fog/storm/cold). `_bg_for` looks in `assets/weather/{key}.jpg` (user-supplied; solid brand-blue fallback).
- `make_weather_post` returns `(card_path, "")` — **card only, NO caption, NO Claude call** (AI-written advice was removed: recurring Mongolian grammar errors + cost). `write_advice` deleted.
- Posted via `_post_card_with_caption(card_path, "", ...)` (empty message).

## Currency (`currency.py`, mode `currency`)
- Source ladder (`RATE_URLS`, actual order): two **old.mongolbank.mn** HTML endpoints first (DNS currently dead), then **monxansh.appspot.com/xansh.json?currency=USD|EUR|CNY|RUB|JPY|KRW** (community JSON mirror — primary working source), then new mongolbank.mn (JS-rendered, usually empty).
- `fetch_rates()`: tries each URL, handles BOTH JSON (`_extract_from_json` walks arbitrary structure) and HTML (`_extract_from_text` regex code+number). Every value **range-checked** per currency (USD 2500-6000, EUR 2800-7000, CNY 350-900, RUB 15-80, JPY 12-45, KRW 1.2-5.0). If <3 parse → **return None, refuse to post** (never post wrong rates).
- Currencies: USD,EUR,CNY,RUB,JPY,KRW. No Claude call (free).
- Dependency risk: monxansh is 3rd-party; if down, currency silently skips.

## Cards (`card.py`)
- Wordmark on ALL cards: **"Иш Тойм"** (was "Иш").
- `make_card(d, out_dir, filename, photo_path)`: if `photo_path` valid → full-card darkened bg + light palette; else classic paper card w/ category color rail. Has auto-shrink ladder (6 steps) so text never overflows footer.
- `make_weather_card(w, bg_image, ...)`, `make_currency_card(d, ...)`.
- Font: detects a Mongolian-capable font (`_supports_mongolian`); DejaVu lacks emoji glyphs so NO emoji on cards (use text labels).

## JSON parsing (`_parse_json_lenient`)
Repairs truncation: strips ```json fences; if invalid, trims to last `}`; else closes open quote + open `[` + open `{` and retries. Handles mid-`full_text` and mid-`bullets`-array truncation. Needed because long full_text can approach max_tokens.

## Bugs already fixed (don't reintroduce)
- **mark_seen**: must mark ALL candidates seen BEFORE prefilter, not after (else infinite re-processing).
- **[B-02] Source-order cap regression re-fixed**: never slice candidates in source order before scoring. Keep the 40-title prefilter budget source-balanced by round-robin so every source is represented before earlier sources receive another slot.
- **max_tokens too low** (900/1000) truncated JSON → lost stories. Now 2200 in both `summarize` and `synthesize_cluster`.
- **[B-04] Stray GitHub schedule regression re-fixed**: `workflow_dispatch` is the only trigger; the rogue native cron was removed.
- **[E-02] Workflow mode-input shell injection**: bind dispatch input through quoted `$MODE` and allowlist only `collect|post|weather|currency` before invoking Python.
- **Collector card render** removed (poster always regenerates; was dead work).
- **Currency**: old.mongolbank.mn dead → use monxansh mirror.

## Cost (per collect run, history): 38¢ → 31¢ (cheap dedup) → 21¢ (title prefilter). Tighter quota (6 hot+2 filler) + 2×/day keeps it low. Weather & currency = $0 (no AI). Prompt caching evaluated = not worth it (unique article text dominates, small cacheable portion).

## Known harmless git warnings (Windows)
- `cd ish\ish` error (already in folder — skip redundant cd).
- `LF will be replaced by CRLF` (line endings — ignore).
- `.git/objects/pack ... Unlink failed. try again? (y/n)` → answer `n`, push still succeeds. Prevent: close File Explorer on folder / run `git gc`. Success line: `xxxxxxx..yyyyyyy  main -> main`.

## Standing risks / advisories
- **zarig.mn**: investigative outlet, editor faced multiple defamation charges + past regulatory blocking. Automated republishing concentrates legal exposure. Safety-floor `block` flag is the mitigation. Added knowingly.
- **Article photos**: every card republishes source's photo (often wire-agency: Reuters/AFP/Getty). Copyright/rights-claim exposure on FB. Disable instantly by not passing `photo_path`. Watch FB notifications.
- **AI Mongolian grammar**: occasional slips in captions (why weather advice was dropped). Operator reviews and catches these.

## Deferred / TODO
- 6 commercial-bank rates (Golomt/TDB/Xac/State/Khan) — fragile per-bank scrapers, deferred; only Mongolbank live.
- Re-enable gogo.mn/news.mn with residential proxy or headless browser.
- Website ish.mn (domain owned, hosting deferred).
- Growth: ad boosting, follower building.
- Watch: first real photo-card look (raise darken 0.62→0.70 if text contrast poor); weekend evening-slot supply (loosen keep quota 6→8 if short).
