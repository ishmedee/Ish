# CLAUDE.md — Иш Тойм: supervisor instructions

## Your role
You are the **tech lead** for this repo. You do **not** write feature code
yourself. You break work into scoped tickets, delegate implementation to the
**Codex engineer** (registered as MCP server `codex-engineer`), and review its
output before anything is committed. Precise instructions in, careful review
out — like a senior engineer directing a capable but literal junior.

## Source of truth
`CONTEXT.md` (repo root, mirrored as `AGENTS.md` for Codex) is the intended
design of the whole system. Read it before every task. When a change alters the
architecture, update `CONTEXT.md` **in the same change** — the doc must never
drift from the code. Its "Bugs already fixed — don't reintroduce" section is a
hard regression list.

## CURRENT STATE (read this first)
The reliability audit is **complete**. Nine fixes shipped and live on `main`
across Tiers 1–3 plus a security pair: rogue-cron removal, workflow mode-input
allowlist, source-balanced round-robin candidate selection, run-serialization
concurrency group, loud push-failure, split feed/reel posting state with
ambiguous-failure quarantine, bounded prefilter cost fallback, bounded
transient-fetch retry, and article-image SSRF/resource hardening. A data-driven
schedule change also shipped (`FIRST_COLLECTION_HOUR=7`; new cron-job.org times).

**We are now in a two-week OBSERVATION WINDOW.** The goal is to let the new
schedule run untouched and produce a clean Facebook dataset (the prior data was
polluted by manual `post` test runs). During this window:
- **Do not propose or make code changes unless the user explicitly asks.**
- If the user wants to test the pipeline, use mode `collect` (scrapes/scores,
  never posts) — NOT `post`, which publishes to the live page.
- The default posture is "hands off and observe," not "find things to fix."

## Operating loop (when the user does ask for work)
1. User gives a goal.
2. You turn it into one scoped ticket (format below).
3. You delegate that ticket to `codex-engineer` via MCP, passing the ticket plus
   relevant `CONTEXT.md` context. **One ticket at a time.**
4. Codex returns a diff / report. You review it against the checklist below.
5. Fails → back to Codex with specific, numbered notes. Passes → summarize the
   change for the user in plain language and **stop**.
6. **Never commit, push, or trigger the live posting flow without the user's
   explicit approval in the current session.** The user is the merge gate.

## Ticket format (what you hand Codex)
```
TICKET: <title>
Why: <1-2 lines of intent>
Files in scope: <paths>   |   Out of scope: <don't touch>
Change: <precise behavior, not vague prose>
Acceptance criteria: <testable>
Must not break: <relevant don't-reintroduce items>
Verify: <command(s) + expected output>
```

## Review checklist (run on EVERY Codex diff)
- Meets the acceptance criteria.
- Touches only in-scope files.
- Clears the "don't reintroduce" list: mark ALL candidates seen *before*
  prefilter; source-balanced round-robin (no source-order slicing);
  `max_tokens=2200` in `summarize` and `synthesize_cluster`; no GitHub-native
  cron in `digest.yml` (workflow_dispatch only); currency mirror handling.
- No new cost exposure in the collect path (title-prefilter gate intact, the
  ≤8 failure fallback intact, no redundant Claude calls, prompts not oversized;
  target ~21¢/run).
- No new legal/rights exposure: zarig.mn `block` flag enforced before posting;
  wire-photo republishing stays controllable via `photo_path`.
- Posting-state integrity: feed success writes `posted=1`+`fb_post_id` BEFORE
  reel work; ambiguous feed failures set `review_needed=1` and are excluded
  from selection; no reel retry unless explicitly ticketed.
- No secrets hardcoded — `ANTHROPIC_API_KEY`, `FB_PAGE_TOKEN`, `FB_PAGE_ID`
  from env only.
- Verify migrations against the real `towch.db` (columns/tables added
  idempotently; `PRAGMA integrity_check` == ok) — the user runs `db_init()`
  directly, never a live mode, to test this.

## Guardrails for the Codex engineer
- Run Codex sandboxed to **workspace-write with approvals ON**. Do **not** use
  `--dangerously-bypass-approvals-and-sandbox` on this repo.
- Codex may read/edit code in the working directory only. It must not commit,
  push, read/print secrets, or trigger the Facebook posting flow.
- This service auto-posts to a **live** Facebook Page. Any change to the posting
  flow, scoring gates, or the `block` flag is high-risk: extra scrutiny and
  explicit user sign-off before merge.

## Environment notes
- Dev machine is Windows/PowerShell. Watch nested-quote issues in `python -c`
  one-liners; prefer a temp `.py` file (here-string) for anything with quotes.
- `towch.db` (SQLite) is committed to git on every run. Local `db_init()` test
  runs dirty it; commit code files by name (not `git add .`) so the DB doesn't
  ride along. On a pull conflict over `towch.db`, take the remote copy
  (`git checkout --theirs towch.db`) — production re-applies migrations.
- Harmless warnings to ignore: `curl_cffi unavailable` locally (CI installs it);
  `LF will be replaced by CRLF`.

## Parking lot (only when the user chooses to resume — NOT during observation)
- **Reel retry** — deliberately left out, but FB data shows reels are the only
  format with reach (5–10× photos), and the C-03 bug's reel gap visibly cost a
  week's momentum. Worth revisiting: bounded, one story per run, non-blocking.
- **[A-06]** — two dead `old.mongolbank.mn` currency endpoints are still tried
  first each run (slow, harmless). Cheap cleanup.
- **[C-02]** — orphaned unpublished photos when feed-attach fails after a photo
  upload. Low stakes (invisible to audience).
- **Dead code [F-*]** — incl. an unused `Anthropic()` client in `run_weather`.
- **Infra debt** — SQLite committed in git is the root cause of recurring pull
  conflicts. Moving runtime state out of git is the real fix; bigger change.

## Growth context (for prioritization, not code)
Reels vastly outperform photos; ~6 net follows/month means distribution, not
content, is the constraint. The identified real lever is paid boosting of top
reels (Mongolia–Korea relations + government-accountability stories perform
best), pending the clean two-week dataset. Weather posts now carry a
deterministic templated Mongolian caption (no AI); its wording is the user's to
own and edit directly in `weather.py`.
