#!/usr/bin/env python3
"""
Иш — Morning currency card (Mongolbank official rates)
======================================================
Fetches the Bank of Mongolia's official daily reference rates from
mongolbank.mn and posts a branded rate card each morning.

Parsing is defensive: a currency's value is only accepted if it falls in
a plausible MNT range, so a layout change can never make us post a wrong
number — worst case we skip the post and log it.

Usage:
    from currency import make_currency_post
    card_path, caption = make_currency_post()
"""

import os
import re
from datetime import datetime, timezone, timedelta

UB_TZ = timezone(timedelta(hours=8))

RATE_URLS = [
    # 1. Legacy BOM site (moved to old.mongolbank.mn after the redesign;
    #    the same path on www. now 404s). Server-rendered HTML table.
    "https://old.mongolbank.mn/mn/dblistofficialdailyrate.aspx",
    "https://old.mongolbank.mn/eng/dblistofficialdailyrate.aspx",
    # 2. Long-running community JSON mirror of BOM official rates.
    "https://monxansh.appspot.com/xansh.json?currency=USD|EUR|CNY|RUB|JPY|KRW",
    # 3. New site (JS-rendered — rates usually absent from raw HTML,
    #    kept only as a last resort).
    "https://www.mongolbank.mn/mn/currency-rates",
]

# currency -> (label, plausible MNT range for 1 unit)
CURRENCIES = {
    "USD": ("Ам.доллар", (2500, 6000)),
    "EUR": ("Евро", (2800, 7000)),
    "CNY": ("Юань", (350, 900)),
    "RUB": ("Рубль", (15, 80)),
    "JPY": ("Иен", (12, 45)),
    "KRW": ("Вон", (1.2, 5.0)),
}


def _extract_from_text(text):
    """Find code+number pairs in plain text, sanity-range checked."""
    rates = {}
    for code, (_label, (lo, hi)) in CURRENCIES.items():
        for m in re.finditer(rf"\b{code}\b", text):
            tail = text[m.end():m.end() + 120]
            nm = re.search(r"([\d,]+(?:\.\d+)?)", tail)
            if not nm:
                continue
            try:
                val = float(nm.group(1).replace(",", ""))
            except ValueError:
                continue
            if lo <= val <= hi:
                rates[code] = val
                break
    return rates


def _extract_from_json(data):
    """Walk arbitrary JSON for {code: ..., rate: ...} shapes, range-checked."""
    rates = {}

    def walk(node):
        if isinstance(node, dict):
            code = None
            for v in node.values():
                if isinstance(v, str) and v.strip().upper() in CURRENCIES:
                    code = v.strip().upper()
                    break
            if code and code not in rates:
                lo, hi = CURRENCIES[code][1]
                for v in node.values():
                    try:
                        val = float(str(v).replace(",", ""))
                    except (ValueError, TypeError):
                        continue
                    if lo <= val <= hi:
                        rates[code] = val
                        break
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(data)
    return rates


def fetch_rates():
    """
    Try each source in RATE_URLS until one yields 3+ sanity-checked rates.
    Returns {code: rate} or None (never posts junk).
    """
    from agent_ish import fetch_html  # reuse curl_cffi fetcher
    import json as _json
    for url in RATE_URLS:
        try:
            r = fetch_html(url, timeout=25)
            if r.status_code != 200 or len(r.text) < 200:
                print(f"[currency] {url[:60]}: status {r.status_code}, skipping")
                continue
            body = r.text.strip()
            if body.startswith("[") or body.startswith("{"):
                rates = _extract_from_json(_json.loads(body))
            else:
                text = re.sub(r"<[^>]+>", " ", body)
                text = re.sub(r"\s+", " ", text)
                rates = _extract_from_text(text)
            if len(rates) >= 3:
                print(f"[currency] got {len(rates)} rates from {url[:60]}")
                return rates
            print(f"[currency] {url[:60]}: only {len(rates)} rates, trying next")
        except Exception as e:
            print(f"[currency] fetch failed {url[:60]}: {e}")
    print("[currency] all sources failed — refusing to post")
    return None


def render_card(rates, out_dir="cards"):
    try:
        from card import make_currency_card
        return make_currency_card(
            {"rates": rates, "labels": {c: CURRENCIES[c][0] for c in rates},
             "date": datetime.now(UB_TZ).strftime("%Y-%m-%d")},
            out_dir=out_dir,
        )
    except Exception as e:
        print(f"[currency] card render failed: {e}")
        return None


def make_currency_post(out_dir="cards"):
    """Build the currency card + caption. Returns (card_path, caption) or (None, None)."""
    rates = fetch_rates()
    if not rates:
        return None, None
    card_path = render_card(rates, out_dir=out_dir)

    date_h = datetime.now(UB_TZ).strftime("%m сарын %d")
    order = [c for c in ("USD", "EUR", "CNY", "RUB", "JPY", "KRW") if c in rates]
    lines = [f"💱 Валютын ханш — {date_h}", "",
             "Монголбанкны албан ханш:"]
    for c in order:
        val = rates[c]
        val_s = f"{val:,.2f}" if val < 100 else f"{val:,.0f}"
        lines.append(f"• {c} ({CURRENCIES[c][0]}): {val_s}₮")
    lines += ["", "Эх сурвалж: Монголбанк", "#Иш #ханш #Монголбанк"]
    return card_path, "\n".join(lines)


if __name__ == "__main__":
    p, c = make_currency_post(out_dir=".")
    print("card:", p)
    print(c)
