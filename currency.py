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
    "https://www.mongolbank.mn/mn/currency-rates",
    "https://www.mongolbank.mn/en/currency-rates",
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


def fetch_rates():
    """
    Return {code: rate_float} for the currencies we track, or None if
    fewer than 3 could be extracted (treat as failure — don't post junk).
    """
    from agent_ish import fetch_html  # reuse curl_cffi fetcher
    html = None
    for url in RATE_URLS:
        try:
            r = fetch_html(url, timeout=25)
            if r.status_code == 200 and len(r.text) > 5000:
                html = r.text
                break
        except Exception as e:
            print(f"[currency] fetch failed {url}: {e}")
    if not html:
        print("[currency] could not load Mongolbank rates page")
        return None

    # strip tags to plain text so code+number end up adjacent
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)

    rates = {}
    for code, (_label, (lo, hi)) in CURRENCIES.items():
        # find the code, then the nearest following number like 3,456.78
        for m in re.finditer(rf"\b{code}\b", text):
            tail = text[m.end():m.end() + 120]
            nm = re.search(r"([\d,]+(?:\.\d+)?)", tail)
            if not nm:
                continue
            try:
                val = float(nm.group(1).replace(",", ""))
            except ValueError:
                continue
            if lo <= val <= hi:          # sanity range — reject junk
                rates[code] = val
                break
    if len(rates) < 3:
        print(f"[currency] only parsed {len(rates)} rates — refusing to post")
        return None
    return rates


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
