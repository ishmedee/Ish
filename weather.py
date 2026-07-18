#!/usr/bin/env python3
"""
Иш — Morning weather card
=========================
Fetches Ulaanbaatar's daily forecast (Open-Meteo, free, no key), asks Claude
for a short Mongolian post with what-to-wear / umbrella advice, and renders a
branded weather card using a condition-matched background image.

Background images (you provide these) live in assets/weather/:
    clear.jpg  clouds.jpg  rain.jpg  snow.jpg  fog.jpg  storm.jpg  cold.jpg
If a matching image is missing, falls back to a solid brand color.

Usage:
    from weather import make_weather_post
    card_path, caption = make_weather_post(client)   # client = Anthropic()
"""

import os
import requests
from datetime import datetime, timezone, timedelta

UB_TZ = timezone(timedelta(hours=8))
UB_LAT, UB_LON = 47.92, 106.92
ASSET_WEATHER_DIR = os.path.join(os.path.dirname(__file__), "assets", "weather")

# WMO weather codes -> (mongolian condition label, background key)
# https://open-meteo.com/en/docs  (weathercode field)
def _condition(code, tmax):
    if code in (0,):
        key = "clear"; label = "Цэлмэг"
    elif code in (1, 2, 3):
        key = "clouds"; label = "Багавтар үүлшинэ" if code < 3 else "Үүлэрхэг"
    elif code in (45, 48):
        key = "fog"; label = "Манантай"
    elif code in (51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82):
        key = "rain"; label = "Бороотой"
    elif code in (71, 73, 75, 77, 85, 86):
        key = "snow"; label = "Цастай"
    elif code in (95, 96, 99):
        key = "storm"; label = "Аадар бороо, аянга"
    else:
        key = "clouds"; label = "Үүлэрхэг"
    # very cold override (UB winters) — use a cold-themed bg if provided
    if tmax is not None and tmax <= -15 and key in ("clear", "clouds"):
        key = "cold"
    return label, key


def fetch_forecast():
    """Return dict with today's UB forecast, or None on failure."""
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": UB_LAT, "longitude": UB_LON,
                "daily": "temperature_2m_max,temperature_2m_min,"
                         "precipitation_sum,weathercode,windspeed_10m_max",
                "timezone": "Asia/Ulaanbaatar", "forecast_days": 1,
            },
            timeout=20,
        )
        d = r.json()["daily"]
        return {
            "date": d["time"][0],
            "tmax": round(d["temperature_2m_max"][0]),
            "tmin": round(d["temperature_2m_min"][0]),
            "precip": d["precipitation_sum"][0],
            "code": d["weathercode"][0],
            "wind": round(d["windspeed_10m_max"][0]),
        }
    except Exception as e:
        print(f"[weather] fetch failed: {e}")
        return None


def _bg_for(key):
    """Return a background image path for the condition, or None."""
    for ext in ("jpg", "jpeg", "png", "webp"):
        p = os.path.join(ASSET_WEATHER_DIR, f"{key}.{ext}")
        if os.path.exists(p):
            return p
    return None


def make_weather_post(client=None, out_dir="cards"):
    """
    Build the morning weather card. No caption, no Claude call — the card
    is self-contained (temp, condition, wind, precip, date).
    `client` is accepted for backward compatibility but unused.
    Returns (card_path, "") or (None, "") on failure.
    """
    fc = fetch_forecast()
    if not fc:
        return None, ""
    label, bg_key = _condition(fc["code"], fc["tmax"])

    try:
        from card import make_weather_card
        bg = _bg_for(bg_key)
        card_path = make_weather_card(
            {"tmax": fc["tmax"], "tmin": fc["tmin"], "label": label,
             "wind": fc["wind"], "precip": fc["precip"], "date": fc["date"]},
            bg_image=bg, out_dir=out_dir,
        )
    except Exception as e:
        print(f"[weather] card render failed: {e}")
        card_path = None

    return card_path, ""


if __name__ == "__main__":
    p, _ = make_weather_post(out_dir=".")
    print("card:", p)
