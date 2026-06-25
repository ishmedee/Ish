#!/usr/bin/env python3
"""
Иш — Reel generator
====================
Turns a card PNG into a 10-second vertical (1080x1920) MP4 Reel with a
subtle slow zoom (so Meta treats it as real video, not a static image)
and a soft ambient music bed.

Requires ffmpeg on PATH (present on GitHub Actions ubuntu runners).

Usage:
    from reel import make_reel
    mp4_path = make_reel("cards/post_abc.png", out_dir="reels")
"""

import os
import subprocess

REEL_SECONDS = 10
PAPER = "0xF4F6F5"   # background pad color (matches card bg)
# user-provided royalty-free track (from Pixabay etc). Falls back to the
# bundled synthesized bed if music.mp3 is absent.
_ASSET_DIR = os.path.join(os.path.dirname(__file__), "assets")
_MUSIC_MP3 = os.path.join(_ASSET_DIR, "music.mp3")
_AMBIENT_FALLBACK = os.path.join(_ASSET_DIR, "ambient_bed.aac")
AMBIENT = _MUSIC_MP3 if os.path.exists(_MUSIC_MP3) else _AMBIENT_FALLBACK


def make_reel(card_path, out_dir="reels", filename=None, seconds=REEL_SECONDS):
    """
    card_path: path to the card PNG (1080x1350).
    Returns path to the rendered MP4, or None on failure.
    """
    if not card_path or not os.path.exists(card_path):
        print(f"[reel] card not found: {card_path}")
        return None
    os.makedirs(out_dir, exist_ok=True)
    base = filename or (os.path.splitext(os.path.basename(card_path))[0] + ".mp4")
    out = os.path.join(out_dir, base)

    # zoompan needs a per-frame zoom step; at 25fps over `seconds`,
    # drift from 1.00 to ~1.08 for a gentle, barely-perceptible push-in.
    frames = seconds * 25
    vf = (
        f"[0:v]scale=1080:-1,"
        f"pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color={PAPER},"
        f"zoompan=z='min(zoom+0.0008,1.08)':d={frames}:s=1080x1920:fps=25[v]"
    )

    has_audio = os.path.exists(AMBIENT)
    cmd = ["ffmpeg", "-y", "-loop", "1", "-i", card_path]
    if has_audio:
        cmd += ["-i", AMBIENT]
    cmd += ["-filter_complex", vf, "-map", "[v]"]
    if has_audio:
        # take first `seconds` of the track, fade in 1s / out 1.5s, normalize
        fade_out_start = max(0, seconds - 1.5)
        af = (f"atrim=0:{seconds},afade=t=in:st=0:d=1,"
              f"afade=t=out:st={fade_out_start}:d=1.5,aresample=44100")
        cmd += ["-map", "1:a", "-af", af, "-c:a", "aac", "-b:a", "128k", "-shortest"]
    cmd += ["-c:v", "libx264", "-t", str(seconds), "-pix_fmt", "yuv420p", out]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if r.returncode != 0:
            print(f"[reel] ffmpeg failed: {r.stderr[-300:]}")
            return None
        if not os.path.exists(out):
            return None
        return out
    except Exception as e:
        print(f"[reel] error: {e}")
        return None


if __name__ == "__main__":
    import sys
    card = sys.argv[1] if len(sys.argv) > 1 else "cards/test.png"
    p = make_reel(card, out_dir=".", filename="reel_sample.mp4")
    print("wrote", p)
