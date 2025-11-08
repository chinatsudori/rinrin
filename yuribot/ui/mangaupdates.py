from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Tuple

import discord

from ..strings import S
from ..utils.mangaupdates import (
    WatchEntry,
    extract_max_chapter,
    extract_max_volume,
    now_utc,
    release_timestamp,
    strip_text,
)

__all__ = ["format_release_bits", "build_release_embed", "build_batch_embed"]

MAX_BATCH_LINES = 15
SEPARATOR = " \x07 "


def format_release_bits(rel: dict) -> Tuple[str, str]:
    vol = strip_text(str(rel.get("volume") or ""))
    ch = strip_text(str(rel.get("chapter") or ""))
    sub = strip_text(str(rel.get("subchapter") or ""))

    if not (vol or ch):
        title_str = " ".join(
            [
                str(rel.get("title", "")),
                str(rel.get("raw_title", "")),
                str(rel.get("description", "")),
            ]
        )
        lowered = title_str.lower()
        if not vol:
            vol = extract_max_volume(lowered)
        if not ch:
            ch, sub = extract_max_chapter(lowered)

    bits = []
    if vol:
        bits.append(f"v{vol}")
    if ch:
        bits.append(f"ch {ch}")
    if sub:
        bits.append(sub)

    chbits = SEPARATOR.join(bits) if bits else S("mu.release.generic")

    group_raw = rel.get("group") or rel.get("group_name") or ""
    if isinstance(group_raw, dict):
        group = strip_text(group_raw.get("name") or group_raw.get("group_name") or "")
    else:
        group = strip_text(str(group_raw))

    url = strip_text(rel.get("url") or rel.get("release_url") or rel.get("link") or "")

    extras = []
    if group:
        extras.append(S("mu.release.group", group=discord.utils.escape_markdown(group)))

    rdate = (
        rel.get("release_date")
        or rel.get("date")
        or rel.get("pubDate")
        or rel.get("pubdate")
    )
    if rdate:
        try:
            dt = datetime.fromisoformat(str(rdate).replace("Z", "+00:00"))
            extras.append(S("mu.release.date_rel", ts=int(dt.timestamp())))
        except Exception:
            extras.append(S("mu.release.date_raw", date=str(rdate)))

    if url:
        extras.append(url)

    return chbits, "\n".join(extras) if extras else ""


def build_release_embed(watch: WatchEntry, release: dict) -> discord.Embed:
    chbits, extras = format_release_bits(release)
    ts = release_timestamp(release)
    if ts > 0:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    else:
        dt = now_utc()

    embed = discord.Embed(
        title=S("mu.update.title", series=watch.series_title, chbits=chbits),
        description=extras or None,
        color=discord.Color.blurple(),
        timestamp=dt,
    )
    embed.set_footer(text=S("mu.update.footer"))
    return embed


def build_batch_embed(watch: WatchEntry, releases: List[dict]) -> discord.Embed:
    lines: List[str] = []
    for rel in releases[:MAX_BATCH_LINES]:
        chbits, _ = format_release_bits(rel)
        url = strip_text(rel.get("url") or "")
        maybe_url = f" ({url})" if url else ""
        lines.append(S("mu.batch.line", chbits=chbits, maybe_url=maybe_url))

    overflow = len(releases) - len(lines)
    if overflow > 0:
        lines.append(f"... +{overflow} more")

    embed = discord.Embed(
        title=S("mu.batch.title", series=watch.series_title, n=len(releases)),
        description="\n".join(lines),
        color=discord.Color.blurple(),
        timestamp=now_utc(),
    )
    embed.set_footer(text=S("mu.batch.footer"))
    return embed
