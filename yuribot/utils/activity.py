from __future__ import annotations

import os
import re
from calendar import monthrange
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import discord

from .storage import resolve_data_dir

XP_MULTIPLIERS: Dict[int, float] = {}  # channel_id -> multiplier
MULTIPLIER_DEFAULT: float = 1.0

ROLE_XP_MULTIPLIERS: Dict[int, float] = {
    1418285755339374785: 2.0,  # Server Boosters
}
PIN_MULTIPLIER: float = 2.0
PIN_FALLBACK_XP: int = 50

WORD_RE = re.compile(r"\b\w+\b", flags=re.UNICODE)
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w+:(\d+)>")
UNICODE_EMOJI_RE = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]"
)
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
DAY_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
WEEK_RE = re.compile(r"^\d{4}-W(0[1-9]|[1-4]\d|5[0-3])$")
PT_TZNAME = "America/Los_Angeles"
_ZW_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
WHITESPACE_PUNCT_RE = re.compile(
    r"[\s\.,;:!?\-\(\)\[\]\{\}_+=/\\|~`\"'<>]+", flags=re.UNICODE
)

GIF_DOMAINS = (
    "tenor.com",
    "media.tenor.com",
    "giphy.com",
    "media.giphy.com",
    "imgur.com",
    "i.imgur.com",
    "discordapp.com",
    "cdn.discordapp.com",
)


def ensure_matplotlib_environment() -> None:
    if "MPLCONFIGDIR" not in os.environ:
        os.environ["MPLCONFIGDIR"] = str(resolve_data_dir("matplotlib"))


def strip_custom_emojis(text: str) -> str:
    return CUSTOM_EMOJI_RE.sub("", text or "")


def strip_unicode_emojis(text: str) -> str:
    return UNICODE_EMOJI_RE.sub("", text or "")


def is_emoji_only(text: str | None) -> bool:
    if not text:
        return False
    t = _ZW_RE.sub("", text)
    t = strip_custom_emojis(t)
    t = strip_unicode_emojis(t)
    t = WHITESPACE_PUNCT_RE.sub("", t)
    return t == ""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def month_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def week_default() -> str:
    dt = datetime.now(timezone.utc).date()
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def day_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def day_key(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.now(timezone.utc)).strftime("%Y-%m-%d")


def count_words(text: str | None) -> int:
    return 0 if not text else len(WORD_RE.findall(text))


def count_emojis_text(text: str | None) -> int:
    if not text:
        return 0
    return len(CUSTOM_EMOJI_RE.findall(text)) + len(UNICODE_EMOJI_RE.findall(text))


def channel_multiplier(ch: discord.abc.GuildChannel | None) -> float:
    if not ch:
        return MULTIPLIER_DEFAULT
    return XP_MULTIPLIERS.get(getattr(ch, "id", 0), MULTIPLIER_DEFAULT)


def role_multiplier(member: Optional[discord.Member]) -> float:
    if not member or not getattr(member, "roles", None):
        return 1.0
    best = 1.0
    for role in member.roles:
        if role and role.id in ROLE_XP_MULTIPLIERS:
            try:
                best = max(best, float(ROLE_XP_MULTIPLIERS[role.id]))
            except Exception:
                continue
    return best


def xp_multiplier(
    member: Optional[discord.Member], ch: Optional[discord.abc.GuildChannel]
) -> float:
    return max(0.0, float(channel_multiplier(ch) * role_multiplier(member)))


def gif_source_from_url(url: str) -> str:
    try:
        from urllib.parse import urlparse

        host = (urlparse(url).hostname or "").lower()
        for domain in GIF_DOMAINS:
            if host.endswith(domain):
                root = domain.split(".")[-2]
                return "discord" if root == "discordapp" else root
        if url.lower().endswith(".gif"):
            return "other"
    except Exception:
        pass
    return "other"


def prime_window_from_hist(hour_counts: List[int], window: int = 1) -> tuple[int, int, int]:
    best_sum, best_hour = -1, 0
    for hour in range(24):
        total = sum(hour_counts[(hour + offset) % 24] for offset in range(window))
        if total > best_sum:
            best_sum, best_hour = total, hour
    return best_hour, (best_hour + window) % 24, best_sum


def parse_scope_and_key(
    scope: str | None, day: str | None, week: str | None, month: str | None
) -> tuple[str, Optional[str]]:
    selected = scope or "month"
    if selected == "day":
        key = day or day_default()
        if not DAY_RE.match(key):
            raise ValueError("bad_day_format")
    elif selected == "week":
        key = week or week_default()
        if not WEEK_RE.match(key):
            raise ValueError("bad_week_format")
    elif selected == "month":
        key = month or month_default()
        if not MONTH_RE.match(key):
            raise ValueError("bad_month_format")
    else:
        selected = "all"
        key = None
    return selected, key


def total_days_in_month(year: int, month: int) -> int:
    return monthrange(year, month)[1]


def clamp_timestamp(ts: datetime, *, min_age_days: int = 0) -> datetime:
    if min_age_days <= 0:
        return ts
    return ts - timedelta(days=min_age_days)
