from __future__ import annotations

from datetime import date as date_cls, datetime, timedelta, timezone
from typing import Optional, Tuple

import discord

from ..config import LOCAL_TZ


def to_local(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(LOCAL_TZ)


def local_dt(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=LOCAL_TZ)


def to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(timezone.utc)


def next_saturday(base: Optional[datetime] = None) -> date_cls:
    base = base or datetime.now(LOCAL_TZ)
    offset = (5 - base.weekday()) % 7  # Saturday is 5
    return (base + timedelta(days=offset)).date()


def parse_date_yyyy_mm_dd(raw: Optional[str]) -> Optional[date_cls]:
    if not raw:
        return None
    try:
        year, month, day = (int(part) for part in raw.split("-", 2))
        return date_cls(year, month, day)
    except Exception:
        return None


def infer_entity_type(
    venue: Optional[discord.abc.GuildChannel],
    *,
    default_location: str,
) -> Tuple[discord.EntityType, dict]:
    if isinstance(venue, discord.StageChannel):
        return discord.EntityType.stage_instance, {"channel": venue}
    if isinstance(venue, discord.VoiceChannel):
        return discord.EntityType.voice, {"channel": venue}
    location = (default_location or "External").strip()
    return discord.EntityType.external, {"location": location[:100]}


async def attachment_to_image_bytes(att: Optional[discord.Attachment]) -> Optional[bytes]:
    if not att:
        return None
    if not (att.content_type or "").startswith("image/"):
        return None
    try:
        return await att.read()
    except Exception:
        return None
