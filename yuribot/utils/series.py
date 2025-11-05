from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from ..config import LOCAL_TZ
from ..utils.collection import normalized_club


def next_friday_at(hour: int) -> datetime:
    now = datetime.now(tz=LOCAL_TZ)
    days = (4 - now.weekday()) % 7
    if days == 0:
        days = 7
    return now.replace(hour=hour, minute=0, second=0, microsecond=0) + timedelta(days=days)


def to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(timezone.utc)


def build_sections(total_chapters: int, per_section: int) -> List[Tuple[int, int]]:
    sections: List[Tuple[int, int]] = []
    start = 1
    while start <= total_chapters:
        end = min(start + per_section - 1, total_chapters)
        sections.append((start, end))
        start = end + 1
    return sections
