from __future__ import annotations

from datetime import datetime, timezone, date as date_cls
from typing import Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from ..config import LOCAL_TZ


def coerce_timezone(tz_str: Optional[str]):
    if tz_str and ZoneInfo:
        try:
            return ZoneInfo(tz_str)
        except Exception:
            pass
    return LOCAL_TZ or timezone.utc


def tz_display(tzinfo) -> str:
    return getattr(tzinfo, "key", None) or str(tzinfo)


def parse_date(raw: str) -> Optional[date_cls]:
    try:
        y, m, d = (int(part) for part in raw.split("-", 2))
        return date_cls(y, m, d)
    except Exception:
        return None


def parse_time(raw: str) -> Optional[Tuple[int, int, int]]:
    parts = raw.split(":")
    try:
        if len(parts) == 2:
            hh, mm = int(parts[0]), int(parts[1])
            return hh, mm, 0
        if len(parts) == 3:
            hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
            return hh, mm, ss
    except Exception:
        return None
    return None


def to_epoch(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp())