from __future__ import annotations
from datetime import datetime
from ..config import LOCAL_TZ

def now_local() -> datetime:
    return datetime.now(tz=LOCAL_TZ)

def to_iso(dt: datetime) -> str:
    return dt.astimezone(LOCAL_TZ).isoformat()

def from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)