# yuribot/models/voice.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta   
from typing import Dict, Iterable, Tuple
from ..db import connect

def _utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def ensure_schema() -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS voice_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,  -- ISO UTC
            left_at   TEXT NOT NULL,  -- ISO UTC
            duration_sec INTEGER NOT NULL
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS voice_minutes_day (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            day TEXT NOT NULL,        -- YYYY-MM-DD (UTC)
            minutes INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id, day)
        )""")
        con.commit()

def add_session(guild_id: int, user_id: int, channel_id: int,
                joined_at: datetime, left_at: datetime) -> int:
    joined_at = _utc(joined_at); left_at = _utc(left_at)
    dur = max(0, int((left_at - joined_at).total_seconds()))
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
        INSERT INTO voice_sessions (guild_id,user_id,channel_id,joined_at,left_at,duration_sec)
        VALUES (?,?,?,?,?,?)""",
        (guild_id, user_id, channel_id, joined_at.isoformat(), left_at.isoformat(), dur))
        con.commit()
        return dur

def upsert_minutes_bulk(items: Iterable[Tuple[int,int,str,int]]) -> int:
    """
    items: (guild_id, user_id, day 'YYYY-MM-DD', minutes)
    """
    items = list(items)
    if not items: return 0
    with connect() as con:
        cur = con.cursor()
        cur.executemany("""
        INSERT INTO voice_minutes_day (guild_id,user_id,day,minutes)
        VALUES (?,?,?,?)
        ON CONFLICT(guild_id,user_id,day) DO UPDATE SET
            minutes = voice_minutes_day.minutes + excluded.minutes
        """, items)
        con.commit()
    return len(items)

@dataclass(frozen=True)
class Session:
    user_id: int
    channel_id: int
    joined_at: datetime

def explode_minutes_per_day(guild_id: int, user_id: int,
                            start: datetime, end: datetime) -> Dict[str, int]:
    """
    Split [start, end) into UTC calendar days and return minutes per day.
    """
    start = _utc(start)
    end = _utc(end)
    if end <= start:
        return {}

    out: Dict[str, int] = {}

    # normalize to minutes on each boundary
    cur = start
    while True:
        cur_day = cur.date()
        next_midnight = datetime(cur.year, cur.month, cur.day, tzinfo=timezone.utc) + timedelta(days=1)
        segment_end = min(end, next_midnight)
        secs = max(0, int((segment_end - cur).total_seconds()))
        if secs:
            out[cur_day.isoformat()] = out.get(cur_day.isoformat(), 0) + int(round(secs / 60.0))
        if segment_end >= end:
            break
        cur = segment_end