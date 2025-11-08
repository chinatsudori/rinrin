from __future__ import annotations


import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, Tuple, List, Optional, DefaultDict
from collections import defaultdict

from ..db import connect

log = logging.getLogger(__name__)


# ---------- time utils ----------


def _utc(dt: datetime) -> datetime:
    """Ensure a timezone-aware UTC datetime."""
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ---------- schema ----------


def ensure_schema() -> None:
    with connect() as con:
        cur = con.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS voice_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                joined_at   TEXT    NOT NULL,  -- ISO UTC
                left_at     TEXT    NOT NULL,  -- ISO UTC
                duration_sec INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS voice_minutes_day (
                guild_id INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                day      TEXT    NOT NULL,     -- YYYY-MM-DD (UTC)
                minutes  INTEGER NOT NULL,
                PRIMARY KEY (guild_id, user_id, day)
            )
            """
        )

        # Helpful indexes for scans / reporting
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_voice_sess_guild_user ON voice_sessions(guild_id, user_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_voice_sess_left ON voice_sessions(left_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_voice_min_user_day ON voice_minutes_day(user_id, day)"
        )

        con.commit()


# ---------- writes ----------


def add_session(
    guild_id: int,
    user_id: int,
    channel_id: int,
    joined_at: datetime,
    left_at: datetime,
) -> int:
    """Insert one completed session; returns duration in seconds (clamped to >= 0)."""
    joined_at = _utc(joined_at)
    left_at = _utc(left_at)
    dur = max(0, int((left_at - joined_at).total_seconds()))
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO voice_sessions (guild_id,user_id,channel_id,joined_at,left_at,duration_sec)
            VALUES (?,?,?,?,?,?)
            """,
            (
                guild_id,
                user_id,
                channel_id,
                joined_at.isoformat(),
                left_at.isoformat(),
                dur,
            ),
        )
        con.commit()
    return dur


def upsert_minutes_bulk(items: Iterable[Tuple[int, int, str, int]]) -> int:
    """
    Upsert many day-minutes rows.
    items: (guild_id, user_id, day 'YYYY-MM-DD', minutes)
    Returns number of rows attempted (not the number of unique keys).
    """
    batch = list(items)
    if not batch:
        return 0
    with connect() as con:
        cur = con.cursor()
        cur.executemany(
            """
            INSERT INTO voice_minutes_day (guild_id,user_id,day,minutes)
            VALUES (?,?,?,?)
            ON CONFLICT(guild_id,user_id,day) DO UPDATE SET
                minutes = voice_minutes_day.minutes + excluded.minutes
            """,
            batch,
        )
        con.commit()
    return len(batch)


# ---------- session + bucketing helpers ----------


@dataclass(frozen=True)
class Session:
    user_id: int
    channel_id: int
    joined_at: datetime
    left_at: datetime


def explode_minutes_per_day(
    guild_id: int,  # kept for interface symmetry (not used in math)
    user_id: int,  # kept for interface symmetry (not used in math)
    start: datetime,
    end: datetime,
) -> Dict[str, int]:
    """
    Split [start, end) into UTC calendar days and return minutes per day.
    Keys are 'YYYY-MM-DD' (UTC). Values are whole minutes (rounded).
    """
    start = _utc(start)
    end = _utc(end)
    if end <= start:
        return {}

    out: Dict[str, int] = {}
    cur = start
    while True:
        next_midnight = datetime(
            cur.year, cur.month, cur.day, tzinfo=timezone.utc
        ) + timedelta(days=1)
        seg_end = min(end, next_midnight)
        secs = max(0, int((seg_end - cur).total_seconds()))
        if secs:
            day_key = cur.date().isoformat()
            out[day_key] = out.get(day_key, 0) + int(round(secs / 60.0))
        if seg_end >= end:
            break
        cur = seg_end

    return out


class SessionAccumulator:
    """
    Lightweight join/leave accumulator.
    - start(uid, cid, ts)
    - end(uid, cid, ts)
    - materialize() -> List[Session]
    """

    def __init__(self) -> None:
        self._open: Dict[Tuple[int, int], datetime] = {}
        self._closed: List[Session] = []

    def start(self, uid: int, cid: int, ts: datetime) -> None:
        ts = _utc(ts)
        key = (uid, cid)

        # If this user has an open session in a different channel, close it at ts (channel move).
        to_close = [(k, st) for k, st in self._open.items() if k[0] == uid and k != key]
        for k, st in to_close:
            if ts > st:
                self._closed.append(Session(k[0], k[1], st, ts))
            del self._open[k]

        # Duplicate join in same channel? ignore; else open
        self._open.setdefault(key, ts)

    def end(self, uid: int, cid: int, ts: datetime) -> None:
        ts = _utc(ts)
        key = (uid, cid)
        st = self._open.pop(key, None)
        if st and ts > st:
            self._closed.append(Session(uid, cid, st, ts))

    def materialize(self, close_open_at: Optional[datetime] = None) -> List[Session]:
        """
        Convert to a list of closed sessions.
        If close_open_at is provided, dangling opens are closed at that time.
        """
        if close_open_at is not None:
            cut = _utc(close_open_at)
            for (uid, cid), st in list(self._open.items()):
                if cut > st:
                    self._closed.append(Session(uid, cid, st, cut))
                del self._open[(uid, cid)]
        return list(self._closed)


def upsert_sessions_minutes(
    guild_id: int, sessions: Iterable[Session]
) -> Tuple[int, int]:
    """
    Convert sessions -> day-minute aggregates -> upsert.
    Returns (rows_upserted, total_minutes_added).
    """
    agg: DefaultDict[Tuple[int, int, str], int] = defaultdict(int)
    total_minutes = 0

    for s in sessions:
        # Bucket by day (UTC)
        per_day = explode_minutes_per_day(guild_id, s.user_id, s.joined_at, s.left_at)
        for day, mins in per_day.items():
            agg[(guild_id, s.user_id, day)] += mins
            total_minutes += mins

        # Also store the raw session
        add_session(guild_id, s.user_id, s.channel_id, s.joined_at, s.left_at)

    # Upsert aggregated minutes
    squashed = _squash(agg)
    rows = upsert_minutes_bulk(
        (gid, uid, day, mins) for (gid, uid, day), mins in squashed.items()
    )

    return rows, total_minutes


def _squash(d: Dict[Tuple[int, int, str], int]) -> Dict[Tuple[int, int, str], int]:
    """Return a copy (helps with typing when passing a generator)."""
    return dict(d)


# ---------- reads / reporting ----------


def total_minutes(guild_id: int, user_id: Optional[int] = None) -> int:
    """Total minutes from voice_minutes_day for a guild (or a single user)."""
    with connect() as con:
        cur = con.cursor()
        if user_id is None:
            row = cur.execute(
                "SELECT COALESCE(SUM(minutes),0) FROM voice_minutes_day WHERE guild_id=?",
                (guild_id,),
            ).fetchone()
        else:
            row = cur.execute(
                "SELECT COALESCE(SUM(minutes),0) FROM voice_minutes_day WHERE guild_id=? AND user_id=?",
                (guild_id, user_id),
            ).fetchone()
    return int(row[0] or 0)


def recent_user_days(
    guild_id: int, user_id: int, limit: int = 10
) -> List[Tuple[str, int]]:
    """
    Return [(YYYY-MM-DD, minutes)] newest-first for a user.
    """
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT day, minutes
            FROM voice_minutes_day
            WHERE guild_id=? AND user_id=?
            ORDER BY day DESC
            LIMIT ?
            """,
            (guild_id, user_id, max(1, limit)),
        ).fetchall()
    return [(d, int(m)) for (d, m) in rows]
