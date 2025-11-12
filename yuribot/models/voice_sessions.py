from __future__ import annotations

import logging
from datetime import datetime
from ..db import connect
from typing import Optional

log = logging.getLogger(__name__)

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS voice_sessions (
    session_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id            INTEGER NOT NULL,
    user_id             INTEGER NOT NULL,
    channel_id          INTEGER NOT NULL,
    join_time           TEXT NOT NULL,
    leave_time          TEXT,
    duration_seconds    INTEGER,
    -- For backfilled data
    join_message_id     INTEGER UNIQUE,
    leave_message_id    INTEGER UNIQUE
)
"""
INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_voice_sessions_user ON
voice_sessions (guild_id, user_id, join_time)
"""


def ensure_table() -> None:
    """Creates the voice_sessions table and index if they don't exist."""
    with connect() as con:
        con.execute(TABLE_SQL)
        con.execute(INDEX_SQL)
        con.commit()


def open_live_session(
    guild_id: int, user_id: int, channel_id: int, join_time: datetime
) -> int:
    """
    Logs the start of a new, live session.
    Returns the new session_id.
    """
    with connect() as con:
        cur = con.execute(
            """
            INSERT INTO voice_sessions (
                guild_id, user_id, channel_id, join_time
            ) VALUES (?, ?, ?, ?)
            """,
            (guild_id, user_id, channel_id, join_time.isoformat()),
        )
        con.commit()
        if cur.lastrowid is None:
            raise RuntimeError("Failed to get lastrowid for new voice session")
        return cur.lastrowid


def close_live_session(
    session_id: int, leave_time: datetime, duration_seconds: int
) -> None:
    """Closes an open, live session by updating its leave time and duration."""
    with connect() as con:
        con.execute(
            """
            UPDATE voice_sessions
            SET leave_time = ?, duration_seconds = ?
            WHERE session_id = ?
            """,
            (leave_time.isoformat(), duration_seconds, session_id),
        )
        con.commit()


def upsert_backfilled_session(
    guild_id: int,
    user_id: int,
    channel_id: int,
    join_message_id: int,
    join_time: str,
    leave_message_id: int,
    leave_time: str,
    duration_seconds: int,
) -> None:
    """Inserts or updates a session from the backfill command."""
    with connect() as con:
        con.execute(
            """
            INSERT INTO voice_sessions (
                guild_id, user_id, channel_id, join_time, leave_time,
                duration_seconds, join_message_id, leave_message_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(join_message_id) DO UPDATE SET
                leave_time=excluded.leave_time,
                leave_message_id=excluded.leave_message_id,
                duration_seconds=excluded.duration_seconds
            """,
            (
                guild_id,
                user_id,
                channel_id,
                join_time,
                leave_time,
                duration_seconds,
                join_message_id,
                leave_message_id,
            ),
        )
        con.commit()


def get_last_processed_log_id(guild_id: int) -> Optional[int]:
    """
    Finds the newest botlog message ID (join or leave) we have
    processed for this guild, to allow resumable backfills.
    """
    with connect() as con:
        row = con.execute(
            """
            SELECT MAX(MAX(join_message_id), MAX(leave_message_id))
            FROM voice_sessions
            WHERE guild_id = ?
            """,
            (guild_id,),
        ).fetchone()
    # row[0] will be None if the table is empty for this guild
    return row[0] if row and row[0] is not None else None
