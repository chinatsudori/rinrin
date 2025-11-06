from __future__ import annotations

from ..db import connect
from .common import now_iso_utc as _now_iso_utc


def role_welcome_already_sent(guild_id: int, user_id: int, role_id: int) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS role_welcome_sent (
                guild_id INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                sent_at  TEXT    NOT NULL,
                PRIMARY KEY (guild_id, user_id, role_id)
            )
            """,
        )
        row = cur.execute(
            "SELECT 1 FROM role_welcome_sent WHERE guild_id=? AND user_id=? AND role_id=? LIMIT 1",
            (guild_id, user_id, role_id),
        ).fetchone()
        return bool(row)


def role_welcome_mark_sent(guild_id: int, user_id: int, role_id: int) -> None:
    when_iso = _now_iso_utc()
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO role_welcome_sent
            (guild_id, user_id, role_id, sent_at)
            VALUES (?, ?, ?, ?)
            """,
            (guild_id, user_id, role_id, when_iso),
        )
        con.commit()


__all__ = ["role_welcome_already_sent", "role_welcome_mark_sent"]
