from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from ..db import connect as db_connect

# --- Schema ---

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS birthdays (
    guild_id            INTEGER NOT NULL,
    user_id             INTEGER NOT NULL,
    month               INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day                 INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    tz                  TEXT NOT NULL,
    last_congrats_year  INTEGER,
    closeness_level     INTEGER,  -- 1..5; NULL -> default handling
    PRIMARY KEY (guild_id, user_id)
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_birthdays_guild_month_day ON birthdays (guild_id, month, day);
"""

# Backfill/migration: add closeness_level if old table exists without it
ALTERS = [
    ("PRAGMA table_info(birthdays)", "closeness_level"),
    ("ALTER TABLE birthdays ADD COLUMN closeness_level INTEGER", None),
]


@dataclass
class Birthday:
    guild_id: int
    user_id: int
    month: int
    day: int
    tz: str
    last_year: Optional[int]
    closeness_level: Optional[int]  # 1..5 or None


# --- DDL ---


def ensure_tables() -> None:
    with db_connect() as con:
        con.execute(TABLE_SQL)
        con.execute(INDEX_SQL)
        # conditional add column
        try:
            cols = {row[1] for row in con.execute(ALTERS[0][0]).fetchall()}
            if ALTERS[0][1] not in cols:
                con.execute(ALTERS[1][0])
        except Exception:
            pass
        con.commit()


# --- CRUD ---


def upsert_birthday(guild_id: int, user_id: int, month: int, day: int, tz: str) -> None:
    with db_connect() as con:
        con.execute(
            "INSERT INTO birthdays (guild_id, user_id, month, day, tz) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(guild_id, user_id) DO UPDATE "
            "SET month=excluded.month, day=excluded.day, tz=excluded.tz",
            (guild_id, user_id, month, day, tz),
        )
        con.commit()


def update_birthday(
    guild_id: int,
    user_id: int,
    month: Optional[int] = None,
    day: Optional[int] = None,
    tz: Optional[str] = None,
) -> bool:
    """Partial update. Returns True if a row was affected."""
    sets = []
    params: List[object] = []
    if month is not None:
        sets.append("month=?")
        params.append(month)
    if day is not None:
        sets.append("day=?")
        params.append(day)
    if tz is not None:
        sets.append("tz=?")
        params.append(tz)
    if not sets:
        return False
    params.extend([guild_id, user_id])

    with db_connect() as con:
        cur = con.execute(
            f"UPDATE birthdays SET {', '.join(sets)} WHERE guild_id=? AND user_id=?",
            params,
        )
        con.commit()
        return cur.rowcount > 0


def get_birthday(guild_id: int, user_id: int) -> Optional[Birthday]:
    with db_connect() as con:
        row = con.execute(
            "SELECT guild_id, user_id, month, day, tz, last_congrats_year, closeness_level "
            "FROM birthdays WHERE guild_id=? AND user_id=?",
            (guild_id, user_id),
        ).fetchone()
    if not row:
        return None
    return Birthday(*row)


def delete_birthday(guild_id: int, user_id: int) -> bool:
    with db_connect() as con:
        cur = con.execute(
            "DELETE FROM birthdays WHERE guild_id=? AND user_id=?", (guild_id, user_id)
        )
        con.commit()
        return cur.rowcount > 0


def fetch_all_for_guild(guild_id: int) -> List[Birthday]:
    with db_connect() as con:
        rows = con.execute(
            "SELECT guild_id, user_id, month, day, tz, last_congrats_year, closeness_level "
            "FROM birthdays WHERE guild_id=? ORDER BY month, day, user_id",
            (guild_id,),
        ).fetchall()
    return [Birthday(*r) for r in rows]


def fetch_for_user(guild_id: int, user_id: int) -> List[Birthday]:
    b = get_birthday(guild_id, user_id)
    return [b] if b else []


def mark_congratulated(guild_id: int, user_id: int, year: int) -> None:
    with db_connect() as con:
        con.execute(
            "UPDATE birthdays SET last_congrats_year=? WHERE guild_id=? AND user_id=?",
            (year, guild_id, user_id),
        )
        con.commit()


def set_closeness(guild_id: int, user_id: int, level: Optional[int]) -> None:
    """Store 1..5 or NULL to reset to default selection behavior."""
    with db_connect() as con:
        con.execute(
            "UPDATE birthdays SET closeness_level=? WHERE guild_id=? AND user_id=?",
            (level, guild_id, user_id),
        )
        con.commit()
