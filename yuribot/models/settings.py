from __future__ import annotations

from typing import Dict, Optional

from ..db import connect


def set_mod_logs_channel(guild_id: int, channel_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO guild_settings (guild_id, mod_logs_channel_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET mod_logs_channel_id=excluded.mod_logs_channel_id
            """,
            (guild_id, channel_id),
        )
        con.commit()


def get_mod_logs_channel(guild_id: int) -> Optional[int]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT mod_logs_channel_id FROM guild_settings WHERE guild_id=?",
            (guild_id,),
        ).fetchone()
        return int(row[0]) if row else None


def set_bot_logs_channel(guild_id: int, channel_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO guild_settings (guild_id, bot_logs_channel_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET bot_logs_channel_id=excluded.bot_logs_channel_id
            """,
            (guild_id, channel_id),
        )
        con.commit()


def get_bot_logs_channel(guild_id: int) -> Optional[int]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT bot_logs_channel_id FROM guild_settings WHERE guild_id=?",
            (guild_id,),
        ).fetchone()
        return int(row[0]) if row else None


def set_welcome_settings(guild_id: int, channel_id: int, image_filename: str) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO guild_settings (guild_id, welcome_channel_id, welcome_image_filename)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
              welcome_channel_id=excluded.welcome_channel_id,
              welcome_image_filename=excluded.welcome_image_filename
            """,
            (guild_id, channel_id, image_filename),
        )
        con.commit()


def get_welcome_settings(guild_id: int) -> Optional[Dict[str, str | int]]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT welcome_channel_id, welcome_image_filename
            FROM guild_settings WHERE guild_id=?
            """,
            (guild_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "welcome_channel_id": row[0],
        "welcome_image_filename": row[1] or "welcome.png",
    }


def set_mu_forum_channel(guild_id: int, channel_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO guild_settings (guild_id, mu_forum_channel_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET mu_forum_channel_id=excluded.mu_forum_channel_id
            """,
            (guild_id, channel_id),
        )
        con.commit()


def get_mu_forum_channel(guild_id: int) -> int | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT mu_forum_channel_id FROM guild_settings WHERE guild_id=?",
            (guild_id,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None


__all__ = [
    "get_bot_logs_channel",
    "get_mod_logs_channel",
    "get_mu_forum_channel",
    "get_welcome_settings",
    "set_bot_logs_channel",
    "set_mod_logs_channel",
    "set_mu_forum_channel",
    "set_welcome_settings",
]


# --- Added by automation: generic per-guild settings helpers ---
import sqlite3

def _conn():
    # Simple local import to avoid circulars
    import os
    from pathlib import Path
    try:
        from .. import db as _db
    except Exception:
        from yuribot import db as _db  # fallback if package name is yuribot
    return _db.connect()

def ensure_table():
    with _conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT,
            PRIMARY KEY (guild_id, key)
        )
        """)
        c.commit()

def get_guild_setting(guild_id: int, key: str, default=None):
    ensure_table()
    with _conn() as c:
        cur = c.execute("SELECT value FROM guild_settings WHERE guild_id=? AND key=?", (int(guild_id), str(key)))
        row = cur.fetchone()
        return row[0] if row else default

def set_guild_setting(guild_id: int, key: str, value: str | None):
    ensure_table()
    with _conn() as c:
        c.execute("INSERT INTO guild_settings (guild_id, key, value) VALUES (?, ?, ?) ON CONFLICT(guild_id, key) DO UPDATE SET value=excluded.value",
                  (int(guild_id), str(key), None if value is None else str(value)))
        c.commit()

def get_channel_id(guild_id: int, key: str, fallback_id: int | None = None) -> int | None:
    v = get_guild_setting(guild_id, key)
    if v is not None:
        try:
            return int(v)
        except ValueError:
            return fallback_id
    return fallback_id

