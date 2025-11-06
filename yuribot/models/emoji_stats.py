from __future__ import annotations

from typing import List, Tuple

from ..db import connect


def bump_emoji_usage(
    guild_id: int,
    when_iso: str,
    emoji_key: str,
    emoji_name: str,
    is_custom: bool,
    via_reaction: bool,
    inc: int = 1,
) -> None:
    month = when_iso[:7]
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO emoji_usage_monthly
                (guild_id, month, emoji_key, emoji_name, is_custom, via_reaction, count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, month, emoji_key, via_reaction)
            DO UPDATE SET count = count + excluded.count
            """,
            (guild_id, month, emoji_key, emoji_name, 1 if is_custom else 0, 1 if via_reaction else 0, inc),
        )
        con.commit()


def top_emojis(guild_id: int, month: str, limit: int = 20) -> List[Tuple[str, str, int, int, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT emoji_key, emoji_name, is_custom, via_reaction, count
            FROM emoji_usage_monthly
            WHERE guild_id=? AND month=?
            ORDER BY count DESC
            LIMIT ?
            """,
            (guild_id, month, limit),
        ).fetchall()


def bump_sticker_usage(guild_id: int, when_iso: str, sticker_id: int, sticker_name: str, inc: int = 1) -> None:
    month = when_iso[:7]
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO sticker_usage_monthly
                (guild_id, month, sticker_id, sticker_name, count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, month, sticker_id)
            DO UPDATE SET count = count + excluded.count
            """,
            (guild_id, month, sticker_id, sticker_name, inc),
        )
        con.commit()


def top_stickers(guild_id: int, month: str, limit: int = 20) -> List[Tuple[int, str, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT sticker_id, sticker_name, count
            FROM sticker_usage_monthly
            WHERE guild_id=? AND month=?
            ORDER BY count DESC
            LIMIT ?
            """,
            (guild_id, month, limit),
        ).fetchall()


def bump_gif_usage(guild_id: int, when_iso: str, gif_key: str, source: str, inc: int = 1) -> None:
    month = when_iso[:7]
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gif_usage_monthly (
                guild_id INTEGER NOT NULL,
                month    TEXT    NOT NULL,
                gif_key  TEXT    NOT NULL,
                source   TEXT    NOT NULL,
                count    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, month, gif_key)
            )
            """,
        )
        cur.execute(
            """
            INSERT INTO gif_usage_monthly (guild_id, month, gif_key, source, count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, month, gif_key)
            DO UPDATE SET count = count + excluded.count
            """,
            (guild_id, month, gif_key[:512], source[:32], inc),
        )
        con.commit()


def top_gifs(guild_id: int, month: str, limit: int = 20) -> List[Tuple[str, str, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT gif_key, source, count
            FROM gif_usage_monthly
            WHERE guild_id=? AND month=?
            ORDER BY count DESC
            LIMIT ?
            """,
            (guild_id, month, limit),
        ).fetchall()


__all__ = [
    "bump_emoji_usage",
    "bump_gif_usage",
    "bump_sticker_usage",
    "top_emojis",
    "top_gifs",
    "top_stickers",
]
