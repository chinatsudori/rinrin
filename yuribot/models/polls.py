from __future__ import annotations

from typing import List, Tuple

from ..db import connect


def create_poll(
    guild_id: int,
    club_id: int,
    channel_id: int,
    created_at: str,
    closes_at: str | None,
) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO polls (guild_id, club_id, channel_id, created_at, closes_at, status)
            VALUES (?, ?, ?, ?, ?, 'open')
            """,
            (guild_id, club_id, channel_id, created_at, closes_at),
        )
        con.commit()
        return cur.lastrowid


def add_poll_option(poll_id: int, label: str, submission_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO poll_options (poll_id, label, submission_id) VALUES (?, ?, ?)",
            (poll_id, label, submission_id),
        )
        con.commit()


def set_poll_message(poll_id: int, channel_id: int, message_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("UPDATE polls SET channel_id=?, message_id=? WHERE id=?", (channel_id, message_id, poll_id))
        con.commit()


def record_vote(poll_id: int, user_id: int, option_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO poll_votes (poll_id, user_id, option_id)
            VALUES (?, ?, ?)
            ON CONFLICT(poll_id, user_id) DO UPDATE SET option_id=excluded.option_id
            """,
            (poll_id, user_id, option_id),
        )
        con.commit()


def tally_poll(poll_id: int) -> List[Tuple[int, str, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT o.id, o.label, COUNT(v.user_id) as c
            FROM poll_options o
            LEFT JOIN poll_votes v ON v.option_id=o.id
            WHERE o.poll_id=?
            GROUP BY o.id, o.label
            ORDER BY c DESC, o.id ASC
            """,
            (poll_id,),
        ).fetchall()


def close_poll(poll_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("UPDATE polls SET status='closed' WHERE id=?", (poll_id,))
        con.commit()


def get_poll_channel_and_message(poll_id: int) -> Tuple[int, int, int] | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("SELECT channel_id, message_id, guild_id FROM polls WHERE id=?", (poll_id,)).fetchone()
        return row if row else None


__all__ = [
    "add_poll_option",
    "close_poll",
    "create_poll",
    "get_poll_channel_and_message",
    "record_vote",
    "set_poll_message",
    "tally_poll",
]