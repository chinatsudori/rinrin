from __future__ import annotations

from typing import List, Optional, Tuple

from ..db import connect


def create_series(
    guild_id: int,
    club_id: int,
    title: str,
    link: str,
    source_submission_id: int | None,
) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO series (guild_id, club_id, title, link, source_submission_id, status)
            VALUES (?, ?, ?, ?, ?, 'active')
            """,
            (guild_id, club_id, title, link or "", source_submission_id),
        )
        con.commit()
        return cur.lastrowid


def latest_active_series_for_guild(guild_id: int) -> Optional[Tuple[int, str, str]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT id, title, link
            FROM series
            WHERE guild_id=? AND status='active'
            ORDER BY id DESC LIMIT 1
            """,
            (guild_id,),
        ).fetchone()


def list_series(guild_id: int, club_id: int) -> List[Tuple[int, str, str, str]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT id, title, link, status
            FROM series WHERE guild_id=? AND club_id=?
            ORDER BY id DESC
            """,
            (guild_id, club_id),
        ).fetchall()


def get_series(series_id: int) -> Optional[Tuple[int, int, str, str, str]]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT id, guild_id, title, link, status FROM series WHERE id=?",
            (series_id,),
        ).fetchone()
        return row if row else None


def latest_active_series(guild_id: int, club_id: int) -> Optional[Tuple[int, str, str]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT id, title, link FROM series
            WHERE guild_id=? AND club_id=? AND status='active'
            ORDER BY id DESC LIMIT 1
            """,
            (guild_id, club_id),
        ).fetchone()


def add_discussion_section(
    series_id: int,
    label: str,
    start_ch: int,
    end_ch: int,
    start_iso: str,
    event_id: int | None,
) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO schedule_sections (series_id, label, start_chapter, end_chapter, discussion_event_id, discussion_start)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (series_id, label, start_ch, end_ch, event_id, start_iso),
        )
        con.commit()


def due_discussions(now_iso: str, limit: int = 25) -> List[Tuple]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT ss.id, ss.series_id, ss.label, ss.start_chapter, ss.end_chapter,
                   ss.discussion_start, ss.discussion_event_id, s.title, s.link
            FROM schedule_sections ss
            JOIN series s ON s.id = ss.series_id
            WHERE (ss.posted IS NULL OR ss.posted = 0)
              AND ss.discussion_start <= ?
            ORDER BY ss.id ASC
            LIMIT ?
            """,
            (now_iso, limit),
        ).fetchall()


def mark_discussion_posted(section_id: int, thread_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "UPDATE schedule_sections SET posted=1, discussion_thread_id=? WHERE id=?",
            (thread_id, section_id),
        )
        con.commit()


__all__ = [
    "add_discussion_section",
    "create_series",
    "due_discussions",
    "get_series",
    "latest_active_series",
    "latest_active_series_for_guild",
    "list_series",
    "mark_discussion_posted",
]
