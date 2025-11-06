from __future__ import annotations

from typing import List, Optional, Tuple

from ..db import connect


def open_collection(guild_id: int, club_id: int, opens_at: str, closes_at: str) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO collections (guild_id, club_id, opens_at, closes_at, status) VALUES (?, ?, ?, ?, 'open')",
            (guild_id, club_id, opens_at, closes_at),
        )
        con.commit()
        return cur.lastrowid


def latest_collection(guild_id: int, club_id: int) -> Optional[Tuple[int, str, str, str]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT id, opens_at, closes_at, status
            FROM collections WHERE guild_id=? AND club_id=?
            ORDER BY id DESC LIMIT 1
            """,
            (guild_id, club_id),
        ).fetchone()


def close_collection_by_id(collection_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("UPDATE collections SET status='closed' WHERE id=?", (collection_id,))
        con.commit()


def add_submission(
    guild_id: int,
    club_id: int,
    collection_id: int,
    author_id: int,
    title: str,
    link: str,
    thread_id: int,
    created_at: str,
) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO submissions (guild_id, club_id, collection_id, author_id, title, link, thread_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (guild_id, club_id, collection_id, author_id, title, link, thread_id, created_at),
        )
        con.commit()
        return cur.lastrowid


def list_submissions_for_collection(collection_id: int) -> List[Tuple]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT id, title, link, author_id, thread_id, created_at
            FROM submissions WHERE collection_id=?
            ORDER BY id ASC
            """,
            (collection_id,),
        ).fetchall()


def get_submission(collection_id: int, ordinal: int) -> Optional[Tuple]:
    rows = list_submissions_for_collection(collection_id)
    return rows[ordinal - 1] if 1 <= ordinal <= len(rows) else None


def get_submissions_by_ordinals(collection_id: int, ordinals: List[int]) -> List[Tuple]:
    rows = list_submissions_for_collection(collection_id)
    out: List[Tuple] = []
    seen: set[int] = set()
    for o in ordinals:
        if 1 <= o <= len(rows) and o not in seen:
            out.append(rows[o - 1])
            seen.add(o)
    return out


__all__ = [
    "add_submission",
    "close_collection_by_id",
    "get_submission",
    "get_submissions_by_ordinals",
    "latest_collection",
    "list_submissions_for_collection",
    "open_collection",
]
