from __future__ import annotations

import sqlite3

from typing import Iterable, List, Tuple

from ..db import connect
from .common import now_iso_utc as _now_iso_utc

def mu_register_thread_series(guild_id: int, thread_id: int, series_id: str, series_title: str) -> None:
    """Associate a forum thread with an MU series; upsert series title."""
    now = _now_iso_utc()
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO mu_series (series_id, title, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(series_id) DO UPDATE SET title=excluded.title
            """,
            (str(series_id), series_title, now),
        )
        cur.execute(
            """
            INSERT INTO mu_thread_series (guild_id, thread_id, series_id)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, thread_id) DO UPDATE SET series_id=excluded.series_id
            """,
            (guild_id, thread_id, str(series_id)),
        )
        con.commit()

def mu_get_thread_series(thread_id: int, guild_id: int | None = None) -> str | None:
    with connect() as con:
        cur = con.cursor()
        if guild_id is None:
            row = cur.execute("SELECT series_id FROM mu_thread_series WHERE thread_id=?", (thread_id,)).fetchone()
        else:
            row = cur.execute(
                "SELECT series_id FROM mu_thread_series WHERE guild_id=? AND thread_id=?",
                (guild_id, thread_id),
            ).fetchone()
        return row[0] if row else None

def mu_upsert_release(
    series_id: str,
    release_id: int,
    *,
    title: str = "",
    raw_title: str = "",
    description: str = "",
    volume: str = "",
    chapter: str = "",
    subchapter: str = "",
    group_name: str = "",
    url: str = "",
    release_ts: int = -1,
) -> bool:
    """Insert or ignore an MU release row. Returns True if inserted, False if existed."""
    now = _now_iso_utc()
    with connect() as con:
        cur = con.cursor()
        try:
            cur.execute(
                """
                INSERT INTO mu_releases
                    (series_id, release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(series_id),
                    int(release_id),
                    title,
                    raw_title,
                    description,
                    volume,
                    chapter,
                    subchapter,
                    group_name,
                    url,
                    int(release_ts),
                    now,
                ),
            )
            con.commit()
            return True
        except sqlite3.IntegrityError:
            return False

def mu_bulk_upsert_releases(series_id: str, items: list[dict]) -> list[int]:
    """Upsert many releases; return newly inserted release_ids (ascending by release_ts)."""
    inserted: list[tuple[int, int]] = []
    for r in items:
        rid = int(r.get("release_id") or r.get("id"))
        ok = mu_upsert_release(
            series_id=series_id,
            release_id=rid,
            title=str(r.get("title") or ""),
            raw_title=str(r.get("raw_title") or ""),
            description=str(r.get("description") or ""),
            volume=str(r.get("volume") or ""),
            chapter=str(r.get("chapter") or ""),
            subchapter=str(r.get("subchapter") or ""),
            group_name=str(r.get("group") or r.get("group_name") or ""),
            url=str(r.get("url") or r.get("release_url") or r.get("link") or ""),
            release_ts=int(r.get("release_ts") if r.get("release_ts") is not None else -1),
        )
        if ok:
            ts = int(r.get("release_ts") if r.get("release_ts") is not None else -1)
            inserted.append((ts, rid))
    inserted.sort(key=lambda x: x[0])  # oldest → newest
    return [rid for _, rid in inserted]

def mu_latest_release_ts(series_id: str) -> int:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("SELECT COALESCE(MAX(release_ts), -1) FROM mu_releases WHERE series_id=?", (str(series_id),)).fetchone()
        return int(row[0] if row and row[0] is not None else -1)

def mu_list_unposted_for_thread(guild_id: int, thread_id: int, series_id: str, *, english_only: bool = False) -> list[tuple]:
    """
    Return releases NOT yet posted in thread (ordered by release_ts asc, unknowns first).
    Columns: (release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts)
    """
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT r.release_id, r.title, r.raw_title, r.description, r.volume, r.chapter, r.subchapter,
                   r.group_name, r.url, r.release_ts
            FROM mu_releases r
            LEFT JOIN mu_thread_posts tp
              ON tp.guild_id=? AND tp.thread_id=? AND tp.series_id=r.series_id AND tp.release_id=r.release_id
            WHERE r.series_id=? AND tp.release_id IS NULL
            ORDER BY r.release_ts ASC, r.release_id ASC
            """,
            (guild_id, thread_id, str(series_id)),
        ).fetchall()

        if not english_only:
            return rows

        def _is_en(title, raw, desc):
            txt = f"{title or ''} {raw or ''} {desc or ''}".lower()
            return any(k in txt for k in ("eng", "english", "[en]", "(en)"))

        return [r for r in rows if _is_en(r[1], r[2], r[3])]

def mu_mark_posted(guild_id: int, thread_id: int, series_id: str, release_id: int, when_iso: str | None = None) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO mu_thread_posts (guild_id, thread_id, series_id, release_id, posted_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, thread_id, release_id) DO NOTHING
            """,
            (guild_id, thread_id, str(series_id), int(release_id), when_iso or _now_iso_utc()),
        )
        con.commit()

def mu_get_release(series_id: str, release_id: int) -> dict | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts
            FROM mu_releases WHERE series_id=? AND release_id=?
            """,
            (str(series_id), int(release_id)),
        ).fetchone()
        if not row:
            return None
        return {
            "title": row[0],
            "raw_title": row[1],
            "description": row[2],
            "volume": row[3],
            "chapter": row[4],
            "subchapter": row[5],
            "group": row[6],
            "url": row[7],
            "release_ts": row[8],
            "release_id": int(release_id),
            "series_id": str(series_id),
        }

def mu_list_links_for_guild(guild_id: int) -> list[tuple[int, str, str]]:
    """Returns [(thread_id, series_id, series_title)] newest threads first when possible."""
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT ts.thread_id, ts.series_id, COALESCE(s.title, '')
            FROM mu_thread_series ts
            LEFT JOIN mu_series s ON s.series_id = ts.series_id
            WHERE ts.guild_id=?
            ORDER BY ts.thread_id DESC
            """,
            (guild_id,),
        ).fetchall()
        return [(int(r[0]), str(r[1]), str(r[2])) for r in rows]

__all__ = ["mu_register_thread_series", "mu_get_thread_series", "mu_upsert_release", "mu_bulk_upsert_releases", "mu_latest_release_ts", "mu_list_unposted_for_thread", "mu_mark_posted", "mu_get_release", "mu_list_links_for_guild"]