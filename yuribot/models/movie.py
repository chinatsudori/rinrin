from __future__ import annotations

from ..db import connect
from .guilds import get_club_cfg, upsert_club_cfg


def get_movie_cfg(guild_id: int):
    return get_club_cfg(guild_id, "movie")


def set_movie_cfg(guild_id: int, announcements_id: int, projection_channel_id: int, polls_id: int) -> int:
    return upsert_club_cfg(
        guild_id,
        "movie",
        ann=announcements_id,
        planning=projection_channel_id,  # reuse planning_forum_id column for projection channel
        polls=polls_id,
        discussion=0,
    )


def create_movie_events(
    guild_id: int,
    club_id: int,
    title: str,
    link: str | None,
    show_date_iso: str,
    event_id_morning: int | None,
    event_id_evening: int | None,
) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO movie_events (guild_id, club_id, title, link, show_date, event_id_morning, event_id_evening)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (guild_id, club_id, title, link or "", show_date_iso, event_id_morning, event_id_evening),
        )
        con.commit()
        return cur.lastrowid


__all__ = ["create_movie_events", "get_movie_cfg", "set_movie_cfg"]
