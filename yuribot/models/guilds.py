from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

from ..db import connect

log = logging.getLogger(__name__)

_ASSET_ROOT = Path(__file__).resolve().parents[1] / "data" / "club_assets"

_GUILD_CFG_TYPE = "__guild__"


def upsert_guild_cfg(
    guild_id: int,
    ann: int,
    planning_forum: int,
    polls: int,
    discussion_forum: int | None = None,
) -> None:
    """Back-compat ‘guild config’ stored in the clubs table."""
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO clubs (guild_id, club_type, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, club_type) DO UPDATE SET
              announcements_channel_id=excluded.announcements_channel_id,
              planning_forum_id=excluded.planning_forum_id,
              polls_channel_id=excluded.polls_channel_id,
              discussion_forum_id=excluded.discussion_forum_id
            """,
            (guild_id, _GUILD_CFG_TYPE, ann, planning_forum, polls, discussion_forum),
        )
        con.commit()
    log.debug(
        "upsert_guild_cfg: guild=%s ann=%s planning=%s polls=%s discussion=%s",
        guild_id,
        ann,
        planning_forum,
        polls,
        discussion_forum,
    )


def get_guild_cfg(guild_id: int) -> Optional[Dict[str, Optional[int]]]:
    """Fetch the reserved clubs row for this guild if present."""
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id
            FROM clubs
            WHERE guild_id=? AND club_type=?
            """,
            (guild_id, _GUILD_CFG_TYPE),
        ).fetchone()
    if not row:
        return None
    return {
        "announcements_channel_id": row[0],
        "planning_forum_id": row[1],
        "polls_channel_id": row[2],
        "discussion_forum_id": row[3],
    }


def upsert_club_cfg(
    guild_id: int,
    club_type: str,
    ann: int,
    planning: int,
    polls: int,
    discussion: int,
) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO clubs (guild_id, club_type, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, club_type) DO UPDATE SET
              announcements_channel_id=excluded.announcements_channel_id,
              planning_forum_id=excluded.planning_forum_id,
              polls_channel_id=excluded.polls_channel_id,
              discussion_forum_id=excluded.discussion_forum_id
            """,
            (guild_id, club_type, ann, planning, polls, discussion),
        )
        con.commit()
        row = cur.execute(
            "SELECT id FROM clubs WHERE guild_id=? AND club_type=?",
            (guild_id, club_type),
        ).fetchone()
    cid = int(row[0])
    log.debug("upsert_club_cfg: guild=%s club=%s -> id=%s", guild_id, club_type, cid)
    return cid


def get_club_cfg(guild_id: int, club_type: str) -> Optional[Dict[str, int]]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT id, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id
            FROM clubs WHERE guild_id=? AND club_type=?
            """,
            (guild_id, club_type),
        ).fetchone()
    if not row:
        return None
    return {
        "club_id": row[0],
        "announcements_channel_id": row[1],
        "planning_forum_id": row[2],
        "polls_channel_id": row[3],
        "discussion_forum_id": row[4],
    }


def get_club_by_planning_forum(guild_id: int, forum_id: int) -> Optional[Tuple[int, str]]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT id, club_type FROM clubs
            WHERE guild_id=? AND planning_forum_id=?
            """,
            (guild_id, forum_id),
        ).fetchone()
        return row if row else None


def get_club_map(guild_id: int) -> Dict[str, Dict[str, int | str | None]]:
    """Return mapping of club slug to stored configuration details."""
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT club_type, id, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id
            FROM clubs
            WHERE guild_id=?
            ORDER BY club_type ASC
            """,
            (guild_id,),
        ).fetchall()

    asset_links: Dict[str, Dict[str, str]] = {}
    link_path = _ASSET_ROOT / str(guild_id) / "links.json"
    if link_path.exists():
        try:
            raw = json.loads(link_path.read_text())
            if isinstance(raw, dict):
                asset_links = {str(k): v for k, v in raw.items() if isinstance(v, dict)}
        except Exception:
            asset_links = {}

    result: Dict[str, Dict[str, int | str | None]] = {}
    for club_type, cid, ann, planning, polls, discussion in rows:
        slug = str(club_type)
        entry: Dict[str, int | str | None] = {
            "club_id": int(cid),
            "announcements_channel_id": ann,
            "planning_forum_id": planning,
            "polls_channel_id": polls,
            "discussion_forum_id": discussion,
        }
        if slug in asset_links:
            entry.update({k: asset_links[slug].get(k) for k in ("link", "image")})
        result[slug] = entry
    return result


def store_club_link(guild_id: int, club_slug: str, url: str) -> None:
    base = _ASSET_ROOT / str(guild_id)
    base.mkdir(parents=True, exist_ok=True)
    link_path = base / "links.json"
    try:
        data = json.loads(link_path.read_text()) if link_path.exists() else {}
    except Exception:
        data = {}
    entry = data.get(club_slug, {}) if isinstance(data, dict) else {}
    if not isinstance(entry, dict):
        entry = {}
    entry["link"] = url
    data[str(club_slug)] = entry
    link_path.write_text(json.dumps(data, indent=2, sort_keys=True))


def store_club_image(guild_id: int, club_slug: str, filename: str, data: bytes) -> None:
    base = _ASSET_ROOT / str(guild_id) / str(club_slug)
    base.mkdir(parents=True, exist_ok=True)
    (base / filename).write_bytes(data)

    link_path = _ASSET_ROOT / str(guild_id) / "links.json"
    try:
        meta = json.loads(link_path.read_text()) if link_path.exists() else {}
    except Exception:
        meta = {}
    entry = meta.get(club_slug, {}) if isinstance(meta, dict) else {}
    if not isinstance(entry, dict):
        entry = {}
    entry["image"] = filename
    meta[str(club_slug)] = entry
    link_path.write_text(json.dumps(meta, indent=2, sort_keys=True))


__all__ = [
    "get_club_map",
    "get_club_by_planning_forum",
    "get_club_cfg",
    "get_guild_cfg",
    "store_club_image",
    "store_club_link",
    "upsert_club_cfg",
    "upsert_guild_cfg",
]