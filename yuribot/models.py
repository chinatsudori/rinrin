from __future__ import annotations

import logging
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timezone
import sqlite3

from .db import connect

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Guild configuration
# -----------------------------------------------------------------------------

def upsert_guild_cfg(
    guild_id: int,
    ann: int,
    planning_forum: int,
    polls: int,
    discussion_forum: int | None = None,
) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO guild_config (guild_id, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
              announcements_channel_id=excluded.announcements_channel_id,
              planning_forum_id=excluded.planning_forum_id,
              polls_channel_id=excluded.polls_channel_id,
              discussion_forum_id=excluded.discussion_forum_id
            """,
            (guild_id, ann, planning_forum, polls, discussion_forum),
        )
        con.commit()
    log.debug("upsert_guild_cfg: guild=%s ann=%s planning=%s polls=%s discussion=%s",
              guild_id, ann, planning_forum, polls, discussion_forum)


def get_guild_cfg(guild_id: int) -> Optional[Dict[str, Optional[int]]]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id
            FROM guild_config WHERE guild_id=?
            """,
            (guild_id,),
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


# -----------------------------------------------------------------------------
# Collections & Submissions
# -----------------------------------------------------------------------------

def open_collection(guild_id: int, club_id: int, opens_at: str, closes_at: str) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO collections (guild_id, club_id, opens_at, closes_at, status) VALUES (?, ?, ?, ?, 'open')",
            (guild_id, club_id, opens_at, closes_at),
        )
        con.commit()
        cid = cur.lastrowid
    log.info("open_collection: guild=%s club=%s id=%s opens=%s closes=%s",
             guild_id, club_id, cid, opens_at, closes_at)
    return cid


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
    log.info("close_collection_by_id: id=%s", collection_id)


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
        sid = cur.lastrowid
    log.debug("add_submission: collection=%s submission_id=%s author=%s title=%r",
              collection_id, sid, author_id, title)
    return sid


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


# -----------------------------------------------------------------------------
# Polls
# -----------------------------------------------------------------------------

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
        pid = cur.lastrowid
    log.info("create_poll: id=%s guild=%s club=%s channel=%s closes_at=%s",
             pid, guild_id, club_id, channel_id, closes_at)
    return pid


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
        cur.execute(
            "UPDATE polls SET channel_id=?, message_id=? WHERE id=?",
            (channel_id, message_id, poll_id),
        )
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


def get_poll_channel_and_message(poll_id: int) -> Optional[Tuple[int, int, int]]:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT channel_id, message_id, guild_id FROM polls WHERE id=?",
            (poll_id,),
        ).fetchone()
        return row if row else None


# -----------------------------------------------------------------------------
# Series / Schedule
# -----------------------------------------------------------------------------

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
        sid = cur.lastrowid
    log.info("create_series: id=%s guild=%s club=%s title=%r", sid, guild_id, club_id, title)
    return sid


def latest_active_series_for_guild(guild_id: int) -> Optional[Tuple[int, str, str]]:
    """Variant that returns the most recent active series across all clubs in a guild."""
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
    """Most recent active series for a specific club in a guild."""
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


# -----------------------------------------------------------------------------
# Settings: mod/bot logs + welcome
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Emoji / Sticker stats
# -----------------------------------------------------------------------------

def bump_emoji_usage(
    guild_id: int,
    when_iso: str,
    emoji_key: str,
    emoji_name: str,
    is_custom: bool,
    via_reaction: bool,
    inc: int = 1,
) -> None:
    month = when_iso[:7]  # 'YYYY-MM'
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


def bump_sticker_usage(
    guild_id: int,
    when_iso: str,
    sticker_id: int,
    sticker_name: str,
    inc: int = 1,
) -> None:
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


# -----------------------------------------------------------------------------
# Member activity
# -----------------------------------------------------------------------------

def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def bump_member_message(
    guild_id: int,
    user_id: int,
    when_iso: str | None = None,
    inc: int = 1,
) -> None:
    when_iso = when_iso or _now_iso_utc()
    month = when_iso[:7]  # 'YYYY-MM'
    with connect() as con:
        cur = con.cursor()
        # monthly
        cur.execute(
            """
            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, month) DO UPDATE SET
              count = count + excluded.count
            """,
            (guild_id, user_id, month, inc),
        )
        # total
        cur.execute(
            """
            INSERT INTO member_activity_total (guild_id, user_id, count)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET
              count = count + excluded.count
            """,
            (guild_id, user_id, inc),
        )
        con.commit()


def top_members_month(guild_id: int, month: str, limit: int = 20) -> List[Tuple[int, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT user_id, count
            FROM member_activity_monthly
            WHERE guild_id=? AND month=?
            ORDER BY count DESC
            LIMIT ?
            """,
            (guild_id, month, limit),
        ).fetchall()


def top_members_total(guild_id: int, limit: int = 20) -> List[Tuple[int, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT user_id, count
            FROM member_activity_total
            WHERE guild_id=?
            ORDER BY count DESC
            LIMIT ?
            """,
            (guild_id, limit),
        ).fetchall()


def member_stats(guild_id: int, user_id: int) -> Tuple[int, List[Tuple[str, int]]]:
    """Return (total_count, [(month, count)...] sorted by month desc)."""
    with connect() as con:
        cur = con.cursor()
        total = cur.execute(
            """
            SELECT count FROM member_activity_total
            WHERE guild_id=? AND user_id=?
            """,
            (guild_id, user_id),
        ).fetchone()
        total_count = int(total[0]) if total else 0
        rows = cur.execute(
            """
            SELECT month, count
            FROM member_activity_monthly
            WHERE guild_id=? AND user_id=?
            ORDER BY month DESC
            """,
            (guild_id, user_id),
        ).fetchall()
        return total_count, rows


def reset_member_activity(guild_id: int, scope: str = "month", month: str | None = None) -> None:
    """Admin utility. scope: 'month' (requires month) or 'all'."""
    with connect() as con:
        cur = con.cursor()
        if scope == "month" and month:
            cur.execute(
                "DELETE FROM member_activity_monthly WHERE guild_id=? AND month=?",
                (guild_id, month),
            )
        elif scope == "all":
            cur.execute("DELETE FROM member_activity_monthly WHERE guild_id=?", (guild_id,))
            cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (guild_id,))
        con.commit()
    log.warning("reset_member_activity: guild=%s scope=%s month=%s", guild_id, scope, month)


# -----------------------------------------------------------------------------
# Mod actions (discipline)
# -----------------------------------------------------------------------------

def add_mod_action(
    guild_id: int,
    target_user_id: int,
    target_username: str,
    rule: str,
    offense: int,
    action: str,
    details: str | None,
    evidence_url: str | None,
    actor_user_id: int,
    created_at: str,
) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO mod_actions (guild_id, target_user_id, target_username, rule, offense, action, details, evidence_url, actor_user_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                target_user_id,
                target_username,
                rule,
                offense,
                action,
                details or "",
                evidence_url or "",
                actor_user_id,
                created_at,
            ),
        )
        con.commit()
        mid = cur.lastrowid
    log.info("add_mod_action: id=%s guild=%s user=%s action=%s rule=%r", mid, guild_id, target_user_id, action, rule)
    return mid


def list_mod_actions_for_user(
    guild_id: int,
    target_user_id: int,
    limit: int = 20,
) -> List[Tuple]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT id, rule, offense, action, details, evidence_url, actor_user_id, created_at
            FROM mod_actions
            WHERE guild_id=? AND target_user_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (guild_id, target_user_id, limit),
        ).fetchall()


# -----------------------------------------------------------------------------
# Movie Night (reuses club slots)
# -----------------------------------------------------------------------------

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
        mid = cur.lastrowid
    log.info("create_movie_events: id=%s guild=%s club=%s title=%r", mid, guild_id, club_id, title)
    return mid


def role_welcome_already_sent(guild_id: int, user_id: int, role_id: int) -> bool:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS role_welcome_sent (
                guild_id INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                sent_at  TEXT    NOT NULL,
                PRIMARY KEY (guild_id, user_id, role_id)
            )
        """)
        row = cur.execute("""
            SELECT 1 FROM role_welcome_sent
            WHERE guild_id=? AND user_id=? AND role_id=?
            LIMIT 1
        """, (guild_id, user_id, role_id)).fetchone()
        return bool(row)

def role_welcome_mark_sent(guild_id: int, user_id: int, role_id: int) -> None:
    when_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO role_welcome_sent
            (guild_id, user_id, role_id, sent_at)
            VALUES (?, ?, ?, ?)
        """, (guild_id, user_id, role_id, when_iso))
        con.commit()
