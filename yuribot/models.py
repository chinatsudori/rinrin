from __future__ import annotations

import logging
import math
import sqlite3
from typing import Optional, List, Tuple, Dict, Iterable
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .db import connect

log = logging.getLogger(__name__)

# =============================================================================
# Internal time helpers
# =============================================================================

def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _iso_parts(when_iso: str) -> tuple[str, str, str, int]:
    """
    Return (day, week_key, month, hour_utc) from ISO timestamp.
    week_key = 'YYYY-Www' using ISO calendar.
    """
    dt = datetime.fromisoformat(when_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    y, w, _ = dt.isocalendar()  # ISO year/week
    day = dt.strftime("%Y-%m-%d")
    month = dt.strftime("%Y-%m")
    week_key = f"{y}-W{int(w):02d}"
    return day, week_key, month, dt.hour

def _upsert_metric_daily_and_total(
    con: sqlite3.Connection,
    guild_id: int,
    user_id: int,
    metric: str,
    day: str,
    week_key: str,
    month: str,
    inc: int,
) -> None:
    cur = con.cursor()
    cur.execute("""
        INSERT INTO member_metrics_daily (guild_id, user_id, metric, day, week, month, count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, metric, day) DO UPDATE SET
          count = count + excluded.count
    """, (guild_id, user_id, metric, day, week_key, month, inc))
    cur.execute("""
        INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, metric) DO UPDATE SET
          count = count + excluded.count
    """, (guild_id, user_id, metric, inc))

def _bump_hour_hist(
    con: sqlite3.Connection,
    guild_id: int,
    user_id: int,
    metric: str,
    hour_utc: int,
    inc: int = 1
) -> None:
    cur = con.cursor()
    cur.execute("""
        INSERT INTO member_hour_hist (guild_id, user_id, metric, hour_utc, count)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, metric, hour_utc) DO UPDATE SET
          count = count + excluded.count
    """, (guild_id, user_id, metric, hour_utc, inc))

# =============================================================================
# Guild & Club Configuration
# =============================================================================

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

# =============================================================================
# Collections & Submissions
# =============================================================================

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

# =============================================================================
# Polls
# =============================================================================

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

# =============================================================================
# Series / Schedule
# =============================================================================

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

# =============================================================================
# Settings: mod/bot logs + welcome
# =============================================================================

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
        cur.execute("""
            INSERT INTO guild_settings (guild_id, mu_forum_channel_id)
            VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET mu_forum_channel_id=excluded.mu_forum_channel_id
        """, (guild_id, channel_id))
        con.commit()

def get_mu_forum_channel(guild_id: int) -> int | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("""
            SELECT mu_forum_channel_id FROM guild_settings WHERE guild_id=?
        """, (guild_id,)).fetchone()
        return int(row[0]) if row and row[0] is not None else None

# =============================================================================
# Emoji / Sticker stats
# =============================================================================

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

# =============================================================================
# Member activity: unified metrics (messages/words/mentions/emoji...) + legacy mirrors
# =============================================================================

def bump_member_message(
    guild_id: int,
    user_id: int,
    when_iso: str | None = None,
    inc: int = 1,
) -> None:
    when_iso = when_iso or _now_iso_utc()
    day, week_key, month, hour_utc = _iso_parts(when_iso)
    with connect() as con:
        cur = con.cursor()
        # legacy monthly
        cur.execute(
            """
            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, month) DO UPDATE SET
              count = count + excluded.count
            """,
            (guild_id, user_id, month, inc),
        )
        # legacy total
        cur.execute(
            """
            INSERT INTO member_activity_total (guild_id, user_id, count)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET
              count = count + excluded.count
            """,
            (guild_id, user_id, inc),
        )
        # unified metrics
        _upsert_metric_daily_and_total(con, guild_id, user_id, "messages", day, week_key, month, inc)
        _bump_hour_hist(con, guild_id, user_id, "messages", hour_utc, inc)
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

def reset_member_activity(guild_id: int, scope: str = "month", key: str | None = None) -> None:
    """Extended: resets unified 'messages' too when appropriate."""
    with connect() as con:
        cur = con.cursor()
        if scope == "all":
            cur.execute("DELETE FROM member_activity_monthly WHERE guild_id=?", (guild_id,))
            cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (guild_id,))
            cur.execute("DELETE FROM member_metrics_daily WHERE guild_id=? AND metric='messages'", (guild_id,))
            cur.execute("DELETE FROM member_metrics_total WHERE guild_id=? AND metric='messages'", (guild_id,))
            cur.execute("DELETE FROM member_hour_hist WHERE guild_id=? AND metric='messages'", (guild_id,))
        elif scope in ("day","week","month") and key:
            cur.execute(f"DELETE FROM member_metrics_daily WHERE guild_id=? AND metric='messages' AND {scope}=?", (guild_id, key))
            if scope == "month":
                cur.execute("DELETE FROM member_activity_monthly WHERE guild_id=? AND month=?", (guild_id, key))
            cur.execute("DELETE FROM member_metrics_total WHERE guild_id=? AND metric='messages'", (guild_id,))
            cur.execute("""
                INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
                SELECT guild_id, user_id, 'messages', SUM(count)
                FROM member_metrics_daily
                WHERE guild_id=? AND metric='messages'
                GROUP BY guild_id, user_id
            """, (guild_id,))
            cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (guild_id,))
            cur.execute("""
                INSERT INTO member_activity_total (guild_id, user_id, count)
                SELECT guild_id, user_id, SUM(count)
                FROM member_activity_monthly
                WHERE guild_id=?
                GROUP BY guild_id, user_id
            """, (guild_id,))
        else:
            raise ValueError("bad_scope_or_key")
        con.commit()

# ---- Unified bumpers for other metrics ----

def bump_member_words(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, hour_utc = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "words", day, week_key, month, inc)
        con.commit()

def bump_member_mentioned(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, hour_utc = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "mentions", day, week_key, month, inc)
        con.commit()

def bump_member_mentions_sent(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, hour_utc = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "mentions_sent", day, week_key, month, inc)
        con.commit()

def bump_member_emoji_chat(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, hour_utc = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "emoji_chat", day, week_key, month, inc)
        con.commit()

def bump_member_emoji_react(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, hour_utc = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "emoji_react", day, week_key, month, inc)
        con.commit()

def bump_reactions_received(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, hour_utc = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "reactions_received", day, week_key, month, inc)
        con.commit()

def bump_voice_minutes(guild_id: int, user_id: int, when_iso: str, minutes: int, stream_minutes: int = 0) -> None:
    if minutes <= 0 and stream_minutes <= 0:
        return
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        if minutes > 0:
            _upsert_metric_daily_and_total(con, guild_id, user_id, "voice_minutes", day, week_key, month, int(minutes))
        if stream_minutes > 0:
            _upsert_metric_daily_and_total(con, guild_id, user_id, "voice_stream_minutes", day, week_key, month, int(stream_minutes))
        con.commit()

def bump_activity_minutes(guild_id: int, user_id: int, when_iso: str, app_name: str, minutes: int, launches: int = 0) -> None:
    if minutes <= 0 and launches <= 0:
        return
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "activity_minutes", day, week_key, month, int(minutes))
        cur = con.cursor()
        cur.execute("""
            INSERT INTO member_activity_apps_daily (guild_id, user_id, app_name, day, minutes, launches)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, app_name, day) DO UPDATE SET
              minutes = minutes + excluded.minutes,
              launches = launches + excluded.launches
        """, (guild_id, user_id, app_name or "(unknown)", day, int(minutes), int(launches)))
        con.commit()

# ---- Channel totals (for “prime channel”) ----

def bump_channel_message_total(guild_id: int, user_id: int, channel_id: int, inc: int = 1) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO member_channel_totals (guild_id, user_id, channel_id, messages)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, channel_id) DO UPDATE SET
              messages = messages + excluded.messages
        """, (guild_id, user_id, channel_id, int(inc)))
        con.commit()

def prime_channel_total(guild_id: int, user_id: int) -> int | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("""
            SELECT channel_id FROM member_channel_totals
            WHERE guild_id=? AND user_id=?
            ORDER BY messages DESC
            LIMIT 1
        """, (guild_id, user_id)).fetchone()
        return int(row[0]) if row else None

# ---- Period/total leaderboards (unified) ----

def _top_members_by_period(guild_id: int, metric: str, scope: str, key: str, limit: int) -> List[Tuple[int, int]]:
    where = {"day": "day=?", "week": "week=?", "month": "month=?"}[scope]
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            f"""
            SELECT user_id, SUM(count) AS c
            FROM member_metrics_daily
            WHERE guild_id=? AND metric=? AND {where}
            GROUP BY user_id
            ORDER BY c DESC
            LIMIT ?
            """,
            (guild_id, metric, key, limit),
        ).fetchall()

def top_members_messages_period(guild_id: int, scope: str, key: str, limit: int) -> List[Tuple[int, int]]:
    return _top_members_by_period(guild_id, "messages", scope, key, limit)

def top_members_words_period(guild_id: int, scope: str, key: str, limit: int) -> List[Tuple[int, int]]:
    return _top_members_by_period(guild_id, "words", scope, key, limit)

def top_members_mentions_period(guild_id: int, scope: str, key: str, limit: int) -> List[Tuple[int, int]]:
    return _top_members_by_period(guild_id, "mentions", scope, key, limit)

def top_members_emoji_chat_period(guild_id: int, scope: str, key: str, limit: int) -> List[Tuple[int, int]]:
    return _top_members_by_period(guild_id, "emoji_chat", scope, key, limit)

def top_members_emoji_react_period(guild_id: int, scope: str, key: str, limit: int) -> List[Tuple[int, int]]:
    return _top_members_by_period(guild_id, "emoji_react", scope, key, limit)

def _top_members_total(guild_id: int, metric: str, limit: int) -> List[Tuple[int, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT user_id, count
            FROM member_metrics_total
            WHERE guild_id=? AND metric=?
            ORDER BY count DESC
            LIMIT ?
            """,
            (guild_id, metric, limit),
        ).fetchall()

def top_members_messages_total(guild_id: int, limit: int) -> List[Tuple[int, int]]:
    rows = _top_members_total(guild_id, "messages", limit)
    if rows:
        return rows
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

def top_members_words_total(guild_id: int, limit: int) -> List[Tuple[int, int]]:
    return _top_members_total(guild_id, "words", limit)

def top_members_mentions_total(guild_id: int, limit: int) -> List[Tuple[int, int]]:
    return _top_members_total(guild_id, "mentions", limit)

def top_members_emoji_chat_total(guild_id: int, limit: int) -> List[Tuple[int, int]]:
    return _top_members_total(guild_id, "emoji_chat", limit)

def top_members_emoji_react_total(guild_id: int, limit: int) -> List[Tuple[int, int]]:
    return _top_members_total(guild_id, "emoji_react", limit)

# ---- Member-centric helpers ----

def member_word_stats(guild_id: int, user_id: int) -> Tuple[int, List[Tuple[str, int]]]:
    """Return (total_words, [(month, count)...] sorted desc)."""
    with connect() as con:
        cur = con.cursor()
        total_row = cur.execute("""
            SELECT count FROM member_metrics_total
            WHERE guild_id=? AND user_id=? AND metric='words'
        """, (guild_id, user_id)).fetchone()
        total = int(total_row[0]) if total_row else 0

        rows = cur.execute("""
            SELECT month, SUM(count) as c
            FROM member_metrics_daily
            WHERE guild_id=? AND user_id=? AND metric='words'
            GROUP BY month
            ORDER BY month DESC
        """, (guild_id, user_id)).fetchall()
        return total, rows

def member_daily_counts_month(guild_id: int, user_id: int | None, month: str) -> List[Tuple[str, int]]:
    """[(YYYY-MM-DD, count)] for messages in a month; user_id=None -> guild aggregate."""
    with connect() as con:
        cur = con.cursor()
        if user_id is None:
            rows = cur.execute("""
                SELECT day, SUM(count) AS c
                FROM member_metrics_daily
                WHERE guild_id=? AND metric='messages' AND month=?
                GROUP BY day
                ORDER BY day ASC
            """, (guild_id, month)).fetchall()
        else:
            rows = cur.execute("""
                SELECT day, count
                FROM member_metrics_daily
                WHERE guild_id=? AND user_id=? AND metric='messages' AND month=?
                ORDER BY day ASC
            """, (guild_id, user_id, month)).fetchall()
        return rows

def member_hour_histogram_total(guild_id: int, user_id: int, tz: str = "UTC") -> List[int]:
    """Return 24-bucket histogram for messages, rotated to tz."""
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute("""
            SELECT hour_utc, count FROM member_hour_hist
            WHERE guild_id=? AND user_id=? AND metric='messages'
        """, (guild_id, user_id)).fetchall()
    counts_utc = [0]*24
    for h, c in rows:
        counts_utc[int(h)] += int(c)

    if tz == "UTC":
        return counts_utc

    try:
        target = ZoneInfo(tz)
        now = datetime.now(timezone.utc)
        offset = int((now.astimezone(target).utcoffset() or 0).total_seconds() // 3600)
        rotated = [0]*24
        for h in range(24):
            pt_h = (h + offset) % 24
            rotated[pt_h] = counts_utc[h]
        return rotated
    except Exception:
        return counts_utc

def available_months(guild_id: int) -> List[str]:
    """Months with any message activity (prefer unified table, fallback to legacy)."""
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute("""
            SELECT DISTINCT month FROM member_metrics_daily
            WHERE guild_id=? AND metric='messages'
            ORDER BY month DESC
            LIMIT 36
        """, (guild_id,)).fetchall()
        if rows:
            return [r[0] for r in rows]
        rows2 = cur.execute("""
            SELECT DISTINCT month FROM member_activity_monthly
            WHERE guild_id=?
            ORDER BY month DESC
            LIMIT 36
        """, (guild_id,)).fetchall()
        return [r[0] for r in rows2]

def _reset_metric(guild_id: int, metric: str, scope: str, key: str | None) -> None:
    with connect() as con:
        cur = con.cursor()
        if scope == "all":
            cur.execute("DELETE FROM member_metrics_daily WHERE guild_id=? AND metric=?", (guild_id, metric))
            cur.execute("DELETE FROM member_metrics_total WHERE guild_id=? AND metric=?", (guild_id, metric))
            if metric == "messages":
                cur.execute("DELETE FROM member_hour_hist WHERE guild_id=? AND metric='messages'", (guild_id,))
        elif scope in ("day","week","month") and key:
            cur.execute(f"DELETE FROM member_metrics_daily WHERE guild_id=? AND metric=? AND {scope}=?", (guild_id, metric, key))
            cur.execute("""
                DELETE FROM member_metrics_total
                WHERE guild_id=? AND metric=? AND user_id IN (
                  SELECT DISTINCT user_id FROM member_metrics_daily
                  WHERE guild_id=? AND metric=?
                )
            """, (guild_id, metric, guild_id, metric))
            cur.execute("""
                INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
                SELECT guild_id, user_id, metric, SUM(count)
                FROM member_metrics_daily
                WHERE guild_id=? AND metric=?
                GROUP BY guild_id, user_id, metric
            """, (guild_id, metric))
        else:
            raise ValueError("bad_scope_or_key")
        con.commit()

def reset_member_words(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "words", scope, key)

def reset_member_mentions(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "mentions", scope, key)

def reset_member_emoji_chat(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "emoji_chat", scope, key)

def reset_member_emoji_react(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "emoji_react", scope, key)

# =============================================================================
# Admin utilities: views, CSV ingest, rebuilders, cleanup
# =============================================================================

def ensure_activity_views() -> None:
    """Create simple rollup views for leaderboards/graphs."""
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_messages_daily AS
        SELECT guild_id, user_id, day, week, month, count
        FROM member_metrics_daily
        WHERE metric='messages'
        """)
        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_messages_weekly AS
        SELECT guild_id, user_id, week, SUM(count) AS count
        FROM member_metrics_daily
        WHERE metric='messages'
        GROUP BY guild_id, user_id, week
        """)
        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_messages_monthly AS
        SELECT guild_id, user_id, month, SUM(count) AS count
        FROM member_metrics_daily
        WHERE metric='messages'
        GROUP BY guild_id, user_id, month
        """)
        con.commit()

def import_month_csv_rows(rows: Iterable[Tuple[int, str, int, int]]) -> int:
    """
    Ingest rows shaped as (guild_id, month, user_id, messages).
    Data are written to unified daily as if sent on the **first day** of the month,
    mirrored into legacy monthly, and totals rebuilt for affected guilds.
    Returns number of rows ingested.
    """
    prepared: list[tuple[int, int, str, str, str, int]] = []
    legacy_monthly: list[tuple[int, int, str, int]] = []

    for guild_id, month, user_id, msgs in rows:
        day_iso = f"{month}-01"
        try:
            dt = datetime.fromisoformat(day_iso).replace(tzinfo=timezone.utc)
        except Exception:
            dt = datetime.strptime(day_iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        iso_year, iso_week, _ = dt.isocalendar()
        week_key = f"{iso_year}-W{iso_week:02d}"
        prepared.append((guild_id, user_id, "messages", day_iso, week_key, month, int(msgs)))
        legacy_monthly.append((guild_id, user_id, month, int(msgs)))

    if not prepared:
        return 0

    with connect() as con:
        cur = con.cursor()
        cur.executemany("""
            INSERT INTO member_metrics_daily (guild_id, user_id, metric, day, week, month, count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, metric, day) DO UPDATE SET
              count = count + excluded.count
        """, prepared)
        cur.executemany("""
            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, month) DO UPDATE SET
              count = count + excluded.count
        """, legacy_monthly)

        gids = sorted({g for (g, _, _, _, _, _, _) in prepared})
        for gid in gids:
            cur.execute("DELETE FROM member_metrics_total WHERE guild_id=? AND metric='messages'", (gid,))
            cur.execute("""
                INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
                SELECT guild_id, user_id, 'messages', SUM(count)
                FROM member_metrics_daily
                WHERE guild_id=? AND metric='messages'
                GROUP BY guild_id, user_id
            """, (gid,))
            cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (gid,))
            cur.execute("""
                INSERT INTO member_activity_total (guild_id, user_id, count)
                SELECT guild_id, user_id, SUM(count)
                FROM member_activity_monthly
                WHERE guild_id=?
                GROUP BY guild_id, user_id
            """, (gid,))
        con.commit()
    return len(prepared)

def rebuild_activity_totals_for_guild(guild_id: int) -> None:
    """Recompute unified totals (all metrics) from daily; legacy totals from legacy monthly."""
    with connect() as con:
        cur = con.cursor()
        cur.execute("DELETE FROM member_metrics_total WHERE guild_id=?", (guild_id,))
        cur.execute("""
            INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
            SELECT guild_id, user_id, metric, SUM(count)
            FROM member_metrics_daily
            WHERE guild_id=?
            GROUP BY guild_id, user_id, metric
        """, (guild_id,))
        cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (guild_id,))
        cur.execute("""
            INSERT INTO member_activity_total (guild_id, user_id, count)
            SELECT guild_id, user_id, SUM(count)
            FROM member_activity_monthly
            WHERE guild_id=?
            GROUP BY guild_id, user_id
        """, (guild_id,))
        con.commit()

def cleanup_activity(guild_id: int | None = None) -> int:
    """
    Purge all Activity-cog data. If guild_id is None, purge all guilds.
    Returns total rows deleted.
    """
    tables = [
        "member_metrics_daily",
        "member_metrics_total",
        "member_hour_hist",
        "member_activity_monthly",
        "member_activity_total",
        "emoji_usage_monthly",
        "sticker_usage_monthly",
    ]
    total_deleted = 0
    with connect() as con:
        cur = con.cursor()
        if guild_id is None:
            for t in tables:
                cur.execute(f"DELETE FROM {t}")
                total_deleted += cur.execute("SELECT changes()").fetchone()[0]
        else:
            for t in tables:
                cur.execute(f"DELETE FROM {t} WHERE guild_id=?", (guild_id,))
                total_deleted += cur.execute("SELECT changes()").fetchone()[0]
        con.commit()
    return total_deleted

# =============================================================================
# RPG progression & XP
# =============================================================================

# MMO-ish level curve: total XP needed to reach level L
# L1 -> 0, L2 -> 100, L3 -> 282, L10 ~ 5.7k, L20 ~ 36k
def _xp_for_level(level: int) -> int:
    if level <= 1:
        return 0
    # ∫ 100*x^1.5 dx ≈ 40 * (level-1)^(2.5)
    return int(40 * (level - 1) ** 2.5)

def level_from_xp(total_xp: int) -> int:
    lo, hi = 1, 300
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _xp_for_level(mid) <= total_xp:
            lo = mid
        else:
            hi = mid - 1
    return lo

def xp_progress(total_xp: int) -> tuple[int, int, int]:
    """Return (level, current_into_level, needed_for_next)."""
    lvl = level_from_xp(total_xp)
    cur = total_xp - _xp_for_level(lvl)
    nxt = _xp_for_level(lvl + 1) - _xp_for_level(lvl)
    return (lvl, cur, nxt)

# Base XP rules (before per-channel multipliers in the cog)
XP_RULES = {
    "messages": 5,               # per message
    "words_per_20": 2,           # +2 per 20 words (floor)
    "mentions_received": 3,
    "mentions_sent": 1,
    "emoji_chat": 0.5,
    "emoji_react": 0.5,
    "reactions_received": 1,     # per reaction received on your messages
    "sticker_use": 2,
    "voice_minutes": 1,
    "voice_stream_minutes": 2,
    "activity_minutes": 1,
}

def _stat_activity_scores(con: sqlite3.Connection, guild_id: int, user_id: int) -> Dict[str, int]:
    """
    Build per-stat 'activity score' using unified totals.
    Emoji-only messages increase DEX signal.
    """
    cur = con.cursor()
    totals = dict(cur.execute("""
        SELECT metric, count FROM member_metrics_total
        WHERE guild_id=? AND user_id=?
    """, (guild_id, user_id)).fetchall())

    # Base signals from existing metrics
    messages            = int(totals.get("messages", 0))
    words               = int(totals.get("words", 0))
    mentions_recv       = int(totals.get("mentions", 0))
    mentions_sent       = int(totals.get("mentions_sent", 0))
    emoji_chat          = int(totals.get("emoji_chat", 0))
    emoji_react         = int(totals.get("emoji_react", 0))
    reacts_recv         = int(totals.get("reactions_received", 0))
    voice_min           = int(totals.get("voice_minutes", 0))
    stream_min          = int(totals.get("voice_stream_minutes", 0))
    activity_min        = int(totals.get("activity_minutes", 0))
    emoji_only_msgs     = int(totals.get("emoji_only", 0))  # NEW

    # Stat signals (simple additive; adjust weights later if you want)
    scores = {
        "str": messages + emoji_chat,                       # throughput + expressive chat
        "int": words,                                       # depth
        "cha": mentions_recv + reacts_recv,                 # recognition
        "vit": voice_min + stream_min,                      # presence
        "dex": emoji_react + mentions_sent + emoji_only_msgs,  # finesse + emoji-only flavor
        "wis": activity_min,                                # focused sessions
    }
    return scores

_LEVELUP_DISTR = [4, 3, 2, 1, 1, 0]  # highest -> lowest activity types

def _apply_levelup_stats(con: sqlite3.Connection, guild_id: int, user_id: int) -> None:
    """
    Apply stat gains based on current activity score ordering:
    +4, +3, +2, +1, +1, 0 to the 6 stats in rank order.
    Ties are stable by the order below.
    """
    cur = con.cursor()
    scores = _stat_activity_scores(con, guild_id, user_id)
    order = ["str", "dex", "int", "wis", "cha", "vit"]  # stable tiebreaker
    ranked = sorted(order, key=lambda k: (-scores.get(k, 0), order.index(k)))

    # Build SQL: add gains to each stat
    sets = []
    params = []
    for stat, add in zip(ranked, _LEVELUP_DISTR):
        if add > 0:
            sets.append(f"{stat}={stat}+?")
            params.append(add)
    if not sets:
        return
    params += [guild_id, user_id]
    cur.execute(f"UPDATE member_rpg_progress SET {', '.join(sets)} WHERE guild_id=? AND user_id=?", params)


def _two_secondaries(primary: str) -> tuple[str, str]:
    wheel = ["str", "dex", "int", "wis", "cha", "vit"]
    i = wheel.index(primary)
    return (wheel[(i+1)%6], wheel[(i+2)%6])

def _apply_xp(con: sqlite3.Connection, guild_id: int, user_id: int, add_xp: int) -> tuple[int, int]:
    cur = con.cursor()
    row = cur.execute("""
        SELECT xp, level FROM member_rpg_progress
        WHERE guild_id=? AND user_id=?
    """, (guild_id, user_id)).fetchone()
    if not row:
        cur.execute("""
            INSERT INTO member_rpg_progress (guild_id, user_id, xp, level)
            VALUES (?, ?, 0, 1)
        """, (guild_id, user_id))
        xp, level = 0, 1
    else:
        xp, level = int(row[0]), int(row[1])

    xp += max(0, int(add_xp))
    new_level = level_from_xp(xp)

    if new_level > level:
        # Apply new ranked distribution +4,+3,+2,+1,+1,0
        _apply_levelup_stats(con, guild_id, user_id)
        cur.execute("""
            UPDATE member_rpg_progress
               SET level=?, xp=?, last_level_up=datetime('now')
             WHERE guild_id=? AND user_id=?
        """, (new_level, xp, guild_id, user_id))
    else:
        cur.execute("UPDATE member_rpg_progress SET xp=? WHERE guild_id=? AND user_id=?", (xp, guild_id, user_id))

    return new_level, xp


def award_xp_for_event(guild_id: int, user_id: int, base_xp: float, channel_multiplier: float = 1.0) -> tuple[int, int]:
    """Round after multiplier; returns (new_level, total_xp)."""
    add = int(round(max(0.0, base_xp) * max(0.0, channel_multiplier)))
    with connect() as con:
        lvl, xp = _apply_xp(con, guild_id, user_id, add)
        con.commit()
        return lvl, xp

def get_rpg_progress(guild_id: int, user_id: int) -> dict:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("""
            SELECT xp, level, str, int, cha, vit, dex, wis, COALESCE(last_level_up, '')
            FROM member_rpg_progress
            WHERE guild_id=? AND user_id=?
        """, (guild_id, user_id)).fetchone()
        if not row:
            return {"xp": 0, "level": 1, "str":5, "int":5, "cha":5, "vit":5, "dex":5, "wis":5, "last_level_up":""}
        return {
            "xp": int(row[0]), "level": int(row[1]),
            "str": int(row[2]), "int": int(row[3]), "cha": int(row[4]),
            "vit": int(row[5]), "dex": int(row[6]), "wis": int(row[7]),
            "last_level_up": row[8],
        }

def top_levels(guild_id: int, limit: int = 20) -> list[tuple[int,int,int]]:
    """[(user_id, level, xp)] by level desc, xp desc."""
    with connect() as con:
        cur = con.cursor()
        return cur.execute("""
            SELECT user_id, level, xp
            FROM member_rpg_progress
            WHERE guild_id=?
            ORDER BY level DESC, xp DESC
            LIMIT ?
        """, (guild_id, limit)).fetchall()

# =============================================================================
# Mod actions (discipline)
# =============================================================================

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

# =============================================================================
# Movie Night (reuses club slots)
# =============================================================================

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
    when_iso = _now_iso_utc()
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO role_welcome_sent
            (guild_id, user_id, role_id, sent_at)
            VALUES (?, ?, ?, ?)
        """, (guild_id, user_id, role_id, when_iso))
        con.commit()

# =============================================================================
# MangaUpdates persistence
# =============================================================================

def mu_register_thread_series(guild_id: int, thread_id: int, series_id: str, series_title: str) -> None:
    """Associate a forum thread with an MU series; upsert series title."""
    now = _now_iso_utc()
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO mu_series (series_id, title, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(series_id) DO UPDATE SET title=excluded.title
        """, (str(series_id), series_title, now))
        cur.execute("""
            INSERT INTO mu_thread_series (guild_id, thread_id, series_id)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, thread_id) DO UPDATE SET series_id=excluded.series_id
        """, (guild_id, thread_id, str(series_id)))
        con.commit()

def mu_get_thread_series(thread_id: int, guild_id: int | None = None) -> str | None:
    with connect() as con:
        cur = con.cursor()
        if guild_id is None:
            row = cur.execute("SELECT series_id FROM mu_thread_series WHERE thread_id=?", (thread_id,)).fetchone()
        else:
            row = cur.execute("SELECT series_id FROM mu_thread_series WHERE guild_id=? AND thread_id=?", (guild_id, thread_id)).fetchone()
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
            cur.execute("""
                INSERT INTO mu_releases
                    (series_id, release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(series_id), int(release_id), title, raw_title, description,
                volume, chapter, subchapter, group_name, url, int(release_ts), now
            ))
            con.commit()
            return True
        except sqlite3.IntegrityError:
            return False

def mu_bulk_upsert_releases(series_id: str, items: list[dict]) -> list[int]:
    """Upsert many releases; return newly inserted release_ids (ascending by release_ts)."""
    inserted: list[tuple[int,int]] = []  # (release_ts, release_id)
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
    """Most recent known release_ts for a series (or -1)."""
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("""
            SELECT COALESCE(MAX(release_ts), -1) FROM mu_releases WHERE series_id=?
        """, (str(series_id),)).fetchone()
        return int(row[0] if row and row[0] is not None else -1)

def mu_list_unposted_for_thread(guild_id: int, thread_id: int, series_id: str, *, english_only: bool = False) -> list[tuple]:
    """
    Return releases NOT yet posted in thread (ordered by release_ts asc, unknowns first).
    Columns: (release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts)
    """
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute("""
            SELECT r.release_id, r.title, r.raw_title, r.description, r.volume, r.chapter, r.subchapter,
                   r.group_name, r.url, r.release_ts
            FROM mu_releases r
            LEFT JOIN mu_thread_posts tp
              ON tp.guild_id=? AND tp.thread_id=? AND tp.series_id=r.series_id AND tp.release_id=r.release_id
            WHERE r.series_id=? AND tp.release_id IS NULL
            ORDER BY r.release_ts ASC, r.release_id ASC
        """, (guild_id, thread_id, str(series_id))).fetchall()

        if not english_only:
            return rows

        def _is_en(title, raw, desc):
            txt = f"{title or ''} {raw or ''} {desc or ''}".lower()
            return any(k in txt for k in ("eng", "english", "[en]", "(en)"))
        return [r for r in rows if _is_en(r[1], r[2], r[3])]

def mu_mark_posted(guild_id: int, thread_id: int, series_id: str, release_id: int, when_iso: str | None = None) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO mu_thread_posts (guild_id, thread_id, series_id, release_id, posted_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, thread_id, release_id) DO NOTHING
        """, (guild_id, thread_id, str(series_id), int(release_id), when_iso or _now_iso_utc()))
        con.commit()

def bump_member_gifs(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "gifs", day, week_key, month, inc)
        con.commit()
def bump_gif_usage(guild_id: int, when_iso: str, gif_key: str, source: str, inc: int = 1) -> None:
    """
    gif_key: stable key for the GIF (prefer the direct media URL if available; else the provider page URL)
    source: short provider token like 'tenor', 'giphy', 'imgur', 'discord', 'other'
    """
    month = when_iso[:7]
    with connect() as con:
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gif_usage_monthly (
                guild_id INTEGER NOT NULL,
                month    TEXT    NOT NULL,   -- YYYY-MM
                gif_key  TEXT    NOT NULL,   -- canonical URL or provider id
                source   TEXT    NOT NULL,   -- tenor/giphy/discord/etc
                count    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, month, gif_key)
            )
        """)
        cur.execute("""
            INSERT INTO gif_usage_monthly (guild_id, month, gif_key, source, count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, month, gif_key)
            DO UPDATE SET count = count + excluded.count
        """, (guild_id, month, gif_key[:512], source[:32], inc))
        con.commit()
def mu_get_release(series_id: str, release_id: int) -> dict | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute("""
            SELECT title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts
            FROM mu_releases WHERE series_id=? AND release_id=?
        """, (str(series_id), int(release_id))).fetchone()
        if not row:
            return None
        return {
            "title": row[0], "raw_title": row[1], "description": row[2],
            "volume": row[3], "chapter": row[4], "subchapter": row[5],
            "group": row[6], "url": row[7], "release_ts": row[8],
            "release_id": int(release_id), "series_id": str(series_id),
        }
def top_gifs(guild_id: int, month: str, limit: int = 20) -> List[Tuple[str, str, int]]:
    """Returns [(gif_key, source, count)] for the month, desc."""
    with connect() as con:
        cur = con.cursor()
        return cur.execute("""
            SELECT gif_key, source, count
            FROM gif_usage_monthly
            WHERE guild_id=? AND month=?
            ORDER BY count DESC
            LIMIT ?
        """, (guild_id, month, limit)).fetchall()
def bump_member_emoji_only(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "emoji_only", day, week_key, month, inc)
        con.commit()

