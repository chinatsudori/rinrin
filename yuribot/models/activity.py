from __future__ import annotations

import logging
import sqlite3
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

from zoneinfo import ZoneInfo

from ..db import connect
from .common import iso_parts as _iso_parts, now_iso_utc as _now_iso_utc

log = logging.getLogger(__name__)


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
    cur.execute(
        """
        INSERT INTO member_metrics_daily (guild_id, user_id, metric, day, week, month, count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, metric, day) DO UPDATE SET
          count = count + excluded.count
        """,
        (guild_id, user_id, metric, day, week_key, month, inc),
    )
    cur.execute(
        """
        INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, metric) DO UPDATE SET
          count = count + excluded.count
        """,
        (guild_id, user_id, metric, inc),
    )


# --- archive merge helpers (messages/words) ---

_WS = re.compile(r"\s+")


def _archive_table_exists(con: sqlite3.Connection) -> bool:
    cur = con.cursor()
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name='message_archive' LIMIT 1"
    ).fetchone()
    return bool(row)


def _count_words_archive(text: str | None) -> int:
    if not text:
        return 0
    return sum(1 for t in _WS.split(text.strip()) if t)


def _iso_week_bounds(week_key: str) -> tuple[str, str]:
    """week_key: 'YYYY-Www' -> (start_day_iso, end_day_iso) using ISO week (Mon..Sun) in UTC."""
    if not week_key:
        # defensive default: current ISO week
        now = datetime.now(timezone.utc)
        y, w, _ = now.isocalendar()
        week_key = f"{y}-W{int(w):02d}"
    year = int(week_key[:4])
    wnum = int(week_key[-2:])
    d = datetime.fromisocalendar(year, wnum, 1).replace(tzinfo=timezone.utc)  # Monday
    start = d.date().isoformat()
    end = (d + timedelta(days=6)).date().isoformat()
    return start, end


def _archive_period_rows(
    con: sqlite3.Connection, guild_id: int, scope: str, key: str | None
):
    cur = con.cursor()
    params = [guild_id]
    where = "guild_id=?"
    if scope == "day":
        where += " AND DATE(created_at)=?"
        params.append(key)
    elif scope == "month":
        where += " AND strftime('%Y-%m', created_at)=?"
        params.append(key)
    elif scope == "week":
        s, e = _iso_week_bounds(key or "")
        where += " AND DATE(created_at) BETWEEN ? AND ?"
        params.extend([s, e])
    # scope == "all" -> no date filter
    return cur.execute(
        f"SELECT author_id, content FROM message_archive WHERE {where}", params
    )

# ---- Targeted totals rebuilds ----

def rebuild_metric_totals_for_guild(guild_id: int, metrics: Iterable[str]) -> None:
    """
    Recompute member_metrics_total for the given metrics from member_metrics_daily.
    Use when daily is correct but totals are stale/missing.
    """
    mets = tuple(set(str(m) for m in metrics))
    if not mets:
        return
    placeholders = ",".join("?" for _ in mets)
    with connect() as con:
        cur = con.cursor()
        # wipe only the metrics we’re rebuilding
        cur.execute(f"DELETE FROM member_metrics_total WHERE guild_id=? AND metric IN ({placeholders})", (guild_id, *mets))
        # write fresh totals from daily
        cur.execute(
            f"""
            INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
            SELECT guild_id, user_id, metric, SUM(count)
            FROM member_metrics_daily
            WHERE guild_id=? AND metric IN ({placeholders})
            GROUP BY guild_id, user_id, metric
            """,
            (guild_id, *mets),
        )
        con.commit()


def rebuild_voice_totals_for_guild(guild_id: int) -> None:
    """
    Convenience wrapper: rebuild totals for voice_minutes and voice_stream_minutes.
    """
    rebuild_metric_totals_for_guild(guild_id, ("voice_minutes", "voice_stream_minutes"))

def _archive_counts_by_user(
    con: sqlite3.Connection, guild_id: int, scope: str, key: str | None, metric: str
) -> dict[int, int]:
    """
    Aggregate archive for messages/words by (author_id).
    scope in {'all','day','week','month'}; key None for 'all'.
    """
    if not _archive_table_exists(con):
        return {}
    counts: dict[int, int] = {}
    for uid, content in _archive_period_rows(con, guild_id, scope, key):
        uid = int(uid)
        if metric == "messages":
            counts[uid] = counts.get(uid, 0) + 1
        elif metric == "words":
            counts[uid] = counts.get(uid, 0) + _count_words_archive(content)
    return counts


def _merge_and_rank(
    base_rows: list[tuple[int, int]], extra: dict[int, int], limit: int
) -> list[tuple[int, int]]:
    acc: dict[int, int] = {}
    for uid, c in base_rows:
        acc[int(uid)] = acc.get(int(uid), 0) + int(c)
    for uid, c in extra.items():
        acc[int(uid)] = acc.get(int(uid), 0) + int(c)
    return sorted(acc.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]


def bump_member_message(guild_id: int, user_id: int, when_iso: str | None = None, inc: int = 1) -> None:
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


def _bump_hour_hist(
    con: sqlite3.Connection,
    guild_id: int,
    user_id: int,
    metric: str,
    hour_utc: int,
    inc: int = 1,
) -> None:
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO member_hour_hist (guild_id, user_id, metric, hour_utc, count)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, metric, hour_utc) DO UPDATE SET
          count = count + excluded.count
        """,
        (guild_id, user_id, metric, hour_utc, inc),
    )


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
            "SELECT count FROM member_activity_total WHERE guild_id=? AND user_id=?",
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
        elif scope in ("day", "week", "month") and key:
            cur.execute(
                f"DELETE FROM member_metrics_daily WHERE guild_id=? AND metric='messages' AND {scope}=?",
                (guild_id, key),
            )
            if scope == "month":
                cur.execute("DELETE FROM member_activity_monthly WHERE guild_id=? AND month=?", (guild_id, key))
            cur.execute("DELETE FROM member_metrics_total WHERE guild_id=? AND metric='messages'", (guild_id,))
            cur.execute(
                """
                INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
                SELECT guild_id, user_id, 'messages', SUM(count)
                FROM member_metrics_daily
                WHERE guild_id=? AND metric='messages'
                GROUP BY guild_id, user_id
                """,
                (guild_id,),
            )
            cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (guild_id,))
            cur.execute(
                """
                INSERT INTO member_activity_total (guild_id, user_id, count)
                SELECT guild_id, user_id, SUM(count)
                FROM member_activity_monthly
                WHERE guild_id=?
                GROUP BY guild_id, user_id
                """,
                (guild_id,),
            )
        else:
            raise ValueError("bad_scope_or_key")
        con.commit()


# ---- Unified bumpers for other metrics ----

def bump_member_words(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "words", day, week_key, month, inc)
        con.commit()


def fetch_metric_totals(guild_id: int, metric: str) -> Dict[int, int]:
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT user_id, count
            FROM member_metrics_total
            WHERE guild_id=? AND metric=?
            """,
            (guild_id, metric),
        ).fetchall()
    return {int(uid): int(count or 0) for uid, count in rows}


def bump_member_mentioned(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "mentions", day, week_key, month, inc)
        con.commit()


def bump_member_mentions_sent(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "mentions_sent", day, week_key, month, inc)
        con.commit()


def bump_member_emoji_chat(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "emoji_chat", day, week_key, month, inc)
        con.commit()


def bump_member_emoji_react(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "emoji_react", day, week_key, month, inc)
        con.commit()


def bump_reactions_received(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
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
            _upsert_metric_daily_and_total(
                con, guild_id, user_id, "voice_stream_minutes", day, week_key, month, int(stream_minutes)
            )
        con.commit()


def bump_activity_minutes(guild_id: int, user_id: int, when_iso: str, app_name: str, minutes: int, launches: int = 0) -> None:
    if minutes <= 0 and launches <= 0:
        return
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "activity_minutes", day, week_key, month, int(minutes))
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO member_activity_apps_daily (guild_id, user_id, app_name, day, minutes, launches)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, app_name, day) DO UPDATE SET
              minutes = minutes + excluded.minutes,
              launches = launches + excluded.launches
            """,
            (guild_id, user_id, app_name or "(unknown)", day, int(minutes), int(launches)),
        )
        con.commit()


# ---- Channel totals (for “prime channel”) ----

def bump_channel_message_total(guild_id: int, user_id: int, channel_id: int, inc: int = 1) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO member_channel_totals (guild_id, user_id, channel_id, messages)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, channel_id) DO UPDATE SET
              messages = messages + excluded.messages
            """,
            (guild_id, user_id, channel_id, int(inc)),
        )
        con.commit()


def prime_channel_total(guild_id: int, user_id: int) -> int | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """
            SELECT channel_id FROM member_channel_totals
            WHERE guild_id=? AND user_id=?
            ORDER BY messages DESC
            LIMIT 1
            """,
            (guild_id, user_id),
        ).fetchone()
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
    # Fallback to legacy totals if unified missing (back-compat).
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
        total_row = cur.execute(
            """
            SELECT count FROM member_metrics_total
            WHERE guild_id=? AND user_id=? AND metric='words'
            """,
            (guild_id, user_id),
        ).fetchone()
        total = int(total_row[0]) if total_row else 0
        rows = cur.execute(
            """
            SELECT month, SUM(count) as c
            FROM member_metrics_daily
            WHERE guild_id=? AND user_id=? AND metric='words'
            GROUP BY month
            ORDER BY month DESC
            """,
            (guild_id, user_id),
        ).fetchall()
        return total, rows


def member_daily_counts_month(guild_id: int, user_id: int | None, month: str) -> List[Tuple[str, int]]:
    """[(YYYY-MM-DD, count)] for messages in a month; user_id=None -> guild aggregate."""
    with connect() as con:
        cur = con.cursor()
        if user_id is None:
            rows = cur.execute(
                """
                SELECT day, SUM(count) AS c
                FROM member_metrics_daily
                WHERE guild_id=? AND metric='messages' AND month=?
                GROUP BY day
                ORDER BY day ASC
                """,
                (guild_id, month),
            ).fetchall()
        else:
            rows = cur.execute(
                """
                SELECT day, count
                FROM member_metrics_daily
                WHERE guild_id=? AND user_id=? AND metric='messages' AND month=?
                ORDER BY day ASC
                """,
                (guild_id, user_id, month),
            ).fetchall()
        return rows


def member_hour_histogram_total(guild_id: int, user_id: int, tz: str = "UTC") -> List[int]:
    """Return 24-bucket histogram for messages, rotated to tz."""
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour_utc, count FROM member_hour_hist
            WHERE guild_id=? AND user_id=? AND metric='messages'
            """,
            (guild_id, user_id),
        ).fetchall()
    counts_utc = [0] * 24
    for h, c in rows:
        counts_utc[int(h)] += int(c)

    if tz == "UTC":
        return counts_utc

    try:
        target = ZoneInfo(tz)
        now = datetime.now(timezone.utc)
        offset = int((now.astimezone(target).utcoffset() or 0).total_seconds() // 3600)
        rotated = [0] * 24
        for h in range(24):
            rotated[(h + offset) % 24] = counts_utc[h]
        return rotated
    except Exception:
        return counts_utc


def available_months(guild_id: int) -> List[str]:
    """Months with any message activity (prefer unified table, fallback to legacy)."""
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT DISTINCT month FROM member_metrics_daily
            WHERE guild_id=? AND metric='messages'
            ORDER BY month DESC
            LIMIT 36
            """,
            (guild_id,),
        ).fetchall()
        if rows:
            return [r[0] for r in rows]
        rows2 = cur.execute(
            """
            SELECT DISTINCT month FROM member_activity_monthly
            WHERE guild_id=?
            ORDER BY month DESC
            LIMIT 36
            """,
            (guild_id,),
        ).fetchall()
        return [r[0] for r in rows2]


def _reset_metric(guild_id: int, metric: str, scope: str, key: str | None) -> None:
    with connect() as con:
        cur = con.cursor()
        if scope == "all":
            cur.execute("DELETE FROM member_metrics_daily WHERE guild_id=? AND metric=?", (guild_id, metric))
            cur.execute("DELETE FROM member_metrics_total WHERE guild_id=? AND metric=?", (guild_id, metric))
            if metric == "messages":
                cur.execute("DELETE FROM member_hour_hist WHERE guild_id=? AND metric='messages'", (guild_id,))
        elif scope in ("day", "week", "month") and key:
            cur.execute(
                f"DELETE FROM member_metrics_daily WHERE guild_id=? AND metric=? AND {scope}=?",
                (guild_id, metric, key),
            )
            cur.execute(
                """
                DELETE FROM member_metrics_total
                WHERE guild_id=? AND metric=? AND user_id IN (
                  SELECT DISTINCT user_id FROM member_metrics_daily
                  WHERE guild_id=? AND metric=?
                )
                """,
                (guild_id, metric, guild_id, metric),
            )
            cur.execute(
                """
                INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
                SELECT guild_id, user_id, metric, SUM(count)
                FROM member_metrics_daily
                WHERE guild_id=? AND metric=?
                GROUP BY guild_id, user_id, metric
                """,
                (guild_id, metric),
            )
        else:
            raise ValueError("bad_scope_or_key")
        con.commit()


def reset_member_words(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "words", scope, key)


def reset_member_mentions(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "mentions", scope, key)


def reset_member_mentions_sent(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "mentions_sent", scope, key)


def reset_member_emoji_chat(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "emoji_chat", scope, key)


def reset_member_emoji_react(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "emoji_react", scope, key)


def reset_member_emoji_only(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "emoji_only", scope, key)


def reset_member_reactions_received(guild_id: int, scope: str, key: str | None = None) -> None:
    _reset_metric(guild_id, "reactions_received", scope, key)


def reset_member_channel_totals(guild_id: int) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute("DELETE FROM member_channel_totals WHERE guild_id=?", (guild_id,))
        con.commit()


# =============================================================================
# Admin utilities: views, CSV ingest, rebuilders, cleanup
# =============================================================================

def ensure_activity_views() -> None:
    """Create simple rollup views for leaderboards/graphs."""
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            CREATE VIEW IF NOT EXISTS v_messages_daily AS
            SELECT guild_id, user_id, day, week, month, count
            FROM member_metrics_daily
            WHERE metric='messages'
            """
        )
        cur.execute(
            """
            CREATE VIEW IF NOT EXISTS v_messages_weekly AS
            SELECT guild_id, user_id, week, SUM(count) AS count
            FROM member_metrics_daily
            WHERE metric='messages'
            GROUP BY guild_id, user_id, week
            """
        )
        cur.execute(
            """
            CREATE VIEW IF NOT EXISTS v_messages_monthly AS
            SELECT guild_id, user_id, month, SUM(count) AS count
            FROM member_metrics_daily
            WHERE metric='messages'
            GROUP BY guild_id, user_id, month
            """
        )
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
        cur.executemany(
            """
            INSERT INTO member_metrics_daily (guild_id, user_id, metric, day, week, month, count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, metric, day) DO UPDATE SET
              count = count + excluded.count
            """,
            prepared,
        )
        cur.executemany(
            """
            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, month) DO UPDATE SET
              count = count + excluded.count
            """,
            legacy_monthly,
        )

        gids = sorted({g for (g, _, _, _, _, _, _) in prepared})
        for gid in gids:
            cur.execute("DELETE FROM member_metrics_total WHERE guild_id=? AND metric='messages'", (gid,))
            cur.execute(
                """
                INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
                SELECT guild_id, user_id, 'messages', SUM(count)
                FROM member_metrics_daily
                WHERE guild_id=? AND metric='messages'
                GROUP BY guild_id, user_id
                """,
                (gid,),
            )
            cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (gid,))
            cur.execute(
                """
                INSERT INTO member_activity_total (guild_id, user_id, count)
                SELECT guild_id, user_id, SUM(count)
                FROM member_activity_monthly
                WHERE guild_id=?
                GROUP BY guild_id, user_id
                """,
                (gid,),
            )
        con.commit()
    return len(prepared)


def rebuild_activity_totals_for_guild(guild_id: int) -> None:
    """Recompute unified totals (all metrics) from daily; legacy totals from legacy monthly."""
    with connect() as con:
        cur = con.cursor()
        cur.execute("DELETE FROM member_metrics_total WHERE guild_id=?", (guild_id,))
        cur.execute(
            """
            INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
            SELECT guild_id, user_id, metric, SUM(count)
            FROM member_metrics_daily
            WHERE guild_id=?
            GROUP BY guild_id, user_id, metric
            """,
            (guild_id,),
        )
        cur.execute("DELETE FROM member_activity_total WHERE guild_id=?", (guild_id,))
        cur.execute(
            """
            INSERT INTO member_activity_total (guild_id, user_id, count)
            SELECT guild_id, user_id, SUM(count)
            FROM member_activity_monthly
            WHERE guild_id=?
            GROUP BY guild_id, user_id
            """,
            (guild_id,),
        )
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
            for table in tables:
                cur.execute(f"DELETE FROM {table}")
                total_deleted += cur.execute("SELECT changes()").fetchone()[0]
        else:
            for table in tables:
                cur.execute(f"DELETE FROM {table} WHERE guild_id=?", (guild_id,))
                total_deleted += cur.execute("SELECT changes()").fetchone()[0]
        con.commit()
    return total_deleted


def bump_activity_join(
    guild_id: int,
    user_id: int,
    when_iso: str,
    app_name: str | None = None,
    joins: int = 1,
) -> None:
    """
    Count a *join* event (scheduled event join, voice watch-party, Discord app, etc).
    - Increments unified metric 'activity_joins' (used by WIS).
    - Also records a 'launch' in member_activity_apps_daily for the given app_name.
    """
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(
            con, guild_id, user_id, "activity_joins", day, week_key, month, int(joins)
        )
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO member_activity_apps_daily (guild_id, user_id, app_name, day, minutes, launches)
            VALUES (?, ?, ?, ?, 0, ?)
            ON CONFLICT(guild_id, user_id, app_name, day) DO UPDATE SET
              launches = launches + excluded.launches
            """,
            (guild_id, user_id, (app_name or "(unknown)")[:80], day, int(joins)),
        )
        con.commit()


def bump_member_gifs(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "gifs", day, week_key, month, inc)
        con.commit()


def bump_member_emoji_only(guild_id: int, user_id: int, when_iso: str, inc: int = 1) -> None:
    day, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        _upsert_metric_daily_and_total(con, guild_id, user_id, "emoji_only", day, week_key, month, inc)
        con.commit()


def _day_string_to_iso(day: str) -> str:
    day = day.strip()
    if len(day) == 10:
        return f"{day}T00:00:00+00:00"
    try:
        dt = datetime.fromisoformat(day)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return f"{day}T00:00:00+00:00"


def upsert_member_messages_day(guild_id: int, user_id: int, day: str, count: int) -> None:
    if count <= 0:
        return
    when_iso = _day_string_to_iso(day)
    day_iso, week_key, month, _ = _iso_parts(when_iso)
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO member_metrics_daily (guild_id, user_id, metric, day, week, month, count)
            VALUES (?, ?, 'messages', ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, metric, day) DO UPDATE SET count=excluded.count
            """,
            (guild_id, user_id, day_iso, week_key, month, int(count)),
        )
        cur.execute(
            """
            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, month) DO UPDATE SET count=excluded.count
            """,
            (guild_id, user_id, month, int(count)),
        )
        cur.execute(
            "DELETE FROM member_metrics_total WHERE guild_id=? AND user_id=? AND metric='messages'",
            (guild_id, user_id),
        )
        cur.execute(
            """
            INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
            SELECT guild_id, user_id, 'messages', SUM(count)
            FROM member_metrics_daily
            WHERE guild_id=? AND user_id=? AND metric='messages'
            GROUP BY guild_id, user_id
            """,
            (guild_id, user_id),
        )
        cur.execute(
            "DELETE FROM member_activity_total WHERE guild_id=? AND user_id=?",
            (guild_id, user_id),
        )
        cur.execute(
            """
            INSERT INTO member_activity_total (guild_id, user_id, count)
            SELECT guild_id, user_id, SUM(count)
            FROM member_activity_monthly
            WHERE guild_id=? AND user_id=?
            GROUP BY guild_id, user_id
            """,
            (guild_id, user_id),
        )
        con.commit()


def upsert_member_messages_month(guild_id: int, user_id: int, month: str, count: int) -> None:
    if count <= 0:
        return
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id, month) DO UPDATE SET count=excluded.count
            """,
            (guild_id, user_id, month, int(count)),
        )
        cur.execute(
            "DELETE FROM member_activity_total WHERE guild_id=? AND user_id=?",
            (guild_id, user_id),
        )
        cur.execute(
            """
            INSERT INTO member_activity_total (guild_id, user_id, count)
            SELECT guild_id, user_id, SUM(count)
            FROM member_activity_monthly
            WHERE guild_id=? AND user_id=?
            GROUP BY guild_id, user_id
            """,
            (guild_id, user_id),
        )
        cur.execute(
            "DELETE FROM member_metrics_total WHERE guild_id=? AND user_id=? AND metric='messages'",
            (guild_id, user_id),
        )
        cur.execute(
            """
            INSERT INTO member_metrics_total (guild_id, user_id, metric, count)
            SELECT guild_id, user_id, 'messages', SUM(count)
            FROM member_metrics_daily
            WHERE guild_id=? AND user_id=? AND metric='messages'
            GROUP BY guild_id, user_id
            """,
            (guild_id, user_id),
        )
        con.commit()


def rebuild_month_from_days(guild_id: int, month: str) -> None:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            "DELETE FROM member_activity_monthly WHERE guild_id=? AND month=?",
            (guild_id, month),
        )
        cur.execute(
            """
            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
            SELECT guild_id, user_id, ?, SUM(count)
            FROM member_metrics_daily
            WHERE guild_id=? AND metric='messages' AND month=?
            GROUP BY guild_id, user_id
            """,
            (month, guild_id, month),
        )
        con.commit()
    rebuild_activity_totals_for_guild(guild_id)


# ---- merged toppers (unified + message_archive) ----

def top_members_period_merged(
    guild_id: int, metric: str, scope: str, key: str, limit: int
) -> List[Tuple[int, int]]:
    """
    Leaderboard for a period that includes counts from message_archive.
    Currently supported metrics: 'messages', 'words'.
    Other metrics fall back to the unified tables.
    """
    base = _top_members_by_period(guild_id, metric, scope, key, limit * 4)  # oversample before merge
    if metric not in ("messages", "words"):
        return base[:limit]
    with connect() as con:
        extra = _archive_counts_by_user(con, guild_id, scope, key, metric)
    return _merge_and_rank(base, extra, limit)


def top_members_total_merged(guild_id: int, metric: str, limit: int) -> List[Tuple[int, int]]:
    """
    All-time leaderboard including message_archive.
    For non-supported metrics, returns the unified totals as-is.
    """
    base = _top_members_total(guild_id, metric, limit * 4)
    if metric not in ("messages", "words"):
        return base[:limit]
    with connect() as con:
        extra = _archive_counts_by_user(con, guild_id, "all", None, metric)
    return _merge_and_rank(base, extra, limit)


__all__ = [
    "available_months",
    "bump_activity_join",
    "bump_activity_minutes",
    "bump_channel_message_total",
    "bump_member_emoji_chat",
    "bump_member_emoji_only",
    "bump_member_emoji_react",
    "bump_member_gifs",
    "bump_member_mentioned",
    "bump_member_mentions_sent",
    "bump_member_message",
    "bump_member_words",
    "bump_reactions_received",
    "bump_voice_minutes",
    "cleanup_activity",
    "ensure_activity_views",
    "fetch_metric_totals",
    "import_month_csv_rows",
    "member_daily_counts_month",
    "member_hour_histogram_total",
    "member_stats",
    "member_word_stats",
    "prime_channel_total",
    "rebuild_month_from_days",
    "reset_member_activity",
    "reset_member_emoji_chat",
    "reset_member_emoji_only",
    "reset_member_emoji_react",
    "reset_member_mentions",
    "reset_member_mentions_sent",
    "reset_member_words",
    "reset_member_reactions_received",
    "reset_member_channel_totals",
    "rebuild_activity_totals_for_guild",
    "top_members_emoji_chat_period",
    "top_members_emoji_chat_total",
    "top_members_emoji_react_period",
    "top_members_emoji_react_total",
    "top_members_mentions_period",
    "top_members_mentions_total",
    "top_members_messages_period",
    "top_members_messages_total",
    "top_members_words_period",
    "top_members_words_total",
    "upsert_member_messages_day",
    "upsert_member_messages_month",
    # merged
    "top_members_period_merged",
    "top_members_total_merged",
]
