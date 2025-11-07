# yuribot/models/rpg.py
from __future__ import annotations

import logging
import sqlite3
from datetime import date, timedelta
from math import log1p, sqrt
from typing import Dict, List, Mapping, Tuple

from ..db import connect

log = logging.getLogger(__name__)

# =============================================================================
# XP curve
# =============================================================================

def _xp_for_level(level: int) -> int:
    """Total XP required to reach `level`. L1→0, L2→100, L3→282, L10≈5.7k, L20≈36k."""
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

# =============================================================================
# Base XP rules (before any channel multipliers the bot might apply)
# =============================================================================

XP_RULES: Dict[str, float] = {
    "messages": 5,               # per message
    "words_per_20": 2,           # +2 per 20 words (floor per message)
    "mentions_received": 3,
    "mentions_sent": 1,
    "emoji_chat": 0.5,
    "emoji_react": 0.5,
    "reactions_received": 1,     # per reaction received on your messages
    "sticker_use": 2,
    "voice_minutes": 1,
    "voice_stream_minutes": 2,
    # presence / activity credit:
    "activity_minutes": 1,       # compatibility
    "activity_joins": 5,
    "gifs": 1,                   # legacy compat
    "gif_use": 1,                # legacy compat
}

# =============================================================================
# Stat scoring (rolling 7d window)
# =============================================================================

# STR target: ~50 @ 500 msgs over a week; ~45 @ 800 msgs in 1-day burst.
K_STR_BASE = 3.0
STR_DAYS_EXP = 0.15
STR_MIN_ACTIVITY_FACTOR = 0.65
K_STR_ECHAT = 0.03

# INT
WORDS_PER_INT_POINT = 30  # floor(words/30)

# DEX
K_DEX_REACT = 3.0
K_DEX_MENTIONS = 2.5
K_DEX_EMOJI_ONLY = 1.5
DEX_LOG_SCALE = 8.0

# CHA
K_CHA_RECV = 3.5
K_CHA_REACT = 3.0
K_CHA_SENT = 1.5
CHA_LOG_SCALE = 12.0

# WIS
K_WIS_JOIN = 6.0
K_WIS_WEEKS = 3.0
K_WIS_DAYS = 0.5
K_WIS_WPM = 0.5
WPM_CAP = 40.0

def _log_squash(value: float, scale: float) -> float:
    if value <= 0:
        return 0.0
    return scale * log1p(value / scale)

# ---- utility iso helpers used in joins bump (self-contained) ----------------

def _iso(d: date) -> str:
    return d.isoformat()

def _parse_iso_day(s: str) -> date:
    return date.fromisoformat(s)

def _week_key(d: date) -> str:
    iso = d.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

def _month_key(d: date) -> str:
    return f"{d.year}-{d.month:02d}"

# upsert helpers for unified metric tables (daily + totals)
def _ensure_metric_tables(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS member_metrics_daily(
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            day      TEXT NOT NULL,   -- YYYY-MM-DD
            week     TEXT NOT NULL,   -- YYYY-Www
            month    TEXT NOT NULL,   -- YYYY-MM
            metric   TEXT NOT NULL,
            count    INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id, metric, day)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS member_metrics_total(
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            metric   TEXT NOT NULL,
            count    INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id, metric)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS member_activity_apps_daily(
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            app_name TEXT NOT NULL,
            day      TEXT NOT NULL,
            minutes  INTEGER NOT NULL,
            launches INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id, app_name, day)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS member_rpg_progress(
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            level    INTEGER NOT NULL DEFAULT 1,
            xp       INTEGER NOT NULL DEFAULT 0,
            str      INTEGER NOT NULL DEFAULT 5,
            int      INTEGER NOT NULL DEFAULT 5,
            cha      INTEGER NOT NULL DEFAULT 5,
            vit      INTEGER NOT NULL DEFAULT 5,
            dex      INTEGER NOT NULL DEFAULT 5,
            wis      INTEGER NOT NULL DEFAULT 5,
            last_level_up TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    con.commit()

def _upsert_metric_daily_and_total(
    con: sqlite3.Connection,
    guild_id: int,
    user_id: int,
    metric: str,
    day: str,
    week: str,
    month: str,
    delta: int,
) -> None:
    _ensure_metric_tables(con)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO member_metrics_daily(guild_id,user_id,day,week,month,metric,count)
        VALUES(?,?,?,?,?, ?,?)
        ON CONFLICT(guild_id,user_id,metric,day)
        DO UPDATE SET count = count + excluded.count
    """, (guild_id, user_id, day, week, month, metric, int(delta)))
    cur.execute("""
        INSERT INTO member_metrics_total(guild_id,user_id,metric,count)
        VALUES(?,?,?,?)
        ON CONFLICT(guild_id,user_id,metric)
        DO UPDATE SET count = count + excluded.count
    """, (guild_id, user_id, metric, int(delta)))

# ---- 7-day scores (NOW) -----------------------------------------------------

def _stat_activity_scores(con: sqlite3.Connection, guild_id: int, user_id: int) -> Dict[str, float]:
    """7-day window ending today (SQLite now())."""
    cur = con.cursor()
    rows = cur.execute("""
        SELECT metric, SUM(count)
        FROM member_metrics_daily
        WHERE guild_id=? AND user_id=? AND day >= date('now','-6 day')
        GROUP BY metric
    """, (guild_id, user_id)).fetchall()
    t = {m: int(c) for (m, c) in rows}

    messages        = t.get("messages", 0)
    words           = t.get("words", 0)
    mentions_recv   = t.get("mentions", 0)
    mentions_sent   = t.get("mentions_sent", 0)
    emoji_chat      = t.get("emoji_chat", 0)
    emoji_react     = t.get("emoji_react", 0)
    reacts_recv     = t.get("reactions_received", 0)
    voice_min       = t.get("voice_minutes", 0)
    stream_min      = t.get("voice_stream_minutes", 0)
    emoji_only_msgs = t.get("emoji_only", 0)

    row = cur.execute("""
        SELECT COALESCE(SUM(launches), 0)
        FROM member_activity_apps_daily
        WHERE guild_id=? AND user_id=? AND day >= date('now','-6 day')
    """, (guild_id, user_id)).fetchone()
    activity_joins = int(row[0] or 0)

    row = cur.execute("""
        SELECT
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN day  END),
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN week END)
        FROM member_metrics_daily
        WHERE guild_id=? AND user_id=? AND day >= date('now','-6 day')
    """, (guild_id, user_id)).fetchone() or (0, 0)
    active_days, active_weeks = int(row[0] or 0), int(row[1] or 0)

    wpm = words / max(messages, 1)
    wpm_score = min(float(wpm), WPM_CAP)

    if active_days > 0:
        days_factor = (active_days / 7.0) ** STR_DAYS_EXP
        days_factor = max(STR_MIN_ACTIVITY_FACTOR, days_factor)
    else:
        days_factor = 0.0
    str_score = (K_STR_BASE * sqrt(float(messages)) * days_factor) + (K_STR_ECHAT * float(emoji_chat))

    int_score = float(words) // WORDS_PER_INT_POINT

    cha_linear = (
        K_CHA_RECV  * sqrt(float(mentions_recv)) +
        K_CHA_REACT * sqrt(float(reacts_recv))  +
        K_CHA_SENT  * sqrt(float(mentions_sent))
    )
    cha_score = _log_squash(cha_linear, CHA_LOG_SCALE)

    vit_score = float(voice_min + stream_min)

    dex_linear = (
        K_DEX_REACT * log1p(float(emoji_react)) +
        K_DEX_MENTIONS * log1p(float(mentions_sent)) +
        K_DEX_EMOJI_ONLY * log1p(float(emoji_only_msgs))
    )
    dex_score = _log_squash(dex_linear, DEX_LOG_SCALE)

    wis_score = (
        K_WIS_JOIN  * float(activity_joins) +
        K_WIS_WEEKS * float(active_weeks)   +
        K_WIS_DAYS  * float(active_days)    +
        K_WIS_WPM   * float(wpm_score)
    )

    return {"str": float(str_score), "int": float(int_score), "cha": float(cha_score),
            "vit": float(vit_score), "dex": float(dex_score), "wis": float(wis_score)}

# ---- 7-day scores at arbitrary day (for chronological allocation) -----------

def _stat_activity_scores_at(con: sqlite3.Connection, guild_id: int, user_id: int, day_iso: str) -> Dict[str, float]:
    end = _parse_iso_day(day_iso)
    start = end - timedelta(days=6)
    cur = con.cursor()

    rows = cur.execute("""
        SELECT metric, SUM(count)
        FROM member_metrics_daily
        WHERE guild_id=? AND user_id=? AND day BETWEEN ? AND ?
        GROUP BY metric
    """, (guild_id, user_id, _iso(start), _iso(end))).fetchall()
    t = {m: int(c) for (m, c) in rows}

    messages        = t.get("messages", 0)
    words           = t.get("words", 0)
    mentions_recv   = t.get("mentions", 0)
    mentions_sent   = t.get("mentions_sent", 0)
    emoji_chat      = t.get("emoji_chat", 0)
    emoji_react     = t.get("emoji_react", 0)
    reacts_recv     = t.get("reactions_received", 0)
    voice_min       = t.get("voice_minutes", 0)
    stream_min      = t.get("voice_stream_minutes", 0)
    emoji_only_msgs = t.get("emoji_only", 0)

    row = cur.execute("""
        SELECT COALESCE(SUM(launches), 0)
        FROM member_activity_apps_daily
        WHERE guild_id=? AND user_id=? AND day BETWEEN ? AND ?
    """, (guild_id, user_id, _iso(start), _iso(end))).fetchone()
    activity_joins = int((row or (0,))[0] or 0)

    row = cur.execute("""
        SELECT
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN day  END),
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN week END)
        FROM member_metrics_daily
        WHERE guild_id=? AND user_id=? AND day BETWEEN ? AND ?
    """, (guild_id, user_id, _iso(start), _iso(end))).fetchone() or (0, 0)
    active_days, active_weeks = int(row[0] or 0), int(row[1] or 0)

    wpm = words / max(messages, 1)
    wpm_score = min(float(wpm), WPM_CAP)

    if active_days > 0:
        days_factor = (active_days / 7.0) ** STR_DAYS_EXP
        days_factor = max(STR_MIN_ACTIVITY_FACTOR, days_factor)
    else:
        days_factor = 0.0
    str_score = (K_STR_BASE * sqrt(float(messages)) * days_factor) + (K_STR_ECHAT * float(emoji_chat))
    int_score = float(words) // WORDS_PER_INT_POINT

    cha_linear = (
        K_CHA_RECV  * sqrt(float(mentions_recv)) +
        K_CHA_REACT * sqrt(float(reacts_recv))  +
        K_CHA_SENT  * sqrt(float(mentions_sent))
    )
    cha_score = _log_squash(cha_linear, CHA_LOG_SCALE)

    vit_score = float(voice_min + stream_min)

    dex_linear = (
        K_DEX_REACT * log1p(float(emoji_react)) +
        K_DEX_MENTIONS * log1p(float(mentions_sent)) +
        K_DEX_EMOJI_ONLY * log1p(float(emoji_only_msgs))
    )
    dex_score = _log_squash(dex_linear, DEX_LOG_SCALE)

    wis_score = (
        K_WIS_JOIN  * float(activity_joins) +
        K_WIS_WEEKS * float(active_weeks)   +
        K_WIS_DAYS  * float(active_days)    +
        K_WIS_WPM   * float(wpm_score)
    )

    return {"str": float(str_score), "int": float(int_score), "cha": float(cha_score),
            "vit": float(vit_score), "dex": float(dex_score), "wis": float(wis_score)}

# =============================================================================
# Progress rows + allocation
# =============================================================================

_LEVELUP_DISTR: tuple[int, ...] = (4, 3, 2, 1, 1, 0)
_TIE_ORDER: List[str] = ["str", "dex", "int", "wis", "cha", "vit"]  # stable

def _apply_levelup_stats(con: sqlite3.Connection, guild_id: int, user_id: int) -> None:
    scores = _stat_activity_scores(con, guild_id, user_id)
    ranked = sorted(_TIE_ORDER, key=lambda k: (-scores.get(k, 0), _TIE_ORDER.index(k)))
    _apply_ranked_awards(con, guild_id, user_id, ranked)

def _apply_levelup_stats_at_day(con: sqlite3.Connection, guild_id: int, user_id: int, day_iso: str) -> None:
    scores = _stat_activity_scores_at(con, guild_id, user_id, day_iso)
    ranked = sorted(_TIE_ORDER, key=lambda k: (-scores.get(k, 0), _TIE_ORDER.index(k)))
    _apply_ranked_awards(con, guild_id, user_id, ranked)

def _apply_ranked_awards(con: sqlite3.Connection, guild_id: int, user_id: int, ranked: List[str]) -> None:
    sets, params = [], []
    for stat, add in zip(ranked, _LEVELUP_DISTR):
        if add > 0:
            sets.append(f"{stat}={stat}+?")
            params.append(add)
    if not sets:
        return
    params += [guild_id, user_id]
    cur = con.cursor()
    cur.execute(f"UPDATE member_rpg_progress SET {', '.join(sets)} WHERE guild_id=? AND user_id=?", params)

def _apply_xp(con: sqlite3.Connection, guild_id: int, user_id: int, add_xp: int) -> tuple[int, int]:
    """Return (new_level, total_xp)."""
    cur = con.cursor()
    row = cur.execute(
        "SELECT xp, level FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
        (guild_id, user_id),
    ).fetchone()
    if not row:
        cur.execute(
            "INSERT INTO member_rpg_progress (guild_id, user_id, xp, level) VALUES (?, ?, 0, 1)",
            (guild_id, user_id),
        )
        xp, level = 0, 1
    else:
        xp, level = int(row[0]), int(row[1])

    xp += max(0, int(add_xp))
    new_level = level_from_xp(xp)

    if new_level > level:
        # Default allocation (NOW) if caller doesn’t do day-specific allocation
        _apply_levelup_stats(con, guild_id, user_id)
        cur.execute(
            """UPDATE member_rpg_progress
                  SET level=?, xp=?, last_level_up=datetime('now')
                WHERE guild_id=? AND user_id=?""",
            (new_level, xp, guild_id, user_id),
        )
    else:
        cur.execute("UPDATE member_rpg_progress SET xp=? WHERE guild_id=? AND user_id=?",
                    (xp, guild_id, user_id))
    return new_level, xp

def _ensure_member_row(con: sqlite3.Connection, guild_id: int, user_id: int) -> tuple[int, int]:
    cur = con.cursor()
    row = cur.execute(
        "SELECT level, xp FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
        (guild_id, user_id),
    ).fetchone()
    if row:
        return int(row[0] or 1), int(row[1] or 0)
    cur.execute("""
        INSERT INTO member_rpg_progress (guild_id, user_id, level, xp, str, int, cha, vit, dex, wis)
        VALUES (?, ?, 1, 0, 5, 5, 5, 5, 5, 5)
    """, (guild_id, user_id))
    return 1, 0

# =============================================================================
# Public API
# =============================================================================

def award_xp_for_event(guild_id: int, user_id: int, base_xp: float, channel_multiplier: float = 1.0) -> tuple[int, int]:
    """Round after multiplier; returns (new_level, total_xp)."""
    add = int(round(max(0.0, base_xp) * max(0.0, channel_multiplier)))
    with connect() as con:
        lvl, xp = _apply_xp(con, guild_id, user_id, add)
        con.commit()
        return lvl, xp

def get_rpg_progress(guild_id: int, user_id: int) -> dict:
    with connect() as con:
        _ensure_metric_tables(con)
        cur = con.cursor()
        row = cur.execute(
            """SELECT xp, level, str, int, cha, vit, dex, wis, COALESCE(last_level_up, '')
               FROM member_rpg_progress WHERE guild_id=? AND user_id=?""",
            (guild_id, user_id),
        ).fetchone()
        if not row:
            return {"xp": 0, "level": 1, "str": 5, "int": 5, "cha": 5, "vit": 5, "dex": 5, "wis": 5, "last_level_up": ""}
        return {
            "xp": int(row[0]), "level": int(row[1]),
            "str": int(row[2]), "int": int(row[3]), "cha": int(row[4]),
            "vit": int(row[5]), "dex": int(row[6]), "wis": int(row[7]),
            "last_level_up": row[8],
        }

def top_levels(guild_id: int, limit: int = 20) -> list[tuple[int, int, int]]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """SELECT user_id, level, xp
               FROM member_rpg_progress
               WHERE guild_id=?
               ORDER BY level DESC, xp DESC
               LIMIT ?""",
            (guild_id, max(1, int(limit))),
        ).fetchall()

# --- Chronological rebuild (per message) -------------------------------------
# --- Add to yuribot/models/rpg.py -------------------------------------------

def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    cur = con.cursor()
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return bool(row)

def _iter_daily_msgs_words(
    con: sqlite3.Connection,
    guild_id: int,
    user_id: int,
    since_day: str | None,
    until_day: str | None,
):
    """
    Yields (day_iso, msgs, words) in ascending day order using the *best available* source:
      1) member_metrics_daily (metrics: 'messages', 'words')
      2) member_messages_day (columns: messages, words?)  -- words optional
    """
    cur = con.cursor()

    if _table_exists(con, "member_metrics_daily"):
        q = """
            SELECT day,
                   SUM(CASE WHEN metric='messages' THEN count ELSE 0 END) AS msgs,
                   SUM(CASE WHEN metric='words'    THEN count ELSE 0 END) AS words
            FROM member_metrics_daily
            WHERE guild_id=? AND user_id=?
        """
        params: list[object] = [guild_id, user_id]
        if since_day:
            q += " AND day>=?"
            params.append(since_day)
        if until_day:
            q += " AND day<=?"
            params.append(until_day)
        q += " GROUP BY day ORDER BY day ASC"
        for day_iso, msgs, words in cur.execute(q, params).fetchall():
            yield day_iso, int(msgs or 0), int(words or 0)
        return

    if _table_exists(con, "member_messages_day"):
        # words column may be absent in some deployments; guard it.
        cols = dict(cur.execute("PRAGMA table_info(member_messages_day)").fetchall() or [])
        has_words = any(str(c[1]).lower() == "words" for c in cur.execute("PRAGMA table_info(member_messages_day)"))

        if has_words:
            q = """
                SELECT day, messages, words
                FROM member_messages_day
                WHERE guild_id=? AND user_id=?
            """
        else:
            q = """
                SELECT day, messages, 0 as words
                FROM member_messages_day
                WHERE guild_id=? AND user_id=?
            """
        params: list[object] = [guild_id, user_id]
        if since_day:
            q += " AND day>=?"
            params.append(since_day)
        if until_day:
            q += " AND day<=?"
            params.append(until_day)
        q += " ORDER BY day ASC"
        for day_iso, msgs, words in cur.execute(q, params).fetchall():
            yield day_iso, int(msgs or 0), int(words or 0)
        return

    # Nothing? Fail closed (don’t silently give tiny XP).
    return

def rebuild_progress_chronological(
    guild_id: int,
    user_id: int | None = None,
    *,
    since_day: str | None = None,
    until_day: str | None = None,
    reset: bool = True,
) -> int:
    processed = 0
    with connect() as con:
        _ensure_metric_tables(con)
        cur = con.cursor()

        # Who to rebuild?
        if user_id is None:
            users = [r[0] for r in cur.execute(
                "SELECT DISTINCT user_id FROM member_metrics_daily WHERE guild_id=? "
                "UNION SELECT DISTINCT user_id FROM member_messages_day WHERE guild_id=?",
                (guild_id, guild_id),
            ).fetchall()]
        else:
            users = [int(user_id)]

        # Optional reset
        if reset:
            if user_id is None:
                cur.execute("DELETE FROM member_rpg_progress WHERE guild_id=?", (guild_id,))
            else:
                cur.execute("DELETE FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
                            (guild_id, user_id))
            con.commit()

        for uid in users:
            level, total_xp = _ensure_member_row(con, guild_id, uid)

            had_rows = False
            for day_iso, msgs, words in _iter_daily_msgs_words(con, guild_id, uid, since_day, until_day):
                had_rows = True
                if msgs <= 0:
                    continue

                # Per-message XP: 5 + 2 per 20 avg words/message (floor)
                wpm = (words / msgs) if msgs else 0.0
                bonus_words = int(wpm // 20) * int(XP_RULES.get("words_per_20", 2))
                per_msg_xp = int(XP_RULES.get("messages", 5)) + bonus_words

                # Award once per message; allocate on level-ups using the 7d window ending on day_iso
                for _ in range(int(msgs)):
                    new_level, total_xp = _apply_xp(con, guild_id, uid, per_msg_xp)
                    if new_level > level:
                        _apply_levelup_stats_at_day(con, guild_id, uid, day_iso)
                        cur.execute(
                            "UPDATE member_rpg_progress SET level=?, last_level_up=? WHERE guild_id=? AND user_id=?",
                            (new_level, f"{day_iso} 12:00:00", guild_id, uid),
                        )
                        level = new_level

            if had_rows:
                processed += 1

        con.commit()

    log.info("rpg.rebuild_progress_chronological",
             extra={"guild_id": guild_id, "users": processed, "reset": reset,
                    "since": since_day, "until": until_day})
    return processed

# --- Snapshot respec (keep XP/level) -----------------------------------------

def respec_stats_to_formula(guild_id: int, user_id: int | None = None) -> int:
    """
    Rebuild stat allocations for one or all members using the *current* 7d activity ranking.
    Keeps XP and level unchanged. Base 5 for all stats, then apply (+4,+3,+2,+1,+1,0) for each level-1.
    """
    processed = 0
    base_stats = {"str": 5, "int": 5, "cha": 5, "vit": 5, "dex": 5, "wis": 5}

    with connect() as con:
        _ensure_metric_tables(con)
        cur = con.cursor()

        if user_id is None:
            rows = cur.execute("SELECT user_id, level FROM member_rpg_progress WHERE guild_id=?",
                               (guild_id,)).fetchall()
        else:
            rows = cur.execute("SELECT user_id, level FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
                               (guild_id, user_id)).fetchall()

        if not rows:
            return 0

        for uid, level in rows:
            lvl = int(level) if level is not None else 1
            levels_to_allocate = max(0, lvl - 1)

            scores = _stat_activity_scores(con, guild_id, int(uid))
            ranked = sorted(_TIE_ORDER, key=lambda k: (-float(scores.get(k, 0)), _TIE_ORDER.index(k)))

            gains = {k: 0 for k in _TIE_ORDER}
            for stat, add in zip(ranked, _LEVELUP_DISTR):
                if add > 0 and levels_to_allocate > 0:
                    gains[stat] += add * levels_to_allocate

            new_vals = {k: base_stats[k] + gains[k] for k in base_stats.keys()}

            cur.execute("""
                UPDATE member_rpg_progress
                   SET str=?, int=?, cha=?, vit=?, dex=?, wis=?
                 WHERE guild_id=? AND user_id=?
            """, (
                int(new_vals["str"]), int(new_vals["int"]), int(new_vals["cha"]),
                int(new_vals["vit"]), int(new_vals["dex"]), int(new_vals["wis"]),
                guild_id, int(uid)
            ))
            processed += 1

        con.commit()

    log.info("rpg.respec", extra={"guild_id": guild_id, "user_id": user_id, "processed": processed})
    return processed

# --- Apply explicit stat snapshot (admin import) ------------------------------

def apply_stat_snapshot(guild_id: int, stat_map: Mapping[int, Mapping[str, int]]) -> int:
    if not stat_map:
        return 0
    processed = 0
    with connect() as con:
        _ensure_metric_tables(con)
        cur = con.cursor()
        for uid, stats in stat_map.items():
            try:
                row = cur.execute(
                    "SELECT level, xp FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
                    (guild_id, int(uid)),
                ).fetchone()
                if row:
                    cur.execute("""
                        UPDATE member_rpg_progress
                           SET str=?, int=?, cha=?, vit=?, dex=?, wis=?
                         WHERE guild_id=? AND user_id=?
                    """, (
                        int(stats.get("str", 5)), int(stats.get("int", 5)), int(stats.get("cha", 5)),
                        int(stats.get("vit", 5)), int(stats.get("dex", 5)), int(stats.get("wis", 5)),
                        guild_id, int(uid),
                    ))
                else:
                    cur.execute("""
                        INSERT INTO member_rpg_progress
                            (guild_id, user_id, level, xp, str, int, cha, vit, dex, wis)
                        VALUES (?, ?, 1, 0, ?, ?, ?, ?, ?, ?)
                    """, (
                        guild_id, int(uid),
                        int(stats.get("str", 5)), int(stats.get("int", 5)), int(stats.get("cha", 5)),
                        int(stats.get("vit", 5)), int(stats.get("dex", 5)), int(stats.get("wis", 5)),
                    ))
                processed += 1
            except Exception:
                log.exception("rpg.apply_stat_snapshot.failed", extra={"guild_id": guild_id, "user_id": uid})
        con.commit()
    log.info("rpg.apply_stat_snapshot", extra={"guild_id": guild_id, "processed": processed})
    return processed

# --- Admin: reset rows --------------------------------------------------------

def reset_progress(guild_id: int, user_id: int | None = None) -> int:
    with connect() as con:
        cur = con.cursor()
        if user_id is None:
            cur.execute("DELETE FROM member_rpg_progress WHERE guild_id=?", (guild_id,))
            affected = cur.rowcount or 0
        else:
            cur.execute("DELETE FROM member_rpg_progress WHERE guild_id=? AND user_id=?", (guild_id, user_id))
            affected = cur.rowcount or 0
        con.commit()
    log.info("rpg.reset_progress", extra={"guild_id": guild_id, "user_id": user_id, "removed": int(affected)})
    return int(affected)

# --- Presence: join bumps (kept for compatibility) ---------------------------

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
    d = _parse_iso_day(when_iso[:10])
    day = _iso(d)
    week_key = _week_key(d)
    month = _month_key(d)
    with connect() as con:
        _ensure_metric_tables(con)
        _upsert_metric_daily_and_total(con, guild_id, user_id, "activity_joins", day, week_key, month, int(joins))
        cur = con.cursor()
        cur.execute("""
            INSERT INTO member_activity_apps_daily (guild_id, user_id, app_name, day, minutes, launches)
            VALUES (?, ?, ?, ?, 0, ?)
            ON CONFLICT(guild_id, user_id, app_name, day) DO UPDATE SET
              launches = launches + excluded.launches
        """, (guild_id, user_id, (app_name or "(unknown)")[:80], day, int(joins)))
        con.commit()

# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "XP_RULES",
    "award_xp_for_event",
    "get_rpg_progress",
    "level_from_xp",
    "apply_stat_snapshot",
    "respec_stats_to_formula",
    "reset_progress",
    "top_levels",
    "xp_progress",
    "rebuild_progress_chronological",
    "bump_activity_join",
]
