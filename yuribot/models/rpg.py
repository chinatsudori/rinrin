# yuribot/models/rpg.py
from __future__ import annotations
import logging
import sqlite3
from math import log1p, sqrt
from typing import Dict, Iterable, Mapping, Tuple, Optional

from ..db import connect

log = logging.getLogger(__name__)

# =========================
# XP curve & public helpers
# =========================

def _xp_for_level(level: int) -> int:
    """Total XP required to reach `level`. L1→0, L2→100, L3→282, L10≈5.7k, L20≈36k."""
    if level <= 1:
        return 0
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
    lvl = level_from_xp(total_xp)
    cur = total_xp - _xp_for_level(lvl)
    nxt = _xp_for_level(lvl + 1) - _xp_for_level(lvl)
    return (lvl, cur, nxt)

# =========
# XP rules
# =========

XP_RULES: Dict[str, float] = {
    "messages": 5,               # per message
    "words_per_20": 2,           # +2 per 20 words (floor)
    "voice_minutes": 1,
    "voice_stream_minutes": 2,
    "reactions_received": 1,
    "emoji_chat": 0.5,
    "emoji_react": 0.5,
    "mentions_received": 3,
    "mentions_sent": 1,
    "sticker_use": 2,
    "activity_minutes": 1,
    "activity_joins": 5,
    "gifs": 1,
    "gif_use": 1,
}

# =======================
# Internal table helpers
# =======================

def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    cur = con.cursor()
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return bool(row)

def _ensure_member_row(con: sqlite3.Connection, guild_id: int, user_id: int) -> tuple[int, int]:
    cur = con.cursor()
    row = cur.execute(
        "SELECT level, xp FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
        (guild_id, user_id),
    ).fetchone()
    if row:
        return int(row[0] or 1), int(row[1] or 0)
    cur.execute(
        """INSERT INTO member_rpg_progress
             (guild_id, user_id, level, xp, str, int, cha, vit, dex, wis)
           VALUES (?, ?, 1, 0, 5, 5, 5, 5, 5, 5)""",
        (guild_id, user_id),
    )
    return 1, 0
def _metrics_for_day(
    con: sqlite3.Connection, guild_id: int, user_id: int, day_iso: str
) -> Dict[str, int]:
    """
    Return counts for the XP-bearing metrics for a single day from member_metrics_daily.
    Missing metrics return 0. Compatible fallbacks included (e.g. reactions_sent vs emoji_react).
    """
    wanted = {
        "mentions_sent",
        "mentions",                 # received
        "emoji_chat",
        "reactions_received",
        "sticker_use",
        "voice_minutes",
        "voice_stream_minutes",
        "activity_joins",
        "gifs",
        "gif_use",
        "reactions_sent",           # if you store this explicitly
        "emoji_react",              # fallback alias for reactions sent
    }
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT metric, SUM(count)
          FROM member_metrics_daily
         WHERE guild_id=? AND user_id=? AND day=?
           AND metric IN ({})
         GROUP BY metric
        """.format(",".join("?" for _ in wanted)),
        (guild_id, user_id, day_iso, *wanted),
    ).fetchall()
    acc: Dict[str, int] = {k: 0 for k in wanted}
    for m, c in rows:
        acc[str(m)] = int(c or 0)

    # normalize aliases
    if acc["reactions_sent"] == 0 and acc["emoji_react"] > 0:
        acc["reactions_sent"] = acc["emoji_react"]

    return acc

# =====================================
# Chronological data feed (messages/words)
# =====================================
def _apply_increments_across_levels(
    con: sqlite3.Connection,
    guild_id: int,
    user_id: int,
    per_inc_xp: int,
    count: int,
    end_day_for_snapshot: str,
) -> None:
    """
    Apply `count` increments of `per_inc_xp`, crossing level boundaries correctly.
    On every level-up, snapshot stats using the 7-day window ending at `end_day_for_snapshot`.
    """
    if count <= 0 or per_inc_xp <= 0:
        return
    cur = con.cursor()
    row = cur.execute(
        "SELECT xp, level FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
        (guild_id, user_id),
    ).fetchone()
    if not row:
        cur.execute(
            "INSERT INTO member_rpg_progress (guild_id, user_id, xp, level, str, int, cha, vit, dex, wis) VALUES (?, ?, 0, 1, 5,5,5,5,5,5)",
            (guild_id, user_id),
        )
        xp, level = 0, 1
    else:
        xp, level = int(row[0]), int(row[1])

    while count > 0:
        need = _xp_for_level(level + 1) - xp
        if need <= 0:
            # already beyond next threshold (shouldn’t happen, but guard)
            level += 1
            continue
        # how many increments until the next level?
        steps = (need + per_inc_xp - 1) // per_inc_xp
        if steps <= count:
            # cross the boundary exactly at this chunk
            xp += steps * per_inc_xp
            level += 1
            count -= steps
            # write and snapshot level-up
            cur.execute("UPDATE member_rpg_progress SET level=?, xp=? WHERE guild_id=? AND user_id=?",
                        (level, xp, guild_id, user_id))
            _apply_levelup_stats_at_day(con, guild_id, user_id, end_day_for_snapshot)
            cur.execute(
                "UPDATE member_rpg_progress SET last_level_up=? WHERE guild_id=? AND user_id=?",
                (f"{end_day_for_snapshot} 12:00:00", guild_id, user_id),
            )
        else:
            # we won't reach the next level with remaining increments
            xp += count * per_inc_xp
            cur.execute("UPDATE member_rpg_progress SET xp=? WHERE guild_id=? AND user_id=?",
                        (xp, guild_id, user_id))
            count = 0

def _iter_daily_msgs_words(
    con: sqlite3.Connection,
    guild_id: int,
    user_id: int,
    since_day: Optional[str],
    until_day: Optional[str],
):
    """
    Yields (day_iso, msgs, words) ascending.
    Source priority:
      1) member_metrics_daily (metrics 'messages','words')
      2) member_messages_day (columns: messages, words?)  [words → 0 if absent]
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
        # Detect words column
        info = cur.execute("PRAGMA table_info(member_messages_day)").fetchall()
        has_words = any(str(col[1]).lower() == "words" for col in info)

        if has_words:
            q = "SELECT day, messages, words FROM member_messages_day WHERE guild_id=? AND user_id=?"
        else:
            q = "SELECT day, messages, 0 as words FROM member_messages_day WHERE guild_id=? AND user_id=?"
        params = [guild_id, user_id]
        if since_day:
            q += " AND day>=?"
            params.append(since_day)
        if until_day:
            q += " AND day<=?"
            params.append(until_day)
        q += " ORDER BY day ASC"
        for day_iso, msgs, words in cur.execute(q, params).fetchall():
            yield day_iso, int(msgs or 0), int(words or 0)

# ======================================
# 7-day window scoring at a point in time
# ======================================

# Tunables (kept pragmatic/light)
K_STR_BASE = 3.0
STR_DAYS_EXP = 0.15
STR_MIN_ACTIVITY_FACTOR = 0.65
K_STR_ECHAT = 0.03

WORDS_PER_INT_POINT = 30

K_DEX_REACT = 3.0
K_DEX_MENTIONS = 2.5
K_DEX_EMOJI_ONLY = 1.5
DEX_LOG_SCALE = 8.0

K_CHA_RECV = 3.5
K_CHA_REACT = 3.0
K_CHA_SENT = 1.5
CHA_LOG_SCALE = 12.0

K_WIS_JOIN = 6.0
K_WIS_WEEKS = 3.0
K_WIS_DAYS = 0.5
K_WIS_WPM  = 0.5
WPM_CAP = 40.0

def _log_squash(value: float, scale: float) -> float:
    if value <= 0:
        return 0.0
    return scale * log1p(value / scale)

def _sum_window(cur: sqlite3.Cursor, q: str, params: tuple) -> int:
    row = cur.execute(q, params).fetchone()
    return int(row[0] or 0)

def _scores_window(con: sqlite3.Connection, guild_id: int, user_id: int, end_day: str) -> Dict[str, float]:
    """
    Compute stat scores using [end_day-6, end_day] inclusive.
    Pulls from member_metrics_daily for metrics; falls back smartly if missing.
    """
    cur = con.cursor()
    start_q = "date(?, '-6 day')"  # computed by sqlite
    # messages/words/emoji/etc from member_metrics_daily
    def msum(metric: str) -> int:
        return _sum_window(
            cur,
            f"""SELECT COALESCE(SUM(count),0)
                  FROM member_metrics_daily
                 WHERE guild_id=? AND user_id=? AND metric=? AND day BETWEEN {start_q} AND ?""",
            (guild_id, user_id, metric, end_day, end_day),
        )

    messages        = msum("messages")
    words           = msum("words")
    mentions_recv   = msum("mentions")
    mentions_sent   = msum("mentions_sent")
    emoji_chat      = msum("emoji_chat")
    emoji_react     = msum("emoji_react")
    reacts_recv     = msum("reactions_received")
    voice_min       = msum("voice_minutes")
    stream_min      = msum("voice_stream_minutes")
    emoji_only      = msum("emoji_only")

    # activity joins
    activity_joins = _sum_window(
        cur,
        f"""SELECT COALESCE(SUM(launches),0)
               FROM member_activity_apps_daily
              WHERE guild_id=? AND user_id=? AND day BETWEEN {start_q} AND ?""",
        (guild_id, user_id, end_day, end_day),
    )

    # active days/weeks (by messages)
    row = cur.execute(
        f"""
        SELECT
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN day  END),
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN week END)
          FROM member_metrics_daily
         WHERE guild_id=? AND user_id=? AND day BETWEEN {start_q} AND ?
        """,
        (guild_id, user_id, end_day, end_day),
    ).fetchone() or (0, 0)
    active_days, active_weeks = int(row[0] or 0), int(row[1] or 0)

    # thoughtfulness
    wpm = words / max(messages, 1)
    wpm_score = min(float(wpm), WPM_CAP)

    # STR
    if active_days > 0:
        days_factor = (active_days / 7.0) ** STR_DAYS_EXP
        days_factor = max(STR_MIN_ACTIVITY_FACTOR, days_factor)
    else:
        days_factor = 0.0
    str_score = (K_STR_BASE * sqrt(float(messages)) * days_factor) + (K_STR_ECHAT * float(emoji_chat))

    # INT
    int_score = float(words) // WORDS_PER_INT_POINT

    # CHA
    cha_linear = (
        K_CHA_RECV  * sqrt(float(mentions_recv)) +
        K_CHA_REACT * sqrt(float(reacts_recv))  +
        K_CHA_SENT  * sqrt(float(mentions_sent))
    )
    cha_score = _log_squash(cha_linear, CHA_LOG_SCALE)

    # VIT
    vit_score = float(voice_min + stream_min)

    # DEX
    dex_linear = (
        K_DEX_REACT     * log1p(float(emoji_react)) +
        K_DEX_MENTIONS  * log1p(float(mentions_sent)) +
        K_DEX_EMOJI_ONLY* log1p(float(emoji_only))
    )
    dex_score = _log_squash(dex_linear, DEX_LOG_SCALE)

    # WIS
    wis_score = (
        K_WIS_JOIN  * float(activity_joins) +
        K_WIS_WEEKS * float(active_weeks)   +
        K_WIS_DAYS  * float(active_days)    +
        K_WIS_WPM   * float(wpm_score)
    )

    return {
        "str": float(str_score),
        "dex": float(dex_score),
        "int": float(int_score),
        "wis": float(wis_score),
        "cha": float(cha_score),
        "vit": float(vit_score),
    }

# ==============================
# Level-up stat application
# ==============================

_LEVELUP_DISTR: tuple[int, ...] = (4, 3, 2, 1, 1, 0)
_TIE_ORDER = ["str", "dex", "int", "wis", "cha", "vit"]

def _apply_levelup_stats_at_day(con: sqlite3.Connection, guild_id: int, user_id: int, end_day: str) -> None:
    """Allocate +4,+3,+2,+1,+1,0 using scores from the 7-day window ending at `end_day`."""
    scores = _scores_window(con, guild_id, user_id, end_day)
    ranked = sorted(_TIE_ORDER, key=lambda k: (-scores.get(k, 0.0), _TIE_ORDER.index(k)))

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

# ==============================
# XP application
# ==============================

def _apply_xp(con: sqlite3.Connection, guild_id: int, user_id: int, add_xp: int) -> tuple[int, int, int, int]:
    """
    Returns (old_level, new_level, old_xp, new_xp).
    """
    cur = con.cursor()
    row = cur.execute(
        "SELECT xp, level FROM member_rpg_progress WHERE guild_id=? AND user_id=?",
        (guild_id, user_id),
    ).fetchone()
    if not row:
        cur.execute(
            "INSERT INTO member_rpg_progress (guild_id, user_id, xp, level, str, int, cha, vit, dex, wis) VALUES (?, ?, 0, 1, 5,5,5,5,5,5)",
            (guild_id, user_id),
        )
        xp, level = 0, 1
    else:
        xp, level = int(row[0]), int(row[1])

    old_level, old_xp = level, xp

    xp += max(0, int(add_xp))
    new_level = level_from_xp(xp)

    if new_level != level:
        cur.execute("UPDATE member_rpg_progress SET level=?, xp=? WHERE guild_id=? AND user_id=?",
                    (new_level, xp, guild_id, user_id))
    else:
        cur.execute("UPDATE member_rpg_progress SET xp=? WHERE guild_id=? AND user_id=?",
                    (xp, guild_id, user_id))

    return old_level, new_level, old_xp, xp

# ==========================================
# Public operations (award, get, top, reset)
# ==========================================

def award_xp_for_event(guild_id: int, user_id: int, base_xp: float, channel_multiplier: float = 1.0) -> tuple[int, int]:
    add = int(round(max(0.0, base_xp) * max(0.0, channel_multiplier)))
    with connect() as con:
        _, new_level, _, new_xp = _apply_xp(con, guild_id, user_id, add)
        con.commit()
        return new_level, new_xp

def get_rpg_progress(guild_id: int, user_id: int) -> dict:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            """SELECT xp, level, str, int, cha, vit, dex, wis, COALESCE(last_level_up,'')
                 FROM member_rpg_progress
                WHERE guild_id=? AND user_id=?""",
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
            (guild_id, limit),
        ).fetchall()

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
    log.info("rpg.reset_progress", extra={"guild_id": guild_id, "user_id": user_id, "removed": affected})
    return int(affected)

# ==============================
# Chronological rebuild (correct)
# ==============================

def rebuild_progress_chronological(
    guild_id: int,
    user_id: int | None = None,
    *,
    since_day: str | None = None,
    until_day: str | None = None,
    reset: bool = True,
) -> int:
    """
    Rebuild XP & stats *chronologically*:
    - Award per message in day order.
    - On each level-up, compute a 7-day window ending at that day for stat allocation.
    """
    processed = 0
    with connect() as con:
        cur = con.cursor()

        # choose who to rebuild
        if user_id is None:
            users = [r[0] for r in cur.execute(
                "SELECT DISTINCT user_id FROM member_metrics_daily WHERE guild_id=? "
                "UNION SELECT DISTINCT user_id FROM member_messages_day WHERE guild_id=?",
                (guild_id, guild_id),
            ).fetchall()]
        else:
            users = [int(user_id)]

        if reset:
            if user_id is None:
                cur.execute("DELETE FROM member_rpg_progress WHERE guild_id=?", (guild_id,))
            else:
                cur.execute("DELETE FROM member_rpg_progress WHERE guild_id=? AND user_id=?", (guild_id, user_id))
            con.commit()

        for uid in users:
            level, xp = _ensure_member_row(con, guild_id, uid)

            had_any = False
            for day_iso, msgs, words in _iter_daily_msgs_words(con, guild_id, uid, since_day, until_day):
                had_any = True

                # 1) Messages (+ words-per-20 bonus baked into per-message XP)
                if msgs > 0:
                    wpm = (words / msgs) if msgs else 0.0
                    bonus_words = int(wpm // 20) * int(XP_RULES.get("words_per_20", 2))
                    per_msg_xp = max(0, int(XP_RULES.get("messages", 5)) + bonus_words)
                    _apply_increments_across_levels(con, guild_id, uid, per_msg_xp, int(msgs), day_iso)

                # 2) Other daily metrics — apply on the same day, boundary-aware
                m = _metrics_for_day(con, guild_id, uid, day_iso)

                # helpers to add integer (batched) or fractional totals
                def add_units(metric_name: str, unit_xp: float, count_val: int) -> None:
                    if count_val <= 0 or unit_xp <= 0:
                        return
                    total_xp = int(round(count_val * unit_xp))
                    if total_xp <= 0:
                        return
                    # apply as one big batch that still respects multi-level crossing
                    _apply_increments_across_levels(con, guild_id, uid, total_xp, 1, day_iso)

                # mentions / replies
                add_units("mentions_sent",       float(XP_RULES.get("mentions_sent", 1.0)),      int(m.get("mentions_sent", 0)))
                add_units("mentions_received",   float(XP_RULES.get("mentions_received", 3.0)),  int(m.get("mentions", 0)))

                # emoji in chat (+0.5 each)
                add_units("emoji_chat",          float(XP_RULES.get("emoji_chat", 0.5)),         int(m.get("emoji_chat", 0)))

                # reactions
                add_units("reactions_received",  float(XP_RULES.get("reactions_received", 1.0)), int(m.get("reactions_received", 0)))
                # reactions sent (fallback to emoji_react if you store that instead)
                add_units("reactions_sent",      float(XP_RULES.get("reactions_sent", 0.5)),     int(m.get("reactions_sent", 0)))

                # stickers
                add_units("sticker_use",         float(XP_RULES.get("sticker_use", 2.0)),        int(m.get("sticker_use", 0)))

                # voice / streaming minutes
                add_units("voice_minutes",       float(XP_RULES.get("voice_minutes", 1.0)),      int(m.get("voice_minutes", 0)))
                add_units("voice_stream_minutes",float(XP_RULES.get("voice_stream_minutes", 2.0)),int(m.get("voice_stream_minutes", 0)))

                # activity joins
                add_units("activity_joins",      float(XP_RULES.get("activity_joins", 5.0)),     int(m.get("activity_joins", 0)))

                # gifs
                # (support both 'gifs' and 'gif_use' for compatibility; whichever exists in your pipeline)
                gifs_total = int(m.get("gifs", 0) or m.get("gif_use", 0))
                add_units("gifs",                float(XP_RULES.get("gifs", 1.0)),               gifs_total)



            if had_any:
                processed += 1

        con.commit()

    log.info("rpg.rebuild_progress_chronological",
             extra={"guild_id": guild_id, "processed": processed,
                    "since_day": since_day, "until_day": until_day, "reset": reset})
    return processed

# export
__all__ = [
    "XP_RULES", "award_xp_for_event", "get_rpg_progress", "level_from_xp",
    "reset_progress", "top_levels", "xp_progress", "rebuild_progress_chronological",
]
