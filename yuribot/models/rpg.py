from __future__ import annotations

import logging
import sqlite3
from math import log1p, sqrt
from typing import Dict, Tuple

from ..db import connect

log = logging.getLogger(__name__)

# ---- Tunables ---------------------------------------------------------------
# STR target: ~50 at 500 messages, diminishing returns via sqrt
# sqrt(500) ≈ 22.36 → 2.24 * 22.36 ≈ 50
K_STR_SQRT   = 2.24     # primary weight on message volume
K_STR_ECHAT  = 0.05     # tiny expressive bonus; DEX owns emoji signals

# WIS weights — give real credit to showing up (joins) + consistency + thoughtfulness
K_WIS_JOIN   = 3.5      # per activity joined (events/apps)
K_WIS_WEEKS  = 2.0      # distinct active ISO weeks in window
K_WIS_DAYS   = 0.5      # distinct active days in window
K_WIS_WPM    = 0.5      # bounded thoughtfulness score (words per message)

# ---- XP curve ---------------------------------------------------------------
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


# ---- Base XP rules (before per-channel multipliers in the cog) --------------
XP_RULES: Dict[str, float] = {
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
    # activity:
    "activity_minutes": 1,       # keep for compatibility; not used in WIS scoring
    "activity_joins": 5,         # NEW: XP credit for joining events/apps
    # keep both for compatibility with older code:
    "gifs": 1,
    "gif_use": 1,
}


# ---- Tunables ---------------------------------------------------------------
# STR target: ~50 @ 500 msgs over a week; ~45 @ 800 msgs in 1 burst day.
K_STR_BASE = 3.0         # base scale for sqrt(messages_7d)
STR_DAYS_EXP = 0.15      # dampen bursts: (active_days/7)^B
STR_MIN_ACTIVITY_FACTOR = 0.65  # ensure bursts still feel rewarding
K_STR_ECHAT = 0.03       # small expressive bonus from emoji in text

# INT: words depth (kept simple/cheap)
WORDS_PER_INT_POINT = 30  # floor(words/30)

# DEX: finesse signals benefit from presence but get softly capped
K_DEX_REACT = 3.0
K_DEX_MENTIONS = 2.5
K_DEX_EMOJI_ONLY = 1.5
DEX_LOG_SCALE = 8.0

# CHA: tone down and make sublinear with an explicit soft cap
K_CHA_RECV = 3.5          # weight on sqrt(mentions received)
K_CHA_REACT = 3.0         # weight on sqrt(reactions received)
K_CHA_SENT = 1.5          # small bump for sqrt(mentions sent)
CHA_LOG_SCALE = 12.0

# WIS: joins + consistency + thoughtfulness
K_WIS_JOIN = 6.0          # each app "join" (presence tick) is meaningful
K_WIS_WEEKS = 3.0         # active ISO-weeks in window
K_WIS_DAYS = 0.5          # active days in window
K_WIS_WPM = 0.5           # bounded WPM contribution (<=40)
WPM_CAP = 40.0


def _log_squash(value: float, scale: float) -> float:
    """Gently cap ``value`` using log1p while keeping small numbers unchanged."""
    if value <= 0:
        return 0.0
    return scale * log1p(value / scale)

def _stat_activity_scores(con: sqlite3.Connection, guild_id: int, user_id: int) -> Dict[str, float]:
    """
    Adaptive 7-day window with consistency dampening.
    - STR ≈ 50 for ~500 msgs over 7d; ≈ 45 for 800 msgs in a 1-day burst.
    - CHA toned down via sqrt scaling and smaller weights.
    - WIS leans on activity joins + consistency + thoughtfulness (WPM).
    """
    cur = con.cursor()

    # --- 7d aggregates from unified daily table ---
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

    # --- 7d "joins" from per-app activity table (any Discord app counts) ---
    row = cur.execute("""
        SELECT COALESCE(SUM(launches), 0)
        FROM member_activity_apps_daily
        WHERE guild_id=? AND user_id=? AND day >= date('now','-6 day')
    """, (guild_id, user_id)).fetchone()
    activity_joins = int(row[0] or 0)

    # --- Consistency (days & weeks active with any messages) ---
    row = cur.execute("""
        SELECT
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN day  END),
          COUNT(DISTINCT CASE WHEN metric='messages' AND count>0 THEN week END)
        FROM member_metrics_daily
        WHERE guild_id=? AND user_id=? AND day >= date('now','-6 day')
    """, (guild_id, user_id)).fetchone() or (0, 0)
    active_days, active_weeks = int(row[0] or 0), int(row[1] or 0)

    # --- Thoughtfulness (bounded WPM so essays don't dominate) ---
    wpm = words / max(messages, 1)
    wpm_score = min(float(wpm), WPM_CAP)

    # --- STR: sublinear + consistency dampener ---
    # For a pure 1-day burst (active_days=1), (1/7)^0.15 ≈ 0.722 → pulls 800 toward ~45.
    if active_days > 0:
        days_factor = (active_days / 7.0) ** STR_DAYS_EXP
        days_factor = max(STR_MIN_ACTIVITY_FACTOR, days_factor)
    else:
        days_factor = 0.0
    str_score = (K_STR_BASE * sqrt(float(messages)) * days_factor) + (K_STR_ECHAT * float(emoji_chat))

    # --- INT: simple depth from words ---
    int_score = float(words) // WORDS_PER_INT_POINT

    # --- CHA: reduce magnitude via sqrt + smaller weights ---
    cha_linear = (
        K_CHA_RECV  * sqrt(float(mentions_recv)) +
        K_CHA_REACT * sqrt(float(reacts_recv))  +
        K_CHA_SENT  * sqrt(float(mentions_sent))
    )
    cha_score = _log_squash(cha_linear, CHA_LOG_SCALE)

    # --- VIT: presence (voice + stream) ---
    vit_score = float(voice_min + stream_min)

    # --- DEX: finesse signals (reactions + emoji messaging)
    dex_linear = (
        K_DEX_REACT * log1p(float(emoji_react)) +
        K_DEX_MENTIONS * log1p(float(mentions_sent)) +
        K_DEX_EMOJI_ONLY * log1p(float(emoji_only_msgs))
    )
    dex_score = _log_squash(dex_linear, DEX_LOG_SCALE)

    # --- WIS: show up (joins) + consistency + thoughtfulness ---
    wis_score = (
        K_WIS_JOIN  * float(activity_joins) +
        K_WIS_WEEKS * float(active_weeks)   +
        K_WIS_DAYS  * float(active_days)    +
        K_WIS_WPM   * float(wpm_score)
    )

    return {
        "str": float(str_score),
        "int": float(int_score),
        "cha": float(cha_score),
        "vit": float(vit_score),
        "dex": float(dex_score),
        "wis": float(wis_score),
    }

_LEVELUP_DISTR: tuple[int, ...] = (4, 3, 2, 1, 1, 0)


def _apply_levelup_stats(con: sqlite3.Connection, guild_id: int, user_id: int) -> None:
    """
    Apply stat gains based on current 7-day activity ordering:
    +4, +3, +2, +1, +1, 0 to the 6 stats in rank order.
    Ties are stable by the order below.
    """
    cur = con.cursor()
    scores = _stat_activity_scores(con, guild_id, user_id)
    order = ["str", "dex", "int", "wis", "cha", "vit"]  # stable tiebreaker
    ranked = sorted(order, key=lambda k: (-scores.get(k, 0), order.index(k)))

    sets, params = [], []
    for stat, add in zip(ranked, _LEVELUP_DISTR):
        if add > 0:
            sets.append(f"{stat}={stat}+?")
            params.append(add)
    if not sets:
        return
    params += [guild_id, user_id]
    cur.execute(f"UPDATE member_rpg_progress SET {', '.join(sets)} WHERE guild_id=? AND user_id=?", params)


def _apply_xp(con: sqlite3.Connection, guild_id: int, user_id: int, add_xp: int) -> tuple[int, int]:
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
        _apply_levelup_stats(con, guild_id, user_id)
        cur.execute(
            """
            UPDATE member_rpg_progress
               SET level=?, xp=?, last_level_up=datetime('now')
             WHERE guild_id=? AND user_id=?
            """,
            (new_level, xp, guild_id, user_id),
        )
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
        row = cur.execute(
            """
            SELECT xp, level, str, int, cha, vit, dex, wis, COALESCE(last_level_up, '')
            FROM member_rpg_progress
            WHERE guild_id=? AND user_id=?
            """,
            (guild_id, user_id),
        ).fetchone()
        if not row:
            return {"xp": 0, "level": 1, "str": 5, "int": 5, "cha": 5, "vit": 5, "dex": 5, "wis": 5, "last_level_up": ""}
        return {
            "xp": int(row[0]),
            "level": int(row[1]),
            "str": int(row[2]),
            "int": int(row[3]),
            "cha": int(row[4]),
            "vit": int(row[5]),
            "dex": int(row[6]),
            "wis": int(row[7]),
            "last_level_up": row[8],
        }

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
        # unified metric used by scoring
        _upsert_metric_daily_and_total(
            con, guild_id, user_id, "activity_joins", day, week_key, month, int(joins)
        )
        # per-app rollup (joins -> launches)
        cur = con.cursor()
        cur.execute("""
            INSERT INTO member_activity_apps_daily (guild_id, user_id, app_name, day, minutes, launches)
            VALUES (?, ?, ?, ?, 0, ?)
            ON CONFLICT(guild_id, user_id, app_name, day) DO UPDATE SET
              launches = launches + excluded.launches
        """, (guild_id, user_id, (app_name or "(unknown)")[:80], day, int(joins)))
        con.commit()
        
def top_levels(guild_id: int, limit: int = 20) -> list[tuple[int, int, int]]:
    """[(user_id, level, xp)] sorted by level desc, xp desc."""
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT user_id, level, xp
            FROM member_rpg_progress
            WHERE guild_id=?
            ORDER BY level DESC, xp DESC
            LIMIT ?
            """,
            (guild_id, limit),
        ).fetchall()
        
# =============================================================================
# Mod actions (discipline)
# =============================================================================

def respec_stats_to_formula(guild_id: int, user_id: int | None = None) -> int:
    """
    Option B: Rebuild stat allocations for one member or all members in a guild
    using the *current* activity score ordering.

    - Keeps XP and level unchanged.
    - Resets str/int/cha/vit/dex/wis to 5 (base) and then allocates points according to
      the ranked activity scores using the +4,+3,+2,+1,+1,0 distribution for each level beyond 1.
    - Ties are broken by the stable order: ["str","dex","int","wis","cha","vit"].

    Returns number of members processed.
    """
    processed = 0
    base_stats = {"str": 5, "int": 5, "cha": 5, "vit": 5, "dex": 5, "wis": 5}
    tiebreak_order = ["str", "dex", "int", "wis", "cha", "vit"]

    with connect() as con:
        cur = con.cursor()

        if user_id is None:
            rows = cur.execute("""
                SELECT user_id, level
                FROM member_rpg_progress
                WHERE guild_id=?
            """, (guild_id,)).fetchall()
        else:
            rows = cur.execute("""
                SELECT user_id, level
                FROM member_rpg_progress
                WHERE guild_id=? AND user_id=?
            """, (guild_id, user_id)).fetchall()

        if not rows:
            return 0

        for uid, level in rows:
            lvl = int(level) if level is not None else 1
            levels_to_allocate = max(0, lvl - 1)

            # Compute activity ordering *now*
            scores = _stat_activity_scores(con, guild_id, int(uid))
            ranked = sorted(
                tiebreak_order,
                key=lambda k: (-float(scores.get(k, 0)), tiebreak_order.index(k))
            )

            # Aggregate gains once (distribution * levels_to_allocate)
            gains = {k: 0 for k in tiebreak_order}
            for stat, add in zip(ranked, _LEVELUP_DISTR):
                if add > 0 and levels_to_allocate > 0:
                    gains[stat] += add * levels_to_allocate

            # Build final values
            new_vals = {k: base_stats[k] + gains[k] for k in base_stats.keys()}

            # Write back (preserve xp, level, last_level_up)
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

    log.info("rpg.respec",
             extra={"guild_id": guild_id, "user_id": user_id, "processed": processed})
    return processed


__all__ = [
    'XP_RULES',
    'award_xp_for_event',
    'get_rpg_progress',
    'level_from_xp',
    'respec_stats_to_formula',
    'top_levels',
    'xp_progress',
]
