from __future__ import annotations

try:
    from . import config
except Exception:
    import config

import os
import sqlite3
import logging
from typing import Optional, Iterable

from .data.booly_defaults import DEFAULT_BOOLY_ROWS

log = logging.getLogger("yuribot.db")


# ----------------------------
# Path resolution
# ----------------------------
def _resolved_db_path() -> str:
    env = os.environ.get("BOT_DB_PATH")
    if env:
        return os.path.abspath(env)
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "data", "bot.sqlite3")
    )


# ----------------------------
# Helpers
# ----------------------------
def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    cur = con.cursor()
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return bool(row)


def _columns(con: sqlite3.Connection, table: str) -> list[str]:
    cur = con.cursor()
    try:
        return [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    except Exception:
        return []


def _ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if not _table_exists(con, table):
        return
    cols = _columns(con, table)
    if column not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _table_sql(con: sqlite3.Connection, table: str) -> Optional[str]:
    cur = con.cursor()
    row = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row[0] if row else None


def _drop_if_exists(con: sqlite3.Connection, table_names: Iterable[str]) -> None:
    cur = con.cursor()
    for t in table_names:
        if t == "sqlite_sequence":
            continue
        try:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception as e:
            log.warning("drop table %s failed: %s", t, e)


# ----------------------------
# Freshness guard
# ----------------------------
def _any_rows(con: sqlite3.Connection, table: str) -> bool:
    if not _table_exists(con, table):
        return False
    cur = con.cursor()
    try:
        n = cur.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return bool(n)
    except Exception:
        return False


def _is_fresh_db(path: str) -> bool:
    if not os.path.exists(path):
        return True
    try:
        con = sqlite3.connect(path, timeout=5)
        core_tables = ("guild_settings",)
        for t in core_tables:
            if _any_rows(con, t):
                con.close()
                return False
        con.close()
        return True
    except Exception:
        return True


# ----------------------------
# Public: connect() / ensure_db()
# ----------------------------
def connect() -> sqlite3.Connection:
    path = _resolved_db_path()
    if os.getenv("DB_REQUIRE_PERSISTENCE") == "1" and _is_fresh_db(path):
        raise RuntimeError(
            f"Refusing to start on fresh DB: {path}. "
            "Set BOT_DB_PATH to a persistent location (e.g. a Docker volume) "
            "or unset DB_REQUIRE_PERSISTENCE."
        )

    con = sqlite3.connect(path, timeout=5)
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=3000")
    return con


def ensure_db() -> None:
    """
    Idempotently create/upgrade the tables actually used by the bot
    and drop known-unused legacy tables. Also fixes guild_settings schema.
    """
    path = _resolved_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with sqlite3.connect(path, timeout=5) as con:
        cur = con.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        journal = cur.execute("PRAGMA journal_mode").fetchone()[0]
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA busy_timeout=3000")

        try:
            size = os.stat(path).st_size
        except FileNotFoundError:
            size = 0
        log.info("db.open path=%s size=%d journal=%s", path, size, journal)

        # ========== FIX guild_settings schema conflict ==========
        # If an old key/value table is sitting under the name 'guild_settings',
        # rename it to 'guild_kv' first.
        if _table_exists(con, "guild_settings"):
            cols = set(_columns(con, "guild_settings"))
            if {"key", "value"}.issubset(cols) and "mod_logs_channel_id" not in cols:
                # This is the KV table misnamed as guild_settings -> rename.
                cur.execute("ALTER TABLE guild_settings RENAME TO guild_kv")

        # Ensure proper columnized guild_settings
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                mod_logs_channel_id INTEGER,
                bot_logs_channel_id INTEGER,
                welcome_channel_id INTEGER,
                welcome_image_filename TEXT,
                mu_forum_channel_id INTEGER
            )
            """
        )
        _ensure_column(con, "guild_settings", "welcome_channel_id", "INTEGER")
        _ensure_column(con, "guild_settings", "welcome_image_filename", "TEXT")
        _ensure_column(con, "guild_settings", "mu_forum_channel_id", "INTEGER")

        # Ensure proper key/value store as guild_kv
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS guild_kv (
                guild_id INTEGER NOT NULL,
                key      TEXT    NOT NULL,
                value    TEXT,
                PRIMARY KEY (guild_id, key)
            )
            """
        )

        # ========== clubs ==========
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS clubs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                club_type TEXT NOT NULL,
                announcements_channel_id INTEGER,
                planning_forum_id INTEGER,
                polls_channel_id INTEGER,
                discussion_forum_id INTEGER,
                UNIQUE(guild_id, club_type)
            )
            """
        )
        # Drop any historical CHECK constraint variant
        sql = _table_sql(con, "clubs")
        if sql and "CHECK" in sql.upper():
            cur.execute("ALTER TABLE clubs RENAME TO clubs_old")
            cur.execute(
                """
                CREATE TABLE clubs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    club_type TEXT NOT NULL,
                    announcements_channel_id INTEGER,
                    planning_forum_id INTEGER,
                    polls_channel_id INTEGER,
                    discussion_forum_id INTEGER,
                    UNIQUE(guild_id, club_type)
                )
                """
            )
            cur.execute(
                """
                INSERT OR IGNORE INTO clubs (id, guild_id, club_type, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id)
                SELECT id, guild_id, club_type, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id
                FROM clubs_old
                """
            )
            cur.execute("DROP TABLE clubs_old")

        # ========== booly_messages ==========
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS booly_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                user_id INTEGER,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_booly_scope ON booly_messages (scope, user_id)"
        )
        existing_booly = cur.execute("SELECT COUNT(1) FROM booly_messages").fetchone()[
            0
        ]
        if not existing_booly:
            cur.executemany(
                "INSERT INTO booly_messages (scope, user_id, content) VALUES (?, ?, ?)",
                DEFAULT_BOOLY_ROWS,
            )

        # ========== mod_actions ==========
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mod_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                target_username TEXT,
                rule TEXT NOT NULL,
                offense INTEGER NOT NULL,
                action TEXT NOT NULL,
                details TEXT,
                evidence_url TEXT,
                actor_user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_mod_actions_lookup ON mod_actions (guild_id, target_user_id, id DESC)"
        )

        # ========== message_archive ==========
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS message_archive (
                message_id  INTEGER PRIMARY KEY,
                guild_id    INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                author_id   INTEGER NOT NULL,
                message_type TEXT    NOT NULL,
                created_at  TEXT    NOT NULL,
                content     TEXT,
                edited_at   TEXT,
                attachments INTEGER NOT NULL DEFAULT 0,
                embeds      INTEGER NOT NULL DEFAULT 0,
                reactions   TEXT,
                reply_to_id INTEGER
            )
            """
        )
        _ensure_column(con, "message_archive", "reactions", "TEXT")
        _ensure_column(con, "message_archive", "reply_to_id", "INTEGER")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_message_archive_guild_channel ON message_archive (guild_id, channel_id, created_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_message_archive_author ON message_archive (guild_id, author_id, created_at)"
        )

        # ========== role_welcome_sent ==========
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS role_welcome_sent (
                guild_id INTEGER NOT NULL,
                user_id  INTEGER NOT NULL,
                role_id  INTEGER NOT NULL,
                sent_at  TEXT    NOT NULL,
                PRIMARY KEY (guild_id, user_id, role_id)
            )
            """
        )

        # ========== MU tables ==========
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mu_series (
                series_id TEXT PRIMARY KEY,
                title     TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mu_releases (
                series_id   TEXT    NOT NULL,
                release_id  INTEGER NOT NULL,
                title       TEXT,
                raw_title   TEXT,
                description TEXT,
                volume      TEXT,
                chapter     TEXT,
                subchapter  TEXT,
                group_name  TEXT,
                url         TEXT,
                release_ts  INTEGER NOT NULL DEFAULT -1,
                created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (series_id, release_id)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_mu_releases_series_ts ON mu_releases (series_id, release_ts DESC)"
        )

        # historical migration guard
        sql = _table_sql(con, "mu_releases")
        if sql and "PRIMARY KEY" not in sql.upper():
            cur.execute("ALTER TABLE mu_releases RENAME TO mu_releases_old")
            cur.execute(
                """
                CREATE TABLE mu_releases (
                    series_id   TEXT    NOT NULL,
                    release_id  INTEGER NOT NULL,
                    title       TEXT,
                    raw_title   TEXT,
                    description TEXT,
                    volume      TEXT,
                    chapter     TEXT,
                    subchapter  TEXT,
                    group_name  TEXT,
                    url         TEXT,
                    release_ts  INTEGER NOT NULL DEFAULT -1,
                    created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (series_id, release_id)
                )
                """
            )
            cur.execute(
                """
                INSERT OR IGNORE INTO mu_releases
                (series_id, release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts, created_at)
                SELECT series_id, release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts,
                       COALESCE(created_at, datetime('now'))
                FROM mu_releases_old
                """
            )
            cur.execute("DROP TABLE mu_releases_old")
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mu_releases_series_ts ON mu_releases (series_id, release_ts DESC)"
            )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mu_thread_series (
                guild_id  INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                series_id TEXT    NOT NULL,
                PRIMARY KEY (guild_id, thread_id)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_mu_thread_series_series ON mu_thread_series (series_id)"
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mu_thread_posts (
                guild_id   INTEGER NOT NULL,
                thread_id  INTEGER NOT NULL,
                series_id  TEXT    NOT NULL,
                release_id INTEGER NOT NULL,
                posted_at  TEXT    NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (guild_id, thread_id, release_id)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_mu_thread_posts_series ON mu_thread_posts (series_id)"
        )

        # ========== DROP unused legacy tables ==========
        _drop_if_exists(
            con,
            (
                # analytics / legacy we don't read
                "collections",
                "emoji_usage_monthly",
                "gif_usage_monthly",
                "sticker_usage_monthly",
                "member_activity_apps_daily",
                "member_activity_monthly",
                "member_activity_total",
                "member_channel_totals",
                "member_hour_hist",
                "member_metrics_daily",
                "member_metrics_total",
                "member_rpg_progress",
                "movie_events",
                "voice_minutes_day",
                "voice_sessions",
                "series",
                "submissions",
                # old polls stack (unused by current code)
                "poll_votes",
                "poll_options",
                "polls",
            ),
        )

        con.commit()
