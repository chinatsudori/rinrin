from __future__ import annotations
try:
    from . import config
except Exception:
    import config

import os
import sqlite3
import logging
from typing import Optional

from .data.booly_defaults import DEFAULT_BOOLY_ROWS

log = logging.getLogger("yuribot.db")

# ----------------------------
# Path resolution
# ----------------------------
def _resolved_db_path() -> str:
    """
    Resolve DB path each call; prefer BOT_DB_PATH, else package-local ./data/bot.sqlite3.
    """
    env = os.environ.get("BOT_DB_PATH")
    if env:
        return os.path.abspath(env)
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "bot.sqlite3"))


# ----------------------------
# Freshness detection (no size heuristics; WAL-safe)
# ----------------------------
def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    cur = con.cursor()
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
    ).fetchone()
    return bool(row)

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
    """
    A DB is 'fresh' iff the file doesn't exist OR it exists but has no schema/data
    in any of our core tables. This is WAL-safe and container-safe.
    """
    if not os.path.exists(path):
        return True

    try:
        con = sqlite3.connect(path, timeout=5)
        # If *any* of these have a row, it's not fresh.
        core_tables = (
            "guild_settings",
        )
        for t in core_tables:
            if _any_rows(con, t):
                con.close()
                return False
        con.close()
        return True
    except Exception:
        # Corrupt or unreadable -> treat as fresh so the guard can stop startup.
        return True


# ----------------------------
# Schema helpers
# ----------------------------
def _ensure_column(con: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cur = con.cursor()
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})")]
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

def _table_sql(con: sqlite3.Connection, table: str) -> Optional[str]:
    cur = con.cursor()
    row = cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row[0] if row else None


# ----------------------------
# Public: connect() and ensure_db()
# ----------------------------
def connect() -> sqlite3.Connection:
    """
    Open a connection with consistent pragmas.
    If DB_REQUIRE_PERSISTENCE=1, refuse to run against a 'fresh' DB (WAL-safe).
    """
    path = _resolved_db_path()

    # Hard guard if requested by env
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
    Idempotently create/upgrade all tables, views, and indexes used by the bot.
    Safe to call multiple times (e.g., on startup).
    """
    path = _resolved_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with sqlite3.connect(path, timeout=5) as con:
        cur = con.cursor()

        # Pragmas for this connection
        cur.execute("PRAGMA journal_mode=WAL")
        journal = cur.execute("PRAGMA journal_mode").fetchone()[0]
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA busy_timeout=3000")

        # Visibility: what file are we actually using?
        try:
            size = os.stat(path).st_size
        except FileNotFoundError:
            size = 0
        log.info("db.open path=%s size=%d journal=%s", path, size, journal)

        # --- clubs (no CHECK on club_type) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clubs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            club_type TEXT NOT NULL,
            announcements_channel_id INTEGER,
            planning_forum_id INTEGER,
            polls_channel_id INTEGER,
            discussion_forum_id INTEGER,
            UNIQUE(guild_id, club_type)
        )""")
        sql = _table_sql(con, "clubs")
        if sql and "CHECK" in sql.upper():
            cur.execute("ALTER TABLE clubs RENAME TO clubs_old")
            cur.execute("""
            CREATE TABLE clubs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                club_type TEXT NOT NULL,
                announcements_channel_id INTEGER,
                planning_forum_id INTEGER,
                polls_channel_id INTEGER,
                discussion_forum_id INTEGER,
                UNIQUE(guild_id, club_type)
            )""")
            cur.execute("""
            INSERT OR IGNORE INTO clubs (id, guild_id, club_type, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id)
            SELECT id, guild_id, club_type, announcements_channel_id, planning_forum_id, polls_channel_id, discussion_forum_id
            FROM clubs_old
            """)
            cur.execute("DROP TABLE clubs_old")

        # polls
        cur.execute("""
        CREATE TABLE IF NOT EXISTS polls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            club_id INTEGER,
            channel_id INTEGER,
            message_id INTEGER,
            created_at TEXT,
            closes_at TEXT,
            status TEXT CHECK(status IN ('open','closed')) DEFAULT 'open'
        )""")
        _ensure_column(con, "polls", "club_id", "INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_polls_guild_status ON polls (guild_id, status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_polls_channel_message ON polls (channel_id, message_id)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS poll_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_id INTEGER,
            label TEXT,
            submission_id INTEGER
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS poll_votes (
            poll_id INTEGER,
            user_id INTEGER,
            option_id INTEGER,
            PRIMARY KEY (poll_id, user_id)
        )""")

        # --- Guild-wide settings ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id INTEGER PRIMARY KEY,
            mod_logs_channel_id INTEGER,
            bot_logs_channel_id INTEGER,
            welcome_channel_id INTEGER,
            welcome_image_filename TEXT,
            mu_forum_channel_id INTEGER
        )""")
        _ensure_column(con, "guild_settings", "welcome_channel_id", "INTEGER")
        _ensure_column(con, "guild_settings", "welcome_image_filename", "TEXT")
        _ensure_column(con, "guild_settings", "mu_forum_channel_id", "INTEGER")

        # --- Booly auto-response messages ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS booly_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope TEXT NOT NULL,
            user_id INTEGER,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_booly_scope ON booly_messages (scope, user_id)")
        existing_booly = cur.execute("SELECT COUNT(1) FROM booly_messages").fetchone()[0]
        if not existing_booly:
            cur.executemany(
                "INSERT INTO booly_messages (scope, user_id, content) VALUES (?, ?, ?)",
                DEFAULT_BOOLY_ROWS,
            )

        # --- Moderation actions log ---
        cur.execute("""
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
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mod_actions_lookup ON mod_actions (guild_id, target_user_id, id DESC)")

        # --- Emoji / Sticker / GIF monthly usage ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS emoji_usage_monthly (
            guild_id INTEGER NOT NULL,
            month TEXT NOT NULL,
            emoji_key TEXT NOT NULL,
            emoji_name TEXT,
            is_custom INTEGER NOT NULL,
            via_reaction INTEGER NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, month, emoji_key, via_reaction)
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS sticker_usage_monthly (
            guild_id INTEGER NOT NULL,
            month TEXT NOT NULL,
            sticker_id INTEGER NOT NULL,
            sticker_name TEXT,
            count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, month, sticker_id)
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS gif_usage_monthly (
            guild_id INTEGER NOT NULL,
            month    TEXT    NOT NULL,
            gif_key  TEXT    NOT NULL,
            source   TEXT    NOT NULL,
            count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, month, gif_key)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gif_usage_lookup ON gif_usage_monthly (guild_id, month, count DESC)")

        # --- Message archive ---
        cur.execute("""
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
        )""")
        _ensure_column(con, "message_archive", "reactions", "TEXT")
        _ensure_column(con, "message_archive", "reply_to_id", "INTEGER")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_message_archive_guild_channel ON message_archive (guild_id, channel_id, created_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_message_archive_author ON message_archive (guild_id, author_id, created_at)"
        )

        # --- Role welcome (first-time DM tracking) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS role_welcome_sent (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            role_id  INTEGER NOT NULL,
            sent_at  TEXT    NOT NULL,
            PRIMARY KEY (guild_id, user_id, role_id)
        )""")

        # --- MangaUpdates ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS mu_series (
            series_id TEXT PRIMARY KEY,
            title     TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )""")

        cur.execute("""
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
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mu_releases_series_ts ON mu_releases (series_id, release_ts DESC)")

        # Migration: ensure composite PK for legacy installs
        sql = _table_sql(con, "mu_releases")
        if sql and "PRIMARY KEY" not in sql.upper():
            cur.execute("ALTER TABLE mu_releases RENAME TO mu_releases_old")
            cur.execute("""
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
            )""")
            cur.execute("""
            INSERT OR IGNORE INTO mu_releases
            (series_id, release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts, created_at)
            SELECT series_id, release_id, title, raw_title, description, volume, chapter, subchapter, group_name, url, release_ts,
                   COALESCE(created_at, datetime('now'))
            FROM mu_releases_old
            """)
            cur.execute("DROP TABLE mu_releases_old")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mu_releases_series_ts ON mu_releases (series_id, release_ts DESC)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS mu_thread_series (
            guild_id  INTEGER NOT NULL,
            thread_id INTEGER NOT NULL,
            series_id TEXT    NOT NULL,
            PRIMARY KEY (guild_id, thread_id)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mu_thread_series_series ON mu_thread_series (series_id)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS mu_thread_posts (
            guild_id   INTEGER NOT NULL,
            thread_id  INTEGER NOT NULL,
            series_id  TEXT    NOT NULL,
            release_id INTEGER NOT NULL,
            posted_at  TEXT    NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (guild_id, thread_id, release_id)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mu_thread_posts_series ON mu_thread_posts (series_id)")

        # Polls convenience (optional)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_poll_options_poll ON poll_options (poll_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_poll_votes_poll ON poll_votes (poll_id)")

        con.commit()