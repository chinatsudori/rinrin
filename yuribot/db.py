from __future__ import annotations

import os
import sqlite3
from typing import Optional

# Point to your desired DB location. Override via ENV if needed.
DB_PATH = os.environ.get("BOT_DB_PATH", os.path.join(os.path.dirname(__file__), "data", "bot.sqlite3"))


# ----------------------------
# Internal helpers (schema)
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
    Call ensure_db() once at startup to guarantee schema exists.
    """
    con = sqlite3.connect(DB_PATH, timeout=5)
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
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH, timeout=5) as con:
        cur = con.cursor()

        # Pragmas (persist for this connection)
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA busy_timeout=3000")

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
        # migrate if an old CHECK exists
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

        # collections
        cur.execute("""
        CREATE TABLE IF NOT EXISTS collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            club_id INTEGER,
            opens_at TEXT,
            closes_at TEXT,
            status TEXT CHECK(status IN ('open','closed')) DEFAULT 'open'
        )""")
        _ensure_column(con, "collections", "club_id", "INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_collections_guild_club_status ON collections (guild_id, club_id, status)")

        # submissions
        cur.execute("""
        CREATE TABLE IF NOT EXISTS submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            club_id INTEGER,
            collection_id INTEGER,
            author_id INTEGER,
            title TEXT,
            link TEXT,
            thread_id INTEGER,
            created_at TEXT
        )""")
        _ensure_column(con, "submissions", "club_id", "INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_submissions_collection ON submissions (collection_id)")

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

        # series
        cur.execute("""
        CREATE TABLE IF NOT EXISTS series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            club_id INTEGER,
            title TEXT,
            link TEXT,
            source_submission_id INTEGER,
            status TEXT CHECK(status IN ('active','queued','completed')) DEFAULT 'active'
        )""")
        _ensure_column(con, "series", "club_id", "INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_series_guild_club_status ON series (guild_id, club_id, status)")

        # schedule_sections
        cur.execute("""
        CREATE TABLE IF NOT EXISTS schedule_sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_id INTEGER,
            label TEXT,
            start_chapter INTEGER,
            end_chapter INTEGER,
            discussion_event_id INTEGER,
            discussion_start TEXT
        )""")
        _ensure_column(con, "schedule_sections", "discussion_thread_id", "INTEGER")
        _ensure_column(con, "schedule_sections", "posted", "INTEGER DEFAULT 0")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sections_series ON schedule_sections (series_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sections_due ON schedule_sections (posted, discussion_start)")

        # --- Guild-wide settings (single definition; includes MU forum channel) ---
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

        # --- Moderation actions log ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS mod_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            target_username TEXT,
            rule TEXT NOT NULL,
            offense INTEGER NOT NULL,
            action TEXT NOT NULL,               -- warning | timeout | kick | ban | other
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
            month TEXT NOT NULL,                -- 'YYYY-MM'
            emoji_key TEXT NOT NULL,            -- custom:<id> or uni:<codepoint(s)>
            emoji_name TEXT,                    -- best-effort label
            is_custom INTEGER NOT NULL,         -- 1 custom guild emoji, 0 unicode
            via_reaction INTEGER NOT NULL,      -- 1 reaction, 0 message body
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
            month    TEXT    NOT NULL,   -- YYYY-MM
            gif_key  TEXT    NOT NULL,   -- canonical URL or provider id
            source   TEXT    NOT NULL,   -- tenor/giphy/discord/etc
            count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, month, gif_key)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gif_usage_lookup ON gif_usage_monthly (guild_id, month, count DESC)")

        # --- Member message activity (monthly + total) (legacy mirrors) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_activity_monthly (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            month TEXT NOT NULL,                -- 'YYYY-MM'
            count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, month)
        )""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_activity_total (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_member_activity_month ON member_activity_monthly (guild_id, month, count DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_member_activity_total ON member_activity_total (guild_id, count DESC)")

        # --- Movie events ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS movie_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            club_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            link TEXT,
            show_date TEXT NOT NULL,            -- YYYY-MM-DD
            event_id_morning INTEGER,
            event_id_evening INTEGER
        )""")

        # --- Role welcome (first-time DM tracking) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS role_welcome_sent (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            role_id  INTEGER NOT NULL,
            sent_at  TEXT    NOT NULL,
            PRIMARY KEY (guild_id, user_id, role_id)
        )""")

        # --- Unified member metrics (daily + totals by metric) ---
        # metric ∈ {'messages','words','mentions','mentions_sent','emoji_chat','emoji_react',
        #           'reactions_received','voice_minutes','voice_stream_minutes','activity_minutes','gifs'}
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_metrics_daily (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            metric   TEXT    NOT NULL,
            day      TEXT    NOT NULL,          -- 'YYYY-MM-DD'
            week     TEXT    NOT NULL,          -- 'YYYY-Www' (ISO week)
            month    TEXT    NOT NULL,          -- 'YYYY-MM'
            count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, metric, day)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_daily_gwk ON member_metrics_daily (guild_id, metric, week)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_daily_gm ON member_metrics_daily (guild_id, metric, month)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_daily_gd ON member_metrics_daily (guild_id, metric, day)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_metrics_total (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            metric   TEXT    NOT NULL,
            count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, metric)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_metrics_total_gm ON member_metrics_total (guild_id, metric, count DESC)")

        # Hour histogram (UTC buckets; rotated to target tz on read)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_hour_hist (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            metric   TEXT    NOT NULL,
            hour_utc INTEGER NOT NULL CHECK(hour_utc BETWEEN 0 AND 23),
            count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, metric, hour_utc)
        )""")

        # Helper view of months for autocomplete (messages metric preferred)
        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_available_months AS
        SELECT DISTINCT month FROM member_metrics_daily
         WHERE metric='messages'
        UNION
        SELECT DISTINCT month FROM member_activity_monthly
        """)

        # --- MangaUpdates ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS mu_series (
            series_id TEXT PRIMARY KEY,         -- MU id (string)
            title     TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS mu_releases (
            series_id   TEXT    NOT NULL,
            release_id  INTEGER NOT NULL,       -- stable if available; else generated
            title       TEXT,
            raw_title   TEXT,
            description TEXT,
            volume      TEXT,
            chapter     TEXT,
            subchapter  TEXT,
            group_name  TEXT,
            url         TEXT,
            release_ts  INTEGER NOT NULL DEFAULT -1,  -- epoch seconds; -1 if unknown
            created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (series_id, release_id)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mu_releases_series_ts ON mu_releases (series_id, release_ts DESC)")

        # Migration: ensure composite PK (legacy installs)
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

        # --- RPG progression (per member) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_rpg_progress (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            xp       INTEGER NOT NULL DEFAULT 0,
            level    INTEGER NOT NULL DEFAULT 1,
            -- base stats; recomputed or incremented on level up
            str      INTEGER NOT NULL DEFAULT 5,
            int      INTEGER NOT NULL DEFAULT 5,
            cha      INTEGER NOT NULL DEFAULT 5,
            vit      INTEGER NOT NULL DEFAULT 5,
            dex      INTEGER NOT NULL DEFAULT 5,
            wis      INTEGER NOT NULL DEFAULT 5,
            last_level_up TEXT,
            PRIMARY KEY (guild_id, user_id)
        )""")

        # --- Channel totals (for prime_channel & per-channel XP multipliers) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_channel_totals (
            guild_id  INTEGER NOT NULL,
            user_id   INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            messages  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, channel_id)
        )""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_member_channel_totals ON member_channel_totals (guild_id, user_id, messages DESC)")

        # --- Member app usage per day (aux to activity_minutes) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_activity_apps_daily (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            app_name TEXT    NOT NULL,
            day      TEXT    NOT NULL,   -- YYYY-MM-DD
            minutes  INTEGER NOT NULL DEFAULT 0,
            launches INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, app_name, day)
        )""")

        con.commit()
