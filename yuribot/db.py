from __future__ import annotations
import os
import sqlite3
import logging
from typing import Optional

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
            "member_metrics_total",
            "member_rpg_progress",
            "member_activity_total",
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

        # --- Member message activity (legacy mirrors) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_activity_monthly (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            month TEXT NOT NULL,
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
            show_date TEXT NOT NULL,
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_metrics_daily (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            metric   TEXT    NOT NULL,
            day      TEXT    NOT NULL,
            week     TEXT    NOT NULL,
            month    TEXT    NOT NULL,
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

        # Hour histogram
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_hour_hist (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            metric   TEXT    NOT NULL,
            hour_utc INTEGER NOT NULL CHECK(hour_utc BETWEEN 0 AND 23),
            count    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, metric, hour_utc)
        )""")

        # Available months view
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

        # --- RPG progression (per member) ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS member_rpg_progress (
            guild_id INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            xp       INTEGER NOT NULL DEFAULT 0,
            level    INTEGER NOT NULL DEFAULT 1,
            str      INTEGER NOT NULL DEFAULT 5,
            int      INTEGER NOT NULL DEFAULT 5,
            cha      INTEGER NOT NULL DEFAULT 5,
            vit      INTEGER NOT NULL DEFAULT 5,
            dex      INTEGER NOT NULL DEFAULT 5,
            wis      INTEGER NOT NULL DEFAULT 5,
            last_level_up TEXT,
            PRIMARY KEY (guild_id, user_id)
        )""")

        # Polls convenience (optional)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mu_thread_posts_lookup ON mu_thread_posts (guild_id, thread_id, series_id, release_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_poll_options_poll ON poll_options (poll_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_poll_votes_poll ON poll_votes (poll_id)")

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
            day      TEXT    NOT NULL,
            minutes  INTEGER NOT NULL DEFAULT 0,
            launches INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, app_name, day)
        )""")

        con.commit()
