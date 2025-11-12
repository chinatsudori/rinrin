from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# Prefer project's DB connector if available; otherwise fall back to local sqlite.
try:
    from .db import connect as _project_connect  # type: ignore
except Exception:  # pragma: no cover
    _project_connect = None


def _fallback_connect() -> sqlite3.Connection:
    db_path = os.getenv("BOT_DB_PATH") or os.path.join(
        os.path.dirname(__file__), "data", "bot.sqlite3"
    )
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path, isolation_level=None)
    con.row_factory = sqlite3.Row
    return con


def connect() -> sqlite3.Connection:
    con = _project_connect() if _project_connect else _fallback_connect()
    try:
        con.row_factory = (
            sqlite3.Row
        )  # ensure row dicts even if project connect returns raw connection
    except Exception:
        pass
    return con


# ────────────────────────────────
# Schema (compact, append/upsert-friendly)
# ────────────────────────────────

DDL_MESSAGE_DAILY = """
CREATE TABLE IF NOT EXISTS message_metrics_daily(
  guild_id     INTEGER NOT NULL,
  user_id      INTEGER NOT NULL,
  day          TEXT    NOT NULL,              -- 'YYYY-MM-DD' UTC
  messages     INTEGER NOT NULL DEFAULT 0,
  words        INTEGER NOT NULL DEFAULT 0,
  replies      INTEGER NOT NULL DEFAULT 0,
  mentions     INTEGER NOT NULL DEFAULT 0,
  gifs         INTEGER NOT NULL DEFAULT 0,
  reactions_rx INTEGER NOT NULL DEFAULT 0,
  url_msgs     INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (guild_id, user_id, day)
);
CREATE INDEX IF NOT EXISTS idx_msg_daily_gd ON message_metrics_daily(guild_id, day);
"""

# Hourly message count per guild for heatmap/bursts/silence ratio
# hour is truncated UTC hour: 'YYYY-MM-DDTHH'
DDL_MESSAGE_HOURLY = """
CREATE TABLE IF NOT EXISTS message_metrics_hourly(
  guild_id INTEGER NOT NULL,
  hour     TEXT    NOT NULL,
  messages INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (guild_id, hour)
);
"""

# Per-user/day vocabulary (type set) for lexical diversity
DDL_USER_TOKEN_DAILY = """
CREATE TABLE IF NOT EXISTS user_token_daily(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  day      TEXT    NOT NULL,
  token    TEXT    NOT NULL,
  PRIMARY KEY (guild_id, user_id, day, token)
);
CREATE INDEX IF NOT EXISTS idx_utd_gud ON user_token_daily(guild_id, user_id, day);
"""

# Reaction histograms (bucketed) per day (kind = 'count' | 'diversity')
# Buckets 0..9 where 9 == 9+
DDL_REACTION_HIST_DAILY = """
CREATE TABLE IF NOT EXISTS reaction_hist_daily(
  guild_id INTEGER NOT NULL,
  day      TEXT    NOT NULL,
  kind     TEXT    NOT NULL,      -- 'count' | 'diversity'
  bucket   INTEGER NOT NULL,      -- 0..9 (9 means 9+)
  n        INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (guild_id, day, kind, bucket)
);
CREATE INDEX IF NOT EXISTS idx_rx_hist_gd ON reaction_hist_daily(guild_id, day);
"""

# Per-channel last message timestamp for online response-gap
DDL_CHANNEL_LAST = """
CREATE TABLE IF NOT EXISTS channel_last_msg(
  guild_id     INTEGER NOT NULL,
  channel_id   INTEGER NOT NULL,
  last_ts_utc  TEXT,
  last_msg_id  INTEGER,
  last_author  INTEGER,
  PRIMARY KEY (guild_id, channel_id)
);
"""

# Response latency histogram per day per channel (log2-bucketed milliseconds)
# bucket 0: <1ms..1ms, 1:[1,2), 2:[2,4), … 20: >= 2^20 ms
DDL_LATENCY_HIST_DAILY = """
CREATE TABLE IF NOT EXISTS latency_hist_daily(
  guild_id   INTEGER NOT NULL,
  channel_id INTEGER NOT NULL,
  day        TEXT    NOT NULL,
  bucket     INTEGER NOT NULL,
  n          INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (guild_id, channel_id, day, bucket)
);
CREATE INDEX IF NOT EXISTS idx_lat_hist_gd ON latency_hist_daily(guild_id, day);
"""

# Reply-chain index for thread lifespan/depth
DDL_THREAD_INDEX = """
CREATE TABLE IF NOT EXISTS thread_index(
  guild_id    INTEGER NOT NULL,
  root_id     INTEGER NOT NULL,
  started_utc TEXT    NOT NULL,
  last_utc    TEXT    NOT NULL,
  max_depth   INTEGER NOT NULL DEFAULT 0,
  messages    INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (guild_id, root_id)
);
CREATE INDEX IF NOT EXISTS idx_thread_g ON thread_index(guild_id);
"""

# Map each message to its thread root & depth
DDL_MESSAGE_THREAD = """
CREATE TABLE IF NOT EXISTS message_thread(
  guild_id    INTEGER NOT NULL,
  message_id  INTEGER NOT NULL,
  root_id     INTEGER NOT NULL,
  parent_id   INTEGER,
  depth       INTEGER NOT NULL,
  created_utc TEXT NOT NULL,
  channel_id  INTEGER NOT NULL,
  PRIMARY KEY (guild_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_msgthread_root ON message_thread(guild_id, root_id);
CREATE INDEX IF NOT EXISTS idx_msgthread_parent ON message_thread(guild_id, parent_id);
"""

# Optional: per-user/day sentiment aggregates (VADER)
DDL_SENTIMENT_DAILY = """
CREATE TABLE IF NOT EXISTS sentiment_daily(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  day      TEXT    NOT NULL,
  n           INTEGER NOT NULL DEFAULT 0,
  sum_compound REAL NOT NULL DEFAULT 0.0,
  sum_pos      REAL NOT NULL DEFAULT 0.0,
  sum_neg      REAL NOT NULL DEFAULT 0.0,
  sum_neu      REAL NOT NULL DEFAULT 0.0,
  PRIMARY KEY (guild_id, user_id, day)
);
CREATE INDEX IF NOT EXISTS idx_sent_gd ON sentiment_daily(guild_id, day);
"""


def ensure_tables() -> None:
    con = connect()
    try:
        cur = con.cursor()
        for ddl in (
            DDL_MESSAGE_DAILY,
            DDL_MESSAGE_HOURLY,
            DDL_USER_TOKEN_DAILY,
            DDL_REACTION_HIST_DAILY,
            DDL_CHANNEL_LAST,
            DDL_LATENCY_HIST_DAILY,
            DDL_THREAD_INDEX,
            DDL_MESSAGE_THREAD,
            DDL_SENTIMENT_DAILY,
        ):
            cur.executescript(ddl)
            # --- migrations ---
        # 1) Add url_msgs to message_metrics_daily if missing
        rows = cur.execute("PRAGMA table_info(message_metrics_daily)").fetchall()
        cols = {r[1] for r in rows}  # r[1] = column name
        if "url_msgs" not in cols:
            cur.execute(
                "ALTER TABLE message_metrics_daily ADD COLUMN url_msgs INTEGER NOT NULL DEFAULT 0"
            )
        con.commit()
    finally:
        con.close()


# ────────────────────────────────
# Helpers
# ────────────────────────────────

WORD_RE = re.compile(r"[A-Za-z0-9_]{3,}")
MENTION_RE = re.compile(r"<@!?\d+>|@(here|everyone)\\b", re.IGNORECASE)
GIF_EXT_RE = re.compile(r"\\.(gif|gifv)(?:\\?.*)?$", re.IGNORECASE)
URL_RE = re.compile(r"https?://\\S+")


def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]


def _hour_key(ts: dt.datetime) -> str:
    utc = ts if ts.tzinfo is not None else ts.replace(tzinfo=dt.timezone.utc)
    utc = utc.astimezone(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    return utc.strftime("%Y-%m-%dT%H")


def _day_key(ts: dt.datetime) -> str:
    utc = ts if ts.tzinfo is not None else ts.replace(tzinfo=dt.timezone.utc)
    return utc.astimezone(dt.timezone.utc).date().toordinal().__str__()


def _iso(ts: dt.datetime) -> str:
    utc = ts if ts.tzinfo is not None else ts.replace(timezone=dt.timezone.utc)
    return utc.astimezone(dt.timezone.utc).isoformat()


def _log2_bucket_millis(ms: float, max_bucket: int = 20) -> int:
    if ms <= 1:
        return 0
    b = int(math.log(ms, 2))
    return b if b < max_bucket else max_bucket


# Optional VADER sentiment
try:
    from nltk.sentiment import SentimentIntensityAnalyzer  # type: ignore

    _SIA: Optional[SentimentIntensityAnalyzer] = None

    def _get_sia() -> Optional[SentimentIntensityAnalyzer]:
        global _SIA
        if _SIA is None:
            try:
                _SIA = SentimentIntensityAnalyzer()
            except Exception:
                _SIA = None
        return _SIA

except Exception:  # pragma: no cover
    _SIA = None

    def _get_sia() -> Optional[Any]:
        return None


# ────────────────────────────────
# Live ingestion from discord.Message
# ────────────────────────────────


def _count_gifs(message) -> int:
    n = 0
    for a in getattr(message, "attachments", []) or []:
        ct = (getattr(a, "content_type", "") or "").lower()
        name = getattr(a, "filename", "") or ""
        url = getattr(a, "url", "") or ""
        if "gif" in ct or GIF_EXT_RE.search(name) or GIF_EXT_RE.search(url):
            n += 1
    for e in getattr(message, "embeds", []) or []:
        try:
            d = e.to_dict()
        except Exception:
            continue
        t = (d.get("type") or "").lower()
        if t == "gifv":
            n += 1
        else:
            for key in ("url", "thumbnail", "image", "video"):
                v = d.get(key)
                if isinstance(v, str):
                    u = v
                elif isinstance(v, dict):
                    u = v.get("url") or v.get("proxy_url") or ""
                else:
                    u = ""
                if isinstance(u, str) and GIF_EXT_RE.search(u):
                    n += 1
                    break
    return n


def _reaction_count_and_diversity(message) -> Tuple[int, int]:
    total = 0
    kinds: set[str] = set()
    for r in getattr(message, "reactions", []) or []:
        try:
            cnt = int(getattr(r, "count", 0) or 0)
            total += cnt
            emoji = getattr(r, "emoji", None)
            if emoji is not None:
                if getattr(emoji, "id", None):
                    kinds.add(f"{getattr(emoji, 'name', '')}:{getattr(emoji, 'id')}")
                else:
                    kinds.add(str(emoji))
        except Exception:
            continue
    return total, len(kinds)


def upsert_from_message(message, *, include_bots: bool = False) -> None:
    """
    Compute & persist live metrics from a single discord.Message.
    Safe to call on every on_message event and during history backfill.
    """
    guild = getattr(message, "guild", None)
    if guild is None:
        return
    if not include_bots and getattr(getattr(message, "author", None), "bot", False):
        return

    ensure_tables()

    guild_id = int(guild.id)
    channel_id = int(getattr(message, "channel", None).id)
    author_id = int(getattr(message, "author", None).id)
    created_at: dt.datetime = getattr(message, "created_at")
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=dt.timezone.utc)

    day = created_at.astimezone(dt.timezone.utc).date().isoformat()
    hour_key = _hour_key(created_at)
    content = getattr(message, "content", "") or ""
    words = len([m.group(0) for m in WORD_RE.finditer(content)])
    mentions = len(MENTION_RE.findall(content)) + len(
        getattr(message, "mentions", []) or []
    )
    gifs = _count_gifs(message)
    rx_total, rx_div = _reaction_count_and_diversity(message)
    url_msgs = 1 if URL_RE.search(content) else 0
    is_reply = (
        1
        if (
            getattr(getattr(message, "reference", None), "message_id", None) is not None
        )
        else 0
    )

    con = connect()
    try:
        cur = con.cursor()

        # 1) per-user daily metrics
        cur.execute(
            """
            INSERT INTO message_metrics_daily(guild_id,user_id,day,messages,words,replies,mentions,gifs,reactions_rx,url_msgs)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(guild_id,user_id,day) DO UPDATE SET
              messages     = messages + excluded.messages,
              words        = words    + excluded.words,
              replies      = replies  + excluded.replies,
              mentions     = mentions + excluded.mentions,
              gifs         = gifs     + excluded.gifs,
              reactions_rx = reactions_rx + excluded.reactions_rx,
              url_msgs     = url_msgs + excluded.url_msgs
            """,
            (
                guild_id,
                author_id,
                day,
                1,
                words,
                is_reply,
                mentions,
                gifs,
                rx_total,
                url_msgs,
            ),
        )

        # 2) hourly guild message counter
        cur.execute(
            """
            INSERT INTO message_metrics_hourly(guild_id, hour, messages)
            VALUES(?,?,1)
            ON CONFLICT(guild_id, hour) DO UPDATE SET messages = messages + 1
            """,
            (guild_id, hour_key),
        )

        # 3) reaction histograms per day (count & diversity, bucketed 0..9)
        def _b9(v: int) -> int:
            return v if v < 9 else 9

        if rx_total:
            cur.execute(
                """
                INSERT INTO reaction_hist_daily(guild_id,day,kind,bucket,n)
                VALUES(?,?,?,?,1)
                ON CONFLICT(guild_id,day,kind,bucket) DO UPDATE SET n = n + 1
                """,
                (guild_id, day, "count", _b9(rx_total)),
            )
        if rx_div:
            cur.execute(
                """
                INSERT INTO reaction_hist_daily(guild_id,day,kind,bucket,n)
                VALUES(?,?,?,?,1)
                ON CONFLICT(guild_id,day,kind,bucket) DO UPDATE SET n = n + 1
                """,
                (guild_id, day, "diversity", _b9(rx_div)),
            )

        # 4) response latency histogram (per channel)
        cur.execute(
            "SELECT last_ts_utc FROM channel_last_msg WHERE guild_id=? AND channel_id=?",
            (guild_id, channel_id),
        )
        prev = cur.fetchone()
        if prev and prev["last_ts_utc"]:
            try:
                prev_dt = dt.datetime.fromisoformat(
                    str(prev["last_ts_utc"]).replace("Z", "+00:00")
                )
                gap_ms = (created_at - prev_dt).total_seconds() * 1000.0
                if 0 <= gap_ms <= 24 * 60 * 60 * 1000:
                    b = _b = _log2_bucket_millis(gap_ms)
                    cur.execute(
                        """
                        INSERT INTO latency_hist_daily(guild_id,channel_id,day,bucket,n)
                        VALUES(?,?,?,?,1)
                        ON CONFLICT(guild_id,channel_id,day,bucket) DO UPDATE SET n = n + 1
                        """,
                        (guild_id, channel_id, day, b),
                    )
            except Exception:
                pass

        cur.execute(
            """
            INSERT INTO channel_last_msg(guild_id,channel_id,last_ts_utc,last_msg_id,last_author)
            VALUES(?,?,?,?,?)
            ON CONFLICT(guild_id,channel_id) DO UPDATE SET
                last_ts_utc = excluded.last_ts_utc,
                last_msg_id = excluded.last_msg_id,
                last_author = excluded.last_author
            """,
            (guild_id, channel_id, created_at.isoformat(), int(message.id), author_id),
        )

        # 5) per-user/day lexical diversity store
        if words:
            tokens = set(m.group(0).lower() for m in WORD_RE.finditer(content))
            cur.executemany(
                "INSERT OR IGNORE INTO user_token_daily(guild_id,user_id,day,token) VALUES(?,?,?,?)",
                [(guild_id, author_id, day, t) for t in tokens],
            )

        # 6) optional sentiment (VADER lexicon must be available)
        sia = _get_sia()
        if sia and content:
            s = sia.polarity_scores(content)
            cur.execute(
                """
                INSERT INTO sentiment_daily(guild_id,user_id,day,n,sum_compound,sum_pos,sum_neg,sum_neu)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(guild_id,user_id,day) DO UPDATE SET
                  n = n + excluded.n,
                  sum_compound = sum_compound + excluded.sum_compound,
                  sum_pos      = sum_pos + excluded.sum_pos,
                  sum_neg      = sum_neg + excluded.sum_neg,
                  sum_neu      = sum_neu + excluded.sum_neu
                """,
                (
                    guild_id,
                    author_id,
                    day,
                    1,
                    float(s["compound"]),
                    float(s["pos"]),
                    float(s["neg"]),
                    float(s["neu"]),
                ),
            )

        con.commit()
    finally:
        con.close()


# ────────────────────────────────
# Query helpers (no heavy rescans)
# ────────────────────────────────


@dataclass
class BasicStats:
    min: int
    max: int
    mean: float
    std: float
    skewness: float
    kurtosis: float
    gini: float


def _gini(values: List[int]) -> float:
    if not values:
        return float("nan")
    x = sorted(float(v) for v in values if v >= 0)
    if not x:
        return float("nan")
    s = sum(x)
    if s == 0:
        return 0.0
    cum = 0.0
    acc = 0.0
    n = len(x)
    for i, v in enumerate(x, 1):
        cum += v
        acc += i * v
    return (2.0 * acc / (n * s)) - (n + 1) / n


def get_basic_stats(guild_id: int, start_day: str, end_day: str) -> BasicStats:
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT user_id, SUM(messages) AS m
            FROM message_metrics_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
    finally:
        con.close()

    arr = [int(r["m"] or 0) for r in rows]
    n = len(arr)
    if n == 0:
        return BasicStats(0, 0, 0.0, 0.0, float("nan"), float("nan"), float("nan"))
    mean = sum(arr) / n
    var = sum((x - mean) ** 2 for x in arr) / n if n > 0 else 0.0
    std = math.sqrt(var)
    # Biased (population) skew/kurtosis to match scipy defaults (bias=True)
    if std > 0:
        m3 = sum(((x - mean) / std) ** 3 for x in arr) / n
        m4 = sum(((x - mean) / std) ** 4 for x in arr) / n
        skewness = float(m3)
        kurt = float(m4 - 3.0)  # excess kurtosis
    else:
        skewness = float("0")
        kurt = float("0")
    return BasicStats(
        min(arr), max(arr), float(mean), float(std), skewness, kurt, _gini(arr)
    )


def get_hourly_counts(guild_id: int, start_hour: str, end_hour: str) -> Dict[str, int]:
    """
    Returns {'YYYY-MM-DDTHH': messages} for hours in [start_hour, end_hour] (UTC).
    """
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, messages
            FROM message_metrics_hourly
            WHERE guild_id=? AND hour BETWEEN ? AND ?
            ORDER BY hour
            """,
            (guild_id, start_hour, end_hour),
        ).fetchall()
    finally:
        con.close()
    return {str(r["hour"]): int(r["messages"]) for r in rows}


def get_heatmap(guild_id: int, start_day: str, end_day: str) -> List[List[float]]:
    """
    7x24 matrix of avg msgs per (weekday,hour) across [start_day, end_day].
    Uses message_metrics_hourly, normalizing by how many occurrences of each weekday
    fall in the range.
    """
    # Build day counts per weekday in the range
    start = dt.date.fromisoformat(start_day)
    end = dt.date.fromisoformat(end_day)
    days_per_dow = [0] * 7
    d = start
    while d <= end:
        days_per_dow[d.weekday()] += 1
        d += dt.timedelta(days=1)
    # Pull hourly counts in range
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, messages
            FROM message_metrics_hourly
            WHERE guild_id=? AND substr(hour,1,10) BETWEEN ? AND ?
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
    finally:
        con.close()
    accum = [[0 for _ in range(24)] for _ in range(7)]
    for r in rows:
        hour_s = str(r["hour"])  # 'YYYY-MM-DDTHH'
        day = dt.date.fromisoformat(hour_s[:10])
        dow = day.weekday()
        hour = int(hour_s[11:13])
        accum[dow][hour] += int(r["messages"])
    # average per weekday by number of that weekday in range
    for dow in range(7):
        denom = max(1, days_per_dow[dow])
        for h in range(24):
            accum[dow][h] = accum[dow][h] / float(denom)
    return [[float(x) for x in row] for row in accum]


def get_burst_std_24h(
    guild_id: int, start_hour: str, end_hour: str
) -> Dict[str, float]:
    """
    24-hour rolling std of hourly message counts in [start_hour, end_hour] (UTC).
    We rebuild the full hourly sequence to include zeros for missing hours.
    """
    # Build timeline of hours
    start_dt = dt.datetime.fromisoformat(start_hour + ":00+00:00")
    end_dt = dt.datetime.fromisoformat(end_hour + ":00+00:00")
    hours: List[str] = []
    t = start_dt
    while t <= end_dt:
        hours.append(t.strftime("%Y-%m-%dT%H"))
        t += dt.timedelta(hours=1)

    # Pull counts
    raw = get_hourly_counts(guild_id, start_hour, end_hour)
    series = [int(raw.get(h, 0)) for h in hours]

    # Rolling std over window=24
    out: Dict[str, float] = {}
    window = 24
    from collections import deque
    import statistics

    dq: deque[int] = deque()
    for idx, h in enumerate(hours):
        dq.append(series[idx])
        if len(dq) > window:
            dq.popleft()
        if len(dq) <= 1:
            out[h] = 0.0
        else:
            out[h] = float(statistics.pstdev(dq))
    return out


def get_latency_stats(guild_id: int, start_day: str, end_day: str) -> Dict[str, Any]:
    """
    Approximate per-channel + global latency (median/p95) using log2(ms) histograms
    in [start_day, end_day].
    """
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT channel_id, bucket, SUM(n) AS n
            FROM latency_hist_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY channel_id, bucket
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
    finally:
        con.close()

    max_bin = 20
    by_chan: Dict[int, List[int]] = defaultdict(lambda: [0] * (max_bin + 1))
    global_hist = [0] * (max_bin + 1)
    for r in rows:
        cid = int(r["channel_id"])
        b = int(r["bucket"])
        n = int(r["n"])
        by_chan[cid][b] += n
        global_hist[b] += n

    def quantiles_from_hist(hist: List[int], probs=(0.5, 0.95)) -> Tuple[float, float]:
        total = sum(hist)
        if total == 0:
            return (float("nan"), float("nan"))
        cdf = []
        acc = 0
        for n in hist:
            acc += n
            cdf.append(acc / total)
        outs = []
        for p in probs:
            i = 0
            while i < len(cdf) and cdf[i] < p:
                i += 1
            # Use lower-edge of bucket as conservative estimate
            ms = float(2**i)
            outs.append(ms)
        return tuple(outs)  # median_ms, p95_ms

    chans = []
    for cid, hist in by_chan.items():
        med, p95 = quantiles_from_hist(hist)
        chans.append(
            {"channel_id": cid, "median_ms": med, "p95_ms": p95, "n": int(sum(hist))}
        )
    gmed, gp95 = quantiles_from_hist(global_hist)
    return {
        "channels": chans,
        "global": {"median_ms": gmed, "p95_ms": gp95, "n": int(sum(global_hist))},
    }


def get_content_stats(guild_id: int, start_day: str, end_day: str) -> Dict[str, Any]:
    """
    Returns totals + words/msg, url rate, lexical diversity by user, and optional sentiment coverage.
    """
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT user_id, SUM(messages) AS m, SUM(words) AS w, SUM(url_msgs) AS u
            FROM message_metrics_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
        tok_rows = cur.execute(
            """
            SELECT user_id, COUNT(DISTINCT token) AS uniq
            FROM user_token_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
        sent_rows = cur.execute(
            """
            SELECT user_id, SUM(n) AS n, SUM(sum_compound) AS csum
            FROM sentiment_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
    finally:
        con.close()

    # totals
    total_msgs = sum(int(r["m"] or 0) for r in rows)
    total_words = sum(int(r["w"] or 0) for r in rows)
    url_msgs = sum(int(r["u"] or 0) for r in rows)
    words_per_msg_samples: List[float] = []
    for r in rows:
        m = int(r["m"] or 0)
        w = int(r["w"] or 0)
        if m > 0:
            words_per_msg_samples.append(w / m)
    words_per_msg_mean = (
        float(sum(words_per_msg_samples) / len(words_per_msg_samples))
        if words_per_msg_samples
        else 0.0
    )
    words_per_msg_median = (
        float(sorted(words_per_msg_samples)[len(words_per_msg_samples) // 2])
        if words_per_msg_samples
        else 0.0
    )
    url_rate = (url_msgs / total_msgs) if total_msgs else 0.0

    uniq_by_user = {int(r["user_id"]): int(r["uniq"]) for r in tok_rows}
    ttr_by_user: Dict[int, float] = {}
    for r in rows:
        uid = int(r["user_id"])
        m = int(r["m"] or 0)
        w = int(r["w"] or 0)
        denom = max(1, w)
        ttr_by_user[uid] = float(uniq_by_user.get(uid, 0)) / float(denom)

    # sentiment coverage + mean/median compound if stored
    cov = 0.0
    comp_samples: List[float] = []
    n_scored = sum(int(r["n"] or 0) for r in sent_rows)
    if total_msgs:
        cov = float(n_scored) / float(total_msgs)
    for r in sent_rows:
        n = int(r["n"] or 0)
        if n:
            comp = float(r["csum"] or 0.0) / float(n)
            comp = max(-1.0, min(1.0, comp))
            comp_samples.append(comp)
    comp_mean = float(sum(comp_samples) / len(comp_samples)) if comp_samples else None
    comp_median = (
        float(sorted(comp_samples)[len(comp_samples) // 2]) if comp_samples else None
    )

    return {
        "total_messages": int(total_msgs),
        "total_words": int(total_words),
        "words_per_msg_mean": words_per_msg_mean,
        "words_per_msg_median": words_per_msg_median,
        "url_rate": float(url_rate),
        "lexical_diversity_by_user": ttr_by_user,
        "sentiment": {
            "coverage": float(cov),
            "compound_mean": comp_mean,
            "compound_median": comp_median,
        },
    }
