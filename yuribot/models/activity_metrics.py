# yuribot/models/activity_metrics.py
from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    # prefer project DB connector if present
    from .db import connect as _connect  # type: ignore
except Exception:  # pragma: no cover
    _connect = None

# ---------- DB plumbing ----------


def _fallback_connect() -> sqlite3.Connection:
    db_path = os.getenv("BOT_DB_PATH") or os.path.join(
        os.path.dirname(__file__), "data", "bot.sqlite3"
    )
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path, isolation_level=None)
    con.row_factory = sqlite3.Row
    return con


def connect() -> sqlite3.Connection:
    return _connect() if _connect else _fallback_connect()


# ---------- Tables (all compact, append/upsert friendly) ----------

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

# hourly message count per guild for heatmap/bursts/silence ratio
# hour is truncated UTC hour: 'YYYY-MM-DDTHH'
DDL_MESSAGE_HOURLY = """
CREATE TABLE IF NOT EXISTS message_metrics_hourly(
  guild_id INTEGER NOT NULL,
  hour     TEXT    NOT NULL,                  -- e.g. '2025-11-11T13'
  messages INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (guild_id, hour)
);
"""

# per user/day token set for lexical diversity (type-token ratio)
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

# reaction emoji diversity & counts per message aggregated into daily histograms for O(1) retrieval
# bucket scheme: for counts/diversity, bucket = exact value up to 8, then 9=9+, adjust if you want
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

# per-channel last message timestamp to compute response gaps online
DDL_CHANNEL_LAST = """
CREATE TABLE IF NOT EXISTS channel_last_msg(
  guild_id     INTEGER NOT NULL,
  channel_id   INTEGER NOT NULL,
  last_ts_utc  TEXT,               -- ISO8601
  last_msg_id  INTEGER,
  last_author  INTEGER,
  PRIMARY KEY (guild_id, channel_id)
);
"""

# response latency histogram per day per channel (log2-bucketed milliseconds)
# bucket 0: <1s, 1: [1s,2s), 2: [2s,4s), ... up to e.g., 20 => ~ 2^20 ms ~ 17 minutes
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

# simple thread index for reply chains: root message spans and depth
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

# map each message to its thread root and depth for O(1) updates on replies
DDL_MESSAGE_THREAD = """
CREATE TABLE IF NOT EXISTS message_thread(
  guild_id   INTEGER NOT NULL,
  message_id INTEGER NOT NULL,
  root_id    INTEGER NOT NULL,
  parent_id  INTEGER,
  depth      INTEGER NOT NULL,
  created_utc TEXT NOT NULL,
  channel_id INTEGER NOT NULL,
  PRIMARY KEY (guild_id, message_id),
  FOREIGN KEY(root_id) REFERENCES thread_index(root_id)
);
CREATE INDEX IF NOT EXISTS idx_msgthread_root ON message_thread(guild_id, root_id);
CREATE INDEX IF NOT EXISTS idx_msgthread_parent ON message_thread(guild_id, parent_id);
"""

# (Optional) sentiment aggregates per user/day if VADER is available
DDL_SENTIMENT_DAILY = """
CREATE TABLE IF NOT EXISTS sentiment_daily(
  guild_id INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  day      TEXT    NOT NULL,
  n        INTEGER NOT NULL DEFAULT 0,
  sum_compound REAL NOT NULL DEFAULT 0.0,
  sum_pos   REAL NOT NULL DEFAULT 0.0,
  sum_neg   REAL NOT NULL DEFAULT 0.0,
  sum_neu   REAL NOT NULL DEFAULT 0.0,
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
    finally:
        con.close()


# ---------- tokenization / regex helpers ----------

WORD_RE = re.compile(r"[A-Za-z0-9_]{3,}")
MENTION_RE = re.compile(r"<@!?\d+>|@(here|everyone)\\b", re.IGNORECASE)
GIF_EXT_RE = re.compile(r"\\.(gif|gifv)(?:\\?.*)?$", re.IGNORECASE)


def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]


def _hour_key(ts: dt.datetime) -> str:
    ts = ts.astimezone(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    return ts.strftime("%Y-%m-%dT%H")


def _day_key(ts: dt.datetime) -> str:
    return ts.astimezone(dt.timezone.utc).date().isoformat()


def _log2_bucket_millis(delta_ms: float, max_bucket: int = 20) -> int:
    if delta_ms <= 0:
        return 0
    b = int(math.log(delta_ms, 2))
    return b if b < max_bucket else max_bucket


# ---------- optional sentiment ----------

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

    def _get_sia():  # type: ignore
        return None


# ---------- Live ingestion from discord.Message ----------


def _count_gifs_on_message(message) -> int:
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
                    kinds.add(f"{getattr(emoji,'name', '')}:{getattr(emoji,'id')}")
                else:
                    kinds.add(str(emoji))
        except Exception:
            continue
    return total, len(kinds)


def upsert_from_message(message, *, include_bots: bool = False) -> None:
    """
    Compute & persist *all* live metrics from a single discord.Message.
    Safe to call on every on_message event and during backfill.
    """
    if getattr(message, "guild", None) is None:
        return
    if not include_bots and getattr(getattr(message, "author", None), "bot", False):
        return

    ensure_tables()

    guild_id = int(message.guild.id)
    channel_id = int(message.channel.id)
    author_id = int(message.author.id)
    created_at: dt.datetime = message.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=dt.timezone.utc)

    day = _day_key(created_at)
    hour_key = _hour_key(created_at)
    content = getattr(message, "content", "") or ""
    words = len(_tokenize(content))
    mentions = len(MENTION_RE.findall(content)) + len(
        getattr(message, "mentions", []) or []
    )
    gifs = _count_gifs_on_message(message)
    rx_total, rx_diversity = _reaction_count_and_diversity(message)
    url_msgs = 1 if re.search(r"https?://\\S+", content) else 0
    is_reply = (
        1
        if getattr(message, "reference", None)
        and getattr(message.reference, "message_id", None)
        else 0
    )

    con = connect()
    try:
        cur = con.cursor()

        # 1) per-user daily counts
        cur.execute(
            """
            INSERT INTO message_metrics_daily(guild_id,user_id,day,messages,words,replies,mentions,gifs,reactions_rx,url_msgs)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(guild_id,user_id,day) DO UPDATE SET
              messages=messages+excluded.messages,
              words=words+excluded.words,
              replies=replies+excluded.replies,
              mentions=mentions+excluded.mentions,
              gifs=gifs+excluded.gifs,
              reactions_rx=reactions_rx+excluded.reactions_rx,
              url_msgs=url_msgs+excluded.url_msgs
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

        # 2) hourly message counter (guild-level)
        cur.execute(
            """
            INSERT INTO message_metrics_hourly(guild_id, hour, messages)
            VALUES(?,?,1)
            ON CONFLICT(guild_id, hour) DO UPDATE SET
              messages = messages + 1
            """,
            (guild_id, hour_key),
        )

        # 3) reaction histograms per day (message-level to bucket)
        def _bucket9(v: int) -> int:
            return v if v < 9 else 9

        if rx_total:
            cur.execute(
                """
                INSERT INTO reaction_hist_daily(guild_id,day,kind,bucket,n)
                VALUES(?,?,?,?,1)
                ON CONFLICT(guild_id,day,kind,bucket) DO UPDATE SET n = n + 1
                """,
                (guild_id, day, "count", _bucket9(rx_total)),
            )
        if rx_diversity:
            cur.execute(
                """
                INSERT INTO reaction_hist_daily(guild_id,day,kind,bucket,n)
                VALUES(?,?,?,?,1)
                ON CONFLICT(guild_id,day,kind,bucket) DO UPDATE SET n = n + 1
                """,
                (guild_id, day, "diversity", _bucket9(rx_diversity)),
            )

        # 4) response latency (per channel) via last_ts gap -> log2(msec) bucket
        cur.execute(
            "SELECT last_ts_utc FROM channel_last_msg WHERE guild_id=? AND channel_id=?",
            (guild_id, channel_id),
        )
        row = cur.fetchone()
        if row and row["last_ts_utc"]:
            try:
                prev = dt.datetime.fromisoformat(
                    row["last_ts_utc"].replace("Z", "+00:00")
                )
                gap = (created_at - prev).total_seconds() * 1000.0
                if (
                    0 <= gap <= 24 * 60 * 60 * 1000
                ):  # cap to 24h to avoid cross-day restarts
                    b = _log2_bucket_millis(gap)
                    cur.execute(
                        """
                        INSERT INTO latency_hist_daily(guild_id,channel_id,day,bucket,n)
                        VALUES(?,?,?,?,1)
                        ON CONFLICT(guild_id,channel_id,day,bucket) DO UPDATE SET n=n+1
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
              last_ts_utc=excluded.last_ts_utc,
              last_msg_id=excluded.last_msg_id,
              last_author=excluded.last_author
            """,
            (
                guild_id,
                channel_id,
                (
                    created_at.iso8601()
                    if hasattr(created_at, "iso8601")
                    else created_at.isoformat()
                ),
                int(message.id),
                author_id,
            ),
        )

        # 5) per-user daily vocabulary set (for lexical diversity)
        if words:
            tokens = set(_tokenize(content))
            cur.executemany(
                "INSERT OR IGNORE INTO user_token_daily(guild_id,user_id,day,token) VALUES(?,?,?,?)",
                [(guild_id, author_id, day, t) for t in tokens],
            )

        # 6) optional VADER sentiment aggregate per user/day (if available)
        sia = _get_sia()
        if sia and content:
            scores = sia.polarity_scores(content)
            cur.execute(
                """
                INSERT INTO sentiment_daily(guild_id,user_id,day,n,sum_compound,sum_pos,sum_neg,sum_neu)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(guild_id,user_id,day) DO UPDATE SET
                  n = n + 1,
                  sum_compound = sum_compound + excluded.sum_compound,
                  sum_pos = sum_pos + excluded.sum_pos,
                  sum_neg = sum_neg + excluded.sum_neg,
                  sum_neu = sum_neu + excluded.sum_neu
                """,
                (
                    guild_id,
                    author_id,
                    day,
                    1,
                    float(scores["compound"]),
                    float(scores["pos"]),
                    float(scores["neg"]),
                    float(scores["neu"]),
                ),
            )

        # 7) thread index (reply chains)
        # rely on message.reference.message_id if present; otherwise treat as new root
        parent_id = getattr(getattr(message, "reference", None), "message_id", None)
        root_id = int(message.id) if not parent_id else None
        depth = 0
        created_iso = created_at.isoformat()

        if parent_id:
            # find parent's thread mapping, else parent becomes root
            cur.execute(
                "SELECT root_id, depth FROM message_thread WHERE guild_id=? AND message_id=?",
                (guild_id, int(parent_id)),
            )
            pr = cur.fetchone()
            if pr:
                root_id = int(pr["root_id"])
                depth = int(pr["depth"]) + 1
            else:
                root_id = int(parent_id)
                depth = 1

        # ensure thread_index row exists
        cur.execute(
            """
            INSERT INTO thread_index(guild_id,root_id,started_utc,last_utc,max_deptH,messages)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(guild_id,root_id) DO NOTHING
            """,
            (guild_id, int(root_id), created_iso, created_iso, 0, 1),
        )
        # map message to thread
        cur.execute(
            """
            INSERT INTO message_thread(guild_id,message_id,root_id,parent_id,depth,created_utc,channel_id)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(guild_id, message_id) DO NOTHING
            """,
            (
                guild_id,
                int(message.id),
                int(root_id),
                int(parent_id) if parent_id else None,
                int(depth),
                created_iso,
                channel_id,
            ),
        )
        # update thread stats
        cur.execute(
            """
            UPDATE thread_index
               SET last_utc = CASE WHEN last_utc < ? THEN ? ELSE last_utc END,
                   max_depth = CASE WHEN ? > max_depth THEN ? ELSE max_depth END,
                   messages = messages + 1
             WHERE guild_id=? AND root_id=?
            """,
            (created_iso, created_iso, int(depth), int(depth), guild_id, int(root_id)),
        )

        con.commit()
    finally:
        con.close()


# ---------- Query helpers (no heavy scans, only aggregates) ----------


@dataclass
class BasicStats:
    min: int
    max: int
    mean: float
    std: float
    skewness: float
    kurtosis: float
    gini: float


def _np_array(vals: List[int]) -> List[int]:
    return vals


def _gini(arr: List[int]) -> float:
    import numpy as np

    x = np.asarray(arr, dtype=float)
    if x.size == 0:
        return float("nan")
    if (x < 0).any():
        return float("nan")
    s = x.sum()
    if s == 0:
        return 0.0
    x = np.sort(x)
    n = x.size
    cum = np.cumsum(x, dtype=float)
    return float((2.0 * (np.arange(1, n + 1) * x).sum() / (n * s)) - (n + 1) / n)


def get_basic_stats(guild_id: int, start_day: str, end_day: str) -> BasicStats:
    """
    Compute min/max/mean/std/skew/kurtosis/gini of per-user message counts in [start_day, end_day].
    """
    import numpy as np
    import scipy.stats as st

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
    counts = np.array([int(r["m"]) for r in rows], dtype=float)
    if counts.size == 0:
        return BasicStats(0, 0, 0.0, 0.0, float("nan"), float("nan"), float("nan"))
    return BasicStats(
        min=int(counts.min()),
        max=int(counts.max()),
        mean=float(counts.mean()),
        std=float(counts.std(ddof=0)),
        skewness=(
            float(st.skew(counts, bias=False)) if counts.size > 2 else float("nan")
        ),
        kurtosis=(
            float(st.kurtosis(counts, fisher=True, bias=False))
            if counts.size > 3
            else float("nan")
        ),
        gini=_gini(counts.tolist()),
    )


def get_hourly_counts(guild_id: int, start_hour: str, end_hour: str) -> Dict[str, int]:
    """
    Returns { 'YYYY-MM-DDTHH': messages } for hours in [start_hour, end_hour].
    """
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, messages FROM message_metrics_houry
            WHERE guild_id=? AND hour BETWEEN ? AND ?
            ORDER BY hour
            """,
            (guild_id, start_hour, end_hour),
        ).fetchall()
    finally:
        con.close()
    return {r["hour"]: int(r["messages"]) for r in rows}


def get_heatmap(guild_id: int, start_day: str, end_day: str) -> List[List[float]]:
    """
    7x24 matrix of avg msgs per (weekday,hour) over [start_day,end_day].
    Uses message_metrics_hourly.
    """
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, SUM(messages) AS m
            FROM message_metrics_hourly
            WHERE guild_id=? AND substr(hour,1,10) BETWEEN ? AND ?
            GROUP BY hour
            ORDER BY hour
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
    finally:
        con.close()

    # Build counts per weekday/hour and days-per-weekday in window
    from collections import Counter

    by = {(r["hour"][:10], int(r["hour"][11:13])): int(r["m"]) for r in rows}
    # count how many times each weekday occurs in window
    start_dt = dt.date.fromisoformat(start_day)
    end_dt = dt.date.fromisoformat(end_day)
    dow_days = Counter()
    d = start_dt
    while d <= end_dt:
        dow_days[d.weekday()] += 1
        d += dt.timedelta(days=1)

    grid = [[0.0 for _ in range(24)] for _ in range(7)]
    for (day, hour), m in by.items():
        dow = dt.date.fromisoformat(day).weekday()
        grid[dow][hour] += m
    for dow in range(7):
        denom = dow_days.get(dow, 1)
        if denom == 0:
            denom = 1
        for h in range(24):
            grid[dow][h] = grid[dow][h] / denom
    return grid


def get_burst_std_24h(
    guild_id: int, start_hour: str, end_hour: str
) -> Dict[str, float]:
    """
    24-hour rolling std of hourly message counts, keyed by hour.
    """
    import pandas as pd

    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, messages FROM message_metrics_hourly
            WHERE guild_id=? AND hour BETWEEN ? AND ?
            ORDER BY hour
            """,
            (guild_id, start_hour, end_hour),
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return {}
    s = pd.Series(
        {pd.Timestamp(r["hour"] + ":00+00:00"): int(r["messages"]) for r in rows}
    ).sort_index()
    std = s.rolling("24H", min_periods=1).std().fillna(0.0)
    return {k.strftime("%Y-%m-%dT%H"): float(v) for k, v in std.items()}


def get_silence_ratio(guild_id: int, start_hour: str, end_hour: str) -> float:
    """
    Share of hours in [start_hour, end_hour] with zero messages.
    """
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, messages FROM message_metrics_hourly
            WHERE guild_id=? AND hour BETWEEN ? AND ?
            """,
            (guild_id, start_hour, end_hour),
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return 1.0
    total = 0
    zeros = 0
    for r in rows:
        total += 1
        if int(r["messages"]) == 0:
            zeros += 1
    return float(zeros) / float(total) if total else 1.0


def _quantiles_from_hist(buckets: List[int], probs: List[float]) -> List[float]:
    """
    buckets[i] is count for log2(ms)==i, with i capped at maxBin.
    Returns approximate quantiles in milliseconds via histogram CDF.
    """
    import numpy as np

    counts = np.array(buckets, dtype=float)
    if counts.sum() == 0:
        return [float("nan") for _ in probs]
    cdf = np.cumsum(counts) / counts.sum()
    outs = []
    for p in probs:
        idx = np.searchsorted(cdf, p, side="left")
        ms = 2**idx
        outs.append(float(ms))
    return outs


def get_latency_stats(guild_id: int, start_day: str, end_day: str) -> Dict[str, Any]:
    """
    Per-channel and global approx latency stats derived from log2(ms) histograms.
    Returns:
      {
        "channels": [{"channel_id": 123, "median_ms": ..., "p95_ms": ..., "n": ...}, ...],
        "global": {"median_ms": ..., "p95_ms": ..., "n": ...}
      }
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
        b = int(r["bucket"])
        n = int(r["n"])
        cid = int(r["channel_id"])
        by_chan[cid][b] += n
        global_hist[b] += n

    out_channels = []
    for cid, hist in by_chan.items():
        med, p95 = _quantiles_from_hist(hist, [0.5, 0.95])
        out_channels.append(
            {"channel_id": cid, "median_ms": med, "p95_ms": p95, "n": int(sum(hist))}
        )
    gmed, gp95 = _quantiles_from_hist(global_hist, [0.5, 0.95])
    return {
        "channels": out_channels,
        "global": {"median_ms": gmed, "p95_ms": gp95, "n": int(sum(global_hist))},
    }


def get_reaction_distributions(
    guild_id: int, start_day: str, end_day: str
) -> Dict[str, Dict[str, int]]:
    """
    Returns histogram dicts for reaction 'count' and 'diversity' kinds with 0..9 buckets (9 means 9+).
    """
    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT kind, bucket, SUM(n) AS n
            FROM reaction_hist_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY kind, bucket
            """,
            (guild_id, start_day, end_day),
        ).fetchall()
    finally:
        con.close()

    out: Dict[str, Dict[str, int]] = {"count": {}, "diversity": {}}
    for r in rows:
        kind = r["kind"]
        bucket = int(r["bucket"])
        out[kind][str(bucket)] = int(r["n"])
    # ensure keys 0..9 exist
    for k in ("count", "diversity"):
        for b in range(0, 10):
            out[k].setdefault(str(b), 0)
    return out


def get_content_stats(guild_id: int, start_day: str, end_day: str) -> Dict[str, Any]:
    """
    Returns: total_messages, total_words, words_per_msg_mean/median, url_rate,
             lexical_diversity_by_user (type-token ratio across window),
             sentiment (coverage + mean/median compound if available)
    """
    import numpy as np

    con = connect()
    try:
        cur = con.cursor()
        dd = cur.execute(
            """
            SELECT user_id, SUM(messages) AS m, SUM(words) AS w
            FROM message_metrics_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchall()

        url_rows = cur.execute(
            """
            SELECT SUM(url_msgs) AS url_msgs, SUM(messages) AS msgs
            FROM message_metrics_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            """,
            (guild_id, start_day, end_day),
        ).fetchone()

        # lexical diversity: distinct tokens per user / total words per user over window
        tok_rows = cur.execute(
            """
            SELECT user_id, COUNT(DISTINCT token) AS uniq_toks
            FROM user_token_daily
            WHERE guild_id=? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchall()

        senti_rows = cur.execute(
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

    total_messages = int(sum(int(r["m"] or 0) for r in dd))
    total_words = int(sum(int(r["w"] or 0) for r in dd))
    w_per_msg = []
    for r in dd:
        m = int(r["m"] or 0)
        w = int(r["w"] or 0)
        if m > 0:
            w_per_msg.append(w / m)
    words_per_msg_mean = float(np.mean(w_per_msg)) if w_per_msg else 0.0
    words_per_msg_median = float(np.median(w_per_msg)) if w_per_msg else 0.0

    url_msgs = int((url_rows["url_msgs"] or 0) if url_rows else 0)
    msgs = int((url_rows["msgs"] or 0) if url_rows else 0)
    url_rate = (url_msgs / msgs) if msgs else 0.0

    uniq_by_user = {int(r["user_id"]): int(r["uniq_toks"]) for r in tok_rows}
    ttr_by_user: Dict[int, float] = {}
    for r in dd:
        uid = int(r["user_id"])
        m = int(r["m"] or 0)
        w = int(r["w"] or 0)
        uniq = uniq_by_user.get(uid, 0)
        denom = max(w, 1)
        ttr_by_user[uid] = float(uniq) / float(denom)

    # sentiment
    cov = 0.0
    cvals: List[float] = []
    if senti_rows:
        n_tot = sum(int(r["n"] or 0) for r in senti_rows)
        cov = float(n_tot) / float(msgs) if msgs else 0.0
        for r in senti_rows:
            n = int(r["n"] or 0)
            csum = float(r["csum"] or 0.0)
            if n > 0:
                cvals.append(csum / n)
    sentiment_compound_mean = float(np.mean(cvals)) if cvals else None
    sentiment_compound_median = float(np.median(cvals)) if cvals else None

    return {
        "total_messages": total_messages,
        "total_words": total_words,
        "words_per_msg_mean": words_per_msg_mean,
        "words_per_msg_median": words_per_msg_median,
        "url_rate": float(url_rate),
        "lexical_diversity_by_user": ttr_by_user,
        "sentiment": {
            "coverage": cov,
            "compound_mean": sentiment_compound_mean,
            "compound_median": sentiment_compound_median,
        },
    }


def get_thread_stats(guild_id: int, start_iso: str, end_iso: str) -> Dict[str, Any]:
    """
    Returns median/p95 lifespan (seconds) and depth stats for threads that *overlap* [start_iso, end_iso].
    """
    import numpy as np

    con = connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT started_utc, last_utc, max_depth, messages
            FROM thread_index
            WHERE guild_id=? AND NOT (last_utc < ? OR started_utc > ?)
            """,
            (guild_id, start_iso, end_iso),
        ).fetchall()
    finally:
        con.close()

    if not rows:
        return {
            "n_threads": 0,
            "lifespan_median_s": None,
            "lifespan_p95_s": None,
            "depth_median": None,
            "depth_max": None,
            "messages_total": 0,
        }

    lifespans = []
    depths = []
    msgs = 0
    for r in rows:
        try:
            a = dt.datetime.fromisoformat(r["started_utc"].replace("Z", "+00:00"))
            b = dt.datetime.fromisoformat(r["last_utc"].replace("Z", "+00:00"))
            lifespans.append((b - a).total_seconds())
        except Exception:
            pass
        depths.append(int(r["max_depth"] or 0))
        msgs += int(r["messages"] or 0)

    import numpy as np

    return {
        "n_threads": len(rows),
        "lifespan_median_s": float(np.median(lifespans)) if lifespans else None,
        "lifespan_p95_s": float(np.percentile(lifespans, 95)) if lifespans else None,
        "depth_median": float(np.median(depths)) if depths else None,
        "depth_max": int(max(depths)) if depths else None,
        "messages_total": int(msgs),
    }
