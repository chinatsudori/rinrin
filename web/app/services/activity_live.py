from __future__ import annotations

import datetime as dt
import math
import os
import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# ---------- DB connector (read-only) ----------


def _connect() -> sqlite3.Connection:
    db_path = os.getenv("BOT_DB_PATH", "/app/data/bot.sqlite3")
    # open read-only if possible
    uri = f"file:{db_path}?mode=ro"
    try:
        con = sqlite3.connect(uri, uri=True, check_same_thread=False)
    except sqlite3.OperationalError:
        # fallback to rw (e.g., when running dev without uri support)
        con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


# ---------- helpers ----------


def _gini(values: List[int]) -> float:
    """Gini coefficient for non-negative values. Returns NaN if empty."""
    vals = [float(v) for v in values if v >= 0]
    if not vals:
        return float("nan")
    s = sum(vals)
    if s == 0:
        return 0.0
    vals.sort()
    n = len(vals)
    cum = 0.0
    acc = 0.0
    for i, v in enumerate(vals, 1):
        cum += v
        acc += i * v
    return (2.0 * acc / (n * s)) - (n + 1) / n


def _mean_std(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 0.0
    n = float(len(vals))
    mu = sum(vals) / n
    var = (
        sum((x - mu) ** 2 for x in vals) / n
    )  # population std (to mirror scipy default bias=True)
    return mu, math.sqrt(var)


def _skew_kurtosis(vals: List[float]) -> Tuple[float, float]:
    """Population skewness and excess kurtosis (approximately, like scipy with bias=True)."""
    n = len(vals)
    if n < 2:
        return float("nan"), float("nan")
    mu, sigma = _calculate_mu_sigma(vals)
    if sigma == 0:
        return 0.0, 0.0
    m3 = sum(((x - mu) / sigma) ** 3 for x in vals) / n
    m4 = sum(((x - mu) / sigma) ** 4 for x in vals) / n
    return float(m3), float(m4 - 3.0)


def _calculate_mu_sigma(vals: List[float]) -> Tuple[float, float]:
    n = float(len(vals))
    mu = sum(vals) / n if n else 0.0
    var = sum((x - mu) ** 2 for x in vals) / n if n else 0.0
    return mu, math.sqrt(var)


def _quantiles_from_hist(
    hist: List[int], probs: Tuple[float, float]
) -> Tuple[float, float]:
    """Approximate quantiles from histogram of log2(ms) buckets. Returns (p50_ms, p95_ms)."""
    total = sum(hist)
    if total == 0:
        return float("nan"), float("nan")
    cdf = []
    acc = 0
    for count in hist:
        acc += count
        cdf.append(acc / total)
    outs: List[float] = []
    for p in probs:
        i = 0
        while i < len(cdf) and cdf[i] < p:
            i += 1
        # use lower edge estimate for bucket i
        ms = float(2**i)
        outs.append(ms)
    return tuple(outs)  # type: ignore


# ---------- metrics over aggregate tables ----------


def get_basic_stats(guild_id: int, start_day: str, end_day: str) -> Dict[str, float]:
    """
    min/max/mean/std/skewness/kurtosis/gini of per-user total message counts in [start_day, end_day] (inclusive).
    Uses message_metrics_daily (messages).
    """
    con = _open = _connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT user_id, SUM(messages) AS m
            FROM message_metrics_daily
            WHERE guild_id = ? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchAll()
        counts = [int(r["m"] or 0) for r in rows]
    finally:
        con.close()
    if not counts:
        return {
            "min": 0,
            "max": 0,
            "mean": 0.0,
            "std": 0.0,
            "skewness": float("nan"),
            "kurtosis": float("nan"),
            "gini": float("nan"),
        }
    mu, sigma = _calculate_mu_sigma([float(x) for x in counts])
    skew, kurt = _skew_kurtosis([float(x) for x in counts])
    return {
        "min": float(min(counts)),
        "max": float(max(counts)),
        "mean": float(mu),
        "std": float(sigma),
        "skewness": float(skew),
        "kurtosis": float(kurt),
        "gini": float(_gini(counts)),
    }


def get_hourly_counts(guild_id: int, start_hour: str, end_hour: str) -> Dict[str, int]:
    """
    Returns {'YYYY-MM-DDTHH': messages} for hours in [start_hour, end_hour].
    Uses message_metrics_hourly.
    """
    con = _connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, messages
            FROM message_metrics_hourly
            WHERE guild_id = ? AND hour BETWEEN ? AND ?
            ORDER BY hour
            """,
            (guild_id, start_hour, end_hour),
        ).fetchAll()
        return {str(r["hour"]): int(r["messages"]) for r in rows}
    finally:
        con.close()


def get_heatmap(guild_id: int, start_day: str, end_day: str) -> List[List[float]]:
    """
    7x24 matrix (rows=Mon..Sun as 0..6) of average message count per hour bucket for each weekday over [start_day,end_day].
    Uses message_metrics_hourly; normalizes by how many occurrences of each weekday fall within the range.
    """
    # count how many times each weekday occurs in [start_day, end_day]
    start = dt.date.fromisoformat(start_day)
    end = dt.date.fromisoformat(end_day)
    days_per_dow = [0] * 7
    d = start
    while d <= end:
        days_per_dow[d.weekday()] += 1
        d += dt.timedelta(days=1)

    con = _connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT hour, messages
            FROM message_metrics_hourly
            WHERE guild_id = ? AND substr(hour,1,10) BETWEEN ? AND ?
            """,
            (guild_id, start_day, end_day),
        ).fetchAll()
    finally:
        con.close()

    grid = [[0.0 for _ in range(24)] for _ in range(7)]
    for r in rows:
        h = str(r["hour"])  # 'YYYY-MM-DDTHH'
        day = dt.date.fromisoformat(h[:10])
        dow = day.weekday()
        hr = int(h[11:13])
        grid[dow][hr] += int(r["messages"])

    for dow in range(7):
        denom = max(1, days_per_dow[dow])
        for hr in range(24):
            grid[dow][hr] = grid[dow][hr] / float(denom)
    return grid


def get_burst_std_24h(
    guild_id: int, start_hour: str, end_hour: str
) -> Dict[str, float]:
    """
    24-hour rolling population std of hourly message counts in [start_hour,end_hour].
    Uses message_metrics_hourly; fills missing hours with zero.
    """
    # Build hour timeline
    start_dt = dt.datetime.fromisoformat(f"{start_hour}:00+00:00")
    end_dt = dt.datetime.fromisoformat(f"{end_hour}:00+00:00")
    hours: List[str] = []
    t = start_dt
    while t <= end_dt:
        hours.append(t.strftime("%Y-%m-%dT%H"))
        t += dt.timedelta(hours=1)

    raw = get_hourly_counts(guild_id, start_hour, end_hour)
    series = [int(raw.get(h, 0)) for h in hours]

    # rolling population std with window=24
    from collections import deque
    import statistics

    out: Dict[str, float] = {}
    dq: deque[int] = deque()
    for i, h in enumerate(hours):
        dq.append(series[i])
        if len(dq) > 24:
            dq.popleft()
        if len(dq) <= 1:
            out[h] = 0.0
        else:
            out[h] = float(statistics.pstdev(dq))
    return out


def get_latency_stats(guild_id: int, start_day: str, end_day: str) -> Dict[str, Any]:
    """
    Returns per-channel + global latency distributions as median/p95 (ms) using latency_hist_daily.
    """
    con = _connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT channel_id, bucket, SUM(n) AS n
            FROM latency_hist_daily
            WHERE guild_id = ? AND day BETWEEN ? AND ?
            GROUP BY channel_id, bucket
            """,
            (guild_id, start_day, end_day),
        ).fetchAll()
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

    channels: List[Dict[str, Any]] = []
    for cid, hist in by_chan.items():
        med, p95 = _quantiles_from_hist(hist, (0.5, 0.95))
        channels.append(
            {"channel_id": cid, "median_ms": med, "p95_ms": p95, "n": int(sum(hist))}
        )
    gmed, gp95 = _quantiles_from_hist(global_hist, (0.5, 0.95))
    return {
        "channels": channels,
        "global": {"median_ms": gmed, "p95_ms": gp95, "n": int(sum(global_hist))},
    }


def get_content_stats(guild_id: int, start_day: str, end_day: str) -> Dict[str, Any]:
    """
    totals, words/msg stats, url rate, lexical diversity per user, sentiment coverage & means (if available)
    Uses: message_metrics_daily, user_token_daily, sentiment_daily
    """
    con = _connect()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT user_id, SUM(messages) AS m, SUM(words) AS w, SUM(url_msgs) AS u
            FROM message_metrics_daily
            WHERE guild_id = ? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchAll()
        tok_rows = cur.execute(
            """
            SELECT user_id, COUNT(DISTINCT token) AS uniq
            FROM user_token_daily
            WHERE guild_id = ? AND day BETWEEN ? AND ?
            GROUP BY user_id
            """,
            (guild_id, start_day, end_day),
        ).fetchAll()
        # sentiment optional
        try:
            sent_rows = cur.execute(
                """
                SELECT user_id, SUM(n) AS n, SUM(sum_compound) AS csum
                FROM sentiment_daily
                WHERE guild_id = ? AND day BETWEEN ? AND ?
                GROUP BY user_id
                """,
                (guild_id, start_day, end_day),
            ).fetchAll()
        except sqlite3.OperationalError:
            sent_rows = []
    finally:
        con.close()

    total_msgs = sum(int(r["m"] or 0) for r in rows)
    total_words = sum(int(r["w"] or 0) for r in rows)
    words_per_msg_samples: List[float] = []
    url_msgs = 0
    for r in rows:
        m = int(r["m"] or 0)
        w = int(r["w"] or 0)
        u = int(r["u"] or 0)
        url_msgs += u
        if m > 0:
            words_per_msg_samples.append(w / m)
    mu, _std = _mean_std(words_per_msg_samples)
    med = (
        float(sorted(words_per_msg_samples)[len(words_per_msg_samples) // 2])
        if words_per_msg_samples
        else 0.0
    )
    url_rate = (url_msgs / total_msgs) if total_msgs else 0.0

    uniq_map = {int(r["user_id"]): int(r["uniq"]) for r in tok_rows}
    ttr_by_user: Dict[int, float] = {}
    for r in rows:
        uid = int(r["user_id"])
        w = int(r["w"] or 0)
        denom = max(1, w)
        ttr_by_user[uid] = float(uniq_map.get(uid, 0)) / float(denom)

    # sentiment coverage and aggregates
    n_scored = sum(int(r["n"] or 0) for r in sent_rows)
    coverage = float(n_scored) / float(total_msgs) if total_msgs else 0.0
    comp_vals: List[float] = []
    for r in sent_rows:
        n = int(r["n"] or 0)
        csum = float(r["csum"] or 0.0)
        if n > 0:
            comp_vals.append(max(-1.0, min(1.0, csum / n)))
    comp_mean = sum(comp_vals) / len(comp_vals) if comp_vals else None
    comp_median = sorted(comp_vals)[len(comp_vals) // 2] if comp_vals else None

    return {
        "total_messages": int(total_msgs),
        "total_words": int(total_words),
        "words_per_msg_mean": float(mu),
        "words_per_msg_median": float(med),
        "url_rate": float(url_rate),
        "lexical_diversity_by_user": ttr_by_user,
        "sentiment": {
            "coverage": float(coverage),
            "compound_mean": comp_mean,
            "compound_median": comp_median,
        },
    }
