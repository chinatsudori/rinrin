from __future__ import annotations

import datetime as dt
import math
import os
import sqlite3
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException
from starlette.responses import JSONResponse

router = APIRouter(prefix="/api/activity", tags=["activity"])


_TABLE_COLUMN_CACHE: Dict[Tuple[str, str], bool] = {}


def _con() -> sqlite3.Connection:
    path = os.getenv("BOT_DB_PATH", "/app/data/bot.sqlite3")
    uri = f"file:{path}?mode=ro"
    try:
        c = sqlite3.connect(uri, uri=True, check_same_thread=False)
    except sqlite3.OperationalError:
        c = sqlite3.connect(path, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def _table_has_column(table: str, column: str) -> bool:
    key = (table, column)
    if key in _TABLE_COLUMN_CACHE:
        return _TABLE_COLUMN_CACHE[key]
    try:
        con = _con()
        cur = con.cursor()
        cur.execute(f"PRAGMA table_info('{table}')")
        cols = [str(r["name"]) for r in cur.fetchall()]
        has = column in cols
    except sqlite3.OperationalError:
        has = False
    finally:
        try:
            con.close()
        except Exception:
            pass
    _TABLE_COLUMN_CACHE[key] = has
    return has


def _bounds(days: int) -> Tuple[str, str, str, str]:
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_day = (now.date() - dt.timedelta(days=days)).isoformat()
    end_day = now.date().isoformat()
    start_hour = (now - dt.timedelta(hours=24 * days)).strftime("%Y-%m-%dT%H")
    end_hour = now.strftime("%Y-%m-%dT%H")
    return start_day, end_day, start_hour, end_hour


def _hour_limits(start_h: str, end_h: str) -> Tuple[str, str]:
    start_dt = dt.datetime.fromisoformat(f"{start_h}:00:00")
    end_dt = dt.datetime.fromisoformat(f"{end_h}:59:59")
    return (
        start_dt.isoformat(timespec="seconds"),
        end_dt.isoformat(timespec="seconds"),
    )


def _gini(values: List[int]) -> float:
    vals = [float(v) for v in values if v >= 0]
    if not vals:
        return float("nan")
    s = sum(vals)
    if s == 0:
        return 0.0
    vals.sort()
    n = len(vals)
    acc = 0.0
    for i, v in enumerate(vals, 1):
        acc += i * v
    return (2.0 * acc / (n * s)) - (n + 1) / n


def _mean_std(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 0.0
    n = float(len(vals))
    mu = sum(vals) / n
    var = sum((x - mu) ** 2 for x in vals) / n
    return float(mu), float(math.sqrt(var))


def _skew_kurtosis(vals: List[float]) -> Tuple[float, float]:
    n = len(vals)
    if n < 2:
        return float("nan"), float("nan")
    mu, sigma = _mean_std(vals)
    if sigma == 0.0:
        return 0.0, 0.0
    m3 = sum(((x - mu) ** 3) for x in vals) / n / (sigma**3)
    m4 = sum(((x - mu) ** 4) for x in vals) / n / (sigma**4)
    return float(m3), float(m4 - 3.0)


def _quantiles_from_hist(hist: List[int], probs=(0.5, 0.95)) -> Tuple[float, float]:
    total = sum(hist)
    if total == 0:
        return float("nan"), float("nan")
    cdf = []
    acc = 0
    for c in hist:
        acc += c
        cdf.append(acc / total)
    outs: List[float] = []
    for p in probs:
        i = 0
        while i < len(cdf) and cdf[i] < p:
            i += 1
        outs.append(float(2**i))
    return tuple(outs)  # type: ignore


def _hourly_counts(
    gid: int, start_h: str, end_h: str, user_id: Optional[int] = None
) -> Dict[str, int]:
    if user_id is not None and not _table_has_column("message_metrics_hourly", "user_id"):
        start_ts, end_ts = _hour_limits(start_h, end_h)
        try:
            con = _con()
            cur = con.cursor()
            rows = cur.execute(
                """
                SELECT STRFTIME('%Y-%m-%dT%H', created_at) AS hour, COUNT(*) AS messages
                FROM message_archive
                WHERE created_at BETWEEN ? AND ? AND user_id = ?
                GROUP BY hour
                ORDER BY hour
                """,
                (start_ts, end_ts, int(user_id)),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = []
        finally:
            try:
                con.close()
            except Exception:
                pass
        return {str(r["hour"]): int(r["messages"]) for r in rows}

    con = _con()
    try:
        cur = con.cursor()
        params: List[Any] = [gid, start_h, end_h]
        where = "guild_id = ? AND hour BETWEEN ? AND ?"
        if user_id is not None:
            where += " AND user_id = ?"
            params.append(int(user_id))
        rows = cur.execute(
            f"""
            SELECT hour, SUM(messages) AS messages
            FROM message_metrics_hourly
            WHERE {where}
            GROUP BY hour
            ORDER BY hour
            """,
            tuple(params),
        ).fetchall()
    finally:
        con.close()
    return {str(r["hour"]): int(r["messages"]) for r in rows}


def _heatmap(
    gid: int, start_day: str, end_day: str, user_id: Optional[int] = None
) -> List[List[float]]:
    start = dt.date.fromisoformat(start_day)
    end = dt.date.fromisoformat(end_day)
    days_per_dow = [0] * 7
    d = start
    while d <= end:
        days_per_dow[d.weekday()] += 1
        d += dt.timedelta(days=1)

    start_hour = f"{start_day}T00"
    end_hour = f"{end_day}T23"
    hourly = _hourly_counts(gid, start_hour, end_hour, user_id=user_id)

    grid = [[0.0 for _ in range(24)] for _ in range(7)]
    for h, count in hourly.items():
        try:
            day = dt.date.fromisoformat(h[:10])
            dow = day.weekday()
            hr = int(h[11:13])
        except (ValueError, IndexError):
            continue
        grid[dow][hr] += int(count)
    for dow in range(7):
        denom = max(1, days_per_dow[dow])
        for hr in range(24):
            grid[dow][hr] = grid[dow][hr] / float(denom)
    return grid


def _burst_std24(
    gid: int, start_h: str, end_h: str, user_id: Optional[int] = None
) -> Dict[str, float]:
    start_dt = dt.datetime.fromisoformat(f"{start_h}:00+00:00")
    end_dt = dt.datetime.fromisoformat(f"{end_h}:00+00:00")
    hours: List[str] = []
    t = start_dt
    while t <= end_dt:
        hours.append(t.strftime("%Y-%m-%dT%H"))
        t += dt.timedelta(hours=1)

    raw = _hourly_counts(gid, start_h, end_h, user_id=user_id)
    series = [int(raw.get(h, 0)) for h in hours]

    out: Dict[str, float] = {}
    dq: deque[int] = deque()
    import statistics

    for i, h in enumerate(hours):
        dq.append(series[i])
        if len(dq) > 24:
            dq.popleft()
        out[h] = float(statistics.pstdev(dq)) if len(dq) > 1 else 0.0
    return out


def _latency_stats(gid: int, start_day: str, end_day: str) -> Dict[str, Any]:
    con = _con()
    try:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT channel_id, bucket, SUM(n) AS n
            FROM latency_hist_daily
            WHERE guild_id = ? AND day BETWEEN ? AND ?
            GROUP BY channel_id, bucket
            """,
            (gid, start_day, end_day),
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

    chans = []
    for cid, hist in by_chan.items():
        med, p95 = _quantiles_from_hist(hist, (0.5, 0.95))
        chans.append(
            {"channel_id": cid, "median_ms": med, "p95_ms": p95, "n": int(sum(hist))}
        )
    gmed, gp95 = _quantiles_from_hist(global_hist, (0.5, 0.95))
    return {
        "channels": chans,
        "global": {"median_ms": gmed, "p95_ms": gp95, "n": int(sum(global_hist))},
    }


def _content_stats(
    gid: int, start_day: str, end_day: str, user_id: Optional[int] = None
) -> Dict[str, Any]:
    con = _con()
    try:
        cur = con.cursor()
        params: List[Any] = [gid, start_day, end_day]
        where = "guild_id = ? AND day BETWEEN ? AND ?"
        if user_id is not None:
            where += " AND user_id = ?"
            params.append(int(user_id))
        rows = cur.execute(
            f"""
            SELECT user_id, SUM(messages) AS m, SUM(words) AS w, SUM(url_msgs) AS u
              FROM message_metrics_daily
             WHERE {where}
             GROUP BY user_id
            """,
            tuple(params),
        ).fetchall()
        tok_rows = cur.execute(
            f"""
            SELECT user_id, COUNT(DISTINCT token) AS uniq
              FROM user_token_daily
             WHERE {where}
             GROUP BY user_id
            """,
            tuple(params),
        ).fetchall()
        # sentiment optional
        try:
            sent_rows = cur.execute(
                f"""
                SELECT user_id, SUM(n) AS n, SUM(sum_compound) AS csum
                  FROM sentiment_daily
                 WHERE {where}
                 GROUP BY user_id
                """,
                tuple(params),
            ).fetchall()
        except sqlite3.OperationalError:
            sent_rows = []
    finally:
        con.close()

    total_msgs = sum(int(r["m"] or 0) for r in rows)
    total_words = sum(int(r["w"] or 0) for r in rows)
    url_msgs = sum(int(r["u"] or 0) for r in rows)

    # words/msg stats
    samples = [int(r["w"] or 0) / int(r["m"]) for r in rows if int(r["m"] or 0) > 0]
    mu = sum(samples) / len(samples) if samples else 0.0
    med = sorted(samples)[len(samples) // 2] if samples else 0.0

    # lexical diversity per user: distinct tokens over total words in window
    uniq_map = {int(r["user_id"]): int(r["uniq"]) for r in tok_rows}
    ttr: Dict[int, float] = {}
    for r in rows:
        uid = int(r["user_id"])
        w = int(r["w"] or 0)
        ttr[uid] = (uniq_map.get(uid, 0) / float(w)) if w else 0.0

    # sentiment coverage + mean/median compound if table present
    n_scored = sum(int(r["n"] or 0) for r in sent_rows)
    coverage = (n_scored / float(total_msgs)) if total_msgs else 0.0
    comp_vals = [
        (float(r["csum"]) / max(int(r["n"]), 1))
        for r in sent_rows
        if int(r["n"] or 0) > 0
    ]
    comp_mean = (sum(comp_vals) / len(comp_vals)) if comp_vals else None
    comp_median = sorted(comp_vals)[len(comp_vals) // 2] if comp_vals else None

    return {
        "total_messages": int(total_msgs),
        "total_words": int(total_words),
        "words_per_msg_mean": float(mu),
        "words_per_msg_median": float(med),
        "url_rate": float((url_msgs / total_msgs) if total_msgs else 0.0),
        "lexical_diversity_by_user": ttr,
        "sentiment": {
            "coverage": float(coverage),
            "compound_mean": comp_mean,
            "compound_median": comp_median,
        },
    }


def _activity_rankings(
    gid: int, end_dt: dt.datetime, limit: int = 5
) -> Dict[str, List[Dict[str, int]]]:
    windows = {
        "day": end_dt - dt.timedelta(days=1),
        "week": end_dt - dt.timedelta(days=7),
        "month": end_dt - dt.timedelta(days=30),
        "all": None,
    }
    out: Dict[str, List[Dict[str, int]]] = {}
    has_guild = _table_has_column("message_archive", "guild_id")
    try:
        con = _con()
        cur = con.cursor()
        for name, start_dt in windows.items():
            where_parts = []
            params: List[Any] = []
            if has_guild:
                where_parts.append("guild_id = ?")
                params.append(gid)
            if start_dt is not None:
                where_parts.append("created_at BETWEEN ? AND ?")
                params.append(start_dt.isoformat(timespec="seconds"))
                params.append(end_dt.isoformat(timespec="seconds"))
            else:
                where_parts.append("created_at <= ?")
                params.append(end_dt.isoformat(timespec="seconds"))
            where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
            try:
                rows = cur.execute(
                    f"""
                    SELECT user_id, COUNT(*) AS messages
                    FROM message_archive
                    {where_sql}
                    GROUP BY user_id
                    ORDER BY messages DESC
                    LIMIT ?
                    """,
                    (*params, limit),
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            out[name] = [
                {"user_id": int(r["user_id"]), "messages": int(r["messages"])}
                for r in rows
            ]
    except sqlite3.OperationalError:
        return {}
    finally:
        try:
            con.close()
        except Exception:
            pass
    return out


@router.get("/{guild_id}/live")
def live_metrics(
    guild_id: int,
    days: int = 30,
    start: Optional[str] = None,
    end: Optional[str] = None,
    scope: str = "guild",
    user_id: Optional[int] = None,
):
    if scope not in {"guild", "personal"}:
        raise HTTPException(status_code=400, detail="invalid scope")
    if scope == "personal" and user_id is None:
        raise HTTPException(status_code=400, detail="user_id required for personal scope")

    if start and not end or end and not start:
        raise HTTPException(status_code=400, detail="start and end must both be provided")

    if start and end:
        try:
            start_date = dt.date.fromisoformat(start)
            end_date = dt.date.fromisoformat(end)
        except ValueError:
            raise HTTPException(status_code=400, detail="start/end must be YYYY-MM-DD")
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="start must be <= end")
        start_day = start_date.isoformat()
        end_day = end_date.isoformat()
        start_hour = f"{start_day}T00"
        end_hour = f"{end_day}T23"
    else:
        if days <= 0:
            raise HTTPException(status_code=400, detail="days must be > 0")
        start_day, end_day, start_hour, end_hour = _bounds(days)

    range_end_dt = dt.datetime.fromisoformat(f"{end_day}T23:59:59")
    filter_user = int(user_id) if scope == "personal" and user_id is not None else None

    # basic distribution scoped to guild or specific user
    con = _con()
    try:
        cur = con.cursor()
        params: List[Any] = [guild_id, start_day, end_day]
        where = "guild_id = ? AND day BETWEEN ? AND ?"
        if filter_user is not None:
            where += " AND user_id = ?"
            params.append(filter_user)
        rows = cur.execute(
            f"""
            SELECT user_id, SUM(messages) AS m
            FROM message_metrics_daily
            WHERE {where}
            GROUP BY user_id
            """,
            tuple(params),
        ).fetchall()
    finally:
        con.close()

    counts = [int(r["m"] or 0) for r in rows]
    f = [float(x) for x in counts]
    mean = sum(f) / len(f) if f else 0.0
    var = sum((x - (mean or 0.0)) ** 2 for x in f) / (len(f) or 1)
    std = math.sqrt(var)
    skew = (
        (sum(((x - (mean or 0.0)) ** 3) for x in f) / (len(f) or 1) / (std**3))
        if len(f) > 1 and std > 0
        else 0.0
    )
    kurt = (
        (sum(((x - (mean or 0.0)) ** 4) for x in f) / (len(f) or 1) / (std**4) - 3.0)
        if len(f) > 1 and std > 0
        else 0.0
    )
    basic = {
        "min": float(min(counts) if counts else 0),
        "max": float(max(counts) if counts else 0),
        "mean": float(mean),
        "std": float(std),
        "skewness": float(skew) if counts else float("nan"),
        "kurtosis": float(kurt) if counts else float("nan"),
        "gini": float(_gini(counts)) if counts else float("nan"),
    }

    heat = _heatmap(guild_id, start_day, end_day, user_id=filter_user)
    burst = _burst_std24(guild_id, start_hour, end_hour, user_id=filter_user)
    hourly = _hourly_counts(guild_id, start_hour, end_hour, user_id=filter_user)
    zeros = sum(1 for _, v in (hourly or {}).items() if int(v) == 0)
    silence_ratio = float(zeros) / float(len(hourly or {}) or 1)
    latency = (
        None if scope == "personal" else _latency_stats(guild_id, start_day, end_day)
    )
    content = _content_stats(guild_id, start_day, end_day, user_id=filter_user)
    rankings = _activity_rankings(guild_id, range_end_dt)

    return JSONResponse(
        {
            "range": {
                "start_day": start_day,
                "end_day": end_day,
                "start_hour": start_hour,
                "end_hour": end_hour,
            },
            "scope": scope,
            "basic": basic,
            "temporal": {
                "heatmap_avg_per_hour": heat,
                "burst_std_24h": burst,
                "hourly_counts": hourly,
                "silence_ratio": silence_ratio,
            },
            "latency": latency,
            "content": content,
            "rankings": rankings,
        }
    )
