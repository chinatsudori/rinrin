from __future__ import annotations

import json
import math
import statistics
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import DefaultDict, Dict, Iterable, List, Tuple

from zoneinfo import ZoneInfo

from ..db import connect
from ..models import activity
from ..models.activity_report import (
    NEGATIVE_WORDS,
    POSITIVE_WORDS,
    STOP_WORDS,
    TOKEN_PATTERN,
    URL_PATTERN,
)


@dataclass
class UserActivityDetails:
    message_count: int
    active_days: int
    total_days: int
    distribution: Dict[str, float]
    hourly_heatmap: List[List[int]]
    daily_heatmap: Dict[str, int]
    bursts: List[Tuple[str, int, float]]
    decay: List[Tuple[str, float]]
    silence_ratio_hourly: float
    silence_ratio_daily: float
    response_latency_seconds: float
    thread_lifespan_seconds: float
    reply_depth: float
    reply_density: float
    attention_ratio: float
    reaction_stats: Dict[str, float]
    reaction_diversity: int
    top_reactors_received: List[Tuple[int, int]]
    top_reactors_given: List[Tuple[int, int]]
    word_metrics: Dict[str, float]
    sentiment: Dict[str, float]
    topics: List[List[str]]
    embed_richness: float
    url_metrics: Dict[str, float]
    bot_ratio: float
    inequality: Dict[str, float]
    derived: Dict[str, float]


def _parse_iso(ts: str) -> datetime:
    if not ts:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S%z")


def _rolling_std(hourly_series: List[Tuple[datetime, int]], window_hours: int = 12) -> List[Tuple[str, int, float]]:
    dq: deque[int] = deque()
    result: List[Tuple[str, int, float]] = []
    timestamps: deque[datetime] = deque()
    for ts, count in hourly_series:
        dq.append(count)
        timestamps.append(ts)
        while timestamps and (ts - timestamps[0]).total_seconds() > window_hours * 3600:
            dq.popleft()
            timestamps.popleft()
        std = statistics.pstdev(dq) if len(dq) > 1 else 0.0
        result.append((ts.isoformat(), count, float(std)))
    return result


def _half_life(hourly_series: List[Tuple[datetime, int]]) -> List[Tuple[str, float]]:
    points: List[Tuple[str, float]] = []
    for i, (ts, count) in enumerate(hourly_series):
        if count <= 0:
            continue
        if i == 0 or i == len(hourly_series) - 1:
            continue
        prev_count = hourly_series[i - 1][1]
        next_count = hourly_series[i + 1][1]
        if count <= prev_count or count <= next_count:
            continue
        half = count / 2.0
        half_life = None
        for future_ts, future_count in hourly_series[i + 1 :]:
            if future_count <= half:
                delta_hours = (future_ts - ts).total_seconds() / 3600.0
                half_life = max(delta_hours, 0.0)
                break
        if half_life is None:
            half_life = 0.0
        points.append((ts.isoformat(), float(half_life)))
    return points


def _silence_ratio(counts: Dict[datetime, int], interval: timedelta) -> float:
    if not counts:
        return 1.0
    start = min(counts.keys())
    end = max(counts.keys())
    total_periods = int(((end - start) / interval)) + 1
    silent = sum(1 for ts in (start + interval * i for i in range(total_periods)) if counts.get(ts, 0) == 0)
    if total_periods <= 0:
        return 0.0
    return silent / total_periods


def _entropy(values: Iterable[int]) -> float:
    values = [v for v in values if v > 0]
    total = sum(values)
    if total <= 0:
        return 0.0
    entropy = 0.0
    for v in values:
        p = v / total
        entropy -= p * math.log(p, 2)
    return entropy


def _gini(values: List[int]) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    cum = 0.0
    for i, val in enumerate(sorted_vals, start=1):
        cum += i * val
    total = sum(sorted_vals)
    if total == 0:
        return 0.0
    return (2 * cum) / (n * total) - (n + 1) / n


def _skew_kurtosis(values: List[int]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    mean = statistics.mean(values)
    if len(values) < 2:
        return 0.0, 0.0
    std = statistics.pstdev(values)
    if std == 0:
        return 0.0, 0.0
    m3 = sum(((v - mean) / std) ** 3 for v in values) / len(values)
    m4 = sum(((v - mean) / std) ** 4 for v in values) / len(values)
    return float(m3), float(m4 - 3.0)


def _topic_distribution(token_counts: Counter[str], topics: int = 5, words_per_topic: int = 5) -> List[List[str]]:
    common = [tok for tok, _ in token_counts.most_common(topics * words_per_topic)]
    return [common[i * words_per_topic : (i + 1) * words_per_topic] for i in range(topics)]


def compute_user_activity_details(
    guild_id: int,
    user_id: int,
    *,
    timezone_name: str = "America/Los_Angeles",
) -> UserActivityDetails | None:
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT message_id, created_at, content, attachments, embeds, reactions, reply_to_id, channel_id
              FROM message_archive
             WHERE guild_id=? AND author_id=?
             ORDER BY created_at ASC, message_id ASC
            """,
            (guild_id, user_id),
        ).fetchall()

    if not rows:
        return None

    tz = ZoneInfo(timezone_name)
    timestamps: List[datetime] = []
    hourly_counts: DefaultDict[datetime, int] = defaultdict(int)
    hourly_heatmap: List[List[int]] = [[0 for _ in range(24)] for _ in range(7)]
    daily_counts: DefaultDict[str, int] = defaultdict(int)
    reaction_counts: List[int] = []
    reaction_emojis: Counter[str] = Counter()
    latencies: List[float] = []
    token_counts: Counter[str] = Counter()
    sentiment_scores: List[float] = []
    url_counts: Counter[str] = Counter()
    replies_total = 0
    reply_messages = 0
    messages_with_embeds = 0
    total_words = 0
    unique_tokens: set[str] = set()

    prev_time: datetime | None = None
    message_ids: List[int] = []

    for msg_id, created_at, content, attachments, embeds, reactions, reply_to_id, channel_id in rows:
        when = _parse_iso(created_at)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        timestamps.append(when)
        message_ids.append(int(msg_id))

        day = when.date().isoformat()
        daily_counts[day] += 1

        local = when.astimezone(tz)
        hourly_heatmap[local.weekday()][local.hour] += 1
        hour_key = when.replace(minute=0, second=0, microsecond=0)
        hourly_counts[hour_key] += 1

        if prev_time is not None:
            latencies.append((when - prev_time).total_seconds())
        prev_time = when

        if reply_to_id:
            reply_messages += 1

        content = content or ""
        tokens = [tok.lower() for tok in TOKEN_PATTERN.findall(content) if tok.lower() not in STOP_WORDS]
        token_counts.update(tokens)
        unique_tokens.update(tokens)
        total_words += len(tokens)

        pos = sum(1 for tok in tokens if tok in POSITIVE_WORDS)
        neg = sum(1 for tok in tokens if tok in NEGATIVE_WORDS)
        sentiment = (pos - neg) / max(len(tokens), 1) if tokens else 0.0
        sentiment_scores.append(sentiment)

        for match in URL_PATTERN.findall(content):
            url_counts[match.lower()] += 1

        if (attachments or 0) > 0 or (embeds or 0) > 0:
            messages_with_embeds += 1

        reaction_total = 0
        if reactions:
            try:
                data = json.loads(reactions)
            except Exception:
                data = []
            for item in data or []:
                count = int(item.get("count", 0))
                reaction_total += count
                emoji = item.get("emoji", {})
                if isinstance(emoji, dict):
                    key = f"{emoji.get('name','')}:{emoji.get('id','')}"
                else:
                    key = str(emoji)
                if key:
                    reaction_emojis[key] += count
        reaction_counts.append(reaction_total)

    hourly_series = sorted(hourly_counts.items(), key=lambda kv: kv[0])
    bursts = _rolling_std(hourly_series)
    decay = _half_life(hourly_series)

    silence_hourly = _silence_ratio(hourly_counts, timedelta(hours=1))
    silence_daily = _silence_ratio(
        {datetime.fromisoformat(f"{day}T00:00:00+00:00"): cnt for day, cnt in daily_counts.items()},
        timedelta(days=1),
    )

    median_latency = statistics.median(latencies) if latencies else 0.0

    # Thread stats via recursive query
    thread_rows: List[Tuple[int, int, int, str]] = []
    if message_ids:
        placeholders = ",".join("?" for _ in message_ids)
        with connect() as con:
            cur = con.cursor()
            cur.execute(
                f"""
                WITH RECURSIVE thread(message_id, root_id, depth, created_at) AS (
                    SELECT message_id, message_id, 0, created_at
                      FROM message_archive
                     WHERE guild_id=? AND message_id IN ({placeholders})
                    UNION ALL
                    SELECT m.message_id, thread.root_id, thread.depth + 1, m.created_at
                      FROM message_archive m
                      JOIN thread ON m.reply_to_id = thread.message_id
                     WHERE m.guild_id=?
                )
                SELECT message_id, root_id, depth, created_at FROM thread
                """,
                (guild_id, *message_ids, guild_id),
            )
            thread_rows = cur.fetchall()

    thread_map: Dict[int, Dict[str, object]] = defaultdict(lambda: {
        "start": None,
        "end": None,
        "max_depth": 0,
        "replies": 0,
        "within_10m": 0,
    })
    root_times: Dict[int, datetime] = {}

    for message_id, root_id, depth, created_at in thread_rows:
        when = _parse_iso(created_at)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        info = thread_map[root_id]
        if info["start"] is None or when < info["start"]:
            info["start"] = when
        if info["end"] is None or when > info["end"]:
            info["end"] = when
        info["max_depth"] = max(info["max_depth"], int(depth))
        if depth == 0:
            root_times[root_id] = when
        else:
            info["replies"] += 1
            replies_total += 1
            root_time = root_times.get(root_id)
            if root_time and (when - root_time).total_seconds() <= 600:
                info["within_10m"] += 1

    if thread_map:
        lifespan = [
            (info["end"] - info["start"]).total_seconds()
            for info in thread_map.values()
            if info["replies"] > 0 and info["start"] and info["end"]
        ]
        avg_lifespan = statistics.mean(lifespan) if lifespan else 0.0
        avg_depth = statistics.mean(info["max_depth"] for info in thread_map.values())
        replies_within_10m = sum(info["within_10m"] for info in thread_map.values())
    else:
        avg_lifespan = 0.0
        avg_depth = 0.0
        replies_within_10m = 0

    reply_density = replies_total / max(len(rows), 1)
    attention_ratio = replies_within_10m / max(replies_total, 1)

    reaction_avg = statistics.mean(reaction_counts) if reaction_counts else 0.0
    reaction_std = statistics.pstdev(reaction_counts) if len(reaction_counts) > 1 else 0.0
    reaction_min = min(reaction_counts) if reaction_counts else 0
    reaction_max = max(reaction_counts) if reaction_counts else 0

    word_metrics = {
        "total_words": float(total_words),
        "unique_tokens": float(len(unique_tokens)),
        "lexical_diversity": (len(unique_tokens) / total_words) if total_words else 0.0,
    }

    sentiment = {
        "mean": statistics.mean(sentiment_scores) if sentiment_scores else 0.0,
        "std": statistics.pstdev(sentiment_scores) if len(sentiment_scores) > 1 else 0.0,
    }

    topics = _topic_distribution(token_counts)

    embed_richness = messages_with_embeds / max(len(rows), 1)
    total_urls = sum(url_counts.values())
    url_metrics = {
        "url_messages": float(sum(1 for _ in url_counts)),
        "total_urls": float(total_urls),
        "domain_diversity": float(len(url_counts)),
        "url_frequency": total_urls / max(len(rows), 1),
    }

    hourly_values = [cnt for _, cnt in hourly_series]
    daily_values = list(daily_counts.values())
    skew_hourly, kurt_hourly = _skew_kurtosis(hourly_values)
    gini_daily = _gini(daily_values)
    entropy_hourly = _entropy(hourly_values)
    entropy_daily = _entropy(daily_values)

    inequality = {
        "skewness": float(skew_hourly),
        "kurtosis": float(kurt_hourly),
        "gini": float(gini_daily),
        "entropy_hourly": float(entropy_hourly),
        "entropy_daily": float(entropy_daily),
    }

    first_ts = timestamps[0]
    last_ts = timestamps[-1]
    total_days = (last_ts.date() - first_ts.date()).days + 1
    active_days = len(daily_counts)

    total_reactions = sum(reaction_counts)
    reaction_ratio = total_reactions / max(len(rows), 1)
    participation_rate = active_days / max(total_days, 1)
    avg_msg_per_day = len(rows) / max(active_days, 1)

    derived = {
        "engagement_index": avg_msg_per_day * reaction_ratio * participation_rate,
        "retention": 1.0 if active_days > 1 else 0.0,
        "conversation_depth": float(avg_depth * reply_density),
        "attention_ratio": float(attention_ratio),
        "longevity_index": participation_rate,
    }

    # Global top reactors (guild level)
    totals_received = activity.fetch_metric_totals(guild_id, "reactions_received")
    totals_given = activity.fetch_metric_totals(guild_id, "emoji_react")
    top_received = sorted(totals_received.items(), key=lambda kv: kv[1], reverse=True)[:5]
    top_given = sorted(totals_given.items(), key=lambda kv: kv[1], reverse=True)[:5]

    distribution = {
        "min": float(min(daily_values) if daily_values else 0),
        "max": float(max(daily_values) if daily_values else 0),
        "mean": statistics.mean(daily_values) if daily_values else 0.0,
        "std": statistics.pstdev(daily_values) if len(daily_values) > 1 else 0.0,
    }

    return UserActivityDetails(
        message_count=len(rows),
        active_days=active_days,
        total_days=total_days,
        distribution=distribution,
        hourly_heatmap=hourly_heatmap,
        daily_heatmap=dict(daily_counts),
        bursts=bursts,
        decay=decay,
        silence_ratio_hourly=float(silence_hourly),
        silence_ratio_daily=float(silence_daily),
        response_latency_seconds=float(median_latency),
        thread_lifespan_seconds=float(avg_lifespan),
        reply_depth=float(avg_depth),
        reply_density=float(reply_density),
        attention_ratio=float(attention_ratio),
        reaction_stats={
            "average": float(reaction_avg),
            "std": float(reaction_std),
            "min": float(reaction_min),
            "max": float(reaction_max),
        },
        reaction_diversity=len(reaction_emojis),
        top_reactors_received=[(int(uid), int(cnt)) for uid, cnt in top_received],
        top_reactors_given=[(int(uid), int(cnt)) for uid, cnt in top_given],
        word_metrics=word_metrics,
        sentiment=sentiment,
        topics=topics,
        embed_richness=float(embed_richness),
        url_metrics=url_metrics,
        bot_ratio=0.0,
        inequality=inequality,
        derived=derived,
    )
