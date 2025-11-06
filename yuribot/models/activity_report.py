from __future__ import annotations

import json
import math
import statistics
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np

from zoneinfo import ZoneInfo

from ..db import connect
from . import activity, message_archive

ISO_FORMATS = ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z")

TOKEN_RE = r"[\w']+"

try:
    import re2 as _regex
except Exception:  # pragma: no cover - optional speedup
    import re as _regex

TOKEN_PATTERN = _regex.compile(TOKEN_RE)
URL_PATTERN = _regex.compile(r"https?://([^/\s]+)[^\s]*", _regex.IGNORECASE)
MENTION_PATTERN = _regex.compile(r"<@!?([0-9]{5,})>")

DEFAULT_TIMEZONE = "America/Los_Angeles"
ROLLING_WINDOW_HOURS = 12


POSITIVE_WORDS = {
    "love",
    "great",
    "fantastic",
    "awesome",
    "good",
    "amazing",
    "nice",
    "cool",
    "happy",
    "yay",
    "best",
    "wonderful",
    "thanks",
    "thank",
    "excellent",
    "enjoy",
    "enjoyed",
    "fun",
    "pleased",
}

NEGATIVE_WORDS = {
    "bad",
    "terrible",
    "awful",
    "sad",
    "angry",
    "mad",
    "hate",
    "hated",
    "upset",
    "annoyed",
    "worst",
    "pain",
    "frustrated",
    "boring",
    "bored",
    "tired",
    "ugh",
    "sucks",
}

STOP_WORDS = {
    "the",
    "and",
    "for",
    "that",
    "with",
    "have",
    "this",
    "from",
    "they",
    "what",
    "your",
    "about",
    "would",
    "there",
    "could",
    "should",
    "their",
    "https",
    "http",
    "discord",
    "like",
    "been",
    "because",
    "where",
    "which",
}


def _parse_iso(when: str) -> datetime:
    if not when:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(when.replace("Z", "+00:00"))
    except ValueError:
        for fmt in ISO_FORMATS:
            try:
                return datetime.strptime(when, fmt)
            except ValueError:
                continue
    return datetime.now(timezone.utc)


def _percentile(sorted_values: Sequence[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if pct <= 0:
        return float(sorted_values[0])
    if pct >= 1:
        return float(sorted_values[-1])
    idx = pct * (len(sorted_values) - 1)
    lower = math.floor(idx)
    upper = math.ceil(idx)
    if lower == upper:
        return float(sorted_values[int(idx)])
    frac = idx - lower
    return float(sorted_values[lower] * (1 - frac) + sorted_values[upper] * frac)


def _gini(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(v for v in values if v >= 0)
    if not sorted_vals:
        return 0.0
    cum = 0.0
    total = sum(sorted_vals)
    if total == 0:
        return 0.0
    n = len(sorted_vals)
    for i, v in enumerate(sorted_vals, start=1):
        cum += i * v
    return (2 * cum) / (n * total) - (n + 1) / n


def _entropy(counts: Mapping[str, int]) -> float:
    total = sum(counts.values())
    if total <= 0:
        return 0.0
    ent = 0.0
    for cnt in counts.values():
        if cnt <= 0:
            continue
        p = cnt / total
        ent -= p * math.log2(p)
    return ent


def _skewness(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    n = len(values)
    if n < 3:
        return 0.0
    mean = statistics.mean(values)
    std = statistics.pstdev(values)
    if std == 0:
        return 0.0
    skew = sum(((v - mean) / std) ** 3 for v in values) * (n / ((n - 1) * (n - 2)))
    return float(skew)


def _kurtosis(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    n = len(values)
    if n < 4:
        return 0.0
    mean = statistics.mean(values)
    std = statistics.pstdev(values)
    if std == 0:
        return 0.0
    kurt = sum(((v - mean) / std) ** 4 for v in values) * (n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3))
    kurt -= 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
    return float(kurt)


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(np.median(np.array(values, dtype=float)))


def _rolling_std(series: List[Tuple[datetime, int]], window: int) -> List[Tuple[datetime, int, float]]:
    counts: deque[int] = deque(maxlen=window)
    result: List[Tuple[datetime, int, float]] = []
    for ts, value in series:
        counts.append(value)
        if len(counts) < 2:
            result.append((ts, value, 0.0))
        else:
            result.append((ts, value, float(statistics.pstdev(counts))))
    return result


def _longest_streak(sorted_days: Sequence[datetime]) -> Tuple[int, int]:
    if not sorted_days:
        return (0, 0)
    longest = 1
    current = 1
    longest_week = 1
    current_week = 1
    prev = sorted_days[0]
    prev_week = prev.isocalendar()[1]
    weeks_seen = [prev_week]
    for day in sorted_days[1:]:
        if (day - prev).days == 1:
            current += 1
        else:
            current = 1
        if day.isocalendar()[1] == prev_week:
            current_week += 0
        else:
            current_week = 1
            prev_week = day.isocalendar()[1]
        longest = max(longest, current)
        longest_week = max(longest_week, len(set(weeks_seen + [day.isocalendar()[1]])))
        prev = day
    return (longest, longest_week)


def _topic_distribution(token_counts: Counter[str], topics: int = 5, words_per_topic: int = 5) -> List[List[str]]:
    common = [tok for tok, _ in token_counts.most_common(topics * words_per_topic)]
    buckets: List[List[str]] = []
    for i in range(topics):
        start = i * words_per_topic
        end = start + words_per_topic
        chunk = common[start:end]
        if chunk:
            buckets.append(chunk)
    return buckets


def _half_life(series: List[Tuple[datetime, int]]) -> List[Tuple[datetime, float]]:
    if not series:
        return []
    half_lives: List[Tuple[datetime, float]] = []
    counts = [cnt for _, cnt in series]
    for idx in range(1, len(series) - 1):
        prev_cnt = counts[idx - 1]
        cur_cnt = counts[idx]
        next_cnt = counts[idx + 1]
        if cur_cnt > prev_cnt and cur_cnt >= next_cnt and cur_cnt > 0:
            half_target = cur_cnt / 2
            decay_time: Optional[float] = None
            for j in range(idx + 1, len(series)):
                future_cnt = counts[j]
                if future_cnt <= half_target:
                    delta = series[j][0] - series[idx][0]
                    decay_time = delta.total_seconds() / 3600.0
                    break
            if decay_time is not None:
                half_lives.append((series[idx][0], decay_time))
    return half_lives


def _sentiment_score(tokens: Iterable[str]) -> float:
    score = 0
    for tok in tokens:
        if tok in POSITIVE_WORDS:
            score += 1
        elif tok in NEGATIVE_WORDS:
            score -= 1
    return float(score)


@dataclass
class BurstPoint:
    timestamp: str
    count: int
    rolling_std: float


@dataclass
class DecayPoint:
    timestamp: str
    half_life_hours: float


@dataclass
class LatencyStats:
    overall_median_seconds: float
    per_user_median: Dict[int, float]
    per_channel_median: Dict[int, float]


@dataclass
class ThreadStats:
    average_lifespan_seconds: float
    average_depth: float
    reply_density: float
    replies_within_10m_ratio: float


@dataclass
class ReactionStats:
    average: float
    stddev: float
    minimum: int
    maximum: int
    diversity: int
    total: int


@dataclass
class EngagementStats:
    engagement_index: float
    retention: float
    conversation_depth: float
    attention_ratio: float
    longevity_index: float


@dataclass
class DistributionStats:
    minimum: float
    maximum: float
    mean: float
    stddev: float


@dataclass
class Heatmaps:
    hourly: List[List[int]]  # [day_of_week][hour]
    daily: Dict[str, int]


@dataclass
class TextStats:
    total_words: int
    unique_tokens: int
    lexical_diversity: float
    sentiment_mean: float
    sentiment_std: float
    topic_clusters: List[List[str]]


@dataclass
class LinkStats:
    url_messages: int
    total_urls: int
    domain_diversity: int
    embed_richness: float


@dataclass
class InequalityStats:
    skewness: float
    kurtosis: float
    gini: float
    hourly_entropy: float
    daily_entropy: float


@dataclass
class UserMetricSnapshot:
    messages: int = 0
    words: int = 0
    token_set: set = None  # type: ignore[assignment]
    mentions_sent: int = 0
    mentions_received: int = 0
    replies_made: int = 0
    replies_received: int = 0
    reactions_received: int = 0
    reactions_given: int = 0
    latency_seconds: List[float] = None  # type: ignore[assignment]
    first_message: Optional[str] = None
    last_message: Optional[str] = None
    active_days: set = None  # type: ignore[assignment]
    active_weeks: set = None  # type: ignore[assignment]
    voice_minutes: int = 0
    stream_minutes: int = 0
    activity_joins: int = 0
    is_bot: bool = False

    def __post_init__(self) -> None:
        if self.latency_seconds is None:
            self.latency_seconds = []
        if self.active_days is None:
            self.active_days = set()
        if self.active_weeks is None:
            self.active_weeks = set()
        if self.token_set is None:
            self.token_set = set()

    def record_message(self, when: datetime) -> None:
        day = when.date()
        iso_week = (when.isocalendar().year, when.isocalendar().week)
        self.active_days.add(day)
        self.active_weeks.add(iso_week)
        if self.first_message is None or when.isoformat() < self.first_message:
            self.first_message = when.isoformat()
        if self.last_message is None or when.isoformat() > self.last_message:
            self.last_message = when.isoformat()

    @property
    def median_latency(self) -> float:
        if not self.latency_seconds:
            return 0.0
        return float(np.median(np.array(self.latency_seconds, dtype=float)))

    @property
    def unique_tokens(self) -> int:
        return len(self.token_set)

    @property
    def longest_day_streak(self) -> int:
        if not self.active_days:
            return 0
        sorted_days = sorted(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) for day in self.active_days)
        longest = 1
        current = 1
        for prev, nxt in zip(sorted_days, islice(sorted_days, 1, None)):
            if (nxt - prev).days == 1:
                current += 1
            else:
                longest = max(longest, current)
                current = 1
        longest = max(longest, current)
        return longest

    @property
    def longest_week_streak(self) -> int:
        if not self.active_weeks:
            return 0
        weeks = sorted(self.active_weeks)
        longest = 1
        current = 1
        for prev, nxt in zip(weeks, weeks[1:]):
            prev_year, prev_week = prev
            nxt_year, nxt_week = nxt
            if (nxt_year == prev_year and nxt_week == prev_week + 1) or (nxt_year == prev_year + 1 and prev_week >= 52 and nxt_week == 1):
                current += 1
            else:
                longest = max(longest, current)
                current = 1
        longest = max(longest, current)
        return longest


@dataclass
class ActivityReport:
    generated_at: str
    guild_id: int
    timezone: str
    summary: DistributionStats
    per_day_summary: DistributionStats
    heatmaps: Heatmaps
    bursts: List[BurstPoint]
    decay: List[DecayPoint]
    silence_ratio_hourly: float
    silence_ratio_daily: float
    latency: LatencyStats
    thread: ThreadStats
    reactions: ReactionStats
    text: TextStats
    links: LinkStats
    inequality: InequalityStats
    health: EngagementStats
    top_reactors_received: List[Tuple[int, int]]
    top_reactors_given: List[Tuple[int, int]]
    user_metrics: Dict[int, Dict[str, float]]

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        return payload


def _build_distribution(values: Sequence[int]) -> DistributionStats:
    if values:
        mean = statistics.mean(values)
        std = statistics.pstdev(values)
        min_v = min(values)
        max_v = max(values)
    else:
        mean = std = min_v = max_v = 0.0
    return DistributionStats(minimum=float(min_v), maximum=float(max_v), mean=float(mean), stddev=float(std))


def _daily_counts(timestamps: List[datetime]) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for ts in timestamps:
        key = ts.date().isoformat()
        counts[key] += 1
    return counts


def _heatmap(timestamps: List[datetime], tz: ZoneInfo) -> Heatmaps:
    hourly: List[List[int]] = [[0 for _ in range(24)] for _ in range(7)]
    daily: Dict[str, int] = defaultdict(int)
    for ts in timestamps:
        local = ts.astimezone(tz)
        hourly[local.weekday()][local.hour] += 1
        day_key = local.date().isoformat()
        daily[day_key] += 1
    return Heatmaps(hourly=hourly, daily=dict(daily))


def _silence_ratio(counts: Mapping[datetime, int], bucket: timedelta) -> float:
    if not counts:
        return 0.0
    start = min(counts.keys())
    end = max(counts.keys())
    total_slots = int(((end - start) / bucket)) + 1
    zero_slots = 0
    current = start
    while current <= end:
        if counts.get(current, 0) == 0:
            zero_slots += 1
        current += bucket
    if total_slots <= 0:
        return 0.0
    return zero_slots / total_slots


def _compute_latency(per_user_times: Mapping[int, List[datetime]], per_channel_times: Mapping[int, List[datetime]]) -> LatencyStats:
    per_user_median: Dict[int, float] = {}
    per_channel_median: Dict[int, float] = {}
    overall_deltas: List[float] = []

    for uid, times in per_user_times.items():
        times_sorted = sorted(times)
        if len(times_sorted) >= 2:
            deltas = [
                (b - a).total_seconds()
                for a, b in zip(times_sorted, times_sorted[1:])
                if b > a
            ]
            if deltas:
                per_user_median[uid] = _median(deltas)
                overall_deltas.extend(deltas)
    for cid, times in per_channel_times.items():
        times_sorted = sorted(times)
        if len(times_sorted) >= 2:
            deltas = [
                (b - a).total_seconds()
                for a, b in zip(times_sorted, times_sorted[1:])
                if b > a
            ]
            if deltas:
                per_channel_median[cid] = _median(deltas)
                overall_deltas.extend(deltas)

    overall = _median(overall_deltas)
    return LatencyStats(
        overall_median_seconds=overall,
        per_user_median=per_user_median,
        per_channel_median=per_channel_median,
    )


def _build_thread_stats(
    message_meta: Mapping[int, Tuple[datetime, Optional[int], int, int]],
    replies_by_parent: Mapping[int, List[int]],
) -> ThreadStats:
    if not replies_by_parent:
        return ThreadStats(0.0, 0.0, 0.0, 0.0)

    lifespans: List[float] = []
    depths: List[int] = []
    reply_count = 0
    quick_replies = 0

    def depth_for(msg_id: int, current: int = 0) -> int:
        children = replies_by_parent.get(msg_id, [])
        if not children:
            return current
        return max(depth_for(child, current + 1) for child in children)

    for parent_id, replies in replies_by_parent.items():
        parent_meta = message_meta.get(parent_id)
        if not parent_meta:
            continue
        parent_time = parent_meta[0]
        reply_times = [message_meta[child][0] for child in replies if child in message_meta]
        if reply_times:
            lifespan = max(reply_times) - parent_time
            lifespans.append(lifespan.total_seconds())
        depths.append(depth_for(parent_id))
        reply_count += len(reply_times)
        quick_replies += sum(1 for rt in reply_times if (rt - parent_time).total_seconds() <= 600)

    total_messages = len(message_meta)
    reply_density = reply_count / total_messages if total_messages else 0.0
    avg_lifespan = statistics.mean(lifespans) if lifespans else 0.0
    avg_depth = statistics.mean(depths) if depths else 0.0
    quick_ratio = quick_replies / reply_count if reply_count else 0.0
    return ThreadStats(
        average_lifespan_seconds=float(avg_lifespan),
        average_depth=float(avg_depth),
        reply_density=float(reply_density),
        replies_within_10m_ratio=float(quick_ratio),
    )


def _reaction_stats(reaction_counts: List[int], unique_emojis: set[str]) -> ReactionStats:
    if reaction_counts:
        avg = statistics.mean(reaction_counts)
        std = statistics.pstdev(reaction_counts) if len(reaction_counts) > 1 else 0.0
        minimum = min(reaction_counts)
        maximum = max(reaction_counts)
        total = sum(reaction_counts)
    else:
        avg = std = 0.0
        minimum = maximum = total = 0
    return ReactionStats(
        average=float(avg),
        stddev=float(std),
        minimum=int(minimum),
        maximum=int(maximum),
        diversity=len(unique_emojis),
        total=int(total),
    )


def _text_stats(
    total_words: int,
    unique_tokens: int,
    token_counts: Counter[str],
    sentiment_scores: List[float],
) -> TextStats:
    lexical_diversity = (unique_tokens / total_words) if total_words else 0.0
    if sentiment_scores:
        sentiment_mean = statistics.mean(sentiment_scores)
        sentiment_std = statistics.pstdev(sentiment_scores) if len(sentiment_scores) > 1 else 0.0
    else:
        sentiment_mean = sentiment_std = 0.0
    topics = _topic_distribution(token_counts)
    return TextStats(
        total_words=int(total_words),
        unique_tokens=int(unique_tokens),
        lexical_diversity=float(lexical_diversity),
        sentiment_mean=float(sentiment_mean),
        sentiment_std=float(sentiment_std),
        topic_clusters=topics,
    )


def _link_stats(url_counts: Counter[str], url_messages: int, embed_messages: int, total_messages: int) -> LinkStats:
    total_urls = sum(url_counts.values())
    diversity = len([domain for domain, count in url_counts.items() if domain])
    embed_richness = (embed_messages / total_messages) if total_messages else 0.0
    return LinkStats(
        url_messages=int(url_messages),
        total_urls=int(total_urls),
        domain_diversity=int(diversity),
        embed_richness=float(embed_richness),
    )


def _inequality_stats(per_user_counts: Mapping[int, int], hourly_counts: Mapping[str, int], daily_counts: Mapping[str, int]) -> InequalityStats:
    values = list(per_user_counts.values())
    hourly_entropy = _entropy(hourly_counts)
    daily_entropy = _entropy(daily_counts)
    return InequalityStats(
        skewness=_skewness(values),
        kurtosis=_kurtosis(values),
        gini=_gini(values),
        hourly_entropy=float(hourly_entropy),
        daily_entropy=float(daily_entropy),
    )


def _engagement_stats(
    total_messages: int,
    unique_users: int,
    total_reactions: int,
    reply_density: float,
    avg_reply_depth: float,
    replies_within_10m_ratio: float,
    active_days: int,
    total_days: int,
    returning_users: int,
    member_count: Optional[int],
) -> EngagementStats:
    avg_messages_per_user = (total_messages / unique_users) if unique_users else 0.0
    reaction_ratio = (total_reactions / total_messages) if total_messages else 0.0
    if member_count and member_count > 0:
        participation_rate = unique_users / member_count
    else:
        participation_rate = 1.0
    engagement_index = avg_messages_per_user * reaction_ratio * participation_rate
    retention = (returning_users / unique_users) if unique_users else 0.0
    conversation_depth = avg_reply_depth * reply_density
    attention_ratio = replies_within_10m_ratio
    longevity_index = (active_days / total_days) if total_days else 0.0
    return EngagementStats(
        engagement_index=float(engagement_index),
        retention=float(retention),
        conversation_depth=float(conversation_depth),
        attention_ratio=float(attention_ratio),
        longevity_index=float(longevity_index),
    )


def _fetch_voice_totals(guild_id: int) -> Tuple[Dict[int, int], Dict[int, int]]:
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT user_id, metric, count
            FROM member_metrics_total
            WHERE guild_id=? AND metric IN ('voice_minutes', 'voice_stream_minutes')
            """,
            (guild_id,),
        ).fetchall()
    voice: Dict[int, int] = defaultdict(int)
    stream: Dict[int, int] = defaultdict(int)
    for uid, metric, count in rows:
        if metric == "voice_minutes":
            voice[int(uid)] += int(count)
        elif metric == "voice_stream_minutes":
            stream[int(uid)] += int(count)
    return voice, stream


def _fetch_activity_joins(guild_id: int) -> Dict[int, int]:
    with connect() as con:
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT user_id, SUM(launches)
            FROM member_activity_apps_daily
            WHERE guild_id=?
            GROUP BY user_id
            """,
            (guild_id,),
        ).fetchall()
    return {int(uid): int(total or 0) for uid, total in rows}


def _fetch_reaction_givers(guild_id: int) -> Dict[int, int]:
    totals = activity.fetch_metric_totals(guild_id, "emoji_react")
    return totals


def _stat_sigma(values: List[float]) -> Tuple[float, float]:
    if not values:
        return (0.0, 1.0)
    sorted_vals = sorted(values)
    q25 = _percentile(sorted_vals, 0.25)
    q75 = _percentile(sorted_vals, 0.75)
    mu = (q25 + q75) / 2.0
    sigma = (q75 - q25) / 2.0
    if sigma <= 0:
        sigma = 1.0
    return (mu, sigma)


def _normalize(value: float, mu: float, sigma: float) -> float:
    if sigma == 0:
        return 0.0
    return (value - mu) / sigma


def _stat_value(z: float, base: float = 5.0) -> int:
    score = base + (z * 2.0)
    if score < 1:
        score = 1
    return int(round(score))


def _build_user_metrics_snapshot(
    guild_id: int,
    timezone_name: str,
    member_count: Optional[int],
    bot_user_ids: Optional[Iterable[int]] = None,
) -> Tuple[ActivityReport, Dict[int, UserMetricSnapshot]]:
    tz = ZoneInfo(timezone_name)
    per_user: Dict[int, UserMetricSnapshot] = defaultdict(UserMetricSnapshot)
    per_channel_times: Dict[int, List[datetime]] = defaultdict(list)
    per_user_times: Dict[int, List[datetime]] = defaultdict(list)
    timestamps: List[datetime] = []
    hourly_counts: Dict[datetime, int] = defaultdict(int)
    hourly_counts_label: Dict[str, int] = defaultdict(int)
    daily_counts = defaultdict(int)
    reaction_counts: List[int] = []
    reaction_emojis: set[str] = set()
    token_counts: Counter[str] = Counter()
    sentiment_scores: List[float] = []
    url_counts: Counter[str] = Counter()
    url_messages = 0
    embed_messages = 0
    message_meta: Dict[int, Tuple[datetime, Optional[int], int, int]] = {}
    replies_by_parent: Dict[int, List[int]] = defaultdict(list)
    reply_depth_map: Dict[int, int] = {}
    returning_users = 0

    bot_ids = set(bot_user_ids or [])

    voice_totals, stream_totals = _fetch_voice_totals(guild_id)
    activity_joins = _fetch_activity_joins(guild_id)
    reaction_givers = _fetch_reaction_givers(guild_id)

    for uid, minutes in voice_totals.items():
        per_user[uid].voice_minutes = minutes
    for uid, minutes in stream_totals.items():
        per_user[uid].stream_minutes = minutes
    for uid, joins in activity_joins.items():
        per_user[uid].activity_joins = joins
    for uid, count in reaction_givers.items():
        per_user[uid].reactions_given = count

    last_message_time_per_user: Dict[int, datetime] = {}

    for row in message_archive.iter_guild_messages(guild_id):
        when = _parse_iso(row.created_at)
        timestamps.append(when)
        per_channel_times[int(row.channel_id)].append(when)
        per_user_times[int(row.author_id)].append(when)
        hourly_key = when.replace(minute=0, second=0, microsecond=0)
        hourly_counts[hourly_key] += 1
        hourly_counts_label[hourly_key.isoformat()] += 1
        daily_counts[when.date().isoformat()] += 1
        msg_id = int(row.message_id)
        parent_id = int(row.reply_to_id) if row.reply_to_id else None
        message_meta[msg_id] = (when, parent_id, int(row.channel_id), uid)
        if parent_id:
            replies_by_parent[parent_id].append(msg_id)

        uid = int(row.author_id)
        snapshot = per_user[uid]
        snapshot.is_bot = uid in bot_ids
        snapshot.messages += 1
        snapshot.record_message(when)

        last_time = last_message_time_per_user.get(uid)
        if last_time is not None and when > last_time:
            snapshot.latency_seconds.append((when - last_time).total_seconds())
        last_message_time_per_user[uid] = when

        content = row.content or ""
        tokens = [tok.lower() for tok in TOKEN_PATTERN.findall(content)]
        words = len(tokens)
        snapshot.words += words
        snapshot.token_set.update(tokens)
        token_counts.update(tok for tok in tokens if tok not in STOP_WORDS)
        sentiment_scores.append(_sentiment_score(tokens))

        if content:
            for match in MENTION_PATTERN.findall(content):
                try:
                    target = int(match)
                except ValueError:
                    continue
                snapshot.mentions_sent += 1
                per_user[target].mentions_received += 1

        if parent_id:
            snapshot.replies_made += 1
            parent_meta = message_meta.get(parent_id)
            if parent_meta:
                parent_author = parent_meta[3]
                per_user[parent_author].replies_received += 1

        reaction_total = 0
        if row.reactions:
            try:
                data = json.loads(row.reactions)
            except Exception:
                data = []
            for entry in data or []:
                count = int(entry.get("count", 0) or 0)
                reaction_total += count
                emoji_repr = entry.get("emoji") or entry.get("emoji_name") or ""
                if emoji_repr:
                    reaction_emojis.add(str(emoji_repr))
        if reaction_total:
            snapshot.reactions_received += reaction_total
        reaction_counts.append(reaction_total)

        if row.attachments or row.embeds:
            embed_messages += 1

        urls_found = URL_PATTERN.findall(content)
        if urls_found:
            url_messages += 1
            for domain in urls_found:
                url_counts[domain.lower()] += 1

    for uid, snapshot in per_user.items():
        if snapshot.latency_seconds and len(snapshot.latency_seconds) > 1:
            pass

    per_user_counts = {uid: snap.messages for uid, snap in per_user.items() if not snap.is_bot and snap.messages > 0}
    per_day_counts = list(daily_counts.values())
    summary = _build_distribution(list(per_user_counts.values()))
    per_day_summary = _build_distribution(per_day_counts)

    tz_obj = ZoneInfo(timezone_name)
    heatmaps = _heatmap(timestamps, tz_obj)

    hourly_series = sorted(hourly_counts.items(), key=lambda item: item[0])
    hourly_series_label = [(ts.isoformat(), count) for ts, count in hourly_series]
    bursts = [
        BurstPoint(timestamp=ts.isoformat(), count=count, rolling_std=std)
        for ts, count, std in _rolling_std(hourly_series, ROLLING_WINDOW_HOURS)
    ]
    decay_points = [
        DecayPoint(timestamp=ts.isoformat(), half_life_hours=hl)
        for ts, hl in _half_life(hourly_series)
    ]

    silence_hourly = _silence_ratio(hourly_counts, timedelta(hours=1))
    silence_daily = _silence_ratio({datetime.fromisoformat(day + "T00:00:00+00:00"): cnt for day, cnt in daily_counts.items()}, timedelta(days=1))

    latency_stats = _compute_latency(per_user_times, per_channel_times)
    thread_stats = _build_thread_stats(message_meta, replies_by_parent)
    reaction_stats = _reaction_stats(reaction_counts, reaction_emojis)
    if per_user:
        combined_tokens: set[str] = set()
        for snap in per_user.values():
            combined_tokens.update(snap.token_set)
        unique_token_count = len(combined_tokens)
    else:
        unique_token_count = 0
    text_stats = _text_stats(sum(snap.words for snap in per_user.values()), unique_token_count, token_counts, sentiment_scores)
    link_stats = _link_stats(url_counts, url_messages, embed_messages, len(timestamps))
    inequality_stats = _inequality_stats(per_user_counts, dict(hourly_series_label), daily_counts)

    active_snapshots = [snap for snap in per_user.values() if not snap.is_bot and snap.messages > 0]
    unique_users = len(active_snapshots)
    total_reactions = reaction_stats.total
    active_days = len(daily_counts)
    total_days = 0
    if timestamps:
        total_days = (max(timestamps).date() - min(timestamps).date()).days + 1

    returning_users = sum(1 for snap in active_snapshots if len(snap.active_days) > 1)

    health_stats = _engagement_stats(
        total_messages=len(timestamps),
        unique_users=unique_users,
        total_reactions=total_reactions,
        reply_density=thread_stats.reply_density,
        avg_reply_depth=thread_stats.average_depth,
        replies_within_10m_ratio=thread_stats.replies_within_10m_ratio,
        active_days=active_days,
        total_days=total_days,
        returning_users=returning_users,
        member_count=member_count,
    )

    reactions_received_sorted = sorted(
        ((uid, snap.reactions_received) for uid, snap in per_user.items() if snap.reactions_received > 0),
        key=lambda item: item[1],
        reverse=True,
    )[:20]
    reactions_given_sorted = sorted(
        ((uid, snap.reactions_given) for uid, snap in per_user.items() if snap.reactions_given > 0),
        key=lambda item: item[1],
        reverse=True,
    )[:20]

    report = ActivityReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        guild_id=guild_id,
        timezone=timezone_name,
        summary=summary,
        per_day_summary=per_day_summary,
        heatmaps=heatmaps,
        bursts=bursts,
        decay=decay_points,
        silence_ratio_hourly=float(silence_hourly),
        silence_ratio_daily=float(silence_daily),
        latency=latency_stats,
        thread=thread_stats,
        reactions=reaction_stats,
        text=text_stats,
        links=link_stats,
        inequality=inequality_stats,
        health=health_stats,
        top_reactors_received=reactions_received_sorted,
        top_reactors_given=reactions_given_sorted,
        user_metrics={},
    )

    return report, per_user


def generate_activity_report(
    guild_id: int,
    *,
    timezone_name: str = DEFAULT_TIMEZONE,
    member_count: Optional[int] = None,
    bot_user_ids: Optional[Iterable[int]] = None,
) -> ActivityReport:
    report, user_snapshots = _build_user_metrics_snapshot(guild_id, timezone_name, member_count, bot_user_ids)

    user_payload: Dict[int, Dict[str, float]] = {}
    for uid, snap in user_snapshots.items():
        user_payload[uid] = {
            "messages": float(snap.messages),
            "words": float(snap.words),
            "unique_tokens": float(snap.unique_tokens),
            "mentions_sent": float(snap.mentions_sent),
            "mentions_received": float(snap.mentions_received),
            "replies_made": float(snap.replies_made),
            "replies_received": float(snap.replies_received),
            "reactions_received": float(snap.reactions_received),
            "reactions_given": float(snap.reactions_given),
            "median_latency": float(snap.median_latency),
            "voice_minutes": float(snap.voice_minutes),
            "stream_minutes": float(snap.stream_minutes),
            "activity_joins": float(snap.activity_joins),
            "longest_day_streak": float(snap.longest_day_streak),
            "longest_week_streak": float(snap.longest_week_streak),
            "active_days": float(len(snap.active_days)),
            "active_weeks": float(len(snap.active_weeks)),
            "is_bot": float(1 if snap.is_bot else 0),
        }

    report.user_metrics = user_payload
    return report


def compute_rpg_stats_from_report(report: ActivityReport) -> Dict[int, Dict[str, int]]:
    per_user = report.user_metrics
    if not per_user:
        return {}

    str_values: Dict[int, float] = {}
    int_values: Dict[int, float] = {}
    dex_values: Dict[int, float] = {}
    wis_values: Dict[int, float] = {}
    cha_values: Dict[int, float] = {}
    vit_values: Dict[int, float] = {}

    for uid, metrics in per_user.items():
        if metrics.get("is_bot"):
            continue
        messages = metrics.get("messages", 0.0)
        words = metrics.get("words", 0.0)
        unique_tokens = metrics.get("unique_tokens", 0.0)
        mentions_sent = metrics.get("mentions_sent", 0.0)
        mentions_received = metrics.get("mentions_received", 0.0)
        replies_received = metrics.get("replies_received", 0.0)
        reactions_received = metrics.get("reactions_received", 0.0)
        reactions_given = metrics.get("reactions_given", 0.0)
        latency = metrics.get("median_latency", 0.0)
        voice_minutes = metrics.get("voice_minutes", 0.0)
        stream_minutes = metrics.get("stream_minutes", 0.0)
        activity_joins = metrics.get("activity_joins", 0.0)
        longest_day_streak = metrics.get("longest_day_streak", 0.0)
        longest_week_streak = metrics.get("longest_week_streak", 0.0)

        str_values[int(uid)] = messages
        lexical_diversity = (unique_tokens / words) if words else 0.0
        int_values[int(uid)] = words + (unique_tokens * 5.0) + (lexical_diversity * 100.0)

        latency_score = 0.0
        if latency > 0:
            latency_score = 1000.0 / (1.0 + latency)
        dex_values[int(uid)] = reactions_given + latency_score

        wis_values[int(uid)] = (
            mentions_sent + (activity_joins * 5.0) + longest_day_streak + (longest_week_streak * 7.0)
        )

        cha_values[int(uid)] = mentions_received + replies_received + reactions_received
        vit_values[int(uid)] = voice_minutes + (stream_minutes * 2.0)

    eligible_users = [uid for uid, metrics in per_user.items() if not metrics.get("is_bot")]

    def values_with_zeros(values: Dict[int, float]) -> List[float]:
        all_values = list(values.values())
        if len(all_values) < len(eligible_users):
            zeros_to_add = len(eligible_users) - len(all_values)
            all_values.extend([0.0] * zeros_to_add)
        return all_values

    mu_str, sigma_str = _stat_sigma(values_with_zeros(str_values))
    mu_int, sigma_int = _stat_sigma(values_with_zeros(int_values))
    mu_dex, sigma_dex = _stat_sigma(values_with_zeros(dex_values))
    mu_wis, sigma_wis = _stat_sigma(values_with_zeros(wis_values))
    mu_cha, sigma_cha = _stat_sigma(values_with_zeros(cha_values))
    mu_vit, sigma_vit = _stat_sigma(values_with_zeros(vit_values))

    result: Dict[int, Dict[str, int]] = {}
    for uid in eligible_users:
        z_str = _normalize(str_values.get(uid, 0.0), mu_str, sigma_str)
        z_int = _normalize(int_values.get(uid, 0.0), mu_int, sigma_int)
        z_dex = _normalize(dex_values.get(uid, 0.0), mu_dex, sigma_dex)
        z_wis = _normalize(wis_values.get(uid, 0.0), mu_wis, sigma_wis)
        z_cha = _normalize(cha_values.get(uid, 0.0), mu_cha, sigma_cha)
        z_vit = _normalize(vit_values.get(uid, 0.0), mu_vit, sigma_vit)
        result[uid] = {
            "str": _stat_value(z_str),
            "int": _stat_value(z_int),
            "dex": _stat_value(z_dex),
            "wis": _stat_value(z_wis),
            "cha": _stat_value(z_cha),
            "vit": _stat_value(z_vit),
        }
    return result


__all__ = [
    "DEFAULT_TIMEZONE",
    "ActivityReport",
    "BurstPoint",
    "DecayPoint",
    "LatencyStats",
    "ThreadStats",
    "ReactionStats",
    "EngagementStats",
    "DistributionStats",
    "Heatmaps",
    "TextStats",
    "LinkStats",
    "InequalityStats",
    "UserMetricSnapshot",
    "generate_activity_report",
    "compute_rpg_stats_from_report",
]
