from __future__ import annotations

import platform
import time
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import discord

try:
    import psutil  # optional dependency
except Exception:  # pragma: no cover - psutil may be missing
    psutil = None  # type: ignore


def ensure_start_metadata(bot: discord.Client) -> None:
    if not hasattr(bot, "start_time"):
        bot.start_time = time.monotonic()
    if not hasattr(bot, "start_datetime"):
        bot.start_datetime = datetime.now(timezone.utc)


def uptime_info(bot: discord.Client) -> Tuple[float, datetime]:
    ensure_start_metadata(bot)
    started: float = getattr(bot, "start_time")
    started_dt: datetime = getattr(bot, "start_datetime")
    uptime_seconds = time.monotonic() - float(started)
    return uptime_seconds, started_dt


def human_delta(seconds: float) -> str:
    s = int(seconds)
    days, s = divmod(s, 86400)
    hrs, s = divmod(s, 3600)
    mins, s = divmod(s, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    parts.append(f"{hrs:02d}h")
    parts.append(f"{mins:02d}m")
    parts.append(f"{s:02d}s")
    return " ".join(parts)


def _member_counts(bot: discord.Client) -> Tuple[int, int, int]:
    members_cached = 0
    humans = 0
    bots = 0
    for guild in getattr(bot, "guilds", []):
        members_cached += guild.member_count or 0
        if hasattr(guild, "members"):
            for member in guild.members:
                if member.bot:
                    bots += 1
                else:
                    humans += 1
    return members_cached, humans, bots


def _process_metrics() -> Tuple[str, str]:
    if not psutil:
        return "n/a", "n/a"
    try:
        proc = psutil.Process()
        with proc.oneshot():
            rss = proc.memory_info().rss
            cpu = proc.cpu_percent(interval=0.1)
        mem_txt = f"{rss / (1024 ** 2):.1f} MiB"
        cpu_txt = f"{cpu:.1f}%"
        return mem_txt, cpu_txt
    except Exception:  # pragma: no cover - defensive
        return "n/a", "n/a"


def gather_botinfo(bot: discord.Client) -> Dict[str, Any]:
    guilds = len(getattr(bot, "guilds", []))
    members_cached, humans, bots = _member_counts(bot)
    mem_txt, cpu_txt = _process_metrics()

    shard_count = getattr(bot, "shard_count", None)
    shard_id = getattr(bot, "shard_id", None)
    if isinstance(shard_count, int) and shard_count > 1 and isinstance(shard_id, int):
        shard_label = f"{shard_id}/{shard_count}"
    else:
        shard_label = "—"

    commands_total = 0
    tree = getattr(bot, "tree", None)
    if tree and hasattr(tree, "get_commands"):
        try:
            commands_total = len(tree.get_commands())
        except Exception:
            commands_total = 0

    return {
        "guilds": guilds,
        "members_cached": members_cached,
        "humans": humans,
        "bots": bots,
        "commands_total": commands_total,
        "shard_id": shard_id if isinstance(shard_id, int) else None,
        "shard_count": shard_count if isinstance(shard_count, int) else None,
        "gw_latency_ms": (getattr(bot, "latency", 0.0) or 0.0) * 1000.0,
        "memory": mem_txt,
        "cpu": cpu_txt,
        "py_version": platform.python_version(),
        "discord_version": getattr(discord, "__version__", "unknown"),
    }
