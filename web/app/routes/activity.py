# web/app/routes/activity.py
from __future__ import annotations

import datetime as dt
import sqlite3
from typing import Dict, List, Tuple

from fastapi import APIRouter, Depends, HTTPException
from starlette.responses import JSONResponse

from ...yuribot.models.activity_metrics import (
    get_basic_stats,
    get_burst_std_24h,
    get_content_stats,
    get_heatmap,
    get_latency_stats,
)

import os

router = APIRouter(prefix="/api/activity", tags=["activity"])


def _connect() -> sqlite3.Connection:
    db_path = os.environ.get("BOT_DB_PATH") or "/app/data/bot.sqlite3"
    con = sqlite3.connect(db_path, isolation_level=None)
    con.row_factory = sqlite3.Row
    return con


def _date_range(days: int | None) -> Tuple[str, str]:
    if not days or days <= 0:
        # last 30 days by default
        days = 30
    end = dt.datetime.utcnow().date()
    start = (end - dt.timedelta(days=days)).isoformat()
    return start, end.isoformat()


def _day_bounds(days: int) -> tuple[str, str, str, str]:
    now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_day = (now.date() - dt.timedelta(days=days)).isoformat()
    end_day = now.date().isoformat()
    start_hour = (now - dt.timedelta(hours=24 * days)).strftime("%Y-%m-%dT%H")
    end_hour = now.strftime("%Y-%m-%dT%H")
    return start_day, end_day, start_hour, end_hour


@router.get("/{guild_id}/live")
def live_metrics(guild_id: int, days: int = 30):
    if days <= 0:
        raise HTTPException(status_code=400, detail="days must be > 0")

    start_day, end_day, start_hour, end_hour = _day_bounds(days)

    basic = get_basic_stats(guild_id, start_day, end_day)
    heat = get_heatmap(guild_id, start_day, end_day)
    burst = get_burst_std_24h(guild_id, start_day, end_day)
    silence = {"silence_ratio": 1.0}  # wip
    latency = get_latency_stats(guild_id, start_day, end_day)
    content = get_content_stats(guild_id, start_day, end_day)

    # derive silence ratio from hourly path
    from ...yuribot.models.activity_metrics import get_hourly_counts

    hourly = get_hourly_counts(guild_id, start_day, end_day)
    if hourly:
        zeros = sum(1 for _, v in hourly.items() if int(v) == 0)
        silence["silence_ratio"] = float(zeros) / float(len(hourly))

    resp = {
        "range": {
            "start_day": start_day,
            "end_day": end_day,
            "start_hour": start_hour,
            "end_hour": end_hour,
        },
        "basic": {
            "min": basic.min,
            "max": basic.max,
            "mean": basic.mean,
            "std": basic.std,
            "skewness": basic.skewness,
            "kurtosis": basic.kurtosis,
            "gini": basic.gini,
        },
        "temporal": {
            "heatmap_avg_per_hour": heat,
            "burst_std_24h": burst,
            **silence,
        },
        "latency": latency,
        "content": content,
    }
    return JSONResponse(resp)
