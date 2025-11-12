# web/app/routes/activity.py
from __future__ import annotations

import datetime as dt
from fastapi import APIRouter, HTTPException
from starlette.responses import JSONResponse

# Use absolute import from project root: 'yuribot' is a top-level package
from yuribot.models.activity_metrics import (
    get_basic_stats,
    get_burst_std_24h,
    get_content_stats,
    get_heatmap,
    get_latency_stats,
    get_hourly_counts,
)

router = APIRouter(prefix="/api/activity", tags=["activity"])


def _day_bounds(days: int) -> tuple[str, str, str, str]:
    # days back in UTC, to day/hour ranges
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
    burst = get_burst_std_24h(guild_id, start_hour, end_hour)
    latency = get_latency_stats(guild_id, start_day, end_day)

    hourly = get_hourly_counts(guild_id, start_hour, end_hour)
    silence_ratio = sum(1 for _, v in (hourly or {}).items() if int(v) == 0) / max(
        len(hourly or {}), 1
    )

    content = get_content_stats(guild_id, start_day, end_day)

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
            "silence_ratio": float(silence_ratio),
        },
        "latency": latency,
        "content": content,
    }
    return JSONResponse(resp)
