from __future__ import annotations

from datetime import datetime, timezone


def now_iso_utc() -> str:
    """Current UTC timestamp as ISO8601 (seconds resolution)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def iso_parts(when_iso: str) -> tuple[str, str, str, int]:
    """Return (day, week_key, month, hour_utc) for an ISO timestamp."""
    dt = datetime.fromisoformat(when_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    year, week, _ = dt.isocalendar()
    return dt.strftime("%Y-%m-%d"), f"{year}-W{int(week):02d}", dt.strftime("%Y-%m"), dt.hour


__all__ = ["iso_parts", "now_iso_utc"]
