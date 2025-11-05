from __future__ import annotations

from typing import Iterable

from ..utils.music import MusicTrack, format_duration


def format_track(track: MusicTrack) -> str:
    return f"[{track.title}]({track.webpage_url}) - {format_duration(track.duration)}"


def format_queue(queue: Iterable[MusicTrack]) -> str:
    lines = [f"{idx + 1}. {format_track(track)}" for idx, track in enumerate(queue)]
    return "\n".join(lines) if lines else "Queue is empty."
