from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import discord

from .storage import resolve_data_dir

log = logging.getLogger(__name__)


def pkg_root() -> Path:
    return Path(__file__).resolve().parents[1]


def app_root() -> Path:
    return Path(__file__).resolve().parents[2]


def ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def basename_only(filename: str) -> str:
    name = (filename or "").strip()
    return Path(name).name


class TTLCache:
    def __init__(self, ttl_seconds: float):
        self.ttl = ttl_seconds
        self._store: Dict[Tuple, Tuple[float, object]] = {}

    def get(self, key):
        rec = self._store.get(key)
        now = time.monotonic()
        if rec and (now - rec[0] < self.ttl):
            return rec[1]
        return None

    def set(self, key, value):
        self._store[key] = (time.monotonic(), value)


cfg_cache = TTLCache(ttl_seconds=60.0)
img_cache = TTLCache(ttl_seconds=300.0)


def resolve_welcome_image(filename: str) -> Optional[Path]:
    fname = basename_only(filename)
    cached = img_cache.get(fname)
    if cached is not None:
        return cached

    candidates = [
        app_root() / fname,
        app_root() / "assets" / fname,
        pkg_root() / fname,
        pkg_root() / "assets" / fname,
        Path.cwd() / fname,
        Path.cwd() / "assets" / fname,
    ]
    found: Optional[Path] = None
    for path in candidates:
        try:
            if path.exists():
                log.debug("welcome.image.resolve", extra={"path": str(path)})
                found = path
                break
        except Exception as exc:
            log.debug(
                "welcome.image.exists_check_failed",
                extra={"path": str(path), "error": str(exc)},
            )
    if not found:
        log.warning(
            "welcome.image.not_found",
            extra={"filename": fname, "tried": [str(p) for p in candidates]},
        )
    img_cache.set(fname, found)
    return found


def has_perms(me: discord.Member, ch: discord.TextChannel) -> Tuple[bool, list[str]]:
    perms = ch.permissions_for(me)
    missing = []
    if not perms.send_messages:
        missing.append("Send Messages")
    if not perms.embed_links:
        missing.append("Embed Links")
    if not perms.attach_files:
        missing.append("Attach Files")
    return (len(missing) == 0, missing)