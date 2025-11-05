from __future__ import annotations

import re
from datetime import timedelta
from typing import Iterable

import discord

from ..strings import S

URL_RE = re.compile(r"(https?://\S+)", re.IGNORECASE)


def first_url(text: str) -> str:
    match = URL_RE.search(text or "")
    return match.group(1) if match else ""


def normalized_club(club: str) -> str:
    return (club or "").strip() or "manga"
