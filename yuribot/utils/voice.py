from __future__ import annotations

import re
import logging
from datetime import datetime, timezone
from typing import Optional, Tuple, Literal

import discord

log = logging.getLogger(__name__)

# Titles from yuribot/cogs/botlog.py
_JOIN_TITLES = {"voice join", "voice_join", "join", "voice connected"}
_LEAVE_TITLES = {"voice leave", "voice_leave", "leave", "voice disconnected"}
_MOVE_TITLES = {"voice move"}

# Regex helpers (from your admin.py)
_PARENS_ANY = re.compile(r"\(([^()]*)\)\s*$", re.S | re.M)
_DIGITS_10P = re.compile(r"(\d{10,})")


def _extract_last_id(text: str | None) -> Optional[int]:
    """Get the last snowflake-like number from the LAST (...) group."""
    if not text:
        return None
    m = _PARENS_ANY.search(text)
    if m:
        inside = re.sub(r"\D+", "", m.group(1))  # keep only digits
        if len(inside) >= 10:
            try:
                return int(inside)
            except ValueError:
                pass
    # Fallback: last 10+ digit run anywhere
    m2 = None
    for m2 in _DIGITS_10P.finditer(text):
        pass
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            return None
    return None


# This is the structured data we'll get from a log message
ParsedVoiceEvent = Tuple[
    datetime,  # event_timestamp (UTC)
    int,  # message_id (the log message's ID)
    int,  # user_id
    Literal["join", "leave", "move"],  # kind
    Optional[int],  # from_channel_id
    Optional[int],  # to_channel_id
]


def parse_voice_log_embed(msg: discord.Message) -> Optional[ParsedVoiceEvent]:
    """
    Parses a bot log embed (from BotLogCog) into a structured voice event.
    """
    if not msg.embeds:
        return None

    for emb in msg.embeds:
        title = (emb.title or "").strip().lower()
        # Ensure timestamp is UTC
        ts = emb.timestamp or msg.created_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)

        kind: Optional[Literal["join", "leave", "move"]] = None
        if title in _JOIN_TITLES:
            kind = "join"
        elif title in _LEAVE_TITLES:
            kind = "leave"
        elif title in _MOVE_TITLES:
            kind = "move"
        else:
            continue  # Not a voice log embed

        user_id: Optional[int] = None
        channel_id: Optional[int] = None  # For join/leave
        from_id: Optional[int] = None  # For move
        to_id: Optional[int] = None  # For move

        for f in emb.fields or []:
            name = (f.name or "").strip().lower()
            val = f.value or ""
            if name == "user":
                user_id = _extract_last_id(val)
            elif name == "channel":  # Used by join/leave
                channel_id = _extract_last_id(val)
            elif name == "from":  # Used by move
                from_id = _extract_last_id(val)
            elif name == "to":  # Used by move
                to_id = _extract_last_id(val)

        if not user_id:
            continue  # Invalid embed if no user

        if kind == "join" and channel_id:
            return (ts, msg.id, user_id, "join", None, channel_id)
        if kind == "leave" and channel_id:
            return (ts, msg.id, user_id, "leave", channel_id, None)
        if kind == "move" and from_id and to_id:
            return (ts, msg.id, user_id, "move", from_id, to_id)

    return None
