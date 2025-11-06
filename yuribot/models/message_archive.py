from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Sequence

from ..db import connect


@dataclass(slots=True)
class ArchivedMessage:
    message_id: int
    guild_id: int
    channel_id: int
    author_id: int
    message_type: str
    created_at: str
    content: str | None
    edited_at: str | None
    attachments: int
    embeds: int

    reactions: str | None
    reply_to_id: int | None

    def as_db_tuple(
        self,
    ) -> tuple[
        int,
        int,
        int,
        int,
        str,
        str,
        str | None,
        str | None,
        int,
        int,
        str | None,
        int | None,
    ]:
        return (
            self.message_id,
            self.guild_id,
            self.channel_id,
            self.author_id,
            self.message_type,
            self.created_at,
            self.content,
            self.edited_at,
            self.attachments,
            self.embeds,
            self.reactions,
            self.reply_to_id,
        )


def _ensure_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat()


def _serialize_reactions(message: "discord.Message") -> str | None:
    reactions = getattr(message, "reactions", None) or []
    if not reactions:
        return None

    payload: list[dict[str, object]] = []
    for reaction in reactions:
        emoji = getattr(reaction, "emoji", None)
        if emoji is None:
            emoji_repr = None
            emoji_id = None
            emoji_name = None
            emoji_animated = None
        else:
            emoji_repr = str(emoji)
            emoji_id = getattr(emoji, "id", None)
            emoji_name = getattr(emoji, "name", None)
            emoji_animated = getattr(emoji, "animated", None)

        payload.append(
            {
                "emoji": emoji_repr,
                "emoji_id": emoji_id,
                "emoji_name": emoji_name,
                "emoji_animated": emoji_animated,
                "count": getattr(reaction, "count", 0),
                "me": getattr(reaction, "me", False),
            }
        )

    return json.dumps(payload, ensure_ascii=False)


def _resolve_reply_to_id(message: "discord.Message") -> int | None:
    reference = getattr(message, "reference", None)
    if reference is not None and getattr(reference, "message_id", None) is not None:
        return int(reference.message_id)

    referenced = getattr(message, "referenced_message", None)
    if referenced is not None and getattr(referenced, "id", None) is not None:
        return int(referenced.id)

    return None


def from_discord_message(message: "discord.Message") -> ArchivedMessage:
    # Local import to avoid importing discord.py for callers that only need DB helpers.
    from discord import Message  # type: ignore

    if not isinstance(message, Message):
        raise TypeError("message must be a discord.Message")

    guild = message.guild
    channel = message.channel
    author = message.author
    if guild is None:
        raise ValueError("Message has no guild; refusing to archive DMs")
    if not hasattr(channel, "id"):
        raise ValueError("Message channel missing id")
    if author is None or not hasattr(author, "id"):
        raise ValueError("Message author missing id")

    return ArchivedMessage(
        message_id=message.id,
        guild_id=guild.id,
        channel_id=channel.id,  # type: ignore[arg-type]
        author_id=author.id,  # type: ignore[arg-type]
        message_type=str(message.type.name if hasattr(message.type, "name") else message.type),
        created_at=_ensure_utc_iso(message.created_at) or "",
        content=message.content or None,
        edited_at=_ensure_utc_iso(message.edited_at),
        attachments=len(getattr(message, "attachments", []) or []),
        embeds=len(getattr(message, "embeds", []) or []),
        reactions=_serialize_reactions(message),
        reply_to_id=_resolve_reply_to_id(message),
    )


def upsert_many(rows: Sequence[ArchivedMessage] | Iterable[ArchivedMessage]) -> int:
    if not rows:
        return 0

    # Support both sequences and general iterables.
    iterable: Iterable[ArchivedMessage]
    if isinstance(rows, Sequence):
        if len(rows) == 0:
            return 0
        iterable = rows
    else:
        iterable = list(rows)
        if not iterable:
            return 0

    tuples = [row.as_db_tuple() for row in iterable]

    with connect() as con:
        cur = con.cursor()
        cur.executemany(
            """
            INSERT INTO message_archive (
                message_id, guild_id, channel_id, author_id,
                message_type, created_at, content, edited_at,
                attachments, embeds, reactions, reply_to_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                guild_id=excluded.guild_id,
                channel_id=excluded.channel_id,
                author_id=excluded.author_id,
                message_type=excluded.message_type,
                created_at=excluded.created_at,
                content=excluded.content,
                edited_at=excluded.edited_at,
                attachments=excluded.attachments,
                embeds=excluded.embeds,
                reactions=excluded.reactions,
                reply_to_id=excluded.reply_to_id
            """,
            tuples,
        )
        con.commit()
    return len(tuples)


def max_message_id(guild_id: int, channel_id: int) -> int | None:
    with connect() as con:
        cur = con.cursor()
        row = cur.execute(
            "SELECT MAX(message_id) FROM message_archive WHERE guild_id=? AND channel_id=?",
            (guild_id, channel_id),
        ).fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None
