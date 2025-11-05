from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, Union
from difflib import SequenceMatcher

import discord

log = logging.getLogger(__name__)

GuildTextish = Union[discord.TextChannel, discord.Thread, discord.ForumChannel]


async def resolve_messageable_from_id(
    bot: discord.Client, guild_id: int, ident: int
) -> Optional[GuildTextish]:
    log.debug("movebot.resolve_messageable start gid=%s ident=%s", guild_id, ident)
    ch = bot.get_channel(ident)
    if isinstance(ch, (discord.TextChannel, discord.ForumChannel)) and getattr(ch.guild, "id", None) == guild_id:
        return ch
    if isinstance(ch, discord.Thread) and getattr(ch.guild, "id", None) == guild_id:
        return ch
    try:
        fetched = await bot.fetch_channel(ident)
        if isinstance(fetched, (discord.TextChannel, discord.Thread, discord.ForumChannel)) and getattr(
            fetched.guild, "id", None
        ) == guild_id:
            return fetched
    except Exception as exc:
        log.debug("movebot.resolve_messageable fetch_channel failed err=%r", exc)
    return None


def parent_for_destination(dest: GuildTextish) -> Optional[Union[discord.TextChannel, discord.ForumChannel]]:
    if isinstance(dest, discord.TextChannel):
        return dest
    if isinstance(dest, discord.Thread):
        parent = dest.parent
        if isinstance(parent, (discord.TextChannel, discord.ForumChannel)):
            return parent
    if isinstance(dest, discord.ForumChannel):
        return dest
    return None


async def get_or_create_webhook(
    parent: Union[discord.TextChannel, discord.ForumChannel],
    me: discord.Member,
    *,
    name: str = "YuriBot Relay",
) -> Optional[discord.Webhook]:
    try:
        hooks = await parent.webhooks()
        for hook in hooks:
            if hook.user and hook.user.id == me.id:
                return hook
        return await parent.create_webhook(name=name)
    except discord.Forbidden:
        return None
    except Exception as exc:
        log.debug("movebot.webhook unexpected err=%r", exc)
        return None


def attach_signature(message: discord.Message) -> str:
    if not getattr(message, "attachments", None):
        return ""
    items = [f"{attachment.filename}:{attachment.size}" for attachment in message.attachments]
    return "|".join(items)


def normalize_content(
    content: str,
    *,
    allow_header: bool,
    ignore_case: bool,
    collapse_ws: bool,
) -> str:
    text = content or ""
    if not allow_header:
        lines = text.splitlines()
        if lines and lines[0].startswith(">"):
            lines = lines[1:]
        text = "\n".join(lines)
    if ignore_case:
        text = text.lower()
    if collapse_ws:
        text = " ".join(text.split())
    return text.strip()


def fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(a=a, b=b).ratio()


@dataclass
class MoveRequest:
    source: discord.TextChannel | discord.Thread
    destination: discord.TextChannel | discord.Thread
    to_copy: List[discord.Message]
    delete_original: bool
    use_webhook: bool
    include_header: bool


async def send_copy(
    destination: Union[discord.TextChannel, discord.Thread],
    source_msg: discord.Message,
    *,
    use_webhook: bool,
    webhook: Optional[discord.Webhook],
    include_header: bool,
) -> Tuple[bool, Optional[discord.Message]]:
    reply_prefix = ""
    try:
        if source_msg.reference and source_msg.reference.message_id:
            ref: Optional[discord.Message] = getattr(source_msg.reference, "resolved", None)
            if ref is None:
                try:
                    ref = await source_msg.channel.fetch_message(source_msg.reference.message_id)
                except Exception:
                    ref = None
            if ref is not None:
                snippet = (ref.content or "").strip().replace("\n", " ")
                if len(snippet) > 140:
                    snippet = snippet[:137] + "..."
                if not snippet and ref.attachments:
                    snippet = "Attachment"
                reply_prefix = f">> {ref.author.display_name} ({ref.jump_url}) {snippet}"
    except Exception:
        reply_prefix = ""

    body_parts = []
    if reply_prefix:
        body_parts.append(reply_prefix)
    if include_header:
        jump = source_msg.jump_url
        ts = f"<t:{int(source_msg.created_at.timestamp())}:F>"
        body_parts.append(f"{source_msg.author.display_name} [{ts}] - {jump}")
    if source_msg.content:
        body_parts.append(source_msg.content)
    body = "\n".join(body_parts).strip()

    files: List[discord.File] = []
    for attachment in source_msg.attachments:
        try:
            data = await attachment.read()
            files.append(discord.File(io.BytesIO(data), filename=attachment.filename))
        except Exception as exc:
            log.debug("movebot.attach.read_failed id=%s err=%r", attachment.id, exc)

    try:
        if use_webhook and webhook:
            sent = await webhook.send(
                body or "\u200b",
                username=source_msg.author.display_name,
                avatar_url=getattr(source_msg.author.display_avatar, "url", None),
                wait=True,
                files=files or None,
            )
            return True, sent
        else:
            sent_msg = await destination.send(
                body or "\u200b",
                files=files or None,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return True, sent_msg
    except discord.HTTPException as exc:
        log.debug("movebot.send_copy http_error err=%r", exc)
        return False, None
    finally:
        for file in files:
            file.close()


def parse_jump_or_id(raw: str) -> Optional[int]:
    text = (raw or "").strip()
    try:
        return int(text)
    except Exception:
        try:
            return int(text.rstrip("/").split("/")[-1])
        except Exception:
            return None
