from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S
from ..ui.movebot import format_move_summary, format_pin_summary
from ..utils.movebot import (
    attach_signature,
    fuzzy_ratio,
    get_or_create_webhook,
    normalize_content,
    parent_for_destination,
    parse_jump_or_id,
    resolve_messageable_from_id,
    send_copy,
)

log = logging.getLogger(__name__)


class MoveAnyCog(commands.Cog):
    """Move or pin messages between channels/threads."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="move", description="Move/pin utilities")

    async def _collect_messages_between(
        self,
        channel: discord.TextChannel | discord.Thread,
        start_id: int,
        end_id: int,
    ) -> List[discord.Message]:
        messages: List[discord.Message] = []
        async for message in channel.history(
            limit=None, oldest_first=True, after=discord.Object(id=start_id - 1)
        ):
            messages.append(message)
            if message.id == end_id:
                break
        return messages

    @group.command(
        name="any",
        description="Copy messages between channels/threads, optionally deleting originals.",
    )
    @app_commands.describe(
        source_id="ID or jump URL of the source message to start from.",
        destination_id="ID or jump URL of the destination channel/thread.",
        end_id="Optional ID or jump URL of the last message to include.",
        delete_original="Delete original messages after copying.",
        include_header="Include author/timestamp/jump header.",
        use_webhook="Relay via webhook to preserve author appearance.",
        post="Post summary publicly in this channel.",
    )
    async def move_any(
        self,
        interaction: discord.Interaction,
        source_id: str,
        destination_id: str,
        end_id: Optional[str] = None,
        delete_original: bool = False,
        include_header: bool = True,
        use_webhook: bool = True,
        post: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        await interaction.response.defer(ephemeral=not post, thinking=True)

        gid = interaction.guild_id
        src_ident = parse_jump_or_id(source_id)
        dst_ident = parse_jump_or_id(destination_id)
        end_ident = parse_jump_or_id(end_id) if end_id else None
        if not src_ident or not dst_ident:
            return await interaction.followup.send(S("move_any.error.bad_ids"), ephemeral=not post)

        source = await resolve_messageable_from_id(self.bot, gid, src_ident)
        destination = await resolve_messageable_from_id(self.bot, gid, dst_ident)
        if not isinstance(source, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_source_type"), ephemeral=not post)
        if not isinstance(destination, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_dest_type_text_or_thread"), ephemeral=not post)

        try:
            start_msg = await source.fetch_message(src_ident)
        except discord.NotFound:
            return await interaction.followup.send(S("move_any.error.start_not_found"), ephemeral=not post)

        end_msg: Optional[discord.Message] = None
        if end_ident:
            try:
                end_msg = await source.fetch_message(end_ident)
            except discord.NotFound:
                return await interaction.followup.send(S("move_any.error.end_not_found"), ephemeral=not post)
            if end_msg.created_at < start_msg.created_at:
                start_msg, end_msg = end_msg, start_msg

        to_copy = await self._collect_messages_between(
            source, start_msg.id, end_msg.id if end_msg else start_msg.id
        )
        if not to_copy:
            return await interaction.followup.send(S("move_any.info.no_messages"), ephemeral=not post)

        me = interaction.guild.me
        if not source.permissions_for(me).read_message_history:
            return await interaction.followup.send(S("move_any.error.need_read_history"), ephemeral=not post)
        if not destination.permissions_for(me).send_messages:
            return await interaction.followup.send(S("move_any.error.need_send_dest"), ephemeral=not post)

        webhook = None
        parent = parent_for_destination(destination)
        if use_webhook and isinstance(parent, (discord.TextChannel, discord.ForumChannel)):
            webhook = await get_or_create_webhook(parent, me)
            if webhook is None:
                use_webhook = False

        copied = 0
        failed = 0
        for message in to_copy:
            ok, _ = await send_copy(
                destination,
                message,
                use_webhook=use_webhook,
                webhook=webhook,
                include_header=include_header,
            )
            if ok:
                copied += 1
            else:
                failed += 1
            await asyncio.sleep(0.2)

        deleted = 0
        if delete_original:
            for message in to_copy:
                try:
                    await message.delete()
                    deleted += 1
                except Exception as exc:
                    log.info(
                        "movebot.delete.failed",
                        extra={"message_id": message.id, "error": str(exc)},
                    )
                await asyncio.sleep(0.1)

        summary = format_move_summary(
            copied=copied,
            total=len(to_copy),
            failed=failed,
            deleted=deleted,
            post_publicly=post,
        )
        await interaction.followup.send(summary, ephemeral=not post)

    @group.command(
        name="pinmatch",
        description="Mirror pins from a source channel/thread into a destination by content matching.",
    )
    @app_commands.describe(
        source_id="ID (or jump URL) of the source TextChannel/Thread with pins.",
        destination_id="ID (or jump URL) of the destination TextChannel/Thread to pin in.",
        search_depth="How many destination messages to scan (default 3000).",
        allow_header="Set true if copies may include a header/backlink you want ignored.",
        ignore_case="Case-insensitive content match (default true).",
        collapse_ws="Collapse whitespace for matching (default true).",
        min_fuzzy="Minimum fuzzy ratio (0.0-1.0) if exact match fails (default 0.88).",
        ts_slack_seconds="Timestamp slack for candidate selection (default 240s).",
        post="Post summary publicly in this channel.",
    )
    async def pinmatch(
        self,
        interaction: discord.Interaction,
        source_id: str,
        destination_id: str,
        search_depth: int = 3000,
        allow_header: bool = True,
        ignore_case: bool = True,
        collapse_ws: bool = True,
        min_fuzzy: float = 0.88,
        ts_slack_seconds: int = 240,
        post: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        await interaction.response.defer(ephemeral=not post, thinking=True)

        gid = interaction.guild_id
        src_ident = parse_jump_or_id(source_id)
        dst_ident = parse_jump_or_id(destination_id)
        if not src_ident or not dst_ident:
            return await interaction.followup.send(S("move_any.error.bad_ids"), ephemeral=not post)

        source = await resolve_messageable_from_id(self.bot, gid, src_ident)
        destination = await resolve_messageable_from_id(self.bot, gid, dst_ident)
        if not isinstance(source, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_source_type"), ephemeral=not post)
        if not isinstance(destination, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_dest_type_text_or_thread"), ephemeral=not post)

        me = interaction.guild.me
        if not source.permissions_for(me).read_message_history:
            return await interaction.followup.send(S("move_any.error.need_read_history"), ephemeral=not post)
        if not destination.permissions_for(me).manage_messages:
            return await interaction.followup.send(S("move_any.error.need_read_and_manage_dest"), ephemeral=not post)

        try:
            src_pins = await source.pins()
        except discord.Forbidden:
            return await interaction.followup.send(S("move_any.error.forbidden_read_pins"), ephemeral=not post)

        if not src_pins:
            return await interaction.followup.send(S("move_any.info.no_pins_source"), ephemeral=not post)

        dest_msgs: List[discord.Message] = [
            message
            async for message in destination.history(limit=search_depth, oldest_first=False)
            if message.type == discord.MessageType.default
        ]

        by_content: Dict[str, List[discord.Message]] = {}
        by_attach: Dict[str, List[discord.Message]] = {}
        for dest_msg in dest_msgs:
            key = normalize_content(
                dest_msg.content or "",
                allow_header=allow_header,
                ignore_case=ignore_case,
                collapse_ws=collapse_ws,
            )
            by_content.setdefault(key, []).append(dest_msg)
            sig = attach_signature(dest_msg)
            if sig:
                by_attach.setdefault(sig, []).append(dest_msg)

        pinned = 0
        misses: List[int] = []
        for src_pin in src_pins:
            if src_pin.type != discord.MessageType.default:
                misses.append(src_pin.id)
                continue

            src_key = normalize_content(
                src_pin.content or "",
                allow_header=False,
                ignore_case=ignore_case,
                collapse_ws=collapse_ws,
            )
            src_sig = attach_signature(src_pin)
            timestamp = int(src_pin.created_at.timestamp())

            candidates: List[discord.Message] = []
            if src_key:
                candidates = list(by_content.get(src_key, []))
            if src_sig:
                with_attach = by_attach.get(src_sig, [])
                if candidates:
                    ids = {msg.id for msg in candidates}
                    both = [msg for msg in with_attach if msg.id in ids]
                    candidates = both or candidates
                else:
                    candidates = list(with_attach)

            if not candidates and src_key:
                best: Optional[Tuple[discord.Message, float]] = None
                for dest_msg in dest_msgs:
                    dkey = normalize_content(
                        dest_msg.content or "",
                        allow_header=allow_header,
                        ignore_case=ignore_case,
                        collapse_ws=collapse_ws,
                    )
                    if not dkey:
                        continue
                    ratio = fuzzy_ratio(src_key, dkey)
                    if ratio >= min_fuzzy and (best is None or ratio > best[1]):
                        best = (dest_msg, ratio)
                if best:
                    candidates = [best[0]]

            if not candidates:
                misses.append(src_pin.id)
                continue

            candidates.sort(key=lambda msg: abs(int(msg.created_at.timestamp()) - timestamp))
            target = candidates[0]
            try:
                await target.pin()
                pinned += 1
                await asyncio.sleep(0.3)
            except Exception:
                misses.append(src_pin.id)

        summary = format_pin_summary(
            pinned=pinned,
            total=len(src_pins),
            destination=destination,
            misses=misses,
        )
        await interaction.followup.send(summary, ephemeral=not post)


async def setup(bot: commands.Bot):
    await bot.add_cog(MoveAnyCog(bot))
