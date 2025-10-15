from __future__ import annotations
import asyncio
import io
from typing import Optional, Tuple, Union

import discord
from discord.ext import commands
from discord import app_commands

from ..strings import S  

GuildTextish = Union[discord.TextChannel, discord.Thread, discord.ForumChannel]


async def _resolve_messageable_from_id(bot: commands.Bot, gid: int, ident: int) -> Optional[GuildTextish]:
    ch = bot.get_channel(ident)
    if isinstance(ch, (discord.TextChannel, discord.ForumChannel)):
        if ch.guild and ch.guild.id == gid:
            return ch
    thr = bot.get_channel(ident)
    if isinstance(thr, discord.Thread):
        if thr.guild and thr.guild.id == gid:
            return thr
    try:
        fetched = await bot.fetch_channel(ident)
        if isinstance(fetched, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
            if fetched.guild and fetched.guild.id == gid:
                return fetched
    except Exception:
        pass
    return None


def _parent_for_destination(dest: GuildTextish) -> Optional[Union[discord.TextChannel, discord.ForumChannel]]:
    if isinstance(dest, discord.TextChannel):
        return dest
    if isinstance(dest, discord.Thread):
        return dest.parent if isinstance(dest.parent, (discord.TextChannel, discord.ForumChannel)) else None
    if isinstance(dest, discord.ForumChannel):
        return dest
    return None


async def _get_or_create_webhook(
    parent: Union[discord.TextChannel, discord.ForumChannel],
    me: discord.Member,
    *,
    name: str = "YuriBot Relay"
) -> Optional[discord.Webhook]:
    try:
        hooks = await parent.webhooks()
        for wh in hooks:
            if wh.user and wh.user.id == me.id:
                return wh
        return await parent.create_webhook(name=name)
    except discord.Forbidden:
        return None


async def _send_copy(
    destination: Union[discord.TextChannel, discord.Thread],
    source_msg: discord.Message,
    *,
    use_webhook: bool,
    webhook: Optional[discord.Webhook],
):
    jump = source_msg.jump_url
    ts = f"<t:{int(source_msg.created_at.timestamp())}:F>"
    author = source_msg.author.display_name
    header = S("move_any.header", author=author, ts=ts, jump=jump)

    content = source_msg.content or ""
    body = (header + ("\n" if content else "") + content).strip()

    files: list[discord.File] = []
    for att in source_msg.attachments:
        try:
            b = await att.read()
            files.append(discord.File(io.BytesIO(b), filename=att.filename))
        except Exception:
            pass

    if source_msg.stickers:
        sticker_lines = []
        for s in source_msg.stickers:
            if getattr(s, "url", None):
                sticker_lines.append(S("move_any.sticker.line_with_url", name=s.name, url=s.url))
            else:
                sticker_lines.append(S("move_any.sticker.line_no_url", name=s.name))
        if sticker_lines:
            body += ("\n\n" if body else "") + "\n".join(sticker_lines)

    # Send
    if use_webhook and webhook is not None:
        if isinstance(destination, discord.Thread):
            await webhook.send(
                content=body or None,
                username=source_msg.author.display_name,
                avatar_url=source_msg.author.display_avatar.url,
                files=files or None,
                thread=destination,
                wait=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        else:
            await webhook.send(
                content=body or None,
                username=source_msg.author.display_name,
                avatar_url=source_msg.author.display_avatar.url,
                files=files or None,
                wait=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )
    else:
        await destination.send(
            content=body or None,
            files=files or None,
            allowed_mentions=discord.AllowedMentions.none(),
        )


async def _maybe_create_destination_thread(
    destination: GuildTextish,
    *,
    dest_thread_title: Optional[str],
) -> Tuple[Optional[Union[discord.TextChannel, discord.Thread]], Optional[str]]:
    if isinstance(destination, discord.Thread):
        return destination, None

    if isinstance(destination, discord.ForumChannel):
        if not dest_thread_title:
            return None, S("move_any.error.forum_needs_title")
        try:
            created = await destination.create_thread(
                name=dest_thread_title,
                content=S("move_any.thread.created_body"))
            return created, None
        except discord.Forbidden:
            return None, S("move_any.error.forbidden_forum")
        except discord.HTTPException as e:
            return None, S("move_any.error.create_forum_failed", err=str(e))

    if isinstance(destination, discord.TextChannel):
        if dest_thread_title:
            try:
                starter = await destination.send(S("move_any.thread.starter_msg", title=dest_thread_title))
                created = await destination.create_thread(name=dest_thread_title, message=starter)
                return created, None
            except discord.Forbidden:
                return None, S("move_any.error.forbidden_thread")
            except discord.HTTPException as e:
                return None, S("move_any.error.create_thread_failed", err=str(e))
        else:
            return destination, None

    return None, S("move_any.error.unsupported_destination")

class MoveAnyCog(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="threadtools", description="Thread & channel utilities")

    @group.command(
        name="move_any",
        description="Copy messages from a channel/thread to a channel/thread (IDs)."
    )
    @app_commands.describe(
        source_id="ID of source TextChannel or Thread",
        destination_id="ID of destination TextChannel/Thread/ForumChannel",
        dest_thread_title="If destination is a Forum, title for the new post (or a new thread title in a TextChannel).",
        use_webhook="Preserve author name & avatar via webhook when possible",
        delete_original="Delete original messages after successful copy",
        limit="Max number of messages to copy (oldest first). Leave empty for all.",
        before="Only copy messages created before this message ID or jump URL (in the source).",
        after="Only copy messages created after this message ID or jump URL (in the source).",
        dry_run="Count messages only; don’t send anything."
    )
    async def move_any(
        self,
        interaction: discord.Interaction,
        source_id: str,
        destination_id: str,
        dest_thread_title: Optional[str] = None,
        use_webhook: bool = True,
        delete_original: bool = False,
        limit: Optional[int] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        dry_run: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        def _parse_id(s: str) -> Optional[int]:
            s = s.strip()
            try:
                return int(s)
            except Exception:
                try:
                    return int(s.rstrip("/").split("/")[-1])
                except Exception:
                    return None

        src_id = _parse_id(source_id)
        dst_id = _parse_id(destination_id)
        if not src_id or not dst_id:
            return await interaction.followup.send(S("move_any.error.bad_ids"), ephemeral=True)

        source = await _resolve_messageable_from_id(self.bot, interaction.guild_id, src_id)
        if not isinstance(source, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_source_type"), ephemeral=True)

        destination_raw = await _resolve_messageable_from_id(self.bot, interaction.guild_id, dst_id)
        if not isinstance(destination_raw, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
            return await interaction.followup.send(S("move_any.error.bad_dest_type"), ephemeral=True)

        me = interaction.guild.me
        src_perms = source.permissions_for(me)
        if not src_perms.read_message_history:
            return await interaction.followup.send(S("move_any.error.need_read_history"), ephemeral=True)

        destination, err = await _maybe_create_destination_thread(destination_raw, dest_thread_title=dest_thread_title)
        if err:
            return await interaction.followup.send(err, ephemeral=True)
        assert destination is not None

        dst_perms = destination.permissions_for(me)
        if not dst_perms.send_messages:
            return await interaction.followup.send(S("move_any.error.need_send_messages"), ephemeral=True)
        if not dst_perms.attach_files:
            return await interaction.followup.send(S("move_any.error.need_attach_files"), ephemeral=True)

        async def _resolve_msg(ref: Optional[str]) -> Optional[discord.Message]:
            if not ref:
                return None
            mid: Optional[int] = None
            try:
                mid = int(ref)
            except Exception:
                try:
                    mid = int(ref.rstrip("/").split("/")[-1])
                except Exception:
                    return None
            try:
                return await source.fetch_message(mid)
            except Exception:
                return None

        before_msg = await _resolve_msg(before)
        after_msg = await _resolve_msg(after)

        to_copy: list[discord.Message] = []
        try:
            async for m in source.history(limit=limit, oldest_first=True, before=before_msg, after=after_msg):
                if m.type != discord.MessageType.default:
                    continue
                to_copy.append(m)
        except discord.Forbidden:
            return await interaction.followup.send(S("move_any.error.forbidden_read_source"), ephemeral=True)

        if not to_copy:
            return await interaction.followup.send(S("move_any.info.none_matched"), ephemeral=True)

        if dry_run:
            where = destination.name
            return await interaction.followup.send(
                S("move_any.info.dry_run", count=len(to_copy), src=source.name, dst=where),
                ephemeral=True
            )

        webhook: Optional[discord.Webhook] = None
        if use_webhook:
            parent = _parent_for_destination(destination)
            if parent and parent.permissions_for(me).manage_webhooks:
                webhook = await _get_or_create_webhook(parent, me)
            if webhook is None:
                await interaction.followup.send(S("move_any.info.webhook_fallback"), ephemeral=True)
                use_webhook = False

        copied = 0
        failed: list[int] = []
        for i, msg in enumerate(to_copy, 1):
            try:
                await _send_copy(destination, msg, use_webhook=use_webhook, webhook=webhook)
                copied += 1
            except Exception:
                failed.append(msg.id)
            if (i % 5) == 0:
                await asyncio.sleep(0.7)

        deleted = 0
        if delete_original and copied:
            if not src_perms.manage_messages:
                await interaction.followup.send(S("move_any.notice.cant_delete_source"), ephemeral=True)
            else:
                for msg in to_copy:
                    if msg.id in failed:
                        continue
                    try:
                        await msg.delete()
                        deleted += 1
                    except Exception:
                        pass
                    await asyncio.sleep(0.2)

        dest_name = destination.name
        summary = S(
            "move_any.summary",
            copied=copied,
            total=len(to_copy),
            src=source.name,
            dst=dest_name,
        )
        if failed:
            summary += " " + S("move_any.summary_failed_tail", failed=len(failed))
        if delete_original:
            summary += " " + S("move_any.summary_deleted_tail", deleted=deleted)

        await interaction.followup.send(summary, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(MoveAnyCog(bot))
