from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional

import discord
from discord.ext import commands

from ..strings import S
from ..ui.botlog import build_embed, channel_reference, format_roles, safe_add_field
from ..utils.botlog import BotLogCache, IGNORED_USER_IDS, channel_from_id

log = logging.getLogger(__name__)


class BotLogCog(commands.Cog):
    """Server audit/bot log; posts structured embeds to a configured channel."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._invite_cache: Dict[int, List[discord.Invite]] = {}
        self._botlog_cache = BotLogCache(ttl_seconds=60.0)

    async def _post(self, guild: discord.Guild, embed: discord.Embed):
        ch_id = self._botlog_cache.get_channel_id(guild.id)
        ch = channel_from_id(guild, ch_id)
        if not isinstance(ch, discord.TextChannel):
            log.debug(
                "botlog.skip_no_channel",
                extra={"guild_id": guild.id, "channel_id": ch_id},
            )
            return

        for attempt in (1, 2):
            try:
                await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                return
            except discord.Forbidden as exc:
                log.warning(
                    "botlog.forbidden",
                    extra={
                        "guild_id": guild.id,
                        "channel_id": ch.id,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                return
            except Exception as exc:
                log.warning(
                    "botlog.post_failed",
                    extra={
                        "guild_id": guild.id,
                        "channel_id": getattr(ch, "id", None),
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                if attempt == 1:
                    await asyncio.sleep(0.4)

    async def _refresh_invites(self, guild: discord.Guild):
        try:
            invites = await guild.invites()
            self._invite_cache[guild.id] = invites
            log.info(
                "botlog.invites_refreshed",
                extra={"guild_id": guild.id, "count": len(invites)},
            )
        except Exception as exc:
            self._invite_cache[guild.id] = []
            log.warning(
                "botlog.invites_refresh_failed",
                extra={"guild_id": guild.id, "error": str(exc)},
            )

    @commands.Cog.listener()
    async def on_ready(self):
        tasks = [self._refresh_invites(g) for g in self.bot.guilds]
        if not tasks:
            return
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            log.exception("botlog.on_ready_invite_warm_failed")

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        await self._refresh_invites(guild)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if not message.guild or (message.author and message.author.id in IGNORED_USER_IDS):
            return
        emb = build_embed("botlog.title.message_deleted", discord.Color.orange())
        author = getattr(message, "author", None)
        if isinstance(author, discord.Member):
            safe_add_field(
                emb,
                name_key="botlog.field.author",
                value=f"{author.mention} (`{author.id}`)",
                inline=True,
            )
        channel = getattr(message, "channel", None)
        if isinstance(channel, discord.TextChannel):
            safe_add_field(
                emb,
                name_key="botlog.field.channel",
                value=f"{channel.mention} (`{channel.id}`)",
                inline=True,
            )
        safe_add_field(
            emb,
            name_key="botlog.field.content",
            value=(message.content or "").strip(),
            inline=False,
        )
        if getattr(message, "attachments", None):
            att = "\n".join([attachment.filename for attachment in message.attachments])
            safe_add_field(
                emb,
                name_key="botlog.field.deleted_attachments",
                value=att,
                inline=False,
            )
        await self._post(message.guild, emb)
        log.info(
            "botlog.event.message_delete",
            extra={
                "guild_id": message.guild.id,
                "channel_id": getattr(channel, "id", None),
            },
        )

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        msg = after or before
        guild = getattr(msg, "guild", None)
        if not guild:
            return
        author = getattr(msg, "author", None)
        if not author or getattr(author, "id", None) in IGNORED_USER_IDS:
            return
        before_content = (getattr(before, "content", None) or "").strip()
        after_content = (getattr(after, "content", None) or "").strip()
        before_atts = tuple(getattr(before, "attachments", []) or [])
        after_atts = tuple(getattr(after, "attachments", []) or [])
        if before_content == after_content and len(before_atts) == len(after_atts):
            return

        emb = build_embed("botlog.title.message_edited", discord.Color.yellow())
        safe_add_field(
            emb,
            name_key="botlog.field.author",
            value=f"{author.mention} (`{author.id}`)",
            inline=True,
        )
        channel = getattr(msg, "channel", None)
        if isinstance(channel, discord.TextChannel):
            safe_add_field(
                emb,
                name_key="botlog.field.channel",
                value=f"{channel.mention} (`{channel.id}`)",
                inline=True,
            )
        safe_add_field(
            emb,
            name_key="botlog.field.before",
            value=before_content or S("botlog.common.none"),
            inline=False,
        )
        safe_add_field(
            emb,
            name_key="botlog.field.after",
            value=after_content or S("botlog.common.none"),
            inline=False,
        )
        await self._post(guild, emb)
        log.info(
            "botlog.event.message_edit",
            extra={
                "guild_id": guild.id,
                "channel_id": getattr(channel, "id", None),
            },
        )

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        guild = self.bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        channel = channel_from_id(guild, payload.channel_id)
        emb = build_embed("botlog.title.bulk_delete", discord.Color.dark_orange())
        safe_add_field(
            emb,
            name_key="botlog.field.channel",
            value=channel_reference(channel),
            inline=True,
        )
        safe_add_field(
            emb,
            name_key="botlog.field.count",
            value=str(len(payload.message_ids)),
            inline=True,
        )
        await self._post(guild, emb)
        log.info(
            "botlog.event.bulk_delete",
            extra={
                "guild_id": guild.id,
                "channel_id": getattr(channel, "id", None),
                "count": len(payload.message_ids),
            },
        )

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):
        if not invite.guild:
            return
        emb = build_embed("botlog.title.invite_created", discord.Color.blue())
        safe_add_field(emb, name_key="botlog.field.code", value=invite.code, inline=True)
        if invite.inviter:
            safe_add_field(
                emb,
                name_key="botlog.field.inviter",
                value=f"{invite.inviter.mention} (`{invite.inviter.id}`)",
                inline=True,
            )
        if invite.channel:
            safe_add_field(
                emb,
                name_key="botlog.field.channel",
                value=channel_reference(invite.channel),
                inline=True,
            )
        if invite.max_uses:
            safe_add_field(
                emb,
                name_key="botlog.field.max_uses",
                value=str(invite.max_uses),
                inline=True,
            )
        if invite.max_age:
            safe_add_field(
                emb,
                name_key="botlog.field.max_age_seconds",
                value=str(invite.max_age),
                inline=True,
            )
        await self._post(invite.guild, emb)
        await self._refresh_invites(invite.guild)
        log.info(
            "botlog.event.invite_create",
            extra={
                "guild_id": invite.guild.id,
                "channel_id": getattr(invite.channel, "id", None),
            },
        )

    @commands.Cog.listener()
    async def on_invite_delete(self, invite: discord.Invite):
        if not invite.guild:
            return
        emb = build_embed("botlog.title.invite_deleted", discord.Color.dark_blue())
        safe_add_field(emb, name_key="botlog.field.code", value=invite.code, inline=True)
        if invite.channel:
            safe_add_field(
                emb,
                name_key="botlog.field.channel",
                value=channel_reference(invite.channel),
                inline=True,
            )
        await self._post(invite.guild, emb)
        await self._refresh_invites(invite.guild)
        log.info(
            "botlog.event.invite_delete",
            extra={
                "guild_id": invite.guild.id,
                "channel_id": getattr(invite.channel, "id", None),
            },
        )

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        emb = build_embed("botlog.title.member_join", discord.Color.green())
        safe_add_field(
            emb,
            name_key="botlog.field.user",
            value=f"{member.mention} (`{member.id}`)",
            inline=False,
        )
        await self._post(member.guild, emb)
        log.info(
            "botlog.event.member_join",
            extra={"guild_id": member.guild.id, "user_id": member.id},
        )

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        emb = build_embed("botlog.title.member_leave", discord.Color.orange())
        safe_add_field(
            emb,
            name_key="botlog.field.user",
            value=f"{member} (`{member.id}`)",
            inline=False,
        )
        await self._post(member.guild, emb)
        log.info(
            "botlog.event.member_remove",
            extra={"guild_id": member.guild.id, "user_id": member.id},
        )

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if before.nick != after.nick:
            emb = build_embed("botlog.title.nick_change", discord.Color.blurple())
            safe_add_field(
                emb,
                name_key="botlog.field.user",
                value=f"{after.mention} (`{after.id}`)",
                inline=False,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.before",
                value=before.nick or S("botlog.common.none"),
                inline=True,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.after",
                value=after.nick or S("botlog.common.none"),
                inline=True,
            )
            await self._post(after.guild, emb)
            log.info(
                "botlog.event.nick_change",
                extra={"guild_id": after.guild.id, "user_id": after.id},
            )

        before_roles = set(before.roles)
        after_roles = set(after.roles)
        added = after_roles - before_roles
        removed = before_roles - after_roles
        if added or removed:
            emb = build_embed("botlog.title.member_roles_updated", discord.Color.teal())
            safe_add_field(
                emb,
                name_key="botlog.field.user",
                value=f"{after.mention} (`{after.id}`)",
                inline=False,
            )
            if added:
                safe_add_field(
                    emb,
                    name_key="botlog.field.roles_added",
                    value=format_roles(added),
                    inline=False,
                )
            if removed:
                safe_add_field(
                    emb,
                    name_key="botlog.field.roles_removed",
                    value=format_roles(removed),
                    inline=False,
                )
            await self._post(after.guild, emb)
            log.info(
                "botlog.event.roles_updated",
                extra={
                    "guild_id": after.guild.id,
                    "user_id": after.id,
                    "added": len(added),
                    "removed": len(removed),
                },
            )

        before_timeout = getattr(before, "communication_disabled_until", None)
        after_timeout = getattr(after, "communication_disabled_until", None)
        if before_timeout != after_timeout:
            emb = build_embed("botlog.title.timeout_updated", discord.Color.dark_teal())
            safe_add_field(
                emb,
                name_key="botlog.field.user",
                value=f"{after.mention} (`{after.id}`)",
                inline=False,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.before",
                value=str(before_timeout) if before_timeout else S("botlog.common.none"),
                inline=True,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.after",
                value=str(after_timeout) if after_timeout else S("botlog.common.none"),
                inline=True,
            )
            await self._post(after.guild, emb)
            log.info(
                "botlog.event.timeout_updated",
                extra={"guild_id": after.guild.id, "user_id": after.id},
            )

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        emb = build_embed("botlog.title.member_banned", discord.Color.red())
        safe_add_field(
            emb,
            name_key="botlog.field.user",
            value=f"{user} (`{user.id}`)",
            inline=False,
        )
        await self._post(guild, emb)
        log.info(
            "botlog.event.member_banned",
            extra={"guild_id": guild.id, "user_id": user.id},
        )

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        emb = build_embed("botlog.title.member_unbanned", discord.Color.dark_red())
        safe_add_field(
            emb,
            name_key="botlog.field.user",
            value=f"{user} (`{user.id}`)",
            inline=False,
        )
        await self._post(guild, emb)
        log.info(
            "botlog.event.member_unbanned",
            extra={"guild_id": guild.id, "user_id": user.id},
        )

    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        emb = build_embed("botlog.title.role_created", discord.Color.green())
        safe_add_field(
            emb,
            name_key="botlog.field.role",
            value=f"{role.mention} (`{role.id}`)",
            inline=False,
        )
        await self._post(role.guild, emb)
        log.info(
            "botlog.event.role_created",
            extra={"guild_id": role.guild.id, "role_id": role.id},
        )

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        emb = build_embed("botlog.title.role_deleted", discord.Color.orange())
        safe_add_field(
            emb,
            name_key="botlog.field.role",
            value=f"{role.name} (`{role.id}`)",
            inline=False,
        )
        await self._post(role.guild, emb)
        log.info(
            "botlog.event.role_deleted",
            extra={"guild_id": role.guild.id, "role_id": role.id},
        )

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        changes: List[str] = []
        if before.name != after.name:
            changes.append(S("botlog.change.role_name", before=before.name, after=after.name))
        if before.color != after.color:
            changes.append(S("botlog.change.role_color", before=before.color, after=after.color))
        if before.permissions.value != after.permissions.value:
            changes.append(S("botlog.change.role_perms"))

        if not changes:
            return

        emb = build_embed("botlog.title.role_updated", discord.Color.yellow())
        safe_add_field(
            emb,
            name_key="botlog.field.role",
            value=f"{after.mention} (`{after.id}`)",
            inline=False,
        )
        safe_add_field(
            emb,
            name_key="botlog.field.changes",
            value="\n".join(changes),
            inline=False,
        )
        await self._post(after.guild, emb)
        log.info(
            "botlog.event.role_updated",
            extra={"guild_id": after.guild.id, "role_id": after.id, "changes": len(changes)},
        )

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        emb = build_embed("botlog.title.channel_created", discord.Color.green())
        safe_add_field(
            emb,
            name_key="botlog.field.channel",
            value=channel_reference(channel),
            inline=False,
        )
        await self._post(channel.guild, emb)
        log.info(
            "botlog.event.channel_created",
            extra={"guild_id": channel.guild.id, "channel_id": getattr(channel, 'id', None)},
        )

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        emb = build_embed("botlog.title.channel_deleted", discord.Color.orange())
        safe_add_field(
            emb,
            name_key="botlog.field.channel",
            value=channel_reference(channel),
            inline=False,
        )
        await self._post(channel.guild, emb)
        log.info(
            "botlog.event.channel_deleted",
            extra={"guild_id": channel.guild.id, "channel_id": getattr(channel, 'id', None)},
        )

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
        changes: List[str] = []
        if getattr(before, "name", None) != getattr(after, "name", None):
            changes.append(
                S(
                    "botlog.change.channel_name",
                    before=getattr(before, "name", None),
                    after=getattr(after, "name", None),
                )
            )
        if getattr(before, "topic", None) != getattr(after, "topic", None):
            changes.append(S("botlog.change.channel_topic"))
        if getattr(before, "nsfw", None) != getattr(after, "nsfw", None):
            changes.append(
                S(
                    "botlog.change.channel_nsfw",
                    before=getattr(before, "nsfw", None),
                    after=getattr(after, "nsfw", None),
                )
            )
        if not changes:
            return

        emb = build_embed("botlog.title.channel_updated", discord.Color.yellow())
        safe_add_field(
            emb,
            name_key="botlog.field.channel",
            value=channel_reference(after),
            inline=False,
        )
        safe_add_field(
            emb,
            name_key="botlog.field.changes",
            value="\n".join(changes),
            inline=False,
        )
        await self._post(after.guild, emb)
        log.info(
            "botlog.event.channel_updated",
            extra={
                "guild_id": after.guild.id,
                "channel_id": getattr(after, "id", None),
                "changes": len(changes),
            },
        )

    @commands.Cog.listener()
    async def on_guild_emojis_update(self, guild: discord.Guild, before: List[discord.Emoji], after: List[discord.Emoji]):
        before_map = {emoji.id: emoji for emoji in before}
        after_map = {emoji.id: emoji for emoji in after}
        created = [emoji for emoji in after if emoji.id not in before_map]
        deleted = [emoji for emoji in before if emoji.id not in after_map]
        renamed = [
            emoji
            for emoji in after
            if emoji.id in before_map and emoji.name != before_map[emoji.id].name
        ]

        if created:
            emb = build_embed("botlog.title.emoji_created", discord.Color.green())
            safe_add_field(
                emb,
                name_key="botlog.field.emojis",
                value=", ".join([f":{emoji.name}:" for emoji in created]),
                inline=False,
            )
            await self._post(guild, emb)

        if deleted:
            emb = build_embed("botlog.title.emoji_deleted", discord.Color.orange())
            safe_add_field(
                emb,
                name_key="botlog.field.emojis",
                value=", ".join([f":{emoji.name}:" for emoji in deleted]),
                inline=False,
            )
            await self._post(guild, emb)

        if renamed:
            emb = build_embed("botlog.title.emoji_renamed", discord.Color.yellow())
            lines = [
                f":{before_map[emoji.id].name}: -> :{emoji.name}:"
                for emoji in renamed
            ]
            safe_add_field(
                emb,
                name_key="botlog.field.changes",
                value="\n".join(lines),
                inline=False,
            )
            await self._post(guild, emb)
        if created or deleted or renamed:
            log.info(
                "botlog.event.emojis_update",
                extra={
                    "guild_id": guild.id,
                    "created": len(created),
                    "deleted": len(deleted),
                    "renamed": len(renamed),
                },
            )

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        # Join
        if before.channel is None and after.channel is not None:
            emb = build_embed("botlog.title.voice_join", discord.Color.green())
            safe_add_field(
                emb,
                name_key="botlog.field.user",
                value=f"{member.mention} (`{member.id}`)",
                inline=True,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.channel",
                value=channel_reference(after.channel),
                inline=True,
            )
            await self._post(member.guild, emb)
            log.info(
                "botlog.event.voice_join",
                extra={
                    "guild_id": member.guild.id,
                    "user_id": member.id,
                    "channel_id": getattr(after.channel, "id", None),
                },
            )
        # Leave
        elif before.channel is not None and after.channel is None:
            emb = build_embed("botlog.title.voice_leave", discord.Color.orange())
            safe_add_field(
                emb,
                name_key="botlog.field.user",
                value=f"{member.mention} (`{member.id}`)",
                inline=True,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.channel",
                value=channel_reference(before.channel),
                inline=True,
            )
            await self._post(member.guild, emb)
            log.info(
                "botlog.event.voice_leave",
                extra={
                    "guild_id": member.guild.id,
                    "user_id": member.id,
                    "channel_id": getattr(before.channel, "id", None),
                },
            )
        # Move
        elif before.channel is not None and after.channel is not None and before.channel.id != after.channel.id:
            emb = build_embed("botlog.title.voice_move", discord.Color.blurple())
            safe_add_field(
                emb,
                name_key="botlog.field.user",
                value=f"{member.mention} (`{member.id}`)",
                inline=False,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.from",
                value=channel_reference(before.channel),
                inline=True,
            )
            safe_add_field(
                emb,
                name_key="botlog.field.to",
                value=channel_reference(after.channel),
                inline=True,
            )
            await self._post(member.guild, emb)
            log.info(
                "botlog.event.voice_move",
                extra={
                    "guild_id": member.guild.id,
                    "user_id": member.id,
                    "from": getattr(before.channel, "id", None),
                    "to": getattr(after.channel, "id", None),
                },
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(BotLogCog(bot))
