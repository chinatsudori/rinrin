from __future__ import annotations
import asyncio
import logging
import time
from typing import Optional, Iterable, Dict, List, Tuple
from datetime import datetime

import discord
from discord.ext import commands

from .. import models
from ..config import LOCAL_TZ
from ..strings import S

IGNORED_USER_IDS: set[int] = {
    1211781489931452447,  # Wordle
}

log = logging.getLogger(__name__)


def _chan(guild: discord.Guild, channel_id: int | None) -> Optional[discord.abc.GuildChannel]:
    if not channel_id:
        return None
    return guild.get_channel(channel_id)

def _embed(title_key: str, color: discord.Color) -> discord.Embed:
    return discord.Embed(
        title=S(title_key),
        color=color,
        timestamp=datetime.now(tz=LOCAL_TZ),
    )

def _safe_add_field(emb: discord.Embed, *, name_key: str, value: str | None, inline: bool):
    if not value:
        return
    # Discord field max is 1024 chars
    emb.add_field(name=S(name_key), value=value[:1024], inline=inline)

def _format_roles(roles: Iterable[discord.Role]) -> str:
    items = [r.mention for r in roles if not r.is_default()]
    return ", ".join(items) if items else S("botlog.common.none")

def _channel_ref(ch: discord.abc.GuildChannel | None) -> str:
    if ch is None:
        return S("botlog.common.unknown")
    if isinstance(ch, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.ForumChannel)):
        return f"{ch.mention} (`{ch.id}`)"
    return f"{getattr(ch, 'name', 'channel')} (`{ch.id}`)"


class BotLogCog(commands.Cog):
    """Server audit/bot log; posts structured embeds to a configured channel."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._invite_cache: Dict[int, List[discord.Invite]] = {}  # guild_id -> invites
        # TTL cache for bot-log channel id: guild_id -> (channel_id, fetched_at)
        self._botlog_cache: Dict[int, Tuple[Optional[int], float]] = {}
        self._botlog_ttl_seconds = 60.0


    def _get_botlog_channel_id(self, guild_id: int) -> Optional[int]:
        now = time.monotonic()
        cached = self._botlog_cache.get(guild_id)
        if cached and (now - cached[1] < self._botlog_ttl_seconds):
            return cached[0]
        try:
            ch_id = models.get_bot_logs_channel(guild_id)
            self._botlog_cache[guild_id] = (ch_id, now)
            return ch_id
        except Exception:
            log.exception("botlog.lookup_failed", extra={"guild_id": guild_id})
            self._botlog_cache[guild_id] = (None, now)
            return None


    async def _post(self, guild: discord.Guild, embed: discord.Embed):
        ch_id = self._get_botlog_channel_id(guild.id)
        ch = _chan(guild, ch_id)
        if not isinstance(ch, discord.TextChannel):
            log.debug("botlog.skip_no_channel", extra={"guild_id": guild.id, "channel_id": ch_id})
            return

        for attempt in (1, 2):
            try:
                await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                # log.info("botlog.posted", extra={"guild_id": guild.id, "channel_id": ch.id, "title": embed.title})
                return
            except discord.Forbidden as e:
                log.warning(
                    "botlog.forbidden",
                    extra={"guild_id": guild.id, "channel_id": ch.id, "attempt": attempt, "error": str(e)},
                )
                return  # no retry on perms
            except Exception as e:
                log.warning(
                    "botlog.post_failed",
                    extra={"guild_id": guild.id, "channel_id": getattr(ch, 'id', None), "attempt": attempt, "error": str(e)},
                )
                if attempt == 1:
                    await asyncio.sleep(0.4)


    async def _refresh_invites(self, guild: discord.Guild):
        try:
            invites = await guild.invites()
            self._invite_cache[guild.id] = invites
            log.info("botlog.invites_refreshed", extra={"guild_id": guild.id, "count": len(invites)})
        except Exception as e:
            self._invite_cache[guild.id] = []
            log.warning("botlog.invites_refresh_failed", extra={"guild_id": guild.id, "error": str(e)})


    @commands.Cog.listener()
    async def on_ready(self):
        # Warm invite caches; don't fail the event loop if gather errors.
        tasks = [self._refresh_invites(g) for g in self.bot.guilds]
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                # Extremely defensive; gather with return_exceptions should prevent this.
                log.exception("botlog.on_ready_invite_warm_failed")

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        await self._refresh_invites(guild)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if not message.guild:
            return
        if message.author and message.author.id in IGNORED_USER_IDS:
            return
        emb = _embed("botlog.title.message_deleted", discord.Color.orange())
        author = getattr(message, "author", None)
        if isinstance(author, discord.Member):
            _safe_add_field(emb, name_key="botlog.field.author", value=f"{author.mention} (`{author.id}`)", inline=True)
        ch = getattr(message, "channel", None)
        if isinstance(ch, discord.TextChannel):
            _safe_add_field(emb, name_key="botlog.field.channel", value=f"{ch.mention} (`{ch.id}`)", inline=True)
        content = getattr(message, "content", None)
        _safe_add_field(emb, name_key="botlog.field.content", value=content, inline=False)
        if getattr(message, "attachments", None):
            att = "\n".join([a.filename for a in message.attachments])
            _safe_add_field(emb, name_key="botlog.field.deleted_attachments", value=att, inline=False)
        await self._post(message.guild, emb)
        log.info("botlog.event.message_delete", extra={"guild_id": message.guild.id, "channel_id": getattr(ch, "id", None)})

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        msg = after or before
        guild = getattr(msg, "guild", None)
        if not guild:
            return
        author = getattr(msg, "author", None)
        if not author:
            return
        if getattr(author, "id", None) in IGNORED_USER_IDS:
            return
        before_content = (getattr(before, "content", None) or "").strip()
        after_content  = (getattr(after, "content", None) or "").strip()
        before_atts = tuple(getattr(before, "attachments", []) or [])
        after_atts  = tuple(getattr(after, "attachments", []) or [])
        if before_content == after_content and len(before_atts) == len(after_atts):
            return
        emb = _embed("botlog.title.message_edited", discord.Color.yellow())
        _safe_add_field(emb, name_key="botlog.field.author", value=f"{author.mention} (`{author.id}`)", inline=True)
        channel = getattr(msg, "channel", None)
        if isinstance(channel, discord.TextChannel):
            _safe_add_field(emb, name_key="botlog.field.channel", value=f"{channel.mention} (`{channel.id}`)", inline=True)
        _safe_add_field(emb, name_key="botlog.field.before", value=before_content, inline=False)
        _safe_add_field(emb, name_key="botlog.field.after", value=after_content, inline=False)
        await self._post(guild, emb)
        log.info("botlog.event.message_edit", extra={"guild_id": guild.id, "channel_id": getattr(channel, "id", None)})

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        guild = self.bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        ch = _chan(guild, payload.channel_id)
        emb = _embed("botlog.title.bulk_delete", discord.Color.dark_orange())
        _safe_add_field(emb, name_key="botlog.field.channel", value=_channel_ref(ch), inline=True)
        _safe_add_field(emb, name_key="botlog.field.count", value=str(len(payload.message_ids)), inline=True)
        await self._post(guild, emb)
        log.info("botlog.event.bulk_delete", extra={"guild_id": guild.id, "channel_id": getattr(ch, "id", None), "count": len(payload.message_ids)})

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):
        if not invite.guild:
            return
        emb = _embed("botlog.title.invite_created", discord.Color.blue())
        _safe_add_field(emb, name_key="botlog.field.code", value=invite.code, inline=True)
        if invite.inviter:
            _safe_add_field(emb, name_key="botlog.field.inviter", value=f"{invite.inviter.mention} (`{invite.inviter.id}`)", inline=True)
        if invite.channel:
            _safe_add_field(emb, name_key="botlog.field.channel", value=_channel_ref(invite.channel), inline=True)
        if invite.max_uses:
            _safe_add_field(emb, name_key="botlog.field.max_uses", value=str(invite.max_uses), inline=True)
        if invite.max_age:
            _safe_add_field(emb, name_key="botlog.field.max_age_seconds", value=str(invite.max_age), inline=True)
        await self._post(invite.guild, emb)
        await self._refresh_invites(invite.guild)
        log.info("botlog.event.invite_create", extra={"guild_id": invite.guild.id, "channel_id": getattr(invite.channel, "id", None)})

    @commands.Cog.listener()
    async def on_invite_delete(self, invite: discord.Invite):
        if not invite.guild:
            return
        emb = _embed("botlog.title.invite_deleted", discord.Color.dark_blue())
        _safe_add_field(emb, name_key="botlog.field.code", value=invite.code, inline=True)
        if invite.channel:
            _safe_add_field(emb, name_key="botlog.field.channel", value=_channel_ref(invite.channel), inline=True)
        await self._post(invite.guild, emb)
        await self._refresh_invites(invite.guild)
        log.info("botlog.event.invite_delete", extra={"guild_id": invite.guild.id, "channel_id": getattr(invite.channel, "id", None)})

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        emb = _embed("botlog.title.member_join", discord.Color.green())
        _safe_add_field(emb, name_key="botlog.field.user", value=f"{member.mention} (`{member.id}`)", inline=False)
        await self._post(member.guild, emb)
        log.info("botlog.event.member_join", extra={"guild_id": member.guild.id, "user_id": member.id})

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        emb = _embed("botlog.title.member_leave", discord.Color.orange())
        _safe_add_field(emb, name_key="botlog.field.user", value=f"{member} (`{member.id}`)", inline=False)
        await self._post(member.guild, emb)
        log.info("botlog.event.member_remove", extra={"guild_id": member.guild.id, "user_id": member.id})

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        # Nickname change
        if before.nick != after.nick:
            emb = _embed("botlog.title.nick_change", discord.Color.blurple())
            _safe_add_field(emb, name_key="botlog.field.user", value=f"{after.mention} (`{after.id}`)", inline=False)
            _safe_add_field(emb, name_key="botlog.field.before", value=before.nick or S("botlog.common.none"), inline=True)
            _safe_add_field(emb, name_key="botlog.field.after", value=after.nick or S("botlog.common.none"), inline=True)
            await self._post(after.guild, emb)
            log.info("botlog.event.nick_change", extra={"guild_id": after.guild.id, "user_id": after.id})

        # Roles added/removed
        broles = set(before.roles); aroles = set(after.roles)
        added = aroles - broles
        removed = broles - aroles
        if added or removed:
            emb = _embed("botlog.title.member_roles_updated", discord.Color.teal())
            _safe_add_field(emb, name_key="botlog.field.user", value=f"{after.mention} (`{after.id}`)", inline=False)
            if added:
                _safe_add_field(emb, name_key="botlog.field.roles_added", value=_format_roles(added), inline=False)
            if removed:
                _safe_add_field(emb, name_key="botlog.field.roles_removed", value=_format_roles(removed), inline=False)
            await self._post(after.guild, emb)
            log.info("botlog.event.roles_updated", extra={"guild_id": after.guild.id, "user_id": after.id, "added": len(added), "removed": len(removed)})

        # Timeout change
        b_to = getattr(before, "communication_disabled_until", None)
        a_to = getattr(after, "communication_disabled_until", None)
        if b_to != a_to:
            emb = _embed("botlog.title.timeout_updated", discord.Color.dark_teal())
            _safe_add_field(emb, name_key="botlog.field.user", value=f"{after.mention} (`{after.id}`)", inline=False)
            _safe_add_field(emb, name_key="botlog.field.before", value=str(b_to) if b_to else S("botlog.common.none"), inline=True)
            _safe_add_field(emb, name_key="botlog.field.after", value=str(a_to) if a_to else S("botlog.common.none"), inline=True)
            await self._post(after.guild, emb)
            log.info("botlog.event.timeout_updated", extra={"guild_id": after.guild.id, "user_id": after.id})

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        emb = _embed("botlog.title.member_banned", discord.Color.red())
        _safe_add_field(emb, name_key="botlog.field.user", value=f"{user} (`{user.id}`)", inline=False)
        await self._post(guild, emb)
        log.info("botlog.event.member_banned", extra={"guild_id": guild.id, "user_id": user.id})

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        emb = _embed("botlog.title.member_unbanned", discord.Color.dark_red())
        _safe_add_field(emb, name_key="botlog.field.user", value=f"{user} (`{user.id}`)", inline=False)
        await self._post(guild, emb)
        log.info("botlog.event.member_unbanned", extra={"guild_id": guild.id, "user_id": user.id})

    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        emb = _embed("botlog.title.role_created", discord.Color.green())
        _safe_add_field(emb, name_key="botlog.field.role", value=f"{role.mention} (`{role.id}`)", inline=False)
        await self._post(role.guild, emb)
        log.info("botlog.event.role_created", extra={"guild_id": role.guild.id, "role_id": role.id})

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        emb = _embed("botlog.title.role_deleted", discord.Color.orange())
        _safe_add_field(emb, name_key="botlog.field.role", value=f"{role.name} (`{role.id}`)", inline=False)
        await self._post(role.guild, emb)
        log.info("botlog.event.role_deleted", extra={"guild_id": role.guild.id, "role_id": role.id})

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        emb = _embed("botlog.title.role_updated", discord.Color.yellow())
        _safe_add_field(emb, name_key="botlog.field.role", value=f"{after.mention} (`{after.id}`)", inline=False)
        changes = []
        if before.name != after.name:
            changes.append(S("botlog.change.role_name", before=before.name, after=after.name))
        if before.color != after.color:
            changes.append(S("botlog.change.role_color", before=before.color, after=after.color))
        if before.permissions.value != after.permissions.value:
            changes.append(S("botlog.change.role_perms"))
        if changes:
            _safe_add_field(emb, name_key="botlog.field.changes", value="\n".join(changes), inline=False)
            await self._post(after.guild, emb)
            log.info("botlog.event.role_updated", extra={"guild_id": after.guild.id, "role_id": after.id, "changes": len(changes)})

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        emb = _embed("botlog.title.channel_created", discord.Color.green())
        _safe_add_field(emb, name_key="botlog.field.channel", value=_channel_ref(channel), inline=False)
        await self._post(channel.guild, emb)
        log.info("botlog.event.channel_created", extra={"guild_id": channel.guild.id, "channel_id": getattr(channel, 'id', None)})

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        emb = _embed("botlog.title.channel_deleted", discord.Color.orange())
        _safe_add_field(emb, name_key="botlog.field.channel", value=f"{getattr(channel, 'name', 'channel')} (`{channel.id}`)", inline=False)
        await self._post(channel.guild, emb)
        log.info("botlog.event.channel_deleted", extra={"guild_id": channel.guild.id, "channel_id": getattr(channel, 'id', None)})

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
        emb = _embed("botlog.title.channel_updated", discord.Color.yellow())
        _safe_add_field(emb, name_key="botlog.field.channel", value=_channel_ref(after), inline=False)
        changes = []
        if getattr(before, "name", None) != getattr(after, "name", None):
            changes.append(S("botlog.change.channel_name", before=getattr(before,'name',None), after=getattr(after,'name',None)))
        if getattr(before, "topic", None) != getattr(after, "topic", None):
            changes.append(S("botlog.change.channel_topic"))
        if getattr(before, "nsfw", None) != getattr(after, "nsfw", None):
            changes.append(S("botlog.change.channel_nsfw", before=getattr(before,'nsfw',None), after=getattr(after,'nsfw',None)))
        if changes:
            _safe_add_field(emb, name_key="botlog.field.changes", value="\n".join(changes), inline=False)
            await self._post(after.guild, emb)
            log.info("botlog.event.channel_updated", extra={"guild_id": after.guild.id, "channel_id": getattr(after, 'id', None), "changes": len(changes)})

    @commands.Cog.listener()
    async def on_guild_emojis_update(self, guild: discord.Guild, before: list[discord.Emoji], after: list[discord.Emoji]):
        before_map = {e.id: e for e in before}
        after_map = {e.id: e for e in after}
        created = [e for e in after if e.id not in before_map]
        deleted = [e for e in before if e.id not in after_map]
        renamed = [e for e in after if e.id in before_map and e.name != before_map[e.id].name]

        if created:
            emb = _embed("botlog.title.emoji_created", discord.Color.green())
            _safe_add_field(emb, name_key="botlog.field.emojis", value=", ".join([f":{e.name}:" for e in created]), inline=False)
            await self._post(guild, emb)
        if deleted:
            emb = _embed("botlog.title.emoji_deleted", discord.Color.orange())
            _safe_add_field(emb, name_key="botlog.field.emojis", value=", ".join([f":{e.name}:" for e in deleted]), inline=False)
            await self._post(guild, emb)
        if renamed:
            emb = _embed("botlog.title.emoji_renamed", discord.Color.yellow())
            lines = [f":{before_map[e.id].name}: → :{e.name}:" for e in renamed]
            _safe_add_field(emb, name_key="botlog.field.changes", value="\n".join(lines), inline=False)
            await self._post(guild, emb)
        log.info("botlog.event.emojis_update", extra={"guild_id": guild.id, "created": len(created), "deleted": len(deleted), "renamed": len(renamed)})

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        # Join
        if before.channel is None and after.channel is not None:
            emb = _embed("botlog.title.voice_join", discord.Color.green())
            _safe_add_field(emb, name_key="botlog.field.user", value=f"{member.mention} (`{member.id}`)", inline=True)
            _safe_add_field(emb, name_key="botlog.field.channel", value=_channel_ref(after.channel), inline=True)
            await self._post(member.guild, emb)
            log.info("botlog.event.voice_join", extra={"guild_id": member.guild.id, "user_id": member.id, "channel_id": getattr(after.channel, 'id', None)})
        # Leave
        elif before.channel is not None and after.channel is None:
            emb = _embed("botlog.title.voice_leave", discord.Color.orange())
            _safe_add_field(emb, name_key="botlog.field.user", value=f"{member.mention} (`{member.id}`)", inline=True)
            _safe_add_field(emb, name_key="botlog.field.channel", value=_channel_ref(before.channel), inline=True)
            await self._post(member.guild, emb)
            log.info("botlog.event.voice_leave", extra={"guild_id": member.guild.id, "user_id": member.id, "channel_id": getattr(before.channel, 'id', None)})
        # Move
        elif before.channel is not None and after.channel is not None and before.channel.id != after.channel.id:
            emb = _embed("botlog.title.voice_move", discord.Color.blurple())
            _safe_add_field(emb, name_key="botlog.field.user", value=f"{member.mention} (`{member.id}`)", inline=False)
            _safe_add_field(emb, name_key="botlog.field.from", value=_channel_ref(before.channel), inline=True)
            _safe_add_field(emb, name_key="botlog.field.to", value=_channel_ref(after.channel), inline=True)
            await self._post(member.guild, emb)
            log.info("botlog.event.voice_move", extra={"guild_id": member.guild.id, "user_id": member.id, "from": getattr(before.channel, 'id', None), "to": getattr(after.channel, 'id', None)})

async def setup(bot: commands.Bot):
    await bot.add_cog(BotLogCog(bot))
