from __future__ import annotations
import asyncio
import logging
from typing import Optional, Iterable
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

def _chan(guild: discord.Guild, channel_id: int) -> Optional[discord.abc.GuildChannel]:
    return guild.get_channel(channel_id)

def _botlog_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    ch_id = models.get_bot_logs_channel(guild.id)
    ch = _chan(guild, ch_id) if ch_id else None
    return ch if isinstance(ch, discord.TextChannel) else None

def _embed(title_key: str, color: discord.Color) -> discord.Embed:
    return discord.Embed(
        title=S(title_key),
        color=color,
        timestamp=datetime.now(tz=LOCAL_TZ),
    )

async def _post(guild: discord.Guild, embed: discord.Embed):
    ch = _botlog_channel(guild)
    if ch:
        try:
            await ch.send(embed=embed)
        except Exception as e:
            log.warning("Failed to post botlog: %s", e)

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
        self._invite_cache: dict[int, list[discord.Invite]] = {}  # guild_id -> invites

    async def _refresh_invites(self, guild: discord.Guild):
        try:
            invites = await guild.invites()
            self._invite_cache[guild.id] = invites
        except Exception:
            self._invite_cache[guild.id] = []

    @commands.Cog.listener()
    async def on_ready(self):
        # warm caches for invites
        await asyncio.gather(*[self._refresh_invites(g) for g in self.bot.guilds])

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        await self._refresh_invites(guild)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message):
        if not message.guild:
            return
        if message.author.id in IGNORED_USER_IDS:
            return
        emb = _embed("botlog.title.message_deleted", discord.Color.orange())
        author = getattr(message, "author", None)
        if isinstance(author, discord.Member):
            emb.add_field(name=S("botlog.field.author"), value=f"{author.mention} (`{author.id}`)", inline=True)
        ch = getattr(message, "channel", None)
        if isinstance(ch, discord.TextChannel):
            emb.add_field(name=S("botlog.field.channel"), value=f"{ch.mention} (`{ch.id}`)", inline=True)
        content = getattr(message, "content", None)
        if content:
            emb.add_field(name=S("botlog.field.content"), value=content[:1024], inline=False)
        if getattr(message, "attachments", None):
            att = "\n".join([a.filename for a in message.attachments])[:1024]
            emb.add_field(name=S("botlog.field.deleted_attachments"), value=att, inline=False)
        await _post(message.guild, emb)

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
        emb = _embed(S("botlog.title.message_edited"), discord.Color.yellow())
        emb.add_field(
            name=S("botlog.field.author"),
            value=f"{author.mention} (`{author.id}`)",
            inline=True,
        )
        channel = getattr(msg, "channel", None)
        if isinstance(channel, discord.TextChannel):
            emb.add_field(
                name=S("botlog.field.channel"),
                value=f"{channel.mention} (`{channel.id}`)",
                inline=True,
            )
        if before_content:
            emb.add_field(name=S("botlog.field.before"), value=before_content[:1024], inline=False)
        if after_content:
            emb.add_field(name=S("botlog.field.after"), value=after_content[:1024], inline=False)
        await _post(guild, emb)

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        guild = self.bot.get_guild(payload.guild_id) if payload.guild_id else None
        if not guild:
            return
        ch = _chan(guild, payload.channel_id)
        emb = _embed("botlog.title.bulk_delete", discord.Color.dark_orange())
        emb.add_field(name=S("botlog.field.channel"), value=_channel_ref(ch), inline=True)
        emb.add_field(name=S("botlog.field.count"), value=str(len(payload.message_ids)), inline=True)
        await _post(guild, emb)

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):
        if not invite.guild:
            return
        emb = _embed("botlog.title.invite_created", discord.Color.blue())
        emb.add_field(name=S("botlog.field.code"), value=invite.code, inline=True)
        if invite.inviter:
            emb.add_field(name=S("botlog.field.inviter"), value=f"{invite.inviter.mention} (`{invite.inviter.id}`)", inline=True)
        if invite.channel:
            emb.add_field(name=S("botlog.field.channel"), value=_channel_ref(invite.channel), inline=True)
        if invite.max_uses:
            emb.add_field(name=S("botlog.field.max_uses"), value=str(invite.max_uses), inline=True)
        if invite.max_age:
            emb.add_field(name=S("botlog.field.max_age_seconds"), value=str(invite.max_age), inline=True)
        await _post(invite.guild, emb)
        await self._refresh_invites(invite.guild)

    @commands.Cog.listener()
    async def on_invite_delete(self, invite: discord.Invite):
        if not invite.guild:
            return
        emb = _embed("botlog.title.invite_deleted", discord.Color.dark_blue())
        emb.add_field(name=S("botlog.field.code"), value=invite.code, inline=True)
        if invite.channel:
            emb.add_field(name=S("botlog.field.channel"), value=_channel_ref(invite.channel), inline=True)
        await _post(invite.guild, emb)
        await self._refresh_invites(invite.guild)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        emb = _embed("botlog.title.member_join", discord.Color.green())
        emb.add_field(name=S("botlog.field.user"), value=f"{member.mention} (`{member.id}`)", inline=False)
        await _post(member.guild, emb)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        emb = _embed("botlog.title.member_leave", discord.Color.orange())
        emb.add_field(name=S("botlog.field.user"), value=f"{member} (`{member.id}`)", inline=False)
        await _post(member.guild, emb)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        # Nickname change
        if before.nick != after.nick:
            emb = _embed("botlog.title.nick_change", discord.Color.blurple())
            emb.add_field(name=S("botlog.field.user"), value=f"{after.mention} (`{after.id}`)", inline=False)
            emb.add_field(name=S("botlog.field.before"), value=before.nick or S("botlog.common.none"), inline=True)
            emb.add_field(name=S("botlog.field.after"), value=after.nick or S("botlog.common.none"), inline=True)
            await _post(after.guild, emb)

        # Roles added/removed
        broles = set(before.roles); aroles = set(after.roles)
        added = aroles - broles
        removed = broles - aroles
        if added or removed:
            emb = _embed("botlog.title.member_roles_updated", discord.Color.teal())
            emb.add_field(name=S("botlog.field.user"), value=f"{after.mention} (`{after.id}`)", inline=False)
            if added:
                emb.add_field(name=S("botlog.field.roles_added"), value=_format_roles(added)[:1024], inline=False)
            if removed:
                emb.add_field(name=S("botlog.field.roles_removed"), value=_format_roles(removed)[:1024], inline=False)
            await _post(after.guild, emb)

        # Timeout change
        b_to = getattr(before, "communication_disabled_until", None)
        a_to = getattr(after, "communication_disabled_until", None)
        if b_to != a_to:
            emb = _embed("botlog.title.timeout_updated", discord.Color.dark_teal())
            emb.add_field(name=S("botlog.field.user"), value=f"{after.mention} (`{after.id}`)", inline=False)
            emb.add_field(name=S("botlog.field.before"), value=str(b_to) if b_to else S("botlog.common.none"), inline=True)
            emb.add_field(name=S("botlog.field.after"), value=str(a_to) if a_to else S("botlog.common.none"), inline=True)
            await _post(after.guild, emb)

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.User):
        emb = _embed("botlog.title.member_banned", discord.Color.red())
        emb.add_field(name=S("botlog.field.user"), value=f"{user} (`{user.id}`)", inline=False)
        await _post(guild, emb)

    @commands.Cog.listener()
    async def on_member_unban(self, guild: discord.Guild, user: discord.User):
        emb = _embed("botlog.title.member_unbanned", discord.Color.dark_red())
        emb.add_field(name=S("botlog.field.user"), value=f"{user} (`{user.id}`)", inline=False)
        await _post(guild, emb)

    @commands.Cog.listener()
    async def on_guild_role_create(self, role: discord.Role):
        emb = _embed("botlog.title.role_created", discord.Color.green())
        emb.add_field(name=S("botlog.field.role"), value=f"{role.mention} (`{role.id}`)", inline=False)
        await _post(role.guild, emb)

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        emb = _embed("botlog.title.role_deleted", discord.Color.orange())
        emb.add_field(name=S("botlog.field.role"), value=f"{role.name} (`{role.id}`)", inline=False)
        await _post(role.guild, emb)

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        emb = _embed("botlog.title.role_updated", discord.Color.yellow())
        emb.add_field(name=S("botlog.field.role"), value=f"{after.mention} (`{after.id}`)", inline=False)
        changes = []
        if before.name != after.name:
            changes.append(S("botlog.change.role_name", before=before.name, after=after.name))
        if before.color != after.color:
            changes.append(S("botlog.change.role_color", before=before.color, after=after.color))
        if before.permissions.value != after.permissions.value:
            changes.append(S("botlog.change.role_perms"))
        if changes:
            emb.add_field(name=S("botlog.field.changes"), value="\n".join(changes)[:1024], inline=False)
            await _post(after.guild, emb)

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        emb = _embed("botlog.title.channel_created", discord.Color.green())
        emb.add_field(name=S("botlog.field.channel"), value=_channel_ref(channel), inline=False)
        await _post(channel.guild, emb)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        emb = _embed("botlog.title.channel_deleted", discord.Color.orange())
        emb.add_field(name=S("botlog.field.channel"), value=f"{getattr(channel, 'name', 'channel')} (`{channel.id}`)", inline=False)
        await _post(channel.guild, emb)

    @commands.Cog.listener()
    async def on_guild_channel_update(self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel):
        emb = _embed("botlog.title.channel_updated", discord.Color.yellow())
        emb.add_field(name=S("botlog.field.channel"), value=_channel_ref(after), inline=False)
        changes = []
        if getattr(before, "name", None) != getattr(after, "name", None):
            changes.append(S("botlog.change.channel_name", before=getattr(before,'name',None), after=getattr(after,'name',None)))
        if getattr(before, "topic", None) != getattr(after, "topic", None):
            changes.append(S("botlog.change.channel_topic"))
        if getattr(before, "nsfw", None) != getattr(after, "nsfw", None):
            changes.append(S("botlog.change.channel_nsfw", before=getattr(before,'nsfw',None), after=getattr(after,'nsfw',None)))
        if changes:
            emb.add_field(name=S("botlog.field.changes"), value="\n".join(changes)[:1024], inline=False)
            await _post(after.guild, emb)

    @commands.Cog.listener()
    async def on_guild_emojis_update(self, guild: discord.Guild, before: list[discord.Emoji], after: list[discord.Emoji]):
        before_map = {e.id: e for e in before}
        after_map = {e.id: e for e in after}
        created = [e for e in after if e.id not in before_map]
        deleted = [e for e in before if e.id not in after_map]
        renamed = [e for e in after if e.id in before_map and e.name != before_map[e.id].name]

        if created:
            emb = _embed("botlog.title.emoji_created", discord.Color.green())
            emb.add_field(name=S("botlog.field.emojis"), value=", ".join([f":{e.name}:" for e in created])[:1024], inline=False)
            await _post(guild, emb)
        if deleted:
            emb = _embed("botlog.title.emoji_deleted", discord.Color.orange())
            emb.add_field(name=S("botlog.field.emojis"), value=", ".join([f":{e.name}:" for e in deleted])[:1024], inline=False)
            await _post(guild, emb)
        if renamed:
            emb = _embed("botlog.title.emoji_renamed", discord.Color.yellow())
            lines = [f":{before_map[e.id].name}: → :{e.name}:" for e in renamed]
            emb.add_field(name=S("botlog.field.changes"), value="\n".join(lines)[:1024], inline=False)
            await _post(guild, emb)

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        # Join
        if before.channel is None and after.channel is not None:
            emb = _embed("botlog.title.voice_join", discord.Color.green())
            emb.add_field(name=S("botlog.field.user"), value=f"{member.mention} (`{member.id}`)", inline=True)
            emb.add_field(name=S("botlog.field.channel"), value=_channel_ref(after.channel), inline=True)
            await _post(member.guild, emb)
        # Leave
        elif before.channel is not None and after.channel is None:
            emb = _embed("botlog.title.voice_leave", discord.Color.orange())
            emb.add_field(name=S("botlog.field.user"), value=f"{member.mention} (`{member.id}`)", inline=True)
            emb.add_field(name=S("botlog.field.channel"), value=_channel_ref(before.channel), inline=True)
            await _post(member.guild, emb)
        # Move
        elif before.channel is not None and after.channel is not None and before.channel.id != after.channel.id:
            emb = _embed("botlog.title.voice_move", discord.Color.blurple())
            emb.add_field(name=S("botlog.field.user"), value=f"{member.mention} (`{member.id}`)", inline=False)
            emb.add_field(name=S("botlog.field.from"), value=_channel_ref(before.channel), inline=True)
            emb.add_field(name=S("botlog.field.to"), value=_channel_ref(after.channel), inline=True)
            await _post(member.guild, emb)

async def setup(bot: commands.Bot):
    await bot.add_cog(BotLogCog(bot))
