from __future__ import annotations
from typing import Optional, Dict, Tuple
from datetime import timedelta
import logging

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..utils.time import now_local, to_iso
from ..strings import S

log = logging.getLogger(__name__)


RULE_CHOICES = [
    "Respect Everyone",
    "Advertising & Self-promo",
    "Sources & Spoilers",
    "NSFW Content",
    "Politics-Free Zone",
    "Content & Posting",
    "AI Generated Content",
    "Roleplay",
    "Staff & Enforcement",
    "Other",
]

def _perm_ok(m: discord.Member) -> bool:
    p = m.guild_permissions
    return any([p.manage_guild, p.kick_members, p.ban_members, p.moderate_members])

def _color_for_temp(temp: int) -> discord.Color:
    # 1=gentle, 2=formal, 3=escalated, 4=critical
    return {
        1: discord.Color.teal(),
        2: discord.Color.orange(),
        3: discord.Color.purple(),
        4: discord.Color.red(),
    }.get(temp, discord.Color.blurple())

def _temp_label(temp: int) -> str:
    return {
        1: S("modlog.temp.gentle"),
        2: S("modlog.temp.formal"),
        3: S("modlog.temp.escalated"),
        4: S("modlog.temp.critical"),
    }.get(temp, S("modlog.temp.unknown", n=temp))


class ModLogCog(commands.Cog):
    """
    Moderation logging with temperature (1-4), optional timeout & ban.
    DMs are optional and OFF by default (dm_user=False).
    Can relay user DM replies back to the mod-logs channel while open.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # user_id -> (guild_id, modlog_channel_id)
        self._dm_relays: Dict[int, Tuple[int, int]] = {}


    @app_commands.command(
        name="modlog",
        description="Record a moderation action (temperature-based), optionally timeout and/or ban."
    )
    @app_commands.describe(
        user="User who broke the rules",
        rule="Which rule was involved",
        temperature="1=Gentle, 2=Formal, 3=Escalated, 4=Critical",
        reason="Short reason to show to the user",
        details="Optional detailed context",
        evidence="Optional image/screenshot",
        timeout_minutes="Optional timeout (minutes, up to ~28 days)",
        ban="Ban the user (yes/no)",
        dm_user="Attempt to DM the user (OFF by default)",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(
        rule=[app_commands.Choice(name=r, value=r) for r in RULE_CHOICES]
    )
    async def modlog_add(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        rule: app_commands.Choice[str],
        temperature: app_commands.Range[int, 1, 4],
        reason: str,
        details: Optional[str] = None,
        evidence: Optional[discord.Attachment] = None,
        timeout_minutes: Optional[app_commands.Range[int, 1, 40320]] = None,  # up to ~28 days
        ban: Optional[bool] = False,
        dm_user: bool = False,
        post: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        if not _perm_ok(interaction.user):
            return await interaction.response.send_message(S("modlog.err.perms"), ephemeral=True)

        # Resolve mod-log channel
        channel_id = models.get_mod_logs_channel(interaction.guild_id)
        if not channel_id:
            return await interaction.response.send_message(S("modlog.err.no_channel"), ephemeral=True)
        ch = interaction.guild.get_channel(channel_id)
        if not isinstance(ch, discord.TextChannel):
            return await interaction.response.send_message(S("modlog.err.bad_channel"), ephemeral=True)

        await interaction.response.defer(ephemeral=not post, thinking=True)

        # Optional enforcement
        actions_taken: list[str] = []
        evidence_url: Optional[str] = None

        # timeout
        if timeout_minutes and timeout_minutes > 0:
            if not interaction.user.guild_permissions.moderate_members:
                actions_taken.append(S("modlog.action.timeout.denied_perm", m=int(timeout_minutes)))
            else:
                try:
                    until = discord.utils.utcnow() + timedelta(minutes=int(timeout_minutes))
                    await user.timeout(until, reason=reason or S("modlog.reason.timeout_default"))
                    actions_taken.append(S("modlog.action.timeout.ok", m=int(timeout_minutes)))
                except discord.Forbidden:
                    actions_taken.append(S("modlog.action.timeout.forbidden", m=int(timeout_minutes)))
                except discord.HTTPException as e:
                    actions_taken.append(S("modlog.action.timeout.http", m=int(timeout_minutes), err=e))

        # ban
        if ban:
            if not interaction.user.guild_permissions.ban_members:
                actions_taken.append(S("modlog.action.ban.denied_perm"))
            else:
                try:
                    await interaction.guild.ban(user, reason=reason or S("modlog.reason.ban_default"), delete_message_days=0)
                    actions_taken.append(S("modlog.action.ban.ok"))
                except discord.Forbidden:
                    actions_taken.append(S("modlog.action.ban.forbidden"))
                except discord.HTTPException as e:
                    actions_taken.append(S("modlog.action.ban.http", err=e))

        # evidence image
        if evidence and evidence.content_type and evidence.content_type.startswith("image/"):
            evidence_url = evidence.url

        # Compose moderation embed for mod channel
        temp_i = int(temperature)
        mod_embed = discord.Embed(
            title=S("modlog.embed.title", temp=_temp_label(temp_i)),
            color=_color_for_temp(temp_i),
            timestamp=now_local(),
        )
        mod_embed.add_field(name=S("modlog.embed.user"), value=f"{user.mention} (`{user.id}`)", inline=False)
        mod_embed.add_field(name=S("modlog.embed.rule"), value=rule.value, inline=True)
        mod_embed.add_field(name=S("modlog.embed.temperature"), value=str(temp_i), inline=True)
        mod_embed.add_field(name=S("modlog.embed.reason"), value=reason[:1000], inline=False)
        if details:
            mod_embed.add_field(name=S("modlog.embed.details"), value=details[:1000], inline=False)
        if actions_taken:
            mod_embed.add_field(name=S("modlog.embed.actions"), value="\n".join(actions_taken)[:1000], inline=False)
        mod_embed.set_footer(text=S("modlog.embed.footer", actor=str(interaction.user), actor_id=interaction.user.id))
        if evidence_url:
            mod_embed.set_image(url=evidence_url)

        try:
            await ch.send(embed=mod_embed, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            log.exception("modlog.post_failed", extra={"guild_id": interaction.guild_id, "channel_id": ch.id})

        # Persist (store temperature in 'offense')
        try:
            models.add_mod_action(
                guild_id=interaction.guild_id,
                target_user_id=user.id,
                target_username=str(user),
                rule=rule.value,
                offense=int(temperature),
                action=("ban" if ban else ("timeout" if timeout_minutes else "warning")),
                details=(details or ""),
                evidence_url=evidence_url or "",
                actor_user_id=interaction.user.id,
                created_at=to_iso(now_local()),
            )
        except Exception:
            log.exception("modlog.persist_failed", extra={"guild_id": interaction.guild_id})

        # DM the user (optional; OFF by default)
        if dm_user:
            dm_embed = discord.Embed(
                title=S("modlog.dm.title"),
                description=_temp_label(temp_i),
                color=_color_for_temp(temp_i),
            )
            dm_embed.add_field(name=S("modlog.dm.rule"), value=rule.value, inline=True)
            dm_embed.add_field(name=S("modlog.dm.status"), value=S("modlog.dm.status_open"), inline=False)
            dm_embed.add_field(name=S("modlog.dm.reason"), value=reason[:1000] if reason else "—", inline=False)
            if details:
                dm_embed.add_field(name=S("modlog.dm.detail"), value=details[:1000], inline=False)
            dm_embed.add_field(
                name=S("modlog.dm.actions"),
                value=("\n".join(actions_taken) if actions_taken else S("modlog.dm.actions_warning")),
                inline=False
            )
            try:
                await user.send(embed=dm_embed)
                # Register relay so replies go to modlog channel
                self._dm_relays[user.id] = (interaction.guild_id, ch.id)
            except Exception:
                await ch.send(S("modlog.dm.could_not_dm", user=user.mention), allowed_mentions=discord.AllowedMentions.none())

        await interaction.followup.send(S("modlog.done"), ephemeral=not post)

        log.info(
            "modlog.add.used",
            extra={
                "guild_id": interaction.guild_id,
                "channel_id": ch.id,
                "actor_id": interaction.user.id,
                "target_id": user.id,
                "rule": rule.value,
                "temp": temp_i,
                "timeout_m": int(timeout_minutes) if timeout_minutes else 0,
                "ban": bool(ban),
                "dm_user": bool(dm_user),
                "post": bool(post),
                "has_evidence": bool(evidence_url),
            },
        )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Only handle DMs from users with an active relay
        if message.guild is not None or message.author.bot:
            return

        relay = self._dm_relays.get(message.author.id)
        if not relay:
            return  # no open conversation

        guild_id, modlog_channel_id = relay
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return

        channel = guild.get_channel(modlog_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return

        emb = discord.Embed(
            title=S("modlog.relay.title"),
            description=(message.content[:2000] if message.content else " "),
            color=discord.Color.blurple(),
            timestamp=now_local(),
        )
        emb.set_footer(text=S("modlog.relay.footer", author=str(message.author), author_id=message.author.id))
        if message.attachments:
            links = "\n".join(a.url for a in message.attachments)
            emb.add_field(name=S("modlog.relay.attachments"), value=links[:1000], inline=False)

        try:
            await channel.send(embed=emb, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            log.exception("modlog.relay.post_failed", extra={"guild_id": guild_id, "channel_id": modlog_channel_id})

    @app_commands.command(name="modlog_close_dm", description="Stop relaying DM replies from a user to the modlog channel.")
    @app_commands.describe(user="User to stop relaying", post="If true, post publicly in this channel")
    async def modlog_close_dm(self, interaction: discord.Interaction, user: discord.Member, post: bool = False):
        if not _perm_ok(interaction.user):
            return await interaction.response.send_message(S("modlog.err.perms"), ephemeral=True)

        await interaction.response.defer(ephemeral=not post, thinking=False)
        self._dm_relays.pop(user.id, None)
        await interaction.followup.send(S("modlog.relay.closed", user=user.mention), ephemeral=not post)

        log.info(
            "modlog.relay.closed",
            extra={
                "guild_id": getattr(interaction, "guild_id", None),
                "actor_id": interaction.user.id,
                "user_id": user.id,
                "post": post,
            },
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(ModLogCog(bot))
