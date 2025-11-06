from __future__ import annotations

import logging
from datetime import timedelta
from typing import Dict, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from ..models import mod_actions, settings
from ..strings import S
from ..ui.modlog import build_dm_embed, build_modlog_embed, build_relay_embed
from ..utils.modlog import RULE_CHOICES, permission_ok
from ..utils.time import now_local, to_iso

log = logging.getLogger(__name__)


class ModLogCog(commands.Cog):
    """Moderation logging with optional timeout/ban and DM relays."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._dm_relays: Dict[int, Tuple[int, int]] = {}

    @app_commands.command(
        name="modlog",
        description="Record a moderation action (temperature-based), optionally timeout and/or ban.",
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
        post="If true, post publicly in this channel",
    )
    @app_commands.choices(rule=[app_commands.Choice(name=r, value=r) for r in RULE_CHOICES])
    async def modlog_add(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        rule: app_commands.Choice[str],
        temperature: app_commands.Range[int, 1, 4],
        reason: str,
        details: Optional[str] = None,
        evidence: Optional[discord.Attachment] = None,
        timeout_minutes: Optional[app_commands.Range[int, 1, 40320]] = None,
        ban: Optional[bool] = False,
        dm_user: bool = False,
        post: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        if not permission_ok(interaction.user):
            return await interaction.response.send_message(S("modlog.err.perms"), ephemeral=True)

        channel_id = settings.get_mod_logs_channel(interaction.guild_id)
        if not channel_id:
            return await interaction.response.send_message(S("modlog.err.no_channel"), ephemeral=True)
        channel = interaction.guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            return await interaction.response.send_message(S("modlog.err.bad_channel"), ephemeral=True)

        await interaction.response.defer(ephemeral=not post, thinking=True)

        temp_value = int(temperature)
        actions_taken: list[str] = []
        evidence_url: Optional[str] = None

        # Optional timeout
        if timeout_minutes and timeout_minutes > 0:
            if not interaction.user.guild_permissions.moderate_members:
                actions_taken.append(S("modlog.action.timeout.denied_perm", m=int(timeout_minutes)))
            else:
                try:
                    until = discord.utils.utcnow() + timedelta(minutes=int(timeout_minutes))
                    await user.timeout(until, reason=reason or S("modlog.reason.timeout_default"))
                    actions_taken.append(S("modlog.action.timeout.ok", m=int(timeout_minutes)))
                except discord.Forbidden:
                    actions_taken.append(S("modlog.action.timeout.forbidden"))
                except discord.HTTPException:
                    actions_taken.append(S("modlog.action.timeout.http"))

        # Optional ban
        if ban:
            if not interaction.user.guild_permissions.ban_members:
                actions_taken.append(S("modlog.action.ban.denied_perm"))
            else:
                try:
                    await interaction.guild.ban(user, reason=reason[:512] if reason else None, delete_message_days=0)
                    actions_taken.append(S("modlog.action.ban.ok"))
                except discord.Forbidden:
                    actions_taken.append(S("modlog.action.ban.forbidden"))
                except discord.HTTPException:
                    actions_taken.append(S("modlog.action.ban.http"))

        if evidence and evidence.content_type and evidence.content_type.startswith("image/"):
            evidence_url = evidence.url

        mod_embed = build_modlog_embed(
            user=user,
            rule=rule.value,
            temperature=temp_value,
            reason=reason,
            details=details,
            actions=actions_taken,
            actor=interaction.user,
            evidence_url=evidence_url,
        )
        try:
            await channel.send(embed=mod_embed, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            log.exception(
                "modlog.post_failed",
                extra={"guild_id": interaction.guild_id, "channel_id": channel.id},
            )

        try:
            mod_actions.add_mod_action(
                guild_id=interaction.guild_id,
                target_user_id=user.id,
                target_username=str(user),
                rule=rule.value,
                offense=temp_value,
                action=("ban" if ban else ("timeout" if timeout_minutes else "warning")),
                details=(details or ""),
                evidence_url=evidence_url or "",
                actor_user_id=interaction.user.id,
                created_at=to_iso(now_local()),
            )
        except Exception:
            log.exception("modlog.persist_failed", extra={"guild_id": interaction.guild_id})

        if dm_user:
            dm_embed = build_dm_embed(
                user=user,
                rule=rule.value,
                temperature=temp_value,
                reason=reason,
                details=details,
                actions=actions_taken,
            )
            try:
                await user.send(embed=dm_embed)
                self._dm_relays[user.id] = (interaction.guild_id, channel.id)
            except Exception:
                await channel.send(
                    S("modlog.dm.could_not_dm", user=user.mention),
                    allowed_mentions=discord.AllowedMentions.none(),
                )

        await interaction.followup.send(S("modlog.done"), ephemeral=not post)

        log.info(
            "modlog.add.used",
            extra={
                "guild_id": interaction.guild_id,
                "channel_id": channel.id,
                "actor_id": interaction.user.id,
                "target_id": user.id,
                "rule": rule.value,
                "temp": temp_value,
                "timeout_m": int(timeout_minutes) if timeout_minutes else 0,
                "ban": bool(ban),
                "dm_user": bool(dm_user),
                "post": bool(post),
                "has_evidence": bool(evidence_url),
            },
        )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.guild is not None or message.author.bot:
            return
        relay = self._dm_relays.get(message.author.id)
        if not relay:
            return
        guild_id, channel_id = relay
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        channel = guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            return

        embed = build_relay_embed(message)
        try:
            await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            log.exception("modlog.relay.post_failed", extra={"guild_id": guild_id, "channel_id": channel_id})

    @app_commands.command(
        name="modlog_close_dm",
        description="Stop relaying DM replies from a user to the modlog channel.",
    )
    @app_commands.describe(user="User to stop relaying", post="If true, post publicly in this channel")
    async def modlog_close_dm(self, interaction: discord.Interaction, user: discord.Member, post: bool = False):
        if not permission_ok(interaction.user):
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
