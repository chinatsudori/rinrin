from __future__ import annotations
from typing import Optional, Dict, Tuple
from datetime import timedelta

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..utils.time import now_local, to_iso

# ---- helpers ---------------------------------------------------------

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
        1: " Gentle Nudge",
        2: "💙 Formal Warning",
        3: "💜 Escalated Warning",
        4: "❤️ Critical / Harmful Behavior",
    }.get(temp, f"Temp {temp}")

# ---- Cog -------------------------------------------------------------

class ModLogCog(commands.Cog):
    """
    Moderation logging with temperature (1-4), optional timeout & ban.
    DMs the user with Reason/Details/Actions/Status and relays DM replies
    back to the guild's mod-logs channel.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # user_id -> (guild_id, modlog_channel_id)
        self._dm_relays: Dict[int, Tuple[int, int]] = {}

    # -------------------- /modlog --------------------

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
        timeout_minutes="Optional timeout (minutes)",
        ban="Ban the user (yes/no)"
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
    ):
        if not _perm_ok(interaction.user):
            return await interaction.response.send_message("Insufficient permissions.", ephemeral=True)

        # Resolve mod-log channel
        channel_id = models.get_mod_logs_channel(interaction.guild_id)
        if not channel_id:
            return await interaction.response.send_message(
                "Mod logs channel not set. Run `/set_mod_logs` first.", ephemeral=True
            )
        ch = interaction.guild.get_channel(channel_id)
        if not isinstance(ch, discord.TextChannel):
            return await interaction.response.send_message(
                "Configured mod logs channel is invalid. Re-run `/set_mod_logs`.", ephemeral=True
            )

        await interaction.response.defer(ephemeral=True, thinking=True)

        # Optional enforcement
        actions_taken: list[str] = []
        evidence_url: Optional[str] = None

        # timeout
        if timeout_minutes and timeout_minutes > 0:
            # permission check for timeout
            if not interaction.user.guild_permissions.moderate_members:
                actions_taken.append(f"Timeout requested ({timeout_minutes}m) — **denied** (missing permission).")
            else:
                try:
                    until = discord.utils.utcnow() + timedelta(minutes=int(timeout_minutes))
                    await user.timeout(until, reason=reason or "Timed out by moderator.")
                    actions_taken.append(f"Timeout: {int(timeout_minutes)} minutes")
                except discord.Forbidden:
                    actions_taken.append(f"Timeout requested ({timeout_minutes}m) — **forbidden**.")
                except discord.HTTPException as e:
                    actions_taken.append(f"Timeout requested ({timeout_minutes}m) — **HTTP error**: {e}")

        # ban
        if ban:
            if not interaction.user.guild_permissions.ban_members:
                actions_taken.append("Ban requested — **denied** (missing permission).")
            else:
                try:
                    await interaction.guild.ban(user, reason=reason or "Banned by moderator.", delete_message_days=0)
                    actions_taken.append("Ban: **applied**")
                except discord.Forbidden:
                    actions_taken.append("Ban requested — **forbidden**.")
                except discord.HTTPException as e:
                    actions_taken.append(f"Ban requested — **HTTP error**: {e}")

        # evidence image
        if evidence and evidence.content_type and evidence.content_type.startswith("image/"):
            evidence_url = evidence.url

        # Compose moderation embed for mod channel
        temp_label = _temp_label(int(temperature))
        color = _color_for_temp(int(temperature))

        mod_embed = discord.Embed(
            title=f"Moderation — {temp_label}",
            color=color,
            timestamp=now_local(),
        )
        mod_embed.add_field(name="User", value=f"{user.mention} (`{user.id}`)", inline=False)
        mod_embed.add_field(name="Rule", value=rule.value, inline=True)
        mod_embed.add_field(name="Temperature", value=str(int(temperature)), inline=True)
        mod_embed.add_field(name="Reason", value=reason[:1000], inline=False)
        if details:
            mod_embed.add_field(name="Details", value=details[:1000], inline=False)
        if actions_taken:
            mod_embed.add_field(name="Actions", value="\n".join(actions_taken)[:1000], inline=False)
        mod_embed.set_footer(text=f"Actor: {interaction.user} ({interaction.user.id})")
        if evidence_url:
            mod_embed.set_image(url=evidence_url)

        await ch.send(embed=mod_embed)

        # Persist (store temperature in the old 'offense' slot)
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
            # DB is optional – don't fail the command if it hiccups
            pass

        # DM the user (Reason / Details / Actions / Status)
        dm_actions_lines = []
        if reason:
            dm_actions_lines.append(f"• **Reason:** {reason}")
        if details:
            dm_actions_lines.append(f"• **Detail:** {details}")
        if actions_taken:
            dm_actions_lines.append(f"• **Actions:** " + "; ".join(actions_taken))
        else:
            dm_actions_lines.append("• **Actions:** Warning recorded")

        status_text = (
            "Open — You can reply to this DM if you want to discuss or request mediation. "
            "A moderator will review your message."
        )

        dm_embed = discord.Embed(
            title="Moderation Notice",
            description=_temp_label(int(temperature)),
            color=color,
        )
        dm_embed.add_field(name="Rule", value=rule.value, inline=True)
        dm_embed.add_field(name="Status", value=status_text, inline=False)
        # break out Reason / Detail / Actions as requested
        dm_embed.add_field(name="Reason", value=reason[:1000] if reason else "—", inline=False)
        if details:
            dm_embed.add_field(name="Detail", value=details[:1000], inline=False)
        dm_embed.add_field(
            name="Actions",
            value=("\n".join(actions_taken) if actions_taken else "Warning recorded"),
            inline=False
        )

        try:
            await user.send(embed=dm_embed)
            # Register relay so replies go to modlog channel
            self._dm_relays[user.id] = (interaction.guild_id, ch.id)
        except Exception:
            # DMs closed; tell staff
            await ch.send(f"Could not DM {user.mention} (privacy settings).")

        await interaction.followup.send("Logged.", ephemeral=True)

    # -------------------- DM reply relay --------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Only handle DMs from users with an active relay
        if message.guild is not None:
            return
        if message.author.bot:
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

        # Forward the user's DM to the modlog channel
        emb = discord.Embed(
            title="User Reply (DM)",
            description=message.content[:2000] if message.content else " ",
            color=discord.Color.blurple(),
            timestamp=now_local(),
        )
        emb.set_footer(text=f"From: {message.author} ({message.author.id})")
        # Include attachments
        files = []
        if message.attachments:
            # Just include links inline to keep it simple
            links = "\n".join(a.url for a in message.attachments)
            emb.add_field(name="Attachments", value=links[:1000], inline=False)

        try:
            await channel.send(embed=emb)
        except Exception:
            pass

    # Optional: command for moderators to close a relay
    @app_commands.command(name="modlog_close_dm", description="Stop relaying DM replies from a user to the modlog channel.")
    async def modlog_close_dm(self, interaction: discord.Interaction, user: discord.Member):
        if not _perm_ok(interaction.user):
            return await interaction.response.send_message("Insufficient permissions.", ephemeral=True)
        self._dm_relays.pop(user.id, None)
        await interaction.response.send_message(f"Relay for {user.mention} closed.", ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(ModLogCog(bot))

