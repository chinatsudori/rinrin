from __future__ import annotations
import discord
from discord import app_commands
from discord.ext import commands

try:
    from ..models import settings as ms
except Exception:
    from yuribot.models import settings as ms

class GuildSettings(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self):
        try:
            ms.ensure_table()
        except Exception:
            pass

    @app_commands.command(name="set_channel", description="Set a per-guild channel setting by key.")
    @app_commands.describe(key="e.g., log_channel, modlog_channel, welcome_channel", channel="Target channel")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def set_channel(self, interaction: discord.Interaction, key: str, channel: discord.abc.GuildChannel):
        ms.set_guild_setting(interaction.guild_id, key, str(channel.id))
        await interaction.response.send_message(f"Set `{key}` to {channel.mention}", ephemeral=True)

    @app_commands.command(name="get_setting", description="Get a per-guild setting by key.")
    @app_commands.describe(key="Setting key to query")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def get_setting(self, interaction: discord.Interaction, key: str):
        val = ms.get_guild_setting(interaction.guild_id, key, default="(not set)")
        await interaction.response.send_message(f"`{key}` = `{val}`", ephemeral=True)

    @app_commands.command(name="list_settings", description="List all per-guild settings.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def list_settings(self, interaction: discord.Interaction):
        try:
            conn = ms._conn()
            cur = conn.execute("SELECT key, value FROM guild_settings WHERE guild_id=? ORDER BY key", (interaction.guild_id,))
            rows = cur.fetchall()
        except Exception:
            rows = []
        if not rows:
            return await interaction.response.send_message("No settings found for this guild.", ephemeral=True)
        lines = [f"- **{k}**: `{v}`" for (k, v) in rows]
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(GuildSettings(bot))