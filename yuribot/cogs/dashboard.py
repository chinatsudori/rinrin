import os
import discord
from discord import app_commands
from discord.ext import commands

DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://yuri.icebrand.dev")
ALLOWED_ROLE_IDS = {
    rid for rid in (os.getenv("ALLOWED_ROLE_IDS", "").split(",")) if rid
}
REQUIRE_MANAGE_GUILD = (
    os.getenv("DASHBOARD_REQUIRE_MANAGE_GUILD", "false").lower() == "true"
)


def user_is_allowed(member: discord.Member) -> bool:
    if REQUIRE_MANAGE_GUILD and member.guild_permissions.manage_guild:
        return True
    if not ALLOWED_ROLE_IDS:
        return member.guild_permissions.manage_guild
    member_role_ids = {str(r.id) for r in member.roles}
    return bool(member_role_ids & ALLOWED_ROLE_IDS)


class Dashboard(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="dashboard", description="Open the admin dashboard (admins only)"
    )
    @app_commands.guild_only()
    async def dashboard(self, interaction: discord.Interaction):
        member = (
            interaction.user
            if isinstance(interaction.user, discord.Member)
            else interaction.guild.get_member(interaction.user.id)
        )
        if not isinstance(member, discord.Member):
            await interaction.response.send_message(
                "Use this in a server.", ephemeral=True
            )
            return
        if not user_is_allowed(member):
            await interaction.response.send_message(
                "You don't have access to the dashboard.", ephemeral=True
            )
            return

        # Embedded entry path for Activity
        url = f"{DASHBOARD_URL}/activity"
        view = discord.ui.View()
        view.add_item(discord.ui.Button(label="Open Dashboard (Embedded)", url=url))
        await interaction.response.send_message(
            "Admin dashboard (SSO required):", view=view, ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(Dashboard(bot))
