import os
import discord
from discord import app_commands
from discord.ext import commands

DASHBOARD_URL = os.getenv("DASHBOARD_URL", "https://yuri.icebrand.dev")
ALLOWED_ROLE_IDS = {rid for rid in os.getenv("ALLOWED_ROLE_IDS", "").split(",") if rid}
REQUIRE_MANAGE_GUILD = (
    os.getenv("DASHBOARD_REQUIRE_MANAGE_GUILD", "false").lower() == "true"
)

APPLICATION_ID = int(os.getenv("DISCORD_CLIENT_ID", "0"))


def user_is_allowed(member: discord.Member) -> bool:
    if REQUIRE_MANAGE_GUILD and member.guild_permissions.manage_guild:
        return True
    if not ALLOWED_ROLE_IDS:
        # default: allow users with Manage Guild
        return member.guild_permissions.manage_guild
    member_role_ids = {str(r.id) for r in member.roles}
    return bool(member_role_ids & ALLOWED_ROLE_IDS)


class Dashboard(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="dashboard",
        description="Open the admin dashboard (embedded Activity, admins only)",
    )
    @app_commands.guild_only()
    async def dashboard(self, interaction: discord.Interaction):
        guild = interaction.guild
        # Must be in a server and in a voice channel
        member = guild.get_member(interaction.user.id) if guild else None
        if not isinstance(member, discord.Member):
            return await interaction.response.send_message(
                "Use this in a server.", ephemeral=True
            )

        if not user_is_allowed(member):
            return await interaction.response.send_message(
                "You don't have access to the dashboard.", ephemeral=True
            )

        if not member.voice or not member.voice.channel:
            return await interaction.response.send_message(
                "Join a voice channel first.", ephemeral=True
            )

        if not APPLICATION_ID:
            return await interaction.response.send_message(
                "Missing DISCORD_CLIENT_ID for Activity launch.", ephemeral=True
            )

        vc: discord.VoiceChannel | discord.StageChannel = member.voice.channel

        perms = vc.permissions_for(guild.me)  # type: ignore
        if not perms.create_instant_invite:
            return await interaction.response.send_message(
                f"I need **Create Invite** permission in {vc.mention}.", ephemeral=True
            )

        try:
            invite = await vc.create_invite(
                max_age=300,
                max_uses=1,
                target_application_id=APPLICATION_ID,
                target_type=discord.InviteTarget.embedded_application,
                reason=f"Launch dashboard Activity for {member} ({member.id})",
            )
        except Exception as e:
            # Fallback: give them the plain embedded URL (opens in browser/iframe, not the Activity surface)
            view = discord.ui.View()
            view.add_item(
                discord.ui.Button(
                    label="Open Dashboard (web)", url=f"{DASHBOARD_URL}/activity"
                )
            )
            return await interaction.response.send_message(
                f"Couldn't start Activity: {e.__class__.__name__}. Using web fallback.",
                view=view,
                ephemeral=True,
            )

        view = discord.ui.View()
        view.add_item(
            discord.ui.Button(label="Open Dashboard (Activity)", url=invite.url)
        )
        await interaction.response.send_message(
            f"Launching dashboard in {vc.mention}…", view=view, ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(Dashboard(bot))
