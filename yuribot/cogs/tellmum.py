from __future__ import annotations

import logging
import discord
from discord.ext import commands

log = logging.getLogger(__name__)

# --- Configuration ---
# Add any other keywords you want to be notified about here.
NOTIFY_KEYWORDS = {"thea", "mum"}


class OwnerNotifyCog(commands.Cog):
    """DMs the bot owner when specific keywords are mentioned."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.owner: discord.User | None = None
        # Start a task to find the bot's owner
        self.bot.loop.create_task(self.fetch_owner_once())

    async def fetch_owner_once(self):
        """Fetches the bot owner's user object from Discord."""
        await self.bot.wait_until_ready()
        try:
            # This is the most reliable way to get the owner
            app_info = await self.bot.application_info()
            self.owner = app_info.owner

            if self.owner:
                log.info(
                    f"OwnerNotifyCog: Owner found and set to {self.owner} ({self.owner.id})"
                )
            else:
                log.warning("OwnerNotifyCog: Could not find bot owner.")

        except Exception as e:
            log.error(f"OwnerNotifyCog: Failed to fetch bot owner: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # 1. Ignore bots (including self) and DMs
        if message.author.bot or message.guild is None:
            return

        # 2. Ensure the owner is set before proceeding
        if not self.owner:
            log.warning(
                "OwnerNotifyCog: Keyword triggered but owner is not set. Ignoring."
            )
            return

        # 3. Check if any keyword is in the message
        content_lower = message.content.lower()
        if not any(keyword in content_lower for keyword in NOTIFY_KEYWORDS):
            return

        # 4. Format and send the DM to you (the owner)
        try:
            embed = discord.Embed(
                title="Keyword Mention Notification",
                description=message.content,
                color=discord.Color.blue(),
                timestamp=message.created_at,
            )
            embed.set_author(
                name=f"{message.author} ({message.author.id})",
                icon_url=message.author.display_avatar.url,
            )
            embed.add_field(
                name="Source",
                value=f"**Server:** {message.guild.name}\n**Channel:** <#{message.channel.id}>",
                inline=False,
            )
            embed.add_field(
                name="Jump to Message",
                value=f"[Click Here]({message.jump_url})",
                inline=False,
            )

            await self.owner.send(embed=embed)

        except discord.Forbidden:
            log.warning(
                f"OwnerNotifyCog: Could not send DM to owner {self.owner.id}. (DMs may be closed)"
            )
        except Exception as e:
            log.exception(f"OwnerNotifyCog: Failed to send DM: {e}")


async def setup(bot: commands.Bot):
    """This function is called by the bot to load the cog."""
    await bot.add_cog(OwnerNotifyCog(bot))
