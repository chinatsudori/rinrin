from __future__ import annotations
import os
import discord
from discord.ext import commands
from .db import ensure_db
from .tasks import discussion_poster_loop
from .strings import _STRINGS

INTENTS = discord.Intents(guilds=True, message_content=True, members=True, emojis_and_stickers=True, voice_states=True, guild_messages=True)

class YuriBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=INTENTS)

    async def setup_hook(self):
        ensure_db()
        await self.load_extension("yuribot.cogs.admin")
        await self.load_extension("yuribot.cogs.collection")
        await self.load_extension("yuribot.cogs.polls")
        await self.load_extension("yuribot.cogs.series")
        await self.load_extension("yuribot.cogs.modlog")
        await self.load_extension("yuribot.cogs.botlog")
        await self.load_extension("yuribot.cogs.welcome")
        await self.load_extension("yuribot.cogs.timeout")
        await self.load_extension("yuribot.cogs.music")
        await self.load_extension("yuribot.cogs.stats")
        await self.load_extension("yuribot.cogs.emoji_stats")
        await self.load_extension("yuribot.cogs.activity")
        await self.load_extension("yuribot.cogs.movie")
        await self.load_extension("yuribot.cogs.movebot")
        await self.load_extension("yuribot.cogs.coin_dice")
        await self.load_extension("yuribot.cogs.mangaupdates")
        GUILD_ID = 1417424777425064059  # your test guild
        guild = discord.Object(id=GUILD_ID)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)  # quick!

    async def on_ready(self):
        try:
            await self.tree.sync()
        except Exception as e:
            print("Command sync failed:", e)
        print(f"Logged in as {self.user} ({self.user.id})")

def main():
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        print("Set DISCORD_TOKEN")
        raise SystemExit(1)
    YuriBot().run(token)
