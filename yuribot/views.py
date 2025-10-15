from __future__ import annotations
import discord
from .models import record_vote

class VoteView(discord.ui.View):
    def __init__(self, poll_id:int, options:list[tuple[int,str]]):
        super().__init__(timeout=None)
        for option_id, label in options:
            self.add_item(VoteButton(poll_id, option_id, label))

class VoteButton(discord.ui.Button):
    def __init__(self, poll_id:int, option_id:int, label:str):
        super().__init__(style=discord.ButtonStyle.primary, label=label)
        self.poll_id = poll_id
        self.option_id = option_id

    async def callback(self, interaction: discord.Interaction):
        record_vote(self.poll_id, interaction.user.id, self.option_id)
        await interaction.response.send_message("Vote recorded.", ephemeral=True)
