from __future__ import annotations
import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..utils.time import now_local, to_iso
from ..views import VoteView
from ..strings import S


def parse_numbers(s: str) -> list[int]:
    parts = [p.strip() for p in s.replace(",", " ").split() if p.strip()]
    out = []
    for p in parts:
        if p.isdigit():
            out.append(int(p))
    seen = set(); res = []
    for n in out:
        if n not in seen:
            seen.add(n); res.append(n)
    return res


class PollsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="create_poll",
        description="Create a poll from current collection by submission numbers (e.g., 1,3,4)",
    )
    @app_commands.describe(
        numbers="Space/comma-separated numbers from /club list_current_submissions",
        title="Optional custom poll title",
        club="Club type (default: manga)",
    )
    async def create_poll(
        self,
        interaction: discord.Interaction,
        numbers: str,
        title: str | None = None,
        club: str = "manga",
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("poll.create.error.no_cfg", club=club), ephemeral=True
            )

        cw = models.latest_collection(interaction.guild_id, cfg["club_id"])
        if not cw:
            return await interaction.response.send_message(S("poll.create.error.no_collection"), ephemeral=True)

        ordinals = parse_numbers(numbers or "")
        chosen = models.get_submissions_by_ordinals(cw[0], ordinals)
        if not chosen:
            return await interaction.response.send_message(S("poll.create.error.no_valid_numbers"), ephemeral=True)

        poll_channel = interaction.guild.get_channel(cfg["polls_channel_id"])
        if not isinstance(poll_channel, discord.TextChannel):
            return await interaction.response.send_message(S("poll.create.error.bad_channel"), ephemeral=True)

        poll_id = models.create_poll(
            interaction.guild_id, cfg["club_id"], poll_channel.id, to_iso(now_local()), None
        )
        for sid, title_s, link, author_id, thread_id, created_at in chosen:
            models.add_poll_option(poll_id, title_s, sid)

        with models.connect() as con:
            cur = con.cursor()
            opts = cur.execute("SELECT id, label FROM poll_options WHERE poll_id=?", (poll_id,)).fetchall()

        view = VoteView(poll_id=poll_id, options=opts)
        embed = discord.Embed(
            title=title or S("poll.create.title", club=club),
            description=S("poll.create.desc", cid=cw[0]),
            color=discord.Color.pink(),
        )
        embed.add_field(
            name=S("poll.options_title"),
            value="\n".join([S("poll.option.bullet", label=label) for _, label in opts]),
            inline=False,
        )

        msg = await poll_channel.send(embed=embed, view=view)
        models.set_poll_message(poll_id, poll_channel.id, msg.id)

        await interaction.response.send_message(
            S("poll.create.posted", id=poll_id, channel=poll_channel.mention),
            ephemeral=True,
        )

    @app_commands.command(name="close_poll", description="Close a poll now and post results")
    @app_commands.describe(poll_id="Poll ID")
    async def close_poll(self, interaction: discord.Interaction, poll_id: int):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        votes = models.tally_poll(poll_id)
        models.close_poll(poll_id)
        row = models.get_poll_channel_and_message(poll_id)
        if not row:
            return await interaction.response.send_message(S("poll.close.not_found"), ephemeral=True)

        channel_id, message_id, guild_id = row
        ch = interaction.guild.get_channel(channel_id)
        if isinstance(ch, discord.TextChannel):
            lines = [S("poll.close.results_header")] + [
                S("poll.close.result_line", label=label, count=c) for _, label, c in votes
            ]
            await ch.send("\n".join(lines))

        await interaction.response.send_message(S("poll.close.closed", id=poll_id), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(PollsCog(bot))
