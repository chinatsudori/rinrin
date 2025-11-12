from __future__ import annotations

from typing import Optional

import discord

from ..strings import S
from ..utils.akinator_game import AkinatorGame, create_game


ANSWER_BUTTONS = [
    ("Yes", "yes", discord.ButtonStyle.success),
    ("Probably", "probably", discord.ButtonStyle.primary),
    ("Unsure", "unknown", discord.ButtonStyle.secondary),
    ("Probably Not", "probably_not", discord.ButtonStyle.primary),
    ("No", "no", discord.ButtonStyle.danger),
]


class AkinatorView(discord.ui.View):
    def __init__(self, *, user: discord.abc.User, yuri_mode: bool):
        super().__init__(timeout=180)
        self.user = user
        self.yuri_mode = yuri_mode
        self.game: AkinatorGame = create_game(yuri_mode=yuri_mode)
        self.message: Optional[discord.Message] = None
        self._closed = False
        for label, value, style in ANSWER_BUTTONS:
            self.add_item(_AnswerButton(label=label, value=value, style=style))
        self.add_item(_GuessButton())
        self.add_item(_EndButton())

    # --------------------------------------------------------------
    async def start(self, interaction: discord.Interaction) -> None:
        embed = self._build_question_embed()
        await interaction.response.send_message(embed=embed, view=self, ephemeral=True)
        self.message = await interaction.original_response()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:  # type: ignore[override]
        if interaction.user.id != self.user.id:
            await interaction.response.send_message(
                S("fun.akinator.not_owner", user=self.user.display_name),
                ephemeral=True,
            )
            return False
        return True

    async def handle_answer(self, interaction: discord.Interaction, value: str) -> None:
        if self._closed:
            return await interaction.response.send_message(
                S("fun.akinator.session_closed"), ephemeral=True
            )
        self.game.record_answer(value)
        if self.game.should_guess():
            await self._present_guess(interaction)
        else:
            embed = self._build_question_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    async def force_guess(self, interaction: discord.Interaction) -> None:
        await self._present_guess(interaction)

    async def cancel(self, interaction: discord.Interaction) -> None:
        self._closed = True
        self.disable_inputs(final=True)
        embed = self._build_notice_embed(S("fun.akinator.cancelled", mode=self.game.title))
        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    async def _present_guess(self, interaction: discord.Interaction) -> None:
        self._closed = True
        embed = self._build_guess_embed()
        self.disable_inputs(final=True)
        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    def disable_inputs(self, *, final: bool = False) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    async def on_timeout(self) -> None:  # type: ignore[override]
        if self._closed:
            return
        self._closed = True
        self.disable_inputs(final=True)
        if self.message:
            embed = self._build_notice_embed(
                S("fun.akinator.timeout", mode=self.game.title)
            )
            await self.message.edit(embed=embed, view=self)
        self.stop()

    # --------------------------------------------------------------
    def _build_question_embed(self) -> discord.Embed:
        question = self.game.current_question() or S("fun.akinator.waiting_guess")
        embed = discord.Embed(
            title=S(
                "fun.akinator.question_title", mode=self.game.title, n=self.game.question_number
            ),
            description=question,
            colour=self._colour,
        )
        embed.set_footer(
            text=S("fun.akinator.footer", count=self.game.candidate_count())
        )
        top = self.game.top_candidates()
        if top:
            summary = ", ".join(
                f"{cand.character['name']} ({int(cand.confidence * 100)}%)" for cand in top
            )
            embed.add_field(
                name=S("fun.akinator.candidates_title"),
                value=S("fun.akinator.candidates", names=summary),
                inline=False,
            )
        if self.yuri_mode:
            embed.set_author(name="Rinrinator", icon_url=self._author_icon)
        return embed

    def _build_guess_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=S("fun.akinator.guess_title", mode=self.game.title),
            colour=self._colour,
        )
        guess = self.game.best_guess()
        if guess:
            embed.description = S(
                "fun.akinator.guess_text",
                pct=int(guess.confidence * 100),
                name=guess.character["name"],
                series=guess.character["series"],
            )
            embed.add_field(
                name=S("fun.akinator.reason_title"),
                value=guess.character["blurb"],
                inline=False,
            )
        else:
            embed.description = S("fun.akinator.no_guess")
            top = self.game.top_candidates()
            if top:
                lines = [
                    f"• {cand.character['name']} ({int(cand.confidence * 100)}%)"
                    for cand in top
                ]
                embed.add_field(
                    name=S("fun.akinator.candidates_title"),
                    value="\n".join(lines),
                    inline=False,
                )
        return embed

    def _build_notice_embed(self, text: str) -> discord.Embed:
        embed = discord.Embed(title=self.game.title, description=text, colour=self._colour)
        return embed

    @property
    def _author_icon(self) -> str:
        # Fun accent colour block; 1x1 png data URI to avoid remote calls.
        return (
            "https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/"
            "Pink_circle.svg/120px-Pink_circle.svg.png"
        )

    @property
    def _colour(self) -> discord.Colour:
        return discord.Colour.magenta() if self.yuri_mode else discord.Colour.gold()


class _AnswerButton(discord.ui.Button):
    def __init__(self, *, label: str, value: str, style: discord.ButtonStyle):
        super().__init__(label=label, style=style)
        self.value = value

    async def callback(self, interaction: discord.Interaction) -> None:  # type: ignore[override]
        view = self.view
        if isinstance(view, AkinatorView):
            await view.handle_answer(interaction, self.value)


class _GuessButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(label=S("fun.akinator.button.guess"), style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction) -> None:  # type: ignore[override]
        view = self.view
        if isinstance(view, AkinatorView):
            await view.force_guess(interaction)


class _EndButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(label=S("fun.akinator.button.end"), style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction) -> None:  # type: ignore[override]
        view = self.view
        if isinstance(view, AkinatorView):
            await view.cancel(interaction)

