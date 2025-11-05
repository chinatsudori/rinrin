from __future__ import annotations

from typing import Iterable

import discord

from ..strings import S


def build_collection_list_embed(*, club: str, collection_id: int, status: str, submissions: Iterable[tuple[int, str, str, int, int]]) -> discord.Embed:
    embed = discord.Embed(
        title=S("collection.embed.title", club=club, id=collection_id, status=status),
        color=discord.Color.pink(),
    )
    for i, (submission_id, title, link, author_id, thread_id) in enumerate(submissions, start=1):
        field_name = S("collection.embed.item_name", i=i, title=title)
        field_value = S(
            "collection.embed.item_value",
            link=link or S("collection.common.no_link"),
            author_id=author_id,
            thread_id=thread_id,
        )
        embed.add_field(name=field_name, value=field_value, inline=False)
    return embed
