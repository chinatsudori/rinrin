from __future__ import annotations

from typing import List

import discord

from ..strings import S


def format_move_summary(
    *,
    copied: int,
    total: int,
    failed: int,
    deleted: int,
    post_publicly: bool,
) -> str:
    return S(
        "move_any.summary",
        copied=copied,
        total=total,
        failed=failed,
        deleted=deleted,
        post=post_publicly,
    )


def format_pin_summary(
    *,
    pinned: int,
    total: int,
    destination: discord.abc.Messageable,
    misses: List[int],
) -> str:
    summary = S(
        "move_any.pin.summary",
        pinned=pinned,
        total=total,
        dst=getattr(destination, "mention", "destination"),
    )
    if misses:
        sample = "\n".join(f"- {mid}" for mid in misses[:10])
        summary += S(
            "move_any.pin.summary_misses_tail",
            missed=len(misses),
            sample=sample,
            shown=min(10, len(misses)),
        )
    return summary


def format_reply_header(author: discord.Member, jump_url: str, snippet: str) -> str:
    return S(
        "move_any.reply.header",
        author=author.display_name,
        jump=jump_url,
        snippet=snippet,
    )


def format_move_header(
    author: discord.Member, created_at: discord.datetime, jump_url: str
) -> str:
    ts = f"<t:{int(created_at.timestamp())}:F>"
    return S("move_any.header", author=author.display_name, ts=ts, jump=jump_url)
