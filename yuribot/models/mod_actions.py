from __future__ import annotations

from typing import List, Tuple

from ..db import connect


def add_mod_action(
    guild_id: int,
    target_user_id: int,
    target_username: str,
    rule: str,
    offense: int,
    action: str,
    details: str | None,
    evidence_url: str | None,
    actor_user_id: int,
    created_at: str,
) -> int:
    with connect() as con:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO mod_actions (guild_id, target_user_id, target_username, rule, offense, action, details, evidence_url, actor_user_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                target_user_id,
                target_username,
                rule,
                offense,
                action,
                details or "",
                evidence_url or "",
                actor_user_id,
                created_at,
            ),
        )
        con.commit()
        return cur.lastrowid


def list_mod_actions_for_user(guild_id: int, target_user_id: int, limit: int = 20) -> List[Tuple]:
    with connect() as con:
        cur = con.cursor()
        return cur.execute(
            """
            SELECT id, rule, offense, action, details, evidence_url, actor_user_id, created_at
            FROM mod_actions
            WHERE guild_id=? AND target_user_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (guild_id, target_user_id, limit),
        ).fetchall()


def add_timeout(
    guild_id: int,
    target_user_id: int,
    target_username: str,
    actor_user_id: int,
    duration_seconds: int,
    reason: str,
    created_at: str,
) -> int:
    details = reason or f"Timeout for {max(0, int(duration_seconds))} seconds"
    return add_mod_action(
        guild_id=guild_id,
        target_user_id=target_user_id,
        target_username=target_username,
        rule="timeout",
        offense=0,
        action="timeout",
        details=details,
        evidence_url="",
        actor_user_id=actor_user_id,
        created_at=created_at,
    )


__all__ = ["add_mod_action", "add_timeout", "list_mod_actions_for_user"]