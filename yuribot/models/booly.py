from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from ..db import connect
from ..data.booly_defaults import (
    DEFAULT_BOOLY_ROWS,
    SPECIAL_DEFAULT_POOL,
)

BoolyScope = str

SCOPE_MENTION_GENERAL: BoolyScope = "mention_general"
SCOPE_MENTION_MOD: BoolyScope = "mention_mod"
SCOPE_PERSONAL: BoolyScope = "personal"


@dataclass
class BoolyMessage:
    id: int
    scope: BoolyScope
    user_id: Optional[int]
    content: str
    created_at: str
    updated_at: str


def _row_to_message(row: sqlite3.Row) -> BoolyMessage:
    return BoolyMessage(
        id=row[0],
        scope=row[1],
        user_id=row[2],
        content=row[3],
        created_at=row[4],
        updated_at=row[5],
    )


def fetch_messages(scope: BoolyScope, user_id: Optional[int] = None) -> List[BoolyMessage]:
    con = connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    if user_id is None:
        rows = cur.execute(
            "SELECT id, scope, user_id, content, created_at, updated_at "
            "FROM booly_messages WHERE scope=? AND user_id IS NULL ORDER BY id",
            (scope,),
        ).fetchall()
    else:
        rows = cur.execute(
            "SELECT id, scope, user_id, content, created_at, updated_at "
            "FROM booly_messages WHERE scope=? AND user_id=? ORDER BY id",
            (scope, user_id),
        ).fetchall()
    con.close()
    return [_row_to_message(r) for r in rows]


def fetch_all_pools() -> tuple[List[str], List[str], Dict[int, List[str]], List[str]]:
    con = connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    rows = cur.execute(
        "SELECT id, scope, user_id, content, created_at, updated_at FROM booly_messages ORDER BY id"
    ).fetchall()
    con.close()

    general: List[str] = []
    mod: List[str] = []
    personal: Dict[int, List[str]] = {}
    for row in rows:
        scope = row[1]
        user_id = row[2]
        content = row[3]
        if scope == SCOPE_MENTION_GENERAL:
            general.append(content)
        elif scope == SCOPE_MENTION_MOD:
            mod.append(content)
        elif scope == SCOPE_PERSONAL and user_id is not None:
            personal.setdefault(user_id, []).append(content)
    return general, mod, personal, list(SPECIAL_DEFAULT_POOL)


def fetch_message(message_id: int) -> Optional[BoolyMessage]:
    con = connect()
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    row = cur.execute(
        "SELECT id, scope, user_id, content, created_at, updated_at "
        "FROM booly_messages WHERE id=?",
        (message_id,),
    ).fetchone()
    con.close()
    return _row_to_message(row) if row else None


def create_message(scope: BoolyScope, content: str, user_id: Optional[int] = None) -> BoolyMessage:
    con = connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO booly_messages (scope, user_id, content) VALUES (?, ?, ?)",
        (scope, user_id, content),
    )
    message_id = cur.lastrowid
    con.commit()
    con.close()
    created = fetch_message(int(message_id))
    if not created:
        raise RuntimeError("Failed to create booly message")
    return created


def update_message(message_id: int, content: str) -> Optional[BoolyMessage]:
    con = connect()
    cur = con.cursor()
    cur.execute(
        "UPDATE booly_messages SET content=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
        (content, message_id),
    )
    con.commit()
    con.close()
    return fetch_message(message_id)


def delete_message(message_id: int) -> bool:
    con = connect()
    cur = con.cursor()
    cur.execute("DELETE FROM booly_messages WHERE id=?", (message_id,))
    deleted = cur.rowcount > 0
    con.commit()
    con.close()
    return deleted


def ensure_seed_data() -> None:
    con = connect()
    cur = con.cursor()
    count = cur.execute("SELECT COUNT(1) FROM booly_messages").fetchone()[0]
    if count:
        con.close()
        return
    cur.executemany(
        "INSERT INTO booly_messages (scope, user_id, content) VALUES (?, ?, ?)",
        DEFAULT_BOOLY_ROWS,
    )
    con.commit()
    con.close()


def bulk_replace(scope: BoolyScope, messages: Iterable[Tuple[Optional[int], str]]) -> None:
    con = connect()
    cur = con.cursor()
    if scope == SCOPE_PERSONAL:
        raise ValueError("Use dedicated functions for personal scope")
    cur.execute("DELETE FROM booly_messages WHERE scope=?", (scope,))
    cur.executemany(
        "INSERT INTO booly_messages (scope, user_id, content) VALUES (?, NULL, ?)",
        [(scope, content) for _, content in messages],
    )
    con.commit()
    con.close()