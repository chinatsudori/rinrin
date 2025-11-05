from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from io import BytesIO, StringIO
from typing import Iterable, List, Sequence, Tuple

CUSTOM_EMOJI_RE = re.compile(r"<a?:(?P<name>\w+):(?P<id>\d+)>")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def month_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def label_for_unicode(emoji_str: str) -> str:
    return emoji_str


def iter_unicode_emojis(text: str) -> List[str]:
    try:
        import emoji as emoji_lib  # optional dependency

        return [match["emoji"] for match in emoji_lib.emoji_list(text)]
    except Exception:
        return [ch for ch in text if ord(ch) >= 0x2190]


def export_usage_csv(
    month: str,
    emoji_rows: Sequence[Tuple[str, str, bool, bool, int]],
    sticker_rows: Sequence[Tuple[str, str, int]],
) -> BytesIO:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["type", "key_or_id", "name", "is_custom", "via_reaction", "count", "month"])
    for key, name, is_custom, via_reaction, count in emoji_rows:
        writer.writerow(["emoji", key, name, is_custom, via_reaction, count, month])
    for sid, sname, count in sticker_rows:
        writer.writerow(["sticker", sid, sname, "", "", count, month])
    data = buf.getvalue().encode("utf-8")
    out = BytesIO(data)
    out.seek(0)
    return out
