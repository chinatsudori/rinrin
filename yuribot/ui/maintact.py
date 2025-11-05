from __future__ import annotations

from typing import Iterable


def format_month_results(months: Iterable[str]) -> str:
    return ", ".join(sorted(months)) if months else "-"
