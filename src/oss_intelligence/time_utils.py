"""Shared UTC parsing for persisted timestamps."""

from datetime import datetime, timezone


def parse_utc(timestamp: str) -> datetime:
    text = timestamp.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    value = datetime.fromisoformat(text)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
