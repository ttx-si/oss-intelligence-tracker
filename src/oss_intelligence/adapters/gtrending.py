"""Gtrending Signal Source adapter."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any

from oss_intelligence.models import RawSignalCandidate

logger = logging.getLogger(__name__)

DEFAULT_GTRENDING_LANGUAGES: tuple[str, ...] = (
    "",
    "python",
    "typescript",
    "rust",
    "go",
    "javascript",
    "c++",
    "java",
    "c",
    "c#",
    "shell",
)
DEFAULT_GTRENDING_WINDOWS: tuple[str, ...] = ("daily", "weekly")


def _default_gtrending_fetcher(
    language: str = "",
    since: str = "daily",
) -> list[dict[str, Any]]:
    import gtrending  # type: ignore[import-untyped]

    fetch_fn = gtrending.fetch_repos
    sig = inspect.signature(fetch_fn)
    if len(sig.parameters) == 0:
        result: list[dict[str, Any]] = fetch_fn()
    else:
        result = fetch_fn(language=language, since=since)
    return result or []


def _normalize_period_stars(raw_period_stars: int, since: str) -> int:
    if raw_period_stars <= 0:
        return 0
    if since == "weekly":
        return max(1, int(round(raw_period_stars / 7.0)))
    return raw_period_stars


class GtrendingAdapter:
    """Fetches GitHub Trending repositories across languages and windows into RawSignalCandidate items."""

    signal_source = "gtrending"

    def __init__(
        self,
        fetcher: Callable[..., list[dict[str, Any]]] | None = None,
        *,
        languages: Sequence[str] = DEFAULT_GTRENDING_LANGUAGES,
        since_windows: Sequence[str] = DEFAULT_GTRENDING_WINDOWS,
    ) -> None:
        self._fetcher = fetcher or _default_gtrending_fetcher
        self._languages: tuple[str, ...] = tuple(languages)
        self._since_windows: tuple[str, ...] = tuple(since_windows)

    def _iter_slices(self) -> list[tuple[str, str, list[dict[str, Any]]]]:
        sig = inspect.signature(self._fetcher)
        if len(sig.parameters) == 0:
            return [("", "daily", self._fetcher() or [])]

        slices: list[tuple[str, str, list[dict[str, Any]]]] = []
        attempts = 0
        failures = 0
        last_error: Exception | None = None

        for since in self._since_windows:
            for language in self._languages:
                attempts += 1
                try:
                    rows = self._fetcher(language, since) or []
                    slices.append((language, since, rows))
                except Exception as exc:
                    failures += 1
                    last_error = exc
                    logger.warning(
                        "Gtrending slice fetch failed (language=%r, since=%r): %s",
                        language,
                        since,
                        exc,
                    )

        if attempts > 0 and failures == attempts and last_error is not None:
            raise last_error

        return slices

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        candidates_by_key: dict[str, RawSignalCandidate] = {}
        seen_daily_keys: set[str] = set()

        for _language, since, raw_repos in self._iter_slices():
            for idx, item in enumerate(raw_repos, start=1):
                full_name = (
                    item.get("fullname")
                    or f"{item.get('author', '')}/{item.get('name', '')}".strip("/")
                )
                if not full_name or "/" not in full_name:
                    continue

                key = full_name.lower()
                stars = int(item.get("stars") or 0)
                forks = int(item.get("forks") or 0)
                raw_period = int(item.get("currentPeriodStars") or 0)
                period_stars = _normalize_period_stars(raw_period, since)
                description = item.get("description")
                primary_language = item.get("language")

                if key not in candidates_by_key:
                    if since == "daily":
                        seen_daily_keys.add(key)
                    candidates_by_key[key] = RawSignalCandidate(
                        full_name=full_name,
                        signal_source=self.signal_source,
                        stars=stars,
                        forks=forks,
                        source_rank=idx,
                        period_stars_reported=period_stars,
                        description=description,
                        primary_language=primary_language,
                    )
                else:
                    existing = candidates_by_key[key]
                    if since == "daily":
                        if key in seen_daily_keys:
                            merged_period = max(existing.period_stars_reported, period_stars)
                        else:
                            merged_period = period_stars
                            seen_daily_keys.add(key)
                    elif key in seen_daily_keys:
                        merged_period = existing.period_stars_reported
                    else:
                        merged_period = max(existing.period_stars_reported, period_stars)

                    existing_rank = existing.source_rank if existing.source_rank is not None else idx
                    candidates_by_key[key] = replace(
                        existing,
                        stars=max(existing.stars, stars),
                        forks=max(existing.forks, forks),
                        source_rank=min(existing_rank, idx),
                        period_stars_reported=merged_period,
                        description=existing.description or description,
                        primary_language=existing.primary_language or primary_language,
                    )

        return list(candidates_by_key.values())
