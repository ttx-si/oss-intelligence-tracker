"""Gtrending Signal Source adapter."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from oss_intelligence.models import RawSignalCandidate


def _default_gtrending_fetcher() -> list[dict[str, Any]]:
    import gtrending  # type: ignore[import-untyped]

    result: list[dict[str, Any]] = gtrending.fetch_repos()
    return result


class GtrendingAdapter:
    """Fetches GitHub Trending repositories and converts them into RawSignalCandidate items."""

    signal_source = "gtrending"

    def __init__(
        self,
        fetcher: Callable[[], list[dict[str, Any]]] | None = None,
    ) -> None:
        self._fetcher = fetcher or _default_gtrending_fetcher

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        raw_repos = self._fetcher()
        candidates: list[RawSignalCandidate] = []
        for idx, item in enumerate(raw_repos, start=1):
            full_name = item.get("fullname") or f"{item.get('author', '')}/{item.get('name', '')}".strip("/")
            if not full_name or "/" not in full_name:
                continue
            candidates.append(
                RawSignalCandidate(
                    full_name=full_name,
                    signal_source="gtrending",
                    stars=int(item.get("stars") or 0),
                    forks=int(item.get("forks") or 0),
                    source_rank=idx,
                    period_stars_reported=int(item.get("currentPeriodStars") or 0),
                    description=item.get("description"),
                    primary_language=item.get("language"),
                )
            )
        return candidates
