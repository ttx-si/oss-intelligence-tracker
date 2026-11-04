"""GitHub Search API spike Signal Source adapter."""

from __future__ import annotations

import inspect
import json
import os
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from oss_intelligence.models import RawSignalCandidate


def _default_github_search_fetcher(
    token: str | None,
    query: str,
    per_page: int = 30,
    base_url: str = "https://api.github.com",
    timeout_sec: float = 10.0,
) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode(
        {
            "q": query,
            "sort": "stars",
            "order": "desc",
            "per_page": str(per_page),
        }
    )
    url = f"{base_url.rstrip('/')}/search/repositories?{params}"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "oss-intelligence-tracker/0.1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload: dict[str, Any] = json.loads(resp.read().decode("utf-8"))
    items: list[dict[str, Any]] = payload.get("items") or []
    return items


class GitHubSearchSpikeAdapter:
    """Queries GitHub Search API for high-velocity repositories created in the last 7 days."""

    signal_source = "gh_search"

    def __init__(
        self,
        *,
        token: str | None = None,
        days_lookback: int = 7,
        min_stars: int = 50,
        per_page: int = 30,
        now: datetime | None = None,
        fetcher: Callable[..., list[dict[str, Any]]] | None = None,
    ) -> None:
        self._token = token if token is not None else os.environ.get("GITHUB_TOKEN")
        self._days_lookback = days_lookback
        self._min_stars = min_stars
        self._per_page = per_page
        self._now = now
        self._fetcher = fetcher

    def _build_query(self) -> str:
        ref_now = self._now or datetime.now(timezone.utc)
        cutoff_date = (ref_now - timedelta(days=self._days_lookback)).strftime("%Y-%m-%d")
        return f"created:>={cutoff_date} stars:>={self._min_stars}"

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        query = self._build_query()
        if self._fetcher is None:
            raw_items = _default_github_search_fetcher(
                token=self._token,
                query=query,
                per_page=self._per_page,
            )
        else:
            sig = inspect.signature(self._fetcher)
            if len(sig.parameters) == 0:
                raw_items = self._fetcher()
            else:
                raw_items = self._fetcher(self._token, query)

        candidates: list[RawSignalCandidate] = []
        for idx, item in enumerate(raw_items, start=1):
            owner_info = item.get("owner") or {}
            full_name = item.get("full_name") or (
                f"{owner_info.get('login', '')}/{item.get('name', '')}".strip("/")
            )
            if not full_name or "/" not in full_name:
                continue

            stars = int(item.get("stargazers_count") or item.get("stars") or 0)
            forks = int(item.get("forks_count") or item.get("forks") or 0)
            period_stars = int(item.get("period_stars_reported") or stars)

            candidates.append(
                RawSignalCandidate(
                    full_name=full_name,
                    signal_source=self.signal_source,
                    stars=stars,
                    forks=forks,
                    source_rank=int(item.get("rank") or idx),
                    period_stars_reported=period_stars,
                    description=item.get("description"),
                    primary_language=item.get("language") or item.get("primary_language"),
                )
            )
        return candidates
