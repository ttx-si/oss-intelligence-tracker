"""GitHub Search API spike Signal Source adapter."""

from __future__ import annotations

import inspect
import json
import logging
import os
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from oss_intelligence.models import RawSignalCandidate, normalize_description

logger = logging.getLogger(__name__)

DEFAULT_SEARCH_SLICES: tuple[tuple[int, int], ...] = (
    (7, 50),
    (30, 200),
)


def _default_github_search_fetcher(
    token: str | None,
    query: str,
    per_page: int = 100,
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


def _estimate_daily_stars(
    item: dict[str, Any],
    stars: int,
    ref_now: datetime,
    slice_days_lookback: int,
) -> int:
    if item.get("period_stars_reported") is not None:
        return int(item["period_stars_reported"])
    if stars <= 0:
        return 0
    created_raw = item.get("created_at")
    age_days = float(max(1, slice_days_lookback))
    if isinstance(created_raw, str) and created_raw.strip():
        try:
            created_dt = datetime.fromisoformat(created_raw.strip().replace("Z", "+00:00"))
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            age_days = max(1.0, (ref_now - created_dt).total_seconds() / 86400.0)
        except ValueError:
            pass
    return max(1, int(round(stars / age_days)))


class GitHubSearchSpikeAdapter:
    """Queries GitHub Search API for high-velocity breakout repositories across 7d and 30d windows."""

    signal_source = "gh_search"

    def __init__(
        self,
        *,
        token: str | None = None,
        days_lookback: int = 7,
        min_stars: int = 50,
        per_page: int = 100,
        search_slices: Sequence[tuple[int, int]] | None = None,
        now: datetime | None = None,
        fetcher: Callable[..., list[dict[str, Any]]] | None = None,
    ) -> None:
        self._token = token if token is not None else os.environ.get("GITHUB_TOKEN")
        self._days_lookback = days_lookback
        self._min_stars = min_stars
        self._per_page = per_page
        if search_slices is not None:
            self._search_slices: tuple[tuple[int, int], ...] = tuple(
                (int(d), int(s)) for d, s in search_slices
            )
        elif days_lookback == 7 and min_stars == 50:
            self._search_slices = DEFAULT_SEARCH_SLICES
        else:
            self._search_slices = ((int(days_lookback), int(min_stars)),)
        self._now = now
        self._fetcher = fetcher

    def _build_query(self, days_lookback: int | None = None, min_stars: int | None = None) -> str:
        ref_now = self._now or datetime.now(timezone.utc)
        lookback = self._days_lookback if days_lookback is None else days_lookback
        stars_floor = self._min_stars if min_stars is None else min_stars
        cutoff_date = (ref_now - timedelta(days=lookback)).strftime("%Y-%m-%d")
        return f"created:>={cutoff_date} stars:>={stars_floor}"

    def _fetch_slice_items(self, query: str) -> list[dict[str, Any]]:
        if self._fetcher is None:
            return _default_github_search_fetcher(
                token=self._token,
                query=query,
                per_page=self._per_page,
            )
        sig = inspect.signature(self._fetcher)
        if len(sig.parameters) == 0:
            return self._fetcher()
        return self._fetcher(self._token, query)

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        ref_now = self._now or datetime.now(timezone.utc)
        if ref_now.tzinfo is None:
            ref_now = ref_now.replace(tzinfo=timezone.utc)

        candidates: list[RawSignalCandidate] = []
        seen_repos: set[str] = set()
        attempts = 0
        failures = 0
        last_error: Exception | None = None

        slices_to_run = (
            ((self._days_lookback, self._min_stars),)
            if self._fetcher is not None and self._search_slices == DEFAULT_SEARCH_SLICES
            else self._search_slices
        )

        for slice_days, slice_stars in slices_to_run:
            attempts += 1
            query = self._build_query(days_lookback=slice_days, min_stars=slice_stars)
            try:
                raw_items = self._fetch_slice_items(query) or []
            except Exception as exc:
                failures += 1
                last_error = exc
                logger.warning("GitHubSearchSpikeAdapter slice failed (query=%r): %s", query, exc)
                continue

            for idx, item in enumerate(raw_items, start=1):
                owner_info = item.get("owner") or {}
                full_name = item.get("full_name") or (
                    f"{owner_info.get('login', '')}/{item.get('name', '')}".strip("/")
                )
                if not full_name or "/" not in full_name:
                    continue
                key = full_name.lower()
                if key in seen_repos:
                    continue
                seen_repos.add(key)

                stars = int(item.get("stargazers_count") or item.get("stars") or 0)
                forks = int(item.get("forks_count") or item.get("forks") or 0)
                period_stars = _estimate_daily_stars(
                    item=item,
                    stars=stars,
                    ref_now=ref_now,
                    slice_days_lookback=slice_days,
                )

                candidates.append(
                    RawSignalCandidate(
                        full_name=full_name,
                        signal_source=self.signal_source,
                        stars=stars,
                        forks=forks,
                        source_rank=int(item.get("rank") or idx),
                        period_stars_reported=period_stars,
                        description=normalize_description(item.get("description")),
                        primary_language=item.get("language") or item.get("primary_language"),
                    )
                )

        if attempts > 0 and failures == attempts and last_error is not None:
            raise last_error

        return candidates
