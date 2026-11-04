"""Opt-in Trendshift Signal Source adapter gated by TRENDSHIFT_API_KEY."""

from __future__ import annotations

import inspect
import json
import os
import urllib.request
from collections.abc import Callable
from typing import Any

from oss_intelligence.models import RawSignalCandidate


def _default_trendshift_fetcher(
    api_key: str,
    url: str = "https://trendshift.io/api/v1/repositories",
    timeout_sec: float = 10.0,
) -> list[dict[str, Any]]:
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "oss-intelligence-tracker/0.1.0",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload: Any = json.loads(resp.read().decode("utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        items: list[dict[str, Any]] = payload.get("data") or payload.get("items") or []
        return items
    return []


class TrendshiftAdapter:
    """Opt-in commercial spike adapter activated only when TRENDSHIFT_API_KEY is configured."""

    signal_source = "trendshift"

    def __init__(
        self,
        *,
        api_key: str | None = None,
        fetcher: Callable[..., list[dict[str, Any]]] | None = None,
    ) -> None:
        self._explicit_api_key = api_key
        self._fetcher = fetcher

    def _resolve_api_key(self) -> str:
        raw_key = (
            self._explicit_api_key
            if self._explicit_api_key is not None
            else os.environ.get("TRENDSHIFT_API_KEY", "")
        )
        return raw_key.strip()

    @property
    def is_enabled(self) -> bool:
        return bool(self._resolve_api_key())

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        api_key = self._resolve_api_key()
        if not api_key:
            return []

        if self._fetcher is None:
            raw_items = _default_trendshift_fetcher(api_key)
        else:
            sig = inspect.signature(self._fetcher)
            if len(sig.parameters) == 0:
                raw_items = self._fetcher()
            else:
                raw_items = self._fetcher(api_key)

        candidates: list[RawSignalCandidate] = []
        for idx, item in enumerate(raw_items, start=1):
            full_name = (
                item.get("full_name")
                or item.get("repository_name")
                or item.get("fullname")
                or ""
            ).strip("/")
            if not full_name or "/" not in full_name:
                continue

            stars = int(item.get("stars") or item.get("stargazers_count") or 0)
            forks = int(item.get("forks") or item.get("forks_count") or 0)
            period_stars = int(
                item.get("period_stars")
                or item.get("period_stars_reported")
                or item.get("stars_delta")
                or 0
            )

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
