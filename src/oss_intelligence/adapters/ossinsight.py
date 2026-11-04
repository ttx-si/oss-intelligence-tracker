"""OSSInsight trending Signal Source adapter."""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from typing import Any

from oss_intelligence.models import RawSignalCandidate


def _default_ossinsight_fetcher(
    url: str = "https://api.ossinsight.io/v1/trends/repos/",
    timeout_sec: float = 10.0,
) -> list[dict[str, Any]]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "oss-intelligence-tracker/0.1.0",
    }
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload: dict[str, Any] = json.loads(resp.read().decode("utf-8"))
    data = payload.get("data") or {}
    rows: list[dict[str, Any]] = data.get("rows") if isinstance(data, dict) else []
    return rows or []


class OSSInsightAdapter:
    """Fetches trending repository metrics from OSSInsight."""

    signal_source = "ossinsight"

    def __init__(
        self,
        fetcher: Callable[[], list[dict[str, Any]]] | None = None,
    ) -> None:
        self._fetcher = fetcher or _default_ossinsight_fetcher

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        raw_rows = self._fetcher()
        candidates: list[RawSignalCandidate] = []
        for idx, item in enumerate(raw_rows, start=1):
            full_name = (
                item.get("repo_name")
                or item.get("full_name")
                or item.get("fullname")
                or ""
            ).strip("/")
            if not full_name or "/" not in full_name:
                continue

            stars = int(item.get("stars") or item.get("stargazers_count") or 0)
            forks = int(item.get("forks") or item.get("forks_count") or 0)
            period_stars = int(
                item.get("stars_inc")
                or item.get("period_stars_reported")
                or item.get("period_stars")
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
                    primary_language=item.get("primary_language") or item.get("language"),
                )
            )
        return candidates
