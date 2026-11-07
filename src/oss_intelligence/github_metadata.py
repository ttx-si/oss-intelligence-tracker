"""GitHub REST API client for lazy ghr_id and repository metadata resolution."""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any

from oss_intelligence.models import RepositoryMetadata, normalize_description


class GitHubMetadataClient:
    """SDK-style client for GET /repos/{owner}/{repo}."""

    def __init__(
        self,
        token: str | None = None,
        base_url: str = "https://api.github.com",
        timeout_sec: float = 10.0,
    ) -> None:
        self._token = token if token is not None else os.environ.get("GITHUB_TOKEN")
        self._base_url = base_url.rstrip("/")
        self._timeout_sec = timeout_sec

    def get_repository_metadata(self, full_name: str) -> RepositoryMetadata:
        url = f"{self._base_url}/repos/{full_name.strip('/')}"
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "oss-intelligence-tracker/0.1.0",
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=self._timeout_sec) as resp:
            payload: dict[str, Any] = json.loads(resp.read().decode("utf-8"))

        license_info = payload.get("license") or {}
        topics_raw = payload.get("topics") or []

        return RepositoryMetadata(
            ghr_id=int(payload["id"]),
            full_name=str(payload["full_name"]),
            description=normalize_description(payload.get("description")),
            primary_language=payload.get("language"),
            license_spdx=license_info.get("spdx_id"),
            topics=tuple(str(t) for t in topics_raw),
            repo_created_at=payload.get("created_at"),
            stars_total=int(payload.get("stargazers_count") or 0),
            forks_total=int(payload.get("forks_count") or 0),
        )
