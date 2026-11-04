"""HelloGitHub Markdown editorial Curation Signal Source adapter."""

from __future__ import annotations

import re
import urllib.request
from collections.abc import Callable

from oss_intelligence.models import RawSignalCandidate

_ISSUE_PATTERNS = (
    re.compile(r"第\s*(\d+)\s*期"),
    re.compile(r"\bvol(?:ume)?[-_\s#]*(\d+)\b", re.IGNORECASE),
    re.compile(r"\bissue[-_\s#]*(\d+)\b", re.IGNORECASE),
)

_HEADING_PATTERN = re.compile(r"^(#{2,4})\s+(.+?)\s*$")
_GITHUB_REPO_LINK_PATTERN = re.compile(
    r"\[(?P<label>[^\]]+)\]\(\s*https?://github\.com/(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+?)(?:\.git|/)?(?:[?#][^\s)]*)?\s*\)"
    r"(?:\s*[：:\-—]\s*(?P<desc>.*))?"
)


def _default_hellogithub_markdown_fetcher(
    url: str = "https://raw.githubusercontent.com/521xueweihan/HelloGitHub/master/README.md",
    timeout_sec: float = 10.0,
) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "oss-intelligence-tracker/0.1.0"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        return resp.read().decode("utf-8")


def _extract_issue_identifier(markdown_text: str, explicit_issue: str | None) -> str:
    if explicit_issue:
        return explicit_issue
    for pattern in _ISSUE_PATTERNS:
        match = pattern.search(markdown_text)
        if match:
            return f"vol-{match.group(1)}"
    return "vol-latest"


class HelloGitHubAdapter:
    """Parses HelloGitHub Markdown issue content into curated RawSignalCandidate items."""

    signal_source = "hellogithub"

    def __init__(
        self,
        *,
        markdown_fetcher: Callable[[], str] | None = None,
        issue: str | None = None,
    ) -> None:
        self._markdown_fetcher = markdown_fetcher or _default_hellogithub_markdown_fetcher
        self._issue = issue

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        markdown_text = self._markdown_fetcher()
        if not isinstance(markdown_text, str) or not markdown_text.strip():
            raise ValueError("HelloGitHub Markdown content is empty")

        resolved_issue = _extract_issue_identifier(markdown_text, self._issue)
        current_category = "Uncategorized"
        candidates: list[RawSignalCandidate] = []
        seen_repos: set[str] = set()

        for raw_line in markdown_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            heading_match = _HEADING_PATTERN.match(line)
            if heading_match:
                current_category = heading_match.group(2).strip()
                continue

            for repo_match in _GITHUB_REPO_LINK_PATTERN.finditer(line):
                owner = repo_match.group("owner")
                repo = repo_match.group("repo")
                if owner.lower() == "521xueweihan" and repo.lower() == "hellogithub":
                    continue
                full_name = f"{owner}/{repo}"
                if full_name.lower() in seen_repos:
                    continue
                seen_repos.add(full_name.lower())

                desc = (repo_match.group("desc") or "").strip() or None
                candidates.append(
                    RawSignalCandidate(
                        full_name=full_name,
                        signal_source=self.signal_source,
                        stars=0,
                        forks=0,
                        source_rank=len(candidates) + 1,
                        period_stars_reported=0,
                        description=desc,
                        primary_language=None,
                        is_curated=True,
                        curated_issue=resolved_issue,
                        curated_category=current_category,
                    )
                )

        if not candidates:
            raise ValueError(
                "Malformed or unrecognized HelloGitHub Markdown format: no curated repositories found"
            )

        return candidates
