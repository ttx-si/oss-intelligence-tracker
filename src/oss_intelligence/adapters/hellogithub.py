"""HelloGitHub Markdown editorial Curation Signal Source adapter."""

from __future__ import annotations

import inspect
import logging
import re
import urllib.parse
import urllib.request
from collections.abc import Callable

from oss_intelligence.models import RawSignalCandidate, normalize_description

logger = logging.getLogger(__name__)

_DEFAULT_README_URL = "https://raw.githubusercontent.com/521xueweihan/HelloGitHub/master/README.md"
_ISSUE_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/521xueweihan/HelloGitHub/master/content/HelloGitHub{num}.md"
)

_ISSUE_PATTERNS = (
    re.compile(r"第\s*(\d+)\s*期"),
    re.compile(r"\bvol(?:ume)?[-_\s#]*(\d+)\b", re.IGNORECASE),
    re.compile(r"\bissue[-_\s#]*(\d+)\b", re.IGNORECASE),
)

_ISSUE_INDEX_LINK_PATTERN = re.compile(r"/content/HelloGitHub(\d+)\.md")
_HEADING_PATTERN = re.compile(r"^(#{2,4})\s+(.+?)\s*$")
_MARKDOWN_LINK_PATTERN = re.compile(
    r"\[(?P<label>[^\]]+)\]\(\s*(?P<url>[^\s)]+)\s*\)(?:\s*[：:\-—]\s*(?P<desc>.*))?"
)
_DIRECT_GITHUB_URL_PATTERN = re.compile(
    r"^https?://github\.com/(?P<owner>[A-Za-z0-9_.-]+)/(?P<repo>[A-Za-z0-9_.-]+?)(?:\.git|/)?(?:[?#][^\s)]*)?$"
)


def _default_hellogithub_markdown_fetcher(
    url: str = _DEFAULT_README_URL,
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


def _extract_github_owner_repo(raw_url: str) -> tuple[str, str] | None:
    target_url = raw_url.strip()
    if "hellogithub.com" in target_url and "target=" in target_url:
        parsed = urllib.parse.urlsplit(target_url)
        qs = urllib.parse.parse_qs(parsed.query)
        targets = qs.get("target")
        if targets and targets[0]:
            target_url = urllib.parse.unquote(targets[0]).strip()

    match = _DIRECT_GITHUB_URL_PATTERN.match(target_url)
    if not match:
        return None
    return match.group("owner"), match.group("repo")


class HelloGitHubAdapter:
    """Parses HelloGitHub Markdown issue content into curated RawSignalCandidate items."""

    signal_source = "hellogithub"

    def __init__(
        self,
        *,
        markdown_fetcher: Callable[..., str] | None = None,
        issue: str | None = None,
        max_issues: int = 3,
    ) -> None:
        self._markdown_fetcher = markdown_fetcher or _default_hellogithub_markdown_fetcher
        self._issue = issue
        self._max_issues = max(1, int(max_issues))

    def _fetch_markdown(self, url: str | None = None) -> str:
        sig = inspect.signature(self._markdown_fetcher)
        if url is None or len(sig.parameters) == 0:
            if url is not None and len(sig.parameters) == 0:
                return _default_hellogithub_markdown_fetcher(url=url)
            return self._markdown_fetcher()
        return self._markdown_fetcher(url)

    def _parse_issue_markdown(
        self,
        markdown_text: str,
        *,
        explicit_issue: str | None,
        seen_repos: set[str],
        candidates: list[RawSignalCandidate],
    ) -> None:
        resolved_issue = _extract_issue_identifier(markdown_text, explicit_issue)
        current_category = "Uncategorized"

        for raw_line in markdown_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            heading_match = _HEADING_PATTERN.match(line)
            if heading_match:
                current_category = heading_match.group(2).strip()
                continue

            for link_match in _MARKDOWN_LINK_PATTERN.finditer(line):
                owner_repo = _extract_github_owner_repo(link_match.group("url"))
                if owner_repo is None:
                    continue
                owner, repo = owner_repo
                if owner.lower() == "521xueweihan" and repo.lower() == "hellogithub":
                    continue
                full_name = f"{owner}/{repo}"
                if full_name.lower() in seen_repos:
                    continue
                seen_repos.add(full_name.lower())

                desc = normalize_description(link_match.group("desc"))
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

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        markdown_text = self._fetch_markdown()
        if not isinstance(markdown_text, str) or not markdown_text.strip():
            raise ValueError("HelloGitHub Markdown content is empty")

        candidates: list[RawSignalCandidate] = []
        seen_repos: set[str] = set()

        issue_numbers = sorted(
            {int(m.group(1)) for m in _ISSUE_INDEX_LINK_PATTERN.finditer(markdown_text)},
            reverse=True,
        )
        for issue_num in issue_numbers[: self._max_issues]:
            issue_url = _ISSUE_URL_TEMPLATE.format(num=issue_num)
            try:
                issue_md = self._fetch_markdown(issue_url)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch HelloGitHub issue %s (%s): %s",
                    issue_num,
                    issue_url,
                    exc,
                )
                continue
            if isinstance(issue_md, str) and issue_md.strip():
                self._parse_issue_markdown(
                    issue_md,
                    explicit_issue=self._issue or f"vol-{issue_num}",
                    seen_repos=seen_repos,
                    candidates=candidates,
                )

        # Also parse the initial fetched Markdown directly (supports single-issue Markdown or direct entries)
        self._parse_issue_markdown(
            markdown_text,
            explicit_issue=self._issue,
            seen_repos=seen_repos,
            candidates=candidates,
        )

        if not candidates:
            raise ValueError(
                "Malformed or unrecognized HelloGitHub Markdown format: no curated repositories found"
            )

        return candidates
