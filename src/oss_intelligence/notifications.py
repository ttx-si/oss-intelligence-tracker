"""Batch Digest delivery through an Apprise-compatible notification boundary."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from typing import Protocol

from oss_intelligence.models import RepositoryRecord
from oss_intelligence.store import IntelligenceStore
from oss_intelligence.time_utils import parse_utc

logger = logging.getLogger(__name__)


class AppriseResultProtocol(Protocol):
    def __bool__(self) -> bool: ...


class AppriseProtocol(Protocol):
    def notify(self, *, title: str, body: str, body_format: str) -> AppriseResultProtocol: ...


def _surging_qualifies(repo: RepositoryRecord) -> bool:
    if repo.lifecycle_state != "SURGING" or repo.surging_entered_at is None:
        return False
    if repo.last_notified_at is None:
        return True
    return parse_utc(repo.surging_entered_at) - parse_utc(repo.last_notified_at) >= timedelta(hours=72)


def _digest_entry(repo: RepositoryRecord, *, include_curation: bool = False) -> str:
    entry = (
        f"- [{repo.full_name}](https://github.com/{repo.full_name})"
        f" — +{repo.stars_delta_24h} stars/24h"
    )
    if include_curation:
        curation_details = [detail for detail in (repo.curated_issue, repo.curated_category) if detail]
        if curation_details:
            entry += " — " + " / ".join(curation_details)
    return entry


def deliver_batch_digest(
    store: IntelligenceStore,
    notifier: AppriseProtocol | None,
    timestamp: str,
    *,
    dry_run: bool = False,
    delivery_clock: Callable[[], str] | None = None,
) -> str | None:
    repositories = store.list_repositories()
    surging = [
        repo
        for repo in repositories
        if _surging_qualifies(repo)
    ]
    curated = [
        repo for repo in repositories if repo.is_curated and repo.last_curated_notified_at is None
    ]
    if not surging and not curated:
        return None

    surging.sort(key=lambda repo: (-repo.stars_delta_24h, repo.full_name, repo.ghr_id))
    curated.sort(key=lambda repo: (-repo.stars_delta_24h, repo.full_name, repo.ghr_id))
    lines: list[str] = []
    if surging:
        lines.append("## 🚀 Surging Breakouts")
        lines.extend(_digest_entry(repo) for repo in surging)
    if curated:
        if lines:
            lines.append("")
        lines.append("## ✨ New Curated Highlights")
        lines.extend(_digest_entry(repo, include_curation=True) for repo in curated)
    body = "\n".join(lines)
    if dry_run:
        return body
    if notifier is None:
        return None
    try:
        delivered = notifier.notify(
            title="Open Source Intelligence", body=body, body_format="markdown"
        )
    except Exception as exc:
        logger.warning("Batch Digest delivery failed: error_type=%s", type(exc).__name__)
        return None
    if delivered:
        store.mark_digest_notified(
            [repo.ghr_id for repo in surging],
            [repo.ghr_id for repo in curated],
            delivery_clock() if delivery_clock is not None else timestamp,
        )
    else:
        logger.warning("Batch Digest delivery was not confirmed")
    return None
