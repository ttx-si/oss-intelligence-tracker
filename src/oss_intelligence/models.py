"""Domain models aligned with GLOSSARY.md."""

from __future__ import annotations

from dataclasses import dataclass

CURATION_SIGNAL_SOURCES: frozenset[str] = frozenset({"hellogithub"})


def is_trending_signal_source(signal_source: str) -> bool:
    """Returns True when signal_source represents a momentum/trending signal rather than editorial Curation."""
    return signal_source not in CURATION_SIGNAL_SOURCES


@dataclass(frozen=True, slots=True)
class RawSignalCandidate:
    """Candidate repository emitted by a Signal Source adapter."""

    full_name: str
    signal_source: str
    stars: int
    forks: int = 0
    source_rank: int | None = None
    period_stars_reported: int = 0
    description: str | None = None
    primary_language: str | None = None
    is_curated: bool = False
    curated_issue: str | None = None
    curated_category: str | None = None


@dataclass(frozen=True, slots=True)
class RepositoryMetadata:
    """Authoritative repository identity and metadata resolved from GitHub REST API."""

    ghr_id: int
    full_name: str
    description: str | None
    primary_language: str | None
    license_spdx: str | None
    topics: tuple[str, ...]
    repo_created_at: str | None
    stars_total: int
    forks_total: int


@dataclass(frozen=True, slots=True)
class RepositoryRecord:
    """Persisted Repository entity keyed by immutable integer ghr_id."""

    ghr_id: int
    full_name: str
    description: str | None
    primary_language: str | None
    license_spdx: str | None
    topics: tuple[str, ...]
    repo_created_at: str | None
    stars_total: int
    forks_total: int
    stars_delta_6h: int
    stars_delta_24h: int
    stars_delta_7d: int
    lifecycle_state: str
    is_curated: bool
    curated_issue: str | None
    curated_category: str | None
    first_seen_at: str
    last_seen_in_trending_at: str
    last_updated_at: str
    last_notified_at: str | None = None
    last_notified_state: str | None = None
    last_curated_notified_at: str | None = None
    surging_entered_at: str | None = None


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    """Time-series Snapshot observation for a Repository."""

    id: int
    ghr_id: int
    captured_at: str
    signal_source: str
    stars: int
    forks: int
    source_rank: int | None
    period_stars_reported: int
