"""Ingestion pipeline with fault-isolated Signal Sources, lazy ghr_id resolution, and rename self-healing."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from typing import Protocol

from oss_intelligence.lifecycle import LifecycleStateMachine
from oss_intelligence.models import (
    RawSignalCandidate,
    RepositoryMetadata,
    RepositoryRecord,
    is_trending_signal_source,
)
from oss_intelligence.notifications import AppriseProtocol, deliver_batch_digest
from oss_intelligence.store import IntelligenceStore

logger = logging.getLogger(__name__)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class SignalAdapterProtocol(Protocol):
    def fetch_candidates(self) -> list[RawSignalCandidate]: ...


class MetadataClientProtocol(Protocol):
    def get_repository_metadata(self, full_name: str) -> RepositoryMetadata: ...


def _adapter_source_name(adapter: SignalAdapterProtocol) -> str:
    source_attr = getattr(adapter, "signal_source", None)
    if isinstance(source_attr, str) and source_attr:
        return f"{type(adapter).__name__}({source_attr})"
    return type(adapter).__name__


def _merge_candidate_into_repository(
    existing: RepositoryRecord,
    candidate: RawSignalCandidate,
    timestamp: str,
    metadata: RepositoryMetadata | None = None,
) -> tuple[RepositoryRecord, int, int]:
    """Merges a RawSignalCandidate (and optional fresh metadata) into an existing RepositoryRecord.

    Preserves orthogonal lifecycle_state while updating Curation fields and non-regressing star/fork totals.
    """
    merged_is_curated = existing.is_curated or candidate.is_curated
    merged_curated_issue = (
        candidate.curated_issue
        if candidate.is_curated and candidate.curated_issue
        else existing.curated_issue
    )
    merged_curated_category = (
        candidate.curated_category
        if candidate.is_curated and candidate.curated_category
        else existing.curated_category
    )

    meta_stars = metadata.stars_total if metadata is not None else 0
    meta_forks = metadata.forks_total if metadata is not None else 0
    merged_stars = max(candidate.stars, meta_stars, existing.stars_total)
    merged_forks = max(candidate.forks, meta_forks, existing.forks_total)

    updated_repo = replace(
        existing,
        full_name=metadata.full_name if metadata is not None else existing.full_name,
        description=(
            (metadata.description if metadata is not None else None)
            or candidate.description
            or existing.description
        ),
        primary_language=(
            (metadata.primary_language if metadata is not None else None)
            or candidate.primary_language
            or existing.primary_language
        ),
        license_spdx=(
            (metadata.license_spdx if metadata is not None else None) or existing.license_spdx
        ),
        topics=(metadata.topics if metadata is not None and metadata.topics else existing.topics),
        repo_created_at=(
            (metadata.repo_created_at if metadata is not None else None)
            or existing.repo_created_at
        ),
        stars_total=merged_stars,
        forks_total=merged_forks,
        is_curated=merged_is_curated,
        curated_issue=merged_curated_issue,
        curated_category=merged_curated_category,
        last_seen_in_trending_at=(
            timestamp
            if is_trending_signal_source(candidate.signal_source)
            else existing.last_seen_in_trending_at
        ),
        last_updated_at=timestamp,
    )
    snapshot_stars = candidate.stars if candidate.stars > 0 else merged_stars
    snapshot_forks = candidate.forks if candidate.forks > 0 else merged_forks
    return updated_repo, snapshot_stars, snapshot_forks


class IngestionPipeline:
    """Coordinates multi-source Signal ingestion, circuit-breaker isolation, lazy ghr_id anchoring, and Snapshot persistence."""

    def __init__(
        self,
        *,
        store: IntelligenceStore,
        metadata_client: MetadataClientProtocol,
        signal_adapter: SignalAdapterProtocol | None = None,
        signal_adapters: Sequence[SignalAdapterProtocol] | None = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        jitter_range_sec: tuple[float, float] = (0.100, 0.200),
        rng_fn: Callable[[float, float], float] = random.uniform,
        notifier: AppriseProtocol | None = None,
        clock: Callable[[], str] = _utc_now,
    ) -> None:
        if signal_adapters is not None:
            self._signal_adapters: tuple[SignalAdapterProtocol, ...] = tuple(signal_adapters)
        elif signal_adapter is not None:
            self._signal_adapters = (signal_adapter,)
        else:
            raise ValueError("Either signal_adapter or signal_adapters must be provided")

        self._store = store
        self._metadata_client = metadata_client
        self._sleep_fn = sleep_fn
        self._jitter_min_sec, self._jitter_max_sec = jitter_range_sec
        self._rng_fn = rng_fn
        self._notifier = notifier
        self._clock = clock

    def _fetch_all_candidates_isolated(self) -> list[RawSignalCandidate]:
        all_candidates: list[RawSignalCandidate] = []
        for adapter in self._signal_adapters:
            source_label = _adapter_source_name(adapter)
            try:
                batch = adapter.fetch_candidates()
                all_candidates.extend(batch)
            except Exception as exc:
                error_type = type(exc).__name__
                error_msg = str(exc)
                logger.warning(
                    "Signal adapter circuit breaker tripped: source=%s error_type=%s message=%s",
                    source_label,
                    error_type,
                    error_msg,
                    extra={
                        "source": source_label,
                        "error_type": error_type,
                        "error_message": error_msg,
                    },
                )
        return all_candidates

    def run(self, captured_at: str | None = None, *, dry_run: bool = False) -> str | None:
        """Ingest and notify; an explicit captured_at fixes the clock for deterministic replay."""
        timestamp = captured_at or self._clock()
        candidates = self._fetch_all_candidates_isolated()
        uncached_api_calls = 0
        seen_snapshots: set[tuple[int, str, str]] = set()

        for candidate in candidates:
            existing_by_name = self._store.get_repository_by_full_name(candidate.full_name)

            if existing_by_name is not None:
                updated_repo, snapshot_stars, snapshot_forks = _merge_candidate_into_repository(
                    existing=existing_by_name,
                    candidate=candidate,
                    timestamp=timestamp,
                )
                self._store.save_repository(updated_repo)
                target_ghr_id = existing_by_name.ghr_id
            else:
                if uncached_api_calls > 0:
                    delay_sec = self._rng_fn(self._jitter_min_sec, self._jitter_max_sec)
                    self._sleep_fn(delay_sec)

                uncached_api_calls += 1
                try:
                    metadata = self._metadata_client.get_repository_metadata(candidate.full_name)
                except (OSError, ValueError) as exc:
                    logger.warning(
                        "Failed to resolve GitHub repository metadata for %s: %s",
                        candidate.full_name,
                        exc,
                    )
                    continue

                existing_by_id = self._store.get_repository_by_ghr_id(metadata.ghr_id)
                if existing_by_id is not None:
                    healed_repo, snapshot_stars, snapshot_forks = _merge_candidate_into_repository(
                        existing=existing_by_id,
                        candidate=candidate,
                        timestamp=timestamp,
                        metadata=metadata,
                    )
                    self._store.save_repository(healed_repo)
                else:
                    resolved_stars = (
                        candidate.stars if candidate.stars > 0 else metadata.stars_total
                    )
                    resolved_forks = (
                        candidate.forks if candidate.forks > 0 else metadata.forks_total
                    )
                    new_repo = RepositoryRecord(
                        ghr_id=metadata.ghr_id,
                        full_name=metadata.full_name,
                        description=metadata.description or candidate.description,
                        primary_language=metadata.primary_language or candidate.primary_language,
                        license_spdx=metadata.license_spdx,
                        topics=metadata.topics,
                        repo_created_at=metadata.repo_created_at,
                        stars_total=resolved_stars,
                        forks_total=resolved_forks,
                        stars_delta_6h=0,
                        stars_delta_24h=0,
                        stars_delta_7d=0,
                        lifecycle_state="DISCOVERY",
                        is_curated=candidate.is_curated,
                        curated_issue=candidate.curated_issue if candidate.is_curated else None,
                        curated_category=(
                            candidate.curated_category if candidate.is_curated else None
                        ),
                        first_seen_at=timestamp,
                        last_seen_in_trending_at=timestamp,
                        last_updated_at=timestamp,
                    )
                    self._store.save_repository(new_repo)
                    snapshot_stars = resolved_stars
                    snapshot_forks = resolved_forks

                target_ghr_id = metadata.ghr_id

            snapshot_key = (target_ghr_id, timestamp, candidate.signal_source)
            if snapshot_key not in seen_snapshots:
                seen_snapshots.add(snapshot_key)
                self._store.add_snapshot(
                    ghr_id=target_ghr_id,
                    captured_at=timestamp,
                    signal_source=candidate.signal_source,
                    stars=snapshot_stars,
                    forks=snapshot_forks,
                    source_rank=candidate.source_rank,
                    period_stars_reported=candidate.period_stars_reported,
                )

        transition_timestamp = captured_at or self._clock()
        LifecycleStateMachine(self._store).evaluate(
            now=transition_timestamp,
            transition_clock=self._clock if not captured_at else None,
        )
        if self._notifier is not None or dry_run:
            return deliver_batch_digest(
                self._store,
                self._notifier,
                transition_timestamp,
                dry_run=dry_run,
                delivery_clock=self._clock if not captured_at else None,
            )
        return None
