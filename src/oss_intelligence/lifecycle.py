"""Orthogonal Lifecycle State Machine and longitudinal velocity calculation."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone

from oss_intelligence.models import (
    RepositoryRecord,
    SnapshotRecord,
    is_trending_signal_source,
)
from oss_intelligence.store import IntelligenceStore

_SECONDS_PER_HOUR = 3600.0
_SECONDS_PER_DAY = 86400.0


def _parse_utc(timestamp: str) -> datetime:
    text = timestamp.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class _ConsolidatedObservation:
    captured_at: str
    captured_dt: datetime
    stars: int
    period_stars_reported: int
    has_trending_source: bool


@dataclass(frozen=True, slots=True)
class _RepoVelocityEvaluation:
    repo: RepositoryRecord
    observations: list[_ConsolidatedObservation]
    latest_stars: int
    delta_6h: int
    delta_24h: int
    delta_7d: int
    days_since_trending: float
    days_tracked: float
    is_cooling: bool


def _consolidate_snapshots(
    snapshots: list[SnapshotRecord],
    now_dt: datetime,
) -> list[_ConsolidatedObservation]:
    by_ts: dict[str, list[SnapshotRecord]] = {}
    for snap in snapshots:
        snap_dt = _parse_utc(snap.captured_at)
        if snap_dt <= now_dt:
            by_ts.setdefault(snap.captured_at, []).append(snap)

    observations: list[_ConsolidatedObservation] = []
    for ts, group in by_ts.items():
        observations.append(
            _ConsolidatedObservation(
                captured_at=ts,
                captured_dt=_parse_utc(ts),
                stars=max(s.stars for s in group),
                period_stars_reported=max(s.period_stars_reported for s in group),
                has_trending_source=any(is_trending_signal_source(s.signal_source) for s in group),
            )
        )
    observations.sort(key=lambda o: o.captured_dt)
    return observations


def _find_closest_observation(
    prior: list[_ConsolidatedObservation],
    latest_dt: datetime,
    target_sec: float,
    max_span_sec: float,
) -> _ConsolidatedObservation | None:
    within_window = [
        obs
        for obs in prior
        if 0 < (latest_dt - obs.captured_dt).total_seconds() <= max_span_sec
    ]
    if not within_window:
        return None
    return min(
        within_window,
        key=lambda obs: abs((latest_dt - obs.captured_dt).total_seconds() - target_sec),
    )


def _compute_velocities(
    repo: RepositoryRecord,
    observations: list[_ConsolidatedObservation],
    now_dt: datetime,
) -> tuple[int, int, int]:
    """Computes (stars_delta_6h, stars_delta_24h, stars_delta_7d) for a Repository at now_dt."""
    if not observations:
        last_seen_dt = _parse_utc(repo.last_seen_in_trending_at)
        age_sec = (now_dt - last_seen_dt).total_seconds()
        delta_6h = repo.stars_delta_6h if age_sec <= 6 * _SECONDS_PER_HOUR else 0
        delta_24h = repo.stars_delta_24h if age_sec <= 24 * _SECONDS_PER_HOUR else 0
        delta_7d = repo.stars_delta_7d if age_sec <= 7 * _SECONDS_PER_DAY else 0
        return delta_6h, delta_24h, delta_7d

    latest = observations[-1]
    age_since_latest_sec = (now_dt - latest.captured_dt).total_seconds()
    prior = observations[:-1]

    # If the only observation is a curation-only snapshot with 0 reported delta on a pre-existing repo
    if (
        len(observations) == 1
        and not latest.has_trending_source
        and latest.period_stars_reported == 0
        and (repo.stars_delta_6h > 0 or repo.stars_delta_24h > 0 or repo.stars_delta_7d > 0)
        and age_since_latest_sec <= 24 * _SECONDS_PER_HOUR
    ):
        return repo.stars_delta_6h, repo.stars_delta_24h, repo.stars_delta_7d

    # 1. stars_delta_6h
    baseline_6h = (
        _find_closest_observation(
            prior,
            latest.captured_dt,
            target_sec=6 * _SECONDS_PER_HOUR,
            max_span_sec=12 * _SECONDS_PER_HOUR,
        )
        if age_since_latest_sec <= 6 * _SECONDS_PER_HOUR
        else None
    )
    delta_6h = max(0, latest.stars - baseline_6h.stars) if baseline_6h is not None else 0

    # 2. stars_delta_24h
    if age_since_latest_sec > 24 * _SECONDS_PER_HOUR:
        delta_24h = 0
    else:
        baseline_24h = _find_closest_observation(
            prior,
            latest.captured_dt,
            target_sec=24 * _SECONDS_PER_HOUR,
            max_span_sec=48 * _SECONDS_PER_HOUR,
        )
        if baseline_24h is None:
            delta_24h = max(0, latest.period_stars_reported)
        else:
            span_24h_sec = (latest.captured_dt - baseline_24h.captured_dt).total_seconds()
            if span_24h_sec < 18 * _SECONDS_PER_HOUR:
                delta_24h = max(0, latest.stars - baseline_24h.stars, latest.period_stars_reported)
            else:
                delta_24h = max(0, latest.stars - baseline_24h.stars)

    # 3. stars_delta_7d
    if age_since_latest_sec > 7 * _SECONDS_PER_DAY:
        delta_7d = 0
    else:
        baseline_7d = _find_closest_observation(
            prior,
            latest.captured_dt,
            target_sec=7 * _SECONDS_PER_DAY,
            max_span_sec=14 * _SECONDS_PER_DAY,
        )
        if baseline_7d is None:
            delta_7d = delta_24h
        else:
            delta_7d = max(0, latest.stars - baseline_7d.stars, delta_24h)

    return delta_6h, delta_24h, delta_7d


class LifecycleStateMachine:
    """Evaluates longitudinal star velocities and orthogonal Lifecycle State transitions."""

    def __init__(self, store: IntelligenceStore) -> None:
        self._store = store

    def evaluate(self, now: str | None = None) -> None:
        timestamp = now or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        now_dt = _parse_utc(timestamp)

        repos = self._store.list_repositories()
        if not repos:
            return

        all_snapshots = self._store.list_snapshots()
        snapshots_by_repo: dict[int, list[SnapshotRecord]] = {}
        for snap in all_snapshots:
            snapshots_by_repo.setdefault(snap.ghr_id, []).append(snap)

        evaluations: list[_RepoVelocityEvaluation] = []
        lang_deltas: dict[str, list[int]] = {}
        lang_earliest_dt: dict[str, datetime] = {}

        for repo in repos:
            repo_snaps = snapshots_by_repo.get(repo.ghr_id, [])
            observations = _consolidate_snapshots(repo_snaps, now_dt)
            delta_6h, delta_24h, delta_7d = _compute_velocities(repo, observations, now_dt)
            latest_stars = max(repo.stars_total, observations[-1].stars if observations else 0)
            days_since_trending = (
                now_dt - _parse_utc(repo.last_seen_in_trending_at)
            ).total_seconds() / _SECONDS_PER_DAY
            days_tracked = (
                now_dt - _parse_utc(repo.first_seen_at)
            ).total_seconds() / _SECONDS_PER_DAY
            is_cooling = days_since_trending >= 14.0 - 1e-9 and delta_24h < 20

            evaluations.append(
                _RepoVelocityEvaluation(
                    repo=repo,
                    observations=observations,
                    latest_stars=latest_stars,
                    delta_6h=delta_6h,
                    delta_24h=delta_24h,
                    delta_7d=delta_7d,
                    days_since_trending=days_since_trending,
                    days_tracked=days_tracked,
                    is_cooling=is_cooling,
                )
            )

            if repo.primary_language and repo.lifecycle_state != "COOLED" and not is_cooling:
                lang = repo.primary_language
                lang_deltas.setdefault(lang, []).append(delta_24h)
                earliest_repo_dt = (
                    observations[0].captured_dt
                    if observations
                    else _parse_utc(repo.first_seen_at)
                )
                if lang not in lang_earliest_dt or earliest_repo_dt < lang_earliest_dt[lang]:
                    lang_earliest_dt[lang] = earliest_repo_dt

        warm_thresholds: dict[str, float] = {}
        for lang, deltas in lang_deltas.items():
            sample_count = len(deltas)
            earliest_dt = lang_earliest_dt[lang]
            history_days = (now_dt - earliest_dt).total_seconds() / _SECONDS_PER_DAY
            if sample_count >= 10 and history_days >= 7.0 - 1e-9:
                language_daily_mean = sum(deltas) / sample_count
                warm_thresholds[lang] = max(language_daily_mean * 3.0, 50.0)

        for ev in evaluations:
            repo = ev.repo
            lang = repo.primary_language
            if lang and lang in warm_thresholds:
                is_surging = ev.delta_24h >= warm_thresholds[lang]
            else:
                is_surging = ev.delta_24h >= 150 or ev.delta_6h >= 50

            if is_surging:
                next_state = "SURGING"
            elif repo.lifecycle_state == "COOLED":
                if ev.days_since_trending < 14.0 - 1e-9:
                    next_state = "DISCOVERY"
                else:
                    next_state = "COOLED"
            elif ev.is_cooling:
                next_state = "COOLED"
            elif (
                ev.days_since_trending < 14.0 - 1e-9
                and ev.days_tracked >= 3.0 - 1e-9
                and ev.delta_24h >= 10
            ):
                next_state = "SUSTAINED"
            else:
                next_state = "DISCOVERY"

            updated = replace(
                repo,
                stars_total=ev.latest_stars,
                stars_delta_6h=ev.delta_6h,
                stars_delta_24h=ev.delta_24h,
                stars_delta_7d=ev.delta_7d,
                lifecycle_state=next_state,
                last_updated_at=timestamp,
            )
            self._store.save_repository(updated)
