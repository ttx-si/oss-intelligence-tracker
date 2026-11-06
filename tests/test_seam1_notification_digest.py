"""Seam 1 integration tests for throttled Batch Digest delivery."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
import tomllib
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from packaging.requirements import Requirement

from oss_intelligence.models import RawSignalCandidate, RepositoryMetadata, RepositoryRecord
from oss_intelligence.pipeline import IngestionPipeline
from oss_intelligence.store import IntelligenceStore


class FakeSignalSource:
    def __init__(self, candidates: list[RawSignalCandidate]) -> None:
        self.candidates = candidates

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        return list(self.candidates)


class FakeGitHubMetadataClient:
    def __init__(self, repositories: dict[str, int]) -> None:
        self.repositories = repositories

    def get_repository_metadata(self, full_name: str) -> RepositoryMetadata:
        return RepositoryMetadata(
            ghr_id=self.repositories[full_name],
            full_name=full_name,
            description=f"Repository {full_name}",
            primary_language="Python",
            license_spdx="MIT",
            topics=(),
            repo_created_at=None,
            stars_total=1000,
            forks_total=0,
        )


class SpyApprise:
    def __init__(self, succeeds: bool = True) -> None:
        self.succeeds = succeeds
        self.messages: list[dict[str, str]] = []

    def notify(self, *, title: str, body: str, body_format: str) -> bool:
        self.messages.append({"title": title, "body": body, "body_format": body_format})
        return self.succeeds


class RaisingApprise(SpyApprise):
    def notify(self, *, title: str, body: str, body_format: str) -> bool:
        super().notify(title=title, body=body, body_format=body_format)
        raise OSError("delivery failed")


class ConfiguredSpyApprise(SpyApprise):
    def __init__(self) -> None:
        super().__init__()
        self.urls: list[str] = []

    def add(self, url: str) -> bool:
        self.urls.append(url)
        return True


def test_slow_run_measures_transition_and_delivery_at_the_actual_times(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    store.save_repository(
        RepositoryRecord(
            ghr_id=1,
            full_name="org/slow",
            description=None,
            primary_language="Python",
            license_spdx=None,
            topics=(),
            repo_created_at=None,
            stars_total=1000,
            forks_total=0,
            stars_delta_6h=0,
            stars_delta_24h=0,
            stars_delta_7d=0,
            lifecycle_state="SUSTAINED",
            is_curated=False,
            curated_issue=None,
            curated_category=None,
            first_seen_at="2026-10-28T03:00:00Z",
            last_seen_in_trending_at="2026-11-03T03:00:00Z",
            last_updated_at="2026-11-03T03:00:00Z",
            last_notified_at="2026-11-01T03:00:00Z",
            last_notified_state="SURGING",
        )
    )
    current_time = datetime(2026, 11, 4, 2, 59, tzinfo=timezone.utc)

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return current_time

    class SlowSource(FakeSignalSource):
        def fetch_candidates(self) -> list[RawSignalCandidate]:
            nonlocal current_time
            current_time += timedelta(minutes=2)
            return super().fetch_candidates()

    class SlowApprise(SpyApprise):
        def notify(self, *, title: str, body: str, body_format: str) -> bool:
            nonlocal current_time
            current_time += timedelta(minutes=3)
            return super().notify(title=title, body=body, body_format=body_format)

    monkeypatch.setattr("oss_intelligence.pipeline.datetime", ControlledDateTime)
    apprise = SlowApprise()
    IngestionPipeline(
        store=store,
        signal_adapters=[
            SlowSource(
                [RawSignalCandidate(
                    "org/slow", "gtrending", 1000, period_stars_reported=200, is_curated=True
                )]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({}),
        notifier=apprise,
    ).run()

    # The run starts a minute before the old cooldown expires, but the actual
    # SURGING entry occurs one minute after it, so the breakout must be sent.
    assert len(apprise.messages) == 1
    assert "org/slow" in apprise.messages[0]["body"]
    assert "Surging Breakouts" in apprise.messages[0]["body"]
    repository = store.get_repository_by_ghr_id(1)
    assert repository.surging_entered_at == "2026-11-04T03:01:00Z"
    assert repository.last_notified_at == "2026-11-04T03:04:00Z"
    assert repository.last_curated_notified_at == "2026-11-04T03:04:00Z"


def test_slow_delivery_starts_cooldown_when_the_notification_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    current_time = datetime(2026, 11, 1, tzinfo=timezone.utc)
    fetch_delay = timedelta(hours=1)

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return current_time

    class SlowSource(FakeSignalSource):
        def fetch_candidates(self) -> list[RawSignalCandidate]:
            nonlocal current_time
            current_time += fetch_delay
            return super().fetch_candidates()

    class SlowApprise(SpyApprise):
        def notify(self, *, title: str, body: str, body_format: str) -> bool:
            nonlocal current_time
            current_time += timedelta(hours=1)
            return super().notify(title=title, body=body, body_format=body_format)

    monkeypatch.setattr("oss_intelligence.pipeline.datetime", ControlledDateTime)
    source = SlowSource(
        [RawSignalCandidate("org/slow-delivery", "gtrending", 1000, period_stars_reported=200)]
    )
    apprise = SlowApprise()
    store = IntelligenceStore(tmp_path / "intelligence.db")
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[source],
        metadata_client=FakeGitHubMetadataClient({"org/slow-delivery": 1}),
        notifier=apprise,
    )

    pipeline.run()  # Fetch completes at 01:00, delivery succeeds at 02:00.
    fetch_delay = timedelta()

    def run(at: str, period_stars: int) -> None:
        source.candidates = [
            RawSignalCandidate(
                "org/slow-delivery", "gtrending", 1000, period_stars_reported=period_stars
            )
        ]
        pipeline.run(captured_at=at)

    run("2026-11-03T20:00:00Z", 20)
    run("2026-11-04T01:30:00Z", 200)
    assert len(apprise.messages) == 1  # Only 71.5 hours since delivery.

    run("2026-11-04T01:45:00Z", 20)
    run("2026-11-04T02:00:00Z", 200)
    assert len(apprise.messages) == 2  # A fresh entry at 72 hours qualifies.
    assert store.get_repository_by_ghr_id(1).last_notified_at == "2026-11-04T02:00:00Z"


def test_write_lock_wait_does_not_backdate_a_surging_entry(tmp_path: Path) -> None:
    db_path = tmp_path / "intelligence.db"
    store = IntelligenceStore(db_path)
    store.save_repository(
        RepositoryRecord(
            ghr_id=1,
            full_name="org/locked",
            description=None,
            primary_language="Python",
            license_spdx=None,
            topics=(),
            repo_created_at=None,
            stars_total=1000,
            forks_total=0,
            stars_delta_6h=0,
            stars_delta_24h=200,
            stars_delta_7d=0,
            lifecycle_state="SUSTAINED",
            is_curated=False,
            curated_issue=None,
            curated_category=None,
            first_seen_at="2026-10-28T03:00:00Z",
            last_seen_in_trending_at="2026-11-04T02:00:00Z",
            last_updated_at="2026-11-04T02:00:00Z",
            last_notified_at="2026-11-01T03:00:00Z",
            last_notified_state="SURGING",
        )
    )
    current_time = "2026-11-04T02:59:00Z"
    clock_seen = threading.Event()

    def clock() -> str:
        clock_seen.set()
        return current_time

    notifier = SpyApprise()
    pipeline = IngestionPipeline(
        store=store, signal_adapters=[], metadata_client=FakeGitHubMetadataClient({}),
        notifier=notifier, clock=clock,
    )

    with sqlite3.connect(db_path) as lock, ThreadPoolExecutor(max_workers=1) as executor:
        lock.execute("BEGIN IMMEDIATE")
        future = executor.submit(pipeline.run)
        try:
            assert clock_seen.wait(timeout=5)
            # Reads can proceed under this lock, but the state write cannot.
            with pytest.raises(TimeoutError):
                future.result(timeout=1)
            current_time = "2026-11-04T03:01:00Z"
        finally:
            lock.rollback()
        future.result(timeout=5)

    assert len(notifier.messages) == 1
    assert "org/locked" in notifier.messages[0]["body"]
    assert store.get_repository_by_ghr_id(1).surging_entered_at == "2026-11-04T03:01:00Z"


def test_spaced_utc_watermarks_allow_an_eligible_surging_entry(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    store.save_repository(
        RepositoryRecord(
            ghr_id=1,
            full_name="org/spaced",
            description=None,
            primary_language="Python",
            license_spdx=None,
            topics=(),
            repo_created_at=None,
            stars_total=1000,
            forks_total=0,
            stars_delta_6h=0,
            stars_delta_24h=200,
            stars_delta_7d=0,
            lifecycle_state="SURGING",
            is_curated=False,
            curated_issue=None,
            curated_category=None,
            first_seen_at="2026-10-28T00:00:00Z",
            last_seen_in_trending_at="2026-11-04T00:00:00Z",
            last_updated_at="2026-11-04T00:00:00Z",
            last_notified_at=" 2026-11-01T08:00:00+08:00 ",
            last_notified_state="SURGING",
            surging_entered_at=" 2026-11-04T00:00:00Z ",
        )
    )
    apprise = SpyApprise()

    IngestionPipeline(
        store=store,
        signal_adapters=[],
        metadata_client=FakeGitHubMetadataClient({}),
        notifier=apprise,
    ).run(captured_at="2026-11-04T00:00:00Z")

    assert len(apprise.messages) == 1
    assert "org/spaced" in apprise.messages[0]["body"]
    assert store.get_repository_by_ghr_id(1).last_notified_at == "2026-11-04T00:00:00Z"


def test_default_cli_path_declares_and_uses_gtrending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from oss_intelligence.cli import main

    monkeypatch.setitem(
        sys.modules,
        "gtrending",
        SimpleNamespace(
            fetch_repos=lambda *args, **kwargs: [
                {"fullname": "org/default-source", "stars": 1000, "currentPeriodStars": 200}
            ]
        ),
    )
    monkeypatch.delenv("TRENDSHIFT_API_KEY", raising=False)

    def fake_urlopen(request, timeout):
        url = request.full_url
        if "/search/repositories?" in url:
            body = {"items": []}
        elif "api.ossinsight.io" in url:
            body = {"data": {"rows": []}}
        elif "raw.githubusercontent.com" in url:
            return io.BytesIO(
                b"# HelloGitHub issue 1\n## Python\n"
                b"[org/default-source](https://github.com/org/default-source)\n"
            )
        elif "/repos/org/default-source" in url:
            body = {"id": 1, "full_name": "org/default-source", "stargazers_count": 1000}
        else:
            raise AssertionError(f"Unexpected URL: {url}")
        return io.BytesIO(json.dumps(body).encode("utf-8"))

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    db_path = tmp_path / "intelligence.db"

    assert main(["--database", str(db_path), "--dry-run"]) == 0
    assert "org/default-source" in capsys.readouterr().out
    assert "gtrending" in {snap.signal_source for snap in IntelligenceStore(db_path).list_snapshots()}

    project_file = Path(__file__).resolve().parents[1] / "pyproject.toml"
    dependencies = tomllib.loads(project_file.read_text(encoding="utf-8"))["project"]["dependencies"]
    requirements = [Requirement(dependency) for dependency in dependencies]
    assert any(
        req.name.lower() == "gtrending" and req.marker is None and "0.5.1" in req.specifier
        for req in requirements
    ), "A clean project installation must install a compatible gtrending distribution"


def test_pipeline_ingests_real_gtrending_with_an_offline_http_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("gtrending")
    import requests

    from oss_intelligence.adapters import GtrendingAdapter

    html = """\
<article>
  <h2>org / real-library</h2>
  <div></div><div></div>
  <div>Python\n1,000\n12\n200 stars today</div>
</article>
"""
    monkeypatch.setattr(requests, "get", lambda url: SimpleNamespace(text=html))

    store = IntelligenceStore(tmp_path / "intelligence.db")
    notifier = SpyApprise()
    IngestionPipeline(
        store=store,
        signal_adapters=[GtrendingAdapter()],
        metadata_client=FakeGitHubMetadataClient({"org/real-library": 1}),
        notifier=notifier,
    ).run(captured_at="2026-11-01T00:00:00Z")

    assert len(notifier.messages) == 1
    assert "org/real-library" in notifier.messages[0]["body"]
    assert "+200 stars/24h" in notifier.messages[0]["body"]
    snapshots = store.list_snapshots()
    assert len(snapshots) == 1
    assert snapshots[0].signal_source == "gtrending"
    assert snapshots[0].stars == 1000
    assert snapshots[0].forks == 12
    assert snapshots[0].period_stars_reported == 200


def test_successful_apprise_result_advances_watermarks(tmp_path: Path) -> None:
    apprise_sdk = pytest.importorskip("apprise")

    class InMemoryService(apprise_sdk.NotifyBase):
        service_name = "In-memory test service"

        def __init__(self) -> None:
            super().__init__()
            self.messages: list[str] = []

        def send(self, body: str, title: str = "", **kwargs) -> bool:
            self.messages.append(body)
            return True

    service = InMemoryService()
    notifier = apprise_sdk.Apprise()
    assert notifier.add(service)
    store = IntelligenceStore(tmp_path / "intelligence.db")
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate(
                    "org/apprise-result", "gtrending", 1000,
                    period_stars_reported=200, is_curated=True,
                )]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/apprise-result": 1}),
        notifier=notifier,
    )

    pipeline.run(captured_at="2026-11-01T00:00:00Z")

    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at == "2026-11-01T00:00:00Z"
    assert repository.last_curated_notified_at == "2026-11-01T00:00:00Z"
    pipeline.run(captured_at="2026-11-01T06:00:00Z")
    assert len(service.messages) == 1


@pytest.mark.parametrize("outcomes", [(False,), (True, False)], ids=["failure", "partial"])
def test_unsuccessful_real_apprise_result_does_not_advance_watermarks(
    tmp_path: Path, outcomes: tuple[bool, ...]
) -> None:
    apprise_sdk = pytest.importorskip("apprise")

    class InMemoryService(apprise_sdk.NotifyBase):
        service_name = "In-memory test service"

        def __init__(self, succeeds: bool) -> None:
            super().__init__()
            self.succeeds = succeeds

        def send(self, body: str, title: str = "", **kwargs) -> bool:
            return self.succeeds

    notifier = apprise_sdk.Apprise()
    services = [InMemoryService(succeeds) for succeeds in outcomes]
    for service in services:
        assert notifier.add(service)
    store = IntelligenceStore(tmp_path / "intelligence.db")
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate(
                    "org/apprise-failure", "gtrending", 1000,
                    period_stars_reported=200, is_curated=True,
                )]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/apprise-failure": 1}),
        notifier=notifier,
    )

    pipeline.run(captured_at="2026-11-01T00:00:00Z")
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at is None
    assert repository.last_curated_notified_at is None

    for service in services:
        service.succeeds = True
    pipeline.run(captured_at="2026-11-01T06:00:00Z")
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at == "2026-11-01T06:00:00Z"
    assert repository.last_curated_notified_at == "2026-11-01T06:00:00Z"


@pytest.mark.parametrize("include_required", [False, True], ids=["optional-only", "mixed"])
def test_failed_optional_apprise_delivery_is_retried_without_advancing_watermarks(
    tmp_path: Path, include_required: bool
) -> None:
    apprise_sdk = pytest.importorskip("apprise")
    if not hasattr(apprise_sdk, "AppriseResult"):
        pytest.skip("Structured Apprise results are required to inspect optional failures")

    class InMemoryService(apprise_sdk.NotifyBase):
        service_name = "In-memory test service"

        def __init__(self, *, optional: bool, succeeds: bool) -> None:
            super().__init__(optional=optional)
            self.succeeds = succeeds
            self.messages: list[str] = []

        def send(self, body: str, title: str = "", **kwargs) -> bool:
            self.messages.append(body)
            return self.succeeds

    optional = InMemoryService(optional=True, succeeds=False)
    notifier = apprise_sdk.Apprise()
    if include_required:
        assert notifier.add(InMemoryService(optional=False, succeeds=True))
    assert notifier.add(optional)
    store = IntelligenceStore(tmp_path / "intelligence.db")
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate(
                    "org/optional", "gtrending", 1000,
                    period_stars_reported=200, is_curated=True,
                )]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/optional": 1}),
        notifier=notifier,
    )

    pipeline.run(captured_at="2026-11-01T00:00:00Z")
    repository = store.get_repository_by_ghr_id(1)
    assert len(optional.messages) == 1
    assert repository.last_notified_at is None
    assert repository.last_notified_state is None
    assert repository.last_curated_notified_at is None

    optional.succeeds = True
    pipeline.run(captured_at="2026-11-01T06:00:00Z")
    repository = store.get_repository_by_ghr_id(1)
    assert len(optional.messages) == 2
    assert repository.last_notified_at == "2026-11-01T06:00:00Z"
    assert repository.last_curated_notified_at == "2026-11-01T06:00:00Z"


def test_successful_apprise_retry_advances_watermarks(tmp_path: Path) -> None:
    apprise_sdk = pytest.importorskip("apprise")
    if not hasattr(apprise_sdk, "AppriseResult"):
        pytest.skip("Structured Apprise results are required to inspect retries")

    class InMemoryRetryService(apprise_sdk.NotifyBase):
        service_name = "In-memory test service"

        def __init__(self) -> None:
            super().__init__(optional=True, retry=1, wait=0)
            self.attempts = 0

        def send(self, body: str, title: str = "", **kwargs) -> bool:
            self.attempts += 1
            return self.attempts > 1

    service = InMemoryRetryService()
    notifier = apprise_sdk.Apprise()
    assert notifier.add(service)
    store = IntelligenceStore(tmp_path / "intelligence.db")
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate(
                    "org/apprise-retry", "gtrending", 1000,
                    period_stars_reported=200, is_curated=True,
                )]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/apprise-retry": 1}),
        notifier=notifier,
    ).run(captured_at="2026-11-01T00:00:00Z")

    assert service.attempts == 2
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at == "2026-11-01T00:00:00Z"
    assert repository.last_curated_notified_at == "2026-11-01T00:00:00Z"


def test_first_surging_repository_is_delivered_and_watermarked(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    apprise = SpyApprise()
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate("org/breakout", "gtrending", 1000, period_stars_reported=200)]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/breakout": 1}),
        notifier=apprise,
    )

    pipeline.run(captured_at="2026-11-01T00:00:00Z")

    assert len(apprise.messages) == 1
    assert apprise.messages[0]["body_format"] == "markdown"
    assert "Surging Breakouts" in apprise.messages[0]["body"]
    assert "org/breakout" in apprise.messages[0]["body"]
    repository = store.get_repository_by_ghr_id(1)
    assert repository is not None
    assert repository.lifecycle_state == "SURGING"
    assert repository.last_notified_at == "2026-11-01T00:00:00Z"
    assert repository.last_notified_state == "SURGING"
    assert repository.last_curated_notified_at is None


def test_new_curation_is_delivered_once_without_changing_surging_watermarks(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    apprise = SpyApprise()
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [
                    RawSignalCandidate(
                        "org/curated",
                        "hellogithub",
                        1000,
                        is_curated=True,
                        curated_issue="vol-104",
                        curated_category="Python",
                    )
                ]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/curated": 2}),
        notifier=apprise,
    )

    pipeline.run(captured_at="2026-11-01T00:00:00Z")
    pipeline.run(captured_at="2026-11-01T06:00:00Z")

    assert len(apprise.messages) == 1
    assert "New Curated Highlights" in apprise.messages[0]["body"]
    assert "org/curated" in apprise.messages[0]["body"]
    repository = store.get_repository_by_ghr_id(2)
    assert repository is not None
    assert repository.lifecycle_state == "DISCOVERY"
    assert repository.last_curated_notified_at == "2026-11-01T00:00:00Z"
    assert repository.last_notified_at is None
    assert repository.last_notified_state is None


def test_one_digest_groups_and_sorts_all_qualifying_repositories(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    apprise = SpyApprise()
    source = FakeSignalSource(
        [
            RawSignalCandidate("org/surge-low", "gtrending", 1000, period_stars_reported=160),
            RawSignalCandidate(
                "org/curated-low", "hellogithub", 1000, period_stars_reported=60, is_curated=True
            ),
            RawSignalCandidate(
                "org/both", "gtrending", 1000, period_stars_reported=300,
                is_curated=True, curated_issue="vol-104", curated_category="Python",
            ),
            RawSignalCandidate("org/surge-high", "gtrending", 1000, period_stars_reported=500),
        ]
    )
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[source],
        metadata_client=FakeGitHubMetadataClient(
            {"org/surge-low": 1, "org/curated-low": 2, "org/both": 3, "org/surge-high": 4}
        ),
        notifier=apprise,
        sleep_fn=lambda _: None,
    )

    pipeline.run(captured_at="2026-11-01T00:00:00Z")

    assert len(apprise.messages) == 1
    body = apprise.messages[0]["body"]
    surging, curated = body.split("## ✨ New Curated Highlights")
    assert surging.index("org/surge-high") < surging.index("org/both") < surging.index("org/surge-low")
    assert curated.index("org/both") < curated.index("org/curated-low")
    assert "https://github.com/org/both" in body
    assert "500" in surging
    assert "vol-104" in curated
    assert "Python" in curated
    both = store.get_repository_by_ghr_id(3)
    assert both is not None
    assert both.last_notified_at == "2026-11-01T00:00:00Z"
    assert both.last_curated_notified_at == "2026-11-01T00:00:00Z"


def test_continuous_and_cooldown_flapping_are_suppressed_but_later_reentry_notifies(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    apprise = SpyApprise()
    source = FakeSignalSource([])
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[source],
        metadata_client=FakeGitHubMetadataClient({"org/flapping": 1}),
        notifier=apprise,
    )

    def run(at: str, period_stars: int, stars: int = 1000) -> None:
        source.candidates = [
            RawSignalCandidate("org/flapping", "gtrending", stars, period_stars_reported=period_stars)
        ]
        pipeline.run(captured_at=at)

    run("2026-10-28T00:00:00Z", 20)
    run("2026-11-01T00:00:00Z", 200)
    assert len(apprise.messages) == 1

    run("2026-11-01T06:00:00Z", 200)
    assert store.get_repository_by_ghr_id(1).lifecycle_state == "SURGING"
    run("2026-11-01T12:00:00Z", 20)
    assert store.get_repository_by_ghr_id(1).lifecycle_state == "SUSTAINED"
    run("2026-11-01T18:00:00Z", 200, stars=1050)
    assert store.get_repository_by_ghr_id(1).lifecycle_state == "SURGING"
    assert len(apprise.messages) == 1

    # The entry at 18 hours was suppressed, not deferred until the window ends.
    run("2026-11-04T00:00:00Z", 200, stars=1050)
    assert store.get_repository_by_ghr_id(1).lifecycle_state == "SURGING"
    assert len(apprise.messages) == 1

    run("2026-11-04T06:00:00Z", 20, stars=1050)
    assert store.get_repository_by_ghr_id(1).lifecycle_state == "SUSTAINED"
    run("2026-11-04T12:00:00Z", 200, stars=1100)
    assert store.get_repository_by_ghr_id(1).lifecycle_state == "SURGING"
    assert len(apprise.messages) == 2
    assert store.get_repository_by_ghr_id(1).last_notified_at == "2026-11-04T12:00:00Z"


def test_reentry_at_exactly_72_hours_qualifies_but_one_minute_early_does_not(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    apprise = SpyApprise()
    source = FakeSignalSource([])
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[source],
        metadata_client=FakeGitHubMetadataClient({"org/early": 1, "org/exact": 2}),
        notifier=apprise,
        sleep_fn=lambda _: None,
    )

    def run(at: str, early_stars: int, exact_stars: int, period: int) -> None:
        source.candidates = [
            RawSignalCandidate("org/early", "gtrending", early_stars, period_stars_reported=period),
            RawSignalCandidate("org/exact", "gtrending", exact_stars, period_stars_reported=period),
        ]
        pipeline.run(captured_at=at)

    run("2026-10-28T00:00:00Z", 1000, 1000, 20)
    run("2026-11-01T08:00:00+08:00", 1000, 1000, 200)
    run("2026-11-03T22:00:00Z", 1000, 1000, 20)
    assert all(repo.lifecycle_state == "SUSTAINED" for repo in store.list_repositories())
    run("2026-11-03T23:59:00Z", 1050, 1000, 20)
    assert len(apprise.messages) == 1
    run("2026-11-04T00:00:00Z", 1050, 1050, 20)

    assert len(apprise.messages) == 2
    assert "org/exact" in apprise.messages[1]["body"]
    assert "org/early" not in apprise.messages[1]["body"]


def test_delivery_exception_leaves_both_watermarks_for_retry_in_next_run(tmp_path: Path) -> None:
    db_path = tmp_path / "intelligence.db"
    source = FakeSignalSource(
        [
            RawSignalCandidate(
                "org/both", "gtrending", 1000, period_stars_reported=200, is_curated=True
            )
        ]
    )
    metadata = FakeGitHubMetadataClient({"org/both": 1})
    failing = RaisingApprise()
    store = IntelligenceStore(db_path)

    IngestionPipeline(
        store=store, signal_adapters=[source], metadata_client=metadata, notifier=failing
    ).run(captured_at="2026-11-01T00:00:00Z")

    assert len(failing.messages) == 1
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at is None
    assert repository.last_notified_state is None
    assert repository.last_curated_notified_at is None

    # A fresh pipeline and store simulate the next scheduled process.
    succeeded = SpyApprise()
    store = IntelligenceStore(db_path)
    IngestionPipeline(
        store=store, signal_adapters=[source], metadata_client=metadata, notifier=succeeded
    ).run(captured_at="2026-11-01T06:00:00Z")

    assert len(succeeded.messages) == 1
    assert "Surging Breakouts" in succeeded.messages[0]["body"]
    assert "New Curated Highlights" in succeeded.messages[0]["body"]
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at == "2026-11-01T06:00:00Z"
    assert repository.last_notified_state == "SURGING"
    assert repository.last_curated_notified_at == "2026-11-01T06:00:00Z"


def test_failed_delivery_preserves_prior_watermarks_and_retries_the_same_reentry(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    apprise = SpyApprise()
    source = FakeSignalSource([])
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[source],
        metadata_client=FakeGitHubMetadataClient({"org/retry": 1}),
        notifier=apprise,
    )

    def run(at: str, stars: int, period: int, *, curated: bool = False) -> None:
        source.candidates = [
            RawSignalCandidate(
                "org/retry", "gtrending", stars, period_stars_reported=period, is_curated=curated
            )
        ]
        pipeline.run(captured_at=at)

    run("2026-10-28T00:00:00Z", 1000, 20)
    run("2026-11-01T00:00:00Z", 1000, 200)
    run("2026-11-03T23:00:00Z", 1000, 20)
    apprise.succeeds = False
    run("2026-11-04T00:00:00Z", 1050, 200, curated=True)

    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at == "2026-11-01T00:00:00Z"
    assert repository.last_notified_state == "SURGING"
    assert repository.last_curated_notified_at is None
    assert len(apprise.messages) == 2

    apprise.succeeds = True
    run("2026-11-04T06:00:00Z", 1050, 200)
    assert len(apprise.messages) == 3
    assert "Surging Breakouts" in apprise.messages[-1]["body"]
    assert "New Curated Highlights" in apprise.messages[-1]["body"]
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at == "2026-11-04T06:00:00Z"
    assert repository.last_curated_notified_at == "2026-11-04T06:00:00Z"


def test_watermark_update_is_atomic_if_sqlite_rejects_part_of_the_digest(tmp_path: Path) -> None:
    db_path = tmp_path / "intelligence.db"
    store = IntelligenceStore(db_path)
    source = FakeSignalSource(
        [
            RawSignalCandidate(
                "org/both", "gtrending", 1000, period_stars_reported=200, is_curated=True
            )
        ]
    )
    metadata = FakeGitHubMetadataClient({"org/both": 1})
    IngestionPipeline(store=store, signal_adapters=[source], metadata_client=metadata).run(
        captured_at="2026-11-01T00:00:00Z"
    )
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """CREATE TRIGGER fail_curated_watermark
               BEFORE UPDATE OF last_curated_notified_at ON repositories
               WHEN NEW.last_curated_notified_at IS NOT NULL
               BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END"""
        )
    apprise = SpyApprise()
    pipeline = IngestionPipeline(
        store=store, signal_adapters=[], metadata_client=metadata, notifier=apprise
    )

    with pytest.raises(sqlite3.IntegrityError, match="simulated write failure"):
        pipeline.run(captured_at="2026-11-01T00:00:00Z")

    assert len(apprise.messages) == 1
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at is None
    assert repository.last_notified_state is None
    assert repository.last_curated_notified_at is None

    with sqlite3.connect(db_path) as connection:
        connection.execute("DROP TRIGGER fail_curated_watermark")
    pipeline.run(captured_at="2026-11-01T06:00:00Z")
    assert len(apprise.messages) == 2
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at == "2026-11-01T06:00:00Z"
    assert repository.last_curated_notified_at == "2026-11-01T06:00:00Z"


def test_pending_surging_notification_survives_sql_dump_and_restore(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "original.db")
    failed = SpyApprise(succeeds=False)
    metadata = FakeGitHubMetadataClient({"org/pending": 1})
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate("org/pending", "gtrending", 1000, period_stars_reported=200)]
            )
        ],
        metadata_client=metadata,
        notifier=failed,
    ).run(captured_at="2026-11-01T00:00:00Z")
    dump = store.export_sql_dump(tmp_path / "state.sql")

    restored = IntelligenceStore(tmp_path / "restored.db")
    restored.import_sql_dump(dump)
    apprise = SpyApprise()
    IngestionPipeline(
        store=restored, signal_adapters=[], metadata_client=metadata, notifier=apprise
    ).run(captured_at="2026-11-01T06:00:00Z")

    assert len(apprise.messages) == 1
    assert "org/pending" in apprise.messages[0]["body"]
    assert restored.get_repository_by_ghr_id(1).last_notified_at == "2026-11-01T06:00:00Z"


def test_dry_run_previews_digest_without_sending_or_advancing_watermarks(tmp_path: Path) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    apprise = SpyApprise()
    source = FakeSignalSource(
        [
            RawSignalCandidate(
                "org/preview", "gtrending", 1000, period_stars_reported=200, is_curated=True
            )
        ]
    )
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[source],
        metadata_client=FakeGitHubMetadataClient({"org/preview": 1}),
        notifier=apprise,
    )

    preview = pipeline.run(captured_at="2026-11-01T00:00:00Z", dry_run=True)

    assert preview is not None
    assert "org/preview" in preview
    assert "Surging Breakouts" in preview
    assert "New Curated Highlights" in preview
    assert apprise.messages == []
    repository = store.get_repository_by_ghr_id(1)
    assert repository.last_notified_at is None
    assert repository.last_notified_state is None
    assert repository.last_curated_notified_at is None

    pipeline.run(captured_at="2026-11-01T06:00:00Z")
    assert len(apprise.messages) == 1
    assert store.get_repository_by_ghr_id(1).last_notified_at == "2026-11-01T06:00:00Z"


def test_pipeline_accepts_a_legacy_sql_dump_and_delivers_its_unnotified_surge(tmp_path: Path) -> None:
    legacy_dump = tmp_path / "legacy.sql"
    legacy_dump.write_text(
        """\
CREATE TABLE repositories (
    ghr_id INTEGER PRIMARY KEY, full_name TEXT NOT NULL UNIQUE, description TEXT,
    primary_language TEXT, license_spdx TEXT, topics_json TEXT NOT NULL DEFAULT '[]',
    repo_created_at TEXT, stars_total INTEGER NOT NULL DEFAULT 0, forks_total INTEGER NOT NULL DEFAULT 0,
    stars_delta_6h INTEGER NOT NULL DEFAULT 0, stars_delta_24h INTEGER NOT NULL DEFAULT 0,
    stars_delta_7d INTEGER NOT NULL DEFAULT 0, lifecycle_state TEXT NOT NULL DEFAULT 'DISCOVERY',
    is_curated INTEGER NOT NULL DEFAULT 0, curated_issue TEXT, curated_category TEXT,
    first_seen_at TEXT NOT NULL, last_seen_in_trending_at TEXT NOT NULL, last_updated_at TEXT NOT NULL,
    last_notified_at TEXT, last_notified_state TEXT, last_curated_notified_at TEXT
);
CREATE TABLE snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ghr_id INTEGER NOT NULL, captured_at TEXT NOT NULL,
    signal_source TEXT NOT NULL, stars INTEGER NOT NULL, forks INTEGER NOT NULL DEFAULT 0,
    source_rank INTEGER, period_stars_reported INTEGER DEFAULT 0
);
INSERT INTO repositories (
    ghr_id, full_name, stars_total, stars_delta_24h, lifecycle_state,
    first_seen_at, last_seen_in_trending_at, last_updated_at
) VALUES (1, 'org/legacy', 1000, 200, 'SURGING',
          '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z');
INSERT INTO repositories (
    ghr_id, full_name, stars_total, stars_delta_24h, lifecycle_state,
    first_seen_at, last_seen_in_trending_at, last_updated_at, last_notified_at, last_notified_state
) VALUES (2, 'org/already-notified', 1000, 200, 'SURGING',
          '2026-10-28T00:00:00Z', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z',
          '2026-10-28T00:00:00Z', 'SURGING');
""",
        encoding="utf-8",
    )
    store = IntelligenceStore(tmp_path / "intelligence.db")
    store.import_sql_dump(legacy_dump)
    apprise = SpyApprise()

    IngestionPipeline(
        store=store,
        signal_adapters=[],
        metadata_client=FakeGitHubMetadataClient({}),
        notifier=apprise,
    ).run(captured_at="2026-11-01T00:00:00Z")

    assert len(apprise.messages) == 1
    assert "org/legacy" in apprise.messages[0]["body"]
    assert "org/already-notified" not in apprise.messages[0]["body"]
    assert store.get_repository_by_ghr_id(1).last_notified_at == "2026-11-01T00:00:00Z"


def test_dry_run_command_previews_pending_notifications_without_configuration(tmp_path: Path) -> None:
    db_path = tmp_path / "intelligence.db"
    store = IntelligenceStore(db_path)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate("org/preview", "gtrending", 1000, period_stars_reported=200)]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/preview": 1}),
    ).run(captured_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    env = os.environ.copy()
    env.pop("APPRISE_URLS", None)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")

    result = subprocess.run(
        [
            sys.executable, "-m", "oss_intelligence", "--database", str(db_path),
            "--notifications-only", "--dry-run",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Surging Breakouts" in result.stdout
    assert "org/preview" in result.stdout
    assert store.get_repository_by_ghr_id(1).last_notified_at is None


def test_dry_run_command_handles_legacy_windows_output_encoding(tmp_path: Path) -> None:
    db_path = tmp_path / "intelligence.db"
    store = IntelligenceStore(db_path)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate("org/preview", "gtrending", 1000, period_stars_reported=200)]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/preview": 1}),
    ).run(captured_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "gbk"
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")

    result = subprocess.run(
        [
            sys.executable, "-m", "oss_intelligence", "--database", str(db_path),
            "--notifications-only", "--dry-run",
        ],
        capture_output=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr.decode("gbk", errors="replace")
    assert "🚀 Surging Breakouts" in result.stdout.decode("utf-8")


def test_command_configures_apprise_and_delivers_pending_digest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from oss_intelligence.cli import main

    db_path = tmp_path / "intelligence.db"
    store = IntelligenceStore(db_path)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalSource(
                [RawSignalCandidate("org/configured", "gtrending", 1000, period_stars_reported=200)]
            )
        ],
        metadata_client=FakeGitHubMetadataClient({"org/configured": 1}),
    ).run(captured_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    apprise = ConfiguredSpyApprise()
    monkeypatch.setitem(sys.modules, "apprise", SimpleNamespace(Apprise=lambda: apprise))
    monkeypatch.setenv("APPRISE_URLS", "test://first\ntest://second")

    assert main(["--database", str(db_path), "--notifications-only"]) == 0

    assert apprise.urls == ["test://first", "test://second"]
    assert len(apprise.messages) == 1
    assert apprise.messages[0]["body_format"] == "markdown"
    assert "org/configured" in apprise.messages[0]["body"]
    assert store.get_repository_by_ghr_id(1).last_notified_state == "SURGING"
