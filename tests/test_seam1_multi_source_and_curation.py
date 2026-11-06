"""Seam 1 integration tests for Ticket 02: Fault-Isolated Multi-Source Spike & Curation Ingestion."""

from __future__ import annotations

import io
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from oss_intelligence.adapters import (
    GitHubSearchSpikeAdapter,
    GtrendingAdapter,
    HelloGitHubAdapter,
    OSSInsightAdapter,
    TrendshiftAdapter,
)
from oss_intelligence.models import (
    RawSignalCandidate,
    RepositoryMetadata,
    RepositoryRecord,
)
from oss_intelligence.pipeline import IngestionPipeline
from oss_intelligence.store import IntelligenceStore


class FakeGitHubMetadataClient:
    """SDK-style system-boundary stub for GET /repos/{owner}/{repo}."""

    def __init__(self, mapping: dict[str, RepositoryMetadata]) -> None:
        self._mapping = dict(mapping)
        self.calls: list[str] = []

    def get_repository_metadata(self, full_name: str) -> RepositoryMetadata:
        self.calls.append(full_name)
        if full_name not in self._mapping:
            raise KeyError(f"Unknown repository in fake metadata client: {full_name}")
        return self._mapping[full_name]


def test_github_search_spike_adapter_ingests_breakout_repos_with_gh_search_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    captured_requests: list[dict[str, Any]] = []

    def fake_search_urlopen(req, timeout=10.0):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        captured_requests.append(
            {
                "url": url,
                "auth": req.get_header("Authorization"),
            }
        )
        if "stars%3A%3E%3D50" in url:
            items = [
                {
                    "id": 901001,
                    "full_name": "browser-use/browser-use",
                    "stargazers_count": 3000,
                    "forks_count": 280,
                    "created_at": "2026-10-29T00:00:00Z",  # 6 days before 2026-11-04 -> 500/day (SURGING)
                    "description": "Make websites accessible for AI agents",
                    "language": "Python",
                },
                {
                    "id": 901002,
                    "full_name": "org/six-day-steady",
                    "stargazers_count": 300,
                    "forks_count": 20,
                    "created_at": "2026-10-29T00:00:00Z",  # 6 days before 2026-11-04 -> 50/day (DISCOVERY, not 300/day!)
                    "description": "Steady 6-day repo",
                    "language": "Go",
                },
            ]
        else:
            items = [
                {
                    "id": 901001,
                    "full_name": "Browser-Use/Browser-Use",
                    "stargazers_count": 3000,
                    "forks_count": 280,
                    "created_at": "2026-10-29T00:00:00Z",
                    "description": "Make websites accessible for AI agents",
                    "language": "Python",
                }
            ]
        return io.BytesIO(json.dumps({"items": items}).encode("utf-8"))

    monkeypatch.setattr(urllib.request, "urlopen", fake_search_urlopen)

    gh_search_adapter = GitHubSearchSpikeAdapter(
        token="ghp_test_token_123",
        now=datetime(2026, 11, 4, 0, 0, 0, tzinfo=timezone.utc),
    )
    metadata_client = FakeGitHubMetadataClient(
        {
            "browser-use/browser-use": RepositoryMetadata(
                ghr_id=901001,
                full_name="browser-use/browser-use",
                description="Make websites accessible for AI agents",
                primary_language="Python",
                license_spdx="MIT",
                topics=("ai", "browser", "agents"),
                repo_created_at="2026-10-29T00:00:00Z",
                stars_total=3000,
                forks_total=280,
            ),
            "org/six-day-steady": RepositoryMetadata(
                ghr_id=901002,
                full_name="org/six-day-steady",
                description="Steady 6-day repo",
                primary_language="Go",
                license_spdx="MIT",
                topics=("go",),
                repo_created_at="2026-10-29T00:00:00Z",
                stars_total=300,
                forks_total=20,
            ),
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[gh_search_adapter],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    assert len(captured_requests) == 2
    assert all(r["auth"] == "Bearer ghp_test_token_123" for r in captured_requests)
    assert all("per_page=100" in r["url"] for r in captured_requests)
    assert any("created%3A%3E%3D2026-10-28+stars%3A%3E%3D50" in r["url"] for r in captured_requests)
    assert any("created%3A%3E%3D2026-10-05+stars%3A%3E%3D200" in r["url"] for r in captured_requests)

    browser_use = store.get_repository_by_ghr_id(901001)
    assert browser_use is not None
    assert browser_use.stars_total == 3000
    assert browser_use.stars_delta_24h == 500
    assert browser_use.lifecycle_state == "SURGING"

    steady = store.get_repository_by_ghr_id(901002)
    assert steady is not None
    assert steady.stars_total == 300
    assert steady.stars_delta_24h == 50
    assert steady.lifecycle_state == "DISCOVERY"

    snapshots = store.list_snapshots(ghr_id=901001)
    assert len(snapshots) == 1
    assert snapshots[0].signal_source == "gh_search"
    assert snapshots[0].stars == 3000
    assert snapshots[0].forks == 280
    assert snapshots[0].period_stars_reported == 500
    assert snapshots[0].source_rank == 1


def test_ossinsight_adapter_ingests_trending_metrics_with_ossinsight_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")

    ossinsight_payload = {
        "type": "sql_endpoint",
        "data": {
            "columns": [{"col": "repo_name"}, {"col": "stars"}, {"col": "forks"}, {"col": "stars_inc"}],
            "rows": [
                {
                    "repo_name": "duckdb/duckdb",
                    "stars": 24500,
                    "forks": 1950,
                    "stars_inc": 380,
                    "description": "DuckDB is an in-process SQL OLAP Database Management System",
                    "primary_language": "C++",
                }
            ],
        },
    }
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=10.0: io.BytesIO(json.dumps(ossinsight_payload).encode("utf-8")),
    )
    ossinsight_adapter = OSSInsightAdapter()
    metadata_client = FakeGitHubMetadataClient(
        {
            "duckdb/duckdb": RepositoryMetadata(
                ghr_id=138400200,
                full_name="duckdb/duckdb",
                description="DuckDB is an in-process SQL OLAP Database Management System",
                primary_language="C++",
                license_spdx="MIT",
                topics=("database", "olap", "sql"),
                repo_created_at="2018-06-25T00:00:00Z",
                stars_total=24500,
                forks_total=1950,
            )
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[ossinsight_adapter],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    repos = store.list_repositories()
    assert len(repos) == 1
    assert repos[0].ghr_id == 138400200
    assert repos[0].full_name == "duckdb/duckdb"

    snapshots = store.list_snapshots(ghr_id=138400200)
    assert len(snapshots) == 1
    assert snapshots[0].signal_source == "ossinsight"
    assert snapshots[0].stars == 24500
    assert snapshots[0].forks == 1950
    assert snapshots[0].period_stars_reported == 380
    assert snapshots[0].source_rank == 1


def test_ossinsight_unavailable_data_quality_trips_circuit_breaker_with_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    unavailable_payload = {
        "type": "sql_endpoint",
        "data": {"columns": [], "rows": [], "result": {"row_count": 0}},
        "data_quality": {
            "status": "unavailable",
            "metric": "github_event_derived_ranking",
            "unavailable_since": "2026-03-01",
            "reason": "capture of those events fell to roughly 0.3% of baseline",
        },
    }
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda req, timeout=10.0: io.BytesIO(json.dumps(unavailable_payload).encode("utf-8")),
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[OSSInsightAdapter()],
        metadata_client=FakeGitHubMetadataClient({}),
        sleep_fn=lambda _: None,
    )
    with caplog.at_level(logging.WARNING):
        pipeline.run(captured_at="2026-11-04T00:00:00Z")

    warning_messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warning_messages) == 1
    assert "OSSInsightAdapter(ossinsight)" in warning_messages[0]
    assert "2026-03-01" in warning_messages[0]
    assert "0.3% of baseline" in warning_messages[0]
    assert store.list_repositories() == []


def test_hellogithub_adapter_updates_orthogonal_curation_without_overwriting_lifecycle_state(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")

    # Pre-populate a SURGING repository in the store
    store.save_repository(
        RepositoryRecord(
            ghr_id=555666777,
            full_name="astral-sh/ruff",
            description="An extremely fast Python linter and code formatter, written in Rust.",
            primary_language="Rust",
            license_spdx="MIT",
            topics=("python", "linter", "rust"),
            repo_created_at="2022-08-01T00:00:00Z",
            stars_total=33000,
            forks_total=1100,
            stars_delta_6h=80,
            stars_delta_24h=310,
            stars_delta_7d=1500,
            lifecycle_state="SURGING",
            is_curated=False,
            curated_issue=None,
            curated_category=None,
            first_seen_at="2026-11-01T00:00:00Z",
            last_seen_in_trending_at="2026-11-03T18:00:00Z",
            last_updated_at="2026-11-03T18:00:00Z",
        )
    )

    sample_hellogithub_markdown = """\
# HelloGitHub 第 104 期

### Python 项目

1、[astral-sh/ruff](https://github.com/astral-sh/ruff)：极速的 Python 代码检查与格式化工具。

### Rust 项目

2、[ratatui/ratatui](https://github.com/ratatui/ratatui)：用于构建终端用户界面 (TUI) 的 Rust 库。
"""

    hellogithub_adapter = HelloGitHubAdapter(
        markdown_fetcher=lambda: sample_hellogithub_markdown,
    )
    metadata_client = FakeGitHubMetadataClient(
        {
            "ratatui/ratatui": RepositoryMetadata(
                ghr_id=666777888,
                full_name="ratatui/ratatui",
                description="A Rust crate for cooking up terminal user interfaces (TUIs).",
                primary_language="Rust",
                license_spdx="MIT",
                topics=("tui", "terminal", "rust"),
                repo_created_at="2023-02-01T00:00:00Z",
                stars_total=11200,
                forks_total=390,
            )
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[hellogithub_adapter],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    # Verify already-cached ruff did not trigger a GitHub metadata API call
    assert metadata_client.calls == ["ratatui/ratatui"]

    ruff = store.get_repository_by_ghr_id(555666777)
    assert ruff is not None
    assert ruff.is_curated is True
    assert ruff.curated_issue == "vol-104"
    assert ruff.curated_category == "Python 项目"
    # CRITICAL: Orthogonal lifecycle_state must remain SURGING and stars must not be zeroed
    assert ruff.lifecycle_state == "SURGING"
    assert ruff.stars_total == 33000

    ratatui = store.get_repository_by_ghr_id(666777888)
    assert ratatui is not None
    assert ratatui.is_curated is True
    assert ratatui.curated_issue == "vol-104"
    assert ratatui.curated_category == "Rust 项目"
    assert ratatui.lifecycle_state == "DISCOVERY"
    assert ratatui.stars_total == 11200


def test_trendshift_adapter_skips_when_api_key_unset_and_ingests_when_configured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    fetcher_calls: list[str] = []

    def fake_trendshift_fetcher(api_key: str) -> list[dict[str, Any]]:
        fetcher_calls.append(api_key)
        return [
            {
                "full_name": "exo-explore/exo",
                "stars": 14800,
                "forks": 920,
                "rank": 1,
                "period_stars": 890,
                "description": "Run your own AI cluster at home with everyday devices",
                "language": "Python",
            }
        ]

    metadata_client = FakeGitHubMetadataClient(
        {
            "exo-explore/exo": RepositoryMetadata(
                ghr_id=812345678,
                full_name="exo-explore/exo",
                description="Run your own AI cluster at home with everyday devices",
                primary_language="Python",
                license_spdx="GPL-3.0",
                topics=("ai", "distributed"),
                repo_created_at="2024-07-01T00:00:00Z",
                stars_total=14800,
                forks_total=920,
            )
        }
    )

    # Case 1: TRENDSHIFT_API_KEY is unset / empty
    monkeypatch.delenv("TRENDSHIFT_API_KEY", raising=False)
    disabled_adapter = TrendshiftAdapter(fetcher=fake_trendshift_fetcher)
    pipeline_disabled = IngestionPipeline(
        store=store,
        signal_adapters=[disabled_adapter],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline_disabled.run(captured_at="2026-11-04T00:00:00Z")

    assert fetcher_calls == []
    assert store.list_repositories() == []

    # Case 2: TRENDSHIFT_API_KEY is configured
    monkeypatch.setenv("TRENDSHIFT_API_KEY", "ts_live_secret_999")
    enabled_adapter = TrendshiftAdapter(fetcher=fake_trendshift_fetcher)
    pipeline_enabled = IngestionPipeline(
        store=store,
        signal_adapters=[enabled_adapter],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline_enabled.run(captured_at="2026-11-04T06:00:00Z")

    assert fetcher_calls == ["ts_live_secret_999"]
    repos = store.list_repositories()
    assert len(repos) == 1
    assert repos[0].ghr_id == 812345678

    snapshots = store.list_snapshots(ghr_id=812345678)
    assert len(snapshots) == 1
    assert snapshots[0].signal_source == "trendshift"
    assert snapshots[0].stars == 14800
    assert snapshots[0].period_stars_reported == 890


def test_circuit_breaker_isolates_failing_adapters_and_logs_structured_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")

    # Broken HelloGitHub adapter where upstream Markdown format completely changed
    broken_hellogithub = HelloGitHubAdapter(
        markdown_fetcher=lambda: "<html><body>Unexpected DOM layout without markdown repos</body></html>"
    )

    # Failing network adapter raising RuntimeError
    def broken_ossinsight_fetcher() -> list[dict[str, Any]]:
        raise RuntimeError("OSSInsight upstream 502 Bad Gateway")

    broken_ossinsight = OSSInsightAdapter(fetcher=broken_ossinsight_fetcher)

    # Healthy Gtrending adapter
    healthy_gtrending = GtrendingAdapter(
        fetcher=lambda: [
            {
                "fullname": "astral-sh/uv",
                "stars": 28500,
                "forks": 820,
                "currentPeriodStars": 640,
                "description": "Fast Python package manager",
                "language": "Rust",
            }
        ]
    )

    metadata_client = FakeGitHubMetadataClient(
        {
            "astral-sh/uv": RepositoryMetadata(
                ghr_id=754891001,
                full_name="astral-sh/uv",
                description="Fast Python package manager",
                primary_language="Rust",
                license_spdx="MIT",
                topics=("python", "rust"),
                repo_created_at="2024-01-15T10:00:00Z",
                stars_total=28500,
                forks_total=820,
            )
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[broken_hellogithub, broken_ossinsight, healthy_gtrending],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )

    with caplog.at_level(logging.WARNING):
        pipeline.run(captured_at="2026-11-04T00:00:00Z")

    # Warnings must be logged for both failing adapters
    warning_messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warning_messages) == 2
    assert any("HelloGitHub" in msg or "hellogithub" in msg.lower() for msg in warning_messages)
    assert any("OSSInsight" in msg or "ossinsight" in msg.lower() for msg in warning_messages)
    assert any("502 Bad Gateway" in msg for msg in warning_messages)

    # Healthy adapter must still complete ingestion cleanly
    repos = store.list_repositories()
    assert len(repos) == 1
    assert repos[0].ghr_id == 754891001
    assert repos[0].full_name == "astral-sh/uv"

    snapshots = store.list_snapshots(ghr_id=754891001)
    assert len(snapshots) == 1
    assert snapshots[0].signal_source == "gtrending"


def test_multi_source_deduplication_by_ghr_id_merges_repository_and_records_source_snapshots(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")

    gtrending_adapter = GtrendingAdapter(
        fetcher=lambda: [
            {
                "fullname": "microsoft/markitdown",
                "stars": 18200,
                "forks": 910,
                "currentPeriodStars": 1200,
                "description": "Python tool for converting files to Markdown",
                "language": "Python",
            }
        ]
    )
    gh_search_adapter = GitHubSearchSpikeAdapter(
        token="ghp_test",
        fetcher=lambda token, query: [
            {
                "full_name": "microsoft/markitdown",
                "stargazers_count": 18250,
                "forks_count": 915,
                "description": "Python tool for converting files and office documents to Markdown.",
                "language": "Python",
            }
        ],
    )
    ossinsight_adapter = OSSInsightAdapter(
        fetcher=lambda: [
            {
                "repo_name": "microsoft/markitdown",
                "stars": 18220,
                "forks": 912,
                "stars_inc": 1180,
                "primary_language": "Python",
            }
        ]
    )
    hellogithub_adapter = HelloGitHubAdapter(
        markdown_fetcher=lambda: (
            "# HelloGitHub 第 105 期\n\n"
            "### Python 项目\n\n"
            "1、[microsoft/markitdown](https://github.com/microsoft/markitdown)：微软开源的文档转 Markdown 工具。\n"
        )
    )

    metadata_client = FakeGitHubMetadataClient(
        {
            "microsoft/markitdown": RepositoryMetadata(
                ghr_id=910203040,
                full_name="microsoft/markitdown",
                description="Python tool for converting files and office documents to Markdown.",
                primary_language="Python",
                license_spdx="MIT",
                topics=("markdown", "llm", "python"),
                repo_created_at="2026-10-29T00:00:00Z",
                stars_total=18250,
                forks_total=915,
            )
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[
            gtrending_adapter,
            gh_search_adapter,
            ossinsight_adapter,
            hellogithub_adapter,
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    # Metadata API must be called only ONCE across all 4 sources for the same repository
    assert metadata_client.calls == ["microsoft/markitdown"]

    # Repository table must have exactly ONE deduplicated record keyed by ghr_id
    repos = store.list_repositories()
    assert len(repos) == 1
    repo = repos[0]
    assert repo.ghr_id == 910203040
    assert repo.full_name == "microsoft/markitdown"
    assert repo.is_curated is True
    assert repo.curated_issue == "vol-105"
    assert repo.curated_category == "Python 项目"
    assert repo.stars_total == 18250

    # Snapshots must be recorded for the distinct signal sources
    snapshots = store.list_snapshots(ghr_id=910203040)
    sources = [s.signal_source for s in snapshots]
    assert "gtrending" in sources
    assert "gh_search" in sources
    assert "ossinsight" in sources
    assert len(set(sources)) == len(sources)


def test_hellogithub_two_stage_issue_discovery_click_redirects_and_nocase_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    # Pre-save repository with mixed casing to verify COLLATE NOCASE lookup
    store.save_repository(
        RepositoryRecord(
            ghr_id=555666777,
            full_name="Clainstone/CBirds",
            description="Existing mixed-case repo",
            primary_language="C",
            license_spdx="MIT",
            topics=("terminal", "c"),
            repo_created_at="2026-01-01T00:00:00Z",
            stars_total=1200,
            forks_total=80,
            stars_delta_6h=10,
            stars_delta_24h=40,
            stars_delta_7d=200,
            lifecycle_state="DISCOVERY",
            is_curated=False,
            curated_issue=None,
            curated_category=None,
            first_seen_at="2026-11-01T00:00:00Z",
            last_seen_in_trending_at="2026-11-03T18:00:00Z",
            last_updated_at="2026-11-03T18:00:00Z",
        )
    )

    readme_index = """\
# HelloGitHub
| [第 127 期](/content/HelloGitHub127.md) | [第 126 期](/content/HelloGitHub126.md) | [第 125 期](/content/HelloGitHub125.md) |
"""
    issue_127 = """\
# 《HelloGitHub》第 127 期
### C 项目
1、[cbirds](https://hellogithub.com/periodical/statistics/click?target=https://github.com/clainstone/cbirds)：终端里看鸟群飞舞。
### 人工智能
2、[EditHere](https://hellogithub.com/periodical/statistics/click?target=https%3A%2F%2Fgithub.com%2FInginnng%2FEditHere)：用截图告诉 AI 该怎么改。
"""
    issue_125 = """\
# 《HelloGitHub》第 125 期
### C 项目
1、[cbirds](https://hellogithub.com/periodical/statistics/click?target=https://github.com/clainstone/cbirds)：旧期重复条目应保留最新期号。
### Python 项目
2、[markitdown](https://github.com/microsoft/markitdown)：文档转 Markdown 工具。
"""
    fetched_urls: list[str] = []

    def fake_urlopen(req, timeout=10.0):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        fetched_urls.append(url)
        if url.endswith("/README.md"):
            return io.BytesIO(readme_index.encode("utf-8"))
        if url.endswith("/content/HelloGitHub127.md"):
            return io.BytesIO(issue_127.encode("utf-8"))
        if url.endswith("/content/HelloGitHub126.md"):
            raise OSError("Simulated transient 503 on issue 126")
        if url.endswith("/content/HelloGitHub125.md"):
            return io.BytesIO(issue_125.encode("utf-8"))
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    metadata_client = FakeGitHubMetadataClient(
        {
            "Inginnng/EditHere": RepositoryMetadata(
                ghr_id=777888999,
                full_name="Inginnng/EditHere",
                description="Edit with AI via screenshots",
                primary_language="C++",
                license_spdx="MIT",
                topics=("ai", "gui"),
                repo_created_at="2026-10-15T00:00:00Z",
                stars_total=850,
                forks_total=45,
            ),
            "microsoft/markitdown": RepositoryMetadata(
                ghr_id=910203040,
                full_name="microsoft/markitdown",
                description="Convert files to Markdown",
                primary_language="Python",
                license_spdx="MIT",
                topics=("markdown", "python"),
                repo_created_at="2026-10-20T00:00:00Z",
                stars_total=5400,
                forks_total=310,
            ),
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[HelloGitHubAdapter(max_issues=3)],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    # Clainstone/CBirds matched case-insensitively without calling metadata_client
    assert metadata_client.calls == ["Inginnng/EditHere", "microsoft/markitdown"]
    assert any(u.endswith("/content/HelloGitHub127.md") for u in fetched_urls)
    assert any(u.endswith("/content/HelloGitHub126.md") for u in fetched_urls)
    assert any(u.endswith("/content/HelloGitHub125.md") for u in fetched_urls)

    cbirds = store.get_repository_by_ghr_id(555666777)
    assert cbirds is not None
    assert cbirds.is_curated is True
    assert cbirds.curated_issue == "vol-127"
    assert cbirds.curated_category == "C 项目"

    edithere = store.get_repository_by_ghr_id(777888999)
    assert edithere is not None
    assert edithere.is_curated is True
    assert edithere.curated_issue == "vol-127"
    assert edithere.curated_category == "人工智能"

    markitdown = store.get_repository_by_ghr_id(910203040)
    assert markitdown is not None
    assert markitdown.is_curated is True
    assert markitdown.curated_issue == "vol-125"
    assert markitdown.curated_category == "Python 项目"


def test_gtrending_multi_language_daily_weekly_matrix_and_velocity_normalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import gtrending

    store = IntelligenceStore(tmp_path / "intelligence.db")
    calls: list[tuple[str, str]] = []

    def fake_fetch_repos(language: str = "", spoken_language_code: str = "", since: str = "daily"):
        calls.append((language, since))
        if language == "rust" and since == "daily":
            raise RuntimeError("Simulated transient GitHub Trending 502 on rust daily")
        if language == "" and since == "daily":
            return [
                {
                    "fullname": "astral-sh/uv",
                    "stars": 28400,
                    "forks": 810,
                    "currentPeriodStars": 220,
                    "description": None,
                    "language": None,
                }
            ]
        if language == "python" and since == "weekly":
            return [
                {
                    "fullname": "Astral-sh/UV",
                    "stars": 28500,
                    "forks": 820,
                    "currentPeriodStars": 1400,
                    "description": "An extremely fast Python package manager",
                    "language": "Rust",
                },
                {
                    "fullname": "org/weekly-steady",
                    "stars": 1500,
                    "forks": 120,
                    "currentPeriodStars": 700,  # 700 / 7 = 100/day (< 150 cold-start SURGING threshold)
                    "description": "Steady weekly mover",
                    "language": "Python",
                },
            ]
        return []

    monkeypatch.setattr(gtrending, "fetch_repos", fake_fetch_repos)

    adapter = GtrendingAdapter(languages=("", "python", "rust"), since_windows=("daily", "weekly"))
    metadata_client = FakeGitHubMetadataClient(
        {
            "astral-sh/uv": RepositoryMetadata(
                ghr_id=754891001,
                full_name="astral-sh/uv",
                description="An extremely fast Python package manager",
                primary_language="Rust",
                license_spdx="MIT",
                topics=("python", "rust"),
                repo_created_at="2024-01-15T10:00:00Z",
                stars_total=28500,
                forks_total=820,
            ),
            "org/weekly-steady": RepositoryMetadata(
                ghr_id=888000111,
                full_name="org/weekly-steady",
                description="Steady weekly mover",
                primary_language="Python",
                license_spdx="MIT",
                topics=("python",),
                repo_created_at="2025-05-01T00:00:00Z",
                stars_total=1500,
                forks_total=120,
            ),
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[adapter],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    assert len(calls) == 6
    assert ("", "daily") in calls
    assert ("python", "weekly") in calls

    uv = store.get_repository_by_ghr_id(754891001)
    assert uv is not None
    assert uv.stars_total == 28500
    assert uv.forks_total == 820
    # Preserved daily's 220/day instead of overwriting with weekly's 1400/7 = 200
    assert uv.stars_delta_24h == 220
    assert uv.lifecycle_state == "SURGING"

    weekly_steady = store.get_repository_by_ghr_id(888000111)
    assert weekly_steady is not None
    # Normalized 700 weekly stars -> 100 daily stars, preventing false-positive SURGING
    assert weekly_steady.stars_delta_24h == 100
    assert weekly_steady.lifecycle_state == "DISCOVERY"


def test_pipeline_skips_deleted_or_unresolvable_repository_metadata_with_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")

    class NotFoundMetadataClient(FakeGitHubMetadataClient):
        def get_repository_metadata(self, full_name: str) -> RepositoryMetadata:
            self.calls.append(full_name)
            if full_name == "deleted-owner/vanished-repo":
                raise urllib.error.HTTPError(
                    f"https://api.github.com/repos/{full_name}",
                    404,
                    "Not Found",
                    {},  # type: ignore[arg-type]
                    None,
                )
            return self._mapping[full_name]

    metadata_client = NotFoundMetadataClient(
        {
            "astral-sh/uv": RepositoryMetadata(
                ghr_id=754891001,
                full_name="astral-sh/uv",
                description="Fast Python package manager",
                primary_language="Rust",
                license_spdx="MIT",
                topics=("python", "rust"),
                repo_created_at="2024-01-15T10:00:00Z",
                stars_total=28500,
                forks_total=820,
            )
        }
    )
    adapter = HelloGitHubAdapter(
        markdown_fetcher=lambda: (
            "# HelloGitHub 第 127 期\n"
            "### Python 项目\n"
            "1、[vanished](https://github.com/deleted-owner/vanished-repo)：已删除项目。\n"
            "2、[uv](https://github.com/astral-sh/uv)：极速包管理器。\n"
        )
    )
    sleeps: list[float] = []
    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[adapter],
        metadata_client=metadata_client,
        sleep_fn=sleeps.append,
    )

    with caplog.at_level(logging.WARNING):
        pipeline.run(captured_at="2026-11-04T00:00:00Z")

    warning_messages = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("deleted-owner/vanished-repo" in msg and "404" in msg for msg in warning_messages)
    assert len(sleeps) == 1
    repos = store.list_repositories()
    assert [r.full_name for r in repos] == ["astral-sh/uv"]



