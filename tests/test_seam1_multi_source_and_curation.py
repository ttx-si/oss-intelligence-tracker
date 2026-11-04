"""Seam 1 integration tests for Ticket 02: Fault-Isolated Multi-Source Spike & Curation Ingestion."""

from __future__ import annotations

import logging
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
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    captured_requests: list[dict[str, Any]] = []

    def fake_search_fetcher(token: str | None, query: str) -> list[dict[str, Any]]:
        captured_requests.append({"token": token, "query": query})
        return [
            {
                "id": 901001,
                "full_name": "browser-use/browser-use",
                "stargazers_count": 3200,
                "forks_count": 280,
                "description": "Make websites accessible for AI agents",
                "language": "Python",
            }
        ]

    gh_search_adapter = GitHubSearchSpikeAdapter(
        token="ghp_test_token_123",
        fetcher=fake_search_fetcher,
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
                repo_created_at="2026-10-30T08:00:00Z",
                stars_total=3200,
                forks_total=280,
            )
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapters=[gh_search_adapter],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    assert len(captured_requests) == 1
    assert captured_requests[0]["token"] == "ghp_test_token_123"
    assert "created:>=" in captured_requests[0]["query"]

    repos = store.list_repositories()
    assert len(repos) == 1
    assert repos[0].ghr_id == 901001
    assert repos[0].full_name == "browser-use/browser-use"
    assert repos[0].stars_total == 3200

    snapshots = store.list_snapshots(ghr_id=901001)
    assert len(snapshots) == 1
    assert snapshots[0].signal_source == "gh_search"
    assert snapshots[0].stars == 3200
    assert snapshots[0].forks == 280
    assert snapshots[0].source_rank == 1


def test_ossinsight_adapter_ingests_trending_metrics_with_ossinsight_source(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")

    ossinsight_adapter = OSSInsightAdapter(
        fetcher=lambda: [
            {
                "repo_name": "duckdb/duckdb",
                "stars": 24500,
                "forks": 1950,
                "stars_inc": 380,
                "description": "DuckDB is an in-process SQL OLAP Database Management System",
                "primary_language": "C++",
            }
        ]
    )
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
