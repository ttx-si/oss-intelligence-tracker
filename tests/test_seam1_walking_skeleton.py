"""Seam 1 integration tests for Ticket 01: Walking Skeleton."""

from __future__ import annotations

from pathlib import Path

from oss_intelligence.adapters.gtrending import GtrendingAdapter
from oss_intelligence.models import RawSignalCandidate, RepositoryMetadata
from oss_intelligence.pipeline import IngestionPipeline
from oss_intelligence.store import IntelligenceStore


class FakeGtrendingAdapter:
    """System-boundary stub for Gtrending Signal Source."""

    def __init__(self, candidates: list[RawSignalCandidate]) -> None:
        self._candidates = list(candidates)

    def fetch_candidates(self) -> list[RawSignalCandidate]:
        return list(self._candidates)


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


def test_ingests_gtrending_candidates_and_creates_repositories_and_snapshots(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "intelligence.db"
    store = IntelligenceStore(db_path)

    adapter = FakeGtrendingAdapter(
        [
            RawSignalCandidate(
                full_name="astral-sh/uv",
                signal_source="gtrending",
                stars=28500,
                forks=820,
                source_rank=1,
                period_stars_reported=640,
                description="An extremely fast Python package and project manager.",
                primary_language="Rust",
            )
        ]
    )
    metadata_client = FakeGitHubMetadataClient(
        {
            "astral-sh/uv": RepositoryMetadata(
                ghr_id=754891001,
                full_name="astral-sh/uv",
                description="An extremely fast Python package and project manager, written in Rust.",
                primary_language="Rust",
                license_spdx="MIT",
                topics=("python", "packaging", "rust"),
                repo_created_at="2024-01-15T10:00:00Z",
                stars_total=28510,
                forks_total=822,
            )
        }
    )

    pipeline = IngestionPipeline(
        store=store,
        signal_adapter=adapter,
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )

    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    repositories = store.list_repositories()
    assert len(repositories) == 1
    repo = repositories[0]
    assert repo.ghr_id == 754891001
    assert repo.full_name == "astral-sh/uv"
    assert repo.primary_language == "Rust"
    assert repo.license_spdx == "MIT"
    assert repo.topics == ("python", "packaging", "rust")
    assert repo.stars_total == 28500
    assert repo.lifecycle_state == "DISCOVERY"

    snapshots = store.list_snapshots(ghr_id=754891001)
    assert len(snapshots) == 1
    snap = snapshots[0]
    assert snap.ghr_id == 754891001
    assert snap.signal_source == "gtrending"
    assert snap.captured_at == "2026-11-04T00:00:00Z"
    assert snap.stars == 28500
    assert snap.forks == 820
    assert snap.source_rank == 1
    assert snap.period_stars_reported == 640


def test_skips_github_metadata_api_when_repository_full_name_is_already_cached(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {
            "pydantic/pydantic-ai": RepositoryMetadata(
                ghr_id=88001122,
                full_name="pydantic/pydantic-ai",
                description="Agent Framework / shim to use Pydantic with LLMs",
                primary_language="Python",
                license_spdx="MIT",
                topics=("ai", "llm"),
                repo_created_at="2024-06-01T00:00:00Z",
                stars_total=4100,
                forks_total=310,
            )
        }
    )

    first_adapter = FakeGtrendingAdapter(
        [
            RawSignalCandidate(
                full_name="pydantic/pydantic-ai",
                signal_source="gtrending",
                stars=4100,
                forks=310,
                source_rank=2,
                period_stars_reported=210,
            )
        ]
    )
    pipeline = IngestionPipeline(
        store=store,
        signal_adapter=first_adapter,
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")
    assert metadata_client.calls == ["pydantic/pydantic-ai"]

    # Second run 6 hours later with the same full_name
    second_adapter = FakeGtrendingAdapter(
        [
            RawSignalCandidate(
                full_name="pydantic/pydantic-ai",
                signal_source="gtrending",
                stars=4350,
                forks=325,
                source_rank=1,
                period_stars_reported=250,
            )
        ]
    )
    second_pipeline = IngestionPipeline(
        store=store,
        signal_adapter=second_adapter,
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    )
    second_pipeline.run(captured_at="2026-11-04T06:00:00Z")

    # Metadata API must NOT be called a second time
    assert metadata_client.calls == ["pydantic/pydantic-ai"]

    repos = store.list_repositories()
    assert len(repos) == 1
    assert repos[0].stars_total == 4350

    snapshots = store.list_snapshots(ghr_id=88001122)
    assert len(snapshots) == 2
    assert [s.stars for s in snapshots] == [4100, 4350]


def test_enforces_100_to_200ms_jitter_pacing_between_uncached_metadata_calls(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {
            "org/repo-one": RepositoryMetadata(
                ghr_id=101,
                full_name="org/repo-one",
                description="Repo 1",
                primary_language="Go",
                license_spdx="Apache-2.0",
                topics=(),
                repo_created_at="2025-01-01T00:00:00Z",
                stars_total=100,
                forks_total=10,
            ),
            "org/repo-two": RepositoryMetadata(
                ghr_id=102,
                full_name="org/repo-two",
                description="Repo 2",
                primary_language="Go",
                license_spdx="Apache-2.0",
                topics=(),
                repo_created_at="2025-01-02T00:00:00Z",
                stars_total=200,
                forks_total=20,
            ),
            "org/repo-three": RepositoryMetadata(
                ghr_id=103,
                full_name="org/repo-three",
                description="Repo 3",
                primary_language="Go",
                license_spdx="Apache-2.0",
                topics=(),
                repo_created_at="2025-01-03T00:00:00Z",
                stars_total=300,
                forks_total=30,
            ),
        }
    )
    adapter = FakeGtrendingAdapter(
        [
            RawSignalCandidate(full_name="org/repo-one", signal_source="gtrending", stars=100),
            RawSignalCandidate(full_name="org/repo-two", signal_source="gtrending", stars=200),
            RawSignalCandidate(full_name="org/repo-three", signal_source="gtrending", stars=300),
        ]
    )

    recorded_sleeps: list[float] = []
    pipeline = IngestionPipeline(
        store=store,
        signal_adapter=adapter,
        metadata_client=metadata_client,
        sleep_fn=recorded_sleeps.append,
    )
    pipeline.run(captured_at="2026-11-04T00:00:00Z")

    assert len(metadata_client.calls) == 3
    # Between 3 consecutive uncached calls there must be at least 2 jitter delays in [0.1, 0.2]s
    assert len(recorded_sleeps) >= 2
    for delay_sec in recorded_sleeps:
        assert 0.100 <= delay_sec <= 0.200


def test_heals_renamed_repository_in_place_by_immutable_ghr_id(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {
            "old-owner/legacy-name": RepositoryMetadata(
                ghr_id=999000111,
                full_name="old-owner/legacy-name",
                description="Before rename",
                primary_language="TypeScript",
                license_spdx="MIT",
                topics=("agent",),
                repo_created_at="2025-05-01T00:00:00Z",
                stars_total=1200,
                forks_total=90,
            ),
            "new-org/modern-name": RepositoryMetadata(
                ghr_id=999000111,  # Same immutable GitHub Repository ID!
                full_name="new-org/modern-name",
                description="After transfer and rename",
                primary_language="TypeScript",
                license_spdx="MIT",
                topics=("agent", "mcp"),
                repo_created_at="2025-05-01T00:00:00Z",
                stars_total=2400,
                forks_total=180,
            ),
        }
    )

    # Run 1 under old name
    IngestionPipeline(
        store=store,
        signal_adapter=FakeGtrendingAdapter(
            [
                RawSignalCandidate(
                    full_name="old-owner/legacy-name",
                    signal_source="gtrending",
                    stars=1200,
                    forks=90,
                    source_rank=5,
                )
            ]
        ),
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T00:00:00Z")

    # Run 2 after repository was transferred and renamed
    IngestionPipeline(
        store=store,
        signal_adapter=FakeGtrendingAdapter(
            [
                RawSignalCandidate(
                    full_name="new-org/modern-name",
                    signal_source="gtrending",
                    stars=2400,
                    forks=180,
                    source_rank=1,
                )
            ]
        ),
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T06:00:00Z")

    # Must still be a single Repository record with updated full_name and preserved first_seen_at
    repos = store.list_repositories()
    assert len(repos) == 1
    repo = repos[0]
    assert repo.ghr_id == 999000111
    assert repo.full_name == "new-org/modern-name"
    assert repo.first_seen_at == "2026-11-04T00:00:00Z"
    assert repo.last_seen_in_trending_at == "2026-11-04T06:00:00Z"
    assert repo.stars_total == 2400

    # Both snapshots must remain attached to the same ghr_id
    snapshots = store.list_snapshots(ghr_id=999000111)
    assert len(snapshots) == 2
    assert [s.stars for s in snapshots] == [1200, 2400]


def test_deterministic_sql_export_orders_by_ghr_id_asc_and_round_trips_cleanly(
    tmp_path: Path,
) -> None:
    metadata_map = {
        "org/zeta": RepositoryMetadata(
            ghr_id=500,
            full_name="org/zeta",
            description="Higher ghr_id inserted first",
            primary_language="Python",
            license_spdx="MIT",
            topics=("z",),
            repo_created_at="2025-01-01T00:00:00Z",
            stars_total=500,
            forks_total=50,
        ),
        "org/alpha": RepositoryMetadata(
            ghr_id=100,
            full_name="org/alpha",
            description="Lower ghr_id inserted second",
            primary_language="Rust",
            license_spdx="Apache-2.0",
            topics=("a",),
            repo_created_at="2025-01-01T00:00:00Z",
            stars_total=900,
            forks_total=90,
        ),
    }

    # Store A: insert ghr_id=500 FIRST, then ghr_id=100 (so internal rowid order is 500, then 100)
    store_a = IntelligenceStore(tmp_path / "store_a.db")
    IngestionPipeline(
        store=store_a,
        signal_adapter=FakeGtrendingAdapter(
            [
                RawSignalCandidate(full_name="org/zeta", signal_source="gtrending", stars=500),
                RawSignalCandidate(full_name="org/alpha", signal_source="gtrending", stars=900),
            ]
        ),
        metadata_client=FakeGitHubMetadataClient(metadata_map),
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T00:00:00Z")

    # Store B: insert ghr_id=100 FIRST, then ghr_id=500 (opposite internal rowid order)
    store_b = IntelligenceStore(tmp_path / "store_b.db")
    IngestionPipeline(
        store=store_b,
        signal_adapter=FakeGtrendingAdapter(
            [
                RawSignalCandidate(full_name="org/alpha", signal_source="gtrending", stars=900),
                RawSignalCandidate(full_name="org/zeta", signal_source="gtrending", stars=500),
            ]
        ),
        metadata_client=FakeGitHubMetadataClient(metadata_map),
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T00:00:00Z")

    sql_dump_a = tmp_path / "dump_a.sql"
    sql_dump_b = tmp_path / "dump_b.sql"
    store_a.export_sql_dump(sql_dump_a)
    store_b.export_sql_dump(sql_dump_b)

    text_a = sql_dump_a.read_text(encoding="utf-8")
    text_b = sql_dump_b.read_text(encoding="utf-8")

    # Regardless of insertion/rowid order, deterministic SQL export must be byte-for-byte identical
    assert text_a == text_b
    # And ghr_id 100 must appear before ghr_id 500
    assert text_a.index("100, 'org/alpha'") < text_a.index("500, 'org/zeta'")

    # Bootstrapping a brand-new SQLite database from the SQL dump reproduces exact state
    restored_store = IntelligenceStore(tmp_path / "restored.db")
    restored_store.import_sql_dump(sql_dump_a)

    restored_repos = restored_store.list_repositories()
    assert [r.ghr_id for r in restored_repos] == [100, 500]
    assert [r.full_name for r in restored_repos] == ["org/alpha", "org/zeta"]
    assert len(restored_store.list_snapshots(ghr_id=100)) == 1
    assert len(restored_store.list_snapshots(ghr_id=500)) == 1


def test_gtrending_adapter_transforms_raw_trending_entries_into_candidates() -> None:
    raw_entries = [
        {
            "fullname": "ggerganov/llama.cpp",
            "name": "llama.cpp",
            "author": "ggerganov",
            "stars": 68000,
            "forks": 9800,
            "currentPeriodStars": 420,
            "description": "LLM inference in C/C++",
            "language": "C++",
        }
    ]
    adapter = GtrendingAdapter(fetcher=lambda: raw_entries)
    candidates = adapter.fetch_candidates()

    assert len(candidates) == 1
    assert candidates[0].full_name == "ggerganov/llama.cpp"
    assert candidates[0].signal_source == "gtrending"
    assert candidates[0].stars == 68000
    assert candidates[0].forks == 9800
    assert candidates[0].source_rank == 1
    assert candidates[0].period_stars_reported == 420
    assert candidates[0].primary_language == "C++"
