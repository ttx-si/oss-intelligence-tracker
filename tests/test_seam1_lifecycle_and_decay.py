"""Seam 1 integration tests for Ticket 03: Orthogonal Lifecycle State Machine and COOLED Decay."""

from __future__ import annotations

from pathlib import Path

from oss_intelligence.models import RawSignalCandidate, RepositoryMetadata
from oss_intelligence.pipeline import IngestionPipeline
from oss_intelligence.store import IntelligenceStore


class FakeSignalAdapter:
    """Configurable system-boundary stub for a trending Signal Source."""

    def __init__(
        self,
        candidates: list[RawSignalCandidate],
        signal_source: str = "gtrending",
    ) -> None:
        self._candidates = list(candidates)
        self.signal_source = signal_source

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


def _make_metadata(
    ghr_id: int,
    full_name: str,
    language: str = "Python",
    stars: int = 1000,
) -> RepositoryMetadata:
    return RepositoryMetadata(
        ghr_id=ghr_id,
        full_name=full_name,
        description=f"Repository {full_name}",
        primary_language=language,
        license_spdx="MIT",
        topics=("oss",),
        repo_created_at="2026-01-01T00:00:00Z",
        stars_total=stars,
        forks_total=50,
    )


def test_velocity_calculation_updates_6h_24h_7d_deltas_from_longitudinal_snapshots_with_first_observation_fallback(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {"org/velocity-repo": _make_metadata(1001, "org/velocity-repo", "Python", 1000)}
    )

    # Run 1 (t = Day 0, 00:00): First observation falls back to period_stars_reported for 24h & 7d
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/velocity-repo",
                        signal_source="gtrending",
                        stars=1000,
                        period_stars_reported=45,
                        primary_language="Python",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T00:00:00Z")

    repo_d0 = store.get_repository_by_ghr_id(1001)
    assert repo_d0 is not None
    assert repo_d0.stars_delta_6h == 0
    assert repo_d0.stars_delta_24h == 45
    assert repo_d0.stars_delta_7d == 45

    # Run 2 (t = Day 0, 06:00): 6 hours later, stars=1035 (+35 over 6h)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/velocity-repo",
                        signal_source="gtrending",
                        stars=1035,
                        period_stars_reported=45,
                        primary_language="Python",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T06:00:00Z")

    repo_6h = store.get_repository_by_ghr_id(1001)
    assert repo_6h is not None
    assert repo_6h.stars_delta_6h == 35
    assert repo_6h.stars_delta_24h == 45
    assert repo_6h.stars_delta_7d == 45

    # Run 3 (t = Day 1, 00:00): 24 hours after first observation, stars=1120 (+120 over 24h)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/velocity-repo",
                        signal_source="gtrending",
                        stars=1120,
                        period_stars_reported=90,
                        primary_language="Python",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-02T00:00:00Z")

    repo_24h = store.get_repository_by_ghr_id(1001)
    assert repo_24h is not None
    assert repo_24h.stars_delta_24h == 120
    assert repo_24h.stars_delta_7d == 120

    # Run 4 (t = Day 6, 00:00): stars=1500
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/velocity-repo",
                        signal_source="gtrending",
                        stars=1500,
                        primary_language="Python",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-07T00:00:00Z")

    # Run 5 (t = Day 6, 18:00): stars=1560
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/velocity-repo",
                        signal_source="gtrending",
                        stars=1560,
                        primary_language="Python",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-07T18:00:00Z")

    # Run 6 (t = Day 7, 00:00): stars=1600 -> 6h delta = 40 (1600-1560), 24h delta = 100 (1600-1500), 7d delta = 600 (1600-1000)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/velocity-repo",
                        signal_source="gtrending",
                        stars=1600,
                        primary_language="Python",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-08T00:00:00Z")

    repo_7d = store.get_repository_by_ghr_id(1001)
    assert repo_7d is not None
    assert repo_7d.stars_delta_6h == 40
    assert repo_7d.stars_delta_24h == 100
    assert repo_7d.stars_delta_7d == 600


def test_cold_start_transitions_to_surging_on_24h_delta_150_or_6h_delta_50(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_map = {
        "rust/daily-surger": _make_metadata(2001, "rust/daily-surger", "Rust", 2000),
        "rust/six-hour-surger": _make_metadata(2002, "rust/six-hour-surger", "Rust", 500),
        "rust/below-threshold": _make_metadata(2003, "rust/below-threshold", "Rust", 800),
    }
    for idx in range(1, 12):
        name = f"go/repo-{idx}"
        metadata_map[name] = _make_metadata(2100 + idx, name, "Go", 1000)

    metadata_client = FakeGitHubMetadataClient(metadata_map)

    # Part A: < 10 active Rust repositories (Cold Start by sample_count < 10)
    # t = Day 0, 00:00
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="rust/daily-surger",
                        signal_source="gtrending",
                        stars=2000,
                        period_stars_reported=150,
                        primary_language="Rust",
                    ),
                    RawSignalCandidate(
                        full_name="rust/six-hour-surger",
                        signal_source="gtrending",
                        stars=500,
                        period_stars_reported=20,
                        primary_language="Rust",
                    ),
                    RawSignalCandidate(
                        full_name="rust/below-threshold",
                        signal_source="gtrending",
                        stars=800,
                        period_stars_reported=90,
                        primary_language="Rust",
                    ),
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T00:00:00Z")

    assert store.get_repository_by_ghr_id(2001).lifecycle_state == "SURGING"
    assert store.get_repository_by_ghr_id(2002).lifecycle_state == "DISCOVERY"
    assert store.get_repository_by_ghr_id(2003).lifecycle_state == "DISCOVERY"

    # t = Day 0, 06:00: rust/six-hour-surger gains 55 stars in 6h (>= 50), rust/below-threshold gains 30 stars (< 50)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="rust/six-hour-surger",
                        signal_source="gtrending",
                        stars=555,
                        primary_language="Rust",
                    ),
                    RawSignalCandidate(
                        full_name="rust/below-threshold",
                        signal_source="gtrending",
                        stars=830,
                        primary_language="Rust",
                    ),
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T06:00:00Z")

    six_hour_repo = store.get_repository_by_ghr_id(2002)
    assert six_hour_repo is not None
    assert six_hour_repo.stars_delta_6h == 55
    assert six_hour_repo.stars_delta_24h == 55
    assert six_hour_repo.lifecycle_state == "SURGING"

    below_repo = store.get_repository_by_ghr_id(2003)
    assert below_repo is not None
    assert below_repo.stars_delta_6h == 30
    assert below_repo.lifecycle_state == "DISCOVERY"

    # Part B: >= 10 active Go repositories, but only 4 days of history (< 7 days -> still Cold Start!)
    go_day0_candidates = [
        RawSignalCandidate(
            full_name=f"go/repo-{idx}",
            signal_source="gtrending",
            stars=1000,
            period_stars_reported=15,
            primary_language="Go",
        )
        for idx in range(1, 12)
    ]
    IngestionPipeline(
        store=store,
        signal_adapters=[FakeSignalAdapter(go_day0_candidates)],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T00:00:00Z")

    # Day 3: baseline snapshot for 24h delta calculation on Day 4
    go_day3_candidates = [
        RawSignalCandidate(
            full_name=f"go/repo-{idx}",
            signal_source="gtrending",
            stars=1045,
            primary_language="Go",
        )
        for idx in range(1, 12)
    ]
    IngestionPipeline(
        store=store,
        signal_adapters=[FakeSignalAdapter(go_day3_candidates)],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T00:00:00Z")

    # Day 4 (only 4 days of history < 7 days):
    # go/repo-1..9 gain +15 stars/day, go/repo-10 gains +100 (< 150 cold-start), go/repo-11 gains +160 (>= 150)
    go_day4_candidates = [
        RawSignalCandidate(
            full_name=f"go/repo-{idx}",
            signal_source="gtrending",
            stars=1060 if idx <= 9 else (1145 if idx == 10 else 1205),
            primary_language="Go",
        )
        for idx in range(1, 12)
    ]
    IngestionPipeline(
        store=store,
        signal_adapters=[FakeSignalAdapter(go_day4_candidates)],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-05T00:00:00Z")

    # Because Go has only 4 days of history (< 7d), Cold Start threshold (150) still governs:
    assert store.get_repository_by_ghr_id(2110).stars_delta_24h == 100
    assert store.get_repository_by_ghr_id(2110).lifecycle_state != "SURGING"
    assert store.get_repository_by_ghr_id(2111).stars_delta_24h == 160
    assert store.get_repository_by_ghr_id(2111).lifecycle_state == "SURGING"


def test_warm_state_applies_composite_threshold_with_niche_language_floor_of_50(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_map: dict[str, RepositoryMetadata] = {}
    for idx in range(1, 11):
        zig_name = f"zig/repo-{idx}"
        metadata_map[zig_name] = _make_metadata(3000 + idx, zig_name, "Zig", 200)
        py_name = f"python/repo-{idx}"
        metadata_map[py_name] = _make_metadata(3100 + idx, py_name, "Python", 5000)

    metadata_client = FakeGitHubMetadataClient(metadata_map)

    # Day 0 (2026-11-01T00:00:00Z): Ingest 10 Zig repos and 10 Python repos
    day0_candidates = [
        RawSignalCandidate(
            full_name=f"zig/repo-{idx}",
            signal_source="gtrending",
            stars=200,
            period_stars_reported=2,
            primary_language="Zig",
        )
        for idx in range(1, 11)
    ] + [
        RawSignalCandidate(
            full_name=f"python/repo-{idx}",
            signal_source="gtrending",
            stars=5000,
            period_stars_reported=80,
            primary_language="Python",
        )
        for idx in range(1, 11)
    ]
    IngestionPipeline(
        store=store,
        signal_adapters=[FakeSignalAdapter(day0_candidates)],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T00:00:00Z")

    # Day 6 (2026-11-07T00:00:00Z): 24h baseline prior to Day 7
    day6_candidates = [
        RawSignalCandidate(
            full_name=f"zig/repo-{idx}",
            signal_source="gtrending",
            stars=212,
            primary_language="Zig",
        )
        for idx in range(1, 11)
    ] + [
        RawSignalCandidate(
            full_name=f"python/repo-{idx}",
            signal_source="gtrending",
            stars=5480,
            primary_language="Python",
        )
        for idx in range(1, 11)
    ]
    IngestionPipeline(
        store=store,
        signal_adapters=[FakeSignalAdapter(day6_candidates)],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-07T00:00:00Z")

    # Day 7 (2026-11-08T00:00:00Z): >= 10 language samples AND >= 7 days of history -> Warm State!
    # Zig deltas on Day 7:
    # - zig/repo-1..8: +2 stars/day
    # - zig/repo-9: +30 stars/day (well above language_daily_mean * 3 = 25.8, but below niche floor 50!)
    # - zig/repo-10: +55 stars/day (>= niche floor 50, even though < 150 cold-start!)
    # Python deltas on Day 7:
    # - python/repo-1..8: +80 stars/day
    # - python/repo-9: +160 stars/day (>= 150 cold-start threshold, but < language_daily_mean * 3 = 360!)
    # - python/repo-10: +400 stars/day (>= 360 warm threshold!)
    day7_candidates = [
        RawSignalCandidate(
            full_name=f"zig/repo-{idx}",
            signal_source="gtrending",
            stars=214 if idx <= 8 else (242 if idx == 9 else 267),
            primary_language="Zig",
        )
        for idx in range(1, 11)
    ] + [
        RawSignalCandidate(
            full_name=f"python/repo-{idx}",
            signal_source="gtrending",
            stars=5560 if idx <= 8 else (5640 if idx == 9 else 5880),
            primary_language="Python",
        )
        for idx in range(1, 11)
    ]
    IngestionPipeline(
        store=store,
        signal_adapters=[FakeSignalAdapter(day7_candidates)],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-08T00:00:00Z")

    # Verify Zig (niche language floor of 50):
    zig_9 = store.get_repository_by_ghr_id(3009)
    assert zig_9 is not None
    assert zig_9.stars_delta_24h == 30
    assert zig_9.lifecycle_state != "SURGING"

    zig_10 = store.get_repository_by_ghr_id(3010)
    assert zig_10 is not None
    assert zig_10.stars_delta_24h == 55
    assert zig_10.lifecycle_state == "SURGING"

    # Verify Python (warm mean * 3 = 360 > 150):
    py_9 = store.get_repository_by_ghr_id(3109)
    assert py_9 is not None
    assert py_9.stars_delta_24h == 160
    assert py_9.lifecycle_state != "SURGING"

    py_10 = store.get_repository_by_ghr_id(3110)
    assert py_10 is not None
    assert py_10.stars_delta_24h == 400
    assert py_10.lifecycle_state == "SURGING"


def test_repositories_tracked_at_least_3_days_with_steady_velocity_below_surging_transition_to_sustained(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {
            "org/steady-repo": _make_metadata(4001, "org/steady-repo", "TypeScript", 1000),
            "org/slow-repo": _make_metadata(4002, "org/slow-repo", "TypeScript", 1000),
        }
    )

    # Day 0 (2026-11-01T00:00:00Z): Tracked 0 days (< 3 days) -> DISCOVERY
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/steady-repo",
                        signal_source="gtrending",
                        stars=1000,
                        period_stars_reported=25,
                        primary_language="TypeScript",
                    ),
                    RawSignalCandidate(
                        full_name="org/slow-repo",
                        signal_source="gtrending",
                        stars=1000,
                        period_stars_reported=25,
                        primary_language="TypeScript",
                    ),
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T00:00:00Z")

    assert store.get_repository_by_ghr_id(4001).lifecycle_state == "DISCOVERY"
    assert store.get_repository_by_ghr_id(4002).lifecycle_state == "DISCOVERY"

    # Day 2 (2026-11-03T00:00:00Z): Tracked 2 days (< 3 days) -> still DISCOVERY
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/steady-repo",
                        signal_source="gtrending",
                        stars=1050,
                        period_stars_reported=25,
                        primary_language="TypeScript",
                    ),
                    RawSignalCandidate(
                        full_name="org/slow-repo",
                        signal_source="gtrending",
                        stars=1050,
                        period_stars_reported=25,
                        primary_language="TypeScript",
                    ),
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-03T00:00:00Z")

    assert store.get_repository_by_ghr_id(4001).lifecycle_state == "DISCOVERY"
    assert store.get_repository_by_ghr_id(4002).lifecycle_state == "DISCOVERY"

    # Day 3 (2026-11-04T00:00:00Z): Tracked 3 days (>= 3 days)
    # steady-repo gains +25 over 24h (>= 10, < 150) -> SUSTAINED
    # slow-repo gains +5 over 24h (< 10) -> DISCOVERY
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/steady-repo",
                        signal_source="gtrending",
                        stars=1075,
                        primary_language="TypeScript",
                    ),
                    RawSignalCandidate(
                        full_name="org/slow-repo",
                        signal_source="gtrending",
                        stars=1055,
                        primary_language="TypeScript",
                    ),
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T00:00:00Z")

    steady = store.get_repository_by_ghr_id(4001)
    assert steady is not None
    assert steady.stars_delta_24h == 25
    assert steady.lifecycle_state == "SUSTAINED"

    slow = store.get_repository_by_ghr_id(4002)
    assert slow is not None
    assert slow.stars_delta_24h == 5
    assert slow.lifecycle_state == "DISCOVERY"


def test_full_table_scan_transitions_stale_repositories_absent_from_scrape_for_14_days_to_cooled(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {
            "org/surging-stale": _make_metadata(5001, "org/surging-stale", "Rust", 3000),
            "org/sustained-stale": _make_metadata(5002, "org/sustained-stale", "Rust", 1000),
            "org/fresh-candidate": _make_metadata(5003, "org/fresh-candidate", "Rust", 500),
        }
    )

    # Day 0 (2026-11-01T00:00:00Z): Ingest surging-stale (SURGING) and sustained-stale (DISCOVERY)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/surging-stale",
                        signal_source="gtrending",
                        stars=3000,
                        period_stars_reported=220,
                        primary_language="Rust",
                    ),
                    RawSignalCandidate(
                        full_name="org/sustained-stale",
                        signal_source="gtrending",
                        stars=1000,
                        period_stars_reported=30,
                        primary_language="Rust",
                    ),
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T00:00:00Z")

    assert store.get_repository_by_ghr_id(5001).lifecycle_state == "SURGING"
    assert store.get_repository_by_ghr_id(5002).lifecycle_state == "DISCOVERY"

    # Day 2 & Day 3 (2026-11-03 and 2026-11-04): sustained-stale transitions to SUSTAINED
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/sustained-stale",
                        signal_source="gtrending",
                        stars=1060,
                        primary_language="Rust",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-03T00:00:00Z")

    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/sustained-stale",
                        signal_source="gtrending",
                        stars=1090,
                        primary_language="Rust",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T00:00:00Z")

    assert store.get_repository_by_ghr_id(5002).lifecycle_state == "SUSTAINED"

    # Day 17 (2026-11-18T00:00:00Z): 17 days since surging-stale was seen, 14 days since sustained-stale was seen.
    # Neither repository appears in the current scrape; only fresh-candidate is scraped.
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/fresh-candidate",
                        signal_source="gtrending",
                        stars=500,
                        period_stars_reported=20,
                        primary_language="Rust",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-18T00:00:00Z")

    surging_stale = store.get_repository_by_ghr_id(5001)
    assert surging_stale is not None
    assert surging_stale.stars_delta_24h == 0
    assert surging_stale.lifecycle_state == "COOLED"

    sustained_stale = store.get_repository_by_ghr_id(5002)
    assert sustained_stale is not None
    assert sustained_stale.stars_delta_24h == 0
    assert sustained_stale.lifecycle_state == "COOLED"

    fresh = store.get_repository_by_ghr_id(5003)
    assert fresh is not None
    assert fresh.lifecycle_state == "DISCOVERY"


def test_cooled_repository_reawakens_to_discovery_or_surging_and_preserves_is_curated_orthogonality(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {
            "org/comeback-discovery": _make_metadata(6001, "org/comeback-discovery", "Rust", 1000),
            "org/comeback-surging": _make_metadata(6002, "org/comeback-surging", "Rust", 2000),
            "org/active-anchor": _make_metadata(6003, "org/active-anchor", "Rust", 500),
        }
    )

    # Day 0 (2026-11-01T00:00:00Z): Ingest both repositories in trending AND curation (HelloGitHub)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/comeback-discovery",
                        signal_source="gtrending",
                        stars=1000,
                        period_stars_reported=25,
                        primary_language="Rust",
                    ),
                    RawSignalCandidate(
                        full_name="org/comeback-surging",
                        signal_source="gtrending",
                        stars=2000,
                        period_stars_reported=200,
                        primary_language="Rust",
                    ),
                ]
            ),
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/comeback-discovery",
                        signal_source="hellogithub",
                        stars=0,
                        is_curated=True,
                        curated_issue="vol-104",
                        curated_category="Rust 项目",
                    ),
                    RawSignalCandidate(
                        full_name="org/comeback-surging",
                        signal_source="hellogithub",
                        stars=0,
                        is_curated=True,
                        curated_issue="vol-104",
                        curated_category="Rust 项目",
                    ),
                ],
                signal_source="hellogithub",
            ),
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-01T00:00:00Z")

    r1 = store.get_repository_by_ghr_id(6001)
    r2 = store.get_repository_by_ghr_id(6002)
    assert r1.lifecycle_state == "DISCOVERY" and r1.is_curated is True
    assert r2.lifecycle_state == "SURGING" and r2.is_curated is True

    # Day 2 & Day 3 (2026-11-03 and 2026-11-04): comeback-discovery reaches SUSTAINED; is_curated stays True
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/comeback-discovery",
                        signal_source="gtrending",
                        stars=1050,
                        primary_language="Rust",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-03T00:00:00Z")

    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/comeback-discovery",
                        signal_source="gtrending",
                        stars=1075,
                        primary_language="Rust",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-04T00:00:00Z")

    r1_sustained = store.get_repository_by_ghr_id(6001)
    assert r1_sustained.lifecycle_state == "SUSTAINED"
    assert r1_sustained.is_curated is True
    assert r1_sustained.curated_issue == "vol-104"

    # Day 18 (2026-11-19T00:00:00Z): 15+ days unseen in trending -> both decay to COOLED, keeping is_curated=True
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/active-anchor",
                        signal_source="gtrending",
                        stars=500,
                        period_stars_reported=15,
                        primary_language="Rust",
                    )
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-19T00:00:00Z")

    r1_cooled = store.get_repository_by_ghr_id(6001)
    r2_cooled = store.get_repository_by_ghr_id(6002)
    assert r1_cooled.lifecycle_state == "COOLED" and r1_cooled.is_curated is True
    assert r2_cooled.lifecycle_state == "COOLED" and r2_cooled.is_curated is True

    # Day 20 (2026-11-21T00:00:00Z): Both reappear in trending (gtrending):
    # - comeback-discovery reappears after 17 days (>48h and >14d) with +160 total stars over 17 days
    #   and period_stars_reported=0 -> must NOT use 17-day-old snapshot as 24h/7d baseline;
    #   stars_delta_24h=0, stars_delta_7d=0, and re-awakens to DISCOVERY (not false SURGING)
    # - comeback-surging reappears with spike delta (240 >= 150) -> re-awakens to SURGING
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="org/comeback-discovery",
                        signal_source="gtrending",
                        stars=1235,
                        period_stars_reported=0,
                        primary_language="Rust",
                    ),
                    RawSignalCandidate(
                        full_name="org/comeback-surging",
                        signal_source="gtrending",
                        stars=2240,
                        period_stars_reported=240,
                        primary_language="Rust",
                    ),
                ]
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-21T00:00:00Z")

    r1_reawakened = store.get_repository_by_ghr_id(6001)
    assert r1_reawakened is not None
    assert r1_reawakened.stars_delta_24h == 0
    assert r1_reawakened.stars_delta_7d == 0
    assert r1_reawakened.lifecycle_state == "DISCOVERY"
    assert r1_reawakened.is_curated is True
    assert r1_reawakened.curated_issue == "vol-104"

    r2_reawakened = store.get_repository_by_ghr_id(6002)
    assert r2_reawakened is not None
    assert r2_reawakened.stars_delta_24h == 240
    assert r2_reawakened.stars_delta_7d == 240
    assert r2_reawakened.lifecycle_state == "SURGING"
    assert r2_reawakened.is_curated is True
    assert r2_reawakened.curated_issue == "vol-104"







def test_subsequent_curation_only_snapshot_does_not_wipe_out_trending_velocity_or_surging_state(
    tmp_path: Path,
) -> None:
    store = IntelligenceStore(tmp_path / "intelligence.db")
    metadata_client = FakeGitHubMetadataClient(
        {
            "alejandrobujan/tendedero": _make_metadata(
                1408099862, "alejandrobujan/tendedero", "Swift", 3063
            )
        }
    )

    # Run 1 (t = 18:38): Repo appears in both gh_search (period_stars_reported=200) and hellogithub (period_stars_reported=0)
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="alejandrobujan/tendedero",
                        signal_source="gh_search",
                        stars=3063,
                        forks=275,
                        source_rank=59,
                        period_stars_reported=200,
                        primary_language="Swift",
                    )
                ],
                signal_source="gh_search",
            ),
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="alejandrobujan/tendedero",
                        signal_source="hellogithub",
                        stars=0,
                        forks=0,
                        source_rank=30,
                        period_stars_reported=0,
                        is_curated=True,
                        curated_issue="vol-127",
                        curated_category="Swift 项目",
                    )
                ],
                signal_source="hellogithub",
            ),
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-06T18:38:52Z")

    after_run1 = store.get_repository_by_ghr_id(1408099862)
    assert after_run1 is not None
    assert after_run1.stars_delta_24h == 200
    assert after_run1.stars_delta_7d == 200
    assert after_run1.lifecycle_state == "SURGING"

    # Run 2 (t = +6h at 00:33): Repo is re-emitted by hellogithub curation archive with 0 period_stars_reported
    IngestionPipeline(
        store=store,
        signal_adapters=[
            FakeSignalAdapter(
                [
                    RawSignalCandidate(
                        full_name="alejandrobujan/tendedero",
                        signal_source="hellogithub",
                        stars=0,
                        forks=0,
                        source_rank=30,
                        period_stars_reported=0,
                        is_curated=True,
                        curated_issue="vol-127",
                        curated_category="Swift 项目",
                    )
                ],
                signal_source="hellogithub",
            )
        ],
        metadata_client=metadata_client,
        sleep_fn=lambda _: None,
    ).run(captured_at="2026-11-07T00:33:27Z")

    after_run2 = store.get_repository_by_ghr_id(1408099862)
    assert after_run2 is not None
    assert after_run2.stars_delta_24h == 200
    assert after_run2.stars_delta_7d == 200
    assert after_run2.lifecycle_state == "SURGING"
    assert after_run2.is_curated is True
