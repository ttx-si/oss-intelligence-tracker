# 06: Multi-Board Signal Recovery, Velocity Normalization, and Categorized FastMCP Intelligence Output

**What to build:** Repair upstream Signal Source silent failures and volume starvation across `HelloGitHubAdapter`, `GtrendingAdapter`, `GitHubSearchSpikeAdapter`, and `OSSInsightAdapter`; normalize multi-day/weekly star counts to true 24-hour daily velocity so first-observation Cold Start evaluation does not falsely inflate `SURGING`; remove all polluted/fabricated data from `data/schema_data.sql` and test harnesses; and enrich `FastMCP` query tools (`get_surging_repos`, `search_repositories`, `get_curated_highlights`, `get_repo_trajectory`) with multi-board attribution (`signal_sources`), deterministic domain `category`, `is_new_breakout`, `url`, parsed `topics`, board/category filter parameters (`source`, `category`), and `metadata` facet breakdowns (`sources_breakdown`, `categories_breakdown`, `total_returned`).

**Blocked by:** 02: Fault-Isolated Multi-Source Spike & Curation Ingestion, 03: Orthogonal Lifecycle State Machine with Cold-Start Baselines and Full-Table COOLED Decay, 05: Windows-Safe Self-Synchronizing Local FastMCP Server and GitHub Actions Dual-Track Workflows

**Status:** done

## Acceptance Criteria

- [x] **Zero Polluted or Fabricated Data (`data/schema_data.sql` & Test Harnesses)**:
  - Reset `data/schema_data.sql` to the clean, empty deterministic schema DDL (`0` repository rows, `0` snapshot rows), removing the 48 distorted rows from the broken initial run (`a847b5e`), and remove untracked local `intelligence.db*`, `build/`, and `src/*.egg-info/` artifacts. Never insert fabricated or hand-crafted repository rows into `data/schema_data.sql`.
  - In `tests/test_actions_workflows.py`, `WorkflowRun.__init__` must initialize `self.directory / "data/schema_data.sql"` as a clean empty deterministic schema dump via `IntelligenceStore` rather than copying live rows from `ROOT / "data/schema_data.sql"`, ensuring workflow tests remain deterministic even after live GitHub Actions runs populate `data/schema_data.sql`.
  - Adapter tests in `tests/test_seam1_multi_source_and_curation.py` and `tests/test_seam1_walking_skeleton.py` must exercise the real adapter fetch/parse logic against simulated outer system boundaries (`urllib.request.urlopen` / `gtrending.fetch_repos`) using authentic upstream payload structures (two-stage `HelloGitHub` index + redirect links, `OSSInsight` `data_quality.status = "unavailable"`, `GitHubSearchSpikeAdapter` multi-slice JSON items with `created_at`) rather than bypassing `_default_*_fetcher` functions with synthetic lambdas.

- [x] **`HelloGitHubAdapter` Two-Stage Issue Discovery & Redirect Link Parsing (`src/oss_intelligence/adapters/hellogithub.py`)**:
  - When fetching `https://raw.githubusercontent.com/521xueweihan/HelloGitHub/master/README.md` (which is now an index table of `[第 127 期](/content/HelloGitHub127.md)` links), discover `/content/HelloGitHub(\d+)\.md` paths, sort issue numbers descending, and fetch up to `max_issues` (default `3`, configurable on `HelloGitHubAdapter(max_issues=3)`) from `https://raw.githubusercontent.com/521xueweihan/HelloGitHub/master/content/HelloGitHub{num}.md`. Isolate per-issue fetch errors so a single failed issue fetch does not abort the remaining issues (raise `ValueError` only if zero curated candidates are extracted overall). If the initial fetched Markdown already contains repository entries directly under headings, parse it directly.
  - Support both direct GitHub repository links (`[label](https://github.com/<owner>/<repo>)`) and HelloGitHub click-tracking redirect links (`[label](https://hellogithub.com/periodical/statistics/click?target=https://github.com/<owner>/<repo>)`, including URL-encoded `target=` values).
  - Extract `curated_issue` (`vol-{num}` per issue file), `curated_category` (from the enclosing `##` / `###` / `####` section heading, e.g. `Python 项目`, `人工智能`, `C 项目`), and `description` (text following `：` / `:` / `-` / `—` on the entry line). Exclude self-links to `521xueweihan/HelloGitHub` and deduplicate across issues by `full_name.lower()` (preserving the newest issue's metadata).

- [x] **`GtrendingAdapter` Multi-Language Daily/Weekly Matrix & Velocity Normalization (`src/oss_intelligence/adapters/gtrending.py`)**:
  - Query `gtrending.fetch_repos(language=lang, since=since)` across default `languages=("", "python", "typescript", "rust", "go", "javascript", "c++", "java", "c", "c#", "shell")` and `since_windows=("daily", "weekly")` (while supporting zero-argument fallback if a legacy zero-arg `fetch_repos()` stub is injected).
  - Isolate per-slice exceptions (`try/except` with warning log) so a failure on one `(language, since)` slice does not abort the remaining slices; raise only if every slice fails with an exception.
  - Deduplicate within `GtrendingAdapter` by `full_name.lower()`:
    - For `since == "daily"`, `period_stars_reported` is `int(item.get("currentPeriodStars") or 0)`.
    - For `since == "weekly"`, `currentPeriodStars` is a 7-day delta and must be normalized to a daily rate (`max(1, int(round(raw_weekly_stars / 7.0)))` when `raw_weekly_stars > 0` else `0`).
    - When a repository appears in both `daily` and `weekly` (or across overall `""` and a language slice), preserve `daily`'s `period_stars_reported`, the maximum `stars` and `forks`, the best (lowest integer) `source_rank`, and non-empty `description` and `primary_language`.

- [x] **`GitHubSearchSpikeAdapter` Multi-Window Breakout Discovery & Age-Normalized Daily Velocity (`src/oss_intelligence/adapters/gh_search.py`)**:
  - Increase default `per_page` from `30` to `100`.
  - Query two complementary breakout slices by default (`search_slices=((7, 50), (30, 200))` when default `days_lookback=7` and `min_stars=50` are used; if custom `days_lookback` or `min_stars` are explicitly passed, query `(days_lookback, min_stars)`):
    1. 7-day new breakouts: `created:>={now - 7d} stars:>=50` (`per_page=100`, `sort=stars`, `order=desc`)
    2. 30-day rising breakouts: `created:>={now - 30d} stars:>=200` (`per_page=100`, `sort=stars`, `order=desc`)
  - Deduplicate across slices by `full_name.lower()`.
  - Normalize `period_stars_reported` to a daily rate instead of copying cumulative lifetime `stars`: when `item.get("period_stars_reported")` is not explicitly provided, compute `age_days = max(1.0, (ref_now - created_dt).total_seconds() / 86400.0)` from `item.get("created_at")` (falling back to `float(max(1, slice_days_lookback))` if `created_at` is absent) and set `period_stars_reported = max(1, int(round(stars / age_days)))` when `stars > 0` else `0`.

- [x] **`OSSInsightAdapter` Explicit Deprecation / Unavailability Detection (`src/oss_intelligence/adapters/ossinsight.py`)**:
  - In `_default_ossinsight_fetcher`, inspect `payload.get("data_quality")`: when `status == "unavailable"`, raise a descriptive `RuntimeError` containing `unavailable_since` and `reason` so `IngestionPipeline`'s circuit breaker logs a clear structured warning (`source=OSSInsightAdapter(ossinsight)`) instead of silently returning `[]`.

- [x] **Case-Insensitive Repository Lookup (`src/oss_intelligence/store.py`)**:
  - Update `IntelligenceStore.get_repository_by_full_name` to query `WHERE full_name = ? COLLATE NOCASE` so casing differences across scrapers (`HelloGitHub` vs `gtrending` vs GitHub API) never trigger redundant `GET /repos/{owner}/{repo}` calls or `UNIQUE` constraint collisions.

- [x] **Multi-Board Attribution, Domain Categorization & Filtering in `FastMCP` (`src/oss_intelligence/mcp_server.py`)**:
  - Preserve the exact 5 registered MCP tool names (`get_surging_repos`, `search_repositories`, `get_curated_highlights`, `get_repo_trajectory`, `sync_database_now`) and all existing SQLite columns on returned repository dicts.
  - Enrich every repository dict returned by `get_surging_repos`, `search_repositories`, `get_curated_highlights`, and `get_repo_trajectory` with:
    - `url`: `f"https://github.com/{row['full_name']}"`
    - `topics`: parsed `list[str]` from `topics_json`
    - `signal_sources`: sorted `list[str]` of distinct `signal_source` values from `snapshots` for that `ghr_id`
    - `category`: deterministic domain category classified from `topics`, `description`, `curated_category`, and `primary_language` (`"AI / Agents & LLM"`, `"DevTools & Coding"`, `"Web & Frontend"`, `"Backend, Cloud & Infra"`, `"Data, Storage & Database"`, `"Security & Privacy"`, `"Mobile & Desktop Apps"`, falling back to `curated_category` if present and not `"Uncategorized"`, else `f"{primary_language} Ecosystem"` if `primary_language` is set, else `"General Open Source"`)
    - `is_new_breakout`: `bool` (`True` when `repo_created_at` is within 30 days prior to `last_updated_at`)
  - Extend `IntelligenceService.search_repositories` and the FastMCP `@server.tool` `search_repositories` wrapper with optional `source: str | None = None` (filtering repositories that have at least one snapshot from `source`, case-insensitive) and `category: str | None = None` (filtering repositories whose resolved `category` or `curated_category` matches `category`, case-insensitive substring or exact match).
  - Enrich `result["metadata"]` in `search_repositories` (and `get_surging_repos` / `get_curated_highlights`) with `total_returned`, `sources_breakdown` (`dict[str, int]`), and `categories_breakdown` (`dict[str, int]`) while preserving `staleness_warning` when offline.

- [x] **Specification & ADR Alignment (`docs/SPEC.md`, `docs/adr/0001-*.md`, `docs/adr/0004-*.md`)**:
  - Update `docs/SPEC.md` and relevant ADRs to document the two-stage `HelloGitHub` issue discovery + click-redirect parsing, multi-language daily/weekly `gtrending` matrix with 7-day velocity normalization, `gh_search` 7d/30d (`per_page=100`) breakout slices with age-normalized daily velocity, `OSSInsight` `data_quality` circuit-breaker handling, and the enriched `FastMCP` repository schema (`signal_sources`, `category`, `is_new_breakout`, `url`, `topics`, `source`/`category` filters, and `metadata` breakdowns).

## Vertical-Slice TDD Execution Log (`Red -> Green`)

| Slice | Test / Verification Seam | Red Failure Observed | Green Verification |
| :--- | :--- | :--- | :--- |
| 1 | `test_hellogithub_two_stage_issue_discovery_click_redirects_and_nocase_lookup` (`tests/test_seam1_multi_source_and_curation.py`) | `TypeError: HelloGitHubAdapter.__init__() got an unexpected keyword argument 'max_issues'` / `ValueError: no curated repositories found` | `1 passed in 0.08s` |
| 2 | `test_ossinsight_unavailable_data_quality_trips_circuit_breaker_with_warning` (`tests/test_seam1_multi_source_and_curation.py`) | `AssertionError: assert 0 == 1` (0 circuit-breaker warnings on `data_quality.status == 'unavailable'`) | `2 passed in 0.08s` |
| 3 | `test_gtrending_multi_language_daily_weekly_matrix_and_velocity_normalization` (`tests/test_seam1_multi_source_and_curation.py`) | `TypeError: GtrendingAdapter.__init__() got an unexpected keyword argument 'languages'` | `1 passed in 0.18s` |
| 4 | `test_github_search_spike_adapter_ingests_breakout_repos_with_gh_search_source` (`tests/test_seam1_multi_source_and_curation.py`) & `repro_signal_sources.py` | `AssertionError: assert 1 == 2` (`per_page=30` single query) & `repro_signal_sources.py` exit code `1` | `9 passed in 0.40s` & `repro_signal_sources.py` exit code `0` |
| 5 | `test_multi_board_attribution_categorization_filtering_and_metadata_breakdowns` (`tests/test_seam2_mcp_sync.py`) | `KeyError: 'total_returned'` | `1 passed in 1.56s` |
| 6 | `test_description_whitespace_normalization_and_curated_merge_priority` (`tests/test_seam1_multi_source_and_curation.py`) | `AssertionError: assert '\n      Ruby...amework\n    ' == 'Ruby on Rails web framework'` | `11 passed in 0.58s` |
| 7 | `test_subsequent_curation_only_snapshot_does_not_wipe_out_trending_velocity_or_surging_state` (`tests/test_seam1_lifecycle_and_decay.py`) | `AssertionError: assert 0 == 200` (subsequent `hellogithub` snapshot wiped out `gh_search` velocity) | `7 passed in 1.12s` |
| 8 | `test_weighted_domain_categorization_and_expanded_query_limits` (`tests/test_seam2_mcp_sync.py`) & `repro_live_db_audit.py` | `AssertionError: assert 500 == 561` & `repro_live_db_audit.py` exit code `1` | `2 passed in 3.05s` & `repro_live_db_audit.py` exit code `0` (`All live-DB and codebase checks passed!`) |
| Full Suite | `python -m pytest -v --basetemp .pytest_cache\ticket06-full-run-2` | — | `100 passed, 1 warning in 285.01s (0:04:45)` |
