# Open Source Intelligence Tracker — Technical Specification

> **Status**: `ready-for-agent`  
> **Domain Vocabulary**: [GLOSSARY.md](../GLOSSARY.md)  
> **Architecture Decision Records**: [ADR-0001](./adr/0001-lazy-ghr-id-resolution-and-zero-cost-spike-discovery.md), [ADR-0002](./adr/0002-dual-track-sqlite-persistence-sql-dump-and-release-asset.md), [ADR-0003](./adr/0003-orthogonal-lifecycle-state-and-curation-model.md), [ADR-0004](./adr/0004-lazy-ttl-release-sync-for-local-fastmcp.md), [ADR-0005](./adr/0005-state-transition-notification-throttling-with-cooldown-digest.md)

---

## Problem Statement

Developers, technical researchers, and AI-assisted builders need to discover high-momentum open-source projects early without manually refreshing GitHub Trending, OSSInsight, or editorial newsletters like HelloGitHub every day. However, existing lightweight Git-scraping approaches suffer from critical architectural and runtime failure modes in production:

1. **Commercial API Lock-in, Identity Fragility & Secondary Rate Limits**: Commercial spike APIs (such as Trendshift Signal) require paid subscriptions, while free scrapers (`gtrending`, `HelloGitHub`) only emit mutable `owner/repo` strings without immutable GitHub integer repository IDs (`ghr_id`), causing historical time-series data to fracture whenever a repository is renamed or transferred. Furthermore, bursting dozens of uncached metadata lookups on cold start triggers GitHub's `403` Secondary Rate Limit (Abuse Detection), and formatting tweaks in third-party Markdown/HTML sources can crash the entire ingestion run if not fault-isolated.
2. **Binary SQLite Bloat, Non-Deterministic Diffs & Workflow Push Collisions**: Committing a binary SQLite database every 6 hours rapidly bloats `.git` history to hundreds of megabytes and causes binary merge conflicts when scheduled workflows overlap. Meanwhile, standard `sqlite3 .dump` outputs rows in internal `rowid` order, causing massive noisy Git diffs whenever rows reorganize, and missing `permissions: contents: write` declarations block GitHub Actions from publishing Release assets.
3. **Conflated Lifecycle & Editorial State**: Treating editorial endorsement (`CURATED`) as a mutually exclusive state alongside momentum states (`DISCOVERY`, `SURGING`, `SUSTAINED`, `COOLED`) erases real-time momentum signals as soon as a project is featured in a newsletter, while naive mean-based thresholds fail during cold start, misfire on niche languages, and fail to decay repositories that silently drop off trending lists.
4. **Cloud-to-Local Agent Disconnection & Windows File Lock (`WinError 32`)**: Cloud ingestion runs in GitHub Actions while the `FastMCP` server runs locally inside the developer's IDE (Cursor, Claude Desktop, Antigravity). On Windows, attempting an atomic file swap (`os.replace`) while an active SQLite read connection is held raises `PermissionError: [WinError 32]` and crashes the MCP server. Additionally, private repositories require token authentication to fetch Release assets.
5. **Notification Fatigue**: High-frequency (6-hour) batch runs repeatedly spam identical notifications when a **Repository** stays in `SURGING` across multiple days or oscillates across the `SURGING`/`SUSTAINED` threshold boundary.

---

## Solution

We will build a zero-cost, serverless **Open Source Intelligence Tracker** structured across five clean architectural boundaries with built-in production hardening:

1. **Fault-Isolated Signal Ingestion & Paced Identity Resolution Layer**: Ingests candidate repositories from free **Signal Sources** (`gtrending`, `OSSInsight`, native GitHub Search API for 7-day high-velocity breakout repositories, and `HelloGitHub` Markdown archives, with an opt-in adapter for `Trendshift Signal` when an API key is provided). Each **Signal Source** runs inside a circuit-breaker boundary so upstream HTML/Markdown formatting errors log a structured warning without aborting the pipeline. Newly encountered `owner/repo` paths lazily query GitHub's REST API (`GET /repos/{owner}/{repo}`) with a **100–200ms jittered pacing delay** (preventing GitHub Secondary Rate Limit `403` bans) to anchor on the immutable integer `ghr_id` and self-heal renamed or transferred repositories in place.
2. **Scheduled Orchestration & Deterministic Dual-Track Persistence Layer**: Runs high-frequency (every 6 hours) and mid-frequency (daily) GitHub Actions workflows with explicit `permissions: contents: write` and serialized by a shared concurrency lock (`db-pipeline`). During execution, pipelines operate on SQLite (`intelligence.db`), export a deterministic plain-text SQL file explicitly sorted by `ghr_id ASC` (for `repositories`) and `ghr_id ASC, captured_at ASC, signal_source ASC` (for `snapshots`) to guarantee minimal line-level Git diffs, and publish the compiled binary `intelligence.db` to a rolling GitHub Release asset (`latest-db`).
3. **Orthogonal Lifecycle & Curation State Machine Layer**: Separates momentum tracking (`lifecycle_state`: `DISCOVERY | SURGING | SUSTAINED | COOLED`) from editorial endorsement (`is_curated` + issue metadata). Applies a cold-start fallback threshold (`daily_delta >= 150` or `6h_delta >= 50`) when historical language samples are sparse, transitions to a composite threshold (`MAX(language_daily_mean * 3, 50)`) once warmed up, and scans all non-`COOLED` repositories on every run to transition repositories unseen for 14 days to `COOLED` (with support for re-awakening on future spikes).
4. **Throttled Batch Digest Notification Layer**: Tracks notification watermarks (`last_notified_at`, `last_notified_state`, `last_curated_notified_at`) per **Repository**. Only upward transitions into `SURGING` outside a 72-hour **Cooldown Window** or newly discovered **Curation** entries are aggregated into a single Markdown **Batch Digest** delivered via `Apprise`, updating watermarks transactionally only after successful delivery.
5. **Windows-Safe Self-Synchronizing Local FastMCP Layer**: Exposes domain-rich query tools to local AI agents. On tool invocation, lazily checks whether the local binary database is older than a configurable TTL (default 1 hour), performs an HTTP `ETag` conditional download from the rolling `latest-db` GitHub Release (supporting zero-config Public repos and optional `GITHUB_TOKEN` auth for Private repos), **explicitly closes all SQLite read connections before executing the atomic `.db.tmp` → `.db` replacement to prevent Windows `PermissionError: [WinError 32]`**, and gracefully degrades to the local cached `.db` or compiles from the plain-text SQL dump when offline.

---

## User Stories

1. As a developer tracking open-source trends, I want the system to ingest daily and weekly trending repositories via `gtrending`, so that I capture mainstream momentum across programming languages at zero cost.
2. As a developer looking for early breakout projects, I want the system to query the native GitHub Search API for repositories created within the last 7 days with high star velocity and compute local **Snapshot** deltas, so that I can detect engagement spikes without paying for a Trendshift Signal subscription.
3. As a user who optionally holds a paid Trendshift Signal key, I want the ingestion pipeline to automatically activate the Trendshift adapter when `TRENDSHIFT_API_KEY` is present in the environment and silently skip it otherwise, so that the system remains zero-cost by default while supporting premium signals as a plug-in.
4. As a developer tracking long-term repository trajectories, I want the ingestion pipeline to query `OSSInsight` and `HelloGitHub` Markdown issues inside isolated circuit-breaker wrappers, so that if a blogger changes Markdown formatting or a third-party site times out, the pipeline logs a warning and still completes ingestion from all other healthy **Signal Sources**.
5. As a data consumer, I want every newly discovered `owner/repo` string to be lazily resolved once against `GET /repos/{owner}/{repo}` with a 100–200ms random jitter delay between requests, so that existing repositories consume zero API quota and cold-start runs never trigger GitHub's `403` Secondary Rate Limit.
6. As a data consumer, I want a repository that has been renamed or transferred to a new owner to be matched by its resolved `ghr_id` and have its `owner/repo` path updated in place, so that its historical **Snapshot** series and notification history are never orphaned or duplicated.
7. As a repository maintainer running scheduled GitHub Actions, I want high-frequency (6-hour) and mid-frequency (daily) workflows to declare `permissions: contents: write`, share a non-canceling concurrency group (`db-pipeline`), and safely handle intervening pushes, so that SQL and Release publication do not overwrite a concurrent SQL change.
8. As a repository maintainer, I want Git commits to store a custom plain-text SQL export explicitly ordered by `ghr_id ASC` rather than relying on SQLite's internal `rowid` dump order, so that row updates never cause spurious line shifts in Git diffs and `.git` history remains tiny.
9. As a downstream consumer of the database, I want each workflow run to overwrite a compiled binary `intelligence.db` asset on a rolling GitHub Release tag (`latest-db`), so that local tools can download a ready-to-query SQLite file in under a second without rebuilding from SQL.
10. As an intelligence analyst, I want a **Repository**'s momentum `lifecycle_state` (`DISCOVERY`, `SURGING`, `SUSTAINED`, `COOLED`) to be stored orthogonally from its editorial **Curation** flag (`is_curated` and issue metadata), so that being featured in HelloGitHub never masks whether the project is currently surging or cooled down.
11. As an intelligence analyst during the first week of deployment, I want the state machine to apply a cold-start fallback threshold (daily star delta $\ge 150$ or 6-hour star delta $\ge 50$) whenever a language has fewer than 10 historical samples or less than 7 days of data, so that `SURGING` detection works immediately from day one.
12. As an intelligence analyst monitoring niche programming languages, I want the warmed-up `SURGING` threshold to require `daily_star_delta >= MAX(language_daily_mean * 3, 50)`, so that a quiet language with an average of 2 stars/day does not trigger false-positive surge alerts when a repository gains 6 stars.
13. As an intelligence analyst, I want every batch run to evaluate all non-`COOLED` repositories in the database—not just the ones returned by scrapers in the current run—and transition any **Repository** unseen in trending signals for 14 consecutive days to `COOLED`, so that stale projects do not remain stuck in `SURGING` or `SUSTAINED` forever.
14. As an intelligence analyst, I want a `COOLED` **Repository** that reappears on trending lists or experiences a new star spike to transition back to `DISCOVERY` or `SURGING`, so that "second-wind" open-source comebacks are captured accurately.
15. As a subscriber receiving push notifications via `Apprise`, I want high-frequency alerts to trigger only when a **Repository** makes a positive upward transition into `SURGING` and at least 72 hours have passed since its last `SURGING` alert (**Cooldown Window**), so that I am never spammed every 6 hours for the same project or due to threshold flapping.
16. As a subscriber, I want all qualifying `SURGING` transitions and newly ingested **Curation** items from a pipeline run to be formatted into a single structured Markdown **Batch Digest** message rather than sent as individual messages per repository, so that my IM/webhook channel stays readable.
17. As a system operator, I want `last_notified_at`, `last_notified_state`, and `last_curated_notified_at` watermarks to be committed to SQLite only after `Apprise` confirms successful delivery (and skipped in `--dry-run` mode), so that transient webhook failures are automatically retried on the next run without losing notifications.
18. As an AI developer on Windows using Cursor or Claude Desktop with `FastMCP`, I want my local MCP server to explicitly close all active SQLite connections under a sync lock before atomically replacing `intelligence.db` with the newly downloaded `intelligence.db.tmp` and re-open connections afterward, so that Windows never throws `PermissionError: [WinError 32]` during hot updates.
19. As an AI developer working offline or against either a Public (zero-config) or Private (token-authenticated via `GITHUB_TOKEN`) GitHub repository, I want `FastMCP` to seamlessly authenticate if a token is configured, or fall back to the existing local `intelligence.db` (with a staleness warning) and finally to compiling the local plain-text SQL dump when offline.
20. As an AI agent invoking `FastMCP` tools, I want structured query endpoints to filter repositories by **Lifecycle State**, **Curation** status, language, star velocity, and keyword, and to inspect longitudinal **Snapshot** histories for a specific **Repository**, so that I can answer complex technology scouting questions for the user.

---

## Implementation Decisions

### 1. Five-Layer Modular Architecture & Fault Isolation
The system is organized into five cohesive modules with explicit interfaces:
- **Signal Adapters Module**: Provides a uniform `fetch_candidates() -> list[RawSignalCandidate]` contract across four built-in adapters (`GtrendingAdapter`, `GitHubSearchSpikeAdapter`, `OSSInsightAdapter`, `HelloGitHubAdapter`) and one optional adapter (`TrendshiftAdapter`, active only when `TRENDSHIFT_API_KEY` is non-empty):
  - `GtrendingAdapter` queries `daily` and `weekly` trending windows across default language boards (`""`, `python`, `typescript`, `rust`, `go`, `javascript`, `c++`, `java`, `c`, `c#`, `shell`) with per-slice fault isolation, deduplicates by `full_name.lower()` (preserving `daily`'s 24h delta when a repository appears in both windows), and normalizes 7-day `weekly` `currentPeriodStars` to a 24-hour daily rate (`max(1, round(weekly_stars / 7))`).
  - `GitHubSearchSpikeAdapter` queries 7-day (`created:>={now-7d} stars:>=50`, `per_page=100`) and 30-day (`created:>={now-30d} stars:>=200`, `per_page=100`) breakout slices, deduplicates by `full_name.lower()`, and normalizes `period_stars_reported` by repository age in days (`max(1, round(stars / age_days))` derived from `created_at`) so Cold Start evaluation uses a daily star velocity rather than cumulative lifetime stars.
  - `HelloGitHubAdapter` supports two-stage issue discovery when `README.md` contains `/content/HelloGitHub{num}.md` index links (fetching the latest `max_issues=3` issues descending with per-issue fault isolation) as well as direct issue Markdown, and parses both direct `https://github.com/<owner>/<repo>` links and `https://hellogithub.com/periodical/statistics/click?target=...` click-tracking redirect links.
  - `OSSInsightAdapter` inspects `payload["data_quality"]` and raises a `RuntimeError` when `status == "unavailable"` so the orchestrator's circuit breaker logs a structured warning (`source`, `error_type`, `message`) rather than silently returning an empty list.
  The orchestrator wraps each adapter execution in an independent `try/except` **Circuit Breaker**: any network timeout, HTTP error, upstream unavailability, or Markdown/HTML parsing exception logs a structured warning (`source`, `error_type`, `message`) and returns an empty candidate list for that source without interrupting the remaining adapters.
- **Identity & Ingestion Pipeline Module**: Consumes `RawSignalCandidate` items, performs case-insensitive repository lookup (`WHERE full_name = ? COLLATE NOCASE`), and resolves unknown `owner/repo` strings against the GitHub REST API (`GET /repos/{owner}/{repo}`) using `GITHUB_TOKEN` with a **100–200ms jittered pacing delay** (configurable via `metadata_pacing_Range_ms=(100, 200)`) between consecutive network calls to prevent GitHub Secondary Rate Limit (`403`) errors during cold-start ingestion bursts. Performs in-place rename healing when a resolved `ghr_id` matches an existing row with a different `owner/repo`, upserts **Repository** metadata, and appends time-series **Snapshot** records.
- **Lifecycle State Machine Engine**: Executes deterministic SQL + domain transition rules across both newly ingested and existing active repositories in SQLite, updating `lifecycle_state`, `is_curated`, velocity metrics (`stars_delta_6h`, `stars_delta_24h`, `stars_delta_7d`), and `last_seen_in_trending_at`.
- **Notification Throttler & Apprise Dispatcher**: Selects repositories eligible for notification based on transition rules and the 72-hour **Cooldown Window**, renders a single Markdown **Batch Digest**, dispatches via `Apprise`, and commits notification watermarks upon delivery success.
- **FastMCP Server & Windows-Safe Release Sync Manager**: Exposes MCP tools (`get_surging_repos`, `search_repositories`, `get_curated_highlights`, `get_repo_trajectory`, `sync_database_now`) wrapped by a lazy TTL synchronization guard that closes all open SQLite handles before replacing the `.db` file on disk.

### 2. Core Database Schema Contract (from Design Synthesis)
To encode the orthogonal state model, identity anchoring, and notification throttling without ambiguity, the SQLite schema centers on two core tables:

```sql
-- Core schema contract (synthesized from domain model decisions)
CREATE TABLE IF NOT EXISTS repositories (
    ghr_id INTEGER PRIMARY KEY,                  -- Immutable GitHub Repository ID
    full_name TEXT NOT NULL UNIQUE,              -- Current "owner/repo" (updated in-place on rename)
    description TEXT,
    primary_language TEXT,
    license_spdx TEXT,
    topics_json TEXT NOT NULL DEFAULT '[]',
    repo_created_at TEXT,
    stars_total INTEGER NOT NULL DEFAULT 0,
    forks_total INTEGER NOT NULL DEFAULT 0,
    stars_delta_6h INTEGER NOT NULL DEFAULT 0,
    stars_delta_24h INTEGER NOT NULL DEFAULT 0,
    stars_delta_7d INTEGER NOT NULL DEFAULT 0,
    lifecycle_state TEXT NOT NULL DEFAULT 'DISCOVERY'
        CHECK (lifecycle_state IN ('DISCOVERY', 'SURGING', 'SUSTAINED', 'COOLED')),
    is_curated INTEGER NOT NULL DEFAULT 0 CHECK (is_curated IN (0, 1)),
    curated_issue TEXT,                          -- e.g. "vol-104"
    curated_category TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_in_trending_at TEXT NOT NULL,
    last_updated_at TEXT NOT NULL,
    last_notified_at TEXT,
    last_notified_state TEXT,
    last_curated_notified_at TEXT,
    surging_entered_at TEXT                       -- Current SURGING entry, for retries and cooldown
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ghr_id INTEGER NOT NULL REFERENCES repositories(ghr_id) ON DELETE CASCADE,
    captured_at TEXT NOT NULL,                   -- ISO-8601 UTC bucket/timestamp
    signal_source TEXT NOT NULL,                 -- 'gtrending' | 'gh_search' | 'ossinsight' | 'hellogithub' | 'trendshift'
    stars INTEGER NOT NULL,
    forks INTEGER NOT NULL DEFAULT 0,
    source_rank INTEGER,
    period_stars_reported INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_snapshots_ghr_captured ON snapshots(ghr_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_repos_state_lang ON repositories(lifecycle_state, primary_language);
```

### 3. State Machine Transition Rules (ADR-0003)
Every pipeline execution evaluates the state machine in two phases:
1. **Velocity & Language Baseline Calculation**:
   - For each **Repository**, compute `stars_delta_24h` (difference between latest **Snapshot** and the snapshot closest to 24 hours prior, falling back to `period_stars_reported` on first day) and `stars_delta_6h`.
   - For each `primary_language`, count active repositories with valid 24h deltas (`sample_count`) and the earliest snapshot span (`history_days`).
   - If `sample_count < 10` or `history_days < 7` (Cold Start), `surge_threshold_24h = 150` (and `surge_threshold_6h = 50`).
   - Otherwise (Warm State), `surge_threshold_24h = MAX(AVG(stars_delta_24h_for_language) * 3.0, 50)`.
2. **Full-Table State Evolution**:
   - **`COOLED` Decay (Full-Table Scan)**: Any repository where `julianday(now) - julianday(last_seen_in_trending_at) >= 14` AND `stars_delta_24h < 20` transitions to `COOLED`.
   - **`SURGING` Transition**: Any repository (including previously `COOLED`, `DISCOVERY`, or `SUSTAINED`) whose `stars_delta_24h >= surge_threshold_24h` (or `stars_delta_6h >= 50` during cold start) transitions to `SURGING`.
   - **`SUSTAINED` Transition**: Any repository seen in trending signals within the last 14 days that has been tracked for at least 3 days (`julianday(now) - julianday(first_seen_at) >= 3`), does not meet the `SURGING` threshold, and maintains positive velocity (`stars_delta_24h >= 10`) transitions to `SUSTAINED`.
   - **`DISCOVERY` Default / Re-entry**: Newly ingested repositories that do not immediately breach `SURGING` start in `DISCOVERY`; a `COOLED` repository that re-enters a trending **Signal Source** without breaching `SURGING` transitions to `DISCOVERY`.
   - **Orthogonal `Curation`**: Whenever `HelloGitHubAdapter` emits a candidate, `is_curated` is set to `1` along with `curated_issue` and `curated_category`, without altering `lifecycle_state`.

### 4. Deterministic SQL Export, Dual-Track Persistence & Workflow Permissions (ADR-0002)
- All GitHub Actions workflows explicitly declare `permissions: contents: write` (required to push commits and upload Release assets) and share `concurrency: { group: db-pipeline, cancel-in-progress: false }`.
- Instead of invoking raw `sqlite3 .dump` (which relies on internal `rowid` order and produces noisy Git diffs), the persistence module uses a custom deterministic SQL exporter:
  - Emits canonical `CREATE TABLE` and `CREATE INDEX` DDL statements.
  - Exports `repositories` rows as deterministic `INSERT INTO repositories ...` statements strictly ordered by `ORDER BY ghr_id ASC`.
  - Exports `snapshots` rows strictly ordered by `ORDER BY ghr_id ASC, captured_at ASC, signal_source ASC`.
- Only the default branch may run the publishing pipeline, including manual dispatches. The runner pulls that branch before installing the checked-out project and starting ingestion, and the script also refuses to execute if `GITHUB_REF_NAME` points at the recovery gate branch.
- Before importing SQL, ingesting signals, or sending notifications, `.github/scripts/run-intelligence.sh` creates a durable write-ahead recovery gate (`refs/heads/intelligence-recovery-pending`) on `origin` using a unique nonce commit (recording `GITHUB_RUN_ID`, `GITHUB_RUN_ATTEMPT`, and baseline revision) with an absent-ref compare-and-swap lease (`--force-with-lease=refs/heads/intelligence-recovery-pending:`). If the gate already exists or cannot be created, subsequent queued or manual runs stop immediately before ingestion, notification delivery, or publication. Only after acquiring the gate does the runner write `intelligence-base-revision.txt`.
- After ingestion and deterministic SQL export, the runner commits any SQL change and retries publication up to three times. On each attempt it fetches `origin/$GITHUB_REF_NAME`, verifies monotonic forward ancestry (each fetched tip must descend from `$base` and from any tip already rebased during an earlier attempt, preventing the runner from undoing a maintainer rollback or history rewrite even when `data/schema_data.sql` is identical), verifies `data/schema_data.sql` has not changed since `$base`, rebases onto the fetched tip, verifies fast-forward ancestry (`$remote_tip` is an ancestor of `$candidate`), and pushes using a fast-forward compare-and-swap lease (`--force-with-lease=refs/heads/$GITHUB_REF_NAME:$remote_tip`). It never performs a non-fast-forward history rewrite, and even when local SQL had no diff (`$candidate == $remote_tip`), a concurrent remote rollback or SQL update between `fetch` and `push` rejects the stale lease so a stale `intelligence.db` is not uploaded over newer remote SQL. Only after both the branch push and `gh release upload latest-db intelligence.db --clobber` succeed does the runner delete its own gate via `--force-with-lease=refs/heads/intelligence-recovery-pending:$pending`.
- On a failed job that already entered gated execution (`hashFiles('intelligence-base-revision.txt') != ''`), the workflow attempts to preserve `intelligence.db`, `data/schema_data.sql`, and `intelligence-base-revision.txt` as a seven-day Actions artifact (`intelligence-recovery-<run_id>-<run_attempt>`), while leaving `refs/heads/intelligence-recovery-pending` active on `origin` so queued scheduled runs fail closed without sending duplicate digests or creating noisy empty recovery artifacts. To recover, a maintainer inspects `git log -1 origin/intelligence-recovery-pending`, downloads the matching recovery artifact, reconciles Repository/Snapshot data and successful-delivery watermarks onto the default branch (uploading `intelligence.db` to `latest-db` if needed, or letting the next run refresh `latest-db` once watermarks are committed), and deletes the gate with `git push origin :refs/heads/intelligence-recovery-pending`.
- Residual non-atomic failure boundaries: gate creation, Apprise delivery, default-branch Git push, `latest-db` Release upload, gate deletion, and Actions artifact upload span separate services and cannot form a single atomic transaction. Because the gate is acquired before ingestion, any failure before notification delivery (such as invalid configuration, process crash, or a `--dry-run` publication error) or after SQL push succeeds (such as `gh release upload` or gate-deletion failure, including if a maintainer replaced the gate commit) also leaves `refs/heads/intelligence-recovery-pending` active until manually cleared. If a runner is forcefully terminated or artifact upload fails after Apprise sends a digest and before SQL push succeeds, the gate still blocks subsequent automated runs from re-notifying, though unexported local watermarks may be unavailable if no artifact was saved; clearing the gate without reconciling watermarks can permit duplicate notifications.
- Public repository visibility is recommended for zero-config local `FastMCP` access; if the repository is Private, the local sync client attaches `Authorization: Bearer <GITHUB_TOKEN>` when downloading the Release asset.

### 5. Notification Throttling & Batch Digest Rules (ADR-0005)
- A **Repository** qualifies for the **Batch Digest** in a given run if and only if:
  - **Condition 1 (Surging Breakout)**: `lifecycle_state == 'SURGING'`, the current entry into `SURGING` has not been delivered, and either `last_notified_at IS NULL` or at least 72 hours elapsed between `last_notified_at` and `surging_entered_at`; OR
  - **Condition 2 (New Curation Highlight)**: `is_curated == 1` AND `last_curated_notified_at IS NULL`.
- `surging_entered_at` records each entry into `SURGING`, and is cleared upon leaving it. For a live run, the clock is sampled after SQLite grants the write lock and immediately before the state update, not at the start of a potentially long run. The update or commit can still be delayed after this sample, so it is not an exact commit timestamp. A continuously `SURGING` repository is not notified again when the clock reaches 72 hours. An entry within the cooldown is suppressed rather than deferred; after leaving `SURGING`, a new entry at or beyond the boundary may notify again. A qualifying entry remains eligible while `SURGING` if delivery fails or is skipped by `--dry-run`.
- All qualifying repositories in the current batch are rendered into a single Markdown digest grouped into **"🚀 Surging Breakouts"** and **"✨ New Curated Highlights"** sections, sorted by `stars_delta_24h DESC`.
- Only when delivery succeeds, update `last_notified_at` and `last_curated_notified_at` to the UTC time immediately after success, and set `last_notified_state = 'SURGING'` for delivered breakouts, in a single SQLite transaction. A repository in both sections receives both updates. For Apprise 2.1 structured results, success requires both a successful overall result and a successful final raw attempt for every dispatched service, including optional services; a successful retry counts. Failed or partial delivery does not advance watermarks. With an older or custom notifier that exposes only a boolean, the boolean is the only available confirmation, so individual destination failures hidden by that notifier cannot be independently detected. `surging_entered_at` records Lifecycle State and is not a delivery watermark. An explicit pipeline `captured_at` fixes all these times for deterministic replay.
- Existing databases and SQL dumps without `surging_entered_at` are migrated. For a currently `SURGING` repository already carrying a notification timestamp, migration conservatively treats that timestamp as its entry; an unknown historical transition is not replayed.
- Configure `APPRISE_URLS` with one Apprise URL per line and run `python -m oss_intelligence --database intelligence.db` (or the installed `oss-intelligence` command). Add `--dry-run` to preview the digest in UTF-8 without sending or changing notification watermarks; ingestion and Lifecycle State updates still occur. Add `--notifications-only` to process stored repositories without fetching Signal Sources.

### 6. Windows-Safe Local FastMCP Lazy Sync & Three-Tier Fallback (ADR-0004)
- Before executing any MCP query tool, the service checks the local `.db` file's last-sync metadata timestamp against `MCP_SYNC_TTL_SECONDS` (default `3600`). The Release URL and SHA-256 of the database must match the saved metadata before the TTL or ETag can be reused. This reads the local database to calculate its digest even for a fresh query.
- If expired (or missing), it sends an HTTP `GET` to the rolling GitHub Release asset URL (`GITHUB_RELEASE_DB_URL`, including `Authorization: Bearer <GITHUB_TOKEN>` if configured for a Private repo) with `If-None-Match: <cached_etag>` and a 5-second timeout:
  - **HTTP 304**: Rechecks that the local database still matches the requested ETag's saved fingerprint, then updates the last-sync timestamp. If the database changed during the request, it uses the offline fallback with a warning and retries without that ETag on a subsequent query.
  - **HTTP 200 (Windows `WinError 32` Safe Swap)**:
    1. Streams response bytes to `intelligence.db.tmp` and verifies SQLite header magic bytes (`SQLite format 3\000`).
    2. Acquires an exclusive in-process connection lock and **explicitly closes all tracked SQLite connections** (invoking `conn.close()` on any shared/cached connection objects so the Windows kernel releases all file handles on `intelligence.db`).
    3. Executes `os.replace('intelligence.db.tmp', 'intelligence.db')` and persists the new `ETag`.
    4. Releases the lock and establishes fresh read-only/query connections on demand.
  - **Network Error / Timeout / Non-200**:
    - **Fallback Tier 1**: If a valid local `intelligence.db` already exists, use it and attach a `staleness_warning` metadata field to the MCP tool output.
    - **Fallback Tier 2**: If no local `intelligence.db` exists, locate the tracked plain-text SQL dump in the workspace and execute `sqlite3.Connection.executescript()` to materialize `intelligence.db` locally.

#### Running the local service

Install the project with `python -m pip install .`, then start `oss-intelligence-mcp` from the checkout directory. The equivalent module command is `python -m oss_intelligence.mcp_server`. The server uses MCP over standard input/output; its five tools return structured results with a `metadata` object (`total_returned`, `sources_breakdown`, `categories_breakdown`, plus `staleness_warning` when serving an offline fallback). Every returned repository object preserves all SQLite columns and is enriched with `url`, parsed `topics`, `signal_sources` (distinct Signal Sources recorded in `snapshots`), a deterministic domain `category` (`"AI / Agents & LLM"`, `"DevTools & Coding"`, `"Web & Frontend"`, `"Backend, Cloud & Infra"`, `"Data, Storage & Database"`, `"Security & Privacy"`, `"Mobile & Desktop Apps"`, or language/curation fallback), and `is_new_breakout` (`True` when created within 30 days of `last_updated_at`). `search_repositories` accepts `language`, `state`, `curated`, `keyword`, `min_stars_delta_24h`, `source`, `category`, and `limit`; `get_repo_trajectory` accepts an integer `ghr_id`.

By default, the Release URL is derived from `GITHUB_REPOSITORY=owner/repository` or the checkout's GitHub `origin`. Set `GITHUB_RELEASE_DB_URL` or pass `--release-url` to choose another asset URL. With `GITHUB_TOKEN` and the standard GitHub rolling Release URL, the service resolves the current asset through the GitHub Release API, then downloads it using the token. The token needs permission to read the private repository; redirects to a different origin do not receive it. No token is required for a public Release.

`--database` and `--dump` select the local SQLite file and SQL dump; their defaults are `intelligence.db` and `data/schema_data.sql` relative to the working directory. `MCP_DATABASE` and `MCP_SQL_DUMP` set the same defaults. `MCP_SYNC_TTL_SECONDS` changes the default 3600-second freshness window, while `sync_database_now` bypasses it. Sync metadata and separate advisory synchronization and database locks are stored next to the database. Cooperative server processes serialize downloads with the sync lock and hold the database lock for reads and swaps; each SQLite connection is closed before releasing the latter. FastMCP tool handlers offload synchronous service calls with `asyncio.to_thread`, so a forced download does not block the asyncio event loop or concurrent queries of an otherwise fresh cache. Other programs that independently hold the database open cannot be closed by this service.

The workflows restore the checked-in SQL dump, ingest signals, export the deterministic dump, commit any change, and upload the compiled database. Configure the Actions secret `APPRISE_URLS` to send notifications; without it the workflows use the existing dry-run mode. An optional `TRENDSHIFT_API_KEY` secret enables that Signal Source. The workflows do not need a personal token: they use the built-in GitHub Actions token with `contents: write`.

---

## Testing Decisions

### What Makes a Good Test
Tests must assert observable external behavior and domain invariants across module boundaries—never internal implementation details, private helper methods, or fragile SQL string formatting.

### High-Level Testing Seams
The entire system will be verified through **two high-level seams**:

1. **Seam 1 — Ingestion, Fault Isolation, State Machine & Notification Pipeline Seam**:
   - **Input**: An in-memory or temporary SQLite database initialized with the schema, injected fake **Signal Source** outputs (including a failing adapter that raises a parse/network exception), a fake GitHub REST metadata resolver (`FakeGitHubMetadataClient`) with a spy sleep/jitter recorder, a controllable clock (`now`), and a spy `Apprise` notifier.
   - **Observable Outputs Tested**:
     - Lazy `ghr_id` resolution, 100–200ms pacing invocation between uncached metadata calls, and in-place rename self-healing (`full_name` updated without row duplication).
     - Circuit-breaker fault isolation (when `HelloGitHubAdapter` raises an exception, `GtrendingAdapter` and `GitHubSearchSpikeAdapter` candidates are still ingested and committed cleanly).
     - Deterministic SQL export ordering (`ORDER BY ghr_id ASC`), ensuring identical SQL text output regardless of insertion order.
     - Cold-start vs. warm-state `SURGING` transitions, orthogonality of `is_curated` and `lifecycle_state`, full-table 14-day `COOLED` decay, and comeback re-entry.
     - 72-hour **Cooldown Window**, single **Batch Digest** aggregation, and transactional watermark rollback on failed `Apprise` delivery.

2. **Seam 2 — Local FastMCP Query & Windows-Safe Sync Seam**:
   - **Input**: Invoking public MCP tool functions against a stubbed HTTP release endpoint (simulating `200 OK`, `304 Not Modified`, Private repo auth header checks, and network timeouts) while holding a simulated active SQLite connection prior to sync on Windows.
   - **Observable Outputs Tested**:
     - Windows-safe atomic replacement (`intelligence.db` is updated on `200 OK` after an initial query has already opened the database, proving all prior connections are closed before `os.replace` without raising `PermissionError: [WinError 32]`).
     - Freshness TTL gating, `304 Not Modified` fast path, optional `GITHUB_TOKEN` header forwarding, and three-tier offline degradation (existing `.db` with `staleness_warning` → fallback compilation from `schema_data.sql`).

---

## Out of Scope

- **Web Frontend / Dashboard UI**: Interaction happens exclusively via `Apprise` push digests and local `FastMCP` agent tools.
- **Paid Commercial Data Subscriptions**: Mandating `Trendshift Signal` or paid GitHub Enterprise tokens is out of scope.
- **Distributed Cloud Databases**: Hosting external PostgreSQL/MySQL/Supabase instances is out of scope.
- **Full-Text README Vector Embeddings**: Running local LLM embeddings inside GitHub Actions is out of scope for MVP.

---

## Further Notes

- All domain terms in code, SQL column names, MCP tool descriptions, and tickets must adhere to [GLOSSARY.md](../GLOSSARY.md).
- To enable zero-config local `FastMCP` Release downloads without requiring users to configure a Personal Access Token, the GitHub repository should be set to **Public** (while still supporting `GITHUB_TOKEN` when run against a Private repo).
