# 05: Windows-Safe Self-Synchronizing Local FastMCP Server and GitHub Actions Dual-Track Workflows

**What to build:** End-to-end cloud-to-local synchronization and agent query interface: scheduled GitHub Actions workflows with explicit `permissions: contents: write` and `concurrency: db-pipeline` lock that commit deterministic plain-text SQL exports and publish binary `intelligence.db` to the rolling `latest-db` GitHub Release tag, paired with a local `FastMCP` server that lazily synchronizes `intelligence.db` via 1-hour TTL + HTTP `ETag` atomic swap (explicitly closing all SQLite read connections before replacement to prevent Windows `WinError 32` crashes, plus three-tier offline fallback) and exposes structured intelligence query tools.

**Blocked by:** 03: Orthogonal Lifecycle State Machine with Cold-Start Baselines and Full-Table COOLED Decay, 04: Throttled Apprise Batch Digest with 72-Hour Cooldown Window

**Status:** done

- [x] High-frequency (every 6 hours) and mid-frequency (daily) GitHub Actions workflows explicitly declare `permissions: contents: write`, share `concurrency: { group: db-pipeline, cancel-in-progress: false }`, execute `git pull --rebase` before committing deterministic `ghr_id ASC` ordered SQL dumps, and overwrite the compiled binary `intelligence.db` asset on the rolling `latest-db` GitHub Release tag.
- [x] Invoking any `FastMCP` query tool checks local database freshness against `MCP_SYNC_TTL_SECONDS` (default 3600s); within TTL, queries execute immediately against the local SQLite file with zero network requests.
- [x] When TTL expires, `FastMCP` requests the `latest-db` Release asset with `If-None-Match: <ETag>` (attaching `Authorization: Bearer <GITHUB_TOKEN>` when configured for Private repos, or unauthenticated for Public repos): on `304 Not Modified` it refreshes the TTL timestamp without downloading; on `200 OK` it streams to `intelligence.db.tmp` and validates the SQLite header.
- [x] Before calling `os.replace('intelligence.db.tmp', 'intelligence.db')`, the sync manager acquires an exclusive lock and **explicitly closes all open SQLite connections** so Windows releases all file handles on `intelligence.db` (preventing `PermissionError: [WinError 32]`), re-opening connections only after the swap succeeds.
- [x] When offline or when the Release request fails, `FastMCP` falls back first to the existing local `intelligence.db` (attaching a `staleness_warning` metadata field to the tool response), and if no local `.db` exists, automatically compiles `intelligence.db` from the local plain-text SQL dump.
- [x] `FastMCP` exposes tools to query surging repositories, filter by language/state/curation/keyword, inspect a repository's longitudinal **Snapshot** trajectory, and force an immediate sync, all verified by Seam 2 integration tests (including a test that opens and queries `intelligence.db` prior to triggering a `200 OK` hot swap on Windows).

## TDD and verification evidence

Each numbered slice below first ran its named behavior as an isolated red test, followed by a green run. The first slice's initial red was module-collection failure because the new public service did not yet exist. No real GitHub Release, Git push, Signal Source, or notification was invoked.

| Slice | Red observation | Green behavior |
| --- | --- | --- |
| 01. Release query | `ModuleNotFoundError` for the new service | Simulated 200 delivers a real SQLite database and SURGING query result. |
| 02. TTL | Second query made a second Release request | No request within 3600 seconds, including a persisted freshness record. |
| 03. ETag and private header | 304 escaped as `HTTPError` | `If-None-Match`, bearer token and 5-second timeout; 304 refreshes freshness. |
| 04. Cached offline fallback | `TimeoutError` escaped | Local result includes `staleness_warning`. |
| 05. SQL fallback | Service rejected `dump_path` | Missing local DB is compiled from the tracked SQL dump. |
| 06. Invalid download | HTML replaced the valid DB and query failed | Invalid SQLite preserves the existing DB with a warning. |
| 07. Concurrent queries | Two expired queries made two requests | Thread lock serializes the shared sync. |
| 08. Forced Windows swap | `sync_database_now` was absent | Query followed by forced 200 swap returns updated data on Windows. |
| 09. Filtering | `search_repositories` was absent | Language, Lifecycle State, Curation, keyword and velocity intersect. |
| 10. Curation and trajectory | `get_curated_highlights` was absent | Curation and ordered Snapshot trajectory are queryable. |
| 11. FastMCP registration | `create_mcp_server` was absent | Real FastMCP 2.14.7 client lists all five tools and queries structured output. |
| 12. Actions definitions | Workflow files were absent | Both schedules declare the shared lock, write permission, rebase, SQL commit and Release upload. |
| 13. Server entry point | FastMCP dependency and entry point were absent | Declared dependency, command and module help are available. |
| 14. Private rolling asset | Treating a GitHub API response as the binary asset failed | Each private sync resolves the current rolling asset via the GitHub Release API. |
| 15. Corrupt local fallback | Corrupt cached DB raised `sqlite3.DatabaseError` | Offline recovery recompiles the valid SQL dump. |
| 16. Corrupt cache revalidation | 304 reused a corrupt cached DB | No stale ETag is sent for an invalid DB; a complete asset is fetched. |
| 17. Windows process lock | Second process got `PermissionError` reading a locked byte | Independent server processes serialize on the lock and share one sync. |
| 18. Non-200 asset | HTTP 206 content replaced the cache | Only 200 is accepted; local DB and warning are retained. |
| 19. Invalid Release URL | `ValueError` escaped | Local DB and staleness warning remain available. |

Additional first-pass checks execute both workflow scripts against fake `git`, `gh`, and ingestion commands; run the actual FastMCP stdio server as a subprocess and query its offline SQL fallback; and use local HTTP redirect servers to verify bearer credentials are not forwarded to another origin. The real SDK is used only with in-process or local stdio communication. Connections opened by this service are short-lived, closed explicitly inside the common thread/process lock, and never retained across a swap. An unrelated program that opens SQLite independently does not participate in this lock and may still block replacement.

Final full run on Windows/Python 3.13: **68 passed, 1 warning**. The warning is an Authlib deprecation emitted while importing FastMCP 2.14.7. Both real SDK sets were isolated: `.pytest_cache/ticket05-sdk` for FastMCP and `.pytest_cache/ticket04-sdk-30a99fa5bb764194965b42bca54efc13/packages` for the existing Apprise/gtrending checks. The exact PowerShell invocation was:

```powershell
$sdk = (Resolve-Path '.pytest_cache\ticket05-sdk').Path
$sdk04 = (Resolve-Path '.pytest_cache\ticket04-sdk-30a99fa5bb764194965b42bca54efc13\packages').Path
$env:PYTHONPATH = "$sdk;$sdk\win32;$sdk\win32\lib;$sdk\pywin32_system32;$sdk04"
Remove-Item Env:GITHUB_TOKEN,Env:GITHUB_REPOSITORY,Env:GITHUB_RELEASE_DB_URL,Env:APPRISE_URLS,Env:TRENDSHIFT_API_KEY -ErrorAction SilentlyContinue
python -m pytest -v --basetemp .pytest_cache\ticket05-final-20261105-b
```

The complete run used elevated sandbox permissions because this Windows sandbox blocks the local asyncio socketpair and MinGit shell initialization; the suite's network, publishing and notification boundaries were simulated. Use a new, unique `--basetemp` path for another run. The live Actions environment and real GitHub Release remain untested by design.

## Review repair and verification

The review of commit `22ade0d1c018043dc7305d754c4fda46d9489537` identified five behavior defects and four test/design issues. All five were reproduced through the public query or workflow boundary. The Windows MinGit shell and real FastMCP local socket tests require the elevated sandbox; all Signal Sources, Git remotes, Release operations, and notifications in workflow tests are simulated. No real push, Release access, or notification is performed.

| Repair slice | Observed red | Observed green |
| --- | --- | --- |
| ETag after offline SQL rebuild | Rebuilt SQL database sent the deleted Release database's `"latest"` ETag. | Rebuilt database invalidates metadata and fetches the asset without that ETag. Freshness and ETag now also require a SHA-256 match, including when size and mtime are unchanged. |
| Forced sync versus fresh queries | A fresh query timed out while another thread waited for a forced download. | Fresh query completes during the download; simultaneous expired queries, separate processes, and the Windows swap remain serialized safely. |
| Manual branch selection | Both workflows completed and would publish from a feature branch. | Both reject non-default branches before ingestion or publication. |
| Installation order | Real ingestion ran the version installed before a simulated checkout update. | The initial pull precedes installation and real ingestion executes the updated checkout. |
| Intervening unrelated push | A rejected push failed the run. | A bounded rebase/push retry succeeds without re-running ingestion or repeating the simulated notification; SQL and database retain its watermark. |
| Intervening SQL change | Both workflows continued through publication despite a changed remote SQL baseline. | Both stop before push and upload; database and SQL with the delivery watermark are retained for a failed-run recovery artifact. |
| Recovery baseline | The failed-run recovery output omitted the original revision. | The baseline revision is saved along with the database and SQL. |
| Replacement during 304 | A database changed during the HTTP request was treated as fresh after the old ETag's 304. | A second fingerprint check reports a stale fallback; the following query retrieves the Release without the old ETag. |

The first non-elevated manual-branch test run appeared green only because MinGit could not initialize; that was an invalid environmental result, not a behavioral green. Its elevated red and green runs above exercised the actual workflow scripts. The former fake-Python workflow harness has been replaced with real project ingestion, a simulated package installation, and fakes only at external boundaries. An offline real `pip` installation now exercises the generated `oss-intelligence-mcp` command, its help, and a real FastMCP stdio query. Search coverage now includes one independent nonmatching Repository for each filter. Both workflows share a small execution script while retaining their explicit schedules, permissions, and shared concurrency group.

Failure artifacts are best-effort: a canceled/terminated runner or failed artifact upload can still lose unsaved delivery watermarks. If the run fails before its SQL export completes, the SQL file may be older than the preserved database. A maintainer must pause subsequent runs and reconcile the artifact against its baseline and the current default branch; otherwise already delivered notifications may be sent again. Push and Release upload are not atomic, and an independent program can still keep a Windows SQLite file open. No live GitHub Actions or real Release behavior has been exercised.

The complete review-repair run on Windows/Python 3.13 finished with **79 passed, 1 warning** in 44.83 seconds; no tests were skipped. The warning is the existing Authlib deprecation from FastMCP 2.14.7. The two SDK directories are isolated from the unrelated bundled build tools; `MCP_TEST_BUILD_PATH` supplies setuptools only to the offline installation subprocess. The elevated PowerShell command was:

```powershell
$sdk05 = (Resolve-Path '.pytest_cache\ticket05-sdk').Path
$sdk04 = (Resolve-Path '.pytest_cache\ticket04-sdk-30a99fa5bb764194965b42bca54efc13\packages').Path
$env:PYTHONPATH = "$sdk05;$sdk05\win32;$sdk05\win32\lib;$sdk05\pywin32_system32;$sdk04"
$env:MCP_TEST_BUILD_PATH = 'C:\Users\t\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\Lib\site-packages'
Remove-Item Env:GITHUB_TOKEN,Env:GITHUB_REPOSITORY,Env:GITHUB_RELEASE_DB_URL,Env:APPRISE_URLS,Env:TRENDSHIFT_API_KEY -ErrorAction SilentlyContinue
python -m pytest -v --basetemp .pytest_cache\ticket05-review-fix-full-20261105-a
```

Use a different exclusive `--basetemp` directory on another run.
