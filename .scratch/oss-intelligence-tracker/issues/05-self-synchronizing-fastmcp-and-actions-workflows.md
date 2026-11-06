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

## Second-round review repair and verification

The second-round review (`22ade0d...b50c551`) identified three spec defects/boundaries and four testing/design standards items. All behavioral changes and test boundaries were verified with local simulated endpoints and real local bare Git repositories (`git init --bare` with `receive.denyNonFastForwards = true`), with zero outbound calls to real GitHub, Releases, Signal Sources, PyPI, or notification services.

| Second-round finding | Observed red | Observed green |
| --- | --- | --- |
| Spec 1: FastMCP 2.14.7 event-loop blocking during forced sync | Registering synchronous `service.*` methods directly on `FastMCP` caused `sync_database_now` to execute on the asyncio event loop thread (`AssertionError: fresh query waited for the forced download`). | `@server.tool` async wrappers delegate to `asyncio.to_thread(...)`; concurrent `get_surging_repos`, `search_repositories`, `get_curated_highlights`, and `get_repo_trajectory` complete over a real `fastmcp.Client` while `sync_database_now` blocks on simulated Release I/O. |
| Spec 2: Durable write-ahead recovery/stop gate & failure boundaries | Without gated `intelligence-base-revision.txt` ordering and `hashFiles(...)` artifact guard, blocked queued runs still created recovery baseline markers (`AssertionError: assert 'intelligence-base-revision.txt' in 'failure()'`); without a durable remote ref, queued runs could immediately resend digests before human intervention. | `run-intelligence.sh` acquires `refs/heads/intelligence-recovery-pending` via a unique nonce commit and `--force-with-lease=refs/heads/intelligence-recovery-pending:` before writing `intelligence-base-revision.txt` or running ingestion/notifications, and deletes only its own lease (`--force-with-lease=refs/heads/intelligence-recovery-pending:$pending`) after both default-branch push and `gh release upload` succeed. Queued runs stop before ingestion/delivery on SQL conflicts, `gh release upload` failures, or exhausted retries; foreign gate commits are preserved; clearing the gate after reconciliation resumes cleanly without duplicate digests. |
| Spec 3: Default-branch monotonic ancestry & CAS fast-forward safety | When attempt 1 fetched and rebased onto an intervening commit `R1` and a maintainer rolled `origin/main` back to `$base` before attempt 1's push, checking only `is-ancestor $base FETCH_HEAD` allowed attempt 2 to push `R1` back onto `main` (`status == 0` instead of failing closed). | Tracking `required_ancestor` monotonically across fetch/retry attempts, verifying fast-forward ancestry (`$remote_tip` -> `$candidate`), and pushing with `--force-with-lease=refs/heads/$GITHUB_REF_NAME:$remote_tip` stops safely on rollbacks before fetch, rollbacks before push, rollbacks of previously rebased commits, divergent history rewrites, and concurrent SQL advances during no-SQL-diff runs—while succeeding under `receive.denyNonFastForwards = true`. |
| Standards 1: Step-level `env` fidelity in workflow tests | Removing `APPRISE_URLS`, `GH_TOKEN`, or `GITHUB_TOKEN` from YAML step `env` was previously masked when the harness injected global environment variables or ignored `GH_TOKEN`. | `WorkflowRun` resolves only workflow/job/step `env` mappings; removing `APPRISE_URLS` triggers `--dry-run` (zero notifications), removing `GH_TOKEN` fails `gh` Release publication while leaving the recovery gate active, and removing `GITHUB_TOKEN` omits `Authorization: Bearer` on GitHub metadata lookups. |
| Standards 2: Real local bare-repository Git integration tests | The canned `git_stub.py` had no commit graph or real rebase/diff evaluation. | `git_stub.py` was removed entirely; all 25 tests in `tests/test_actions_workflows.py` execute against real local `git init --bare` repositories with real clones, commits, rebases, diffs, ancestry checks, and CAS leases (`GIT_ALLOW_PROTOCOL=file`). |
| Standards 3: Offline `pip` dependency resolution check | `pip install --no-deps --target ...` exited `0` even when `"missing-offline-dependency>=1.0"` was injected into `pyproject.toml` (`AssertionError: installer ignored an unsatisfied declared dependency`). | Installing with `pip install --no-index --no-build-isolation --prefix ...` (without `--no-deps`) exercises `pip`'s dependency resolver across `apprise>=1.7`, `gtrending>=0.5.1`, `fastmcp>=2.14,<3`, and their transitive graph in the offline SDKs, rejects unsatisfied declared dependencies, passes `python -m pip check`, and exercises both `oss-intelligence --help` and `oss-intelligence-mcp` over stdio. |
| Standards 4: Workflow YAML guard/recovery evaluation | Evaluated consolidating YAML steps further versus keeping top-level guards explicit. | Added `if: failure() && hashFiles('intelligence-base-revision.txt') != ''` on recovery artifact upload and a defense-in-depth gate-branch refusal in `run-intelligence.sh`, while retaining explicit top-level `Require the default branch`, `Rebase before ingestion`, and `Install project` steps so non-default dispatches fail before Python setup/pull/install, updated source is pulled before `pip install .`, and `permissions: contents: write` + `concurrency: db-pipeline` remain unambiguous without reusable-workflow secret/concurrency scoping hazards. |

### Recovery procedure and residual non-atomic boundaries

When a workflow run fails after acquiring `refs/heads/intelligence-recovery-pending`, subsequent scheduled or manual runs fail closed before ingestion or notification delivery. To recover:
1. Inspect `git log -1 origin/intelligence-recovery-pending` to identify the failed `GITHUB_RUN_ID`, `GITHUB_RUN_ATTEMPT`, and baseline commit SHA.
2. Download the `intelligence-recovery-<run_id>-<run_attempt>` Actions artifact (present whenever the failed run reached gated execution and wrote `intelligence-base-revision.txt`), compare `intelligence.db` and `data/schema_data.sql` against `intelligence-base-revision.txt` and `origin/<default-branch>`, and commit reconciled Repository/Snapshot state and notification watermarks (`last_notified_at`, `last_notified_state`, `last_curated_notified_at`, `surging_entered_at`) onto the default branch.
3. Upload `intelligence.db` to `latest-db` (`gh release upload latest-db intelligence.db --clobber`) if immediate Release parity is needed, or allow the next scheduled run to refresh `latest-db` once watermarks are committed on the default branch.
4. Delete the recovery gate ref with `git push origin :refs/heads/intelligence-recovery-pending` (or delete the `intelligence-recovery-pending` branch in the GitHub UI).

Residual non-atomic limitations and offline test boundaries:
- Gate creation, Apprise delivery, default-branch Git push, `latest-db` Release upload, gate deletion, and Actions artifact upload span separate systems and cannot form a single atomic transaction.
- Because the gate is acquired before ingestion for write-ahead safety, failures prior to notification delivery (such as configuration errors, crashes, or `--dry-run` push/upload failures) and failures after default-branch push (`gh release upload` failure or gate-deletion failure, including if a maintainer replaced the gate commit) also hold `refs/heads/intelligence-recovery-pending` until manually cleared.
- If a runner is forcefully terminated/canceled or artifact upload fails after Apprise sends a digest and before SQL push completes, the gate blocks automated re-notification, though unexported local watermarks may be lost if no artifact was uploaded; deleting the gate without reconciling watermarks can permit duplicate notifications.
- Live PyPI index resolution, PEP 517 isolated build-environment downloads (`--no-build-isolation` is used offline), live GitHub Actions runners, and live GitHub Releases remain unexercised by design.

The complete isolated suite on Windows/Python 3.13 finished with **94 passed, 1 warning** in 313.25 seconds (no skipped tests; the warning is FastMCP 2.14.7's upstream Authlib deprecation):

```powershell
$sdk05 = (Resolve-Path '.pytest_cache\ticket05-sdk').Path
$sdk04 = (Resolve-Path '.pytest_cache\ticket04-sdk-30a99fa5bb764194965b42bca54efc13\packages').Path
$env:PYTHONPATH = "$sdk05;$sdk05\win32;$sdk05\win32\lib;$sdk05\pywin32_system32;$sdk04"
$env:MCP_TEST_BUILD_PATH = 'C:\Users\t\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\Lib\site-packages'
Remove-Item Env:GITHUB_TOKEN,Env:GITHUB_REPOSITORY,Env:GITHUB_RELEASE_DB_URL,Env:APPRISE_URLS,Env:TRENDSHIFT_API_KEY -ErrorAction SilentlyContinue
python -m pytest -v --basetemp .pytest_cache\ticket05-rereview-fix-20261106-full
```

Use a fresh unique `--basetemp` directory for subsequent runs.

## Third-round review repair and verification

The third-round review (`b50c551...942a1b7`) identified zero spec findings and two testing/script standards items. Both were reproduced first as isolated offline red failures before applying the minimal repairs:

| Third-round finding | Observed red | Observed green |
| --- | --- | --- |
| Standards 1: Installed `oss-intelligence-mcp` Git `origin` isolation under repository `--basetemp` | Initializing a parent Git directory above the subprocess workspace with `remote.origin.url = https://github.com/org/demo.git` caused `_default_release_url()` to discover the parent `.git/config` and attempt an outbound HTTPS tunnel (`AssertionError: assert ['CONNECT github.com:443'] == []` on a localhost `127.0.0.1` proxy spy). | Isolating Git discovery (`GIT_CONFIG_NOSYSTEM=1`, `GIT_CONFIG_GLOBAL=os.devnull`, `GIT_CEILING_DIRECTORIES`, `GIT_DIR`, and clearing `GIT_WORK_TREE`/`GIT_CONFIG*`) plus `FASTMCP_HOME` in `test_installed_mcp_command_serves_a_real_stdio_session_offline_from_sql`—and passing `show_banner=False` in `mcp_server.main()` so stdio startup never triggers FastMCP's PyPI version check—completes offline from the SQL dump with zero proxy/network requests (`proxy_requests == []`). |
| Standards 2: POSIX `sh` pipeline masking `${GITHUB_RUN_ID:?}` / `${GITHUB_RUN_ATTEMPT:?}` in `run-intelligence.sh` | Expanding `${GITHUB_RUN_ID:?}` and `${GITHUB_RUN_ATTEMPT:?}` inside the left-hand `printf` of `printf ... \| git commit-tree ...` under POSIX `/bin/sh` (`set -eu`, no `pipefail`) allowed `git commit-tree` to exit `0` on empty stdin, push a blank-message `refs/heads/intelligence-recovery-pending` commit, and finish with exit status `0` (`AssertionError: empty-run-id unexpectedly succeeded`). | Validating `: "${GITHUB_REF_NAME:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"` up front and building `pending_message` outside any pipeline (`git commit-tree ... -m "$pending_message"`) fails closed across both workflows when `GITHUB_RUN_ID` or `GITHUB_RUN_ATTEMPT` is empty or unset, before `git commit-tree`, `git push`, ingestion, notification, or `gh release` executes (`refs/heads/intelligence-recovery-pending` remains absent). |

The complete isolated suite on Windows/Python 3.13 finished with **96 passed, 1 warning** in 284.99 seconds (no skipped tests; the warning is FastMCP 2.14.7's upstream Authlib deprecation):

```powershell
$sdk05 = (Resolve-Path '.pytest_cache\ticket05-sdk').Path
$sdk04 = (Resolve-Path '.pytest_cache\ticket04-sdk-30a99fa5bb764194965b42bca54efc13\packages').Path
$env:PYTHONPATH = "$sdk05;$sdk05\win32;$sdk05\win32\lib;$sdk05\pywin32_system32;$sdk04"
$env:MCP_TEST_BUILD_PATH = 'C:\Users\t\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\Lib\site-packages'
Remove-Item Env:GITHUB_TOKEN,Env:GITHUB_REPOSITORY,Env:GITHUB_RELEASE_DB_URL,Env:APPRISE_URLS,Env:TRENDSHIFT_API_KEY -ErrorAction SilentlyContinue
python -m pytest -v --basetemp .pytest_cache\ticket05-r3-fix-20261106-final
```

Use a fresh unique `--basetemp` directory for subsequent runs.
