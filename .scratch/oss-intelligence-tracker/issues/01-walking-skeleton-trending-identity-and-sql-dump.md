# 01: Walking Skeleton — Trending Ingestion, Paced Lazy ghr_id Resolution, and Deterministic SQL Export

**What to build:** An end-to-end vertical slice that fetches trending repositories from `gtrending`, lazily resolves unknown `owner/repo` strings via GitHub's REST API (`GET /repos/{owner}/{repo}`) with a 100–200ms jittered pacing delay to avoid GitHub Secondary Rate Limits (`403`), anchors on the immutable integer `ghr_id` (healing renamed or transferred repositories in place), records time-series **Snapshot** rows in SQLite, and exports/imports a deterministic plain-text SQL dump explicitly ordered by `ghr_id ASC`.

**Blocked by:** None (can start immediately)

**Status:** done

- [x] Running the ingestion pipeline with `gtrending` candidates creates the SQLite schema (`repositories` and `snapshots` tables) and populates both **Repository** metadata and time-series **Snapshot** entries keyed by immutable integer `ghr_id`.
- [x] When an `owner/repo` already exists in `repositories`, subsequent pipeline runs do not invoke `GET /repos/{owner}/{repo}`, preserving GitHub API rate limits.
- [x] When multiple unknown `owner/repo` candidates are resolved in a run (such as cold start), the metadata resolver enforces a 100–200ms random jitter delay between consecutive `GET /repos/{owner}/{repo}` calls to prevent GitHub Secondary Rate Limit (`403`) bans.
- [x] When a newly resolved `ghr_id` already exists in `repositories` under an older `full_name`, the existing record's `full_name` is updated in place without creating a duplicate row or losing prior **Snapshot** history.
- [x] Exporting the database produces a deterministic plain-text SQL file where `repositories` rows are exported in `ORDER BY ghr_id ASC` order and `snapshots` rows in `ORDER BY ghr_id ASC, captured_at ASC, signal_source ASC` order (independent of internal SQLite `rowid`), and bootstrapping a fresh SQLite instance from that file reproduces the exact database state.
- [x] Seam 1 integration tests verify initial ingestion, cached `ghr_id` skip, 100–200ms jitter pacing invocation, rename self-healing, and insertion-order-independent deterministic SQL export round-tripping.
