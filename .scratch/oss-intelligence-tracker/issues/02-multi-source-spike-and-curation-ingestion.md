# 02: Fault-Isolated Multi-Source Spike & Curation Ingestion (GitHub Search, OSSInsight, HelloGitHub, Opt-In Trendshift)

**What to build:** Expand the ingestion pipeline end-to-end across all remaining **Signal Sources**—zero-cost breakout discovery via the native GitHub Search API (repositories created within the last 7 days sorted by stars), statistical trend ingestion via `OSSInsight`, editorial **Curation** parsing from `HelloGitHub` Markdown issues, and an opt-in `Trendshift` adapter activated only when `TRENDSHIFT_API_KEY` is provided—with strict per-adapter circuit-breaker fault isolation so a single broken source never crashes the pipeline.

**Blocked by:** 01: Walking Skeleton — Trending Ingestion, Paced Lazy ghr_id Resolution, and Deterministic SQL Export

**Status:** done

- [x] The GitHub Search spike adapter queries repositories created in the last 7 days with high star counts using `GITHUB_TOKEN` and records **Snapshot** entries with `signal_source = 'gh_search'`.
- [x] The `OSSInsight` adapter fetches trending repository metrics and records **Snapshot** entries with `signal_source = 'ossinsight'`.
- [x] The `HelloGitHub` adapter parses structured Markdown issue content, resolves `ghr_id` via the shared identity pipeline, and sets `is_curated = 1`, `curated_issue`, and `curated_category` on the target **Repository** without overwriting its momentum `lifecycle_state`.
- [x] Every **Signal Source** adapter executes inside an isolated circuit-breaker wrapper: if `HelloGitHub` Markdown formatting changes or any adapter raises a network/parse error, the pipeline logs a structured warning and completes ingestion for all remaining healthy adapters without crashing.
- [x] When `TRENDSHIFT_API_KEY` is unset or empty, the pipeline skips `Trendshift` cleanly with zero errors; when `TRENDSHIFT_API_KEY` is configured, `Trendshift` spikes are ingested with `signal_source = 'trendshift'`.
- [x] Seam 1 integration tests verify multi-source deduplication by `ghr_id`, orthogonal **Curation** metadata updates, opt-in `Trendshift` toggling, and circuit-breaker fault isolation when one adapter throws an exception.
