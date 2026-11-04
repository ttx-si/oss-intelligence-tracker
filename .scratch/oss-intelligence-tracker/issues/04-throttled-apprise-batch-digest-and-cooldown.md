# 04: Throttled Apprise Batch Digest with 72-Hour Cooldown Window

**What to build:** End-to-end notification throttling and delivery that selects upward `SURGING` transitions outside the 72-hour **Cooldown Window** and newly discovered **Curation** highlights, aggregates them into a single structured Markdown **Batch Digest** sent via `Apprise`, and transactionally updates notification watermarks only after delivery succeeds.

**Blocked by:** 03: Orthogonal Lifecycle State Machine with Cold-Start Baselines and Full-Table COOLED Decay

**Status:** ready-for-agent

- [ ] A **Repository** enters the notification queue for a run if and only if: (a) its `lifecycle_state` is `SURGING`, its `last_notified_state` is not `SURGING` (or is null), and at least 72 hours have elapsed since `last_notified_at` (or `last_notified_at` is null); OR (b) `is_curated == 1` and `last_curated_notified_at` is null.
- [ ] A **Repository** that remains continuously in `SURGING` across consecutive 6-hour runs or oscillates `SURGING -> SUSTAINED -> SURGING` within 72 hours is suppressed by the **Cooldown Window** and not re-notified.
- [ ] All qualifying repositories in a batch run are formatted into a single Markdown **Batch Digest** grouped into "Surging Breakouts" and "New Curated Highlights" sorted by `stars_delta_24h DESC`, sending at most one `Apprise` notification per pipeline run.
- [ ] `last_notified_at`, `last_notified_state`, and `last_curated_notified_at` are updated in SQLite only when `Apprise` confirms delivery success (and are left untouched on delivery failure or `--dry-run`, allowing automatic retry on the next run).
- [ ] Seam 1 integration tests with a spy `Apprise` dispatcher verify 72-hour cooldown suppression, flapping suppression, single-digest aggregation, and transactional watermark rollback on failed send.
