# 04: Throttled Apprise Batch Digest with 72-Hour Cooldown Window

**What to build:** End-to-end notification throttling and delivery that selects upward `SURGING` transitions outside the 72-hour **Cooldown Window** and newly discovered **Curation** highlights, aggregates them into a single structured Markdown **Batch Digest** sent via `Apprise`, and transactionally updates notification watermarks only after delivery succeeds.

**Blocked by:** 03: Orthogonal Lifecycle State Machine with Cold-Start Baselines and Full-Table COOLED Decay

**Status:** done

- [x] A **Repository** enters the notification queue if and only if: (a) its `lifecycle_state` is `SURGING`, its current entry into `SURGING` has not been delivered, and that entry occurred at least 72 hours after `last_notified_at` (or `last_notified_at` is null); OR (b) `is_curated == 1` and `last_curated_notified_at` is null. A qualifying entry stays eligible for retry while the **Repository** remains `SURGING`.
- [x] A **Repository** that remains continuously in `SURGING` across consecutive 6-hour runs or oscillates `SURGING -> SUSTAINED -> SURGING` within 72 hours is suppressed by the **Cooldown Window** and not re-notified. An entry suppressed within the window is not deferred; a fresh entry at or after the 72-hour boundary can notify.
- [x] All qualifying repositories in a batch run are formatted into a single Markdown **Batch Digest** grouped into "Surging Breakouts" and "New Curated Highlights" sorted by `stars_delta_24h DESC`, sending at most one `Apprise` notification per pipeline run.
- [x] `last_notified_at`, `last_notified_state`, and `last_curated_notified_at` are updated in SQLite only when `Apprise` confirms delivery success (and are left untouched on delivery failure or `--dry-run`, allowing automatic retry on the next run).
- [x] Seam 1 integration tests with a spy `Apprise` dispatcher verify 72-hour cooldown suppression, flapping suppression, single-digest aggregation, and transactional watermark rollback on failed send.

## TDD and verification evidence

Each red run used the named Seam 1 test alone; the following green run included all notification tests written to that point:

| Slice | Red evidence | Green evidence |
| --- | --- | --- |
| First SURGING delivery | `test_first_surging_repository_is_delivered_and_watermarked`: pipeline rejected `notifier` | 1 passed |
| New Curation delivery | `test_new_curation_is_delivered_once_without_changing_surging_watermarks`: no message sent | 2 passed |
| One sorted digest | `test_one_digest_groups_and_sorts_all_qualifying_repositories`: breakout order incorrect | 3 passed |
| Actual re-entry | `test_continuous_and_cooldown_flapping_are_suppressed_but_later_reentry_notifies`: no second notification after a later transition | 4 passed |
| Delivery exception and retry | `test_delivery_exception_leaves_both_watermarks_for_retry_in_next_run`: `OSError` escaped | 5 passed |
| Dry-run preview | `test_dry_run_previews_digest_without_sending_or_advancing_watermarks`: pipeline rejected `dry_run` | 6 passed |
| Legacy SQL import | `test_pipeline_accepts_a_legacy_sql_dump_and_delivers_its_unnotified_surge`: imported rows lacked `surging_entered_at` | 7 passed |
| Command-line dry-run | `test_dry_run_command_previews_pending_notifications_without_configuration`: package had no `__main__` | 8 passed |
| Apprise configuration | `test_command_configures_apprise_and_delivers_pending_digest`: command rejected delivery configuration | 9 passed |
| Windows dry-run output | `test_dry_run_command_handles_legacy_windows_output_encoding`: GBK stdout raised `UnicodeEncodeError` | 14 passed |

Additional regression tests passed on first run for the exact 72-hour boundary versus one minute early, `False` delivery and retry with prior watermarks, SQLite transaction rollback after an injected write failure, and pending-transition SQL dump restoration. Final full run: `python -m pytest -v --basetemp <unique directory under .pytest_cache>` — **32 passed**.

The environment did not have the real Apprise SDK or a cached distribution, and a restricted-environment pip attempt could not locate a distribution. Delivery and configuration were verified against an Apprise-compatible spy; no real notification was sent. The project declares the Apprise dependency for installation in an environment with package access.
