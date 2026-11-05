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

## Review repairs and additional verification

The review of commit `7ee6954bac769b963b3ec67ad79b78669cb24009` led to the following new regression cycles. All tests exercise the pipeline or CLI entry point; clocks, HTTP responses, and notification services are controlled at their boundaries.

| Finding | Observed red | Green and repair |
| --- | --- | --- |
| Run-start timestamp used for a later `SURGING` entry and successful delivery | `test_slow_run_measures_transition_and_delivery_at_the_actual_times`: a source fetch crossed the 72-hour boundary but the breakout was absent from the sent digest | Sample a clock at each state entry and after successful delivery; the notification suite passed with the timestamp assertions. |
| Slow delivery shortened the effective cooldown | After the first time repair, the delivery-clock call was temporarily disabled to isolate the original behavior: `test_slow_delivery_starts_cooldown_when_the_notification_succeeds` observed a second notification after only 71.5 hours since success | Restoring the delivery clock suppressed the early entry and allowed a fresh entry at exactly 72 hours. |
| Slow lifecycle evaluation still backdated an entry | `test_slow_lifecycle_evaluation_records_the_actual_surging_entry`: evaluation crossed the boundary but sent no breakout | Capture the entry time when each repository is updated; the lifecycle and notification suites passed. |
| Duplicate UTC parsers disagreed on surrounding whitespace | `test_spaced_utc_watermarks_allow_an_eligible_surging_entry`: `ValueError` on a timestamp with surrounding spaces | Both lifecycle and notifications now use the same UTC parser; the lifecycle and notification suites passed. |
| The default CLI lacked its `gtrending` install dependency | `test_default_cli_path_declares_and_uses_gtrending`: the default CLI ingested the stubbed source, but the required distribution was absent from project dependencies | Declare `gtrending>=0.5.1`, the distribution providing the adapter's `fetch_repos()` API. |
| Apprise 2.1 returns a successful result object rather than the literal `True` | `test_successful_apprise_result_advances_watermarks`: real Apprise 2.1 dispatched to an in-memory service, but neither watermark advanced. After improving the test to use the real dispatcher, the old `is True` check was temporarily restored to observe the same red again. | Use the SDK's documented boolean outcome contract. Both old boolean notifiers and Apprise result objects are supported. |

One intermediate green attempt failed collection because `SupportsBool` was incorrectly imported from `typing`; it was replaced by a local boolean-result protocol before the successful runs. This was a test-collection error, not behavioral red evidence.

Additional checks for a real Apprise failure and a partial success, and for the real `gtrending` parser using a simulated HTTP response, passed on their first run; they are not claimed as red-first evidence. `apprise==2.1.0` and `gtrending==0.5.1` were installed from official PyPI into an isolated `.pytest_cache` target, with no global installation. The Apprise service used in tests only records into memory, and the gtrending HTTP response is simulated: no real notification or upstream scrape was performed.

Final full run with the isolated SDK target on `PYTHONPATH`: `python -m pytest -v --basetemp .pytest_cache/ticket04-fix-final-6009c3c7975d4e03bb897a6cf045ad30` — **41 passed**. No commit or branch change was made.
