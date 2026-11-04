# 03: Orthogonal Lifecycle State Machine with Cold-Start Baselines and Full-Table COOLED Decay

**What to build:** End-to-end momentum evaluation that computes 6h, 24h, and 7d star velocity deltas from historical **Snapshot** records and transitions every tracked **Repository** across the four mutually exclusive **Lifecycle State** values (`DISCOVERY`, `SURGING`, `SUSTAINED`, `COOLED`) using hybrid cold-start/warm-state language baselines and full-table 14-day decay while keeping **Curation** (`is_curated`) strictly orthogonal.

**Blocked by:** 02: Fault-Isolated Multi-Source Spike & Curation Ingestion (GitHub Search, OSSInsight, HelloGitHub, Opt-In Trendshift)

**Status:** ready-for-agent

- [ ] Velocity calculation accurately updates `stars_delta_6h`, `stars_delta_24h`, and `stars_delta_7d` on each **Repository** from longitudinal **Snapshot** deltas (falling back to reported period stars on first observation).
- [ ] In Cold Start (when a `primary_language` has `< 10` active repositories or `< 7` days of snapshot history), a **Repository** transitions to `SURGING` when `stars_delta_24h >= 150` or `stars_delta_6h >= 50`.
- [ ] In Warm State (`>= 10` language samples and `>= 7` days of history), a **Repository** transitions to `SURGING` only when `stars_delta_24h >= MAX(language_daily_mean * 3, 50)`, preventing niche languages with near-zero averages from firing false-positive surges.
- [ ] Repositories seen in trending signals within the last 14 days that have been tracked for at least 3 days, maintain `stars_delta_24h >= 10`, and are below the `SURGING` threshold transition to `SUSTAINED`.
- [ ] Every batch run scans all non-`COOLED` repositories across the entire table (including those absent from the current scrape) and transitions any **Repository** with `now - last_seen_in_trending_at >= 14 days` and `stars_delta_24h < 20` to `COOLED`.
- [ ] A `COOLED` **Repository** that reappears in a trending **Signal Source** or breaches the `SURGING` threshold re-awakens to `DISCOVERY` or `SURGING`, and `is_curated` remains intact across all `lifecycle_state` transitions.
- [ ] Seam 1 integration tests with simulated time progression verify cold-start fallback, warm niche-language floor (`50`), full-table 14-day `COOLED` decay, comeback re-entry, and `is_curated` orthogonality.
