-- Deterministic SQL Dump (ordered by ghr_id ASC)
PRAGMA foreign_keys = OFF;
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS repositories (
    ghr_id INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL UNIQUE,
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
    curated_issue TEXT,
    curated_category TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_in_trending_at TEXT NOT NULL,
    last_updated_at TEXT NOT NULL,
    last_notified_at TEXT,
    last_notified_state TEXT,
    last_curated_notified_at TEXT,
    surging_entered_at TEXT
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ghr_id INTEGER NOT NULL REFERENCES repositories(ghr_id) ON DELETE CASCADE,
    captured_at TEXT NOT NULL,
    signal_source TEXT NOT NULL,
    stars INTEGER NOT NULL,
    forks INTEGER NOT NULL DEFAULT 0,
    source_rank INTEGER,
    period_stars_reported INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_snapshots_ghr_captured ON snapshots(ghr_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_repos_state_lang ON repositories(lifecycle_state, primary_language);
COMMIT;
PRAGMA foreign_keys = ON;
