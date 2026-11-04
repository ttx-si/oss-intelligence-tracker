"""SQLite storage and deterministic SQL dump import/export."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from oss_intelligence.models import RepositoryRecord, SnapshotRecord

SCHEMA_DDL = """\
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
    last_curated_notified_at TEXT
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
"""

_REPO_COLUMNS = (
    "ghr_id",
    "full_name",
    "description",
    "primary_language",
    "license_spdx",
    "topics_json",
    "repo_created_at",
    "stars_total",
    "forks_total",
    "stars_delta_6h",
    "stars_delta_24h",
    "stars_delta_7d",
    "lifecycle_state",
    "is_curated",
    "curated_issue",
    "curated_category",
    "first_seen_at",
    "last_seen_in_trending_at",
    "last_updated_at",
    "last_notified_at",
    "last_notified_state",
    "last_curated_notified_at",
)

_SNAPSHOT_EXPORT_COLUMNS = (
    "ghr_id",
    "captured_at",
    "signal_source",
    "stars",
    "forks",
    "source_rank",
    "period_stars_reported",
)


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


class IntelligenceStore:
    """Manages SQLite persistence and deterministic plain-text SQL dumps."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @property
    def db_path(self) -> Path:
        return self._db_path

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        """Opens a transactional SQLite session and guarantees handle closure on Windows."""
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._session() as conn:
            conn.executescript(SCHEMA_DDL)

    def get_repository_by_full_name(self, full_name: str) -> RepositoryRecord | None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT * FROM repositories WHERE full_name = ?",
                (full_name,),
            ).fetchone()
        return self._row_to_repository(row) if row is not None else None

    def get_repository_by_ghr_id(self, ghr_id: int) -> RepositoryRecord | None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT * FROM repositories WHERE ghr_id = ?",
                (ghr_id,),
            ).fetchone()
        return self._row_to_repository(row) if row is not None else None

    def save_repository(self, record: RepositoryRecord) -> None:
        topics_json = json.dumps(list(record.topics), ensure_ascii=False)
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO repositories (
                    ghr_id, full_name, description, primary_language, license_spdx,
                    topics_json, repo_created_at, stars_total, forks_total,
                    stars_delta_6h, stars_delta_24h, stars_delta_7d,
                    lifecycle_state, is_curated, curated_issue, curated_category,
                    first_seen_at, last_seen_in_trending_at, last_updated_at,
                    last_notified_at, last_notified_state, last_curated_notified_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ghr_id) DO UPDATE SET
                    full_name = excluded.full_name,
                    description = excluded.description,
                    primary_language = excluded.primary_language,
                    license_spdx = excluded.license_spdx,
                    topics_json = excluded.topics_json,
                    repo_created_at = excluded.repo_created_at,
                    stars_total = excluded.stars_total,
                    forks_total = excluded.forks_total,
                    stars_delta_6h = excluded.stars_delta_6h,
                    stars_delta_24h = excluded.stars_delta_24h,
                    stars_delta_7d = excluded.stars_delta_7d,
                    lifecycle_state = excluded.lifecycle_state,
                    is_curated = excluded.is_curated,
                    curated_issue = excluded.curated_issue,
                    curated_category = excluded.curated_category,
                    first_seen_at = excluded.first_seen_at,
                    last_seen_in_trending_at = excluded.last_seen_in_trending_at,
                    last_updated_at = excluded.last_updated_at,
                    last_notified_at = excluded.last_notified_at,
                    last_notified_state = excluded.last_notified_state,
                    last_curated_notified_at = excluded.last_curated_notified_at
                """,
                (
                    record.ghr_id,
                    record.full_name,
                    record.description,
                    record.primary_language,
                    record.license_spdx,
                    topics_json,
                    record.repo_created_at,
                    record.stars_total,
                    record.forks_total,
                    record.stars_delta_6h,
                    record.stars_delta_24h,
                    record.stars_delta_7d,
                    record.lifecycle_state,
                    1 if record.is_curated else 0,
                    record.curated_issue,
                    record.curated_category,
                    record.first_seen_at,
                    record.last_seen_in_trending_at,
                    record.last_updated_at,
                    record.last_notified_at,
                    record.last_notified_state,
                    record.last_curated_notified_at,
                ),
            )

    def add_snapshot(
        self,
        *,
        ghr_id: int,
        captured_at: str,
        signal_source: str,
        stars: int,
        forks: int = 0,
        source_rank: int | None = None,
        period_stars_reported: int = 0,
    ) -> None:
        with self._session() as conn:
            conn.execute(
                """
                INSERT INTO snapshots (
                    ghr_id, captured_at, signal_source, stars, forks, source_rank, period_stars_reported
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ghr_id,
                    captured_at,
                    signal_source,
                    stars,
                    forks,
                    source_rank,
                    period_stars_reported,
                ),
            )

    def list_repositories(self) -> list[RepositoryRecord]:
        with self._session() as conn:
            rows = conn.execute(
                "SELECT * FROM repositories ORDER BY ghr_id ASC"
            ).fetchall()
        return [self._row_to_repository(r) for r in rows]

    def list_snapshots(self, ghr_id: int | None = None) -> list[SnapshotRecord]:
        with self._session() as conn:
            if ghr_id is None:
                rows = conn.execute(
                    "SELECT * FROM snapshots ORDER BY ghr_id ASC, captured_at ASC, signal_source ASC, id ASC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM snapshots WHERE ghr_id = ? ORDER BY captured_at ASC, signal_source ASC, id ASC",
                    (ghr_id,),
                ).fetchall()
        return [self._row_to_snapshot(r) for r in rows]

    def export_sql_dump(self, dump_path: str | Path) -> Path:
        """Exports a deterministic SQL file ordered strictly by ghr_id ASC."""
        target = Path(dump_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        lines: list[str] = [
            "-- Deterministic SQL Dump (ordered by ghr_id ASC)",
            "PRAGMA foreign_keys = OFF;",
            "BEGIN TRANSACTION;",
            SCHEMA_DDL.strip(),
        ]

        repo_cols_sql = ", ".join(_REPO_COLUMNS)
        snap_cols_sql = ", ".join(_SNAPSHOT_EXPORT_COLUMNS)

        with self._session() as conn:
            repo_rows = conn.execute(
                f"SELECT {repo_cols_sql} FROM repositories ORDER BY ghr_id ASC"
            ).fetchall()
            for row in repo_rows:
                vals = ", ".join(_sql_literal(row[col]) for col in _REPO_COLUMNS)
                lines.append(f"INSERT INTO repositories ({repo_cols_sql}) VALUES ({vals});")

            snap_rows = conn.execute(
                f"SELECT {snap_cols_sql} FROM snapshots ORDER BY ghr_id ASC, captured_at ASC, signal_source ASC, id ASC"
            ).fetchall()
            for row in snap_rows:
                vals = ", ".join(_sql_literal(row[col]) for col in _SNAPSHOT_EXPORT_COLUMNS)
                lines.append(f"INSERT INTO snapshots ({snap_cols_sql}) VALUES ({vals});")

        lines.append("COMMIT;")
        lines.append("PRAGMA foreign_keys = ON;")
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return target

    def import_sql_dump(self, dump_path: str | Path) -> None:
        """Bootstraps or replaces database state from a deterministic SQL dump."""
        sql_text = Path(dump_path).read_text(encoding="utf-8")
        with self._session() as conn:
            conn.executescript(
                """
                DROP TABLE IF EXISTS snapshots;
                DROP TABLE IF EXISTS repositories;
                """
            )
            conn.executescript(sql_text)

    @staticmethod
    def _row_to_repository(row: sqlite3.Row) -> RepositoryRecord:
        topics_list = json.loads(row["topics_json"] or "[]")
        return RepositoryRecord(
            ghr_id=int(row["ghr_id"]),
            full_name=str(row["full_name"]),
            description=row["description"],
            primary_language=row["primary_language"],
            license_spdx=row["license_spdx"],
            topics=tuple(str(t) for t in topics_list),
            repo_created_at=row["repo_created_at"],
            stars_total=int(row["stars_total"]),
            forks_total=int(row["forks_total"]),
            stars_delta_6h=int(row["stars_delta_6h"]),
            stars_delta_24h=int(row["stars_delta_24h"]),
            stars_delta_7d=int(row["stars_delta_7d"]),
            lifecycle_state=str(row["lifecycle_state"]),
            is_curated=bool(row["is_curated"]),
            curated_issue=row["curated_issue"],
            curated_category=row["curated_category"],
            first_seen_at=str(row["first_seen_at"]),
            last_seen_in_trending_at=str(row["last_seen_in_trending_at"]),
            last_updated_at=str(row["last_updated_at"]),
            last_notified_at=row["last_notified_at"],
            last_notified_state=row["last_notified_state"],
            last_curated_notified_at=row["last_curated_notified_at"],
        )

    @staticmethod
    def _row_to_snapshot(row: sqlite3.Row) -> SnapshotRecord:
        return SnapshotRecord(
            id=int(row["id"]),
            ghr_id=int(row["ghr_id"]),
            captured_at=str(row["captured_at"]),
            signal_source=str(row["signal_source"]),
            stars=int(row["stars"]),
            forks=int(row["forks"]),
            source_rank=int(row["source_rank"]) if row["source_rank"] is not None else None,
            period_stars_reported=int(row["period_stars_reported"] or 0),
        )
