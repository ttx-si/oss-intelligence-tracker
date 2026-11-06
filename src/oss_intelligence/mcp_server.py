"""Local intelligence query tools backed by the published SQLite database."""

from __future__ import annotations

import os
import argparse
import asyncio
import json
import errno
import hashlib
import http.client
import re
import sqlite3
import subprocess
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from urllib.parse import urlsplit
from pathlib import Path
from contextlib import contextmanager
from typing import Callable

_locks_guard = threading.Lock()
_database_locks: dict[str, threading.RLock] = {}


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Never forward GitHub credentials to a different redirect origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected and urlsplit(req.full_url)[:2] != urlsplit(newurl)[:2]:
            redirected.remove_header("Authorization")
        return redirected


def _safe_urlopen(request, *, timeout):
    return urllib.request.build_opener(_SafeRedirectHandler()).open(request, timeout=timeout)


def _valid_database(path: Path) -> bool:
    try:
        with path.open("rb") as source:
            if source.read(16) != b"SQLite format 3\x00":
                return False
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            if conn.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                return False
            conn.execute("SELECT ghr_id, full_name, description, topics_json, primary_language, "
                         "lifecycle_state, is_curated, stars_delta_24h FROM repositories LIMIT 0")
            conn.execute("SELECT id, ghr_id, captured_at, signal_source FROM snapshots LIMIT 0")
            return True
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return False


def _database_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@contextmanager
def _exclusive_lock(lock_path: Path):
    """Acquire a cooperative thread and process lock."""
    key = os.path.normcase(str(lock_path.resolve()))
    with _locks_guard:
        local_lock = _database_locks.setdefault(key, threading.RLock())
    with local_lock:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as lock_file:
            # Windows can lock a byte beyond EOF; no read/write is needed before locking.
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt

                while True:
                    try:
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError as error:
                        if error.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                            raise
                        time.sleep(0.05)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _exclusive_database_lock(database: Path):
    return _exclusive_lock(database.with_name(database.name + ".lock"))


def _exclusive_sync_lock(database: Path):
    return _exclusive_lock(database.with_name(database.name + ".sync.lock"))


_CATEGORY_RULES: tuple[tuple[str, set[str], tuple[str, ...]], ...] = (
    (
        "AI / Agents & LLM",
        {
            "ai", "llm", "llms", "agent", "agents", "mcp", "rag", "openai",
            "anthropic", "claude", "gpt", "deepseek", "transformer", "diffusion",
            "vllm", "ollama", "langchain", "llamaindex", "autogen", "embedding",
            "embeddings", "neural", "pytorch", "tensorflow",
        },
        (
            "model-context-protocol", "machine-learning", "deep-learning",
            "artificial intelligence", "language model", "人工智能", "大模型",
            "机器学习", "深度学习",
        ),
    ),
    (
        "Data, Storage & Database",
        {
            "database", "databases", "sql", "olap", "oltp", "postgres",
            "postgresql", "sqlite", "mysql", "duckdb", "clickhouse", "redis",
            "storage", "parquet", "arrow", "etl", "nosql",
        },
        (
            "key-value", "vector-database", "data-engineering", "time-series",
            "distributed-database", "数据库", "存储",
        ),
    ),
    (
        "Security & Privacy",
        {
            "security", "cryptography", "encryption", "decryption", "pentest",
            "vulnerability", "cybersecurity", "oauth", "tls", "ssl", "firewall",
            "privacy", "openssl",
        },
        ("zero-trust", "安全", "加密"),
    ),
    (
        "Mobile & Desktop Apps",
        {
            "android", "ios", "flutter", "swiftui", "electron", "tauri",
            "macos", "multimedia", "mpv",
        },
        ("react-native", "kotlin-multiplatform", "desktop-app", "mobile-app", "media-player"),
    ),
    (
        "Web & Frontend",
        {
            "frontend", "react", "vue", "svelte", "nextjs", "angular",
            "tailwindcss", "css", "pwa", "webgl", "threejs", "dom", "ui",
        },
        ("web-framework", "ui-components", "web-performance", "前端"),
    ),
    (
        "Backend, Cloud & Infra",
        {
            "kubernetes", "k8s", "docker", "container", "containers", "cncf",
            "microservices", "serverless", "terraform", "devops", "prometheus",
            "monitoring", "grpc", "raft", "consensus",
        },
        ("cloud-native", "reverse-proxy", "load-balancer", "distributed-systems"),
    ),
    (
        "DevTools & Coding",
        {
            "linter", "formatter", "compiler", "ide", "editor", "neovim",
            "vscode", "cli", "terminal", "tui", "shell", "nushell", "debugger",
            "git", "testing", "benchmark", "zed", "ruff",
        },
        ("text-editor", "static-analysis", "package-manager", "visual-studio-code", "build-tool"),
    ),
)


def _classify_category(row: dict, topics: list[str]) -> str:
    text_blob = " ".join(
        str(part)
        for part in (
            " ".join(topics),
            row.get("description") or "",
            row.get("curated_category") or "",
        )
        if part
    ).lower()
    words = set(re.findall(r"[a-z0-9]+", text_blob)) | {t.lower() for t in topics}
    for label, word_tokens, phrases in _CATEGORY_RULES:
        if words & word_tokens or any(phrase in text_blob for phrase in phrases):
            return label

    curated_cat = str(row.get("curated_category") or "").strip()
    if curated_cat and curated_cat != "Uncategorized":
        return curated_cat
    lang = str(row.get("primary_language") or "").strip()
    if lang:
        return f"{lang} Ecosystem"
    return "General Open Source"


def _is_new_breakout(repo_created_at: str | None, last_updated_at: str | None) -> bool:
    if not repo_created_at or not last_updated_at:
        return False
    try:
        created_dt = datetime.fromisoformat(repo_created_at.strip().replace("Z", "+00:00"))
        updated_dt = datetime.fromisoformat(last_updated_at.strip().replace("Z", "+00:00"))
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)
        if updated_dt.tzinfo is None:
            updated_dt = updated_dt.replace(tzinfo=timezone.utc)
        age_seconds = (updated_dt - created_dt).total_seconds()
        return 0.0 <= age_seconds <= 30.0 * 86400.0
    except ValueError:
        return False


def _enrich_repository(row: sqlite3.Row | dict, signal_sources: list[str]) -> dict:
    item = dict(row)
    try:
        raw_topics = json.loads(item.get("topics_json") or "[]")
        topics = [str(t) for t in raw_topics] if isinstance(raw_topics, list) else []
    except ValueError:
        topics = []
    item["url"] = f"https://github.com/{item['full_name']}"
    item["topics"] = topics
    item["signal_sources"] = list(signal_sources)
    item["category"] = _classify_category(item, topics)
    item["is_new_breakout"] = _is_new_breakout(
        item.get("repo_created_at"),
        item.get("last_updated_at"),
    )
    return item


class IntelligenceService:
    """Query tools with a release synchronization guard."""

    def __init__(
        self,
        *,
        database: str | Path = "intelligence.db",
        dump_path: str | Path = "data/schema_data.sql",
        release_url: str | None = None,
        opener: Callable = _safe_urlopen,
        clock: Callable[[], float] = time.time,
        ttl_seconds: int | None = None,
    ) -> None:
        self.database = Path(database)
        self.dump_path = Path(dump_path)
        self.release_url = release_url
        self.opener = opener
        self.clock = clock
        self.ttl_seconds = ttl_seconds if ttl_seconds is not None else int(
            os.environ.get("MCP_SYNC_TTL_SECONDS", "3600")
        )
        self.metadata_path = self.database.with_name(self.database.name + ".sync.json")

    def _cached_metadata(self) -> tuple[dict, bool]:
        try:
            metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        try:
            stat = self.database.stat()
            same_database = (metadata.get("db_size") == stat.st_size
                             and metadata.get("db_mtime_ns") == stat.st_mtime_ns
                             and metadata.get("db_sha256") == _database_digest(self.database))
        except OSError:
            same_database = False
        return metadata, same_database

    def _is_fresh(self, metadata: dict, same_database: bool) -> bool:
        last_check = metadata.get("last_check")
        return bool(self.ttl_seconds > 0 and same_database
                    and metadata.get("release_url") == self.release_url
                    and isinstance(last_check, (int, float))
                    and 0 <= self.clock() - last_check < self.ttl_seconds)

    def _sync(self, *, force: bool = False) -> str:
        with _exclusive_database_lock(self.database):
            metadata, same_database = self._cached_metadata()
            valid_local = _valid_database(self.database)
        if not force and self._is_fresh(metadata, same_database):
            return "fresh"
        headers = {"Accept": "application/octet-stream", "User-Agent": "oss-intelligence-tracker"}
        if (valid_local and same_database and metadata.get("release_url") == self.release_url
                and isinstance(metadata.get("etag"), str)):
            headers["If-None-Match"] = metadata["etag"]
        if token := os.environ.get("GITHUB_TOKEN"):
            headers["Authorization"] = f"Bearer {token}"
        if not self.release_url:
            raise OSError("No Release URL configured")
        asset_url = self.release_url
        match = re.fullmatch(
            r"https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/releases/download/latest-db/intelligence\.db",
            asset_url,
        )
        if match and token:
            repository = match.group(1)
            api_base = f"https://api.github.com/repos/{repository}/releases"
            release_request = urllib.request.Request(
                f"{api_base}/tags/latest-db",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                         "User-Agent": "oss-intelligence-tracker"},
            )
            try:
                with self.opener(release_request, timeout=5) as response:
                    if response.status != 200:
                        raise OSError("GitHub Release lookup failed")
                    release = json.loads(response.read())
                asset_url = next(asset["url"] for asset in release["assets"]
                                 if asset["name"] == "intelligence.db")
                if not re.fullmatch(re.escape(api_base) + r"/assets/[0-9]+", asset_url):
                    raise ValueError("Unexpected Release asset URL")
            except (ValueError, KeyError, TypeError, StopIteration) as error:
                raise OSError("Could not find the Release database asset") from error
        request = urllib.request.Request(asset_url, headers=headers)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.database.with_name(self.database.name + ".tmp")
        try:
            with self.opener(request, timeout=5) as response, temporary.open("wb") as target:
                if response.status != 200:
                    raise OSError("Release asset request did not return HTTP 200")
                while chunk := response.read(65536):
                    target.write(chunk)
                etag = response.headers.get("ETag")
        except urllib.error.HTTPError as error:
            error.close()
            if error.code != 304 or not valid_local or "If-None-Match" not in headers:
                raise
            with _exclusive_database_lock(self.database):
                current, still_same = self._cached_metadata()
                if (not still_same or current.get("db_sha256") != metadata.get("db_sha256")
                        or current.get("etag") != metadata.get("etag")
                        or current.get("release_url") != self.release_url
                        or not _valid_database(self.database)):
                    raise OSError("Local database changed during Release revalidation")
                self._save_metadata(metadata.get("etag"))
            return "not_modified"
        if not _valid_database(temporary):
            raise OSError("Release asset is not a valid intelligence database")
        with _exclusive_database_lock(self.database):
            os.replace(temporary, self.database)
            self._save_metadata(etag)
        return "updated"

    def _save_metadata(self, etag: str | None) -> None:
        stat = self.database.stat()
        payload = {"last_check": self.clock(), "etag": etag, "release_url": self.release_url,
                   "db_size": stat.st_size, "db_mtime_ns": stat.st_mtime_ns,
                   "db_sha256": _database_digest(self.database)}
        temporary = self.metadata_path.with_name(self.metadata_path.name + ".tmp")
        try:
            temporary.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(temporary, self.metadata_path)
        finally:
            temporary.unlink(missing_ok=True)

    def get_surging_repos(self) -> dict:
        return self.search_repositories(state="SURGING")

    def search_repositories(
        self, language: str | None = None, state: str | None = None,
        curated: bool | None = None, keyword: str | None = None,
        min_stars_delta_24h: int | None = None, source: str | None = None,
        category: str | None = None, limit: int = 50,
    ) -> dict:
        conditions = ["1 = 1"]
        parameters: list = []
        if min_stars_delta_24h is not None:
            conditions.append("stars_delta_24h >= ?")
            parameters.append(min_stars_delta_24h)
        if language is not None:
            conditions.append("primary_language = ? COLLATE NOCASE")
            parameters.append(language)
        if state is not None:
            conditions.append("lifecycle_state = ?")
            parameters.append(state.upper())
        if curated is not None:
            conditions.append("is_curated = ?")
            parameters.append(int(curated))
        if keyword:
            conditions.append("instr(lower(full_name || ' ' || coalesce(description, '') || ' ' || topics_json), lower(?)) > 0")
            parameters.append(keyword)
        if source:
            conditions.append(
                "EXISTS (SELECT 1 FROM snapshots s WHERE s.ghr_id = repositories.ghr_id "
                "AND lower(s.signal_source) = lower(?))"
            )
            parameters.append(source.strip())
        clamped_limit = max(1, min(limit, 500))
        sql = (
            "SELECT * FROM repositories WHERE "
            + " AND ".join(conditions)
            + " ORDER BY stars_delta_24h DESC, ghr_id ASC"
        )
        if not category:
            sql += " LIMIT ?"
            parameters.append(clamped_limit)

        _, metadata = self._ready()
        with _exclusive_database_lock(self.database):
            conn = sqlite3.connect(self.database)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(sql, parameters).fetchall()
                ghr_ids = [int(r["ghr_id"]) for r in rows]
                sources_by_id: dict[int, list[str]] = {gid: [] for gid in ghr_ids}
                if ghr_ids:
                    placeholders = ", ".join("?" for _ in ghr_ids)
                    snap_rows = conn.execute(
                        f"SELECT DISTINCT ghr_id, signal_source FROM snapshots "
                        f"WHERE ghr_id IN ({placeholders}) ORDER BY ghr_id ASC, signal_source ASC",
                        ghr_ids,
                    ).fetchall()
                    for srow in snap_rows:
                        sources_by_id.setdefault(int(srow["ghr_id"]), []).append(
                            str(srow["signal_source"])
                        )
                enriched = [
                    _enrich_repository(row, sources_by_id.get(int(row["ghr_id"]), []))
                    for row in rows
                ]
                if category:
                    needle = category.strip().lower()
                    enriched = [
                        repo
                        for repo in enriched
                        if needle in repo["category"].lower()
                        or needle in str(repo.get("curated_category") or "").lower()
                    ][:clamped_limit]

                sources_breakdown: dict[str, int] = {}
                categories_breakdown: dict[str, int] = {}
                for repo in enriched:
                    for src in repo["signal_sources"]:
                        sources_breakdown[src] = sources_breakdown.get(src, 0) + 1
                    cat = repo["category"]
                    categories_breakdown[cat] = categories_breakdown.get(cat, 0) + 1

                enriched_metadata = {
                    **metadata,
                    "total_returned": len(enriched),
                    "sources_breakdown": sources_breakdown,
                    "categories_breakdown": categories_breakdown,
                }
                return {"repositories": enriched, "metadata": enriched_metadata}
            finally:
                conn.close()

    def get_curated_highlights(self, limit: int = 50) -> dict:
        return self.search_repositories(curated=True, limit=limit)

    def get_repo_trajectory(self, ghr_id: int) -> dict:
        _, metadata = self._ready()
        with _exclusive_database_lock(self.database):
            conn = sqlite3.connect(self.database)
            conn.row_factory = sqlite3.Row
            try:
                repository = conn.execute("SELECT * FROM repositories WHERE ghr_id = ?", (ghr_id,)).fetchone()
                rows = conn.execute(
                    "SELECT * FROM snapshots WHERE ghr_id = ? "
                    "ORDER BY captured_at ASC, signal_source ASC, id ASC", (ghr_id,),
                ).fetchall()
                distinct_sources = sorted({str(r["signal_source"]) for r in rows})
                enriched_repo = (
                    _enrich_repository(repository, distinct_sources)
                    if repository is not None
                    else None
                )
                return {"repository": enriched_repo,
                        "snapshots": [dict(row) for row in rows], "metadata": metadata}
            finally:
                conn.close()

    def sync_database_now(self) -> dict:
        status, metadata = self._ready(force=True)
        return {"status": status, "metadata": metadata}

    def _ready(self, *, force: bool = False) -> tuple[str, dict]:
        if not force:
            with _exclusive_database_lock(self.database):
                cached, same_database = self._cached_metadata()
                if self._is_fresh(cached, same_database):
                    return "fresh", {}
        with _exclusive_sync_lock(self.database):
            return self._sync_or_fallback(force=force)

    def _sync_or_fallback(self, *, force: bool) -> tuple[str, dict]:
        metadata = {}
        try:
            status = self._sync(force=force)
        except (OSError, ValueError, http.client.HTTPException):
            self.database.with_name(self.database.name + ".tmp").unlink(missing_ok=True)
            with _exclusive_database_lock(self.database):
                if not _valid_database(self.database):
                    sql = self.dump_path.read_text(encoding="utf-8")
                    self.database.parent.mkdir(parents=True, exist_ok=True)
                    temporary = self.database.with_name(self.database.name + ".tmp")
                    try:
                        conn = sqlite3.connect(temporary)
                        try:
                            conn.executescript(sql)
                        finally:
                            conn.close()
                        if not _valid_database(temporary):
                            raise OSError("SQL dump did not produce a valid intelligence database")
                        self.metadata_path.unlink(missing_ok=True)
                        os.replace(temporary, self.database)
                    finally:
                        temporary.unlink(missing_ok=True)
            metadata["staleness_warning"] = "Release sync failed; using the local database."
            status = "offline"
        return status, metadata


def create_mcp_server(service: IntelligenceService):
    """Register the structured query interface with FastMCP."""
    from fastmcp import FastMCP

    server = FastMCP("Open Source Intelligence Tracker")

    @server.tool(description="List currently SURGING Repositories.")
    async def get_surging_repos() -> dict:
        return await asyncio.to_thread(service.get_surging_repos)

    @server.tool(description="Filter Repositories by language, Lifecycle State, Curation, Signal Source, domain category, keyword and star velocity.")
    async def search_repositories(
        language: str | None = None, state: str | None = None,
        curated: bool | None = None, keyword: str | None = None,
        min_stars_delta_24h: int | None = None, source: str | None = None,
        category: str | None = None, limit: int = 50,
    ) -> dict:
        return await asyncio.to_thread(
            service.search_repositories, language=language, state=state, curated=curated,
            keyword=keyword, min_stars_delta_24h=min_stars_delta_24h, source=source,
            category=category, limit=limit,
        )

    @server.tool(description="List curated Repository highlights.")
    async def get_curated_highlights(limit: int = 50) -> dict:
        return await asyncio.to_thread(service.get_curated_highlights, limit=limit)

    @server.tool(description="Retrieve one Repository and its ordered Snapshot trajectory by GitHub repository ID.")
    async def get_repo_trajectory(ghr_id: int) -> dict:
        return await asyncio.to_thread(service.get_repo_trajectory, ghr_id=ghr_id)

    @server.tool(description="Immediately check the Release for a fresh database.")
    async def sync_database_now() -> dict:
        return await asyncio.to_thread(service.sync_database_now)

    return server


def _default_release_url() -> str | None:
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        try:
            remote = subprocess.run(
                ["git", "config", "--get", "remote.origin.url"], capture_output=True,
                text=True, timeout=5, check=True,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return None
        match = re.fullmatch(
            r"(?:https://github\.com/|git@github\.com:|ssh://git@github\.com/)([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+?)(?:\.git)?/?",
            remote,
        )
        if not match:
            return None
        repository = match.group(1)
    return f"https://github.com/{repository}/releases/download/latest-db/intelligence.db"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the local Open Source Intelligence FastMCP server.")
    parser.add_argument("--database", default=os.environ.get("MCP_DATABASE", "intelligence.db"))
    parser.add_argument("--dump", default=os.environ.get("MCP_SQL_DUMP", "data/schema_data.sql"))
    parser.add_argument("--release-url", default=os.environ.get("GITHUB_RELEASE_DB_URL"))
    args = parser.parse_args(argv)
    service = IntelligenceService(
        database=args.database, dump_path=args.dump, release_url=args.release_url or _default_release_url(),
    )
    create_mcp_server(service).run(transport="stdio", show_banner=False)


if __name__ == "__main__":
    main()
