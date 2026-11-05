"""Local intelligence query tools backed by the published SQLite database."""

from __future__ import annotations

import os
import argparse
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
        min_stars_delta_24h: int | None = None, limit: int = 50,
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
        parameters.append(max(1, min(limit, 500)))
        _, metadata = self._ready()
        with _exclusive_database_lock(self.database):
            conn = sqlite3.connect(self.database)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    "SELECT * FROM repositories WHERE " + " AND ".join(conditions)
                    + " ORDER BY stars_delta_24h DESC, ghr_id ASC LIMIT ?", parameters,
                ).fetchall()
                return {"repositories": [dict(row) for row in rows], "metadata": metadata}
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
                return {"repository": dict(repository) if repository else None,
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
    server.tool(service.get_surging_repos, description="List currently SURGING Repositories.")
    server.tool(service.search_repositories, description="Filter Repositories by language, Lifecycle State, Curation, keyword and star velocity.")
    server.tool(service.get_curated_highlights, description="List curated Repository highlights.")
    server.tool(service.get_repo_trajectory, description="Retrieve one Repository and its ordered Snapshot trajectory by GitHub repository ID.")
    server.tool(service.sync_database_now, description="Immediately check the Release for a fresh database.")
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
    create_mcp_server(service).run(transport="stdio")


if __name__ == "__main__":
    main()
