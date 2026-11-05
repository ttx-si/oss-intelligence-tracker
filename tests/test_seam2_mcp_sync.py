"""Seam 2: public intelligence tools against a simulated Release boundary."""

from __future__ import annotations

import io
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import tomllib
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from pathlib import Path

from oss_intelligence.mcp_server import IntelligenceService
from oss_intelligence.models import RepositoryRecord
from oss_intelligence.store import IntelligenceStore


class ReleaseResponse(io.BytesIO):
    def __init__(self, content: bytes, *, status: int = 200, etag: str = '"v1"') -> None:
        super().__init__(content)
        self.status = status
        self.headers = {"ETag": etag}


class ReleaseEndpoint:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.requests = []

    def __call__(self, request, *, timeout):
        self.requests.append((request, timeout))
        return ReleaseResponse(self.content)


def populated_database(path: Path) -> bytes:
    store = IntelligenceStore(path)
    store.save_repository(RepositoryRecord(
        ghr_id=1, full_name="org/rocket", description="Fast database", primary_language="Rust",
        license_spdx="MIT", topics=("database",), repo_created_at=None, stars_total=1000,
        forks_total=10, stars_delta_6h=60, stars_delta_24h=300, stars_delta_7d=500,
        lifecycle_state="SURGING", is_curated=True, curated_issue="vol-1", curated_category="tools",
        first_seen_at="2026-11-01T00:00:00Z", last_seen_in_trending_at="2026-11-04T00:00:00Z",
        last_updated_at="2026-11-04T00:00:00Z",
    ))
    return path.read_bytes()


def test_query_downloads_release_database_and_returns_surging_repositories(tmp_path: Path) -> None:
    endpoint = ReleaseEndpoint(populated_database(tmp_path / "release.db"))
    service = IntelligenceService(
        database=tmp_path / "intelligence.db", release_url="https://example.invalid/intelligence.db",
        opener=endpoint, clock=lambda: 10000.0,
    )

    result = service.get_surging_repos()

    assert [repo["full_name"] for repo in result["repositories"]] == ["org/rocket"]
    assert result["repositories"][0]["stars_delta_24h"] == 300
    assert len(endpoint.requests) == 1


def test_repeated_queries_within_default_ttl_make_no_network_request(tmp_path: Path) -> None:
    endpoint = ReleaseEndpoint(populated_database(tmp_path / "release.db"))
    now = [10000.0]
    service = IntelligenceService(
        database=tmp_path / "intelligence.db", release_url="https://example.invalid/intelligence.db",
        opener=endpoint, clock=lambda: now[0],
    )

    service.get_surging_repos()
    now[0] += 3599
    result = service.get_surging_repos()

    assert result["repositories"][0]["full_name"] == "org/rocket"
    assert len(endpoint.requests) == 1


def test_expired_ttl_sends_etag_and_private_token_and_304_refreshes_freshness(
    tmp_path: Path, monkeypatch,
) -> None:
    content = populated_database(tmp_path / "release.db")
    requests = []

    def endpoint(request, *, timeout):
        requests.append((request, timeout))
        if len(requests) == 1:
            return ReleaseResponse(content, etag='"release-v1"')
        raise urllib.error.HTTPError(request.full_url, 304, "Not Modified", {}, None)

    monkeypatch.setenv("GITHUB_TOKEN", "private-token")
    now = [10000.0]
    kwargs = dict(database=tmp_path / "intelligence.db",
                  release_url="https://example.invalid/intelligence.db", opener=endpoint,
                  clock=lambda: now[0])
    IntelligenceService(**kwargs).get_surging_repos()
    now[0] += 3600
    service = IntelligenceService(**kwargs)
    assert service.get_surging_repos()["repositories"][0]["full_name"] == "org/rocket"
    assert requests[1][0].get_header("If-none-match") == '"release-v1"'
    assert requests[1][0].get_header("Authorization") == "Bearer private-token"
    assert requests[1][1] == 5
    now[0] += 3599
    service.get_surging_repos()
    assert len(requests) == 2


def test_release_failure_uses_existing_database_and_reports_staleness(tmp_path: Path) -> None:
    database = tmp_path / "intelligence.db"
    original = populated_database(database)

    def offline(request, *, timeout):
        raise TimeoutError("release unavailable")

    result = IntelligenceService(
        database=database, release_url="https://example.invalid/intelligence.db", opener=offline,
    ).get_surging_repos()

    assert result["repositories"][0]["full_name"] == "org/rocket"
    assert "staleness_warning" in result["metadata"]
    assert database.read_bytes() == original


def test_offline_without_local_database_materializes_tracked_sql_dump(tmp_path: Path) -> None:
    source = tmp_path / "release.db"
    populated_database(source)
    dump = IntelligenceStore(source).export_sql_dump(tmp_path / "schema_data.sql")

    def offline(request, *, timeout):
        raise OSError("offline")

    service = IntelligenceService(
        database=tmp_path / "intelligence.db", dump_path=dump,
        release_url="https://example.invalid/intelligence.db", opener=offline,
    )
    result = service.get_surging_repos()

    assert result["repositories"][0]["full_name"] == "org/rocket"
    assert "staleness_warning" in result["metadata"]
    assert (tmp_path / "intelligence.db").exists()


def test_invalid_release_response_preserves_the_existing_database(tmp_path: Path) -> None:
    database = tmp_path / "intelligence.db"
    original = populated_database(database)
    endpoint = ReleaseEndpoint(b"<html>not a sqlite database</html>")

    result = IntelligenceService(
        database=database, release_url="https://example.invalid/intelligence.db", opener=endpoint,
    ).get_surging_repos()

    assert result["repositories"][0]["full_name"] == "org/rocket"
    assert "staleness_warning" in result["metadata"]
    assert database.read_bytes() == original


def test_simultaneous_expired_queries_share_one_sync_and_both_succeed(tmp_path: Path) -> None:
    content = populated_database(tmp_path / "release.db")
    entered = threading.Event()
    unexpected_request = threading.Event()
    resume = threading.Event()
    requests = []

    def endpoint(request, *, timeout):
        requests.append(request)
        if len(requests) == 2:
            entered.set()
            assert resume.wait(5)
        elif len(requests) > 2:
            unexpected_request.set()
        return ReleaseResponse(content)

    now = [10000.0]
    service = IntelligenceService(
        database=tmp_path / "intelligence.db", release_url="https://example.invalid/intelligence.db",
        opener=endpoint, clock=lambda: now[0],
    )
    service.get_surging_repos()
    now[0] += 3600
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(service.get_surging_repos)
        assert entered.wait(5)
        second = executor.submit(service.get_surging_repos)
        try:
            unexpected_request.wait(0.5)
        finally:
            resume.set()
        results = [first.result(timeout=5), second.result(timeout=5)]

    assert len(requests) == 2
    assert all(item["repositories"][0]["full_name"] == "org/rocket" for item in results)


def test_force_sync_hot_swaps_after_an_existing_query_on_windows(tmp_path: Path) -> None:
    release = tmp_path / "release.db"
    first_version = populated_database(release)
    conn = sqlite3.connect(release)
    try:
        conn.execute("UPDATE repositories SET stars_total = 2222 WHERE ghr_id = 1")
        conn.commit()
    finally:
        conn.close()
    second_version = release.read_bytes()
    versions = iter([first_version, second_version])

    def endpoint(request, *, timeout):
        return ReleaseResponse(next(versions))

    service = IntelligenceService(
        database=tmp_path / "intelligence.db", release_url="https://example.invalid/intelligence.db",
        opener=endpoint, clock=lambda: 10000.0,
    )
    assert service.get_surging_repos()["repositories"][0]["stars_total"] == 1000
    assert service.sync_database_now()["status"] == "updated"
    assert service.get_surging_repos()["repositories"][0]["stars_total"] == 2222


def test_search_intersects_language_state_curation_keyword_and_velocity(tmp_path: Path) -> None:
    release = tmp_path / "release.db"
    populated_database(release)
    store = IntelligenceStore(release)
    original = store.get_repository_by_ghr_id(1)
    store.save_repository(replace(original, ghr_id=2, full_name="org/quiet", is_curated=False,
                                  stars_delta_24h=4))
    store.save_repository(replace(original, ghr_id=3, full_name="org/other", primary_language="Go"))
    endpoint = ReleaseEndpoint(release.read_bytes())
    service = IntelligenceService(
        database=tmp_path / "intelligence.db", release_url="https://example.invalid/intelligence.db",
        opener=endpoint,
    )

    result = service.search_repositories(language="Rust", state="SURGING", curated=True,
                                         keyword="database", min_stars_delta_24h=50)

    assert [repo["full_name"] for repo in result["repositories"]] == ["org/rocket"]


def test_curated_highlights_and_repository_trajectory_report_snapshot_history(tmp_path: Path) -> None:
    release = tmp_path / "release.db"
    populated_database(release)
    store = IntelligenceStore(release)
    store.add_snapshot(ghr_id=1, captured_at="2026-11-02T00:00:00Z", signal_source="gtrending", stars=700)
    store.add_snapshot(ghr_id=1, captured_at="2026-11-04T00:00:00Z", signal_source="gh_search", stars=1000)
    service = IntelligenceService(
        database=tmp_path / "intelligence.db", release_url="https://example.invalid/intelligence.db",
        opener=ReleaseEndpoint(release.read_bytes()),
    )

    assert service.get_curated_highlights()["repositories"][0]["curated_issue"] == "vol-1"
    trajectory = service.get_repo_trajectory(ghr_id=1)
    assert trajectory["repository"]["full_name"] == "org/rocket"
    assert [snapshot["stars"] for snapshot in trajectory["snapshots"]] == [700, 1000]


def test_real_fastmcp_protocol_exposes_structured_query_tools(tmp_path: Path) -> None:
    fastmcp = pytest.importorskip("fastmcp")
    from oss_intelligence.mcp_server import create_mcp_server

    service = IntelligenceService(
        database=tmp_path / "intelligence.db", release_url="https://example.invalid/intelligence.db",
        opener=ReleaseEndpoint(populated_database(tmp_path / "release.db")),
    )

    async def exercise():
        async with fastmcp.Client(create_mcp_server(service)) as client:
            names = {tool.name for tool in await client.list_tools()}
            assert names == {"get_surging_repos", "search_repositories", "get_curated_highlights",
                             "get_repo_trajectory", "sync_database_now"}
            result = await client.call_tool("search_repositories", {"language": "Rust"})
            assert result.data["repositories"][0]["full_name"] == "org/rocket"

    asyncio.run(exercise())


def test_mcp_server_has_installed_entry_point_and_command_line_help() -> None:
    root = Path(__file__).resolve().parents[1]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert any(dependency.startswith("fastmcp") for dependency in project["dependencies"])
    assert project["scripts"]["oss-intelligence-mcp"] == "oss_intelligence.mcp_server:main"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src") + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run([sys.executable, "-m", "oss_intelligence.mcp_server", "--help"],
                               env=env, capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0
    assert "--release-url" in completed.stdout and "--database" in completed.stdout


def test_private_rolling_release_resolves_current_asset_through_github_api(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "private-token")
    content = populated_database(tmp_path / "release.db")
    requests = []

    def endpoint(request, *, timeout):
        requests.append(request)
        if len(requests) == 1:
            body = {"assets": [{"name": "intelligence.db", "url":
                    "https://api.github.com/repos/org/private/releases/assets/123"}]}
            return ReleaseResponse(json.dumps(body).encode())
        return ReleaseResponse(content)

    service = IntelligenceService(
        database=tmp_path / "intelligence.db",
        release_url="https://github.com/org/private/releases/download/latest-db/intelligence.db",
        opener=endpoint,
    )
    assert service.get_surging_repos()["repositories"][0]["full_name"] == "org/rocket"
    assert [request.full_url for request in requests] == [
        "https://api.github.com/repos/org/private/releases/tags/latest-db",
        "https://api.github.com/repos/org/private/releases/assets/123",
    ]
    assert all(request.get_header("Authorization") == "Bearer private-token" for request in requests)
    assert requests[1].get_header("Accept") == "application/octet-stream"


def test_corrupt_local_database_falls_back_to_sql_dump_when_offline(tmp_path: Path) -> None:
    release = tmp_path / "release.db"
    populated_database(release)
    dump = IntelligenceStore(release).export_sql_dump(tmp_path / "schema_data.sql")
    database = tmp_path / "intelligence.db"
    database.write_bytes(b"SQLite format 3\0" + b"broken")

    def offline(request, *, timeout):
        raise TimeoutError("offline")

    result = IntelligenceService(database=database, dump_path=dump,
                                 release_url="https://example.invalid/db", opener=offline).get_surging_repos()
    assert result["repositories"][0]["full_name"] == "org/rocket"
    assert "staleness_warning" in result["metadata"]


def test_corrupt_cache_is_not_revalidated_with_an_old_etag(tmp_path: Path) -> None:
    content = populated_database(tmp_path / "release.db")
    requests = []

    def endpoint(request, *, timeout):
        requests.append(request)
        if request.get_header("If-none-match"):
            raise urllib.error.HTTPError(request.full_url, 304, "Not Modified", {}, None)
        return ReleaseResponse(content)

    now = [10000.0]
    database = tmp_path / "intelligence.db"
    service = IntelligenceService(database=database, release_url="https://example.invalid/db",
                                  opener=endpoint, clock=lambda: now[0])
    service.get_surging_repos()
    database.write_bytes(b"broken")
    now[0] += 3600

    assert service.get_surging_repos()["repositories"][0]["full_name"] == "org/rocket"
    assert requests[1].get_header("If-none-match") is None


def test_separate_server_processes_share_the_database_lock(tmp_path: Path) -> None:
    populated_database(tmp_path / "release.db")
    script = tmp_path / "client.py"
    script.write_text('''
import io, os, time
from pathlib import Path
from oss_intelligence.mcp_server import IntelligenceService

class Response(io.BytesIO):
    headers = {"ETag": "v1"}
    status = 200

def endpoint(request, *, timeout):
    with open(os.environ["LOG"], "a", encoding="utf-8") as log:
        log.write("request\\n")
    Path(os.environ["READY"]).touch()
    until = time.monotonic() + 10
    while not Path(os.environ["RESUME"]).exists() and time.monotonic() < until:
        time.sleep(0.02)
    return Response(Path(os.environ["ASSET"]).read_bytes())

Path(os.environ["BEGIN"]).touch()
service = IntelligenceService(database=os.environ["DB"], release_url="https://example.invalid/db",
                              opener=endpoint, clock=lambda: 10000.0)
assert service.get_surging_repos()["repositories"][0]["full_name"] == "org/rocket"
''', encoding="utf-8")
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.update({"PYTHONPATH": str(root / "src"), "DB": str(tmp_path / "intelligence.db"),
                "ASSET": str(tmp_path / "release.db"), "LOG": str(tmp_path / "requests.log"),
                "READY": str(tmp_path / "ready"), "RESUME": str(tmp_path / "resume")})
    first_env = {**env, "BEGIN": str(tmp_path / "begin1")}
    second_env = {**env, "BEGIN": str(tmp_path / "begin2")}

    def wait_for(path: Path) -> None:
        deadline = time.monotonic() + 10
        while not path.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert path.exists(), f"Timed out waiting for {path.name}"

    processes = []
    try:
        processes.append(subprocess.Popen([sys.executable, str(script)], env=first_env,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        wait_for(tmp_path / "ready")
        processes.append(subprocess.Popen([sys.executable, str(script)], env=second_env,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE))
        wait_for(tmp_path / "begin2")
        time.sleep(0.5)
        (tmp_path / "resume").touch()
        for process in processes:
            _, stderr = process.communicate(timeout=10)
            assert process.returncode == 0, stderr.decode(errors="replace")
        assert (tmp_path / "requests.log").read_text(encoding="utf-8").splitlines() == ["request"]
    finally:
        (tmp_path / "resume").touch()
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_mcp_command_serves_a_real_stdio_session_offline_from_sql(tmp_path: Path) -> None:
    fastmcp = pytest.importorskip("fastmcp")
    from fastmcp.client.transports import StdioTransport

    source = tmp_path / "release.db"
    populated_database(source)
    dump = IntelligenceStore(source).export_sql_dump(tmp_path / "schema_data.sql")
    env = os.environ.copy()
    for key in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "GITHUB_RELEASE_DB_URL"):
        env.pop(key, None)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src") + os.pathsep + env.get("PYTHONPATH", "")
    transport = StdioTransport(command=sys.executable, args=["-m", "oss_intelligence.mcp_server",
                               "--database", str(tmp_path / "intelligence.db"), "--dump", str(dump)],
                               env=env, cwd=str(tmp_path), keep_alive=False)

    async def exercise():
        async with fastmcp.Client(transport, timeout=15) as client:
            result = await client.call_tool("get_curated_highlights", {})
            assert result.data["repositories"][0]["full_name"] == "org/rocket"
            assert "staleness_warning" in result.data["metadata"]

    asyncio.run(exercise())


def test_release_redirect_never_leaks_token_to_another_origin(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "private-token")
    payload = populated_database(tmp_path / "release.db")
    headers_seen = {}

    class Target(BaseHTTPRequestHandler):
        def do_GET(self):
            headers_seen["target"] = self.headers.get("Authorization")
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    target = ThreadingHTTPServer(("127.0.0.1", 0), Target)

    class Redirect(BaseHTTPRequestHandler):
        def do_GET(self):
            headers_seen["source"] = self.headers.get("Authorization")
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{target.server_port}/asset")
            self.end_headers()

        def log_message(self, *args):
            pass

    source = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
    threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in (source, target)]
    for thread in threads:
        thread.start()
    try:
        service = IntelligenceService(database=tmp_path / "intelligence.db",
                                      release_url=f"http://127.0.0.1:{source.server_port}/asset")
        assert service.get_surging_repos()["repositories"][0]["full_name"] == "org/rocket"
        assert headers_seen == {"source": "Bearer private-token", "target": None}
    finally:
        for server in (source, target):
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=5)


def test_non_200_response_does_not_replace_the_cached_database(tmp_path: Path) -> None:
    database = tmp_path / "intelligence.db"
    original = populated_database(database)
    updated = tmp_path / "updated.db"
    updated.write_bytes(original)
    conn = sqlite3.connect(updated)
    try:
        conn.execute("UPDATE repositories SET stars_total = 2222")
        conn.commit()
    finally:
        conn.close()

    def endpoint(request, *, timeout):
        return ReleaseResponse(updated.read_bytes(), status=206)

    result = IntelligenceService(database=database, release_url="https://example.invalid/db",
                                 opener=endpoint).get_surging_repos()
    assert result["repositories"][0]["stars_total"] == 1000
    assert "staleness_warning" in result["metadata"]


def test_invalid_release_url_uses_available_local_database(tmp_path: Path) -> None:
    database = tmp_path / "intelligence.db"
    populated_database(database)
    result = IntelligenceService(database=database, release_url="not-a-url").get_surging_repos()
    assert result["repositories"][0]["full_name"] == "org/rocket"
    assert "staleness_warning" in result["metadata"]


def test_environment_ttl_is_honored_by_all_query_tools(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MCP_SYNC_TTL_SECONDS", "5")
    endpoint = ReleaseEndpoint(populated_database(tmp_path / "release.db"))
    now = [10000.0]
    service = IntelligenceService(database=tmp_path / "intelligence.db",
                                  release_url="https://example.invalid/db", opener=endpoint,
                                  clock=lambda: now[0])
    service.get_curated_highlights()
    now[0] += 4
    service.get_repo_trajectory(1)
    assert len(endpoint.requests) == 1
    now[0] += 1
    service.search_repositories()
    assert len(endpoint.requests) == 2
