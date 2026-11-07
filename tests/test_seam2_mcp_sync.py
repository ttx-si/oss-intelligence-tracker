"""Seam 2: public intelligence tools against a simulated Release boundary."""

from __future__ import annotations

import io
import asyncio
import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import sysconfig
import threading
import time
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


def test_fresh_cache_queries_continue_while_forced_sync_downloads(tmp_path: Path) -> None:
    content = populated_database(tmp_path / "release.db")
    entered = threading.Event()
    resume = threading.Event()
    requests = []

    def endpoint(request, *, timeout):
        requests.append(request)
        if len(requests) == 2:
            entered.set()
            assert resume.wait(5)
        return ReleaseResponse(content)

    service = IntelligenceService(database=tmp_path / "intelligence.db",
                                  release_url="https://example.invalid/db", opener=endpoint,
                                  clock=lambda: 10000.0)
    service.get_surging_repos()
    with ThreadPoolExecutor(max_workers=2) as executor:
        sync = executor.submit(service.sync_database_now)
        assert entered.wait(5)
        try:
            cached = executor.submit(service.get_surging_repos)
            assert cached.result(timeout=1)["repositories"][0]["full_name"] == "org/rocket"
        finally:
            resume.set()
        assert sync.result(timeout=5)["status"] == "updated"


def test_search_intersects_language_state_curation_keyword_and_velocity(tmp_path: Path) -> None:
    release = tmp_path / "release.db"
    populated_database(release)
    store = IntelligenceStore(release)
    original = store.get_repository_by_ghr_id(1)
    for mismatch in (
        replace(original, ghr_id=2, full_name="org/other-language", primary_language="Go"),
        replace(original, ghr_id=3, full_name="org/other-state", lifecycle_state="SUSTAINED"),
        replace(original, ghr_id=4, full_name="org/uncurated", is_curated=False),
        replace(original, ghr_id=5, full_name="org/no-keyword", description="Fast tooling", topics=("cli",)),
        replace(original, ghr_id=6, full_name="org/slow", stars_delta_24h=49),
    ):
        store.save_repository(mismatch)
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


def test_real_fastmcp_queries_fresh_cache_while_forced_sync_is_downloading(tmp_path: Path) -> None:
    fastmcp = pytest.importorskip("fastmcp")
    from oss_intelligence.mcp_server import create_mcp_server

    content = populated_database(tmp_path / "release.db")
    entered = threading.Event()
    resume = threading.Event()
    download_finished = threading.Event()
    requests = []

    def endpoint(request, *, timeout):
        requests.append(request)
        if len(requests) == 2:
            entered.set()
            assert resume.wait(5)
            download_finished.set()
        return ReleaseResponse(content)

    service = IntelligenceService(database=tmp_path / "intelligence.db",
                                  release_url="https://example.invalid/db", opener=endpoint,
                                  clock=lambda: 10000.0)

    async def exercise():
        async with fastmcp.Client(create_mcp_server(service)) as client:
            await client.call_tool("get_surging_repos", {})
            forced = asyncio.create_task(client.call_tool("sync_database_now", {}))
            try:
                assert await asyncio.to_thread(entered.wait, 5)
                surging, searched, curated, trajectory = await asyncio.wait_for(
                    asyncio.gather(
                        client.call_tool("get_surging_repos", {}),
                        client.call_tool("search_repositories", {"language": "Rust"}),
                        client.call_tool("get_curated_highlights", {}),
                        client.call_tool("get_repo_trajectory", {"ghr_id": 1}),
                    ),
                    2,
                )
                assert surging.data["repositories"][0]["full_name"] == "org/rocket"
                assert searched.data["repositories"][0]["full_name"] == "org/rocket"
                assert curated.data["repositories"][0]["full_name"] == "org/rocket"
                assert trajectory.data["repository"]["full_name"] == "org/rocket"
                assert not download_finished.is_set(), "fresh query waited for the forced download"
            finally:
                resume.set()
                assert (await forced).data["status"] == "updated"

    asyncio.run(exercise())


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


def test_sql_fallback_is_not_revalidated_as_the_previous_release_database(tmp_path: Path) -> None:
    old_db = tmp_path / "old.db"
    populated_database(old_db)
    dump = IntelligenceStore(old_db).export_sql_dump(tmp_path / "schema_data.sql")
    release = tmp_path / "release.db"
    release.write_bytes(old_db.read_bytes())
    with sqlite3.connect(release) as conn:
        conn.execute("UPDATE repositories SET stars_total = 2222 WHERE ghr_id = 1")
    requests = []
    online = [True]

    def endpoint(request, *, timeout):
        requests.append(request)
        if not online[0]:
            raise TimeoutError("offline")
        if request.get_header("If-none-match"):
            raise urllib.error.HTTPError(request.full_url, 304, "Not Modified", {}, None)
        return ReleaseResponse(release.read_bytes(), etag='"latest"')

    database = tmp_path / "intelligence.db"
    service = IntelligenceService(database=database, dump_path=dump,
                                  release_url="https://example.invalid/db", opener=endpoint)
    assert service.get_surging_repos()["repositories"][0]["stars_total"] == 2222
    database.unlink()
    online[0] = False
    assert service.get_surging_repos()["repositories"][0]["stars_total"] == 1000
    online[0] = True

    result = service.get_surging_repos()

    assert requests[-1].get_header("If-none-match") is None
    assert result["repositories"][0]["stars_total"] == 2222


def test_replaced_database_with_unchanged_size_and_mtime_is_not_treated_as_fresh(tmp_path: Path) -> None:
    release = tmp_path / "release.db"
    old_content = populated_database(release)
    with sqlite3.connect(release) as conn:
        conn.execute("UPDATE repositories SET stars_total = 2222 WHERE ghr_id = 1")
    endpoint = ReleaseEndpoint(release.read_bytes())
    database = tmp_path / "intelligence.db"
    service = IntelligenceService(database=database, release_url="https://example.invalid/db",
                                  opener=endpoint, clock=lambda: 10000.0)
    assert service.get_surging_repos()["repositories"][0]["stars_total"] == 2222
    stat = database.stat()
    assert len(old_content) == stat.st_size
    database.write_bytes(old_content)
    os.utime(database, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    result = service.get_surging_repos()

    assert result["repositories"][0]["stars_total"] == 2222
    assert len(endpoint.requests) == 2
    assert endpoint.requests[-1][0].get_header("If-none-match") is None


def test_database_changed_during_304_is_not_marked_as_the_release(tmp_path: Path) -> None:
    release = tmp_path / "release.db"
    fallback = populated_database(release)
    with sqlite3.connect(release) as conn:
        conn.execute("UPDATE repositories SET stars_total = 2222 WHERE ghr_id = 1")
    latest = release.read_bytes()
    database = tmp_path / "intelligence.db"
    requests = []

    def endpoint(request, *, timeout):
        requests.append(request)
        if len(requests) == 2:
            database.write_bytes(fallback)
            raise urllib.error.HTTPError(request.full_url, 304, "Not Modified", {}, None)
        return ReleaseResponse(latest, etag='"latest"')

    now = [10000.0]
    service = IntelligenceService(database=database, release_url="https://example.invalid/db",
                                  opener=endpoint, clock=lambda: now[0])
    assert service.get_surging_repos()["repositories"][0]["stars_total"] == 2222
    now[0] += 3600

    interim = service.get_surging_repos()
    assert "staleness_warning" in interim["metadata"]
    assert service.get_surging_repos()["repositories"][0]["stars_total"] == 2222
    assert requests[-1].get_header("If-none-match") is None


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


def test_installed_mcp_command_serves_a_real_stdio_session_offline_from_sql(tmp_path: Path) -> None:
    fastmcp = pytest.importorskip("fastmcp")
    from fastmcp.client.transports import StdioTransport

    build_path = os.environ.get("MCP_TEST_BUILD_PATH")
    if not build_path and importlib.util.find_spec("setuptools") is None:
        pytest.skip("setuptools is required for an offline package installation")
    root = Path(__file__).resolve().parents[1]
    project = tmp_path / "project"
    project.mkdir()
    for filename in ("pyproject.toml", "GLOSSARY.md"):
        shutil.copy(root / filename, project / filename)
    shutil.copytree(root / "src", project / "src", ignore=shutil.ignore_patterns("__pycache__"))
    prefix = tmp_path / "installed-prefix"
    layout = sysconfig.get_paths(vars={"base": str(prefix), "platbase": str(prefix)})
    site_packages = Path(layout["purelib"])
    scripts_dir = Path(layout["scripts"])
    env = os.environ.copy()
    env["PIP_CONFIG_FILE"] = os.devnull
    env["PIP_NO_INDEX"] = "1"
    # Drop src from PYTHONPATH so pip and the installed scripts resolve the built wheel, not the source tree.
    clean_paths = [
        entry for entry in env.get("PYTHONPATH", "").split(os.pathsep)
        if entry and Path(entry).resolve() != (root / "src").resolve()
    ]
    env["PYTHONPATH"] = os.pathsep.join(clean_paths)
    install_env = env.copy()
    if build_path:
        install_env["PYTHONPATH"] = install_env["PYTHONPATH"] + os.pathsep + build_path
    install = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-index", "--no-build-isolation",
         "--disable-pip-version-check", "--prefix", str(prefix), str(project)],
        env=install_env, cwd=tmp_path, capture_output=True, text=True, timeout=60,
    )
    assert install.returncode == 0, install.stdout + install.stderr
    broken = tmp_path / "broken-project"
    shutil.copytree(project, broken)
    pyproject = broken / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8").replace(
            '"fastmcp>=2.14,<3"', '"fastmcp>=2.14,<3", "missing-offline-dependency>=1.0"'
        ),
        encoding="utf-8",
    )
    broken_install = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-index", "--no-build-isolation",
         "--disable-pip-version-check", "--prefix", str(tmp_path / "broken-prefix"), str(broken)],
        env=install_env, cwd=tmp_path, capture_output=True, text=True, timeout=60,
    )
    assert broken_install.returncode != 0, "installer ignored an unsatisfied declared dependency"

    env["PYTHONPATH"] = str(site_packages) + os.pathsep + env["PYTHONPATH"]
    pip_check = subprocess.run(
        [sys.executable, "-m", "pip", "check", "--disable-pip-version-check"],
        env=env, cwd=tmp_path, capture_output=True, text=True, timeout=30,
    )
    assert pip_check.returncode == 0, pip_check.stdout + pip_check.stderr
    cli_command = scripts_dir / ("oss-intelligence.exe" if os.name == "nt" else "oss-intelligence")
    cli_help = subprocess.run([str(cli_command), "--help"], env=env, cwd=tmp_path,
                              capture_output=True, text=True, timeout=15)
    assert cli_help.returncode == 0, cli_help.stderr
    assert "--dry-run" in cli_help.stdout
    command = scripts_dir / ("oss-intelligence-mcp.exe" if os.name == "nt" else "oss-intelligence-mcp")
    help_result = subprocess.run([str(command), "--help"], env=env, cwd=tmp_path,
                                 capture_output=True, text=True, timeout=15)
    assert help_result.returncode == 0, help_result.stderr
    assert "--release-url" in help_result.stdout and "--database" in help_result.stdout

    source = tmp_path / "release.db"
    populated_database(source)
    dump = IntelligenceStore(source).export_sql_dump(tmp_path / "schema_data.sql")
    outer_clone = tmp_path / "outer-clone"
    workspace = outer_clone / "workspace"
    workspace.mkdir(parents=True)
    git_setup_env = {**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull}
    subprocess.run(["git", "init"], cwd=outer_clone, env=git_setup_env, check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "remote.origin.url", "https://github.com/org/demo.git"],
        cwd=outer_clone, env=git_setup_env, check=True, capture_output=True,
    )
    proxy_requests: list[str] = []

    class ProxySpy(BaseHTTPRequestHandler):
        def _reject(self) -> None:
            proxy_requests.append(f"{self.command} {self.path}")
            self.send_response(502)
            self.end_headers()

        do_CONNECT = _reject
        do_GET = _reject
        do_HEAD = _reject

        def log_message(self, *args):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), ProxySpy)
    proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    proxy_thread.start()
    proxy_url = f"http://127.0.0.1:{proxy.server_port}"
    for key in (
        "GITHUB_TOKEN", "GITHUB_REPOSITORY", "GITHUB_RELEASE_DB_URL",
        "GIT_WORK_TREE", "GIT_CONFIG", "GIT_CONFIG_COUNT", "GIT_CONFIG_PARAMETERS",
        "NO_PROXY", "no_proxy",
    ):
        env.pop(key, None)
    env.update({
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CEILING_DIRECTORIES": str(outer_clone),
        "GIT_DIR": str(workspace / "isolated.git"),
        "FASTMCP_HOME": str(tmp_path / "fastmcp-home"),
        "HTTP_PROXY": proxy_url,
        "HTTPS_PROXY": proxy_url,
        "http_proxy": proxy_url,
        "https_proxy": proxy_url,
    })
    transport = StdioTransport(command=str(command),
                               args=["--database", str(workspace / "intelligence.db"), "--dump", str(dump)],
                               env=env, cwd=str(workspace), keep_alive=False)

    async def exercise():
        async with fastmcp.Client(transport, timeout=15) as client:
            result = await client.call_tool("get_curated_highlights", {})
            assert result.data["repositories"][0]["full_name"] == "org/rocket"
            assert "staleness_warning" in result.data["metadata"]

    try:
        asyncio.run(exercise())
    finally:
        proxy.shutdown()
        proxy.server_close()
        proxy_thread.join(timeout=5)
    assert proxy_requests == []


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


def test_multi_board_attribution_categorization_filtering_and_metadata_breakdowns(
    tmp_path: Path,
) -> None:
    from oss_intelligence.mcp_server import create_mcp_server

    release_db = tmp_path / "release.db"
    store = IntelligenceStore(release_db)

    store.save_repository(
        RepositoryRecord(
            ghr_id=101,
            full_name="browser-use/browser-use",
            description="Make websites accessible for AI agents and LLM workflows",
            primary_language="Python",
            license_spdx="MIT",
            topics=("ai", "agents", "llm", "mcp"),
            repo_created_at="2026-10-25T00:00:00Z",  # 10 days before last_updated_at -> is_new_breakout=True
            stars_total=4500,
            forks_total=320,
            stars_delta_6h=120,
            stars_delta_24h=520,
            stars_delta_7d=2800,
            lifecycle_state="SURGING",
            is_curated=True,
            curated_issue="vol-127",
            curated_category="人工智能",
            first_seen_at="2026-11-01T00:00:00Z",
            last_seen_in_trending_at="2026-11-04T00:00:00Z",
            last_updated_at="2026-11-04T00:00:00Z",
        )
    )
    store.add_snapshot(
        ghr_id=101,
        captured_at="2026-11-04T00:00:00Z",
        signal_source="gtrending",
        stars=4500,
        forks=320,
        source_rank=1,
        period_stars_reported=520,
    )
    store.add_snapshot(
        ghr_id=101,
        captured_at="2026-11-04T00:00:00Z",
        signal_source="gh_search",
        stars=4500,
        forks=320,
        source_rank=1,
        period_stars_reported=450,
    )
    store.add_snapshot(
        ghr_id=101,
        captured_at="2026-11-04T00:00:00Z",
        signal_source="hellogithub",
        stars=4500,
        forks=320,
        source_rank=2,
        period_stars_reported=0,
    )

    store.save_repository(
        RepositoryRecord(
            ghr_id=102,
            full_name="duckdb/duckdb",
            description="DuckDB is an in-process SQL OLAP Database Management System",
            primary_language="C++",
            license_spdx="MIT",
            topics=("database", "sql", "olap"),
            repo_created_at="2018-06-25T00:00:00Z",  # Old repo -> is_new_breakout=False
            stars_total=24500,
            forks_total=1950,
            stars_delta_6h=20,
            stars_delta_24h=85,
            stars_delta_7d=480,
            lifecycle_state="SUSTAINED",
            is_curated=False,
            curated_issue=None,
            curated_category=None,
            first_seen_at="2026-10-20T00:00:00Z",
            last_seen_in_trending_at="2026-11-04T00:00:00Z",
            last_updated_at="2026-11-04T00:00:00Z",
        )
    )
    store.add_snapshot(
        ghr_id=102,
        captured_at="2026-11-04T00:00:00Z",
        signal_source="gtrending",
        stars=24500,
        forks=1950,
        source_rank=4,
        period_stars_reported=85,
    )

    endpoint = ReleaseEndpoint(release_db.read_bytes())
    service = IntelligenceService(
        database=tmp_path / "intelligence.db",
        release_url="https://example.invalid/intelligence.db",
        opener=endpoint,
        clock=lambda: 10000.0,
    )

    all_res = service.search_repositories()
    assert all_res["metadata"]["total_returned"] == 2
    assert all_res["metadata"]["sources_breakdown"] == {
        "gtrending": 2,
        "gh_search": 1,
        "hellogithub": 1,
    }
    assert all_res["metadata"]["categories_breakdown"] == {
        "AI / Agents & LLM": 1,
        "Data, Storage & Database": 1,
    }

    ai_repo = all_res["repositories"][0]
    assert ai_repo["full_name"] == "browser-use/browser-use"
    assert ai_repo["url"] == "https://github.com/browser-use/browser-use"
    assert ai_repo["topics"] == ["ai", "agents", "llm", "mcp"]
    assert ai_repo["signal_sources"] == ["gh_search", "gtrending", "hellogithub"]
    assert ai_repo["category"] == "AI / Agents & LLM"
    assert ai_repo["is_new_breakout"] is True

    db_repo = all_res["repositories"][1]
    assert db_repo["full_name"] == "duckdb/duckdb"
    assert db_repo["url"] == "https://github.com/duckdb/duckdb"
    assert db_repo["topics"] == ["database", "sql", "olap"]
    assert db_repo["signal_sources"] == ["gtrending"]
    assert db_repo["category"] == "Data, Storage & Database"
    assert db_repo["is_new_breakout"] is False

    # Filter by source
    gh_search_only = service.search_repositories(source="gh_search")
    assert [r["full_name"] for r in gh_search_only["repositories"]] == ["browser-use/browser-use"]
    assert gh_search_only["metadata"]["total_returned"] == 1

    # Filter by category
    data_only = service.search_repositories(category="database")
    assert [r["full_name"] for r in data_only["repositories"]] == ["duckdb/duckdb"]

    # Trajectory enrichment
    traj = service.get_repo_trajectory(101)
    assert traj["repository"]["url"] == "https://github.com/browser-use/browser-use"
    assert traj["repository"]["signal_sources"] == ["gh_search", "gtrending", "hellogithub"]
    assert traj["repository"]["category"] == "AI / Agents & LLM"
    assert traj["repository"]["is_new_breakout"] is True
    assert len(traj["snapshots"]) == 3

    # Verify FastMCP tool wrapper accepts source and category filters
    fastmcp = pytest.importorskip("fastmcp")
    server = create_mcp_server(service)

    async def exercise_mcp():
        async with fastmcp.Client(server) as client:
            res = await client.call_tool(
                "search_repositories",
                {"source": "hellogithub", "category": "AI"},
            )
            assert [r["full_name"] for r in res.data["repositories"]] == ["browser-use/browser-use"]

    asyncio.run(exercise_mcp())


def test_weighted_domain_categorization_and_expanded_query_limits(
    tmp_path: Path,
) -> None:
    release_db = tmp_path / "release.db"
    store = IntelligenceStore(release_db)

    benchmark_cases = [
        (
            201,
            "Gathin23/cleanyourtv",
            "Debloat any Android or Google TV over Wi-Fi and get an Apple TV-style home.",
            "Python",
            ("adb", "android-tv", "claude-code", "google-tv", "launcher", "privacy", "smart-tv", "vibe-coding"),
            None,
            "Mobile & Desktop Apps",
        ),
        (
            202,
            "curl/curl",
            "A command line tool and library for transferring data with URL syntax, supporting HTTP, HTTPS, FTP.",
            "C",
            ("c", "client", "curl", "http", "https", "libcurl", "user-agent"),
            None,
            "DevTools & Coding",
        ),
        (
            203,
            "ClickHouse/ClickHouse",
            "ClickHouse is a real-time analytics database management system",
            "C++",
            ("ai", "analytics", "big-data", "clickhouse", "database", "dbms", "olap", "sql"),
            None,
            "Data, Storage & Database",
        ),
        (
            204,
            "pingcap/tidb",
            "TiDB is built for agentic workloads with ACID guarantees and native support for transactions, analytics, and vector search.",
            "Go",
            ("agent", "agentic", "ai", "database", "distributed-database", "htap", "mysql", "sql", "tidb"),
            None,
            "Data, Storage & Database",
        ),
        (
            205,
            "microsoft/vscode",
            "Visual Studio Code",
            "TypeScript",
            ("editor", "electron", "microsoft", "typescript", "visual-studio-code"),
            None,
            "DevTools & Coding",
        ),
        (
            206,
            "l0ng-ai/openidea",
            "The IDE you know, rebuilt in Rust: editor, code insight, debugger, Git, terminal, database and HTTP clients.",
            "TypeScript",
            ("code-editor", "database-client", "debugger", "desktop-app", "git", "ide", "lsp", "tauri", "terminal"),
            None,
            "DevTools & Coding",
        ),
        (
            207,
            "cryptomator/cryptomator",
            "Cryptomator for Windows, macOS, and Linux: Secure client-side encryption for your cloud storage.",
            "Java",
            ("cloud-storage", "crypto", "cryptography", "cryptomator", "encryption", "privacy", "security"),
            None,
            "Security & Privacy",
        ),
        (
            208,
            "agavra/tuicr",
            "方便在终端里做代码审查的工具。支持 Vim 快捷键的终端代码审查工具。",
            "Rust",
            ("ai-tools", "code-review", "rust", "tui"),
            "Rust 项目",
            "DevTools & Coding",
        ),
        (
            209,
            "rails/rails",
            "Ruby on Rails",
            "Ruby",
            ("activejob", "activerecord", "framework", "html", "mvc", "rails", "ruby"),
            None,
            "Web & Frontend",
        ),
        (
            210,
            "tokio-rs/tokio",
            "A runtime for writing reliable asynchronous applications with Rust. Provides I/O, networking, scheduling.",
            "Rust",
            ("asynchronous", "networking", "rust"),
            None,
            "Backend, Cloud & Infra",
        ),
        (
            211,
            "org/mystery-untagged",
            None,
            "Rust",
            (),
            "其它",
            "Rust Ecosystem",
        ),
    ]

    for ghr_id, full_name, desc, lang, topics, curated_cat, _expected in benchmark_cases:
        store.save_repository(
            RepositoryRecord(
                ghr_id=ghr_id,
                full_name=full_name,
                description=desc,
                primary_language=lang,
                license_spdx="MIT",
                topics=topics,
                repo_created_at="2026-10-25T00:00:00Z",
                stars_total=5000,
                forks_total=300,
                stars_delta_6h=50,
                stars_delta_24h=250,
                stars_delta_7d=1000,
                lifecycle_state="SURGING",
                is_curated=True,
                curated_issue="vol-127",
                curated_category=curated_cat,
                first_seen_at="2026-11-01T00:00:00Z",
                last_seen_in_trending_at="2026-11-04T00:00:00Z",
                last_updated_at="2026-11-04T00:00:00Z",
            )
        )

    # Also add 550 additional SURGING & curated repositories to verify >500 query capacity
    for idx in range(300, 850):
        store.save_repository(
            RepositoryRecord(
                ghr_id=idx,
                full_name=f"scale/repo-{idx}",
                description="Fast CLI developer tool",
                primary_language="Go",
                license_spdx="MIT",
                topics=("cli",),
                repo_created_at="2026-10-25T00:00:00Z",
                stars_total=1000,
                forks_total=50,
                stars_delta_6h=20,
                stars_delta_24h=180,
                stars_delta_7d=500,
                lifecycle_state="SURGING",
                is_curated=True,
                curated_issue="vol-127",
                curated_category="Go 项目",
                first_seen_at="2026-11-01T00:00:00Z",
                last_seen_in_trending_at="2026-11-04T00:00:00Z",
                last_updated_at="2026-11-04T00:00:00Z",
            )
        )

    service = IntelligenceService(
        database=tmp_path / "intelligence.db",
        release_url="https://example.invalid/intelligence.db",
        opener=ReleaseEndpoint(release_db.read_bytes()),
        clock=lambda: 10000.0,
    )

    all_res = service.search_repositories(limit=1000)
    total_expected = len(benchmark_cases) + 550
    assert all_res["metadata"]["total_returned"] == total_expected

    by_name = {r["full_name"]: r for r in all_res["repositories"]}
    for _ghr_id, full_name, _desc, _lang, _topics, _curated_cat, expected_cat in benchmark_cases:
        assert by_name[full_name]["category"] == expected_cat, (
            f"{full_name}: got {by_name[full_name]['category']!r}, expected {expected_cat!r}"
        )

    # Default limit of 500 for get_surging_repos and get_curated_highlights (and custom limit support)
    assert service.get_surging_repos()["metadata"]["total_returned"] == 500
    assert service.get_surging_repos(limit=1000)["metadata"]["total_returned"] == total_expected
    assert service.get_curated_highlights()["metadata"]["total_returned"] == 500
    assert service.get_curated_highlights(limit=1000)["metadata"]["total_returned"] == total_expected


