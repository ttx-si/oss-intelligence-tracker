"""The two published workflows form the scheduled persistence boundary."""

from pathlib import Path
import os
import shutil
import subprocess
import sys

import pytest


def test_scheduled_workflows_serialize_export_commit_and_release_publication() -> None:
    yaml = pytest.importorskip("yaml")
    root = Path(__file__).resolve().parents[1]
    workflows = [
        (root / ".github/workflows/high-frequency.yml", "0 */6 * * *"),
        (root / ".github/workflows/mid-frequency.yml", "0 0 * * *"),
    ]
    for path, schedule in workflows:
        workflow = yaml.safe_load(path.read_text(encoding="utf-8"))
        events = workflow.get("on", workflow.get(True))
        assert {item["cron"] for item in events["schedule"]} == {schedule}
        assert workflow["permissions"]["contents"] == "write"
        assert workflow["concurrency"] == {"group": "db-pipeline", "cancel-in-progress": False}
        steps = workflow["jobs"]["ingest"]["steps"]
        scripts = "\n".join(step.get("run", "") for step in steps)
        assert "git pull --rebase" in scripts
        assert "data/schema_data.sql" in scripts
        assert "git commit" in scripts and "git push" in scripts
        assert "gh release upload latest-db intelligence.db --clobber" in scripts
        assert scripts.index("git pull --rebase") < scripts.index("git commit")


@pytest.mark.parametrize("filename", ["high-frequency.yml", "mid-frequency.yml"])
def test_workflow_restores_exports_and_publishes_through_fake_boundaries(tmp_path: Path, filename: str) -> None:
    yaml = pytest.importorskip("yaml")
    root = Path(__file__).resolve().parents[1]
    if os.name == "nt":
        git = Path(shutil.which("git"))
        shell = git.parent.parent / "usr" / "bin" / "sh.exe"
    else:
        shell = Path(shutil.which("sh"))
    if not shell.exists():
        pytest.skip("POSIX shell unavailable")

    def shell_path(path: Path) -> str:
        value = str(path.resolve()).replace("\\", "/")
        if len(value) > 1 and value[1] == ":":
            value = "/" + value[0].lower() + value[2:]
        return value

    bin_path = tmp_path / "bin"
    bin_path.mkdir()
    (bin_path / "git").write_text(
        '#!/bin/sh\nprintf "git %s\\n" "$*" >> "$STUB_LOG"\n'
        '[ "$1" = diff ] && exit 1\nexit 0\n', encoding="utf-8")
    (bin_path / "gh").write_text(
        '#!/bin/sh\nprintf "gh %s\\n" "$*" >> "$STUB_LOG"\n'
        '[ "$1 $2" = "release view" ] && exit 1\nexit 0\n', encoding="utf-8")
    (bin_path / "python").write_text(
        '#!/bin/sh\nif [ "$1" = "-m" ]; then\n'
        '  exec "$REAL_PYTHON" "$FAKE_INGEST"\nelse\n  exec "$REAL_PYTHON" "$@"\nfi\n', encoding="utf-8")
    fake_ingest = tmp_path / "fake_ingest.py"
    fake_ingest.write_text(
        "import sqlite3\n"
        "conn = sqlite3.connect('intelligence.db')\n"
        "conn.execute('INSERT INTO repositories (ghr_id, full_name, first_seen_at, "
        "last_seen_in_trending_at, last_updated_at) VALUES (?, ?, ?, ?, ?)', "
        "(9, 'org/demo', '2026-01-01', '2026-01-01', '2026-01-01'))\n"
        "conn.commit()\nconn.close()\n", encoding="utf-8")
    for path in bin_path.iterdir():
        path.chmod(0o755)
    (tmp_path / "data").mkdir()
    shutil.copy(root / "data/schema_data.sql", tmp_path / "data/schema_data.sql")
    env = os.environ.copy()
    env.update({
        "PATH": str(bin_path) + os.pathsep + str(shell.parent),
        "PYTHONPATH": str(root / "src"),
        "REAL_PYTHON": shell_path(Path(sys.executable)),
        "FAKE_INGEST": shell_path(fake_ingest),
        "STUB_LOG": shell_path(tmp_path / "commands.log"),
        "GITHUB_REF_NAME": "main", "APPRISE_URLS": "", "GH_TOKEN": "simulated",
    })
    workflow = yaml.safe_load((root / ".github/workflows" / filename).read_text(encoding="utf-8"))
    for step in workflow["jobs"]["ingest"]["steps"]:
        if step.get("name") in {"Rebase before ingestion", "Restore, ingest, and export",
                                "Commit SQL and publish database"}:
            subprocess.run([str(shell), "-eu", "-c", step["run"]], cwd=tmp_path, env=env,
                           check=True, capture_output=True, text=True, timeout=20)
    assert "org/demo" in (tmp_path / "data/schema_data.sql").read_text(encoding="utf-8")
    assert (tmp_path / "intelligence.db").read_bytes().startswith(b"SQLite format 3\0")
    commands = (tmp_path / "commands.log").read_text(encoding="utf-8")
    assert commands.index("git pull --rebase") < commands.index("git commit")
    assert "git push origin HEAD:main" in commands
    assert "gh release create latest-db" in commands
    assert "gh release upload latest-db intelligence.db --clobber" in commands
