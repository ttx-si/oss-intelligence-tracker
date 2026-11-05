"""Exercise the scheduled persistence boundary with simulated external services."""

from pathlib import Path
import json
import os
import shutil
import sqlite3
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ("high-frequency.yml", "mid-frequency.yml")


def shell_path(path: Path) -> str:
    value = str(path.resolve()).replace("\\", "/")
    return "/" + value[0].lower() + value[2:] if len(value) > 1 and value[1] == ":" else value


class WorkflowRun:
    """Run real workflow commands with fake Git, GitHub and Signal Source boundaries."""

    def __init__(self, tmp_path: Path, filename: str, *, branch: str = "main", **settings):
        yaml = pytest.importorskip("yaml")
        git = Path(shutil.which("git"))
        self.shell = git.parent.parent / "usr/bin/sh.exe" if os.name == "nt" else Path(shutil.which("sh"))
        if not self.shell.exists():
            pytest.skip("POSIX shell unavailable")
        self.directory = tmp_path
        self.commands = tmp_path / "commands.log"
        self.notifications = tmp_path / "notifications.log"
        shutil.copytree(ROOT / "src", tmp_path / "src")
        shutil.copytree(ROOT / "data", tmp_path / "data")
        if (ROOT / ".github/scripts").exists():
            shutil.copytree(ROOT / ".github/scripts", tmp_path / ".github/scripts")
        bin_path = tmp_path / "bin"
        bin_path.mkdir()
        boundary = tmp_path / "boundary"
        boundary.mkdir()
        installed = tmp_path / "installed"
        installed.mkdir()
        (boundary / "gtrending.py").write_text(
            "def fetch_repos():\n"
            "    return [{'fullname': 'org/demo', 'stars': 500, 'currentPeriodStars': 200}]\n",
            encoding="utf-8",
        )
        (boundary / "sitecustomize.py").write_text(
            """import io, json, urllib.request
def urlopen(request, *args, **kwargs):
    url = request.full_url
    if '/search/repositories?' in url:
        content = json.dumps({'items': []})
    elif 'api.ossinsight.io/' in url:
        content = json.dumps({'data': {'rows': []}})
    elif 'raw.githubusercontent.com/' in url:
        content = '## Tools\\n[Demo](https://github.com/org/demo)\\n'
    elif url.endswith('/repos/org/demo'):
        content = json.dumps({'id': 9, 'full_name': 'org/demo', 'stargazers_count': 500})
    else:
        raise AssertionError('Unexpected outbound request: ' + url)
    return io.BytesIO(content.encode())
urllib.request.urlopen = urlopen
""",
            encoding="utf-8",
        )
        (boundary / "apprise.py").write_text(
            """import os
class Apprise:
    def add(self, url): return True
    def notify(self, **kwargs):
        with open(os.environ['NOTIFICATIONS'], 'a', encoding='utf-8') as out:
            out.write('delivered\\n')
        return True
""",
            encoding="utf-8",
        )
        install_stub = tmp_path / "install.py"
        install_stub.write_text(
            """import os, shutil
from pathlib import Path
with open(os.environ['COMMANDS'], 'a', encoding='utf-8') as out:
    out.write('python -m pip install .\\n')
shutil.copytree('src/oss_intelligence', Path(os.environ['INSTALLED']) / 'oss_intelligence',
                dirs_exist_ok=True)
""",
            encoding="utf-8",
        )
        git_stub = tmp_path / "git_stub.py"
        git_stub.write_text(
            """import os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ['COMMANDS'], 'a', encoding='utf-8') as out:
    out.write('git ' + ' '.join(args) + '\\n')
if args[:2] == ['pull', '--rebase'] and os.environ.get('UPDATE_CODE') == '1':
    path = Path('src/oss_intelligence/__main__.py')
    text = path.read_text(encoding='utf-8')
    path.write_text(text.replace('raise SystemExit', 'print("INSTALLED_CODE_AFTER_PULL")\\nraise SystemExit'),
                    encoding='utf-8')
if args[:2] == ['rev-parse', 'HEAD']:
    print('base')
if args[:3] == ['diff', '--cached', '--quiet']:
    sys.exit(1)
if args[:2] == ['diff', '--quiet'] and os.environ.get('REMOTE_SQL_CHANGED') == '1':
    sys.exit(1)
if args and args[0] == 'push' and os.environ.get('REJECT_ALL_PUSH') == '1':
    sys.exit(1)
if args and args[0] == 'push' and os.environ.get('REJECT_FIRST_PUSH') == '1':
    marker = Path(os.environ['PUSH_MARKER'])
    if not marker.exists():
        marker.touch()
        sys.exit(1)
""",
            encoding="utf-8",
        )
        for name, content in {
            "git": '#!/bin/sh\nexec "$REAL_PYTHON" "$GIT_STUB" "$@"\n',
            "gh": '#!/bin/sh\nprintf "gh %s\\n" "$*" >> "$COMMANDS"\n'
                  '[ "$1 $2" = "release view" ] && exit 1\nexit 0\n',
            "python": '#!/bin/sh\nif [ "$1 $2 $3" = "-m pip install" ]; then\n'
                      '  exec "$REAL_PYTHON" "$INSTALL_STUB"\nfi\n'
                      'exec "$REAL_PYTHON" "$@"\n',
        }.items():
            path = bin_path / name
            path.write_text(content, encoding="utf-8")
            path.chmod(0o755)
        self.env = os.environ.copy()
        for name in ("GITHUB_TOKEN", "GITHUB_RELEASE_DB_URL", "TRENDSHIFT_API_KEY"):
            self.env.pop(name, None)
        self.env.update({
            "PATH": str(bin_path) + os.pathsep + str(self.shell.parent),
            "PYTHONPATH": str(boundary) + os.pathsep + str(installed),
            "REAL_PYTHON": shell_path(Path(sys.executable)), "GIT_STUB": shell_path(git_stub),
            "INSTALL_STUB": shell_path(install_stub), "INSTALLED": str(installed),
            "COMMANDS": str(self.commands), "NOTIFICATIONS": str(self.notifications),
            "PUSH_MARKER": str(tmp_path / "push-rejected"), "GITHUB_REF_NAME": branch,
            "RUNNER_TEMP": str(tmp_path),
            "GITHUB_REF": f"refs/heads/{branch}", "DEFAULT_BRANCH": "main",
            "APPRISE_URLS": "simulated://notification", "GH_TOKEN": "simulated",
            **{key: str(value) for key, value in settings.items()},
        })
        self.workflow = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text(encoding="utf-8"))

    def run(self):
        output = []
        for step in self.workflow["jobs"]["ingest"]["steps"]:
            if "run" not in step:
                continue
            result = subprocess.run([str(self.shell), "-eu", "-c", step["run"]], cwd=self.directory,
                                    env=self.env, capture_output=True, text=True, timeout=30)
            output.append(result.stdout + result.stderr)
            if result.returncode:
                return result.returncode, "".join(output)
        return 0, "".join(output)

    def command_log(self):
        return self.commands.read_text(encoding="utf-8") if self.commands.exists() else ""


def test_scheduled_workflows_declare_permissions_concurrency_and_schedules():
    yaml = pytest.importorskip("yaml")
    for filename, schedule in zip(WORKFLOWS, ("0 */6 * * *", "0 0 * * *")):
        workflow = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text(encoding="utf-8"))
        events = workflow.get("on", workflow.get(True))
        assert {item["cron"] for item in events["schedule"]} == {schedule}
        assert "workflow_dispatch" in events
        assert workflow["permissions"]["contents"] == "write"
        assert workflow["concurrency"] == {"group": "db-pipeline", "cancel-in-progress": False}


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_manual_nondefault_branch_cannot_publish_the_shared_release(tmp_path: Path, filename: str):
    run = WorkflowRun(tmp_path, filename, branch="feature")
    status, output = run.run()

    assert status != 0
    assert "default branch" in output
    assert "git push" not in run.command_log()
    assert "gh release" not in run.command_log()
    assert not run.notifications.exists()


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_ingestion_uses_the_checkout_after_the_initial_pull(tmp_path: Path, filename: str):
    run = WorkflowRun(tmp_path, filename, UPDATE_CODE=1)
    status, output = run.run()

    assert status == 0, output
    assert "INSTALLED_CODE_AFTER_PULL" in output
    assert run.command_log().index("git pull --rebase") < run.command_log().index("python -m pip install .")


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_intervening_non_sql_push_retries_without_repeating_delivery(tmp_path: Path, filename: str):
    run = WorkflowRun(tmp_path, filename, REJECT_FIRST_PUSH=1)
    status, output = run.run()

    assert status == 0, output
    log = run.command_log()
    assert log.count("git push origin HEAD:main") == 2
    assert "gh release upload latest-db intelligence.db --clobber" in log
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    with sqlite3.connect(tmp_path / "intelligence.db") as db:
        watermark = db.execute(
            "SELECT last_curated_notified_at FROM repositories WHERE full_name = 'org/demo'"
        ).fetchone()[0]
    assert watermark is not None
    assert watermark in (tmp_path / "data/schema_data.sql").read_text(encoding="utf-8")


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_remote_sql_changes_stop_publication_and_preserve_delivery_watermarks(tmp_path: Path, filename: str):
    run = WorkflowRun(tmp_path, filename, REMOTE_SQL_CHANGED=1)
    status, output = run.run()

    assert status != 0
    assert "SQL" in output
    assert "git push" not in run.command_log()
    assert "gh release" not in run.command_log()
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    with sqlite3.connect(tmp_path / "intelligence.db") as db:
        watermark = db.execute(
            "SELECT last_curated_notified_at FROM repositories WHERE full_name = 'org/demo'"
        ).fetchone()[0]
    assert watermark is not None
    assert watermark in (tmp_path / "data/schema_data.sql").read_text(encoding="utf-8")
    assert (tmp_path / "intelligence-base-revision.txt").read_text(encoding="utf-8").strip() == "base"
    recovery_steps = [step for step in run.workflow["jobs"]["ingest"]["steps"]
                      if step.get("uses", "").startswith("actions/upload-artifact@")]
    assert len(recovery_steps) == 1
    assert recovery_steps[0]["if"] == "failure()"
    assert "intelligence.db" in recovery_steps[0]["with"]["path"]
    assert "data/schema_data.sql" in recovery_steps[0]["with"]["path"]
    assert "intelligence-base-revision.txt" in recovery_steps[0]["with"]["path"]


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_exhausted_push_retries_do_not_publish_or_repeat_delivery(tmp_path: Path, filename: str):
    run = WorkflowRun(tmp_path, filename, REJECT_ALL_PUSH=1)
    status, output = run.run()

    assert status != 0
    assert "preserve this run" in output
    assert run.command_log().count("git push origin HEAD:main") == 3
    assert "gh release" not in run.command_log()
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
