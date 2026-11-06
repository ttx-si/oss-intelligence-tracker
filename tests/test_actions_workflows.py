"""Exercise the scheduled persistence boundary against real local bare Git repositories."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ("high-frequency.yml", "mid-frequency.yml")
PENDING_REF = "refs/heads/intelligence-recovery-pending"


def shell_path(path: Path) -> str:
    value = str(path.resolve()).replace("\\", "/")
    return "/" + value[0].lower() + value[2:] if len(value) > 1 and value[1] == ":" else value


class WorkflowRun:
    """Execute real workflow YAML steps against a local bare Git remote and simulated network boundaries."""

    def __init__(self, tmp_path: Path, filename: str, *, branch: str = "main", **settings):
        yaml = pytest.importorskip("yaml")
        git_exe = shutil.which("git")
        if not git_exe:
            pytest.skip("git unavailable")
        self.real_git = str(Path(git_exe).resolve())
        self.shell = (
            Path(self.real_git).parent.parent / "usr/bin/sh.exe"
            if os.name == "nt"
            else Path(shutil.which("sh") or "/bin/sh")
        )
        if not self.shell.exists():
            pytest.skip("POSIX shell unavailable")

        self.root = tmp_path
        self.root.mkdir(parents=True, exist_ok=True)
        self.filename = filename
        self.directory = tmp_path / "runner"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.remote = tmp_path / "remote.git"
        self.actor = tmp_path / "actor"
        self.commands = tmp_path / "commands.log"
        self.notifications = tmp_path / "notifications.log"
        self.http_requests = tmp_path / "http_requests.jsonl"
        self.hooks_file = tmp_path / "git_hooks.json"
        self.hooks_file.write_text("[]", encoding="utf-8")

        shutil.copytree(ROOT / "src", self.directory / "src")
        shutil.copytree(ROOT / "data", self.directory / "data")
        shutil.copytree(ROOT / ".github/scripts", self.directory / ".github/scripts")

        self.git_env = os.environ.copy()
        self.git_env.update({
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ALLOW_PROTOCOL": "file",
        })

        self.git(tmp_path, "init", "--bare", "--initial-branch=main", str(self.remote))
        # Enforce fast-forward-only pushes on the bare server, matching protected default branches.
        self.git(self.remote, "config", "receive.denyNonFastForwards", "true")
        self.git(self.remote, "config", "receive.denyDeletes", "false")

        self.git(self.directory, "init", "--initial-branch=main")
        self.configure(self.directory)
        (self.directory / "README.state").write_text("initial\n", encoding="utf-8")
        self.git(self.directory, "add", "src", "data", ".github/scripts", "README.state")
        self.git(self.directory, "commit", "-m", "Initial state")
        self.initial = self.git(self.directory, "rev-parse", "HEAD").strip()

        (self.directory / "README.state").write_text("later baseline\n", encoding="utf-8")
        self.git(self.directory, "add", "README.state")
        self.git(self.directory, "commit", "-m", "Unrelated baseline change")
        self.baseline = self.git(self.directory, "rev-parse", "HEAD").strip()

        self.git(self.directory, "remote", "add", "origin", str(self.remote))
        self.git(self.directory, "push", "-u", "origin", "main")
        if branch != "main":
            self.git(self.directory, "checkout", "-b", branch)
            self.git(self.directory, "push", "-u", "origin", branch)

        self.git(tmp_path, "clone", "-b", "main", str(self.remote), str(self.actor))
        self.configure(self.actor)

        self._build_harness(tmp_path / "harness", branch=branch, settings=settings)
        self.expressions = {
            "${{ github.event.repository.default_branch }}": "main",
            "${{ github.token }}": "simulated",
            "${{ secrets.APPRISE_URLS }}": "simulated://notification",
            "${{ secrets.TRENDSHIFT_API_KEY }}": "",
        }
        self.workflow = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text(encoding="utf-8"))

    def _build_harness(self, harness_dir: Path, *, branch: str, settings: dict) -> None:
        harness_dir.mkdir(parents=True, exist_ok=True)
        bin_path = harness_dir / "bin"
        bin_path.mkdir()
        boundary = harness_dir / "boundary"
        boundary.mkdir()
        installed = harness_dir / "installed"
        installed.mkdir()

        (boundary / "gtrending.py").write_text(
            "import os\n"
            "def fetch_repos():\n"
            "    if os.environ.get('EMPTY_SIGNALS') == '1':\n"
            "        return []\n"
            "    return [{'fullname': 'org/demo', 'stars': 500, 'currentPeriodStars': 200}]\n",
            encoding="utf-8",
        )
        (boundary / "sitecustomize.py").write_text(
            """import io, json, os, urllib.request
def urlopen(request, *args, **kwargs):
    url = request.full_url
    with open(os.environ['HTTP_REQUESTS'], 'a', encoding='utf-8') as log:
        log.write(json.dumps({'url': url, 'authorization': request.get_header('Authorization')}) + '\\n')
    if '/search/repositories?' in url:
        content = json.dumps({'items': []})
    elif 'api.ossinsight.io/' in url:
        content = json.dumps({'data': {'rows': []}})
    elif 'raw.githubusercontent.com/' in url:
        content = '## Tools\\n' if os.environ.get('EMPTY_SIGNALS') == '1' else (
            '## Tools\\n[Demo](https://github.com/org/demo)\\n'
        )
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
        install_stub = harness_dir / "install.py"
        install_stub.write_text(
            """import os, shutil
from pathlib import Path
with open(os.environ['COMMANDS'], 'a', encoding='utf-8') as out:
    out.write('python -m pip install .\\n')
target = Path(os.environ['INSTALLED']) / 'oss_intelligence'
if target.exists():
    shutil.rmtree(target)
shutil.copytree('src/oss_intelligence', target)
""",
            encoding="utf-8",
        )
        driver = harness_dir / "real_git_driver.py"
        driver.write_text(
            """import json, os, subprocess, sys
from pathlib import Path

args = sys.argv[1:]
with open(os.environ['COMMANDS'], 'a', encoding='utf-8') as out:
    out.write('git ' + ' '.join(args) + '\\n')

hooks_path = Path(os.environ['GIT_HOOKS'])
hooks = json.loads(hooks_path.read_text(encoding='utf-8'))

def matches(trigger):
    if trigger == 'fetch':
        return args[:2] == ['fetch', 'origin']
    if trigger == 'push_main':
        return bool(args) and args[0] == 'push' and any(a.endswith(':refs/heads/main') for a in args)
    if trigger == 'delete_gate':
        return bool(args) and args[0] == 'push' and any(
            a == ':refs/heads/intelligence-recovery-pending' for a in args
        )
    return False

for index, hook in enumerate(hooks):
    if matches(hook['trigger']):
        remaining = hooks[:index] + hooks[index + 1:]
        hooks_path.write_text(json.dumps(remaining), encoding='utf-8')
        remote = os.environ['REMOTE_REPO']
        admin = hook.get('admin', False)
        if admin:
            subprocess.run([os.environ['REAL_GIT'], 'config', 'receive.denyNonFastForwards', 'false'],
                           cwd=remote, check=True)
        try:
            for command in hook['commands']:
                subprocess.run([os.environ['REAL_GIT'], *command], cwd=os.environ['ACTOR'], check=True)
        finally:
            if admin:
                subprocess.run([os.environ['REAL_GIT'], 'config', 'receive.denyNonFastForwards', 'true'],
                               cwd=remote, check=True)
        break

sys.exit(subprocess.run([os.environ['REAL_GIT'], *args]).returncode)
""",
            encoding="utf-8",
        )
        for name, content in {
            "git": '#!/bin/sh\nexec "$REAL_PYTHON" "$REAL_GIT_DRIVER" "$@"\n',
            "gh": (
                '#!/bin/sh\n'
                'printf "gh %s\\n" "$*" >> "$COMMANDS"\n'
                'if [ -z "${GH_TOKEN:-}" ]; then\n'
                '  echo "gh: GH_TOKEN environment variable is required" >&2\n'
                '  exit 1\n'
                'fi\n'
                '[ "$1 $2" = "release view" ] && exit 1\n'
                'if [ "$1 $2" = "release upload" ] && [ "${GH_UPLOAD_FAIL:-0}" = "1" ]; then\n'
                '  echo "simulated release upload failure" >&2\n'
                '  exit 1\n'
                'fi\n'
                'exit 0\n'
            ),
            "python": (
                '#!/bin/sh\n'
                'if [ "$1 $2 $3" = "-m pip install" ]; then\n'
                '  exec "$REAL_PYTHON" "$INSTALL_STUB"\n'
                'fi\n'
                'exec "$REAL_PYTHON" "$@"\n'
            ),
        }.items():
            path = bin_path / name
            path.write_text(content, encoding="utf-8")
            path.chmod(0o755)

        self.env = os.environ.copy()
        for name in (
            "GITHUB_TOKEN",
            "GH_TOKEN",
            "GITHUB_RELEASE_DB_URL",
            "APPRISE_URLS",
            "TRENDSHIFT_API_KEY",
            "DEFAULT_BRANCH",
        ):
            self.env.pop(name, None)
        for name in ("GIT_CONFIG_NOSYSTEM", "GIT_CONFIG_GLOBAL", "GIT_TERMINAL_PROMPT", "GIT_ALLOW_PROTOCOL"):
            self.env[name] = self.git_env[name]
        self.env.update({
            "PATH": str(bin_path) + os.pathsep + str(self.shell.parent),
            "PYTHONPATH": str(boundary) + os.pathsep + str(installed),
            "REAL_PYTHON": shell_path(Path(sys.executable)),
            "REAL_GIT": self.real_git,
            "REAL_GIT_DRIVER": shell_path(driver),
            "INSTALL_STUB": shell_path(install_stub),
            "INSTALLED": str(installed),
            "ACTOR": str(self.actor),
            "REMOTE_REPO": str(self.remote),
            "GIT_HOOKS": str(self.hooks_file),
            "COMMANDS": str(self.commands),
            "NOTIFICATIONS": str(self.notifications),
            "HTTP_REQUESTS": str(self.http_requests),
            "GITHUB_REF_NAME": branch,
            "GITHUB_REF": f"refs/heads/{branch}",
            "GITHUB_RUN_ID": "1",
            "GITHUB_RUN_ATTEMPT": "1",
            "RUNNER_TEMP": str(harness_dir),
            **{key: str(value) for key, value in settings.items()},
        })

    def git(self, cwd: Path, *args: str) -> str:
        result = subprocess.run(
            [self.real_git, *args],
            cwd=cwd,
            env=self.git_env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return result.stdout

    def configure(self, cwd: Path) -> None:
        self.git(cwd, "config", "user.name", "Test")
        self.git(cwd, "config", "user.email", "test@example.invalid")

    def on_git(self, trigger: str, commands: list[list[str]], *, admin: bool = False) -> None:
        hooks = json.loads(self.hooks_file.read_text(encoding="utf-8"))
        hooks.append({"trigger": trigger, "commands": commands, "admin": admin})
        self.hooks_file.write_text(json.dumps(hooks), encoding="utf-8")

    def resolve_env(self, mapping: dict[str, object]) -> dict[str, str]:
        resolved: dict[str, str] = {}
        for name, raw in mapping.items():
            text = str(raw).strip()
            if text.startswith("${{"):
                if text not in self.expressions:
                    raise KeyError(f"Unsupported workflow expression: {text}")
                resolved[name] = self.expressions[text]
            else:
                resolved[name] = str(raw)
        return resolved

    def run(self) -> tuple[int, str]:
        output: list[str] = []
        job = self.workflow["jobs"]["ingest"]
        base_job_env = {
            **self.resolve_env(self.workflow.get("env") or {}),
            **self.resolve_env(job.get("env") or {}),
        }
        for step in job["steps"]:
            if "run" not in step:
                continue
            step_env = self.env.copy()
            step_env.update(base_job_env)
            step_env.update(self.resolve_env(step.get("env") or {}))
            result = subprocess.run(
                [str(self.shell), "-eu", "-c", step["run"]],
                cwd=self.directory,
                env=step_env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=45,
            )
            output.append(result.stdout + result.stderr)
            if result.returncode:
                return result.returncode, "".join(output)
        return 0, "".join(output)

    def queued_run(self, label: str = "queued", *, run_id: int = 2, **settings) -> WorkflowRun:
        yaml = pytest.importorskip("yaml")
        queued = object.__new__(WorkflowRun)
        queued.real_git = self.real_git
        queued.shell = self.shell
        queued.root = self.root
        queued.filename = self.filename
        queued.remote = self.remote
        queued.actor = self.actor
        queued.git_env = self.git_env
        queued.initial = self.initial
        queued.baseline = self.baseline

        slot = self.root / f"run-{label}-{run_id}"
        slot.mkdir()
        queued.directory = slot / "runner"
        self.git(slot, "clone", "-b", "main", str(self.remote), str(queued.directory))
        queued.configure(queued.directory)

        queued.commands = slot / "commands.log"
        queued.notifications = self.notifications
        queued.http_requests = slot / "http_requests.jsonl"
        queued.hooks_file = slot / "git_hooks.json"
        queued.hooks_file.write_text("[]", encoding="utf-8")
        queued._build_harness(
            slot / "harness",
            branch="main",
            settings={"GITHUB_RUN_ID": str(run_id), "GITHUB_RUN_ATTEMPT": "1", **settings},
        )
        queued.expressions = self.expressions.copy()
        queued.workflow = yaml.safe_load((ROOT / ".github/workflows" / self.filename).read_text(encoding="utf-8"))
        return queued

    def command_log(self) -> str:
        return self.commands.read_text(encoding="utf-8") if self.commands.exists() else ""

    def outbound_requests(self) -> list[dict]:
        if not self.http_requests.exists():
            return []
        return [
            json.loads(line)
            for line in self.http_requests.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]


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
def test_step_environment_controls_notifications_github_auth_and_gh_release_token(
    tmp_path: Path, filename: str
):
    dry = WorkflowRun(tmp_path / "dry", filename)
    ingest_step = next(
        step for step in dry.workflow["jobs"]["ingest"]["steps"] if step.get("name") == "Ingest and publish"
    )
    del ingest_step["env"]["APPRISE_URLS"]

    status, output = dry.run()

    assert status == 0, output
    assert not dry.notifications.exists()
    repo_calls = [req for req in dry.outbound_requests() if req["url"].endswith("/repos/org/demo")]
    assert repo_calls and repo_calls[0]["authorization"] == "Bearer simulated"

    missing_gh = WorkflowRun(tmp_path / "missing-gh", filename)
    gh_step = next(
        step
        for step in missing_gh.workflow["jobs"]["ingest"]["steps"]
        if step.get("name") == "Ingest and publish"
    )
    del gh_step["env"]["GH_TOKEN"]
    del gh_step["env"]["GITHUB_TOKEN"]

    gh_status, gh_output = missing_gh.run()

    assert gh_status != 0
    assert "GH_TOKEN" in gh_output
    unauth_calls = [req for req in missing_gh.outbound_requests() if req["url"].endswith("/repos/org/demo")]
    assert unauth_calls and unauth_calls[0]["authorization"] is None
    assert missing_gh.git(missing_gh.actor, "ls-remote", "origin", PENDING_REF) != ""


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_manual_nondefault_branch_cannot_publish_the_shared_release(tmp_path: Path, filename: str):
    run = WorkflowRun(tmp_path, filename, branch="feature")
    status, output = run.run()

    assert status != 0
    assert "default branch" in output
    assert "git push" not in run.command_log()
    assert "gh release" not in run.command_log()
    assert not run.notifications.exists()
    assert not (run.directory / "intelligence-base-revision.txt").exists()


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_ingestion_uses_the_checkout_after_the_initial_pull(tmp_path: Path, filename: str):
    run = WorkflowRun(tmp_path, filename)
    main_module = run.actor / "src/oss_intelligence/__main__.py"
    main_module.write_text(
        main_module.read_text(encoding="utf-8").replace(
            "raise SystemExit", 'print("INSTALLED_CODE_AFTER_PULL")\nraise SystemExit'
        ),
        encoding="utf-8",
    )
    run.git(run.actor, "add", "src/oss_intelligence/__main__.py")
    run.git(run.actor, "commit", "-m", "Update CLI entry before runner pull")
    run.git(run.actor, "push", "origin", "HEAD:main")

    status, output = run.run()

    assert status == 0, output
    assert "INSTALLED_CODE_AFTER_PULL" in output
    assert run.command_log().index("git pull --rebase") < run.command_log().index("python -m pip install .")


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_intervening_non_sql_push_rebases_and_clears_gate_under_protected_fast_forward(
    tmp_path: Path, filename: str
):
    run = WorkflowRun(tmp_path, filename)
    (run.actor / "README.state").write_text("concurrent update\n", encoding="utf-8")
    run.on_git("push_main", [["add", "README.state"], ["commit", "-m", "Concurrent update"], ["push", "origin", "HEAD:main"]])

    status, output = run.run()

    assert status == 0, output
    assert run.git(run.remote, "show", "refs/heads/main:README.state") == "concurrent update\n"
    with sqlite3.connect(run.directory / "intelligence.db") as db:
        watermark = db.execute(
            "SELECT last_curated_notified_at FROM repositories WHERE full_name = 'org/demo'"
        ).fetchone()[0]
    assert watermark
    assert watermark in run.git(run.remote, "show", "refs/heads/main:data/schema_data.sql")
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) == ""
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    log = run.command_log()
    assert sum(
        line.startswith("git push ") and line.endswith(":refs/heads/main")
        for line in log.splitlines()
    ) == 2
    assert "gh release upload latest-db intelligence.db --clobber" in log


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_unchanged_sql_run_publishes_cleanly_but_stops_if_remote_sql_advanced_before_push(
    tmp_path: Path, filename: str
):
    # First seed canonical exported SQL on origin/main so an empty-signal run produces zero SQL diff.
    seed = WorkflowRun(tmp_path / "seed", filename, EMPTY_SIGNALS=1)
    status, output = seed.run()
    assert status == 0, output

    clean_no_diff = seed.queued_run("clean-nodiff", run_id=2, EMPTY_SIGNALS=1)
    tip_before = seed.git(seed.remote, "rev-parse", "refs/heads/main").strip()
    clean_status, clean_output = clean_no_diff.run()

    assert clean_status == 0, clean_output
    assert seed.git(seed.remote, "rev-parse", "refs/heads/main").strip() == tip_before
    assert "gh release upload latest-db intelligence.db --clobber" in clean_no_diff.command_log()
    assert seed.git(seed.actor, "ls-remote", "origin", PENDING_REF) == ""

    # Now verify a no-SQL-diff run does NOT upload a stale intelligence.db if remote SQL changed after fetch.
    stale_no_diff = seed.queued_run("stale-nodiff", run_id=3, EMPTY_SIGNALS=1)
    seed.git(seed.actor, "pull", "--rebase", "origin", "main")
    with (seed.actor / "data/schema_data.sql").open("a", encoding="utf-8") as sql:
        sql.write("\n-- newer remote sql\n")
    stale_no_diff.on_git(
        "push_main",
        [["add", "data/schema_data.sql"], ["commit", "-m", "Advance remote SQL"], ["push", "origin", "HEAD:main"]],
    )
    stale_status, stale_output = stale_no_diff.run()

    assert stale_status != 0
    assert "SQL" in stale_output
    assert "gh release" not in stale_no_diff.command_log()
    assert seed.git(seed.actor, "ls-remote", "origin", PENDING_REF) != ""


@pytest.mark.parametrize("trigger", ("fetch", "push_main"))
@pytest.mark.parametrize("filename", WORKFLOWS)
def test_concurrent_remote_rollback_with_unchanged_sql_is_not_undone(
    tmp_path: Path, filename: str, trigger: str
):
    run = WorkflowRun(tmp_path, filename)
    run.on_git(trigger, [["reset", "--hard", "HEAD^"], ["push", "--force", "origin", "HEAD:main"]], admin=True)

    status, output = run.run()

    assert status != 0, output
    assert run.git(run.actor, "rev-parse", "origin/main").strip() == run.initial
    assert "gh release" not in run.command_log()
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) != ""


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_rollback_of_rebased_intervening_commit_back_to_baseline_is_not_undone(
    tmp_path: Path, filename: str
):
    run = WorkflowRun(tmp_path, filename)
    (run.actor / "README.state").write_text("mistaken commit\n", encoding="utf-8")
    run.on_git("fetch", [["add", "README.state"], ["commit", "-m", "Mistaken commit"], ["push", "origin", "HEAD:main"]])
    run.on_git("push_main", [["reset", "--hard", "HEAD^"], ["push", "--force", "origin", "HEAD:main"]], admin=True)

    status, output = run.run()

    assert status != 0, output
    assert run.git(run.remote, "rev-parse", "refs/heads/main").strip() == run.baseline
    assert run.git(run.remote, "show", "refs/heads/main:README.state") == "later baseline\n"
    assert "gh release" not in run.command_log()
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) != ""


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_divergent_remote_rewrite_with_unchanged_sql_stops_publication(
    tmp_path: Path, filename: str
):
    run = WorkflowRun(tmp_path, filename)
    run.on_git(
        "fetch",
        [
            ["reset", "--hard", "HEAD^"],
            ["commit", "--allow-empty", "-m", "Divergent history rewrite"],
            ["push", "--force", "origin", "HEAD:main"],
        ],
        admin=True,
    )

    status, output = run.run()

    assert status != 0, output
    rewritten_tip = run.git(run.actor, "rev-parse", "HEAD").strip()
    assert run.git(run.remote, "rev-parse", "refs/heads/main").strip() == rewritten_tip
    assert "gh release" not in run.command_log()
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) != ""


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_failed_sql_publication_gates_queued_runs_until_maintainer_reconciles(
    tmp_path: Path, filename: str
):
    run = WorkflowRun(tmp_path, filename)
    with (run.actor / "data/schema_data.sql").open("a", encoding="utf-8") as sql:
        sql.write("\n-- independent update\n")
    run.on_git("fetch", [["add", "data/schema_data.sql"], ["commit", "-m", "Change SQL"], ["push", "origin", "HEAD:main"]])

    status, output = run.run()

    assert status != 0, output
    assert "SQL" in output
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    with sqlite3.connect(run.directory / "intelligence.db") as db:
        watermark = db.execute(
            "SELECT last_curated_notified_at FROM repositories WHERE full_name = 'org/demo'"
        ).fetchone()[0]
    assert watermark is not None
    preserved_sql = (run.directory / "data/schema_data.sql").read_text(encoding="utf-8")
    assert watermark in preserved_sql
    assert (run.directory / "intelligence-base-revision.txt").read_text(encoding="utf-8").strip() == run.baseline

    recovery_steps = [
        step
        for step in run.workflow["jobs"]["ingest"]["steps"]
        if step.get("uses", "").startswith("actions/upload-artifact@")
    ]
    assert len(recovery_steps) == 1
    assert "failure()" in recovery_steps[0]["if"]
    assert "intelligence-base-revision.txt" in recovery_steps[0]["if"]
    assert "intelligence.db" in recovery_steps[0]["with"]["path"]
    assert "data/schema_data.sql" in recovery_steps[0]["with"]["path"]
    assert "intelligence-base-revision.txt" in recovery_steps[0]["with"]["path"]

    pending = run.git(run.actor, "ls-remote", "origin", PENDING_REF)
    assert pending
    gate_message = run.git(run.remote, "log", "-1", "--format=%B", PENDING_REF)
    assert f"Baseline: {run.baseline}" in gate_message

    # A queued scheduled run hits the gate before ingestion/notification and leaves the gate intact.
    queued = run.queued_run("blocked", run_id=2)
    next_status, next_output = queued.run()
    assert next_status != 0, next_output
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    assert "gh release" not in queued.command_log()
    assert not (queued.directory / "intelligence-base-revision.txt").exists()
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) == pending

    # Maintainer reconciles the preserved watermarked SQL onto main and clears the recovery gate.
    run.git(run.actor, "pull", "--rebase", "origin", "main")
    (run.actor / "data/schema_data.sql").write_text(preserved_sql, encoding="utf-8")
    run.git(run.actor, "add", "data/schema_data.sql")
    run.git(run.actor, "commit", "-m", "Reconcile recovery artifact watermarks")
    run.git(run.actor, "push", "origin", "HEAD:main")
    run.git(run.actor, "push", "origin", f":{PENDING_REF}")

    resumed = run.queued_run("resumed", run_id=3)
    resumed_status, resumed_output = resumed.run()
    assert resumed_status == 0, resumed_output
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    assert "gh release upload latest-db intelligence.db --clobber" in resumed.command_log()
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) == ""


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_release_upload_failure_leaves_gate_active_and_resumes_cleanly_after_recovery(
    tmp_path: Path, filename: str
):
    run = WorkflowRun(tmp_path, filename, GH_UPLOAD_FAIL=1)

    status, output = run.run()

    assert status != 0
    assert "simulated release upload failure" in output
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    pending = run.git(run.actor, "ls-remote", "origin", PENDING_REF)
    assert pending

    blocked = run.queued_run("blocked-after-upload-fail", run_id=2)
    blocked_status, _ = blocked.run()
    assert blocked_status != 0
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) == pending

    # Maintainer clears the gate; main already contains the watermarked SQL so the next run self-heals latest-db.
    run.git(run.actor, "push", "origin", f":{PENDING_REF}")
    resumed = run.queued_run("resumed-after-upload-fail", run_id=3)
    resumed_status, resumed_output = resumed.run()
    assert resumed_status == 0, resumed_output
    assert run.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    assert "gh release upload latest-db intelligence.db --clobber" in resumed.command_log()
    assert run.git(run.actor, "ls-remote", "origin", PENDING_REF) == ""


@pytest.mark.parametrize("filename", WORKFLOWS)
def test_exhausted_push_retries_and_foreign_gate_lease_fail_closed(
    tmp_path: Path, filename: str
):
    exhausted = WorkflowRun(tmp_path / "exhausted", filename)
    for index in range(3):
        exhausted.on_git(
            "push_main",
            [["commit", "--allow-empty", "-m", f"Intervening {index}"], ["push", "origin", "HEAD:main"]],
        )

    status, output = exhausted.run()

    assert status != 0
    assert "preserve this run" in output
    assert sum(
        line.startswith("git push ") and line.endswith(":refs/heads/main")
        for line in exhausted.command_log().splitlines()
    ) == 3
    assert "gh release" not in exhausted.command_log()
    assert exhausted.notifications.read_text(encoding="utf-8").splitlines() == ["delivered"]
    assert exhausted.git(exhausted.actor, "ls-remote", "origin", PENDING_REF) != ""

    foreign = WorkflowRun(tmp_path / "foreign-gate", filename)
    foreign.on_git(
        "delete_gate",
        [["commit", "--allow-empty", "-m", "Maintainer manual hold"], ["push", "--force", "origin", f"HEAD:{PENDING_REF}"]],
        admin=True,
    )

    foreign_status, foreign_output = foreign.run()

    assert foreign_status != 0
    assert "Could not clear the recovery gate" in foreign_output
    manual_hold = foreign.git(foreign.actor, "rev-parse", "HEAD").strip()
    assert foreign.git(foreign.remote, "rev-parse", PENDING_REF).strip() == manual_hold
