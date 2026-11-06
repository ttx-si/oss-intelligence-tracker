#!/bin/sh
set -eu

# This script starts after the default-branch guard, pull and installation.
pending_ref=refs/heads/intelligence-recovery-pending
if [ "refs/heads/${GITHUB_REF_NAME:?}" = "$pending_ref" ]; then
  echo "Refusing to run the publishing pipeline on the recovery gate ref." >&2
  exit 1
fi
base=$(git rev-parse HEAD)
git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'

# A unique commit and an absent-ref lease make creation an atomic fail-closed gate.
nonce=$(python -c 'import secrets; print(secrets.token_hex(16))')
pending=$(printf 'Intelligence run %s attempt %s\nBaseline: %s\nNonce: %s\n' \
  "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "$base" "$nonce" | git commit-tree "$base^{tree}" -p "$base")
if ! git push --force-with-lease="$pending_ref:" origin "$pending:$pending_ref"; then
  echo "A recovery gate may already exist or could not be created; stop before ingestion." >&2
  exit 1
fi
printf '%s\n' "$base" > intelligence-base-revision.txt

python -c 'from pathlib import Path; from oss_intelligence.store import IntelligenceStore; s = IntelligenceStore("intelligence.db"); p = Path("data/schema_data.sql"); s.import_sql_dump(p) if p.exists() else None'
if [ -n "${APPRISE_URLS:-}" ]; then
  python -m oss_intelligence --database intelligence.db
else
  python -m oss_intelligence --database intelligence.db --dry-run
fi
python -c 'from oss_intelligence.store import IntelligenceStore; IntelligenceStore("intelligence.db").export_sql_dump("data/schema_data.sql")'

git add data/schema_data.sql
if ! git diff --cached --quiet; then
  git commit -m 'Update intelligence SQL dump'
fi

pushed=false
required_ancestor=$base
for attempt in 1 2 3; do
  git fetch origin "$GITHUB_REF_NAME"
  remote_tip=$(git rev-parse FETCH_HEAD)
  if ! git merge-base --is-ancestor "$required_ancestor" "$remote_tip"; then
    echo "Remote branch no longer descends from the expected revision; preserve this run for recovery." >&2
    exit 1
  fi
  if ! git diff --quiet "$base" "$remote_tip" -- data/schema_data.sql; then
    echo "Remote SQL changed during ingestion; preserve this run for recovery." >&2
    exit 1
  fi
  required_ancestor=$remote_tip
  git rebase "$remote_tip"
  candidate=$(git rev-parse HEAD)
  if ! git merge-base --is-ancestor "$remote_tip" "$candidate"; then
    echo "Publication is not a fast-forward; preserve this run for recovery." >&2
    exit 1
  fi
  if git push --force-with-lease="refs/heads/$GITHUB_REF_NAME:$remote_tip" origin "$candidate:refs/heads/$GITHUB_REF_NAME"; then
    pushed=true
    break
  fi
done
if [ "$pushed" != true ]; then
  echo "Could not push the intelligence SQL; preserve this run for recovery." >&2
  exit 1
fi

gh release view latest-db >/dev/null 2>&1 || gh release create latest-db --title 'Latest intelligence database' --notes 'Rolling SQLite database asset.'
gh release upload latest-db intelligence.db --clobber

# Delete only our own gate after both tracks have been published.
if ! git push --force-with-lease="$pending_ref:$pending" origin ":$pending_ref"; then
  echo "Could not clear the recovery gate; inspect the run before resuming." >&2
  exit 1
fi
