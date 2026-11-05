#!/bin/sh
set -eu

# This script starts after the default-branch guard, pull and installation.
base=$(git rev-parse HEAD)
printf '%s\n' "$base" > intelligence-base-revision.txt
python -c 'from pathlib import Path; from oss_intelligence.store import IntelligenceStore; s = IntelligenceStore("intelligence.db"); p = Path("data/schema_data.sql"); s.import_sql_dump(p) if p.exists() else None'
if [ -n "${APPRISE_URLS:-}" ]; then
  python -m oss_intelligence --database intelligence.db
else
  python -m oss_intelligence --database intelligence.db --dry-run
fi
python -c 'from oss_intelligence.store import IntelligenceStore; IntelligenceStore("intelligence.db").export_sql_dump("data/schema_data.sql")'

git config user.name 'github-actions[bot]'
git config user.email '41898282+github-actions[bot]@users.noreply.github.com'
git add data/schema_data.sql
if ! git diff --cached --quiet; then
  git commit -m 'Update intelligence SQL dump'
fi

pushed=false
for attempt in 1 2 3; do
  git fetch origin "$GITHUB_REF_NAME"
  if ! git diff --quiet "$base" FETCH_HEAD -- data/schema_data.sql; then
    echo "Remote SQL changed during ingestion; preserve this run for recovery." >&2
    exit 1
  fi
  git rebase FETCH_HEAD
  if git push origin "HEAD:$GITHUB_REF_NAME"; then
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
