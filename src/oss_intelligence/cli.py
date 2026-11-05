"""Command-line entry point for ingestion and Batch Digest previews."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from oss_intelligence.adapters import (
    GitHubSearchSpikeAdapter,
    GtrendingAdapter,
    HelloGitHubAdapter,
    OSSInsightAdapter,
    TrendshiftAdapter,
)
from oss_intelligence.github_metadata import GitHubMetadataClient
from oss_intelligence.pipeline import IngestionPipeline
from oss_intelligence.store import IntelligenceStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest open source signals and deliver a Batch Digest.")
    parser.add_argument("--database", default="intelligence.db", help="Path to the SQLite database")
    parser.add_argument("--dry-run", action="store_true", help="Preview without sending notifications")
    parser.add_argument(
        "--notifications-only", action="store_true", help="Process existing Repositories without fetching sources"
    )
    args = parser.parse_args(argv)
    notifier = None
    if not args.dry_run:
        urls = [line.strip() for line in os.environ.get("APPRISE_URLS", "").splitlines() if line.strip()]
        if not urls:
            parser.error("Set APPRISE_URLS to one Apprise URL per line before sending")
        try:
            from apprise import Apprise
        except ImportError:
            parser.error("Apprise is not installed; install the project dependencies")
        notifier = Apprise()
        for url in urls:
            if not notifier.add(url):
                parser.error("An APPRISE_URLS entry was rejected by Apprise")

    logging.basicConfig(level=logging.WARNING)
    sources = [] if args.notifications_only else [
        GtrendingAdapter(),
        GitHubSearchSpikeAdapter(),
        OSSInsightAdapter(),
        HelloGitHubAdapter(),
        TrendshiftAdapter(),
    ]
    preview = IngestionPipeline(
        store=IntelligenceStore(args.database),
        signal_adapters=sources,
        metadata_client=GitHubMetadataClient(),
        notifier=notifier,
    ).run(dry_run=args.dry_run)
    if preview:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        print(preview)
    return 0
