"""Signal Source adapters."""

from oss_intelligence.adapters.gh_search import GitHubSearchSpikeAdapter
from oss_intelligence.adapters.gtrending import GtrendingAdapter
from oss_intelligence.adapters.hellogithub import HelloGitHubAdapter
from oss_intelligence.adapters.ossinsight import OSSInsightAdapter
from oss_intelligence.adapters.trendshift import TrendshiftAdapter

__all__ = [
    "GitHubSearchSpikeAdapter",
    "GtrendingAdapter",
    "HelloGitHubAdapter",
    "OSSInsightAdapter",
    "TrendshiftAdapter",
]
