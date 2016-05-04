#!/usr/bin/env python
"""Graphite utility module"""

import fnmatch
import re

# http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
GRAPHITE_GLOB_RE = re.compile("^[^*?{}\\[\\]]+$")
def is_graphite_glob(metric_name):
    """Return whether a metric is a Graphite glob"""
    return GRAPHITE_GLOB_RE.match(metric_name) is None


def graphite_to_cassandra_glob(graphite_glob):
    """Transform Graphite glob to Cassandra glob"""
    return ["*" if is_graphite_glob(c) else c for c in graphite_glob.split(".")]


def filter_metrics(entries, pattern):
    """A drop-in replacement for fnmatch.filter that supports pattern
    variants (ie. {foo,bar}baz = foobaz or barbaz)."""
    brace_open, brace_c = pattern.find('{'), pattern.find('}')

    matches = set()
    variants = [pattern]
    if brace_open > -1 and brace_c > brace_open:
        brace_variants = pattern[brace_open+1:brace_c].split(',')
        variants = [pattern[:brace_open] + p + pattern[brace_c+1:] for p in brace_variants]

    for variant in variants:
        for match in fnmatch.filter(entries, variant):
            matches.add(match)

    return sorted(matches)


def get_graphite_metrics(accessor, graphite_glob):
    """Get Cassandra metrics matching a Graphite glob"""
    cassandra_glob = graphite_to_cassandra_glob(graphite_glob)
    cassandra_metrics = accessor.glob_metric_names(cassandra_glob)
    return filter_metrics(cassandra_metrics, graphite_glob)

