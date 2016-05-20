#!/usr/bin/env python
"""Graphite utility module."""

import fnmatch
import re

# http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
_GRAPHITE_GLOB_RE = re.compile("^[^*?{}\\[\\]]+$")


def _is_graphite_glob(metric_component):
    """Return whether a metric component is a Graphite glob."""
    return _GRAPHITE_GLOB_RE.match(metric_component) is None


def _graphite_glob_to_accessor_components(graphite_glob):
    """Transform Graphite glob into Cassandra accessor components."""
    return ".".join([
        "*" if _is_graphite_glob(c) else c
        for c in graphite_glob.split(".")
    ])


def _filter_metrics(metrics, glob):
    """fnmatch.filter supporting braces. Adapted from graphite-web/webapp/graphite/finders/__init__.py.

    Args:
      metrics: list of strings, the accessor elements to match to glob
      glob: pattern used to filter, like fnmatch with one pair of braces allowed

    Returns:
      Sorted unique list of metrics matching the glob.
    """
    brace_open, brace_close = glob.find("{"), glob.find("}")

    matches = set()
    variants = [glob]
    if brace_open > -1 and brace_close > brace_open:
        brace_variants = glob[brace_open+1:brace_close].split(",")
        variants = [glob[:brace_open] + p + glob[brace_close+1:] for p in brace_variants]

    for variant in variants:
        for match in fnmatch.filter(metrics, variant):
            matches.add(match)

    return sorted(matches)


def glob_metrics_directories(accessor, graphite_glob):
    """Get Cassandra metrics matching a Graphite glob.

    Args:
      accessor: Cassandra accessor
      graphite_glob: Graphite glob expression

    Returns:
      A tuple:
        First element: sorted list of Cassandra metrics matched by the glob.
        Second element: sorted list of Cassandra directories matched by the glob.
    """
    accessor_components = _graphite_glob_to_accessor_components(graphite_glob)
    metrics = _filter_metrics(accessor.glob_metric_names(accessor_components), graphite_glob)
    directories = _filter_metrics(accessor.glob_directory_names(accessor_components), graphite_glob)
    return (metrics, directories)
