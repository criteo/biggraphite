#!/usr/bin/env python
# Copyright 2016 Criteo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Graphite utility module."""

import fnmatch
from os import path as os_path
import re

from biggraphite.drivers import cassandra as bg_cassandra

# http://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
_GRAPHITE_GLOB_RE = re.compile(r"^[^*?{}\[\]]+$")


class Error(Exception):
    """Base class for all exceptions from this module."""


class ConfigError(Error):
    """Configuration problems."""


def _get_setting(settings, name, optional=False):
    # Only way we found to support both Carbon & Django settings
    try:
        res = getattr(settings, name)
    except:
        res = None
    if not res and not optional:
        raise ConfigError("%s missing in configuration" % name)
    return res


def accessor_from_settings(settings):
    """Get Accessor from configuration.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      Cassandra accessor (not connected)
    """
    keyspace = _get_setting(settings, "BG_KEYSPACE")
    contact_points_str = _get_setting(settings, "BG_CONTACT_POINTS")
    port = _get_setting(settings, "BG_PORT", optional=True)
    if port is not None:
        port = int(port)
    contact_points = [s.strip() for s in contact_points_str.split(",")]
    return bg_cassandra.connect(keyspace, contact_points, port)


def storage_path_from_settings(settings):
    """Get storage path from configuration.

    Args:
      settings: either carbon_conf.Settings or a Django-like settings object

    Returns:
      An absolute path.
    """
    path = _get_setting(settings, "STORAGE_DIR")
    if not os_path.exists(path):
        raise ConfigError("STORAGE_DIR is set to an unexisting directory: '%s'" % path)
    return os_path.abspath(path)


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
      Sorted unique list of metrics matching the glob
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


def glob(accessor, graphite_glob):
    """Get Cassandra metrics & directories matching a Graphite glob.

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
