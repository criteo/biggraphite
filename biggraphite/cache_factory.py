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
"""Module to create caches."""

from biggraphite import metadata_cache


CACHES = frozenset(
    [
        ("disk", metadata_cache.DiskCache),
        ("memory", metadata_cache.MemoryCache),
        ("none", metadata_cache.NoneCache)
    ]
)
DEFAULT_CACHE = "memory"


class Error(Exception):
    """Base class for all exceptions from this module."""


class ConfigError(Error):
    """Configuration problems."""


def add_argparse_arguments(parser):
    """Add cache related BigGraphite arguments to an argparse parser.

    Args:
      parser: argparse.ArgumentParser()
    """
    parser.add_argument(
        "--cache",
        help="BigGraphite cache (%s))" % ", ".join([v[0] for v in CACHES]),
        default=DEFAULT_CACHE,
    )
    parser.add_argument("--cache-size", help="Metadata cache size.")
    parser.add_argument("--cache-sync", help="Metadata cache sync.")


def cache_from_settings(accessor, settings, cname=None):
    """Get Cache from configuration.

    Args:
      settings: dict(str -> value).

    Returns:
      Cache (not opened).
    """
    cache_name = settings.get("cache", DEFAULT_CACHE)
    cache_settings = {"path": settings.get("storage_dir")}
    for opt in ["size", "ttl", "sync"]:
        value = settings.get("cache_%s" % opt)
        if value is not None:
            cache_settings[opt] = value

    for name, cache in CACHES:
        if name == cache_name:
            return cache(accessor, cache_settings, cname)

    raise ConfigError("Invalid value '%s' for BG_CACHE." % cache_name)
