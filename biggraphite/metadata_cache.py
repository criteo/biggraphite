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
"""Implements the DiskCache for metrics metadata.

The DiskCache is implemented with lmdb, an on-disk file DB that can be accessed
by multiple processes. Keys are metric names, values are json-serialised metadata.
In deployment the graphite storage dir is used as a rendez-vous point where all processes
(carbon, graphite, ...) can find the metadata.

TODO(b.arnould): Currently that cache never expires, as we don't allow for deletion.
"""

from __future__ import absolute_import
from __future__ import print_function

import os
from os import path as os_path
import sys
import threading
import logging

import lmdb

from biggraphite import accessor as bg_accessor


class Error(Exception):
    """Base class for all exceptions from this module."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


class DiskCache(object):
    """A metadata cache that can be shared between processes trusting each other.

    open() and close() are the only thread unsafe methods.
    See module-level comments for the design.
    """

    __SINGLETONS = {}
    __SINGLETONS_LOCK = threading.Lock()
    # Maximum number of concurrent readers.
    # Used to size a file that is mmapped in all readers. Cannot be raised while the DB is opened.
    # According to LMDB's author, 128 readers is about 8KiB of RAM, 1024 is about 128kiB and even
    # 4096 is safe: https://twitter.com/armon/status/534867803426533376
    _MAX_READERS = 2048

    _METRIC_SEPARATOR = '|'

    def __init__(self, accessor, path):
        """Create a new DiskCache."""
        self.hit_count = 0
        self.miss_count = 0
        self.__accessor_lock = threading.Lock()
        self.__accessor = accessor
        self.__env = None
        # __json_cache associates unparsed json to metadata instances. The idea is that there are
        # very few configs in use in a given cluster so the few same strings will show up over
        # and over.
        self.__json_cache_lock = threading.Lock()
        self.__json_cache = {}
        self.__path = os_path.join(path, "biggraphite", "cache", "version0")
        self.__databases = {
            "metric_to_meta": None
        }
        self.__metric_to_metadata_db = None

    def open(self):
        """Allocate ressources used by the cache.

        Safe to call again after close() returned.
        """
        if self.__env:
            return
        try:
            os.makedirs(self.__path)
        except OSError:
            pass  # Directory already exists
        map_size = 1024*1024*1024  # 1G on 32 bits systems
        if sys.maxsize > 2**32:
            map_size *= 16  # 16G on 64 bits systems
        self.__env = lmdb.open(
            self.__path,
            map_size=map_size,
            # Only one sync per transaction, system crash can undo a transaction.
            metasync=False,
            # Use mmap()
            writemap=True,
            # Max number of concurrent readers, see _MAX_READERS for details
            max_readers=self._MAX_READERS,
            # How many DBs we may create (until we increase version prefix).
            max_dbs=8,
            # A cache of read-only transactions, should match max number of threads.
            # Only transactions that are actually used concurrently allocate memory,
            # so setting a high number doesn't cost much even if thread count is low.
            max_spare_txns=128,
        )

        databases = {}
        for name in self.__databases:
            databases[name] = self.__env.open_db(name)
        self.__databases = databases

        self.__metric_to_metadata_db = databases["metric_to_meta"]

    def close(self):
        """Free resources allocated by open().

        Safe to call multiple time.
        """
        if self.__env:
            self.__env.close()
            self.__env = None

    def make_metric(self, name, metadata):
        """Create a metric object from a name and metadata."""
        return self.__accessor.make_metric(name, metadata)

    def create_metric(self, metric):
        """Create a metric definition from a Metric."""
        with self.__accessor_lock:
            self.__accessor.create_metric(metric)
        self._cache(metric)

    def has_metric(self, metric_name):
        """Check if a metric exists."""
        encoded_metric_name = bg_accessor.encode_metric_name(metric_name)
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            found = bool(txn.get(encoded_metric_name))
        if not found:
            with self.__accessor_lock:
                found = self.__accessor.has_metric(metric_name)
            if found:
                # The metric was found in the database but not cached, let's
                # cache it now.
                self.get_metric(metric_name)
        return found

    def _get_metric_from_cache(self, metric_name):
        """Return a Metric from a the cache, None if no such metric."""
        encoded_metric_name = bg_accessor.encode_metric_name(metric_name)
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            id_metadata = txn.get(encoded_metric_name)

        if id_metadata:
            split = id_metadata.split(self._METRIC_SEPARATOR, 1)
            if len(split) == 2:
                # get id and metadata string
                # TODO: optimization: id is a UUID (known length)
                id, metadata_str = split
                with self.__json_cache_lock:
                    metadata = self.__json_cache.get(metadata_str)
                    if not metadata:
                        metadata = bg_accessor.MetricMetadata.from_json(metadata_str)
                        self.__json_cache[metadata_str] = metadata
                    return bg_accessor.Metric(metric_name, id, metadata)
            else:
                # invalid string => evict from cache
                with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
                    txn.delete(key=encoded_metric_name)

        return None

    def get_metric(self, metric_name):
        """Return a Metric for this metric_name, None if no such metric."""
        metric = self._get_metric_from_cache(metric_name)

        if metric:
            # valid string => on disk cache hit
            self.hit_count += 1
        else:
            # invalid string or on disk cache miss
            self.miss_count += 1
            with self.__accessor_lock:
                metric = self.__accessor.get_metric(metric_name)
                self._cache(metric)

        return metric

    def stat(self):
        """Count number of cached entries."""
        ret = {}
        ret[''] = self.__env.stat(),
        for name, database in self.__databases.iteritems():
            with self.__env.begin(database, write=False) as txn:
                ret[name] = txn.stat(database)
        return ret

    def get_value(self, metric):
        """Get cache value for a metric.

        The cache value is a combination of:
          - id
          - _METRIC_SEPARATOR
          - metadata
        """
        return str(metric.id) + self._METRIC_SEPARATOR + metric.metadata.as_json()

    def repair(self):
        """Remove spurious entries from the cache."""
        # TODO: add support for start_key and end_key.
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                metric = self.__accessor.get_metric(key)
                expected_value = self.get_value(metric) if metric else None
                if value != expected_value:
                    logging.warning(
                        "Removing invalid key '%s': expected: %s cached: %s" % (
                            key, expected_value, value))
                    txn.delete(key=key)

    def _cache(self, metric):
        """If metric is valid, add it to the cache.

        The metric is stored in the cache as follows:
          - its id, which is a UUID.
          - vertical bar (pipe) separator.
          - its metadata JSON representation.
        """
        if not metric:
            # Do not cache absent metrics, they will probably soon be created.
            return None
        key = metric.name
        value = self.get_value(metric)
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            txn.put(key, value, dupdata=False, overwrite=True)
