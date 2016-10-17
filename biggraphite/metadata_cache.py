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

import abc
import os
from os import path as os_path
import sys
import threading
import logging
import uuid

import cachetools
import lmdb

from biggraphite import accessor as bg_accessor


class Error(Exception):
    """Base class for all exceptions from this module."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


class Cache(object):
    """A metadata cache."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, accessor, settings):
        """Create a new DiskCache."""
        assert accessor
        self.hit_count = 0
        self.miss_count = 0
        self._accessor_lock = threading.Lock()
        self._accessor = accessor
        # _json_cache associates unparsed json to metadata instances.
        # The idea is that there are very few configs in use in a given
        # cluster so the few same strings will show up over and over.
        self._json_cache_lock = threading.Lock()
        self._json_cache = {}

    @abc.abstractmethod
    def open(self):
        """Allocate ressources used by the cache.

        Safe to call again after close() returned.
        """
        pass

    @abc.abstractmethod
    def close(self):
        """Free resources allocated by open().

        Safe to call multiple time.
        """
        pass

    def make_metric(self, name, metadata):
        """Create a metric object from a name and metadata."""
        return self._accessor.make_metric(name, metadata)

    def create_metric(self, metric):
        """Create a metric definition from a Metric."""
        with self._accessor_lock:
            self._accessor.create_metric(metric)
        self._cache_set(metric.name, metric)

    def has_metric(self, metric_name):
        """Check if a metric exists."""
        found = self._cache_has(metric_name)
        if not found:
            with self._accessor_lock:
                found = self._accessor.has_metric(metric_name)
            if found:
                # The metric was found in the database but not cached, let's
                # cache it now.
                metric = self.get_metric(metric_name)
                self._cache_set(metric_name, metric)
        return found

    def get_metric(self, metric_name):
        """Return a Metric for this metric_name, None if no such metric."""
        metric, hit = self._cache_get(metric_name)
        if hit:
            self.hit_count += 1
        else:
            self.miss_count += 1
            with self._accessor_lock:
                metric = self._accessor.get_metric(metric_name)
                self._cache_set(metric_name, metric)

        return metric

    @abc.abstractmethod
    def repair(self, start_key=None, end_key=None, shard=0, nshards=1):
        """Remove spurious entries from the cache.

        During the repair the keyspace is split in nshards and
        this function will only take care of 1/n th of the data
        as specified by shard. This allows the caller to parallelize
        the repair if needed.

        Args:
          start_key: string, start at key >= start_key.
          end_key: string, stop at key < end_key.
          shard: int, shard to repair.
          nshards: int, number of shards.
        """
        pass

    def metadata_from_str(self, metadata_str):
        """Create a MetricMetadata from a json string."""
        with self._json_cache_lock:
            metadata = self._json_cache.get(metadata_str)
            if not metadata:
                metadata = bg_accessor.MetricMetadata.from_json(metadata_str)
                self._json_cache[metadata_str] = metadata
        return metadata

    @abc.abstractmethod
    def _cache_has(self, metric_name):
        """Check if metric is cached."""
        pass

    @abc.abstractmethod
    def _cache_get(self, metric_name):
        """Get metric from cache."""
        pass

    @abc.abstractmethod
    def _cache_set(self, metric_name, metric):
        """Put metric in the cache."""
        pass


class MemoryCache(Cache):
    """A per-process memory cache."""

    def __init__(self, accessor, settings):
        """Initialize the memory cache."""
        super(MemoryCache, self).__init__(accessor, settings)
        self.__size = settings.get('size', 1*1000*1000)
        self.__ttl = settings.get('ttl', 24*60*60)

    def open(self):
        """Allocate ressources used by the cache."""
        self.__cache = cachetools.TTLCache(maxsize=self.__size, ttl=self.__ttl)

    def close(self):
        """Free resources allocated by open()."""
        self.__cache = None

    def _cache_has(self, metric_name):
        """Check if metric is cached."""
        return metric_name in self.__cache

    def _cache_get(self, metric_name):
        """Get metric from cache."""
        metric = self.__cache.get(metric_name, False)
        if metric is False:
            return None, False
        else:
            return metric, True

    def _cache_set(self, metric_name, metric):
        """Put metric in the cache."""
        if metric:
            self.__cache[metric_name] = metric

    def repair(self, start_key=None, end_key=None, shard=0, nshards=1):
        """Remove spurious entries from the cache."""
        i = 0
        for key in self.__cache:
            if start_key is not None and key < start_key:
                continue
            i += 1
            if end_key is not None and key >= end_key:
                break
            if nshards > 1 and (i % nshards) != shard:
                continue

            metric = self._accessor.get_metric(key)
            expected_metric = self.__cache[key]
            if metric != expected_metric:
                logging.warning(
                    "Removing invalid key '%s': expected: %s cached: %s" % (
                        key, expected_metric, metric))
                del self.__cache[key]


class DiskCache(Cache):
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
    _EMPTY = 'nil'

    # 1G on 32 bits systems
    # 16G on 64 bits systems
    MAP_SIZE = (1024*1024*1024*16 if sys.maxsize > 2**32 else 1024*1024*1024)

    def __init__(self, accessor, settings):
        """Create a new DiskCache."""
        super(DiskCache, self).__init__(accessor, settings)

        path = settings.get("path")
        assert path

        self.__env = None
        self.__path = os_path.join(path, "biggraphite", "cache", "version0")
        self.__size = settings.get("size", self.MAP_SIZE)
        self.__databases = {
            "metric_to_meta": None
        }
        self.__metric_to_metadata_db = None

    def open(self):
        """Allocate ressources used by the cache.

        Safe to call again after close() returned.
        """
        super(DiskCache, self).open()
        if self.__env:
            return
        try:
            os.makedirs(self.__path)
        except OSError:
            pass  # Directory already exists
        logging.info('Opening cache %s' % self.__path)
        self.__env = lmdb.open(
            self.__path,
            map_size=self.__size,
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
            super(DiskCache, self).close()

    def _cache_has(self, metric_name):
        """Check if metric is cached."""
        encoded_metric_name = bg_accessor.encode_metric_name(metric_name)
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            return bool(txn.get(encoded_metric_name))

    def _cache_get(self, metric_name):
        """Return a Metric from a the cache, None if no such metric."""
        encoded_metric_name = bg_accessor.encode_metric_name(metric_name)
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            id_metadata = txn.get(encoded_metric_name)

        if id_metadata == self._EMPTY:
            return None, True

        if not id_metadata:
            # cache miss
            return None, False

        # found something in the cache
        split = id_metadata.split(self._METRIC_SEPARATOR, 1)
        if len(split) != 2:
            # invalid string => evict from cache
            with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
                txn.delete(key=encoded_metric_name)
            return None, False

        # valid value => get id and metadata string
        # TODO: optimization: id is a UUID (known length)
        id_str, metadata_str = split
        try:
            id = uuid.UUID(id_str)
        except:
            with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
                txn.delete(key=encoded_metric_name)
            return None, False

        metadata = self.metadata_from_str(metadata_str)
        return bg_accessor.Metric(metric_name, id, metadata), True

    def _cache_set(self, metric_name, metric):
        """If metric is valid, add it to the cache.

        The metric is stored in the cache as follows:
          - its id, which is a UUID.
          - vertical bar (pipe) separator.
          - its metadata JSON representation.
        """
        encoded_metric_name = bg_accessor.encode_metric_name(metric_name)
        key = encoded_metric_name
        value = self.__get_value(metric)
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            txn.put(key, value, dupdata=False, overwrite=True)

    def stat(self):
        """Count number of cached entries."""
        ret = {}
        ret[''] = self.__env.stat(),
        for name, database in self.__databases.iteritems():
            with self.__env.begin(database, write=False) as txn:
                ret[name] = txn.stat(database)
        return ret

    def __get_value(self, metric):
        """Get cache value for a metric.

        The cache value is a combination of:
          - id
          - _METRIC_SEPARATOR
          - metadata
        """
        if not metric:
            return self._EMPTY
        return str(metric.id) + self._METRIC_SEPARATOR + metric.metadata.as_json()

    def repair(self, start_key=None, end_key=None, shard=0, nshards=1):
        """Remove spurious entries from the cache.

        During the repair the keyspace is split in nshards and
        this function will only take care of 1/n th of the data
        as specified by shard. This allows the caller to parallelize
        the repair if needed.

        Args:
          start_key: string, start at key >= start_key.
          end_key: string, stop at key < end_key.
          shard: int, shard to repair.
          nshards: int, number of shards.
        """
        i = 0
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            cursor = txn.cursor()
            if start_key is not None:
                if not cursor.set_range(start_key):
                    return
            for key, value in cursor:
                i += 1
                if end_key is not None and key >= end_key:
                    break
                if nshards > 1 and (i % nshards) != shard:
                    continue

                metric = self._accessor.get_metric(key)
                expected_value = self.__get_value(metric) if metric else None
                if value != expected_value:
                    logging.warning(
                        "Removing invalid key '%s': expected: %s cached: %s" % (
                            key, expected_value, value))
                    txn.delete(key=key)
