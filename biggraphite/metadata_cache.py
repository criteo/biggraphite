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

The Memory cache is implemented with a simple LRU cache. It is good for low
latency when data is unlikely to be shared accross multiple processes.

The DiskCache is implemented with lmdb, an on-disk file DB that can be accessed
by multiple processes. Keys are metric names, values are json-serialised metadata.
In deployment the graphite storage dir is used as a rendez-vous point where all processes
(carbon, graphite, ...) can find the metadata.
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
import time
import random

import cachetools
import lmdb
import six

from biggraphite import accessor as bg_accessor


class Error(Exception):
    """Base class for all exceptions from this module."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


class MetadataCache(object):
    """A metadata cache."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, accessor, settings):
        """Create a new DiskCache."""
        assert accessor
        self.hit_count = 0
        self.miss_count = 0
        self._lock = threading.Lock()
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

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, typ, value, traceback):
        self.close()
        return False

    @abc.abstractmethod
    def close(self):
        """Free resources allocated by open().

        Safe to call multiple time.
        """
        pass

    def cache_has(self, metric_name):
        """Check if a metric is cached."""
        return self._cache_has(metric_name)

    def cache_set(self, metric_name, metric):
        """Insert a metric in the cache."""
        return self._cache_set(metric_name, metric)

    def make_metric(self, name, metadata):
        """Create a metric object from a name and metadata."""
        return self._accessor.make_metric(name, metadata)

    def create_metric(self, metric, metric_name=None):
        """Create a metric definition from a Metric."""
        metric_name = metric_name or metric.name
        with self._accessor_lock:
            self._accessor.create_metric(metric)
        self._cache_set(metric_name, metric)

    def has_metric(self, metric_name):
        """Check if a metric exists.

        The result is cached and can be stale. Call get_metric()
        if you want to go to the database on a miss.
        """
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

    def get_metric(self, metric_name, touch=False):
        """Return a Metric for this metric_name, None if no such metric."""
        metric, hit = self._cache_get(metric_name)
        if hit:
            self.hit_count += 1
        else:
            self.miss_count += 1
        # Check that is still doesn't exists.
        if not metric:
            with self._accessor_lock:
                metric = self._accessor.get_metric(metric_name, touch=touch)
                self._cache_set(metric_name, metric)

        return metric

    @abc.abstractmethod
    def clean(self):
        """Clean the cache from expired metrics."""
        pass

    @abc.abstractmethod
    def repair(self, start_key=None, end_key=None, shard=0, nshards=1, callback_on_progress=None):
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


class MemoryCache(MetadataCache):
    """A per-process memory cache."""

    def __init__(self, accessor, settings):
        """Initialize the memory cache."""
        super(MemoryCache, self).__init__(accessor, settings)
        self.__size = settings.get('size', 1 * 1000 * 1000)
        self.__ttl = int(settings.get('ttl', 24 * 60 * 60))

    def open(self):
        """Allocate ressources used by the cache."""
        def _timer():
            # Use a custom timer to try to spread expirations. Within one instance it
            # won't change anything but it will be better if you run multiple instances.
            return time.time() + self.__ttl * random.uniform(-0.25, 0.25)
        self.__cache = cachetools.TTLCache(
            maxsize=self.__size, ttl=self.__ttl, timer=_timer)

    def close(self):
        """Free resources allocated by open()."""
        self.__cache = None

    def _cache_has(self, metric_name):
        """Check if metric is cached."""
        with self._lock:
            return metric_name in self.__cache

    def _cache_get(self, metric_name):
        """Get metric from cache."""
        try:
            with self._lock:
                metric = self.__cache.get(metric_name, False)
        except KeyError:
            # When metrics expire, we still get a KeyError.
            metric = False
        if metric is False:
            return None, False
        else:
            return metric, True

    def _cache_set(self, metric_name, metric):
        """Put metric in the cache."""
        with self._lock:
            self.__cache[metric_name] = metric

    def clean(self):
        """Automatically cleaned by cachetools."""
        with self._lock:
            self.__cache.expire()

    def repair(self, start_key=None, end_key=None, shard=0, nshards=1, callback_on_progress=None):
        """Remove spurious entries from the cache."""
        with self._lock:
            self._repair(start_key, end_key, shard,
                         nshards, callback_on_progress)

    def _repair(self, start_key, end_key, shard, nshards, callback_on_progress):
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

            if callback_on_progress:
                done = key - start_key if start_key else key
                total = end_key - start_key if start_key and end_key else None
                callback_on_progress(done, total)


class DiskCache(MetadataCache):
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
    _EMPTY = b'nil'

    # 1G on 32 bits systems
    # 16G on 64 bits systems
    MAP_SIZE = (1024 * 1024 * 1024 * 16 if sys.maxsize >
                2**32 else 1024 * 1024 * 1024)

    def __init__(self, accessor, settings):
        """Create a new DiskCache."""
        super(DiskCache, self).__init__(accessor, settings)

        path = settings.get("path")
        assert path

        self.__env = None
        self.__path = os_path.join(path, "biggraphite", "cache", "version0")
        self.__size = settings.get("size", self.MAP_SIZE)
        self.__ttl = int(settings.get("ttl", 24 * 60 * 60))
        self.__sync = settings.get("sync", True)
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

        logging.info('Opening cache %s (ttl: %s, sync: %s)',
                     self.__path, self.__ttl, self.__sync)
        self.__env = lmdb.open(
            self.__path,
            map_size=self.__size,
            # Only one sync per transaction, system crash can undo a transaction.
            metasync=False,
            # Actually, don't sync at all.
            sync=self.__sync,
            map_async=not self.__sync,
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

        # Clean stale readers. This can be needed if a previous instance crashed or
        # was killed abruptly (HUP, TERM, KILL...) and the lock file was always used
        # typically by other UWSGI workers.
        cleaned_stale_reader = self.__env.reader_check()
        logging.info("%d stale readers cleared." % cleaned_stale_reader)

        databases = {}
        for name in self.__databases:
            databases[name] = self.__env.open_db(name.encode())
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
        encoded_metric_name = self._encode(metric_name)
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            payload = txn.get(encoded_metric_name)
            return payload is not None

    def __expired_timestamp(self, timestamp):
        """Check if timestamp is expired.

        A timestamp expires when it is older than half the TTL from now
        """
        return int(time.time()) > timestamp + self.__ttl

    def _encode(self, value):
        """Make sure we have only str (or bytes)."""
        if value is None:
            return value
        if isinstance(value, six.binary_type):
            return value
        return value.encode('utf-8')

    def _decode(self, value):
        if value is None:
            return value
        return value.decode('utf-8')

    def _cache_get(self, metric_name):
        """Return a Metric from a the cache, None if no such metric."""
        encoded_metric_name = self._encode(metric_name)
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            payload = txn.get(encoded_metric_name)

        if payload == self._EMPTY:
            return None, True

        if payload is not None:
            payload = self._decode(payload)

        if not payload:
            # cache miss
            return None, False

        # found something in the cache
        split = self.__split_payload(payload)

        if split is None:
            # invalid string => evict from cache
            with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
                txn.delete(key=encoded_metric_name)
            return None, False

        # valid value => get id and metadata string
        # TODO: optimization: id is a UUID (known length)
        id_str, metadata_str, timestamp = split
        try:
            id = uuid.UUID(id_str)
        except Exception as e:
            logging.debug(str(e))
            with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
                txn.delete(key=encoded_metric_name)
            return None, False

        # if the timestamp expired evict it in order to force
        # its recreation for the next time
        if self.__expired_timestamp(timestamp):
            with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
                txn.delete(key=encoded_metric_name)

        metadata = self.metadata_from_str(metadata_str)
        return bg_accessor.Metric(metric_name, id, metadata), True

    def _cache_set(self, metric_name, metric):
        """If metric is valid, add it to the cache.

        The metric is stored in the cache as follows:
          - its id, which is a UUID.
          - vertical bar (pipe) separator.
          - its metadata JSON representation.
        """
        encoded_metric_name = self._encode(metric_name)
        key = encoded_metric_name
        value = self.__value_from_metric(metric)
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            txn.put(key, value, dupdata=False, overwrite=True)

    def stat(self):
        """Count number of cached entries."""
        ret = {}
        ret[''] = self.__env.stat(),
        for name, database in self.__databases.items():
            with self.__env.begin(database, write=False) as txn:
                ret[name] = txn.stat(database)
        return ret

    def __value_from_strings(self, metric_id, metric_metadata):
        """Build cache value from strings.

        The cache value is a combination of:
          - id
          - _METRIC_SEPARATOR
          - metadata
          - _METRIC_SEPARATOR
          - timestamp
        """
        timestamp = str(int(time.time()))
        return self._encode(
            self._METRIC_SEPARATOR.join(
                [metric_id, metric_metadata, timestamp])
        )

    def __value_from_metric(self, metric):
        """Build cache value for a metric."""
        if not metric:
            return self._EMPTY
        return self.__value_from_strings(str(metric.id), metric.metadata.as_json())

    def __split_payload(self, payload):
        """Split the payload in components.

        Returns None if it cannot parse the payload.
        """
        if not payload:
            return None

        split = payload.split(self._METRIC_SEPARATOR, 2)
        # check for exaclt 3 fields
        if len(split) != 3:
            return None
        metric_id, metric_metadata, timestamp_str = split
        # check timestamp conversion to int
        try:
            timestamp = int(timestamp_str)
        except (TypeError, ValueError, OverflowError):
            return None
        return (metric_id, metric_metadata, timestamp)

    def clean(self):
        """Remove all expired metrics.

        Note: This will also remove metrics without a timestamp or
              whose timestamp value is not a digit.
        """
        cutoff = int(time.time()) - int(self.__ttl)
        logging.info("Cleaning cache with cutoff time %d" % cutoff)

        start_key = None
        while True:
            # Split in small transactions to avoid blocking other processes.
            with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
                with txn.cursor() as cursor:
                    if start_key is not None:
                        if not cursor.set_range(self._encode(start_key)):
                            break
                    start_key = self._clean_some(txn, cursor, cutoff)
                    if start_key is None:
                        break

    def _clean_some(self, txn, cursor, cutoff):
        """Clean a few rows of cursor and stop."""
        count = 0
        for key, value in cursor:
            count += 1
            # Limit to 100 keys per iteration.
            if count > 100:
                return key

            split = self.__split_payload(self._decode(value))
            if split is None:
                logging.warning(
                    "Removing undecodable key '%s' with value %s" % (key, value))
                txn.delete(key=key)
                continue

            _, _, timestamp = split
            if timestamp < cutoff:
                logging.warning(
                    "Removing expired key '%s' with timestamp %d" % (
                        key, timestamp))
                txn.delete(key=key)
        return None

    def repair(self, start_key=None, end_key=None, shard=0, nshards=1, callback_on_progress=None):
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
          callback_on_progress: Take 2 parameters, current key, last_key to check
        """
        i = 0
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            cursor = txn.cursor()
            if start_key is not None:
                if not cursor.set_range(start_key.encode()):
                    return
            for key, value in cursor:
                i += 1
                if end_key is not None and key >= end_key:
                    break
                if nshards > 1 and (i % nshards) != shard:
                    continue

                metric = self._accessor.get_metric(self._decode(key))
                expected_value = self.__value_from_metric(
                    metric) if metric else None

                split = self.__split_payload(self._decode(value))
                v_id, v_metadata, _ = split if split is not None else (
                    None, None, None)

                split = self.__split_payload(self._decode(expected_value))
                e_id, e_metadata, _ = split if split is not None else (
                    None, None, None)

                if v_id is None or e_id is None or (v_id, v_metadata) != (e_id, e_metadata):
                    logging.warning(
                        "Removing invalid key '%s': expected: %s cached: %s" % (
                            key, expected_value, value))
                    txn.delete(key=key)

                if callback_on_progress:
                    done = key - start_key if start_key else key
                    total = end_key - start_key if start_key and end_key else None
                    callback_on_progress(done, total)
