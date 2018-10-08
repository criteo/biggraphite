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
import logging
import os
import random
import threading
import time
import uuid
from os import path as os_path

import cachetools
import prometheus_client
from diskcache import Cache as disk_cache

from biggraphite import metric as bg_metric

METRICS_LABELS = ["type", "name"]

CACHE_SIZE = prometheus_client.Gauge("bg_cache_size", "Current size", METRICS_LABELS)
CACHE_MAX_SIZE = prometheus_client.Gauge("bg_cache_maxsize", "Max size", METRICS_LABELS)
CACHE_HITS = prometheus_client.Counter(
    "bg_cache_hits_total", "Cache hits", METRICS_LABELS
)
CACHE_MISSES = prometheus_client.Counter(
    "bg_cache_misses_total", "Cache misses", METRICS_LABELS
)


class Error(Exception):
    """Base class for all exceptions from this module."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


class MetadataCache(object):
    """A metadata cache."""

    __metaclass__ = abc.ABCMeta
    TYPE = "abstract"

    def __init__(self, accessor, settings, name=None):
        """Create a new DiskCache."""
        assert accessor
        self._lock = threading.Lock()
        self._accessor_lock = threading.Lock()
        self._accessor = accessor
        # _json_cache associates unparsed json to metadata instances.
        # The idea is that there are very few configs in use in a given
        # cluster so the few same strings will show up over and over.
        self._json_cache_lock = threading.Lock()
        self._json_cache = {}

        if name is None:
            name = str(hash(self))
        self.name = name

        self._size = CACHE_SIZE.labels(self.TYPE, name)
        self._size.set_function(lambda: self.stats()["size"])
        self._max_size = CACHE_MAX_SIZE.labels(self.TYPE, name)
        self._hits = CACHE_HITS.labels(self.TYPE, name)
        self._misses = CACHE_MISSES.labels(self.TYPE, name)

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

    @property
    def hit_count(self):
        """Hit counter."""
        return self._hits._value.get()

    @property
    def miss_count(self):
        """Miss counter."""
        return self._misses._value.get()

    def cache_has(self, metric_name):
        """Check if a metric is cached."""
        hit = self._cache_has(metric_name)
        if hit:
            self._hits.inc()
        else:
            self._misses.inc()
        return hit

    def cache_set(self, metric_name, metric):
        """Insert a metric in the cache."""
        return self._cache_set(metric_name, metric)

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

    def get_metric(self, metric_name):
        """Return a Metric for this metric_name, None if no such metric."""
        metric, hit = self._cache_get(metric_name)
        if hit:
            self._hits.inc()
        else:
            self._misses.inc()
        # Check that is still doesn't exists.
        if not metric:
            with self._accessor_lock:
                metric = self._accessor.get_metric(metric_name)
                self._cache_set(metric_name, metric)

        return metric

    @abc.abstractmethod
    def clean(self):
        """Clean the cache from expired metrics."""
        pass

    @abc.abstractmethod
    def repair(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
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
                metadata = bg_metric.MetricMetadata.from_json(metadata_str)
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

    @abc.abstractmethod
    def stats(self):
        """Current stats about the cache the cache."""
        return {"size": 0, "maxsize": 0, "hits": 0, "miss": 0}


class MemoryCache(MetadataCache):
    """A per-process memory cache."""

    TYPE = "memory"

    def __init__(self, accessor, settings, name=None):
        """Initialize the memory cache."""
        super(MemoryCache, self).__init__(accessor, settings, name)
        self.__size = settings.get("size", 1 * 1000 * 1000)
        self.__ttl = int(settings.get("ttl", 24 * 60 * 60))
        self._max_size.set(self.__size)

    def open(self):
        """Allocate ressources used by the cache."""
        super(MemoryCache, self).open()

        def _timer():
            # Use a custom timer to try to spread expirations. Within one instance it
            # won't change anything but it will be better if you run multiple instances.
            return time.time() + self.__ttl * random.uniform(-0.25, 0.25)

        self.__cache = cachetools.TTLCache(
            maxsize=self.__size, ttl=self.__ttl, timer=_timer
        )

    def close(self):
        """Free resources allocated by open()."""
        super(MemoryCache, self).close()
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

    def repair(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """Remove spurious entries from the cache."""
        with self._lock:
            self._repair(start_key, end_key, shard, nshards, callback_on_progress)

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
                    "Removing invalid key '%s': expected: %s cached: %s"
                    % (key, expected_metric, metric)
                )
                del self.__cache[key]

            if callback_on_progress:
                done = key - start_key if start_key else key
                total = end_key - start_key if start_key and end_key else None
                callback_on_progress(done, total)

    def stats(self):
        """Current stats about the cache the cache."""
        return {
            "size": self.__cache.currsize,
            "maxsize": self.__cache.maxsize,
            "hits": self._hits._value.get(),
            "miss": self._misses._value.get(),
        }


class DiskCache(MetadataCache):
    """A metadata cache that can be shared between processes trusting each other.

    open() and close() are the only thread unsafe methods.
    See module-level comments for the design.
    """

    TYPE = "disk"

    def __init__(self, accessor, settings, name=None):
        """Create a new DiskCache."""
        super(DiskCache, self).__init__(accessor, settings, name)

        path = settings.get("path")
        assert path

        self.__cache = None
        self.__path = os_path.join(path, "biggraphite", "cache", "version0")
        self.__ttl = int(settings.get("ttl", 24 * 60 * 60))

    def open(self):
        """Allocate ressources used by the cache.

        Safe to call again after close() returned.
        """
        super(DiskCache, self).open()
        if self.__cache:
            return
        try:
            os.makedirs(self.__path)
        except OSError:
            pass  # Directory already exists

        logging.info(
            "Opening cache %s (ttl: %s)", self.__path, self.__ttl
        )
        self.__cache = disk_cache(self.__path)

    def close(self):
        """Free resources allocated by open().

        Safe to call multiple time.
        """
        if self.__cache:
            self.__cache.close()
            self.__cache = None
        super(DiskCache, self).close()

    def __serialize(self, data):
        if data is None:
            return data
        return data.as_string_dict()

    def __deserialize(self, data):
        if data is None:
            return data
        metadata = bg_metric.MetricMetadata.from_string_dict(data['metadata'])
        metric = bg_metric.Metric(data['name'], uuid.UUID(str(data['id'])), metadata,
                                  created_on=data['created_on'],
                                  updated_on=data['updated_on'],
                                  read_on=data['read_on'])
        return metric

    def _cache_has(self, metric_name):
        """Check if metric is cached."""
        metric = self.__deserialize(
            self.__cache.get(metric_name, default=None))
        return metric is not None

    def _cache_get(self, metric_name):
        """Return a Metric from a the cache, None if no such metric."""
        metric = self.__deserialize(
            self.__cache.get(metric_name, default=None))
        if metric is None:
            return None, False
        else:
            return metric, True

    def _cache_set(self, metric_name, metric):
        """Add metric to the cache."""
        return self.__cache.set(metric_name,
                                self.__serialize(metric),
                                expire=self.__ttl)

    def stats(self):
        """Count number of cached entries."""
        ret = super(DiskCache, self).stats()

        return ret

    def clean(self):
        """Remove is not handled by us."""
        pass

    def repair(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """Repair is not handled by us."""
        pass


class NoneCache(MetadataCache):
    """Dummy metadata cache used when no cache is required."""

    def __init__(self, accessor, settings, name=None):
        """Create a new NoneCache."""
        super(NoneCache, self).__init__(accessor, settings, name)
        assert accessor
        self._accessor = accessor

    def open(self):
        """No resource has to be allocated for this cache. Does nothing."""
        super(NoneCache, self).open()

    def close(self):
        """No resource have to be freed for this cache. Does nothing."""
        super(NoneCache, self).close()

    def clean(self):
        """Cleaning this cache is useless. Does nothing."""
        super(NoneCache, self).clean()

    def repair(self, start_key=None, end_key=None, shard=0, nshards=1, callback_on_progress=None):
        """Repairing this cache is useless. Does nothing."""
        super(NoneCache, self).repair()

    def _cache_has(self, metric_name):
        return True

    def _cache_get(self, metric_name):
        return self._accessor.get_metric(metric_name), True

    def _cache_set(self, metric_name, metric):
        pass

    def stats(self):
        """Stats are not computed for this cache."""
        return super(NoneCache, self).stats()
