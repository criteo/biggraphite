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
        self.__metric_to_metadata_db = None
        self.__path = os_path.join(path, "biggraphite", "cache", "version0")

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
        self.__metric_to_metadata_db = self.__env.open_db("metric_to_meta")

    def close(self):
        """Free resources allocated by open().

        Safe to call multiple time.
        """
        if self.__env:
            self.__env.close()
            self.__env = None

    def create_metric(self, metadata):
        """Create a metric definition from a MetricMetadata.

        Args:
          metadata: The metric definition.
        """
        self.__accessor.create_metric(metadata)
        self._cache(metadata)

    def get_metric(self, metric_name):
        """Return a MetricMetadata for this metric_name, None if no such metric."""
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            metadata_str = txn.get(metric_name)
        if metadata_str:
            # on disk cache hit
            self.hit_count += 1
            with self.__json_cache_lock:
                metadata = self.__json_cache.get(metadata_str)
                if not metadata:
                    metadata = bg_accessor.MetricMetadata.from_json(metric_name, metadata_str)
                    self.__json_cache[metadata_str] = metadata
            return metadata
        else:
            # on disk cache miss
            self.miss_count += 1
            with self.__accessor_lock:
                metadata = self.__accessor.get_metric(metric_name)
            self._cache(metadata)
            return metadata

    def _cache(self, metadata):
        """If metadata add it to the cache."""
        if not metadata:
            # Do not cache absent metrics, they will probably soon be created.
            return None
        metadata_json = metadata.as_json()
        name = metadata.name
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            txn.put(name, metadata_json, dupdata=False, overwrite=True)
