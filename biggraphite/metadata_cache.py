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
"""Exports the DiskCache for metadata.

The DiskCache is implemented with lmdb, an on-disk file format that can be accessed
by multiple processes.
In deployment the graphite storage dir is used as a rendez-vous point where all processes
(carbon, graphite, ...) can find the metadata.

Currently that cache never expires, as we don't allow for deletion.
"""

from __future__ import absolute_import
from __future__ import print_function

import os
import resource
from os import path as os_path
import threading

import lmdb

from biggraphite import accessor


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

    def __init__(self, accessor, path):
        """Create a new DiskCache."""
        self.__accessor_lock = threading.Lock()
        self.__accessor = accessor
        self.__env = None
        self.__path = os_path.join(path, "biggraphite", "cache", "version0")

    def open(self):
        """Allocate ressources used by the cache.

        Safe to call again after close() returned.
        """
        if self.__env:
            return
        max_fds = min(resource.getrlimit(resource.RLIMIT_NOFILE))
        if max_fds == resource.RLIM_INFINITY:
            max_fds = 4096
        try:
            os.makedirs(self.__path)
        except OSError:
            pass  # Directory already exists
        self.__env = lmdb.open(
            self.__path,
            # Only one sync per transaction, system crash can undo a transaction.
            metasync=False,
            # Use mmap()
            writemap=True,
            # Max number of
            max_readers=max_fds,
            # How many DBs we may create (until we increase version prefix).
            max_dbs=8,
            # Should match max number of threads
            max_spare_txns=128,
        )
        self.__metric_to_metadata_db = self.__env.open_db("metric_to_meta")

    def close(self):
        """Free ressources allocated by open().

        Safe to call multiple time.
        """
        if self.__env:
            self.__env.close()
            self.__env = None

    def create_metric(self, metadata):
        """Create a metric definition from a MetricMetadata.

        Args:
          metric_metadata: The metric definition.
        """
        self.__accessor.create_metric(metadata)
        self._cache(metadata)

    def get_metric(self, metric_name):
        """Return a MetricMetadata for this metric_name, None if no such metric."""
        with self.__env.begin(self.__metric_to_metadata_db, write=False) as txn:
            metdata_str = txn.get(metric_name)
            if metdata_str:
                # TODO(unbrice): Consider caching MetaData instances.
                return accessor.MetricMetadata.from_json(metdata_str)
        with self.__accessor_lock:
            metadata = self.__accessor.get_metric(metric_name)
        self._cache(metadata)
        return metadata

    def _cache(self, metadata):
        if not metadata:
            # Do not cache absent metrics, they will probably soon be created.
            return None
        metadata_json = metadata.as_json()
        with self.__env.begin(self.__metric_to_metadata_db, write=True) as txn:
            txn.put(metadata.name, metadata_json, dupdata=False, overwrite=True)
