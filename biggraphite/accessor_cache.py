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

"""Cache for the accessors."""
from __future__ import absolute_import
from __future__ import print_function

import abc
import hashlib

import cachetools


class AccessorCache(object):
    """A cache that can be given to an accessor.

    It looks like Django's cache.
    https://docs.djangoproject.com/en/1.11/topics/cache/#the-low-level-cache-api
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def set(self, key, value, timeout=None, version=None):
        """Set a key in the cache."""
        pass

    @abc.abstractmethod
    def get(self, key, default=None, version=None):
        """Get a single key."""
        pass

    def set_many(self, data, timeout=None, version=None):
        """Set a bunch of keys in the cache."""
        for key, value in data.items():
            self.set(key, value, timeout=timeout, version=version)

    def get_many(self, keys, version=None):
        """Fetch a bunch of keys from the cache.

        Args:
          keys: a list of keys.
          version: an optional version.
        """
        d = {}
        for k in keys:
            val = self.get(k, version=version)
            if val is not None:
                d[k] = val
        return d


class MemoryCache(AccessorCache):
    """A per-process memory cache."""

    def __init__(self, size, ttl):
        """Initialize the memory cache."""
        super(MemoryCache, self).__init__()
        self.__size = size
        self.__ttl = ttl
        self.__cache = cachetools.TTLCache(maxsize=self.__size, ttl=self.__ttl)

    def _make_key(self, key, version):
        return str(version) + "-" + key

    def set(self, key, value, timeout=None, version=None):
        """Set a key in the cache."""
        self.__cache[self._make_key(key, version)] = value

    def get(self, key, default=None, version=None):
        """Get a single key."""
        return self.__cache.get(self._make_key(key, version), default=default)


class DjangoCache(AccessorCache):
    """Django cache, but safe."""

    def __init__(self, django_cache):
        """Initialize the cache."""
        self.__cache = django_cache

    def _make_key(self, key):
        """Construct a clean key from a key."""
        return hashlib.md5(key).hexdigest()

    def set(self, key, value, timeout=None, version=None):
        """Set a key."""
        key = self._make_key(key)
        return self.__cache.set(key, value, timeout=timeout, version=version)

    def get(self, key, value, default=None, version=None):
        """Get a key."""
        key = self._make_key(key)
        return self.__cache.get(key, value, default=default, version=version)

    def set_many(self, data, timeout=None, version=None):
        """Set a bunch of keys in the cache."""
        new_data = {self._make_key(key): value for key, value in data.items()}
        return self.__cache.set_many(new_data, timeout=timeout, version=None)

    def get_many(self, keys, version=None):
        """Fetch a bunch of keys from the cache."""
        keymap = {self._make_key(key): key for key in keys}
        data = self.__cache.get_many(keymap.keys(), version=None)
        return {keymap[key]: value for key, value in data.items()}
