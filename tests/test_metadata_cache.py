#!/usr/bin/env python
# coding: utf-8
# Copyright 2018 Criteo
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
from __future__ import print_function

import unittest

from mock import Mock

from freezegun import freeze_time

from biggraphite import metadata_cache as bg_metadata_cache
from biggraphite import metric as bg_metric
from tests import test_utils as bg_test_utils

_TEST_METRIC = bg_test_utils.make_metric("a.b.c")


class CacheBaseTest(object):
    def _assert_hit_miss(self, hit, miss):
        self.assertEqual(hit, self.metadata_cache.hit_count)
        self.assertEqual(miss, self.metadata_cache.miss_count)

    def test_double_open(self):
        self.metadata_cache.open()
        self.metadata_cache.close()

    def test_hit_counts(self):
        """Check that we use the on disk cache to reduce access reads."""
        hit, miss = 0, 0
        self._assert_hit_miss(hit, miss)

        self.metadata_cache.get_metric(_TEST_METRIC.name)
        miss += 1
        self._assert_hit_miss(hit, miss)

        self.metadata_cache.create_metric(_TEST_METRIC)
        self._assert_hit_miss(hit, miss)

        self.metadata_cache.get_metric(_TEST_METRIC.name)
        hit += 1
        self._assert_hit_miss(hit, miss)

    def test_instance_cache(self):
        """Check that we do cache JSON instances."""
        name = _TEST_METRIC.name
        self.assertEqual(self.metadata_cache.has_metric(name), False)
        self.assertEqual(self.metadata_cache.get_metric(name), None)
        self.assertEqual(self.metadata_cache.cache_has(name), True)
        self.assertEqual(self.metadata_cache.has_metric(name), True)

        self.metadata_cache.create_metric(_TEST_METRIC)
        self.assertEqual(self.metadata_cache.has_metric(name), True)

        first = self.metadata_cache.get_metric(name)
        second = self.metadata_cache.get_metric(name)
        self.assertIs(first.metadata, second.metadata)

    def test_unicode(self):
        metric_name = u"a.b.testé"
        metric = bg_test_utils.make_metric(metric_name)
        self.metadata_cache.create_metric(metric)
        self.metadata_cache.get_metric(metric_name)

    def test_repair(self):
        # Add a normal metric.
        metric_name = "a.b.test"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache.create_metric(metric)
        self.metadata_cache.repair()
        self.metadata_cache.get_metric(metric_name)
        # Should be only one entry, and it was a hit.
        self.assertEqual(self.metadata_cache.hit_count, 1)
        self.assertEqual(self.metadata_cache.miss_count, 0)

        # Add a spurious metric.
        metric_name = "a.b.fake"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache._cache_set(metric_name, metric)
        self.metadata_cache.repair()
        self.metadata_cache.get_metric(metric_name)
        # repair() will remove it, and the get will produce a miss.
        self.assertEqual(self.metadata_cache.hit_count, 1)
        self.assertEqual(self.metadata_cache.miss_count, 1)

    def test_repair_shard(self):
        # Add a normal metric.
        metric_name = "a.b.test"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache.create_metric(metric)
        self.metadata_cache.repair(shard=0, nshards=2)
        self.metadata_cache.repair(shard=1, nshards=2)
        self.metadata_cache.get_metric(metric_name)
        # Should be only one entry, and it was a hit.
        self.assertEqual(self.metadata_cache.hit_count, 1)
        self.assertEqual(self.metadata_cache.miss_count, 0)

        # Add a spurious metric.
        metric_name = "a.b.fake"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache._cache_set(metric_name, metric)

        # Will not fix.
        self.metadata_cache.repair(start_key="b")
        self.metadata_cache.get_metric(metric_name)
        self.assertEqual(self.metadata_cache.hit_count, 2)

        # Will fix.
        self.metadata_cache.repair(start_key="a", shard=0, nshards=2)
        self.metadata_cache.repair(start_key="a", shard=1, nshards=2)
        self.metadata_cache.get_metric(metric_name)
        # repair() will remove it, and the get will produce a miss.
        self.assertEqual(self.metadata_cache.hit_count, 2)
        self.assertEqual(self.metadata_cache.miss_count, 1)

    def test_stats(self):
        ret = self.metadata_cache.stats()
        self.assertNotEqual(len(ret), 0)

        metric_name = "a.b.test"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache.create_metric(metric)

        ret = self.metadata_cache.stats()
        self.assertNotEqual(len(ret), 0)


class TestDiskCache(CacheBaseTest, bg_test_utils.TestCaseWithFakeAccessor):

    CACHE_CLASS = bg_metadata_cache.DiskCache

    def _get_metric_timestamp(self):
        """Get the metric timestamp directly from DiskCache."""
        with self.metadata_cache._DiskCache__env.begin(
            self.metadata_cache._DiskCache__metric_to_metadata_db, write=False
        ) as txn:
            payload = txn.get(_TEST_METRIC.name.encode()).decode()
        return self.metadata_cache._DiskCache__split_payload(payload)[-1]

    def test_timestamp_update(self):
        """Check that the timestamp is updated if older than half the TTL.

        This test will create a metric at 00:00:00 and then get the same metric
        at two different points in time to check if it the timestamp is updated
        only when older than now + the default TTL.
        """
        with freeze_time("2014-01-01 00:00:00"):
            self.metadata_cache.create_metric(_TEST_METRIC)
        timestamp_creation = self._get_metric_timestamp()

        with freeze_time("2014-01-01 11:00:00"):
            self.metadata_cache.get_metric(_TEST_METRIC.name)

        timestamp_get_within_half_ttl = self._get_metric_timestamp()
        self.assertEqual(timestamp_creation, timestamp_get_within_half_ttl)

        with freeze_time("2014-01-02 04:00:00"):
            self.assertIsNot(self.metadata_cache.get_metric(_TEST_METRIC.name), None)
            self.assertIs(self.metadata_cache.cache_has(_TEST_METRIC.name), False)

    def test_cache_clean(self):
        """Check that the cache is cleared out of metrics older than the TTL."""
        with freeze_time("2014-01-01 00:00:00"):
            old_metric = bg_test_utils.make_metric("i.am.old")
            self.metadata_cache.create_metric(old_metric)
        with freeze_time("2015-01-01 00:00:00"):
            new_metric = bg_test_utils.make_metric("i.am.new")
            self.metadata_cache.create_metric(new_metric)
        with freeze_time("2015-01-01 20:00:00"):
            self.metadata_cache.clean()

        self.assertEqual(self.metadata_cache._cache_has(new_metric.name), True)
        self.assertEqual(self.metadata_cache._cache_has(old_metric.name), False)


class TestMemoryCache(CacheBaseTest, bg_test_utils.TestCaseWithFakeAccessor):

    CACHE_CLASS = bg_metadata_cache.MemoryCache


class TestNoneCache(unittest.TestCase):

    TEST_METRIC_NAME = "foo.bar"
    TEST_METRIC = bg_metric.make_metric(
        TEST_METRIC_NAME,
        bg_metric.MetricMetadata()
    )

    def setUp(self):
        self._accessor = Mock()
        self._accessor.TYPE = 'mock'
        self._cache = bg_metadata_cache.NoneCache(self._accessor, None)

    def test_has_metric_should_always_be_true(self):
        self.assertTrue(self._cache.has_metric("foo.bar"))

    def test_has_metric_should_not_use_the_accessor(self):
        self._cache.has_metric("foo.bar")
        self.assert_empty(self._accessor.method_calls)

    def test_get_metric_should_always_use_the_accessor(self):
        self._accessor.get_metric.return_value = self.TEST_METRIC

        self._cache.get_metric(self.TEST_METRIC_NAME)

        self._accessor.get_metric.assert_called_once()
        self._accessor.get_metric.assert_called_with(self.TEST_METRIC_NAME)

    def assert_empty(self, iterable):
        self.assertEqual(0, len(iterable))


if __name__ == "__main__":
    unittest.main()
