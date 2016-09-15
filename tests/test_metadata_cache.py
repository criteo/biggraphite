#!/usr/bin/env python
# coding: utf-8
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
from __future__ import print_function

import unittest

from biggraphite import test_utils as bg_test_utils

_TEST_METRIC = bg_test_utils.make_metric("a.b.c")


class TestGraphiteUtilsInternals(bg_test_utils.TestCaseWithFakeAccessor):

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
        self.assertEquals(self.metadata_cache.has_metric(name), False)
        self.assertEquals(self.metadata_cache.get_metric(name), None)

        self.metadata_cache.create_metric(_TEST_METRIC)
        self.assertEquals(self.metadata_cache.has_metric(name), True)

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
        self.assertEquals(self.metadata_cache.hit_count, 1)
        self.assertEquals(self.metadata_cache.miss_count, 0)

        # Add a spurious metric.
        metric_name = "a.b.fake"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache._cache(metric)
        self.metadata_cache.repair()
        self.metadata_cache.get_metric(metric_name)
        # repair() will remove it, and the get will produce a miss.
        self.assertEquals(self.metadata_cache.hit_count, 1)
        self.assertEquals(self.metadata_cache.miss_count, 1)

    def test_repair_shard(self):
        # Add a normal metric.
        metric_name = "a.b.test"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache.create_metric(metric)
        self.metadata_cache.repair(shard=0, nshards=2)
        self.metadata_cache.repair(shard=1, nshards=2)
        self.metadata_cache.get_metric(metric_name)
        # Should be only one entry, and it was a hit.
        self.assertEquals(self.metadata_cache.hit_count, 1)
        self.assertEquals(self.metadata_cache.miss_count, 0)

        # Add a spurious metric.
        metric_name = "a.b.fake"
        metric = bg_test_utils.make_metric(metric_name)

        self.metadata_cache._cache(metric)

        # Will not fix.
        self.metadata_cache.repair(start_key="b")
        self.metadata_cache.get_metric(metric_name)
        self.assertEquals(self.metadata_cache.hit_count, 2)

        # Will fix.
        self.metadata_cache.repair(start_key="a", shard=0, nshards=2)
        self.metadata_cache.repair(start_key="a", shard=1, nshards=2)
        self.metadata_cache.get_metric(metric_name)
        # repair() will remove it, and the get will produce a miss.
        self.assertEquals(self.metadata_cache.hit_count, 2)
        self.assertEquals(self.metadata_cache.miss_count, 1)


if __name__ == "__main__":
    unittest.main()
