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
from __future__ import print_function

import unittest

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils

_TEST_METRIC = bg_accessor.MetricMetadata("a.b.c")


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
        self.metadata_cache.create_metric(_TEST_METRIC)
        first = self.metadata_cache.get_metric(_TEST_METRIC.name)
        second = self.metadata_cache.get_metric(_TEST_METRIC.name)
        self.assertIs(first, second)


if __name__ == "__main__":
    unittest.main()
