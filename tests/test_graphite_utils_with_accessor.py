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
from biggraphite import graphite_utils as bg_graphite_utils
from biggraphite import test_utils as bg_test_utils


class TestGraphiteUtilsWithAccessor(bg_test_utils.TestCaseWithAccessor):

    def test_insert_fetch(self):
        self.addCleanup(self.accessor.drop_all_metrics)
        self.accessor.insert_points('a.b', [(0, 0)])

        fetched = self.accessor.fetch_points('a.b', 0, 10, step=1)
        print(fetched)
        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        #self.assertEqual(_QUERY_RANGE, len(fetched))
        #self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        #self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        #self.assertEqual(_USEFUL_POINTS, fetched)

    def test_glob(self):
        self.addCleanup(self.accessor.drop_all_metrics)
        for name in "a", "a.b.c", "a.b.d", "x.y.c", "a.a.a":
            meta = bg_accessor.MetricMetadata(name)
            self.accessor.update_metric(meta)
        self.assertEqual(["a"], bg_graphite_utils.glob_metrics(self.accessor, "*"))
        self.assertEqual(["*.*.c"], bg_graphite_utils.glob_metrics(self.accessor, "*.*.c"))
        self.assertEqual(["a.*.a", "a.*.c", "a.*.d"], bg_graphite_utils.glob_metrics(self.accessor, "a.*.*"))
        self.assertEqual(["*.*.a", "*.*.c", "*.*.d"], bg_graphite_utils.glob_metrics(self.accessor, "*.*.*"))


if __name__ == "__main__":
    unittest.main()
