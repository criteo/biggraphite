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
from biggraphite.drivers import _downsampling as bg_ds


class TestDownsampler(unittest.TestCase):
    METRIC_NAME = "test.metric"
    PRECISION = 10
    CAPACITY = 3

    def setUp(self):
        """Set up a Downsampler, aggregating with the sum function."""
        aggregator = bg_accessor.Aggregator.total
        capacity_precisions = (self.CAPACITY, self.PRECISION,
                               self.CAPACITY, self.PRECISION ** 2)
        retention_string = "%d*%ds:%d*%ds" % (capacity_precisions)
        retention = bg_accessor.Retention.from_string(retention_string)
        self.stage_0 = retention.stages[0]
        self.stage_1 = retention.stages[1]
        metric_metadata = bg_accessor.MetricMetadata(aggregator=aggregator, retention=retention)
        self.metric = bg_accessor.Metric(self.METRIC_NAME, metric_metadata)
        self.ds = bg_ds.Downsampler(self.CAPACITY)

    def test_simple(self):
        """Simple test with few points."""
        # 1. Put value 1 at timestamp 0.
        # 2. Evict this value with a large timestamp that fits in the buffer.
        # 3. Check that only the first point is used in the computation.
        points = [
            (0, 1),
            (self.PRECISION * self.CAPACITY, 55555),
        ]
        expected = [
            (0, 1, 1, self.stage_0),
            (0, 1, 1, self.stage_1)
        ]
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)

        # 1. Reset point with value 55555 (this should not evict anything).
        # 2. Check that the aggregates are still the same.
        points = [(self.PRECISION * self.CAPACITY, -1)]
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)

    def test_out_of_order(self):
        """Test feeding points out of order."""
        points = [
            (self.PRECISION ** 2 + 1, 42),  # not evicted
            (self.PRECISION ** 2, 84),      # not evicted
            (self.PRECISION - 1, 1),        # overrides (0, -1) once sorted
            (self.PRECISION, 2),
            (0, -1)
        ]
        expected_stage_0 = [
            (0, 1, 1, self.stage_0),
            (self.PRECISION, 2, 1, self.stage_0)
        ]
        expected_stage_1 = [
            (0, 3, 2, self.stage_1)  # 3 = 1 + 2, sum of stage_0 values
        ]
        expected = expected_stage_0 + expected_stage_1
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)

    def test_extended(self):
        """Check with several points."""
        # Since we have some capacity in the buffer, the points with the largest
        # timestamps should not be evicted
        points = [
            (0, 1),
            (self.PRECISION - 1, 9),        # replaces previous point
            (self.PRECISION, 10),
            (self.PRECISION * self.CAPACITY, 7),
            (self.PRECISION ** 2 - 1, 20),  # not evicted
            (self.PRECISION ** 2, 50)       # not evicted
        ]
        expected_stage_0 = [
            (0, 9, 1, self.stage_0),
            (self.PRECISION, 10, 1, self.stage_0),
            (self.PRECISION * self.CAPACITY, 7, 1, self.stage_0)
        ]
        expected_stage_1 = [
            (0, 26, 3, self.stage_1)  # 26 = 9 + 10 +7, sum of stage_0 values
        ]
        expected = expected_stage_0 + expected_stage_1
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
