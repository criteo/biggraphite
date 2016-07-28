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

    def test_feed_simple(self):
        """Test feed with few points."""
        # 1. Put value 1 at timestamp 0.
        # 2. Check that it is used in the aggregates, even though it is not expired.
        points = [
            (0, 1)
        ]
        expected = [
            (0, 1, 1, self.stage_0),
            (0, 1, 1, self.stage_1)
        ]
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)

        # 1. Add point with value 9 at index 1 in raw buffer.
        # 2. Check that the aggregates are updated using both points.
        points = [
            (0, 5),  # Overrides previous point.
            (self.PRECISION, 9)
        ]
        expected = [
            (0, 5, 1, self.stage_0),
            (self.PRECISION, 9, 1, self.stage_0),
            (0, 14, 2, self.stage_1)
        ]
        result = self.ds.feed(self.metric, points)
        print(self.ds._names_to_aggregates[self.METRIC_NAME]._values)
        print(result)
        self.assertEqual(result, expected)

    def test_feed_extended(self):
        """Test feed with several points."""
        # 1. Add point with value 15 which expires the point at index 0.
        # 2. Check that the aggregates are updated using the three points.
        points = [
            (0, 1),  # Point at index 0.
            (1, 2),  # Overrides previous point at index 0.
            (self.PRECISION, 15),  # Point at index 1.
            (self.PRECISION * self.CAPACITY, 25),  # Evicts the point at index 0.
            (self.PRECISION * self.CAPACITY * 2, 150),  # Evicts all previous points.
            (self.PRECISION ** 2 * self.CAPACITY, 1500),  # Bump stage 1 epoch.
            (self.PRECISION ** 2 * self.CAPACITY, 1501)  # Replace previous point.
        ]
        expected_stage_0 = [
            (0, 2, 1, self.stage_0),  # Point at index 0.
            (self.PRECISION, 15, 1, self.stage_0),  # Point at index 1.
            (self.PRECISION * self.CAPACITY, 25, 1, self.stage_0),
            (self.PRECISION * self.CAPACITY * 2, 150, 1, self.stage_0),
            (self.CAPACITY * self.PRECISION ** 2, 1501, 1, self.stage_0),
        ]
        expected_stage_1 = [
            (0, 192, 4, self.stage_1),  # 192 = 2 + 15 + 25 + 150, sum of stage_0 values
            (self.CAPACITY * self.PRECISION ** 2, 1501, 1, self.stage_1)
        ]
        expected = expected_stage_0 + expected_stage_1
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)

    def test_out_of_order(self):
        """Test feeding points out of order."""
        points = [
            (self.PRECISION ** 2 + 1, 42),  # Overrides next point once sorted.
            (self.PRECISION ** 2, 84),
            (self.PRECISION - 1, 1),  # Overrides next point once sorted
            (self.PRECISION, 2),
            (0, -10)
        ]
        expected_stage_0 = [
            (0, 1, 1, self.stage_0),
            (self.PRECISION, 2, 1, self.stage_0),
            (self.PRECISION ** 2, 42, 1, self.stage_0)
        ]
        expected_stage_1 = [
            (0, 3, 2, self.stage_1),  # 3 = 1 + 2.
            (self.PRECISION ** 2, 42, 1, self.stage_1)
        ]
        expected = expected_stage_0 + expected_stage_1
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
