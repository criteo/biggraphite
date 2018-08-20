#!/usr/bin/env python
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
import uuid

from biggraphite import metric as bg_metric
from biggraphite.drivers import _downsampling as bg_ds
from tests import test_utils

test_utils.setup_logging()


class TestDownsampler(unittest.TestCase):
    METRIC_NAME_SUM = "test.metric.sum"
    METRIC_NAME_AVG = "test.metric.avg"
    PRECISION = 10
    CAPACITY = 3

    def setUp(self):
        """Set up a Downsampler, aggregating with the sum and average function."""
        capacity_precisions = (
            self.CAPACITY,
            self.PRECISION,
            self.CAPACITY,
            self.PRECISION ** 2,
        )
        retention_string = "%d*%ds:%d*%ds" % (capacity_precisions)
        retention = bg_metric.Retention.from_string(retention_string)
        self.stage_0 = retention.stages[0]
        self.stage_1 = retention.stages[1]
        uid = uuid.uuid4()
        metric_metadata = bg_metric.MetricMetadata(
            aggregator=bg_metric.Aggregator.total, retention=retention
        )
        self.metric_sum = bg_metric.Metric(self.METRIC_NAME_SUM, uid, metric_metadata)

        uid = uuid.uuid4()
        metric_metadata = bg_metric.MetricMetadata(
            aggregator=bg_metric.Aggregator.average, retention=retention
        )
        self.metric_avg = bg_metric.Metric(self.METRIC_NAME_AVG, uid, metric_metadata)
        self.ds = bg_ds.Downsampler(self.CAPACITY)

    def test_feed_simple_sum(self):
        """Test feed with few points."""
        # 1. Put value 1 at timestamp 0.
        # 2. Check that it is used in the aggregates, even though it is not expired.
        points = [(0, 1)]
        expected = [(0, 1, 1, self.stage_0), (0, 1, 1, self.stage_1)]
        result = self.ds.feed(self.metric_sum, points)
        self.assertEqual(result, expected)

        # 1. Feed no point, and check that nothing is thrown.
        points = []
        result = self.ds.feed(self.metric_sum, points)
        self.assertEqual(result, [])

        # 1. Add point with value 3 that overrides the previous point.
        # 2. Check the result takes into account the override.
        points = [(0, 3)]
        expected = [(0, 3, 1, self.stage_0), (0, 3, 1, self.stage_1)]
        result = self.ds.feed(self.metric_sum, points)
        self.assertEqual(result, expected)

        # 1. Add point with value 9 at index 1 in stage0 buffer.
        # 2. Check that the aggregates are updated using both points.
        points = [(0, 5), (self.PRECISION, 9)]  # Overrides previous point.
        expected = [
            (0, 5, 1, self.stage_0),
            (self.PRECISION, 9, 1, self.stage_0),
            (0, 14, 2, self.stage_1),
        ]
        result = self.ds.feed(self.metric_sum, points)
        self.assertEqual(result, expected)

        # 1. Feed no point, and check that nothing is thrown.
        points = []
        result = self.ds.feed(self.metric_sum, points)
        self.assertEqual(result, [])

    def test_feed_simple_avg(self):
        """Test feed with few points."""
        # 1. Put value 1 at timestamp 0.
        # 2. Check that it is used in the aggregates, even though it is not expired.
        points = [(0, 1)]
        expected = [(0, 1, 1, self.stage_0), (0, 1, 1, self.stage_1)]
        result = self.ds.feed(self.metric_avg, points)
        self.assertEqual(result, expected)

        # 1. Add point with value 9 at index 1 in stage0 buffer.
        # 2. Check that the aggregates are updated using both points.
        points = [
            (0, 5),  # Overrides previous point.
            (self.PRECISION, 9),
            (self.PRECISION ** 2 * self.CAPACITY, 10),
        ]
        expected = [
            (0, 5, 1, self.stage_0),
            (self.PRECISION, 9, 1, self.stage_0),
            (300, 10.0, 1, self.stage_0),
            (0, 14.0, 2, self.stage_1),
            (300, 10.0, 1, self.stage_1),
        ]
        result = self.ds.feed(self.metric_avg, points)
        self.assertEqual(result, expected)

    def test_feed_multiple(self):
        """Test feed with one point per minute for 30 minutes."""
        for i in range(30):
            result = self.ds.feed(self.metric_sum, [(1, i)])
            # We should generate only one metric per retention.
            self.assertEqual(len(result), 2)

        for i in range(30):
            result = self.ds.feed(self.metric_sum, [(0, i)])
            self.assertEqual(len(result), 2)

    def test_feed_extended(self):
        """Test feed with several points."""
        # 1. Add point with value 15 which expires the point at index 0.
        # 2. Check that the aggregates are updated using the three points.
        points = [
            (0, 1),  # Point at index 0.
            (1, 2),  # Overrides previous point at index 0.
            (self.PRECISION, 15),  # Point at index 1.
            # Evicts the point at index 0.
            (self.PRECISION * self.CAPACITY, 25),
            # Evicts all previous points.
            (self.PRECISION * self.CAPACITY * 2, 150),
            (self.PRECISION ** 2 * self.CAPACITY, 1500),  # Bump stage 1 epoch.
            # Replace previous point.
            (self.PRECISION ** 2 * self.CAPACITY, 1501),
        ]
        expected_stage_0 = [
            (0, 2, 1, self.stage_0),  # Point at index 0.
            (self.PRECISION, 15, 1, self.stage_0),  # Point at index 1.
            (self.PRECISION * self.CAPACITY, 25, 1, self.stage_0),
            (self.PRECISION * self.CAPACITY * 2, 150, 1, self.stage_0),
            (self.CAPACITY * self.PRECISION ** 2, 1501, 1, self.stage_0),
        ]
        expected_stage_1 = [
            # 192 = 2 + 15 + 25 + 150, sum of stage_0 values
            (0, 192, 4, self.stage_1),
            (self.CAPACITY * self.PRECISION ** 2, 1501, 1, self.stage_1),
        ]
        expected = expected_stage_0 + expected_stage_1
        result = self.ds.feed(self.metric_sum, points)
        self.assertEqual(result, expected)

    def test_out_of_order(self):
        """Test feeding points out of order."""
        points = [
            (self.PRECISION ** 2 + 1, 42),  # Overrides next point once sorted.
            (self.PRECISION ** 2, 84),
            (self.PRECISION - 1, 1),  # Overrides next point once sorted
            (self.PRECISION, 2),
            (0, -10),
        ]
        expected_stage_0 = [
            (0, 1, 1, self.stage_0),
            (self.PRECISION, 2, 1, self.stage_0),
            (self.PRECISION ** 2, 42, 1, self.stage_0),
        ]
        expected_stage_1 = [
            (0, 3, 2, self.stage_1),  # 3 = 1 + 2.
            (self.PRECISION ** 2, 42, 1, self.stage_1),
        ]
        expected = expected_stage_0 + expected_stage_1
        result = self.ds.feed(self.metric_sum, points)
        self.assertEqual(result, expected)

    def test_purge(self):
        """Test that we purge old metrics correctly."""
        points = [(1, 1)]
        self.ds.PURGE_EVERY_S = 0

        self.ds.feed(self.metric_sum, points)

        # Should no remove anything.
        self.ds.purge(now=1)
        self.assertEqual(len(self.ds._names_to_aggregates), 1)

        # Should remove everything.
        self.ds.purge(now=(self.PRECISION ** 2) * 3)
        self.assertEqual(len(self.ds._names_to_aggregates), 0)


if __name__ == "__main__":
    unittest.main()
