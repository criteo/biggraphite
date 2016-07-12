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
from biggraphite.drivers import stage_aggregator as bg_stage_aggregator


class TestStageAggregator(unittest.TestCase):
    PRECISION = 60

    def setUp(self):
        self.sa = bg_stage_aggregator.StageAggregator(self.PRECISION, bg_accessor.Aggregator.average)

    def _test_compute_aggregate(self, points, expected):
        """Call compute_aggregate & check that state is unchanged."""
        self.assertEqual(expected, self.sa.compute_aggregate(points))

        self.assertIsNone(self.sa._epoch)
        self.assertEqual(0, self.sa._count)
        self.assertIsNone(self.sa._value)

    def test_compute_aggregate(self):
        """Test aggregation operations."""
        points = []
        expected = []
        self._test_compute_aggregate(points, expected)

        points = [(0, 2)]
        expected = [(0, 1, 2)]
        self._test_compute_aggregate(points, expected)

        points = [(0, 2), (1, 4)]
        expected = [(0, 2, 3)]
        self._test_compute_aggregate(points, expected)

        points = [(0, 2), (1, 8), (self.PRECISION, -1)]
        expected = [(0, 2, 5), (self.PRECISION, 1, -1)]
        self._test_compute_aggregate(points, expected)

        points = [(0, 2), (1, 18), (self.PRECISION, -1), (self.PRECISION * 3, 5)]
        expected = [(0, 2, 10), (self.PRECISION, 1, -1), (3 * self.PRECISION, 1, 5)]
        self._test_compute_aggregate(points, expected)

    def test_update_aggregate(self):
        """Test aggregation + update operations."""
        points = [(0, 2)]
        expected = [(0, 1, 2)]
        self.assertEqual(expected, self.sa.update_aggregate(points))
        self.assertEqual(self.sa._epoch, 0)
        self.assertEqual(self.sa._count, 1)
        self.assertEqual(self.sa._value, 2)

        points = []
        expected = [(0, 1, 2)]
        self.assertEqual(expected, self.sa.update_aggregate(points))
        self.assertEqual(self.sa._epoch, 0)
        self.assertEqual(self.sa._count, 1)
        self.assertEqual(self.sa._value, 2)

        points = [(1, 42)]
        expected = [(0, 2, 22)]
        self.assertEqual(expected, self.sa.update_aggregate(points))
        self.assertEqual(self.sa._epoch, 0)
        self.assertEqual(self.sa._count, 2)
        self.assertEqual(self.sa._value, 22)

        points = [(self.PRECISION, -1), (self.PRECISION * 5, -5)]
        expected = [(0, 2, 22), (self.PRECISION, 1, -1), (5 * self.PRECISION, 1, -5)]
        self.assertEqual(expected, self.sa.update_aggregate(points))
        self.assertEqual(self.sa._epoch, 5)
        self.assertEqual(self.sa._count, 1)
        self.assertEqual(self.sa._value, -5)


class TestMetricAggregator(unittest.TestCase):
    PRECISION = 60

    def setUp(self):
        aggregator = bg_accessor.Aggregator.average
        retention_string = "10*%ds:25*%ds" % (self.PRECISION, self.PRECISION ** 2)
        retention = bg_accessor.Retention.from_string(retention_string)
        metric_metadata = bg_accessor.MetricMetadata(aggregator=aggregator, retention=retention)
        self.ma = bg_stage_aggregator.MetricAggregator(metric_metadata)

    def test_compute_aggregate(self):
        """Test aggregation operations."""
        points = []
        expected = [[], []]
        result = self.ma.compute_aggregates(points)
        self.assertEqual(result, expected)

        points = [(0, 1)]
        expected = [[(0, 1)], [(0, 1)]]
        result = self.ma.compute_aggregates(points)
        self.assertEqual(result, expected)

        points = [(0, 1), (self.PRECISION, 5)]
        expected = [[(0, 1), (self.PRECISION, 5)], [(0, 3)]]
        result = self.ma.compute_aggregates(points)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
