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
from biggraphite.drivers import downsampling as bg_ds


class TestMetricBuffer(unittest.TestCase):
    PRECISION = 60
    CAPACITY = 10

    def setUp(self):
        self.mb = bg_ds.MetricBuffer(self.PRECISION, self.CAPACITY)

    def test_put_simple(self):
        """Test simple put operations."""
        # add points which should fill only the 0-th slot
        self.assertEqual([], self.mb.put(0, 0))
        self.assertEqual([], self.mb.put(self.PRECISION / 2, 1))
        self.assertEqual([], self.mb.put(self.PRECISION - 1, 2))
        self.assertEqual(0, self.mb._epoch)
        self.assertEqual([2] + [None] * (self.CAPACITY - 1), self.mb._buffer)
        # adding a point at timestamp self.PRECISION should fill the 1-th slot
        self.assertEqual([], self.mb.put(self.PRECISION, 3))
        self.assertEqual([2, 3] + [None] * (self.CAPACITY - 2), self.mb._buffer)

    def test_put_past(self):
        """Test that inserting a point in the far past does nothing."""
        # set the epoch at some point in time
        timestamp = 5 * self.CAPACITY * self.PRECISION
        self.assertEqual([], self.mb.put(timestamp, 0))
        # this point in the past does not fit in the buffer, so it must be discarded
        self.assertEqual([], self.mb.put(0, -1))
        self.assertEqual([0] + [None] * (self.CAPACITY - 1), self.mb._buffer)
        self.assertEqual(timestamp // self.PRECISION, self.mb._epoch)

    def test_put_future(self):
        """Test that inserting a point in the far future keeps only this point."""
        # fill the 0-th slot to set the epoch
        self.assertEqual([], self.mb.put(0, 42))
        # this point is in the future, and it
        timestamp_future = 5 * self.CAPACITY * self.PRECISION
        self.assertEqual([(0, 42)], self.mb.put(timestamp_future, 1))
        self.assertEqual(timestamp_future // self.PRECISION, self.mb._epoch)

    def test_put_complex(self):
        """Test that put works across several epochs."""
        # fill the buffer exactly once
        for i in xrange(0, self.CAPACITY):
            self.mb.put(i * self.PRECISION, i * 100)
            self.assertEqual(i, self.mb._epoch)
        expected = [i * 100 for i in xrange(0, self.CAPACITY)]
        self.assertEqual(expected, self.mb._buffer)
        # fill all slots except the last one a second time
        for i in xrange(self.CAPACITY, 2 * self.CAPACITY - 1):
            expected = [((i - self.CAPACITY) * self.PRECISION, (i - self.CAPACITY) * 100)]
            self.assertEqual(expected, self.mb.put(i * self.PRECISION, i * 100))
            self.assertEqual(i, self.mb._epoch)
        expected = [(i * 100) for i in xrange(self.CAPACITY, 2 * self.CAPACITY - 1)] +\
                   [(self.CAPACITY - 1) * 100]
        self.assertEqual(expected, self.mb._buffer)

    def test_get(self):
        """Test that get has the correct behavior."""
        # the next statements fill the 0-the slot only
        self.mb.put(0, 1)
        self.assertEqual(self.mb.get(0), 1)
        self.mb.put(self.PRECISION - 1, 2)
        self.mb.put(self.PRECISION - 1, 3)
        self.assertEqual(self.mb.get(0), 3)
        self.assertEqual(self.mb.get(self.PRECISION - 1), 3)
        self.assertIsNone(self.mb.get(self.PRECISION))

    def test_current_points(self):
        """Add some points and check there are in the buffer."""
        self.mb.put(0, 1)
        self.mb.put(self.PRECISION - 1, 2)
        self.assertEqual([(0, 2)], self.mb.current_points())
        self.mb.put(self.PRECISION, 3)
        self.assertEqual([(0, 2), (self.PRECISION, 3)], self.mb.current_points())

    def test_pop_expired(self):
        """Test expiry of points according to capacity and precision."""
        # fill all the slots once
        for i in xrange(0, self.CAPACITY):
            self.mb.put(i * self.PRECISION, i * 100)
        # pop points strictly before (self.CAPACITY - 1) * self.PRECISION
        # these should be the points [0, 1, ..., self.CAPACITY - 2]
        expected = [(i * self.PRECISION, i * 100) for i in xrange(0, self.CAPACITY - 1)]
        self.assertEqual(expected, self.mb.pop_expired((self.CAPACITY - 1) * self.PRECISION))
        # check that the points we just popped were removed from the buffer
        expected = [None] * (self.CAPACITY - 1) + [(self.CAPACITY - 1) * 100]
        self.assertEqual(expected, self.mb._buffer)
        # check that points can't be expired/popped twice
        self.assertEqual([], self.mb.pop_expired((self.CAPACITY - 1) * self.PRECISION))
        # pop the last point, and check that it's expired only once
        expected = [((self.CAPACITY - 1) * self.PRECISION, (self.CAPACITY - 1) * 100)]
        self.assertEqual(expected, self.mb.pop_expired(self.CAPACITY * self.PRECISION))
        self.assertEqual([], self.mb.pop_expired(self.CAPACITY * self.PRECISION))


class TestStageAggregate(unittest.TestCase):
    PRECISION = 60

    def setUp(self):
        self.sa = bg_ds.StageAggregate(self.PRECISION, bg_accessor.Aggregator.average)

    def _test_compute(self, points, expected):
        """Call compute & check that state is unchanged."""
        self.assertEqual(expected, self.sa.compute(points))

        self.assertIsNone(self.sa._epoch)
        self.assertEqual(0, self.sa.count)
        self.assertIsNone(self.sa.value)

    def test_compute(self):
        """Test aggregation operations."""
        points = []
        expected = []
        self._test_compute(points, expected)

        points = [(0, 2)]
        expected = [(0, 2, 1, self.PRECISION)]
        self._test_compute(points, expected)

        points = [(0, 2), (1, 4)]
        expected = [(0, 3, 2, self.PRECISION)]
        self._test_compute(points, expected)

        points = [(0, 2), (1, 8), (self.PRECISION, -1)]
        expected = [(0, 5, 2, self.PRECISION),
                    (self.PRECISION, -1, 1, self.PRECISION)]
        self._test_compute(points, expected)

        points = [(0, 2),
                  (1, 18),
                  (self.PRECISION, -1),
                  (self.PRECISION * 3, 5)]
        expected = [(0, 10, 2, self.PRECISION),
                    (self.PRECISION, -1, 1, self.PRECISION),
                    (3 * self.PRECISION, 5, 1, self.PRECISION)]
        self._test_compute(points, expected)

    def test_update(self):
        """Test aggregation + update operations."""
        points = [(0, 2)]
        expected = [(0, 2, 1, self.PRECISION)]
        self.assertEqual(expected, self.sa.update(points))
        self.assertEqual(self.sa._epoch, 0)
        self.assertEqual(self.sa.count, 1)
        self.assertEqual(self.sa.value, 2)

        points = []
        expected = [(0, 2, 1, self.PRECISION)]
        self.assertEqual(expected, self.sa.update(points))
        self.assertEqual(self.sa._epoch, 0)
        self.assertEqual(self.sa.count, 1)
        self.assertEqual(self.sa.value, 2)

        points = [(1, 42)]
        expected = [(0, 22, 2, self.PRECISION)]
        self.assertEqual(expected, self.sa.update(points))
        self.assertEqual(self.sa._epoch, 0)
        self.assertEqual(self.sa.count, 2)
        self.assertEqual(self.sa.value, 22)

        points = [(self.PRECISION, -1),
                  (self.PRECISION * 5, -5)]
        expected = [(0, 22, 2, self.PRECISION),
                    (self.PRECISION, -1, 1, self.PRECISION),
                    (5 * self.PRECISION, -5, 1, self.PRECISION)]
        self.assertEqual(expected, self.sa.update(points))
        self.assertEqual(self.sa._epoch, 5)
        self.assertEqual(self.sa.count, 1)
        self.assertEqual(self.sa.value, -5)


class TestMetricAggregates(unittest.TestCase):
    PRECISION = 10

    def setUp(self):
        aggregator = bg_accessor.Aggregator.average
        precisions = tuple([self.PRECISION ** i for i in [1, 2, 3]])
        retention_string = "5*%ds:10*%ds:15*%ds" % (precisions)
        retention = bg_accessor.Retention.from_string(retention_string)
        metric_metadata = bg_accessor.MetricMetadata(aggregator=aggregator, retention=retention)
        self.ma = bg_ds.MetricAggregates(metric_metadata)

    def test_compute(self):
        """Test aggregation operations."""
        points = []
        expected = [[], [], []]
        result = self.ma.compute(points)
        self.assertEqual(result, expected)

        points = [(0, 1)]
        expected = [[(0, 1, 1, self.PRECISION)],
                    [(0, 1, 1, self.PRECISION ** 2)],
                    [(0, 1, 1, self.PRECISION ** 3)]]
        result = self.ma.compute(points)
        self.assertEqual(result, expected)

        points = [
            (0, 30),
            (self.PRECISION, 60),
            (self.PRECISION ** 2 - 1, 120),
            (self.PRECISION ** 2, 240),
            (self.PRECISION ** 3 - 1, 480),
            (self.PRECISION ** 3, 600)
        ]
        expected_stage_0 = [
            (0, 30, 1, self.PRECISION),
            (self.PRECISION, 60, 1, self.PRECISION),
            (self.PRECISION * (self.PRECISION - 1), 120, 1, self.PRECISION),
            (self.PRECISION ** 2, 240, 1, self.PRECISION),
            (self.PRECISION * (self.PRECISION ** 2 - 1), 480, 1, self.PRECISION),
            (self.PRECISION ** 3, 600, 1, self.PRECISION)
        ]
        expected_stage_1 = [
            (0, 70, 3, self.PRECISION ** 2),
            (self.PRECISION ** 2, 240, 1, self.PRECISION ** 2),
            (self.PRECISION ** 2 * (self.PRECISION - 1), 480, 1, self.PRECISION ** 2),
            (self.PRECISION ** 3, 600, 1, self.PRECISION ** 2),
        ]
        expected_stage_2 = [
            (0, 186, 5, self.PRECISION ** 3),
            (self.PRECISION ** 3, 600, 1, self.PRECISION ** 3)
        ]
        expected = [
            expected_stage_0,
            expected_stage_1,
            expected_stage_2
        ]
        result = self.ma.compute(points)
        self.assertEqual(result, expected)


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
        metric_metadata = bg_accessor.MetricMetadata(aggregator=aggregator, retention=retention)
        self.metric = bg_accessor.Metric(self.METRIC_NAME, metric_metadata)
        self.ds = bg_ds.Downsampler()

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
            (0, 1, 1, self.PRECISION),
            (0, 1, 1, self.PRECISION ** 2)
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
            (0, 1, 1, self.PRECISION),
            (self.PRECISION, 2, 1, self.PRECISION)
        ]
        expected_stage_1 = [
            (0, 3, 2, self.PRECISION ** 2)  # 3 = 1 + 2, sum of stage_0 values
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
            (0, 9, 1, self.PRECISION),
            (self.PRECISION, 10, 1, self.PRECISION),
            (self.PRECISION * self.CAPACITY, 7, 1, self.PRECISION)
        ]
        expected_stage_1 = [
            (0, 26, 3, self.PRECISION ** 2)  # 26 = 9 + 10 +7, sum of stage_0 values
        ]
        expected = expected_stage_0 + expected_stage_1
        result = self.ds.feed(self.metric, points)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
