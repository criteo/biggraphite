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

import math
import unittest

import mock

from biggraphite import accessor as bg_accessor
from biggraphite import metric as bg_metric
from tests import test_utils as bg_test_utils

_METRIC = bg_test_utils.make_metric("test.metric")
_NAN = float("nan")


class TestAggregator(unittest.TestCase):

    # This does not test seralisation as TestRetention exercise that already.

    def test_aggregate(self):
        values = [_NAN, 0, 1, _NAN, 2, 3, _NAN]
        counts = [0, 1, 1, 0, 2, 1, 0]
        expectations = (
            ("average", 1.2),
            ("last", 0),  # Values from most recent to oldest
            ("minimum", 0),
            ("maximum", 3),
            ("total", 6),
        )
        for name, value_expected in expectations:
            aggregator = bg_metric.Aggregator.from_config_name(name)
            aggregated = aggregator.aggregate(
                values=values, counts=counts, newest_first=True
            )
            self.assertEqual(value_expected, aggregated)

    def test_aggregate_nan(self):
        values = [_NAN, _NAN]
        counts = [0, 0]
        for aggregator in bg_metric.Aggregator:
            aggregated = aggregator.aggregate(
                values=values, counts=counts, newest_first=True
            )
            self.assertTrue(math.isnan(aggregated), aggregator)

    def test_aggregate_newest_last(self):
        aggregator = bg_metric.Aggregator.last
        values = [10, 20, _NAN]
        aggregated = aggregator.aggregate(values=values, newest_first=False)
        self.assertEqual(20, aggregated)

    def test_aggregate_no_values(self):
        aggregator = bg_metric.Aggregator.last
        aggregated = aggregator.aggregate(values=[], newest_first=False)
        self.assertTrue(math.isnan(aggregated))

    def test_merge(self):
        values = [10, 20]
        count = [1, 1]
        expectations = (
            ("average", (30, 2)),
            ("last", (20, 2)),
            ("minimum", (10, 2)),
            ("maximum", (20, 2)),
            ("total", (30, 2)),
        )
        for name, value_expected in expectations:
            aggregator = bg_metric.Aggregator.from_config_name(name)
            merged = aggregator.merge(values, count)
            self.assertEqual(value_expected, merged)

    def test_merge_nans(self):
        aggregator = bg_metric.Aggregator.average
        self.assertEqual((10, 1), aggregator.merge([10, _NAN]))
        self.assertEqual((10, 1), aggregator.merge([_NAN, 10]))

    def test_config_names(self):
        self.assertEqual(
            bg_metric.Aggregator.from_carbon_name("average"),
            bg_metric.Aggregator.average,
        )
        self.assertIsNone(bg_metric.Aggregator.from_carbon_name(""))


class TestStage(unittest.TestCase):
    # A lot is tested through TestRetention

    def test_operators(self):
        # Doesn't use assertEqual to make == and != are called
        s1 = bg_metric.Stage(points=24, precision=3600)
        self.assertTrue(s1 != object())
        self.assertFalse(s1 == object())

        s2 = bg_metric.Stage.from_string("24*3600s")
        self.assertTrue(s1 == s2)
        self.assertFalse(s1 != s2)

        s3 = bg_metric.Stage.from_string("12*3600s")
        self.assertFalse(s1 == s3)
        self.assertTrue(s1 != s3)

    def test_stage0(self):
        s1 = bg_metric.Stage(points=24, precision=3600, stage0=True)
        s2 = bg_metric.Stage.from_string("24*3600s_0")
        s3 = bg_metric.Stage.from_string("24*3600s_aggr")

        self.assertTrue(s1.stage0)
        self.assertTrue(s2.stage0)
        self.assertFalse(s3.stage0)
        self.assertTrue(s1 == s2)
        self.assertFalse(s1 != s2)


class TestRetention(unittest.TestCase):

    _TEST_STRING = "60*60s:24*3600s"
    _TEST = bg_metric.Retention.from_string(_TEST_STRING)

    def test_simple(self):
        for i, points, precision in ((0, 60, 60), (1, 24, 3600)):
            self.assertEqual(precision, self._TEST.stages[i].precision)
            self.assertEqual(points, self._TEST.stages[i].points)
        self.assertEqual(self._TEST_STRING, self._TEST.as_string)

    def test_operators(self):
        # Doesn't use assertEqual to make == and != are called
        r1 = self._TEST
        self.assertFalse(r1 == object())
        self.assertTrue(r1 != object())

        r2 = bg_metric.Retention.from_string(self._TEST_STRING)
        self.assertFalse(r1 != r2)
        self.assertTrue(r1 == r2)

        r3 = bg_metric.Retention.from_string(self._TEST_STRING + ":2*86400s")
        self.assertFalse(r1 == r3)

    def test_invalid(self):
        strings = [
            "",  # Empty
            "60*60s:1*1234s",  # 1234 not multiple of 60
            "60*1s:15*2s",  # 60*1>15*2
        ]
        for s in strings:
            self.assertRaises(
                bg_metric.InvalidArgumentError, bg_metric.Retention.from_string, s
            )

    def test_align_time_window(self):
        retention = self._TEST

        self.assertNotEqual(
            retention.align_time_window(0, 0, 0),
            (0, 0, bg_metric.Stage(precision=60, points=60)),
        )
        stage0 = bg_metric.Stage(precision=60, points=60, stage0=True)
        self.assertEqual(retention.align_time_window(0, 0, 0), (0, 0, stage0))
        self.assertEqual(retention.align_time_window(60, 120, 1200), (60, 120, stage0))
        self.assertEqual(retention.align_time_window(61, 119, 1200), (60, 120, stage0))
        self.assertEqual(retention.align_time_window(59, 121, 1200), (0, 180, stage0))
        self.assertEqual(
            retention.align_time_window(59, 3601, 8000),
            (0, 7200, bg_metric.Stage(precision=3600, points=24)),
        )


class TestMetricMetadata(unittest.TestCase):
    def test_setattr(self):
        m = bg_test_utils.make_metric("test")
        self.assertTrue(hasattr(m, "carbon_xfilesfactor"))
        self.assertRaises(AttributeError, setattr, m, "carbon_xfilesfactor", 0.5)


class TestAccessor(bg_test_utils.TestCaseWithFakeAccessor):
    def test_context_manager(self):
        self.accessor.shutdown()
        self.assertFalse(self.accessor.is_connected)
        with self.accessor:
            self.assertTrue(self.accessor.is_connected)
        self.assertFalse(self.accessor.is_connected)

    def test_insert_error(self):
        """Check that errors propagate from asynchronous API calls to synchronous ones."""
        class CustomException(Exception):
            pass

        def async_mock(metric, datapoints, on_done):
            on_done(CustomException("fake failure"))
            return mock.DEFAULT

        with mock.patch.object(
            self.accessor, "insert_points_async", side_effect=async_mock
        ):
            self.assertRaises(
                CustomException, self.accessor.insert_points, _METRIC, (0, 42)
            )

    def test_map(self):
        self.accessor.create_metric(_METRIC)

        def _callback(metric, done, total):
            self.assertIsNotNone(metric)
            self.assertTrue(done <= total)

        def _errback(name):
            self.assertIsNotNone(name)

        self.accessor.map(_callback, errback=_errback)


class TestPointGrouper(unittest.TestCase):
    def test_basic(self):
        """Test that we can read simple stages."""
        stage = _METRIC.metadata.retention.stage0
        data = [(True, [(0, 0, 1), (0, 1, 2)]), (True, [(0, 2, 3)])]
        results = bg_accessor.PointGrouper(_METRIC, 0, 3600, stage, data)
        expected_results = [(0.0, 1), (1.0, 2), (2.0, 3)]
        self.assertEqual(list(results), expected_results)

    def test_aggregating(self):
        """Test that we can aggregate a stage."""
        stage0 = _METRIC.metadata.retention.stage0
        stage1 = _METRIC.metadata.retention.stages[1]
        data = [(True, [(0, 0, 1), (0, 1, 2)]), (True, [(0, 2, 3)])]
        results = bg_accessor.PointGrouper(
            _METRIC, 0, 3600, stage1, data, source_stage=stage0
        )
        expected_results = [(0.0, 2.0)]
        self.assertEqual(list(results), expected_results)

    def test_without_aggregating(self):
        """Test that we can merge instead of aggregate a stage."""
        stage0 = _METRIC.metadata.retention.stage0
        stage1 = _METRIC.metadata.retention.stages[1]
        data = [(True, [(0, 0, 1), (0, 1, 2)]), (True, [(0, 2, 3)])]
        results = bg_accessor.PointGrouper(
            _METRIC, 0, 3600, stage1, data, source_stage=stage0, aggregated=False
        )
        expected_results = [(0.0, 6.0, 3.0)]
        self.assertEqual(list(results), expected_results)

    def test_aggregated(self):
        """Test that we can read aggregated stages."""
        stage1 = _METRIC.metadata.retention.stages[1]
        replica0, replica1 = 0xFFFF, 0x0000
        data = [
            (True, [(0, 0, replica0, 1, 1), (0, 1, replica0, 2, 2)]),
            (True, [(0, 1, replica1, 2, 4)]),
        ]
        results = bg_accessor.PointGrouper(_METRIC, 0, 86000, stage1, data)
        expected_results = [(0.0, 1.0), (60.0, 0.5)]
        self.assertEqual(list(results), expected_results)


class TestReplicasAndShard(unittest.TestCase):
    def test_pack_shard(self):
        self.assertEqual(bg_accessor.pack_shard(0, 0), 0)
        self.assertEqual(bg_accessor.pack_shard(0, 1), 1)
        self.assertEqual(bg_accessor.pack_shard(1, 0), 0x4000)
        self.assertEqual(bg_accessor.pack_shard(1, 1), 0x4001)

    def test_unpack_shard(self):
        self.assertEqual(bg_accessor.unpack_shard(0), (0, 0))
        self.assertEqual(bg_accessor.unpack_shard(1), (0, 1))
        self.assertEqual(bg_accessor.unpack_shard(0x4000), (1, 0))
        self.assertEqual(bg_accessor.unpack_shard(0x4001), (1, 1))


if __name__ == "__main__":
    unittest.main()
