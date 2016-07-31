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

import math
import unittest

import mock

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils

_METRIC = bg_test_utils.make_metric("test.metric")
_NAN = float("nan")


class TestAggregator(unittest.TestCase):

    # This does not test seralisation as TestRetention exercise that already.

    def test_downsample(self):
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
            aggregator = bg_accessor.Aggregator.from_config_name(name)
            downsampled = aggregator.downsample(
                values=values, counts=counts, newest_first=True)
            self.assertEqual(value_expected, downsampled)

    def test_downsample_nan(self):
        values = [_NAN, _NAN]
        counts = [0, 0]
        for aggregator in bg_accessor.Aggregator:
            downsampled = aggregator.downsample(
                values=values, counts=counts, newest_first=True)
            self.assertTrue(math.isnan(downsampled), aggregator)

    def test_downsample_newest_last(self):
        aggregator = bg_accessor.Aggregator.last
        values = [10, 20, _NAN, ]
        downsampled = aggregator.downsample(values=values, newest_first=False)
        self.assertEqual(20, downsampled)

    def test_downsample_no_values(self):
        aggregator = bg_accessor.Aggregator.last
        downsampled = aggregator.downsample(values=[], newest_first=False)
        self.assertTrue(math.isnan(downsampled))

    def test_merge(self):
        old = 10
        old_weight = 10
        fresh = 120
        expectations = (
            ("average", 20),
            ("last", 120),
            ("minimum", 10),
            ("maximum", 120),
            ("total", 130),
        )
        for name, value_expected in expectations:
            aggregator = bg_accessor.Aggregator.from_config_name(name)
            merged = aggregator.merge(old, old_weight, fresh)
            self.assertEqual(value_expected, merged)

    def test_merge_nans(self):
        aggregator = bg_accessor.Aggregator.average
        self.assertEqual(10, aggregator.merge(old=10, old_weight=1, fresh=_NAN))
        self.assertEqual(10, aggregator.merge(old=_NAN, old_weight=1, fresh=10))

    def test_config_names(self):
        self.assertEqual(
            bg_accessor.Aggregator.from_carbon_name("average"),
            bg_accessor.Aggregator.average,
        )
        self.assertIsNone(bg_accessor.Aggregator.from_carbon_name(""))


class TestStage(unittest.TestCase):
    # A lot is tested through TestRetention

    def test_operators(self):
        # Doesn't use assertEqual to make == and != are called
        s1 = bg_accessor.Stage(points=24, precision=3600)
        self.assertTrue(s1 != object())
        self.assertFalse(s1 == object())

        s2 = bg_accessor.Stage.from_string("24*3600s")
        self.assertTrue(s1 == s2)
        self.assertFalse(s1 != s2)

        s3 = bg_accessor.Stage.from_string("12*3600s")
        self.assertFalse(s1 == s3)
        self.assertTrue(s1 != s3)


class TestRetention(unittest.TestCase):

    _TEST_STRING = "60*60s:24*3600s"
    _TEST = bg_accessor.Retention.from_string(_TEST_STRING)

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

        r2 = bg_accessor.Retention.from_string(self._TEST_STRING)
        self.assertFalse(r1 != r2)
        self.assertTrue(r1 == r2)

        r3 = bg_accessor.Retention.from_string(self._TEST_STRING + ":2*86400s")
        self.assertFalse(r1 == r3)

    def test_invalid(self):
        strings = [
            "",  # Empty
            "60*60s:1*1234s",  # 1234 not multiple of 60
            "60*1s:15*2s",  # 60*1>15*2
        ]
        for s in strings:
            self.assertRaises(bg_accessor.InvalidArgumentError,
                              bg_accessor.Retention.from_string, s)


class TestMetricMetadata(unittest.TestCase):

    def test_setattr(self):
        m = bg_test_utils.make_metric("test")
        self.assertTrue(hasattr(m, "carbon_xfilesfactor"))
        self.assertRaises(AttributeError, setattr, m, "carbon_xfilesfactor", 0.5)


class TestMetric(unittest.TestCase):

    def test_dir(self):
        metric = bg_test_utils.make_metric("a.b.c")
        self.assertIn("name", dir(metric))
        self.assertIn("carbon_xfilesfactor", dir(metric))


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

        with mock.patch.object(self.accessor, "insert_points_async", side_effect=async_mock):
            self.assertRaises(
                CustomException,
                self.accessor.insert_points, _METRIC, (0, 42),
            )


if __name__ == "__main__":
    unittest.main()
