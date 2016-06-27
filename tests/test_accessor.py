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

import mock

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils

_METRIC = bg_test_utils.make_metric("test.metric")


class TestAggregator(unittest.TestCase):

    # This does not test seralisation as TestRetention exercise that already.

    def test_aggregate(self):
        # counts = 0 causes values to be ignored
        counts = [0, 1, 1, 1, 1, 0]
        values = [-1000, 0, 1, 2, 3, 1000]
        expectations = (
            ("average", 1.5),
            ("last", 0),  # Values from most recent to oldest
            ("minimum", 0),
            ("maximum", 3),
            ("total", 6),
        )
        for name, value_expected in expectations:
            aggregator = bg_accessor.Aggregator.from_config_name(name)
            m = bg_accessor.MetricMetadata(aggregator=aggregator)
            value, count = m.aggregate(counts=counts, values=values, newest_first=True)
            self.assertEqual(value_expected, value)
            self.assertEqual(sum(counts), count)

    def test_aggregate_newest_last(self):
        aggregator = bg_accessor.Aggregator.last
        counts = [1, 1]
        values = [10, 20]
        value, count = aggregator.aggregate(counts=counts, values=values, newest_first=False)
        self.assertEqual(values[-1], value)
        self.assertEqual(sum(counts), count)

    def test_aggregate_no_values(self):
        m = bg_accessor.MetricMetadata()
        v, c = m.aggregate(counts=[], values=[])
        self.assertEqual(0, c)

    def test_aggregate_no_counts(self):
        m = bg_accessor.MetricMetadata()
        v, c = m.aggregate(counts=[0, 0], values=[42, 42])
        self.assertEqual(0, c)

    def test_config_names(self):
        self.assertEqual(
            bg_accessor.Aggregator.from_carbon_name("avg"),
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

    def test_empty(self):
        r = bg_accessor.Retention.from_string("")
        self.assertEqual((), r.stages)

    def test_operators(self):
        # Doesn't use assertEqual to make == and != are called
        r1 = self._TEST
        self.assertFalse(r1 == object())
        self.assertTrue(r1 != object())

        r2 = bg_accessor.Retention.from_string(self._TEST_STRING)
        self.assertFalse(r1 != r2)
        self.assertTrue(r1 == r2)

        r3 = bg_accessor.Retention.from_string("")
        self.assertFalse(r1 == r3)

        r4 = bg_accessor.Retention.from_string(self._TEST_STRING + ":1*86400s")
        self.assertFalse(r1 == r4)

    def test_invalid(self):
        strings = [
            "60:60s:1:1234",  # 1234 not multiple of 60
            "60:1s:15:2s",  # 60*1>15*2
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

        def async_mock(metric, timestamps_and_values, on_done):
            on_done(CustomException("fake failure"))
            return mock.DEFAULT

        with mock.patch.object(self.accessor, "insert_points_async", side_effect=async_mock):
            self.assertRaises(
                CustomException,
                self.accessor.insert_points, _METRIC, (0, 42),
            )


if __name__ == "__main__":
    unittest.main()
