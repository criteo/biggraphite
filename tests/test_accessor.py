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


class TestMetricMetadata(unittest.TestCase):

    _PRECISION = 60  # Period of the most precise retention policy.
    _RETENTIONS = [(_PRECISION, 24*3600/_PRECISION)]

    def _make_metric_metadata(self, **kwargs):
        """Like bg_accessor.MetricMetadata but with different default values."""
        kwargs.setdefault("carbon_retentions", self._RETENTIONS)
        return bg_accessor.MetricMetadata(**kwargs)

    def test_carbon_aggregations(self):
        points = [0, 1, 2, 3]
        points_duration = len(points) * self._PRECISION
        expectations = (
            ('average', 1.5),
            ('last', 0),  # Points from most recent to oldest
            ('min', 0),
            ('max', 3),
            ('sum', 6),
        )
        for aggregation, value in expectations:
            m = self._make_metric_metadata(carbon_aggregation=aggregation)
            aggregate = m.carbon_aggregate_points(time_span=points_duration, points=points)
            self.assertEqual(value, aggregate)

        m = self._make_metric_metadata(carbon_aggregation="does not exist")
        self.assertRaises(bg_accessor.InvalidArgumentError,
                          m.carbon_aggregate_points, time_span=points_duration, points=points)

    def test_carbon_aggregations_no_points(self):
        m = self._make_metric_metadata()
        self.assertIsNone(m.carbon_aggregate_points(time_span=1.0, points=[]))

    def test_carbon_xfilesfactor(self):
        points = range(10)
        points_duration = len(points) * self._PRECISION

        m_explicit = self._make_metric_metadata(carbon_xfilesfactor=0.3)
        self.assertFalse(m_explicit.carbon_aggregate_points(points_duration, points=points[:2]))
        self.assertTrue(m_explicit.carbon_aggregate_points(points_duration, points=points[:3]))

        m_default = bg_accessor.MetricMetadata()
        self.assertEqual(0.5, m_default.carbon_xfilesfactor)

    def test_setattr(self):
        m = self._make_metric_metadata()
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
