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

import statistics

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils

_METRIC = bg_test_utils.make_metric("test.metric")

# Points test query.
_QUERY_RANGE = 3600
_QUERY_START = 1000 * _QUERY_RANGE
_QUERY_END = _QUERY_START + _QUERY_RANGE

# Points injected in the test DB, a superset of above.
_EXTRA_POINTS = 1000
_POINTS_START = _QUERY_START - _EXTRA_POINTS
_POINTS_END = _QUERY_END + _EXTRA_POINTS
_POINTS = [(t, v) for v, t in enumerate(xrange(_POINTS_START, _POINTS_END))]
_USEFUL_POINTS = _POINTS[_EXTRA_POINTS:-_EXTRA_POINTS]
assert _QUERY_RANGE == len(_USEFUL_POINTS)


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


class TestMetric(unittest.TestCase):

    def test_dir(self):
        metric = bg_test_utils.make_metric("a.b.c")
        self.assertIn("name", dir(metric))
        self.assertIn("carbon_xfilesfactor", dir(metric))


class TestAccessorWithCassandra(bg_test_utils.TestCaseWithAccessor):

    def setUp(self):
        """Create a new Accessor in self.acessor."""
        super(TestAccessorWithCassandra, self).setUp()
        self.accessor.connect()

    def test_context_manager(self):
        self.accessor.shutdown()
        self.assertFalse(self.accessor.is_connected)
        with self.accessor:
            self.assertTrue(self.accessor.is_connected)
        self.assertFalse(self.accessor.is_connected)

    def test_fetch_empty(self):
        no_such_metric = bg_test_utils.make_metric("no.such.metric")
        self.accessor.insert_points(_METRIC, _POINTS)
        self.accessor.drop_all_metrics()
        self.assertFalse(
            self.accessor.fetch_points(no_such_metric, _POINTS_START, _POINTS_END, step=1))
        self.assertFalse(
            self.accessor.fetch_points(_METRIC, _POINTS_START, _POINTS_END, step=1))

    def test_insert_fetch(self):
        self.accessor.insert_points(_METRIC, _POINTS)
        self.addCleanup(self.accessor.drop_all_metrics)

        fetched = self.accessor.fetch_points(_METRIC, _QUERY_START, _QUERY_END, step=1)
        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        self.assertEqual(_QUERY_RANGE, len(fetched))
        self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        self.assertEqual(_USEFUL_POINTS, fetched)

    def test_fetch_lower_res(self):
        self.accessor.insert_points(_METRIC, _POINTS)
        self.addCleanup(self.accessor.drop_all_metrics)

        fetched_tenth = self.accessor.fetch_points(
            _METRIC, _QUERY_START, _QUERY_END, step=_QUERY_RANGE / 10)
        self.assertEqual(10, len(fetched_tenth))

        fetched_median = self.accessor.fetch_points(
            _METRIC, _QUERY_START, _QUERY_END, step=_QUERY_RANGE)
        self.assertEqual(1, len(fetched_median))
        median = statistics.median(v for t, v in _USEFUL_POINTS)
        self.assertEqual(median, fetched_median[0][1])

    @staticmethod
    def _remove_after_dot(string):
        if "." not in string:
            return string
        return string[:string.rindex(".")]

    def test_glob_metrics(self):
        for name in "a", "a.a", "a.b", "a.a.a", "x.y.z":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)

        def assert_find(glob, expected_matches):
            # Check we can find the matches of a glob
            self.assertEqual(expected_matches, self.accessor.glob_metric_names(glob))

        assert_find("a.a", ["a.a"])  # Test exact match
        assert_find("A", [])  # Test case mismatch

        # Test various lengths
        assert_find("*", ["a"])
        assert_find("*.*", ["a.a", "a.b"])
        assert_find("*.*.*", ["a.a.a", "x.y.z"])

        self.accessor.drop_all_metrics()
        assert_find("*", [])

    def test_glob_directories(self):
        for name in "a", "a.b", "x.y.z":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)

        def assert_find(glob, expected_matches):
            # Check we can find the matches of a glob
            self.assertEqual(expected_matches, self.accessor.glob_directory_names(glob))

        assert_find("x.y", ["x.y"])  # Test exact match
        assert_find("A", [])  # Test case mismatch

        # Test various depths
        assert_find("*", ["a", "x"])
        assert_find("*.*", ["x.y"])
        assert_find("*.*.*", [])

        self.accessor.drop_all_metrics()
        assert_find("*", [])

    def test_create_metrics(self):
        meta_dict = {
            "carbon_aggregation": "last",
            "carbon_retentions": [[1, 60], [60, 3600]],
            "carbon_xfilesfactor": 0.3,
        }
        metric = bg_test_utils.make_metric("a.b.c.d.e.f", **meta_dict)

        self.accessor.create_metric(metric)
        metric_again = self.accessor.get_metric(metric.name)
        self.assertEqual(metric.name, metric_again.name)
        for k, v in meta_dict.iteritems():
            self.assertEqual(v, getattr(metric_again.metadata, k))


if __name__ == "__main__":
    unittest.main()
