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


class TestAccessorWithCassandra(bg_test_utils.TestCaseWithAccessor):

    def fetch(self, metric, *args, **kwargs):
        """Helper to fetch points as a list."""
        # default kwargs for stage.
        if 'stage' not in kwargs:
            kwargs['stage'] = metric.retention[0]
        ret = self.accessor.fetch_points(metric, *args, **kwargs)
        self.assertTrue(hasattr(ret, "__iter__"))
        return list(ret)

    def test_fetch_empty(self):
        no_such_metric = bg_test_utils.make_metric("no.such.metric")
        self.accessor.insert_points(_METRIC, _POINTS)
        self.accessor.drop_all_metrics()
        self.assertEqual(
            len(self.fetch(no_such_metric, _POINTS_START, _POINTS_END)),
            0,
        )
        self.assertFalse(
            len(self.fetch(_METRIC, _POINTS_START, _POINTS_END)),
            0,
        )

    def test_insert_fetch(self):
        self.accessor.insert_points(_METRIC, _POINTS)
        self.addCleanup(self.accessor.drop_all_metrics)

        # TODO: Test fetch at different stages for a given metric.
        fetched = self.fetch(_METRIC, _QUERY_START, _QUERY_END)
        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        self.assertEqual(_QUERY_RANGE, len(fetched))
        self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        self.assertEqual(_USEFUL_POINTS, fetched)
        
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
            "aggregator": bg_accessor.Aggregator.last,
            "retention": bg_accessor.Retention.from_string("60*1s:60*60s"),
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
