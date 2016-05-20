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
# See the License for the specific lanbg_guage governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import print_function

import unittest

#from graphite import storage

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils
from biggraphite.plugins import graphite as bg_graphite


_METRIC_NAME = "test_metric"


# TODO: Move this to test_utils.
def _make_easily_queryable_points(start, end, period):
    """Return points that aggregats easily.

    Averaging over each period gives range((end-start)/period).
    Taking last or max for each period gives [x*3 for x in range(end-start)].
    Taking min for each period gives [-1]*(end-start).
    """
    assert period % 4 == 0
    assert start % period == 0
    res = []
    for t in xrange(start, end, 4):
        current_period = (t - start) // period
        # A fourth of points are -1
        res.append((t+0, -1))
        # A fourth of points are +1
        res.append((t+1, +1))
        # A fourth of points are the start timestamp
        res.append((t+2, current_period * 3))
        # A fourth of points are missing
    return res


class TestReader(bg_test_utils.TestCaseWithFakeAccessor):

    _POINTS_START = 3600 * 24 * 10
    _POINTS_END = _POINTS_START + 3600
    _PRECISION = 60
    _RETENTION_DURATION = 24 * 3600
    _RETENTIONS = [(_PRECISION, _RETENTION_DURATION/_PRECISION)]
    _POINTS = _make_easily_queryable_points(
        start=_POINTS_START, end=_POINTS_END, period=_PRECISION,
    )

    def setUp(self):
        super(TestReader, self).setUp()
        meta = bg_accessor.MetricMetadata(
            _METRIC_NAME,
            carbon_retentions=self._RETENTIONS,
        )
        self.accessor.create_metric(meta)
        self.accessor.insert_points(_METRIC_NAME, self._POINTS)
        self.reader = bg_graphite.Reader(self.accessor, _METRIC_NAME)

    def test_fresh_read(self):
        (start, end, step), points = self.reader.fetch(
            start_time=self._POINTS_START+3,
            end_time=self._POINTS_END-3,
            now=self._POINTS_END+10,
        )
        self.assertEqual(self._PRECISION, step)
        # We expect these to have been rounded to match precision.
        self.assertEqual(self._POINTS_START, start)
        self.assertEqual(self._POINTS_END, end)

        expected_points = range((end-start)//step)
        self.assertEqual(expected_points, points)

    def test_get_intervals(self):
        # start and end are the expected results, aligned on the precision
        now_rounded = 10000000 * self._PRECISION
        now = now_rounded - 3
        res = self.reader.get_intervals(now=now)

        self.assertEqual(self._RETENTION_DURATION, res.size)
        self.assertEqual(1, len(res.intervals))
        self.assertEqual(now_rounded, res.intervals[0].end)


class FakeFindQuery(object):
    def __init__(self, pattern):
        self.pattern = pattern

class TestFinder(bg_test_utils.TestCaseWithFakeAccessor):

    def setUp(self):
        super(TestFinder, self).setUp()
        for metric in "a", "a.a", "a.b.c", "x.y":
            meta = bg_accessor.MetricMetadata(metric)
            self.accessor.create_metric(meta)
        self.finder = bg_graphite.Finder(accessor=self.accessor)


    def find_nodes(self, pattern):
        return self.finder.find_nodes(FakeFindQuery(pattern))

    def test_find_nodes_a(self):
        nodes = list(self.find_nodes("a"))
        self.assertEquals(len(nodes), 2)
        self.assertEquals(sum(n.is_leaf and n.path == "a" for n in nodes), 1)
        self.assertEquals(sum(not n.is_leaf and n.path == "a" for n in nodes), 1)

    def test_find_nodes_a_star(self):
        nodes = list(self.find_nodes("a.*"))
        self.assertEquals(len(nodes), 2)
        self.assertEquals(sum(n.is_leaf and n.path == "a.a" for n in nodes), 1)
        self.assertEquals(sum(not n.is_leaf and n.path == "a.b" for n in nodes), 1)

    def test_find_nodes_a_braces(self):
        nodes = list(self.find_nodes("*.{a,b,c,y,z}"))
        self.assertEquals(len(nodes), 3)
        self.assertEquals(sum(n.is_leaf and n.path == "a.a" for n in nodes), 1)
        self.assertEquals(sum(n.is_leaf and n.path == "x.y" for n in nodes), 1)
        self.assertEquals(sum(not n.is_leaf and n.path == "a.b" for n in nodes), 1)

    def test_find_nodes_a_range_0(self):
        nodes = list(self.find_nodes("?.[a-c]"))
        self.assertEquals(len(nodes), 2)
        self.assertEquals(sum(n.is_leaf and n.path == "a.a" for n in nodes), 1)
        self.assertEquals(sum(not n.is_leaf and n.path == "a.b" for n in nodes), 1)

    def test_find_nodes_a_range_1(self):
        nodes = list(self.find_nodes("?.[a-z]"))
        self.assertEquals(len(nodes), 3)
        self.assertEquals(sum(n.is_leaf and n.path == "a.a" for n in nodes), 1)
        self.assertEquals(sum(n.is_leaf and n.path == "x.y" for n in nodes), 1)
        self.assertEquals(sum(not n.is_leaf and n.path == "a.b" for n in nodes), 1)


if __name__ == "__main__":
    unittest.main()
