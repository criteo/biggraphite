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

from biggraphite import test_utils as bg_test_utils
from biggraphite import accessor as bg_accessor

# This needs to run before we import the plugin.
bg_test_utils.prepare_graphite()

from biggraphite.plugins import graphite as bg_graphite  # noqa
from graphite import readers  # noqa

_METRIC_NAME = "test_metric"


# TODO: Move this to test_utils.
def _make_easily_queryable_points(start, end, period):
    """Return points that aggregates easily.

    Averaging over each period gives range((end-start)/period).
    Taking last or max for each period gives [x*3 for x in range(end-start)].
    Taking min for each period gives [-1]*(end-start).
    """
    assert period % 4 == 0
    assert start % period == 0
    subperiod = period / 4
    res = []
    for t in xrange(start, end, period):
        current_period = (t - start) // period
        # A fourth of points are -1
        res.append((t+0*subperiod, -1))
        # A fourth of points are +1
        res.append((t+1*subperiod, +1))
        # A fourth of points are the start timestamp
        res.append((t+2*subperiod, current_period * 3))
        # A fourth of points are missing

    return res


class TestReader(bg_test_utils.TestCaseWithFakeAccessor):

    _POINTS_START = 3600 * 24 * 10
    _POINTS_END = _POINTS_START + 3600
    _RETENTION = bg_accessor.Retention.from_string("20*15s:1440*60s:48*3600s")
    _POINTS = _make_easily_queryable_points(
        start=_POINTS_START, end=_POINTS_END, period=_RETENTION[1].precision,
    )
    _METRIC = bg_test_utils.make_metric(_METRIC_NAME, retention=_RETENTION)

    def setUp(self):
        super(TestReader, self).setUp()
        self.accessor.connect()
        self.accessor.create_metric(self._METRIC)
        self.accessor.insert_points(self._METRIC, self._POINTS)
        self.accessor.flush()
        self.reader = bg_graphite.Reader(self.accessor, self.metadata_cache, _METRIC_NAME)

    def fetch(self, *args, **kwargs):
        result = self.reader.fetch(*args, **kwargs)
        # Readers can return a list or an object.
        if isinstance(result, readers.FetchInProgress):
            result = result.waitForResults()
        return result

    def test_fresh_read(self):
        (start, end, step), points = self.fetch(
            start_time=self._POINTS_START+3,
            end_time=self._POINTS_END-3,
            now=self._POINTS_END+10,
        )
        self.assertEqual(self._RETENTION[1].precision, step)
        # We expect these to have been rounded to match precision.
        self.assertEqual(self._POINTS_START, start)
        self.assertEqual(self._POINTS_END, end)

        expected_points = range((end-start)//step)
        self.assertEqual(expected_points, points)

    def test_get_intervals(self):
        # start and end are the expected results, aligned on the precision
        now_rounded = 10000000 * self._RETENTION[2].precision
        now = now_rounded - 3
        res = self.reader.get_intervals(now=now)

        self.assertEqual(self._RETENTION.duration, res.size)
        self.assertEqual(1, len(res.intervals))
        self.assertEqual(now_rounded, res.intervals[0].end)


class FakeFindQuery(object):
    """"Fake Query object for testing puposes.

    We don't use the Graphite Query because it imports too many things from Django.
    """

    def __init__(self, pattern):
        self.pattern = pattern


class TestFinder(bg_test_utils.TestCaseWithFakeAccessor):

    def setUp(self):
        super(TestFinder, self).setUp()
        for metric_name in "a", "a.a", "a.b.c", "x.y":
            metric = bg_test_utils.make_metric(metric_name)
            self.accessor.create_metric(metric)
        self.finder = bg_graphite.Finder(
            accessor=self.accessor,
            metadata_cache=self.metadata_cache,
        )

    def find_nodes(self, pattern):
        return self.finder.find_nodes(FakeFindQuery(pattern))

    def assertMatch(self, glob, branches, leaves):
        found = list(self.find_nodes(glob))
        found_branches = [node.path for node in found if not node.is_leaf]
        found_leaves = [node.path for node in found if node.is_leaf]
        for path in found_branches + found_leaves:
            self.assertIsInstance(path, str)
        self.assertItemsEqual(found_branches, branches)
        self.assertItemsEqual(found_leaves, leaves)

    def test_find_nodes(self):
        self.assertMatch("a", ["a"], ["a"])
        self.assertMatch("a.*", ["a.b"], ["a.a"])
        self.assertMatch("*.{a,b,c,y,z}", ["a.b"], ["a.a", "x.y"])
        self.assertMatch("?.[a-c]", ["a.b"], ["a.a"])
        self.assertMatch("?.[a-z]", ["a.b"], ["a.a", "x.y"])


if __name__ == "__main__":
    unittest.main()
