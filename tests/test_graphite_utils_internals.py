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

from biggraphite import graphite_utils as gu


class TestGraphiteUtilsInternals(unittest.TestCase):

    def test_is_graphite_glob(self):
        self.assertTrue(gu._is_graphite_glob("a*"))
        self.assertTrue(gu._is_graphite_glob("a.b*"))
        self.assertTrue(gu._is_graphite_glob("a.b?"))
        self.assertTrue(gu._is_graphite_glob("a.b[a-z]?"))
        self.assertTrue(gu._is_graphite_glob("a{b,c,d}.a"))
        self.assertTrue(gu._is_graphite_glob("a.*.a"))
        self.assertFalse(gu._is_graphite_glob("a.a"))
        self.assertFalse(gu._is_graphite_glob("a-z"))

    def test_graphite_glob_to_accessor_components(self):
        self.assertEqual('a.*.b', gu._graphite_glob_to_accessor_components('a.*.b'))
        self.assertEqual('a.*.b', gu._graphite_glob_to_accessor_components('a.?.b'))
        self.assertEqual('*.b', gu._graphite_glob_to_accessor_components('a?.b'))
        self.assertEqual('*.b', gu._graphite_glob_to_accessor_components('a{x,y}.b'))
        self.assertEqual('*.b', gu._graphite_glob_to_accessor_components('a{x,y}z.b'))
        self.assertEqual('*.b', gu._graphite_glob_to_accessor_components('a[0-9].b'))
        self.assertEqual('*.b', gu._graphite_glob_to_accessor_components('a[0-9]z.b'))

    def test_format_metric(self):
        self.assertEqual('a.b', gu._format_metric('a.b', 'a.?'))
        self.assertEqual('a.b', gu._format_metric('a.b', 'a.*'))
        self.assertEqual('a.b', gu._format_metric('a.b', 'a.{b,c}'))
        self.assertEqual('*.b', gu._format_metric('a.b', '*.{b,c}'))
        self.assertEqual('*.b', gu._format_metric('a.b', '*.*'))
        self.assertEqual('*.*.c', gu._format_metric('a.b.c', '*.*.?'))

    def test_filter_metrics(self):
        self.assertEqual(['a.b', 'a.cc'], gu._filter_metrics(['a', 'a.b', 'a.cc'], 'a.*'))
        self.assertEqual(['a.b'], gu._filter_metrics(['a.b', 'a.cc'], 'a.?'))
        self.assertEqual(['?.b', '?.cc', '?.z'], gu._filter_metrics(['y.z', 'a.b', 'a.cc'], '?.*'))
        self.assertEqual(['?.bd', '?.cd'], gu._filter_metrics(['y.z', 'a.bd', 'a.cd'], '?.{b,c}?'))
        self.assertEqual(['?.0_', '?.1_'], gu._filter_metrics(['a.b_', 'a.0_', 'a.1_'], '?.[0-9]?'))
        self.assertEqual(['a.*.c', 'a.*.y'], gu._filter_metrics(['a.b', 'a.b.c', 'a.x.y'], 'a.*.*'))


if __name__ == "__main__":
    unittest.main()
