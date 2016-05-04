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


class TestWithCassandra(unittest.TestCase):

    def test_is_graphite_glob(self):
        self.assertTrue(gu.is_graphite_glob("a*"))
        self.assertTrue(gu.is_graphite_glob("a.b*"))
        self.assertTrue(gu.is_graphite_glob("a.b?"))
        self.assertTrue(gu.is_graphite_glob("a.b[a-z]?"))
        self.assertTrue(gu.is_graphite_glob("a{b,c,d}.a"))
        self.assertTrue(gu.is_graphite_glob("a.*.a"))

        self.assertFalse(gu.is_graphite_glob("a.a"))
        self.assertFalse(gu.is_graphite_glob("a-z"))

    def test_graphite_to_cassandra_glob(self):
        self.assertEqual(['a', '*', 'b'], gu.graphite_to_cassandra_glob('a.*.b'))
        self.assertEqual(['a', '*', 'b'], gu.graphite_to_cassandra_glob('a.?.b'))
        self.assertEqual(['*', 'b'], gu.graphite_to_cassandra_glob('a?.b'))
        self.assertEqual(['*', 'b'], gu.graphite_to_cassandra_glob('a{x,y}.b'))
        self.assertEqual(['*', 'b'], gu.graphite_to_cassandra_glob('a{x,y}z.b'))
        self.assertEqual(['*', 'b'], gu.graphite_to_cassandra_glob('a[0-9].b'))
        self.assertEqual(['*', 'b'], gu.graphite_to_cassandra_glob('a[0-9]z.b'))

    def test_filter_metrics(self):
        self.assertEqual(['a.b', 'a.cc'], gu.filter_metrics(['a.b', 'a.cc'], 'a.*'))
        self.assertEqual(['a.b'], gu.filter_metrics(['a.b', 'a.cc'], 'a.?'))
        self.assertEqual(['a.b', 'a.cc', 'y.z'], gu.filter_metrics(['y.z', 'a.b', 'a.cc'], '?.*'))
        self.assertEqual(['a.bd', 'a.cd'], gu.filter_metrics(['y.z', 'a.bd', 'a.cd'], '?.{b,c}?'))
        self.assertEqual(['a.0_', 'a.1_'], gu.filter_metrics(['a.b_', 'a.0_', 'a.1_'], '?.[0-9]?'))


if __name__ == "__main__":
    unittest.main()
