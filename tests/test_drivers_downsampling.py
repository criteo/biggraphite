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

import sys
import time
import unittest

from biggraphite.drivers import _downsampling as bg_ds

class TestDownsampling2(unittest.TestCase):
    def setUp(self):
        self.md = bg_ds.MetricData(60, 10)

    def test_put_simple(self):
        self.assertEqual([], self.md.put(0, 0))
        self.assertEqual([], self.md.put(10, 1))
        self.assertEqual([], self.md.put(59, 2))
        self.assertEqual(0, self.md._epoch)

    def test_put_complex(self):
        for i in xrange(0, 10):
            self.md.put(i * 60, i * 100)
            self.assertEqual(i, self.md._epoch)
        self.assertEqual([i * 100 for i in xrange(0, 10)], self.md._buffer)
        for i in xrange(10, 25):
            self.md.put(i * 60, i * 100)
            self.assertEqual(i, self.md._epoch)
        self.assertEqual([(i * 100) for i in xrange(20, 25)] + [(i * 100) for i in xrange(15, 20)], self.md._buffer)

    def test_get(self):
        self.md.put(0, 1)
        self.assertEqual(self.md.get(0), 1)
        self.md.put(59, 2)
        self.md.put(59, 3)
        self.assertEqual(self.md.get(0), 3)
        self.assertEqual(self.md.get(59), 3)
        self.assertIsNone(self.md.get(60))

    def test_pop_expired(self):
        for i in xrange(0, 10):
            self.md.put(i * 60, i * 100)
        self.md.put(60, 100)
        self.assertEqual([(i * 60, i * 100) for i in xrange(0, 5)], self.md.pop_expired(5 * 60))
        self.assertEqual([None] * 5 + [(i * 100) for i in xrange(5, 10)], self.md._buffer)
        self.assertEqual([], self.md.pop_expired(5 * 60))
        self.assertEqual([(300, 500)], self.md.pop_expired(6 * 60))
        self.assertEqual([], self.md.pop_expired(6 * 60))

if __name__ == "__main__":
    unittest.main()
