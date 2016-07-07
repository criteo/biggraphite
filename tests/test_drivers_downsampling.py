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


from biggraphite.drivers import downsampling as bg_ds


class TestMetricBuffer(unittest.TestCase):
    PRECISION = 60
    CAPACITY = 10

    def setUp(self):
        self.mb = bg_ds.MetricBuffer(self.PRECISION, self.CAPACITY)

    def test_put_simple(self):
        """Test simple put operations."""
        # add points which should fill only the 0-th slot
        self.assertEqual([], self.mb.put(0, 0))
        self.assertEqual([], self.mb.put(self.PRECISION / 2, 1))
        self.assertEqual([], self.mb.put(self.PRECISION - 1, 2))
        self.assertEqual(0, self.mb._epoch)
        self.assertEqual([2] + [None] * (self.CAPACITY - 1), self.mb._buffer)
        # adding a point at timestamp self.PRECISION should fill the 1-th slot
        self.assertEqual([], self.mb.put(self.PRECISION, 3))
        self.assertEqual([2, 3] + [None] * (self.CAPACITY - 2), self.mb._buffer)

    def test_put_past(self):
        """Test that inserting a point in the far past does nothing."""
        # set the epoch at some point in time
        timestamp = 5 * self.CAPACITY * self.PRECISION
        self.assertEqual([], self.mb.put(timestamp, 0))
        # this point in the past does not fit in the buffer, so it must be discarded
        self.assertEqual([], self.mb.put(0, -1))
        self.assertEqual([0] + [None] * (self.CAPACITY - 1), self.mb._buffer)
        self.assertEqual(timestamp // self.PRECISION, self.mb._epoch)

    def test_put_future(self):
        """Test that inserting a point in the far future keeps only this point."""
        # fill the 0-th slot to set the epoch
        self.assertEqual([], self.mb.put(0, 42))
        # this point is in the future, and it
        timestamp_future = 5 * self.CAPACITY * self.PRECISION
        self.assertEqual([(0, 42)], self.mb.put(timestamp_future, 1))
        self.assertEqual(timestamp_future // self.PRECISION, self.mb._epoch)

    def test_put_complex(self):
        """Test that put works across several epochs."""
        # fill the buffer exactly once
        for i in xrange(0, self.CAPACITY):
            self.mb.put(i * self.PRECISION, i * 100)
            self.assertEqual(i, self.mb._epoch)
        expected = [i * 100 for i in xrange(0, self.CAPACITY)]
        self.assertEqual(expected, self.mb._buffer)
        # fill all slots except the last one a second time
        for i in xrange(self.CAPACITY, 2 * self.CAPACITY - 1):
            expected = [((i - self.CAPACITY) * self.PRECISION, (i - self.CAPACITY) * 100)]
            self.assertEqual(expected, self.mb.put(i * self.PRECISION, i * 100))
            self.assertEqual(i, self.mb._epoch)
        expected = [(i * 100) for i in xrange(self.CAPACITY, 2 * self.CAPACITY - 1)] +\
                   [(self.CAPACITY - 1) * 100]
        self.assertEqual(expected, self.mb._buffer)

    def test_get(self):
        """Test that get has the correct behavior."""
        # the next statements fill the 0-the slot only
        self.mb.put(0, 1)
        self.assertEqual(self.mb.get(0), 1)
        self.mb.put(self.PRECISION - 1, 2)
        self.mb.put(self.PRECISION - 1, 3)
        self.assertEqual(self.mb.get(0), 3)
        self.assertEqual(self.mb.get(self.PRECISION - 1), 3)
        self.assertIsNone(self.mb.get(self.PRECISION))

    def test_current_points(self):
        """Add some points and check there are in the buffer."""
        self.mb.put(0, 1)
        self.mb.put(self.PRECISION - 1, 2)
        self.assertEqual([(0, 2)], self.mb.current_points())
        self.mb.put(self.PRECISION, 3)
        self.assertEqual([(0, 2), (self.PRECISION, 3)], self.mb.current_points())

    def test_pop_expired(self):
        """Test expiry of points according to capacity and precision."""
        # fill all the slots once
        for i in xrange(0, self.CAPACITY):
            self.mb.put(i * self.PRECISION, i * 100)
        # pop points strictly before (self.CAPACITY - 1) * self.PRECISION
        # these should be the points [0, 1, ..., self.CAPACITY - 2]
        expected = [(i * self.PRECISION, i * 100) for i in xrange(0, self.CAPACITY - 1)]
        self.assertEqual(expected, self.mb.pop_expired((self.CAPACITY - 1) * self.PRECISION))
        # check that the points we just popped were removed from the buffer
        expected = [None] * (self.CAPACITY - 1) + [(self.CAPACITY - 1) * 100]
        self.assertEqual(expected, self.mb._buffer)
        # check that points can't be expired/popped twice
        self.assertEqual([], self.mb.pop_expired((self.CAPACITY - 1) * self.PRECISION))
        # pop the last point, and check that it's expired only once
        expected = [((self.CAPACITY - 1) * self.PRECISION, (self.CAPACITY - 1) * 100)]
        self.assertEqual(expected, self.mb.pop_expired(self.CAPACITY * self.PRECISION))
        self.assertEqual([], self.mb.pop_expired(self.CAPACITY * self.PRECISION))

if __name__ == "__main__":
    unittest.main()
