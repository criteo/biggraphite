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

"""Downsampling interface."""

from __future__ import absolute_import
from __future__ import print_function

import time

_NAN = float("nan")


class MetricData(object):
    """Perform computation on the data of a given Metric."""

    __slots__ = (
        "precision",
        "_count",
        "_buffer",
        "_epoch"
    )

    def __init__(self, precision, count):
        """"Initialize a MetricBuffer object.

        Args:
          precision: precision of the raw buffer in seconds.
          count: number of slots in the raw buffer.
          now: function to get current time, in seconds. # TODO: check if in BG, times are in seconds.
        """
        self.precision = precision
        self._count = count
        self._buffer = [None] * count
        self._epoch = None

    def pop_expired(self, timestamp):
        """"Pop expired elements strictly older than timestamp.

        Args:
          timestamp: time horizon to clear elements, in seconds.

        Returns:
          The list of (timestamp, value) popped from the buffer.
        """
        if self._epoch is None:
            return []
        res = []
        epoch = timestamp // self.precision
        for t in xrange(self._epoch - (self._count - 1), epoch):
            index = t % self._count
            if self._buffer[index] is not None:
                res.append((t * self.precision, self._buffer[index]))
                self._buffer[index] = None
        return res

    def get(self, timestamp):
        """"Get a data point from the raw buffer.

        Args:
          timestamp: timestamp of the data point.

        Returns:
          The value of the data point at the requested time,
          or None if it's not in the raw buffer.
        """
        if self._epoch is None:
            return None
        epoch = timestamp // self.precision
        if epoch > self._epoch - self._count and epoch <= self._epoch:
            return self._buffer[epoch % self._count]
        return None

    def put(self, timestamp, value):
        """"Insert a data point in the raw buffer and pop expired elements.

        Args:
          timestamp: timestamp of the data point.
          value: value of the data point.

        Returns:
          The list of expired points form the raw buffer.
        """
        expiry_timestamp = timestamp - (self._count - 1) * self.precision
        expired = self.pop_expired(expiry_timestamp)
        epoch = timestamp // self.precision
        if self._epoch is None:
            self._epoch = epoch
        if epoch > self._epoch - self._count:
            self._buffer[epoch % self._count] = value
        if epoch > self._epoch:
            self._epoch = epoch
        return expired
