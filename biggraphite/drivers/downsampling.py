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

"""Downsampling helpers for drivers that do not implement it server-side."""
from __future__ import absolute_import
from __future__ import print_function

import collections


class Downsampler(object):
    """Stupid downsampler that produces per minute average."""

    def __init__(self):
        self.__precision = 60
        self.__epoch = 0
        self.__counters = {}

    def feed(self, metric, datapoints):
        """Feed the downsampler and produce points.

        Arg:
          metric: Metric
          datapoints: tuple(timestamp as sec, value as float)
        Returns:
          tuple(timestamp as sec, value as float) generated datapoints.
        """
        results = []

        for timestamp, value in datapoints:
            epoch = timestamp // self.__precision

            if epoch < self.__epoch:
                continue
            if epoch > self.__epoch:
                self.__epoch = epoch
                values = self.__counters.values()
                if values:
                    results.extend(values)
                self.__counters.clear()

            count = 1
            if metric.name in self.__counters:
                value += self.__counters[metric.name][1]
                count += self.__counters[metric.name][2]
            self.__counters[metric.name] = (
                epoch * self.__precision,
                value,
                count,
                self.__precision
            )
        return results


class MetricBuffer(object):
    """Perform computation on the data of a given metric."""

    __slots__ = (
        "_precision",
        "_capacity",
        "_buffer",
        "_epoch"
    )

    def __init__(self, precision=60, capacity=10):
        """"Initialize a MetricBuffer object.

        Args:
          precision: precision of the raw buffer in seconds.
          capacity: number of slots in the raw buffer.
        """
        self._precision = precision
        self._capacity = capacity
        self._buffer = [None] * capacity
        self._epoch = None

    def current_points(self):
        """Get list of points currently in the buffer.

        Returns:
          The list of (timestamp, value) of points in the buffer.
        """
        if self._epoch is None:
            return []
        res = []
        epoch_start = self._epoch - (self._capacity - 1)
        epoch_end = epoch_start + self._capacity
        for e in xrange(epoch_start, epoch_end):
            index = e % self._capacity
            if self._buffer[index] is not None:
                res.append((e * self._precision, self._buffer[index]))
        return res

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
        epoch_start = self._epoch - (self._capacity - 1)
        epoch_end = timestamp // self._precision
        for e in xrange(epoch_start, epoch_end):
            index = e % self._capacity
            if self._buffer[index] is not None:
                res.append((e * self._precision, self._buffer[index]))
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
        epoch = timestamp // self._precision
        epoch_start = self._epoch - (self._capacity - 1)
        epoch_end = self._epoch
        if epoch >= epoch_start and epoch <= epoch_end:
            return self._buffer[epoch % self._capacity]
        return None

    def put(self, timestamp, value):
        """"Insert a data point in the raw buffer and pop expired points.

        Args:
          timestamp: timestamp of the data point.
          value: value of the data point.

        Returns:
          The list of expired points form the raw buffer.
        """
        expiry_timestamp = timestamp - (self._capacity - 1) * self._precision
        expired = self.pop_expired(expiry_timestamp)
        epoch = timestamp // self._precision
        if self._epoch is None:
            self._epoch = epoch
        if epoch > self._epoch - self._capacity:
            self._buffer[epoch % self._capacity] = value
        if epoch > self._epoch:
            self._epoch = epoch
        return expired
