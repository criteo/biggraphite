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
