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
"""Functions currently used by the Cassandra driver but not specific to it."""
from __future__ import absolute_import
from __future__ import print_function

import threading

from biggraphite import accessor as bg_accessor


class Error(bg_accessor.Error):
    """Base class for all exceptions from this module."""


class CountDown(object):
    """Decrements a count, calls a callback when it reaches 0.

    This is used to wait for queries to complete without storing & sorting their results.
    """

    __slots__ = ("_canceled", "count", "_lock", "_on_zero", )

    def __init__(self, count, on_zero):
        """Record parameters.

        Args:
          count: The integer that will be decremented, must be > 0
          on_zero: called once count reaches zero, see decrement
        """
        assert count > 0
        self.count = count
        self._canceled = False
        self._lock = threading.Lock()
        self._on_zero = on_zero

    def cancel(self, reason):
        """Call the callback now with reason as argument."""
        with self._lock:
            if self._canceled:
                return
            self._canceled = True
            self._on_zero(reason)

    def decrement(self):
        """Call the callback if count reached zero, with None as argument."""
        with self._lock:
            self.count -= 1
            if self._canceled:
                return
            elif not self.count:
                self._on_zero(None)

    def on_result(self, unused_result):
        """Call decrement(), suitable for Cassandra's execute_async."""
        self.decrement()

    def on_failure(self, exc):
        """Call cancel(), suitable for Cassandra's execute_async."""
        self.cancel(Error(exc))


def list_from_str(value):
    """Convert a comma separated string into a list.

    Args:
      value: str or list or set.

    Returns:
      list a list of values.
    """
    if type(value) is str:
        value = [s.strip() for s in value.split(",")]
    elif type(value) in (list, set):
        value = list(value)
    elif value is None:
        value = []
    else:
        raise Error("Unkown type for '%s'" % (value))
    return value


def bool_from_str(value):
    """Convert a user-specified string to a bool."""
    if value == 'True':
        return value
    elif value == 'False':
        return value
    elif type(value) is bool:
        return value

    return str(value)
