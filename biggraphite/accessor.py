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

"""Abstracts interface implementing by the different datastores."""
from __future__ import absolute_import
from __future__ import print_function

import abc
import codecs
import json
import threading


class Error(Exception):
    """Base class for all exceptions from this module."""


class RetryableError(Error):
    """Errors accessing Cassandra that could succeed if retried."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


_UTF8_CODEC = codecs.getencoder('utf8')


def encode_metric_name(name):
    """Encode name as utf-8, raise UnicodeError if it can't.

    Args:
      name: The metric to encode, must be an instance of basestring.
        If it is an instance of string, it will be assumed to already have been
        encoded for performance reasons.

    Raises:
      UnicodeError: Couldn't encode.
    """
    if isinstance(name, str):
        return name
    # Next line may raise UnicodeError
    return _UTF8_CODEC(name)[0]


def round_down(rounded, devider):
    """Round down an integer to a multiple of devider."""
    return int(rounded) // devider * devider


def round_up(rounded, devider):
    """Round up an integer to a multiple of devider."""
    return int(rounded + devider - 1) // devider * devider


class MetricMetadata(object):
    """Represents all information about a metric except its name.

    Not meant to be mutated.
    """

    __slots__ = (
        "carbon_aggregation", "carbon_highest_precision",
        "carbon_retentions", "carbon_xfilesfactor",
    )

    _DEFAULT_AGGREGATION = "average"
    _DEFAULT_RETENTIONS = [[1, 24*3600]]  # One second for one day
    _DEFAULT_XFILESFACTOR = 0.5

    def __init__(self, carbon_aggregation=None, carbon_retentions=None, carbon_xfilesfactor=None):
        """Record its arguments."""
        self.carbon_aggregation = carbon_aggregation or self._DEFAULT_AGGREGATION
        self.carbon_retentions = carbon_retentions or self._DEFAULT_RETENTIONS
        self.carbon_xfilesfactor = carbon_xfilesfactor
        if self.carbon_xfilesfactor is None:
            self.carbon_xfilesfactor = self._DEFAULT_XFILESFACTOR
        self.carbon_highest_precision = self.carbon_retentions[0][0]

    def __setattr__(self, name, value):
        # carbon_highest_precision is the last attribute __init__ sets.
        if hasattr(self, "carbon_highest_precision"):
            raise AttributeError("can't set attribute")
        super(MetricMetadata, self).__setattr__(name, value)

    def as_json(self):
        """Serialize MetricMetadata into a JSon string from_json() can parse."""
        return json.dumps(self.as_string_dict())

    def as_string_dict(self):
        """Turn an instance into a dict of string to string."""
        return {
            "carbon_aggregation": self.carbon_aggregation,
            "carbon_retentions": json.dumps(self.carbon_retentions),
            "carbon_xfilesfactor": "%f" % self.carbon_xfilesfactor,
        }

    @classmethod
    def from_json(cls, s):
        """Parse MetricMetadata from a JSon string produced by as_json()."""
        return cls.from_string_dict(json.loads(s))

    @classmethod
    def from_string_dict(cls, d):
        """Turn a dict of string to string into a MetricMetadata."""
        return cls(
            carbon_aggregation=d.get("carbon_aggregation"),
            carbon_retentions=json.loads(d.get("carbon_retentions")),
            carbon_xfilesfactor=float(d.get("carbon_xfilesfactor")),
        )

    def carbon_aggregate_points(self, time_span, points):
        """"An aggregator function suitable for Accessor.fetch().

        Args:
          time_span: the duration for which we are aggregating, in seconds.
            For example, if points are meant to represent an hour, the value is 3600.
          points: values to aggregate as float from most recent to oldest

        Returns:
          A float, or None to reject the points.
        """
        # TODO: Handle precomputation of aggregates.
        assert time_span
        coverage = len(points) * self.carbon_highest_precision / float(time_span)
        if not points or coverage < self.carbon_xfilesfactor:
            return None
        if self.carbon_aggregation == "average":
            return float(sum(points)) / len(points)
        if self.carbon_aggregation == "last":
            # Points are stored in descending order, the "last" is actually the first
            return points[0]
        if self.carbon_aggregation == "min":
            return min(points)
        if self.carbon_aggregation == "max":
            return max(points)
        if self.carbon_aggregation == "sum":
            return sum(points)
        raise InvalidArgumentError("Unknown aggregation method: %s" % self.carbon_aggregation)


class Metric(object):
    """Represents all information about a metric.

    This is not an instance of MetricMetadata: It cannot be serialized
    in JSON to minimise confusion in cache that expects few possible
    Metadata at any time.

    Not meant to be mutated.
    """

    __slots__ = ("name", "metadata")

    def __init__(self, name, metadata):
        """Record its arguments."""
        super(Metric, self).__init__()
        assert metadata, "Metric: metadata is None"
        assert name, "Metric: name is None"
        self.name = encode_metric_name(name)
        self.metadata = metadata

    def __getattr__(self, name):
        return getattr(self.metadata, name)

    def __dir__(self):
        res = dir(self.metadata)
        res.extend(self.__slots__)
        res.sort()
        return res


class Accessor(object):
    """Provides Read/Write accessors to BigGraphite.

    It is safe to fork() or start new process until connect() has been called.
    It is not safe to share a given accessor across threads.
    Calling a method that requires a connection without being connected() will result
    in NotConnectedError being raised.
    """

    __slots__ = ('is_connected')

    __metaclass__ = abc.ABCMeta

    # Current value is based on Cassandra page settings, so that everything fits in a single
    # reply with default settings.
    # TODO: Mesure actual number of metrics for existing queries and estimate a more
    # reasonable limit, also consider other engines.
    MAX_METRIC_PER_GLOB = 5000

    def __init__(self, backend_name):
        """Set internal variables."""
        self.is_connected = False
        self.backend_name = backend_name

    def __enter__(self):
        """Call connect()."""
        self.connect()
        return self

    def __exit__(self, _type, _value, _traceback):
        """Call shutdown()."""
        self.shutdown()
        return False

    @abc.abstractmethod
    def connect(self, skip_schema_upgrade=False):
        """Establish a connection.

        This must be called AFTER creating subprocess with the multiprocessing module.
        """
        pass

    @abc.abstractmethod
    def create_metric(self, metric):
        """Create a metric from its definition as Metric.

        Parent directory are implicitly created.
        This can be expensive, it is worthwile to first check if the metric exists.

        Args:
          metric: The metric definition.
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        self._check_connected()

    def _check_connected(self):
        if not self.is_connected:
            raise Error("Accessor's connect() wasn't called")

    @abc.abstractmethod
    def drop_all_metrics(self):
        """Delete all metrics from the database."""
        self._check_connected()

    @abc.abstractmethod
    def fetch_points(self, metric, time_start, time_end, resolution):
        """Fetch points from time_start included to time_end excluded.

        connect() must have previously been called.

        Args:
          metric: The metric definition as per get_metric.
          time_start: timestamp in second from the Epoch as an int, inclusive,
            must be a multiple of resolution
          time_end: timestamp in second from the Epoch as an int, exclusive,
            must be a multiple of resolution
          resolution: time delta in seconds as an int, must be > 0, must be one of
            the resolution the metrics stores

        Yields:
          (timestamp, value) where timestamp indicates the value is about the
          range from timestamp included to timestamp+resolution excluded

        Raises:
          InvalidArgumentError: if time_start, time_end or resolution are not as per above
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        if resolution < 1:
            raise InvalidArgumentError("resolution (%d) is not bpositive" % resolution)
        if time_start % resolution or time_start < 0:
            raise InvalidArgumentError(
                "time_start (%d) is not a multiple of resolution (%d)" % (
                    time_start, resolution))
        if time_end % resolution or time_end < 0:
            raise InvalidArgumentError(
                "time_end (%d) is not a multiple of resolution (%d)" % (
                    time_end, resolution))

    @abc.abstractmethod
    def get_metric(self, metric_name):
        """Return a MetricMetadata for this metric_name, None if no such metric."""
        self._check_connected()

    @abc.abstractmethod
    def glob_metric_names(self, glob):
        """Return a sorted list of metric names matching this glob."""
        self._check_connected()

    @abc.abstractmethod
    def glob_directory_names(self, glob):
        """Return a sorted list of metric directories matching this glob."""
        self._check_connected()

    def insert_points(self, metric, timestamps_and_values):
        """Insert points for a given metric.

        Args:
          metric: A Metric instance.
          timestamps_and_values: An iterable of (timestamp in seconds, values as double)
        """
        self._check_connected()

        event = threading.Event()
        exception_box = [None]

        def on_done(exception):
            exception_box[0] = exception

            event.set()

        self.insert_points_async(metric, timestamps_and_values, on_done)
        event.wait()
        if exception_box[0]:
            raise exception_box[0]

    @abc.abstractmethod
    def insert_points_async(self, metric, timestamps_and_values, on_done=None):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          timestamps_and_values: An iterable of (timestamp in seconds, values as double)
          on_done(e: Exception): called on done, with an exception or None if succesfull
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        self._check_connected()

    @abc.abstractmethod
    def shutdown(self):
        """Close the connection."""
        pass


class PointGrouper(object):
    """Helper for client-side aggregation.

    TODO: It hardcodes a knowledge of how Casssandra results are returned together.
    """

    def __init__(self, metric, time_start_ms, time_end_ms, step_ms, query_results):
        """Constructor for PointGrouper.

        Args:
          metric: The metric for which to insert point.
          time_start_ms: timestamp in second from the Epoch as an int,
            inclusive,  must be a multiple of step
          time_end_ms: timestamp in second from the Epoch as an int,
            exclusive, must be a multiple of step
          step_ms: time delta in seconds as an int, must be > 0
          query_results: query results to fetch point from.
        """
        self.metric = metric
        self.time_start_ms = time_start_ms
        self.time_end_ms = time_end_ms
        self.step_ms = step_ms
        self.query_results = query_results

        self.current_points = []
        self.current_timestamp_ms = None

    def __iter__(self):
        return self.generate_points()

    def run_aggregation(self):
        """Aggregate points in current_points.

        This will skip the first point and return (None, None)
        if the function doesn't generate any aggregated point.
        """
        # This is the first point we encounter, do not emit it on its own,
        # rather wait until we have found points fitting in the next period.
        ret = (None, None)
        if self.current_timestamp_ms is None:
            return ret
        aggregate = self.metric.metadata.carbon_aggregate_points(
            self.step_ms / 1000.0, self.current_points)
        if aggregate is not None:
            ret = (self.current_timestamp_ms / 1000.0, aggregate)
            del self.current_points[:]
        return ret

    def generate_points(self):
        """Generator function ton consume query_results and produce points."""
        first_exc = None

        for successfull, rows_or_exception in self.query_results:
            if first_exc:
                # A query failed, we still consume the results
                continue
            if not successfull:
                first_exc = rows_or_exception
            for row in rows_or_exception:
                timestamp_ms = row[0] + row[1]
                assert timestamp_ms >= self.time_start_ms
                assert timestamp_ms < self.time_end_ms
                timestamp_ms = round_down(timestamp_ms, self.step_ms)

                if self.current_timestamp_ms != timestamp_ms:
                    ts, point = self.run_aggregation()
                    if ts is not None:
                        yield (ts, point)

                    self.current_timestamp_ms = timestamp_ms

                self.current_points.append(row[2])

        ts, point = self.run_aggregation()
        if ts is not None:
            yield (ts, point)

        if first_exc:
            raise RetryableError(first_exc)
