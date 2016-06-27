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

"""Abstract interfaces implemented by the different datastores."""
from __future__ import absolute_import
from __future__ import print_function

import abc
import array
import codecs
import enum
import json
import math
import re
import threading


class Error(Exception):
    """Base class for all exceptions from this module."""


class RetryableError(Error):
    """Errors accessing Cassandra that could succeed if retried."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


_UTF8_CODEC = codecs.getencoder('utf8')

_NAN = float("nan")


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


def round_down(rounded, divider):
    """Round down an integer to a multiple of divider."""
    return int(rounded) // divider * divider


def round_up(rounded, divider):
    """Round up an integer to a multiple of divider."""
    return int(rounded + divider - 1) // divider * divider


@enum.unique
class Aggregator(enum.Enum):
    """Represents one of the known aggregations."""

    minimum = "min"
    maximum = "max"
    total = "sum"
    average = "avg"
    last = "last"

    def __init__(self, carbon_name):
        """Set attributes."""
        self.carbon_name = carbon_name
        self._aggregate = getattr(self, "_aggregate_" + self.name)

    def aggregate(self, values, counts, newest_first=False):
        """Aggregate values for which counts is > 0.

        Args:
          values: values to aggregate as float from oldest to most recent (unless
            newest_first is True).
          counts: Total number of points represented by a single value.
            The count for values[n] is counts[n].
            Different from 1 if values are already partially aggregated.
          newest_first: if True, values are in reverse order.

        Returns:
          A pair of value, sum(count). value is meaningless if count is 0.
        """
        if not values:
            assert not counts
            return _NAN, 0
        assert len(counts) == len(values), "%s vs %s" % (counts, values)
        total_count = sum(counts)
        if not total_count:
            return _NAN, 0
        return self._aggregate(total_count, values, counts, newest_first)

    @staticmethod
    def _aggregate_average(total_count, values, counts, newest_first):
        avg = 0.0
        for i, c in enumerate(counts):
            avg += c * values[i]
        return avg/total_count, total_count

    @staticmethod
    def _aggregate_last(total_count, values, counts, newest_first):
        if not newest_first:
            counts = counts[::-1]
            values = values[::-1]
        for i, c in enumerate(counts):
            if c:
                return values[i], total_count
        raise AssertionError("Count did not contain a value greater than 0")

    @staticmethod
    def _aggregate_maximum(total_count, values, counts, newest_first):
        maximum = float("-inf")
        for i, c in enumerate(counts):
            if c and values[i] > maximum:
                maximum = values[i]
        assert not math.isinf(maximum)
        return maximum, total_count

    @staticmethod
    def _aggregate_minimum(total_count, values, counts, newest_first):
        minimum = float("+inf")
        for i, c in enumerate(counts):
            if c and values[i] < minimum:
                minimum = values[i]
        assert not math.isinf(minimum)
        return minimum, total_count

    @staticmethod
    def _aggregate_total(total_count, values, counts, newest_first):
        total_values = 0.0
        for i, c in enumerate(counts):
            if c:
                total_values += values[i]
        return total_values, total_count

    @classmethod
    def from_carbon_name(cls, name):
        """Make an instance from a carbon-like name."""
        if not name:
            return None
        try:
            return cls(name)
        except ValueError:
            raise InvalidArgumentError("Unknown carbon aggregation: %s" % name)

    @classmethod
    def from_config_name(cls, name):
        """Make an instance from a BigGraphite name."""
        if not name:
            return None
        try:
            return cls[name]
        except KeyError:
            raise InvalidArgumentError("Unknown BG aggregator: %s" % name)


class Stage(object):
    """One of the element of a retention policy.

    A stage means "keep that many points with that precision".
    Precision is the amount of time a point covers, measured in seconds.
    """

    __slots__ = ("points", "precision", )

    # Parses the values of as_string into points and precision group
    _STR_RE = re.compile(r"^(?P<points>[\d]+)\*(?P<precision>[\d]+)s$")

    def __init__(self, points, precision):
        """Set attributes."""
        self.points = int(points)
        self.precision = int(precision)

    def __str__(self):
        return self.as_string

    def __eq__(self, other):
        if not isinstance(other, Stage):
            return False
        return self.points == other.points and self.precision == other.precision

    def __ne__(self, other):
        return not (self == other)

    @property
    def duration(self):
        """The duration of this stage in seconds."""
        return self.points * self.precision

    @property
    def as_string(self):
        """A string like "${POINTS}*${PRECISION}s"."""
        return "{}*{}s".format(self.points, self.precision)

    @classmethod
    def from_string(cls, s):
        """Parse results of as_string into an instance."""
        match = cls._STR_RE.match(s)
        if not match:
            raise InvalidArgumentError("Invalid retention: '%s'" % s)
        groups = match.groupdict()
        return cls(
            points=int(groups['points']),
            precision=int(groups['precision']),
        )

    def epoch(self, timestamp):
        """Return time elapsed since Unix epoch in count of self.duration.

        A "stage epoch" is a range of timestamps: [N*stage_duration, (N+1)*stage_duration[
        This function returns N.

        Args:
          timestamp: A timestamp in seconds.
        """
        return int(timestamp / self.duration)


class Retention(object):
    """A retention policy, made of 0 or more Stages."""

    __slots__ = ("stages", )

    def __init__(self, stages):
        """Set self.stages ."""
        prev = None
        for s in stages:
            if prev and s.precision % prev.precision:
                raise InvalidArgumentError("precision of %s must be a multiple of %s" % (s, prev))
            if prev and prev.duration >= s.duration:
                raise InvalidArgumentError("duration of %s must be lesser than %s" % (s, prev))
            prev = s
        self.stages = tuple(stages)

    def __eq__(self, other):
        if not isinstance(other, Retention):
            return False
        return self.stages == other.stages

    def __ne__(self, other):
        return not (self == other)

    @property
    def as_string(self):
        """Return strings like "60*60s:24*3600s"."""
        return ":".join(s.as_string for s in self.stages)

    @classmethod
    def from_string(cls, string):
        """Parse results of as_string into an instance.

        Args:
          string: A string like "60*60s:24*3600s"
        """
        if string:
            stages = [Stage.from_string(s) for s in string.split(":")]
        else:
            stages = []
        return cls(stages=stages)

    @property
    def duration(self):
        """Return the maximum duration of all stages."""
        if not self.stages:
            return 0
        return self.stages[-1].duration

    def __getitem__(self, n):
        """Return the n-th stage."""
        return self.stages[n]

    @classmethod
    def from_carbon(cls, l):
        """Make new instance from list of (precision, points).

        Note that precision is first, unlike in Stage.__init__
        """
        stages = [Stage(points=points, precision=precision)
                  for precision, points in l]
        return cls(stages)


class MetricMetadata(object):
    """Represents all information about a metric except its name.

    Not meant to be mutated.
    """

    __slots__ = (
        "aggregator", "retention", "carbon_xfilesfactor",
    )

    _DEFAULT_AGGREGATOR = Aggregator.average
    _DEFAULT_RETENTION = Retention.from_string("86400*1s")
    _DEFAULT_XFILESFACTOR = 0.5

    def __init__(self, aggregator=None, retention=None, carbon_xfilesfactor=None):
        """Record its arguments."""
        self.aggregator = aggregator or self._DEFAULT_AGGREGATOR
        assert isinstance(self.aggregator, Aggregator), self.aggregator
        self.retention = retention or self._DEFAULT_RETENTION
        if carbon_xfilesfactor is None:
            self.carbon_xfilesfactor = self._DEFAULT_XFILESFACTOR
        else:
            self.carbon_xfilesfactor = carbon_xfilesfactor

    def __setattr__(self, name, value):
        # carbon_xfilesfactor is the last attribute __init__ sets.
        if hasattr(self, "carbon_xfilesfactor"):
            raise AttributeError("can't set attribute")
        super(MetricMetadata, self).__setattr__(name, value)

    def aggregate(self, *args, **kwargs):
        """Call self.aggregator.aggregate with its arguments."""
        return self.aggregator.aggregate(*args, **kwargs)

    def as_json(self):
        """Serialize MetricMetadata into a JSon string from_json() can parse."""
        return json.dumps(self.as_string_dict())

    def as_string_dict(self):
        """Turn an instance into a dict of string to string."""
        return {
            "aggregator": self.aggregator.name,
            "retention": self.retention.as_string,
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
            aggregator=Aggregator.from_config_name(d.get("aggregator")),
            retention=Retention.from_string(d.get("retention")),
            carbon_xfilesfactor=float(d.get("carbon_xfilesfactor")),
        )


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

    Calling other methods before connect() will raise NotConnectedError, unless
    noted otherwise.
    """

    __slots__ = ('is_connected', )

    __metaclass__ = abc.ABCMeta

    # Current value is based on Cassandra page settings, so that everything fits in a single
    # reply with default settings.
    # TODO: Mesure actual number of metrics for existing queries and estimate a more
    # reasonable limit, also consider other engines.
    MAX_METRIC_PER_GLOB = 5000

    def __init__(self, backend_name):
        """Set internal variables."""
        self.backend_name = backend_name
        self.is_connected = False

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
        """Establish a connection, idempotent.

        This must be called AFTER creating subprocess with the multiprocessing module.
        """
        pass

    @abc.abstractmethod
    def create_metric(self, metric):
        """Create a metric from its definition as Metric.

        Parent directories are implicitly created.
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

        Args:
          metric: The metric definition as per get_metric.
          time_start: timestamp in second from the Epoch as an int, inclusive,
            must be a multiple of resolution
          time_end: timestamp in second from the Epoch as an int, exclusive,
            must be a multiple of resolution
          resolution: time delta in seconds as an int, must be > 0, must be one of
            the resolution the metrics stores

        Yields:
          pairs of (timestamp, value) to indicate value is an aggregate for the range
          [timestamp, timestep+step[

        Raises:
          InvalidArgumentError: if time_start, time_end or resolution are not as per above
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        if resolution < 1:
            raise InvalidArgumentError("resolution (%d) is not positive" % resolution)
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
        """Close the connection.

        This is safe to call even if connect() was never called.
        """
        pass


class PointGrouper(object):
    """Helper for client-side aggregator.

    It hardcodes a knowledge of how Casssandra results are returned together, this should be
    abstracted away if more datastores do client-side agregation.
    """

    def __init__(self, metric, time_start_ms, time_end_ms, step_ms, query_results):
        """Constructor for PointGrouper.

        Args:
          metric: The metric for which to group values.
          time_start_ms: timestamp in second from the Epoch as an int,
            inclusive,  must be a multiple of step
          time_end_ms: timestamp in second from the Epoch as an int,
            exclusive, must be a multiple of step
          step_ms: time delta in seconds as an int, must be > 0
          query_results: query results to fetch values from.
        """
        self.metric = metric
        self.time_start_ms = time_start_ms
        self.time_end_ms = time_end_ms
        self.step_ms = step_ms
        self.query_results = query_results

        self.current_values = array.array("d")
        self.current_counts = array.array("L")
        self.current_timestamp_ms = None

    def __iter__(self):
        return self.generate_values()

    def run_aggregator(self):
        """Aggregate values in current_values.

        This will skip the first point and return (None, None)
        if the function doesn't generate any aggregated point.
        """
        # This is the first point we encounter, do not emit it on its own,
        # rather wait until we have found values fitting in the next period.
        ret = (None, None)
        if self.current_timestamp_ms is None:
            return ret
        aggregate = self.metric.metadata.aggregator.aggregate(
            values=self.current_values, counts=self.current_counts,
            newest_first=True)
        if aggregate is not None:
            ret = (self.current_timestamp_ms / 1000.0, aggregate[0])
            del self.current_values[:]
            del self.current_counts[:]
        return ret

    def generate_values(self):
        """Generator function ton consume query_results and produce values."""
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
                    ts, point = self.run_aggregator()
                    if ts is not None:
                        yield (ts, point)

                    self.current_timestamp_ms = timestamp_ms

                self.current_values.append(row[2])
                self.current_counts.append(row[3])

        ts, point = self.run_aggregator()
        if ts is not None:
            yield (ts, point)

        if first_exc:
            raise RetryableError(first_exc)
