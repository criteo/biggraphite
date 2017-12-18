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

from builtins import zip

import abc
import array
import codecs
import enum
import json
import math
import re
import threading
import six


class Error(Exception):
    """Base class for all exceptions from this module."""


class RetryableError(Error):
    """Errors accessing Cassandra that could succeed if retried."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


_UTF8_CODEC = codecs.getencoder('utf8')

_NAN = float("nan")

# Number of bits allocated to dinstinguish replicas in the shard
# id. The default is 2 which means that you can have up to 3 replicas
# writting a the same time to your database.
# See CASSANDRA_DESIGN.md for details. This should eventually move off
# to drivers/ and be used only by drivers who needs that, they should in
# turn just forward the replica id instead of the shard id.
SHARD_REPLICA_MASK = 0xC000
SHARD_WRITER_MASK = (SHARD_REPLICA_MASK ^ 0xFFFF)
SHARD_REPLICA_BITS = bin(SHARD_REPLICA_MASK).count('1')
SHARD_WRITER_BITS = 16 - SHARD_REPLICA_BITS
SHARD_REPLICA_SHIFT = SHARD_WRITER_BITS
SHARD_MAX_REPLICAS = 2**SHARD_REPLICA_BITS


def pack_shard(replica, writer):
    """Pack a replica id and writer id in a short."""
    return (
        (replica << SHARD_REPLICA_SHIFT) |
        (writer & SHARD_WRITER_MASK)
    )


def unpack_shard(shard):
    """Unpack what pack_shard() did."""
    replica = (shard & SHARD_REPLICA_MASK) >> SHARD_REPLICA_SHIFT
    writer = (shard & SHARD_WRITER_MASK)
    return (replica, writer)


def _wait_async_call(async_function, *args, **kwargs):
    """Call async_function and synchronously wait for it to be done.

    Args:
      async_function: Function taking a on_done(e=None:Exception) callback.
      *args: Passed down to async_function
      **kwargs: Passed down to async_function
    """
    event = threading.Event()
    exception_box = [None]

    def on_done(exception):
        exception_box[0] = exception
        event.set()

    async_function(*args, on_done=on_done, **kwargs)
    event.wait()
    if exception_box[0]:
        raise exception_box[0]


def encode_metric_name(name):
    """Encode name as utf-8, raise UnicodeError if it can't.

    Args:
      name: The metric to encode.

    This function make sure that we only have simple strings.

    For Python 2: must be an instance of basestring.
        If it is an instance of string, it will be assumed to already have been
        encoded for performance reasons.

    For Python 3: breaks bytes are given. We could probably decode them instead.

    Raises:
      UnicodeError: Couldn't encode.
    """
    if six.PY3:
        assert(name) is not bytes, "%s should not be of type 'bytes'" % name
        return name

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
    """Represents one of the known aggregations.

    Name is BigGraphite name, chosen to avoid conflicts with Python's builtins.
    Value is Whisper name.
    """

    minimum = "min"
    maximum = "max"
    total = "sum"
    average = "average"
    last = "last"
    # TODO: Add avg_zero.

    def __init__(self, carbon_name):
        """Set attributes."""
        self.carbon_name = carbon_name
        self._aggregate = getattr(self, "_aggregate_" + self.name)
        self._merge = getattr(self, "_merge_" + self.name)

    def merge(self, values=[], counts=[], newest_first=False):
        """Merge aggregated values from a similar stage.

        Args:
          values: values to aggregate as float from oldest to most recent (unless
            newest_first is True).
          counts: counts associated with values as int.  when dealing with
            already aggregated values.
          newest_first: if True, values are in reverse order.

        Returns:
          The merged values and counts, NaN if values is empty or all values are NaN.
        """
        if not values:
            return _NAN
        if not counts:
            counts = [1] * len(values)
        return self._merge(values, counts, newest_first)

    def _merge_average(self, values, counts, values_newest_first):
        # Averages are simply sum+count. The actual average is computed
        # only during reads by dividing sum by count.
        total, count = self.__sum_and_count(values, counts)
        return total, count

    def _merge_last(self, values, counts, values_newest_first):
        if not values_newest_first:
            values = reversed(values)
        for v in values:
            if not math.isnan(v):
                return v, sum(counts)
        return _NAN, sum(counts)

    def _merge_maximum(self, values, counts, values_newest_first):
        _, maximum = self.__min_and_max(values)
        return maximum, sum(counts)

    def _merge_minimum(self, values, counts, values_newest_first):
        minimum, _ = self.__min_and_max(values)
        return minimum, sum(counts)

    def _merge_total(self, values, counts, values_newest_first):
        total, count = self.__sum_and_count(values, counts)
        return total, count

    def aggregate(self, values=[], counts=[], newest_first=False):
        """Aggregate together values of a given stage.

        Args:
          values: values to aggregate as float from oldest to most recent (unless
            newest_first is True).
          counts: counts associated with values as int.  when dealing with
            already aggregated values.
          newest_first: if True, values are in reverse order.

        Returns:
          The aggregated value, NaN if values is empty or all values are NaN.
        """
        if not values:
            return _NAN
        if not counts:
            counts = [1] * len(values)
        return self._aggregate(values, counts, newest_first)

    def _aggregate_average(self, values, counts, values_newest_first):
        total, count = self.__sum_and_count(values, counts)
        return total / count

    def _aggregate_last(self, values, counts, values_newest_first):
        if not values_newest_first:
            values = reversed(values)
        for v in values:
            if not math.isnan(v):
                return v
        return _NAN

    def _aggregate_maximum(self, values, counts, values_newest_first):
        _, maximum = self.__min_and_max(values)
        return maximum

    def _aggregate_minimum(self, values, counts, values_newest_first):
        minimum, _ = self.__min_and_max(values)
        return minimum

    def _aggregate_total(self, values, counts, values_newest_first):
        total, _ = self.__sum_and_count(values, counts)
        return total

    @classmethod
    def from_carbon_name(cls, name):
        """Make an instance from a carbon-like name."""
        if not name:
            return None
        try:
            return cls(name)
        except ValueError:
            raise InvalidArgumentError("Unknown carbon aggregation: %s" % name)

    def carbon_name(self):
        """Returns the carbon name of this aggregator."""
        return self.value

    @classmethod
    def from_config_name(cls, name):
        """Make an instance from a BigGraphite name."""
        if not name:
            return None
        try:
            return cls[name]
        except KeyError:
            raise InvalidArgumentError("Unknown BG aggregator: %s" % name)

    @staticmethod
    def __sum_and_count(values, counts):
        total = 0.0
        count = 0
        for v, c in zip(values, counts):
            if math.isnan(v):
                continue
            total += v
            count += c
        if not count:
            return _NAN, _NAN
        return total, count

    @staticmethod
    def __min_and_max(values):
        minimum = None
        maximum = None
        for v in values:
            if math.isnan(v):
                continue
            if minimum is None:
                maximum = v
                minimum = v
            elif maximum < v:
                maximum = v
            elif minimum > v:
                minimum = v
        if minimum is None:
            assert maximum is None
            return _NAN, _NAN
        return minimum, maximum


class Stage(object):
    """One of the element of a retention policy.

    A stage means "keep that many points with that precision".
    Precision is the amount of time a point covers, measured in seconds.

    stage0 means that it's the first stage of a retention policy which
    is special because it doesn't contain aggregated values.
    """

    __slots__ = ("duration", "points", "precision", "stage0", )

    # Parses the values of as_string into points and precision group
    _STR_RE = re.compile(
        r"^(?P<points>[\d]+)\*(?P<precision>[\d]+)s(?P<type>(_0|_aggr))?$")

    def __init__(self, points, precision, stage0=False):
        """Set attributes."""
        self.points = int(points)
        self.precision = int(precision)
        self.duration = self.points * self.precision
        self.stage0 = stage0

    def __str__(self):
        return self.as_string

    def __repr__(self):
        return '<{0}.{1}({3}) object at {2}>'.format(
            self.__module__, type(self).__name__, hex(id(self)),
            self.as_string)

    def __eq__(self, other):
        if not isinstance(other, Stage):
            return False
        return (self.points == other.points
                and self.precision == other.precision
                and self.stage0 == other.stage0)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash((self.points, self.precision, self.stage0))

    @property
    def as_string(self):
        """A string like "${POINTS}*${PRECISION}s"."""
        return "{}*{}s".format(self.points, self.precision)

    @property
    def as_full_string(self):
        """A string like "${POINTS}*${PRECISION}s"."""
        ret = self.as_string
        if self.stage0:
            ret += '_0'
        else:
            ret += '_aggr'
        return ret

    @property
    def duration_ms(self):
        """The duration in milliseconds."""
        return self.duration * 1000

    def epoch(self, timestamp):
        """Return time elapsed since Unix epoch in count of self.duration.

        A "stage epoch" is a range of timestamps: [N*stage_duration, (N+1)*stage_duration[
        This function returns N.

        Args:
          timestamp: A timestamp in seconds.
        """
        return int(timestamp // self.duration)

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
            stage0=bool(groups['type'] == '_0')
        )

    @property
    def precision_ms(self):
        """The precision of this stage in milliseconds."""
        return self.precision * 1000

    def round_down(self, timestamp):
        """Round down a timestamp to a multiple of the precision."""
        return round_down(timestamp, self.precision)

    def round_up(self, timestamp):
        """Round down a timestamp to a multiple of the precision."""
        return round_up(timestamp, self.precision)

    def step(self, timestamp):
        """Return time elapsed since Unix epoch in count of self.precision.

        A "stage step" is a range of timestamps: [N*stage_precision, (N+1)*stage_precision[
        This function returns N.

        Args:
          timestamp: A timestamp in seconds.
        """
        return int(timestamp // self.precision)

    def step_ms(self, timestamp_ms):
        """Return time elapsed since Unix epoch in count of self.precision_ms.

        A "stage step" is a range of timestamps: [N*stage_precision, (N+1)*stage_precision[
        This function returns N.

        Args:
          timestamp_ms: A timestamp in milliseconds.
        """
        return int(timestamp_ms // self.precision_ms)

    def aggregated(self):
        """Return true if this contains aggregated values.

        Aggregated values are in the form (sum, count) instead of just (value).
        Only the first stage of a retention policy isn't aggregated (called stage0).

        Returns:
          bool, True is not first stage.
        """
        return not self.stage0


class Retention(object):
    """A retention policy, made of 0 or more Stages."""

    __slots__ = ("stages", )

    def __init__(self, stages):
        """Set self.stages ."""
        prev = None
        if not stages:
            raise InvalidArgumentError("there must be at least one stage")
        for s in stages:
            if prev and s.precision % prev.precision:
                raise InvalidArgumentError(
                    "precision of %s must be a multiple of %s" % (s, prev))
            if prev and prev.duration >= s.duration:
                raise InvalidArgumentError(
                    "duration of %s must be lesser than %s" % (s, prev))
            prev = s
        self.stages = tuple(stages)
        self.stages[0].stage0 = True

    def __getitem__(self, n):
        """Return the n-th stage."""
        return self.stages[n]

    def __hash__(self):
        return hash(self.stages)

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

    @property
    def stage0(self):
        """An alias for stage[0]."""
        return self.stages[0]

    @property
    def downsampled_stages(self):
        """An alias for stages[1:]."""
        return self.stages[1:]

    @classmethod
    def from_carbon(cls, l):
        """Make new instance from list of (precision, points).

        Note that precision is first, unlike in Stage.__init__
        """
        stages = [Stage(points=points, precision=precision)
                  for precision, points in l]
        return cls(stages)

    def find_stage_for_ts(self, searched, now):
        """Return the most precise stage that contains "searched".

        Args:
          searched: A timestamp to search, in seconds.
          now: The current timestamp, in seconds.
        """
        for stage in self.stages:
            if searched > now - stage.duration:
                return stage
        # There is always at least one stage.
        return self.stages[-1]

    def align_time_window(self, start_time, end_time, now, shift=False):
        """Constrain the provided range in an aligned interval within retention."""
        stage = self.find_stage_for_ts(searched=start_time, now=now)

        now = stage.round_up(now)

        if shift:
            oldest_timestamp = now - stage.duration
            start_time = max(start_time, oldest_timestamp)
        start_time = min(now, start_time)
        start_time = stage.round_down(start_time)

        end_time = min(now, end_time)
        end_time = stage.round_up(end_time)

        if end_time < start_time:
            end_time = start_time
        return start_time, end_time, stage

    @property
    def points(self):
        """Return the total number of points for this retention."""
        return sum(stage.points for stage in self.stages)


class MetricMetadata(object):
    """Represents all information about a metric except its name.

    Not meant to be mutated.
    """

    __slots__ = (
        "aggregator",
        "retention",
        "carbon_xfilesfactor",
    )

    _DEFAULT_AGGREGATOR = Aggregator.average
    _DEFAULT_RETENTION = Retention.from_string("86400*1s:10080*60s")
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
        if name not in self.__slots__:
            raise AttributeError("can't set attribute")
        super(MetricMetadata, self).__setattr__(name, value)

    def __eq__(self, other):
        if not isinstance(other, MetricMetadata):
            return False
        return self.as_string_dict() == other.as_string_dict()

    def __ne__(self, other):
        return not (self == other)

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
        if d is None:
            return cls()

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

    __slots__ = (
        "name",
        "id",
        "metadata"
    )

    def __init__(self, name, id, metadata):
        """Record its arguments."""
        super(Metric, self).__init__()
        assert name, "Metric: name is None"
        assert id, "Metric: id is None"
        assert metadata, "Metric: metadata is None"
        self.name = encode_metric_name(name)
        self.id = id
        self.metadata = metadata

    def __getattr__(self, name):
        return getattr(self.metadata, name)

    def __dir__(self):
        res = dir(self.metadata)
        res.extend(self.__slots__)
        res.sort()
        return res

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, Metric):
            return False
        return (self.name == other.name and
                self.metadata == other.metadata)

    def __ne__(self, other):
        return not (self == other)


class Accessor(object):
    """Provides Read/Write accessors to BigGraphite.

    It is safe to fork() or start new process until connect() has been called.
    It is not safe to share a given accessor across threads.

    Calling other methods before connect() will raise NotConnectedError, unless
    noted otherwise.
    """

    __slots__ = ('is_connected', )

    __metaclass__ = abc.ABCMeta

    def __init__(self, backend_name):
        """Set internal variables."""
        self.backend_name = backend_name
        self.is_connected = False
        self.cache = None
        self.cache_data_ttl = None
        self.cache_metadata_ttl = None

    def __enter__(self):
        """Call connect()."""
        self.connect()
        return self

    def __exit__(self, _type, _value, _traceback):
        """Call shutdown()."""
        self.shutdown()
        return False

    def set_cache(self, cache, data_ttl=60, metadata_ttl=60):
        """Allows a caller to set a cache for this accessor.

        Args:
          cache: an AccessorCache or similar.
          data_ttl: int, TTL in seconds to apply to data.
          metadata_ttl: int, TTL in seconds to apply to metadata.
        """
        self.cache = cache
        self.cache_data_ttl = data_ttl
        self.cache_metadata_ttl = metadata_ttl

    @abc.abstractmethod
    def connect(self):
        """Establish a connection, idempotent.

        This must be called AFTER creating subprocess with the multiprocessing module.
        """
        pass

    def syncdb(self, retentions=None, dry_run=False):
        """Create the database schema.

        Args:
          retentions, iterable or None, list of retentions to create.
          dry_run: bool, if True nothing will be applied to the database.
        """
        pass

    @abc.abstractmethod
    def create_metric(self, metric):
        """Create a metric from its definition as a Metric object.

        Parent directories are implicitly created.
        This can be expensive, it is worthwile to first check if the metric exists.

        Args:
          metric: definition as a Metric object.
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        self._check_connected()

    @abc.abstractmethod
    def update_metric(self, name, updated_metadata):
        """Update a Metric object from its definition as a Metric object.

        Args:
          name: metric name.
          updated_metadata: updated metric metadata.
        """
        self._check_connected()

    @abc.abstractmethod
    def delete_metric(self, name):
        """Delete a metric.

        Args:
          name: metric name.
        """
        self._check_connected()

    @abc.abstractmethod
    def delete_directory(self, name):
        """Delete a directory.

        Args:
          name: directory name.
        """
        self._check_connected()

    def _check_connected(self):
        if not self.is_connected:
            raise Error("Accessor's connect() wasn't called")

    @abc.abstractmethod
    def drop_all_metrics(self):
        """Delete all metrics from the database."""
        self._check_connected()

    @abc.abstractmethod
    def fetch_points(self, metric, time_start, time_end, stage, aggregated=True):
        """Fetch points from time_start included to time_end excluded.

        Args:
          metric: The metric definition as per get_metric.
          time_start: timestamp in seconds from the Unix Epoch as an int, inclusive,
            must be a multiple of stage.precision
          time_end: timestamp in seconds from the Unix Epoch as an int, exclusive,
            must be a multiple of stage.precision
          stage: the retention stage at which to fetch data
          aggregated : flag used by the returned PointGrouper

        Yields:
          pairs of (timestamp, value) as default,
          or tuples of (timestamp, value, count) if "aggregated" flag is False,
          to indicate value is an aggregate for the range [timestamp, timestamp+stage.precision[

        Raises:
          InvalidArgumentError: if time_start or time_end are not as per above
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        if not isinstance(stage, Stage):
            raise InvalidArgumentError("%s is not a Stage instance" % stage)
        if time_start % stage.precision or time_start < 0:
            raise InvalidArgumentError(
                "time_start (%d) is not a multiple of the stage's precision (%s)" % (
                    time_start, stage.as_string))
        if time_end % stage.precision or time_end < 0:
            raise InvalidArgumentError(
                "time_end (%d) is not a multiple of the stage's precision (%s)" % (
                    time_end, stage.as_string))

    @abc.abstractmethod
    def has_metric(self, metric_name):
        """Return a True if this metric exists, else return False."""
        self._check_connected()

    @abc.abstractmethod
    def get_metric(self, metric_name, touch=False):
        """Return a Metric for this metric_name, None if no such metric."""
        self._check_connected()

    @abc.abstractmethod
    def make_metric(self, name, metadata):
        """Create a Metric object from its definition as name and metadata.

        Args:
          name: metric name.
          metadadata: metric metadata.

        Returns: a Metric object with a valid id.
        """

    @abc.abstractmethod
    def glob_metric_names(self, glob):
        """Return a sorted list of metric names matching this glob."""
        self._check_connected()

    @abc.abstractmethod
    def glob_directory_names(self, glob):
        """Return a sorted list of metric directories matching this glob."""
        self._check_connected()

    @abc.abstractmethod
    def background(self):
        """Perform background operations, should be called every minute."""
        pass

    @abc.abstractmethod
    def flush(self):
        """Flush any internal buffers."""
        pass

    def insert_points(self, metric, datapoints):
        """Insert points for a given metric.

        Args:
          metric: A Metric instance.
          datapoints: An iterable of (timestamp in seconds, values as double)
        """
        self._check_connected()
        _wait_async_call(self.insert_points_async,
                         metric=metric, datapoints=datapoints)

    @abc.abstractmethod
    def insert_points_async(self, metric, datapoints, on_done=None):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          datapoints: An iterable of (timestamp in seconds, values as double)
          on_done(e: Exception): called on done, with an exception or None if succesfull
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        self._check_connected()

    def insert_downsampled_points(self, metric, datapoints):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          datapoints: An iterable of (timestamp in seconds, values as double, count as int, stage)
        """
        self._check_connected()
        _wait_async_call(
            self.insert_downsampled_points_async, metric=metric, datapoints=datapoints)

    @abc.abstractmethod
    def insert_downsampled_points_async(self, metric, datapoints, on_done=None):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          datapoints: An iterable of (timestamp in seconds, values as double, count as int, stage)
          on_done(e: Exception): called on done, with an exception or None if succesfull
        """
        if not isinstance(metric, Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        self._check_connected()

    @abc.abstractmethod
    def map(self, callback, start_key=None, end_key=None, shard=0, nshards=1):
        """Call callback on each metric.

        This operation can potentially be very slow.

        This will list each metrics matching the arguments and call
        `callback(metric, done, total)`.

        Args:
          callback: callable(metric: Metric, done: int, total: int)
          start_key: string, start at key >= start_key.
          end_key: string, stop at key < end_key.
          shard: int, shard to repair.
          nshards: int, number of shards.

        """
        assert shard >= 0
        assert nshards > 0
        assert shard < nshards
        self._check_connected()

    @abc.abstractmethod
    def repair(self, start_key=None, end_key=None, shard=0, nshards=1, callback_on_progress=None):
        """Repair potential corruptions in the database.

        This operation can potentially be very slow.

        During the repair the keyspace is split in n shards and
        this function will only take care of 1/n th of the data
        as specified by shard. This allows the caller to parallelize
        the repair if needed.

        Args:
          start_key: string, start at key >= start_key.
          end_key: string, stop at key < end_key.
          shard: int, shard to repair.
          nshards: int, number of shards.
          callback_on_progress: Take 2 parameters, work done so far, total work to do

        """
        assert shard >= 0
        assert nshards > 0
        assert shard < nshards
        self._check_connected()

    @abc.abstractmethod
    def shutdown(self):
        """Close the connection.

        This is safe to call even if connect() was never called.
        """
        pass

    @abc.abstractmethod
    def touch_metric(self, metric_name):
        """Update a metric to refresh its last write timestamp."""
        self._check_connected()

    @abc.abstractmethod
    def clean(self, max_age=None, start_key=None, end_key=None, shard=1, nshards=0,
              callback_on_progress=None):
        """Remove metrics that have expired (not used anymore)."""
        self._check_connected()


class PointGrouper(object):
    """Helper for client-side aggregator.

    Prepare pairs of (timestamp, value) as default,
    or tuples of (timestamp, value, count) if "aggregated" flag is False.
    It hardcodes a knowledge of how Casssandra results are returned together, this should be
    abstracted away if more datastores do client-side agregation.
    """

    def __init__(self, metric, time_start_ms, time_end_ms, stage, query_results,
                 source_stage=None, aggregated=True):
        """Constructor for PointGrouper.

        Args:
          metric: The metric for which to group values.
          time_start_ms: timestamp in second from the Epoch as an int,
            inclusive,  must be a multiple of stage.precision
          time_end_ms: timestamp in second from the Epoch as an int,
            exclusive, must be a multiple of stage.precision
          stage: the retention stage we are producing points for
          query_results: query results to fetch values from.
          source_stage: the retentation stage we are consuming points from.
            if None, this is equivalent to stage.
          aggregated: flag to specify if points should be aggregated or just merged.
        """
        self.metric = metric
        self.time_start_ms = time_start_ms
        self.time_end_ms = time_end_ms
        self.stage = stage
        self.source_stage = source_stage or stage
        self.query_results = query_results
        self.aggregated = aggregated
        self.simple = self.source_stage.stage0 and self.stage == self.source_stage
        if not self.simple:
            self.current_values = []
            self.current_counts = []
            for r in range(SHARD_MAX_REPLICAS):
                self.current_values.append(array.array("d"))
                self.current_counts.append(array.array("l"))
            self.current_timestamp_ms = None

    def __iter__(self):
        if not self.simple:
            return self.generate_values_aggregated()
        else:
            return self.generate_values_stage0()

    def run_aggregator(self):
        """Aggregate or merge values in current_values.

        This will skip the first point and return (None, None, None)
        if the function doesn't generate any aggregated or merged point.
        """
        # This is the first point we encounter, do not emit it on its own,
        # rather wait until we have found values fitting in the next period.
        ret = (None, None, None)
        max_count = 0
        if self.current_timestamp_ms is None:
            return ret
        # Try to find the most complete replica and return its results. It would
        # be nice to implement all that in the database directly if we can.
        for r in range(SHARD_MAX_REPLICAS):
            r_count = sum(self.current_counts[r])
            if not r_count:
                continue
            if self.aggregated:
                count = None
                value = self.metric.metadata.aggregator.aggregate(
                    values=self.current_values[r],
                    counts=self.current_counts[r],
                    newest_first=True,
                )
            else:
                value, count = self.metric.metadata.aggregator.merge(
                    values=self.current_values[r],
                    counts=self.current_counts[r],
                    newest_first=True,
                )
            if value is not None:
                if r_count > max_count:
                    ret = (self.current_timestamp_ms / 1000.0, value, count)
                    max_count = r_count
                del self.current_values[r][:]
                del self.current_counts[r][:]
        return ret

    def generate_values_aggregated(self):
        """Generator function, consume query_results and produce values."""
        first_exc = None
        same_stage = self.stage == self.source_stage
        aggregated_stage = self.source_stage.aggregated()

        for successful, rows_or_exception in self.query_results:
            if not successful:
                first_exc = rows_or_exception
            if first_exc:
                # A query failed, we still consume the results
                continue
            for row in rows_or_exception:
                if aggregated_stage:
                    (time_start_ms, offset, shard, value, count) = row
                else:
                    (time_start_ms, offset, value) = row
                    shard = 0
                    count = 1

                # Find the replica id from the shard id.
                replica = (shard & SHARD_REPLICA_MASK) >> SHARD_REPLICA_SHIFT
                timestamp_ms = (
                    time_start_ms + offset * self.source_stage.precision_ms)

                assert timestamp_ms >= self.time_start_ms
                assert timestamp_ms < self.time_end_ms
                if not same_stage:
                    timestamp_ms = round_down(
                        timestamp_ms, self.stage.precision_ms)

                if self.current_timestamp_ms != timestamp_ms:
                    # This needs to be optimized because in the common case
                    # there is absolutely nothing to aggregate.
                    ts, _value, _count = self.run_aggregator()
                    if ts is not None:
                        yield (ts, _value) if self.aggregated else (ts, _value, _count)

                    self.current_timestamp_ms = timestamp_ms

                self.current_values[replica].append(value)
                self.current_counts[replica].append(count)

        if not same_stage or aggregated_stage:
            ts, _value, _count = self.run_aggregator()
            if ts is not None:
                yield (ts, _value) if self.aggregated else (ts, _value, _count)

        if first_exc:
            raise RetryableError(first_exc)

    def generate_values_stage0(self):
        """Generator function, consume query_results and produce values."""
        first_exc = None

        for successful, rows_or_exception in self.query_results:
            if not successful:
                first_exc = rows_or_exception
            if first_exc:
                # A query failed, we still consume the results
                continue
            for row in rows_or_exception:
                (time_start_ms, offset, value) = row

                timestamp_ms = (
                    time_start_ms + offset * self.source_stage.precision_ms)

                assert timestamp_ms >= self.time_start_ms
                assert timestamp_ms < self.time_end_ms

                # Non aggregated stages are pretty simple, nothing to do.
                yield ((timestamp_ms / 1000.0, value) if self.aggregated
                       else (timestamp_ms / 1000.0, value, 1))

        if first_exc:
            raise RetryableError(first_exc)
