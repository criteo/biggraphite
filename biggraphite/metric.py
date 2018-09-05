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
"""Metric class definition."""
import codecs
import datetime
import json
import math
import re
import uuid

import enum
import six

from biggraphite import utils as bg_utils

_UTF8_CODEC = codecs.getencoder("utf8")
_UUID_NAMESPACE = uuid.UUID("{00000000-1111-2222-3333-444444444444}")
_NAN = float("nan")


class Error(Exception):
    """Base class for all exceptions from this module."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


class Metric(object):
    """Represents all information about a metric.

    This is not an instance of MetricMetadata: It cannot be serialized
    in JSON to minimise confusion in cache that expects few possible
    Metadata at any time.

    Not meant to be mutated.
    """

    __slots__ = ("name", "id", "metadata", "created_on", "updated_on", "read_on")

    def __init__(
        self, name, id, metadata, created_on=None, updated_on=None, read_on=None
    ):
        """Record its arguments."""
        super(Metric, self).__init__()
        assert name, "Metric: name is None"
        assert id, "Metric: id is None"
        assert metadata, "Metric: metadata is None"
        self.name = encode_metric_name(name)
        self.id = id
        self.metadata = metadata
        self.created_on = created_on
        self.updated_on = updated_on
        self.read_on = read_on

    def as_string_dict(self):
        """Turn an instance into a dict of string to string."""
        return {
            "id": str(self.id),
            "name": self.name,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "read_on": self.read_on,
            "metadata": self.metadata.as_string_dict(),
        }

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
        return self.name == other.name and self.metadata == other.metadata

    def __ne__(self, other):
        return not (self == other)


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
        assert (name) is not bytes, "%s should not be of type 'bytes'" % name
        return name

    if isinstance(name, str):
        return name
    # Next line may raise UnicodeError
    return _UTF8_CODEC(name)[0]


def make_metric(name, metadata, created_on=None, updated_on=None, read_on=None):
    """Create a Metric object from its definition as name and metadata.

    Args:
      name: metric name.
      metadata: metric metadata.
      created_on: metric creation date.
      updated_on: metric last update date.
      read_on: metric last read date.

    Returns: a Metric object with a valid id.
    """
    encoded_name = encode_metric_name(
        sanitize_metric_name(name)
    )
    uid = uuid.uuid5(_UUID_NAMESPACE, encoded_name)
    now = datetime.datetime.now()
    return Metric(
        encoded_name,
        uid,
        metadata,
        created_on=created_on or now,
        updated_on=updated_on or now,
        read_on=read_on,
    )


def sanitize_metric_name(name):
    """Sanitize a metric name by removing double dots.

    :param name: Metric name
    :return: Sanitized metric name
    """
    if name is None:
        return None
    return ".".join(_components_from_name(name))


def _components_from_name(metric_name):
    res = metric_name.split(".")
    return list(filter(None, res))


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

    __slots__ = ("duration", "points", "precision", "stage0")

    # Parses the values of as_string into points and precision group
    _STR_RE = re.compile(
        r"^(?P<points>[\d]+)\*(?P<precision>[\d]+)s(?P<type>(_0|_aggr))?$"
    )

    def __init__(self, points, precision, stage0=False):
        """Set attributes."""
        self.points = int(points)
        self.precision = int(precision)
        self.duration = self.points * self.precision
        self.stage0 = stage0

    def __str__(self):
        return self.as_string

    def __repr__(self):
        return "<{0}.{1}({3}) object at {2}>".format(
            self.__module__, type(self).__name__, hex(id(self)), self.as_string
        )

    def __eq__(self, other):
        if not isinstance(other, Stage):
            return False
        return (
            self.points == other.points
            and self.precision == other.precision
            and self.stage0 == other.stage0
        )

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
            ret += "_0"
        else:
            ret += "_aggr"
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
            points=int(groups["points"]),
            precision=int(groups["precision"]),
            stage0=bool(groups["type"] == "_0"),
        )

    @property
    def precision_ms(self):
        """The precision of this stage in milliseconds."""
        return self.precision * 1000

    def round_down(self, timestamp):
        """Round down a timestamp to a multiple of the precision."""
        return bg_utils.round_down(timestamp, self.precision)

    def round_up(self, timestamp):
        """Round down a timestamp to a multiple of the precision."""
        return bg_utils.round_up(timestamp, self.precision)

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

    __slots__ = ("stages",)

    def __init__(self, stages):
        """Set self.stages ."""
        prev = None
        if not stages:
            raise InvalidArgumentError("there must be at least one stage")
        for s in stages:
            if prev and s.precision % prev.precision:
                raise InvalidArgumentError(
                    "precision of %s must be a multiple of %s" % (s, prev)
                )
            if prev and prev.duration >= s.duration:
                raise InvalidArgumentError(
                    "duration of %s must be lesser than %s" % (s, prev)
                )
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
        stages = [Stage(points=points, precision=precision) for precision, points in l]
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

    __slots__ = ("aggregator", "retention", "carbon_xfilesfactor")

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
