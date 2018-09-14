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
import threading

from biggraphite import metric as bg_metric
from biggraphite import utils as bg_utils


class Error(Exception):
    """Base class for all exceptions from this module."""


class RetryableError(Error):
    """Errors accessing Cassandra that could succeed if retried."""


class InvalidArgumentError(Error):
    """Callee did not follow requirements on the arguments."""


# Number of bits allocated to dinstinguish replicas in the shard
# id. The default is 2 which means that you can have up to 3 replicas
# writting a the same time to your database.
# See CASSANDRA_DESIGN.md for details. This should eventually move off
# to drivers/ and be used only by drivers who needs that, they should in
# turn just forward the replica id instead of the shard id.
SHARD_REPLICA_MASK = 0xC000
SHARD_WRITER_MASK = SHARD_REPLICA_MASK ^ 0xFFFF
SHARD_REPLICA_BITS = bin(SHARD_REPLICA_MASK).count("1")
SHARD_WRITER_BITS = 16 - SHARD_REPLICA_BITS
SHARD_REPLICA_SHIFT = SHARD_WRITER_BITS
SHARD_MAX_REPLICAS = 2 ** SHARD_REPLICA_BITS


def pack_shard(replica, writer):
    """Pack a replica id and writer id in a short."""
    return (replica << SHARD_REPLICA_SHIFT) | (writer & SHARD_WRITER_MASK)


def unpack_shard(shard):
    """Unpack what pack_shard() did."""
    replica = (shard & SHARD_REPLICA_MASK) >> SHARD_REPLICA_SHIFT
    writer = shard & SHARD_WRITER_MASK
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
    # c.f. https://github.com/criteo/biggraphite/issues/296
    # under higher load BG will freeze, blocking on this wait()
    event.wait(3.0)
    if exception_box[0]:
        raise exception_box[0]


class Accessor(object):
    """Provides Read/Write accessors to BigGraphite.

    It is safe to fork() or start new process until connect() has been called.
    It is not safe to share a given accessor across threads.

    Calling other methods before connect() will raise NotConnectedError, unless
    noted otherwise.
    """

    __slots__ = ("is_connected",)

    __metaclass__ = abc.ABCMeta

    def __init__(self, backend_name):
        """Set internal variables."""
        self.backend_name = backend_name
        self.is_connected = False
        self.cache = None
        self.cache_data_ttl = None
        self.cache_metadata_ttl = None
        self.metadata_enabled = True

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
        if not isinstance(metric, bg_metric.Metric):
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
        if not isinstance(metric, bg_metric.Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        if not isinstance(stage, bg_metric.Stage):
            raise InvalidArgumentError("%s is not a Stage instance" % stage)
        if time_start % stage.precision or time_start < 0:
            raise InvalidArgumentError(
                "time_start (%d) is not a multiple of the stage's precision (%s)"
                % (time_start, stage.as_string)
            )
        if time_end % stage.precision or time_end < 0:
            raise InvalidArgumentError(
                "time_end (%d) is not a multiple of the stage's precision (%s)"
                % (time_end, stage.as_string)
            )

    @abc.abstractmethod
    def has_metric(self, metric_name):
        """Return a True if this metric exists, else return False."""
        self._check_connected()

    @abc.abstractmethod
    def get_metric(self, metric_name):
        """Return a Metric for this metric_name, None if no such metric."""
        self._check_connected()

    @abc.abstractmethod
    def glob_metric_names(self, glob, start_time=None, end_time=None):
        """Return a sorted list of metric names matching this glob."""
        self._check_connected()

    @abc.abstractmethod
    def glob_metrics(self, glob, start_time=None, end_time=None):
        """Return a sorted list of metrics matching this glob."""
        self._check_connected()

    @abc.abstractmethod
    def glob_directory_names(self, glob, start_time=None, end_time=None):
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
        _wait_async_call(self.insert_points_async, metric=metric, datapoints=datapoints)

    @abc.abstractmethod
    def insert_points_async(self, metric, datapoints, on_done=None):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          datapoints: An iterable of (timestamp in seconds, values as double)
          on_done(e: Exception): called on done, with an exception or None if succesfull
        """
        if not isinstance(metric, bg_metric.Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        self._check_connected()

        if self.metadata_enabled:
            self.touch_metric(metric)

    def insert_downsampled_points(self, metric, datapoints):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          datapoints: An iterable of (timestamp in seconds, values as double, count as int, stage)
        """
        self._check_connected()
        _wait_async_call(
            self.insert_downsampled_points_async, metric=metric, datapoints=datapoints
        )

    @abc.abstractmethod
    def insert_downsampled_points_async(self, metric, datapoints, on_done=None):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          datapoints: An iterable of (timestamp in seconds, values as double, count as int, stage)
          on_done(e: Exception): called on done, with an exception or None if succesfull
        """
        if not isinstance(metric, bg_metric.Metric):
            raise InvalidArgumentError("%s is not a Metric instance" % metric)
        self._check_connected()

    @abc.abstractmethod
    def map(
        self,
        callback,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        errback=None,
        callback_on_progress=None,
    ):
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
          errback: callable(metric: string) when a corrupted metric is found
          callback_on_progress: Take 2 parameters, work done so far, total work to do.
        """
        self._check_connected()

    @abc.abstractmethod
    def repair(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """Repair potential corruptions in the database.

        This operation can potentially be very slow.

        During the repair the metric space is split in n shards and
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
    def clean(
        self,
        max_age=None,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """Remove metrics older than @max_age, considered to have expired (not used anymore)."""
        self._check_connected()

    @abc.abstractmethod
    def shutdown(self):
        """Close the connection.

        This is safe to call even if connect() was never called.
        """
        pass

    @abc.abstractmethod
    def touch_metric(self, metric):
        """Update a metric to refresh its last write timestamp."""
        self._check_connected()


class PointGrouper(object):
    """Helper for client-side aggregator.

    Prepare pairs of (timestamp, value) as default,
    or tuples of (timestamp, value, count) if "aggregated" flag is False.
    It hardcodes a knowledge of how Casssandra results are returned together, this should be
    abstracted away if more datastores do client-side agregation.
    """

    def __init__(
        self,
        metric,
        time_start_ms,
        time_end_ms,
        stage,
        query_results,
        source_stage=None,
        aggregated=True,
    ):
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
                timestamp_ms = time_start_ms + offset * self.source_stage.precision_ms

                assert timestamp_ms >= self.time_start_ms
                assert timestamp_ms < self.time_end_ms
                if not same_stage:
                    timestamp_ms = bg_utils.round_down(
                        timestamp_ms, self.stage.precision_ms
                    )

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

                timestamp_ms = time_start_ms + offset * self.source_stage.precision_ms

                assert timestamp_ms >= self.time_start_ms
                assert timestamp_ms < self.time_end_ms

                # Non aggregated stages are pretty simple, nothing to do.
                yield (
                    (timestamp_ms / 1000.0, value)
                    if self.aggregated
                    else (timestamp_ms / 1000.0, value, 1)
                )

        if first_exc:
            raise RetryableError(first_exc)
