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
"""Adapter between BigGraphite and Carbon."""
from __future__ import absolute_import  # Otherwise carbon is this module.

import time
from six.moves import queue
import prometheus_client

try:
    from graphite.tags import utils as tags_utils
    from biggraphite.plugins import tags
    # TODO: change this once the code actually works.
    HAVE_TAGS = False
except ImportError:
    HAVE_TAGS = False

from carbon import database
from carbon import log
from twisted.internet import task

from biggraphite import utils
from biggraphite import graphite_utils
from biggraphite import accessor


# Ignore D102: Missing docstring in public method: Most of them come from upstream module.
# pylama:ignore=D102

WRITE_TIME = prometheus_client.Histogram(
    "bg_write_latency_seconds", "write latency in seconds",
    buckets=(0.005, .01, .025, .05, .075,
             .1, .25, .5, .75,
             1.0, 2.5, 5.0, 7.5))
CREATE_TIME = prometheus_client.Summary(
    "bg_create_latency_seconds", "create latency in seconds")
CREATES = prometheus_client.Counter("bg_creates", "metric creations")


class BigGraphiteDatabase(database.TimeSeriesDatabase):
    """Database plugin for Carbon.

    The class definition registers the plugin thanks to TimeSeriesDatabase's metaclass.

    It performs an asynchronous (non durable) write most of the time, except once
    every _SYNC_EVERY_N_WRITE to give errors a chance to bubble up and bound the
    number of points we may lose when the process terminates.
    Writing every point synchronously increase CPU usage by ~300% as per https://goo.gl/xP5fD9 .
    """

    plugin_name = "biggraphite"
    aggregationMethods = [
        member.value for member in list(accessor.Aggregator)]

    # See class pydoc for the rational.
    _SYNC_EVERY_N_WRITE = 10

    def __init__(self, settings):
        """Create a BigGraphiteDatabase."""
        try:
            super(BigGraphiteDatabase, self).__init__(settings)
        except TypeError:
            # For backward compatibility with 1.0.
            super(BigGraphiteDatabase, self).__init__()

        self._cache = None
        self._accessor = None
        self._tagdb = None
        self._settings = settings
        self._metricsToCreate = queue.Queue()
        self._sync_countdown = 0

        utils.start_admin(utils.settings_from_confattr(settings))
        self.reactor.addSystemEventTrigger('before', 'shutdown', self._flush)
        self.reactor.callInThread(self._createMetrics)
        self._lc = task.LoopingCall(self._background)
        self._lc.start(settings['CARBON_METRIC_INTERVAL'], now=False)

    @property
    def reactor(self):
        # We are included first, but we don't want to create the reactor.
        from twisted.internet import reactor
        return reactor

    @property
    def tagdb(self):
        if not HAVE_TAGS:
            return None

        if not self._tagdb:
            accessor = self.accessor
            cache = self.cache
            if accessor and cache:
                self._tagdb = tags.BigGraphiteTagDB(
                    accessor=accessor, metadata_cache=cache)

        return self._tagdb

    @property
    def accessor(self):
        if not self._accessor:
            accessor = graphite_utils.accessor_from_settings(self._settings)
            accessor.connect()
            self._accessor = accessor

        return self._accessor

    @property
    def cache(self):
        if not self._cache:
            cache = graphite_utils.cache_from_settings(
                self.accessor, self._settings)
            cache.open()
            self._cache = cache

        return self._cache

    def encode(self, metric_name):
        if HAVE_TAGS:
            return tags_utils.TaggedSeries.encode(metric_name)
        else:
            return metric_name

    @WRITE_TIME.time()
    def write(self, metric_name, datapoints):
        metric_name = self.encode(metric_name)

        # Get a Metric object from metric name.
        metric = self.cache.get_metric(metric_name=metric_name, touch=True)
        if not metric:
            raise Exception('Could not find %s' % (metric_name))

        # Round down timestamp because inner functions expect integers.
        datapoints = [(int(timestamp), value)
                      for timestamp, value in datapoints]

        # Writing every point synchronously increase CPU usage by ~300% as per https://goo.gl/xP5fD9
        if self._sync_countdown < 1:
            self.accessor.insert_points(metric=metric, datapoints=datapoints)
            self._sync_countdown = self._SYNC_EVERY_N_WRITE
        else:
            self._sync_countdown -= 1
            self.accessor.insert_points_async(
                metric=metric, datapoints=datapoints)

    def exists(self, metric_name):
        return self.cache.cache_has(metric_name)

    @CREATE_TIME.time()
    def create(self, metric_name, retentions, xfilesfactor, aggregation_method):
        orig_metric_name = metric_name
        metric_name = self.encode(metric_name)

        metadata = accessor.MetricMetadata(
            aggregator=accessor.Aggregator.from_carbon_name(
                aggregation_method),
            retention=accessor.Retention.from_carbon(retentions),
            carbon_xfilesfactor=xfilesfactor,
        )
        metric = self.cache.make_metric(metric_name, metadata)
        self.cache.cache_set(metric_name, metric)
        self._createAsync(metric, orig_metric_name)

    def tag(self, metric):
        # FIXME: We probably don't want this to be synchronous.
        if not HAVE_TAGS:
            return
        self.tagdb.tag_series(metric)

    def getMetadata(self, metric_name, key):
        metric_name = self.encode(metric_name)
        metadata = self.cache.get_metric(metric_name=metric_name, touch=True)

        if not metadata:
            raise ValueError("%s: No such metric" % metric_name)

        if key == "aggregationMethod":
            assert metadata.aggregator.carbon_name
            return metadata.aggregator.carbon_name

        try:
            return metadata.__getattr__(key)
        except AttributeError:
            msg = "%s[%s]: Unsupported metadata" % (metric_name, key)
            raise ValueError(msg)

    def setMetadata(self, metric_name, key, value):
        metric_name = self.encode(metric_name)
        old_value = self.getMetadata(metric_name, key)
        if old_value != value:
            metadata = self.cache.get_metric(
                metric_name=metric_name, touch=True)
            if not metadata:
                raise ValueError("%s: No such metric" % metric_name)

            try:
                if key == "aggregationMethod":
                    metadata.aggregator.carbon_name = value
                else:
                    setattr(metadata.metadata, key, value)
            except AttributeError:
                msg = "%s[%s]: Unsupported metadata" % (metric_name, key)
                raise ValueError(msg)
            self.accessor.update_metric(metric_name, metadata.metadata)

    def getFilesystemPath(self, metric_name):
        return "/".join(("//biggraphite", self.accessor.backend_name, metric_name))

    def validateArchiveList(self, archiveList):
        pass

    def _flush(self):
        log.cache("flushing the accessor")
        if self._accessor:
            self.accessor.flush()

    def _background(self):
        # import here because else it will import cache.py too early
        from carbon import instrumentation

        log.cache("background operations")
        for metric in prometheus_client.REGISTRY.collect():
            for name, labels, value in metric.samples:
                name = name
                if labels:
                    name += "." + ".".join(
                        ["%s.%s" % (k, v.replace('.', '_'))
                         for k, v in sorted(labels.items())])
                instrumentation.cache_record(name, value)
        if self._accessor:
            self.reactor.callInThread(self.accessor.background)

    def _createAsync(self, metric, metric_name):
        """Add metric to the queue of metrics to create.

        Args:
          metric: a Metric object
          metric_name: the original, un-encoded metric name
        """
        self._metricsToCreate.put((metric, metric_name))

    def _createMetrics(self):
        # We create metrics in a separate thread because this is potentially
        # slow and we don't want to slow down the main ingestion thread.
        # With a cold cache we will put all the metric in the queue and
        # asynchronously create the non-existing one but the points will
        # be written as soon as they are received.
        while self.reactor.running:
            try:
                self._createOneMetric()
                # Hard limit to 300 creations per seconds. This is mostly
                # to give priority to other threads. A typical carbon instance
                # can handle up to 200k metrics per second so it will take
                # ~10 minutes to check all metrics.
                time.sleep(0.003)
            except Exception:
                log.err()
                # Give the system time to recover, errors might be related
                # to the current load.
                time.sleep(1)

    def _createOneMetric(self):
        try:
            metric, metric_name = self._metricsToCreate.get(True, 1)
        except queue.Empty:
            return

        existing_metric = self.accessor.get_metric(metric.name, touch=True)

        if metric == existing_metric:
            return

        if existing_metric:
            # The retention policy is different, update it.
            log.creates("updating database metric %s (%s)" % (
                metric.name, metric.metadata.as_string_dict()))
        else:
            # The metric doesn't exists, create it.
            log.creates("creating database metric %s" % metric.name)

        self.cache.create_metric(metric)
        self.tag(metric_name)
        CREATES.inc()


class MultiDatabase(database.TimeSeriesDatabase):
    """Multi Database plugin for Carbon.

    The class definition registers the plugin thanks to TimeSeriesDatabase's
    metaclass.

    This class allows using multiple existing database plugins at the same time.
    """

    def __init__(self, settings, dbs):
        """Create a MultiDatabase."""
        try:
            super(MultiDatabase, self).__init__(settings)
        except TypeError:
            super(MultiDatabase, self).__init__()

        assert len(dbs) > 1
        self._dbs = dbs
        # Support the intersection of both.
        self.aggregationMethods = []
        for db in dbs:
            if not hasattr(db, 'aggregationMethods'):
                continue
            if not self.aggregationMethods:
                self.aggregationMethods = db.aggregationMethods
            else:
                self.aggregationMethods = list(
                    set(self.aggregationMethods) &
                    set(db.aggregationMethods))

    def write(self, metric_name, datapoints):
        for db in self._dbs:
            db.write(metric_name, datapoints)

    def exists(self, metric_name):
        for db in self._dbs:
            if not db.exists(metric_name):
                return False
        return True

    def create(self, metric_name, retentions, xfilesfactor, aggregation_method):
        for db in self._dbs:
            if db.exists(metric_name):
                continue
            db.create(
                metric_name, retentions, xfilesfactor, aggregation_method)

    def getMetadata(self, metric_name, key):
        return self._dbs[0].getMetadata(metric_name, key)

    def setMetadata(self, metric_name, key, value):
        self._dbs[0].setMetadata(metric_name, key, value)

    def getFilesystemPath(self, metric_name):
        return metric_name

    def validateArchiveList(self, archiveList):
        for db in self._dbs:
            db.validateArchiveList(archiveList)


if hasattr(database, 'WhisperDatabase'):
    class WhisperAndBigGraphiteDatabase(MultiDatabase):
        """Whisper then BigGraphite."""

        plugin_name = "whisper+biggraphite"
        aggregationMethods = BigGraphiteDatabase.aggregationMethods

        def __init__(self, settings):
            """Create a WhisperAndBigGraphiteDatabase."""
            self._biggraphite = BigGraphiteDatabase(settings)
            self._whisper = database.WhisperDatabase(settings)
            MultiDatabase.__init__(
                self, settings, [self._whisper, self._biggraphite])

    class BigGraphiteAndWhisperDatabase(MultiDatabase):
        """BigGraphite then Whisper."""

        plugin_name = "biggraphite+whisper"
        aggregationMethods = BigGraphiteDatabase.aggregationMethods

        def __init__(self, settings):
            """Create a BigGraphiteDatabaseAndWhisper."""
            self._biggraphite = BigGraphiteDatabase(settings)
            self._whisper = database.WhisperDatabase(settings)
            MultiDatabase.__init__(
                self, settings, [self._biggraphite, self._whisper])
