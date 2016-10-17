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
"""Adapter between BigGraphite and Carbon."""
from __future__ import absolute_import  # Otherwise carbon is this module.

# Version 0.9.15 (the one from PIP) does not have the "database" module.
# To make use of the plugin with carbon, you will need a version that has
# upstream commit 3d260b0f663b5577bc3a0fc3f0741802109a28c4 or apply this
# patch: https://goo.gl/1gAcz1 .
# test-requirements.txt as a URL pinned at the correct version.
from carbon import database

from biggraphite import graphite_utils
from biggraphite import accessor

# Ignore D102: Missing docstring in public method: Most of them come from upstream module.
# pylama:ignore=D102


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
        name for name, _ in list(accessor.Aggregator.__members__.items())]

    # See class pydoc for the rational.
    _SYNC_EVERY_N_WRITE = 10

    def __init__(self, settings):
        self._cache = None
        self._accessor = None
        self._settings = settings
        self._sync_countdown = 0

    def accessor(self):
        if not self._accessor:
            accessor = graphite_utils.accessor_from_settings(self._settings)
            accessor.connect()
            self._accessor = accessor

        return self._accessor

    def cache(self):
        if not self._cache:
            cache = graphite_utils.cache_from_settings(self.accessor(), self._settings)
            cache.open()
            self._cache = cache

        return self._cache

    def write(self, metric_name, datapoints):
        # Get a Metric object from metric name.
        metric = self.cache().get_metric(metric_name=metric_name)

        # Round down timestamp because inner functions expect integers.
        datapoints = [(int(timestamp), value) for timestamp, value in datapoints]

        # Writing every point synchronously increase CPU usage by ~300% as per https://goo.gl/xP5fD9
        if self._sync_countdown < 1:
            self.accessor().insert_points(metric=metric, datapoints=datapoints)
            self._sync_countdown = self._SYNC_EVERY_N_WRITE
        else:
            self._sync_countdown -= 1
            self.accessor().insert_points_async(metric=metric, datapoints=datapoints)

    def exists(self, metric_name):
        # If exists returns "False" then "create" will be called.
        # New metrics are also throttled by some settings.
        return bool(self.cache().has_metric(metric_name))

    def create(self, metric_name, retentions, xfilesfactor, aggregation_method):
        metadata = accessor.MetricMetadata(
            aggregator=accessor.Aggregator.from_carbon_name(aggregation_method),
            retention=accessor.Retention.from_carbon(retentions),
            carbon_xfilesfactor=xfilesfactor,
        )
        metric = self.cache().make_metric(metric_name, metadata)
        self.cache().create_metric(metric)

    def getMetadata(self, metric_name, key):
        if key != "aggregationMethod":
            msg = "%s[%s]: Unsupported metadata" % (metric_name, key)
            raise ValueError(msg)
        metadata = self.cache().get_metric(metric_name=metric_name)
        if not metadata:
            raise ValueError("%s: No such metric" % metric_name)
        assert metadata.aggregator.carbon_name
        return metadata.aggregator.carbon_name

    def setMetadata(self, metric_name, key, value):
        old_value = self.getMetadata(metric_name, key)
        # Changing aggregation or xfilesfactor requires invalidating aggregates and caches,
        # and the meaning of existing data would be ambiguous at best.
        # Changing retention would be feasible but we'll probably end up restraining
        # metadata to be by determined by prefix/suffix in the metric name for
        # efficiency.
        if old_value != value:
            msg = "%s[%s]=%s: Changing metadata is not supported" % (metric_name, key, value)
            raise ValueError(msg)
        return old_value

    def getFilesystemPath(self, metric_name):
        # Only used for logging.
        return "/".join(("//biggraphite", self.accessor().backend_name, metric_name))

    def validateArchiveList(self, archiveList):
        # TODO(c.chary): maybe run repair() ?
        pass


class MultiDatabase(database.TimeSeriesDatabase):
    """Multi Database plugin for Carbon.

    The class definition registers the plugin thanks to TimeSeriesDatabase's
    metaclass.

    This class allows using multiple existing database plugins at the same time.
    """

    def __init__(self, dbs):
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
                self.aggregationMethods = (
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
            self._biggraphite = BigGraphiteDatabase(settings)
            self._whisper = database.WhisperDatabase(settings)
            MultiDatabase.__init__(self, [self._whisper, self._biggraphite])

    class BigGraphiteAndWhisperDatabase(MultiDatabase):
        """BigGraphite then Whisper."""

        plugin_name = "biggraphite+whisper"
        aggregationMethods = BigGraphiteDatabase.aggregationMethods

        def __init__(self, settings):
            self._biggraphite = BigGraphiteDatabase(settings)
            self._whisper = database.WhisperDatabase(settings)
            MultiDatabase.__init__(self, [self._biggraphite, self._whisper])
