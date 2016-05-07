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

try:
    # Version 0.9.15 (the one from PIP) does not have the "database" module.
    # Since the TimeSeriesDatabase class there has no behaviour appart from
    # registering instances, we can use "object" in this case.
    # To make use of the plugin with carbon, you will need a version that has
    # upstream commit 3d260b0f663b5577bc3a0fc3f0741802109a28c4 or apply this
    # patch: https://goo.gl/1gAcz1
    # TODO: Add this to requirements.txt and README.
    from carbon import database
    BG_DATABASE_PARENT_CLASS = database.TimeSeriesDatabase
except:
    BG_DATABASE_PARENT_CLASS = object

from carbon import exceptions
from biggraphite import accessor

# Ignore D102: Missing docstring in public method: Most of them come from upstream module.
# pylama:ignore=D102


_DEFAULT_PORT = 9042


# TODO: Add a cache for metadata. lmdb is a reasonable candidate so that the
# cache is shared by all processes and is backed by mmap'd memory.

class BigGraphiteDatabase(BG_DATABASE_PARENT_CLASS):
    """Database plugin for Carbon.

    The class definition registers the plugin thanks to TimeSeriesDatabase's metaclass.
    """

    plugin_name = "biggraphite"

    def __init__(self, settings):
        keyspace = settings.get("BG_KEYSPACE")
        contact_points_str = settings.get("BG_CONTACT_POINTS")
        port = settings.get("BG_PORT")

        if not keyspace:
            raise exceptions.CarbonConfigException("BG_KEYSPACE is mandatory")
        if not contact_points_str:
            raise exceptions.CarbonConfigException("BG_CONTACT_POINTS are mandatory")
        # BG_PORT is optional

        contact_points = [s.strip() for s in contact_points_str.split(",")]

        self._accessor = accessor.Accessor(keyspace, contact_points, port)
        self._accessor.connect()

        # TODO: we may want to use/implement these
        # settings.WHISPER_AUTOFLUSH:
        # settings.WHISPER_SPARSE
        # settings.WHISPER_FALLOCATE_CREATE:
        # settings.WHISPER_LOCK_WRITES:

    def write(self, metric, datapoints):
        self._accessor.insert_points(
            metric_name=metric, timestamps_and_values=datapoints)

    def exists(self, metric):
        # If exists returns "False" then "create" will be called.
        # New metrics are also throttled by some settings.
        return bool(self._accessor.get_metric(metric))

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
        metadata = accessor.MetricMetadata(
            name=metric,
            carbon_aggregation=aggregation_method,
            carbon_retentions=retentions,
            carbon_xfilesfactor=xfilesfactor,
        )
        self._accessor.create_metric(metadata)

    def getMetadata(self, metric, key):
        if key != "aggregationMethod":
            msg = "%s[%s]: Unsupported metadata" % (metric, key)
            raise ValueError(msg)
        metadata = self._accessor.get_metric(metric)
        if not metadata:
            raise ValueError("%s: No such metric" % metric)
        return metadata.carbon_aggregation

    def setMetadata(self, metric, key, value):
        old_value = self.getMetadata(metric, key)
        # Changing aggregation or xfilesfactor requires invalidating aggregates and caches,
        # and the meaning of existing data would be ambiguous at best.
        # Changing retention would be feasible but we'll probably end up restraining
        # metadata to be by determined by prefix/suffix in the metric name for
        # efficiency.
        if old_value != value:
            msg = "%s[%s]=%s: Changing metadata is not supported" % (metric, key, value)
            raise ValueError(msg)
        return old_value

    def getFilesystemPath(self, metric):
        # Only used for logging.
        return "/".join(("//biggraphite", self._accessor.keyspace, metric))
