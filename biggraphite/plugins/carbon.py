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

from carbon import database
from carbon import exceptions
from biggraphite import accessor

# Ignore D102: Missing docstring in public method: Most of them come from upstream module.
# pylama:ignore=D102


_DEFAULT_PORT = 9042


class BigGraphiteDatabase(database.TimeSeriesDatabase):
    """Database plugin for Carbon.

    The class definition registers the plugin thanks to TimeSeriesDatabase's metaclass.
    """

    plugin_name = "biggraphite"

    def __init__(self, settings):
        keyspace = settings.get("keyspace")
        contact_points_str = settings.get("contact_points")

        if not keyspace:
            raise exceptions.CarbonConfigException("keyspace is mandatory")

        if not contact_points_str:
            raise exceptions.CarbonConfigException(
                "contact_points are mandatory")

        contact_points = [s.strip() for s in contact_points_str.split(",")]
        port = settings.get("port")

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
        return True

    def create(self, metric, retentions, xfilesfactor, aggregation_method):
        pass

    def getMetadata(self, metric, key):
        if key != "aggregationMethod":
            raise ValueError("Unsupported metadata key \"%s\"" % key)

        # TODO: Use the accessor to determine the return value.
        return "average"

    def setMetadata(self, metric, key, value):
        # TODO: Check name and use the accessor to set the value.
        return "average"

    def getFilesystemPath(self, metric):
        # Only used for logging.
        return metric
