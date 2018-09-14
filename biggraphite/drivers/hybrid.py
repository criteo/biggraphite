# Copyright 2018 Criteo
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
"""Hybrid accessor, using one for metadata and one for data."""

from biggraphite import accessor as bg_accessor


class HybridAccessor(bg_accessor.Accessor):
    """Hybrid accessor, using one for metadata and one for data."""

    TYPE = "hybrid"

    def __init__(self, backend_name, metadata_accessor, data_accessor):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).__init__(backend_name)
        self._metadata_accessor = metadata_accessor
        self._data_accessor = data_accessor
        self.TYPE = metadata_accessor.TYPE + "+" + data_accessor.TYPE

    def connect(self):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).connect()
        self._metadata_accessor.connect()
        self._data_accessor.connect()
        self.is_connected = True

    def create_metric(self, metric):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).create_metric(metric)
        self._metadata_accessor.create_metric(metric)

    def update_metric(self, name, updated_metadata):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).update_metric(name, updated_metadata)
        self._metadata_accessor.update_metric(name, updated_metadata)

    def delete_metric(self, name):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).delete_metric(name)
        self._metadata_accessor.delete_metric(name)

    def delete_directory(self, name):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).delete_directory(name)
        self._metadata_accessor.delete_directory(name)

    def drop_all_metrics(self):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).drop_all_metrics()
        self._metadata_accessor.drop_all_metrics()
        self._data_accessor.drop_all_metrics()

    def fetch_points(self, metric, time_start, time_end, stage, aggregated=True):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).fetch_points(
            metric, time_start, time_end, stage, aggregated
        )
        self._metadata_accessor.fetch_points(
            metric, time_start, time_end, stage, aggregated
        )
        return self._data_accessor.fetch_points(
            metric, time_start, time_end, stage, aggregated
        )

    def has_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).has_metric(metric_name)
        return self._metadata_accessor.has_metric(metric_name)

    def get_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).get_metric(metric_name)
        return self._metadata_accessor.get_metric(metric_name)

    def touch_metric(self, metric):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).touch_metric(metric)
        self._metadata_accessor.touch_metric(metric)

    def glob_metrics(self, glob, start_time=None, end_time=None):
        """Return a sorted list of metrics matching this glob."""
        super(HybridAccessor, self).glob_metrics(glob, start_time, end_time)
        return self._metadata_accessor.glob_metrics(glob)

    def glob_metric_names(self, glob, start_time=None, end_time=None):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).glob_metric_names(glob)
        return self._metadata_accessor.glob_metric_names(glob)

    def glob_directory_names(self, glob, start_time=None, end_time=None):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).glob_directory_names(glob)
        return self._metadata_accessor.glob_directory_names(glob)

    def background(self):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).background()
        self._metadata_accessor.background()
        self._data_accessor.background()

    def flush(self):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).flush()
        self._metadata_accessor.flush()
        self._data_accessor.flush()

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).insert_points_async(metric, datapoints, on_done)
        # Updating metadata here would be too expensive
        self._data_accessor.insert_points_async(metric, datapoints, on_done)

    def insert_downsampled_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).insert_downsampled_points_async(
            metric, datapoints, on_done
        )
        # Updating metadata here would be too expensive
        self._data_accessor.insert_downsampled_points_async(metric, datapoints, on_done)

    def map(
        self, namespaces, start_key=None, end_key=None, shard=0, nshards=1, errback=None
    ):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).map(
            namespaces, start_key, end_key, shard, nshards, errback
        )
        return self._metadata_accessor.map(
            namespaces, start_key, end_key, shard, nshards, errback
        )

    def repair(
        self,
        start_key=None,
        end_key=None,
        shard=0,
        nshards=1,
        callback_on_progress=None,
    ):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).repair(
            start_key, end_key, shard, nshards, callback_on_progress
        )
        self._metadata_accessor.repair(
            start_key, end_key, shard, nshards, callback_on_progress
        )

    def clean(
        self,
        max_age=None,
        start_key=None,
        end_key=None,
        shard=1,
        nshards=0,
        callback_on_progress=None,
    ):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).clean(
            max_age, start_key, end_key, shard, nshards, callback_on_progress
        )
        self._metadata_accessor.clean(
            max_age, start_key, end_key, shard, nshards, callback_on_progress
        )

    def shutdown(self):
        """See the real Accessor for a description."""
        super(HybridAccessor, self).shutdown()
        self._metadata_accessor.shutdown()
        self._data_accessor.shutdown()

    def syncdb(self, retentions=None, dry_run=False):
        """See the real Accessor for a description."""
        self._metadata_accessor.syncdb(retentions, dry_run)
        self._data_accessor.syncdb(retentions, dry_run)
