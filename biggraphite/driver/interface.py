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
"""WIP of interface of DB driver to extract, support downsampling and server side aggregation.

# Work will happen in three phases:
1 - Extract Cassandra code from accessor in a Driver, Accessor takes driver.
2 - Change data table to match schemas that follows but only write most precise retention
3 - Write all retention data
4 - (probably will never happens) write a trigger to keep track of min/max more efficiently

The interface proposed here is for after phase 1.
"""

import abc

class Driver(object):

    __metaclass__ = abc.ABCMeta

    def connect(self, skip_schema_upgrade=False):
        """Establish a connection.

        This must be called AFTER creating subprocess with the multiprocessing module.
        """

    def shutdown(self):
        """Close the connection to Cassandra."""


    def create_metric(self, metric_metadata):
        """Create a metric definition from a MetricMetadata.

        Parent directory are implicitly created.
        This can be expensive, it is worthwile to first check if the metric exists.

        Args:
          metric_metadata: The metric definition.
        """
        # cassandra impl: insert in directories and metrics tables

    def drop_all_metrics(self):
        """Delete all metrics from the database."""
        # cassandra impl: TRUNCATE

    def get_metric(self, metric_name):
        """Return a MetricMetadata for this metric_name, None if no such metric."""
        # cassandra impl: select from the metrics tables

    def glob_metric_names(self, glob):
        """Return a sorted list of metric names matching this glob."""
        # cassandra impl: select from the metrics tables using SASI

    def glob_directory_names(self, glob):
        """Return a sorted list of metric directories matching this glob."""
        # cassandra impl: select from the metrics tables using SASI

    @classmethod
    def _fetch_points_validate_args(cls, metric_name, time_start, time_end, step, aggregator_func):
        """Check arguments as per fetch() spec, raises InvalidArgumentError if needed."""
        # Helper to ease implementation of fetch_point

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

    def insert_points(self, metric, timestamps_and_values):
        """Insert points for a given metric.

        Args:
          metric: The metric definition as per get_metric.
          timestamps_and_values: An iterable of (timestamp in seconds, values as double)
        """
        event = threading.Event()
        exception_box = [None]

        def on_done(exception):
            exception_box[0] = exception
            event.set()

        self.insert_points_async(on_done, metric, timestamps_and_values)
        event.wait()
        if exception_box[0]:
            raise exception_box[0]

    def insert_points_async(self, on_done, metric, timestamps_and_values):
        """Insert points for a given metric.

        Args:
          on_done(e: Exception): called on done, with an exception or None if succesfull
          metric: The metric definition as per get_metric.
          timestamps_and_values: An iterable of (timestamp in seconds, values as double)
        """
        # cassandra impl: do a bunch of insert at all resolutions described by metadata
        # the carbon accessor will just log in on_done and not wait
