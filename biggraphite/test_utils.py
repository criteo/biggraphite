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
"""Utilities factoring code across tests.

The dependencies for this module are not included in requirements.txt or in the package
dependencies, instead one needs the elements of tests-requirements.txt .
"""
from __future__ import absolute_import
from __future__ import print_function

import collections
import fnmatch
import os
import re
import sys
import tempfile
import shutil
import unittest
import logging

from cassandra import cluster as c_cluster
import mock
import sortedcontainers

from biggraphite import accessor as bg_accessor
from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite import metadata_cache as bg_metadata_cache


HAS_CASSANDRA_HOME = bool(os.getenv("CASSANDRA_HOME"))

# Only try to import cassandra if we are going to use it. This is better
# than using try/except because the failure case is easier to handle.
if HAS_CASSANDRA_HOME:
    from testing import cassandra as testing_cassandra

    class _SlowerTestingCassandra(testing_cassandra.Cassandra):
        """Just like testing_cassandra.Cassandra but waits 5 minutes for start."""

        BOOT_TIMEOUT = 5 * 60


def setup_logging():
    """To be called to enable logs in unittests."""
    logger = logging.getLogger()
    logger.level = logging.DEBUG
    stream_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(stream_handler)


def create_unreplicated_keyspace(contact_points, port, keyspace):
    """Create a keyspace, mostly used for tests."""
    cluster = c_cluster.Cluster(contact_points, port)
    session = cluster.connect()
    template = (
        "CREATE KEYSPACE \"%s\" "
        " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};"
    )
    session.execute(template % keyspace)
    session.execute(template % (keyspace + "_metadata"))
    session.shutdown()
    cluster.shutdown()


def prepare_graphite():
    """Make sure that we have a working Graphite environment."""
    # Setup sys.path
    prepare_graphite_imports()
    os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'

    # Redirect logs somewhere writable
    from django.conf import settings
    settings.LOG_DIR = tempfile.gettempdir()

    # Setup Django
    import django
    django.setup()


def prepare_graphite_imports():
    """Add to graphite libs to sys.path."""
    try:
        import carbon  # noqa
    except ImportError:
        to_add = ["/opt/graphite/lib", "/opt/graphite/webapp"]
        if os.environ.get("VIRTUAL_ENV"):
            # Running in a virtual environment
            for package_path in sys.path:
                if package_path.endswith("site-packages"):
                    for module_path in list(to_add):
                        venv_module_path = package_path + module_path
                        if os.path.isdir(venv_module_path):
                            # Replace the path in the list.
                            to_add.remove(module_path)
                            to_add.append(venv_module_path)

        # Add all custom paths to sys.path.
        for path in to_add:
            if path not in sys.path:
                sys.path.insert(0, path)


def make_metric(name, metadata=None, **kwargs):
    """Create a bg_accesor.Metric with specified metadata."""
    retention = kwargs.get("retention")
    if isinstance(retention, basestring):
        kwargs["retention"] = bg_accessor.Retention.from_string(retention)
    if metadata:
        assert isinstance(metadata, bg_accessor.MetricMetadata)
        assert not kwargs
    else:
        metadata = bg_accessor.MetricMetadata(**kwargs)
    return bg_accessor.Metric(name, metadata)


class FakeAccessor(bg_accessor.Accessor):
    """A fake acessor that never connects and doubles as a fake MetadataCache."""

    def __init__(self):
        """Create a new FakeAccessor."""
        super(FakeAccessor, self).__init__("fake")
        self._metric_to_points = collections.defaultdict(sortedcontainers.SortedDict)
        self._metric_to_metadata = {}
        self._directory_names = sortedcontainers.SortedSet()

    @property
    def _metric_names(self):
        return self._metric_to_metadata.keys()

    def connect(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).connect(*args, **kwargs)
        self.is_connected = True

    def shutdown(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).shutdown(*args, **kwargs)
        self.is_connected = False

    def insert_points_async(self, metric, datapoints, on_done=None):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).insert_points_async(metric, datapoints, on_done)
        assert metric.name in self._metric_to_metadata
        points = self._metric_to_points[metric.name]
        for datapoint in datapoints:
            timestamp = datapoint[0]
            value = datapoint[1]
            points[timestamp] = value
        if on_done:
            on_done(None)

    def drop_all_metrics(self, *args, **kwargs):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).drop_all_metrics(*args, **kwargs)
        self._metric_to_points.clear()
        self._metric_to_metadata.clear()
        self._directory_names.clear()

    def create_metric(self, metric):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).create_metric(metric)
        self._metric_to_metadata[metric.name] = metric.metadata
        parts = metric.name.split(".")[:-1]
        path = []
        for part in parts:
            path.append(part)
            self._directory_names.add(".".join(path))

    @staticmethod
    def __glob_names(names, glob):
        res = []
        dots_count = glob.count(".")
        glob_re = re.compile(fnmatch.translate(glob))
        for name in names:
            # "*" can match dots for fnmatch
            if name.count(".") == dots_count and glob_re.match(name):
                res.append(name)
        return res

    def glob_metric_names(self, glob):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).glob_metric_names(glob)
        return self.__glob_names(self._metric_names, glob)

    def glob_directory_names(self, glob):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).glob_directory_names(glob)
        return self.__glob_names(self._directory_names, glob)

    def get_metric(self, metric_name):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).get_metric(metric_name)
        metadata = self._metric_to_metadata.get(metric_name)
        if metadata:
            return bg_accessor.Metric(metric_name, metadata)
        else:
            return None

    def fetch_points(self, metric, time_start, time_end, stage):
        """See the real Accessor for a description."""
        super(FakeAccessor, self).fetch_points(metric, time_start, time_end, stage)
        points = self._metric_to_points[metric.name]
        rows = []
        for ts in points.irange(time_start, time_end):
            # A row is time_base_ms, time_offset_ms, value, count
            row = (ts * 1000.0, 0, float(points[ts]), 1)
            rows.append(row)
        query_results = [(True, rows)]

        time_start_ms = int(time_start) * 1000
        time_end_ms = int(time_end) * 1000
        return bg_accessor.PointGrouper(metric, time_start_ms, time_end_ms, stage, query_results)


class TestCaseWithTempDir(unittest.TestCase):
    """A TestCase with a temporary directory."""

    def setUp(self):
        """Create a new temporary diractory in self.tempdir."""
        super(TestCaseWithTempDir, self).setUp()
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir)


class TestCaseWithFakeAccessor(TestCaseWithTempDir):
    """"A TestCase with a FakeAccessor."""

    KEYSPACE = "fake_keyspace"

    def setUp(self):
        """Create a new Accessor in self.acessor."""
        super(TestCaseWithFakeAccessor, self).setUp()
        self.accessor = FakeAccessor()
        self.accessor.connect()
        self.addCleanup(self.accessor.shutdown)
        self.metadata_cache = bg_metadata_cache.DiskCache(self.accessor, self.tempdir)
        self.metadata_cache.open()
        self.addCleanup(self.metadata_cache.close)

    def fake_drivers(self):
        """Hijack drivers' connect() functions to return self.accessor."""
        patcher = mock.patch('biggraphite.drivers.cassandra.connect', return_value=self.accessor)
        patcher.start()
        self.addCleanup(patcher.stop)


@unittest.skipUnless(
    HAS_CASSANDRA_HOME, "CASSANDRA_HOME must be set to a 3.5 install",
)
class TestCaseWithAccessor(TestCaseWithTempDir):
    """"A TestCase with an Accessor for an ephemeral Cassandra cluster."""

    KEYSPACE = "testkeyspace"

    @classmethod
    def setUpClass(cls):
        """Create the test Cassandra Cluster as cls.cassandra."""
        super(TestCaseWithAccessor, cls).setUpClass()
        cls.cassandra = _SlowerTestingCassandra(auto_start=False)
        try:
            cls.cassandra.setup()
            cls.cassandra.start()
        except:
            print("fail to starting cassandra, logging potentially useful debug info",
                  file=sys.stderr)
            for attr in "cassandra_home", "cassandra_yaml", "cassandra_bin", "base_dir", "settings":
                print(attr, ":", getattr(cls.cassandra, attr, "Unknown"), file=sys.stderr)
            cls.cassandra.cleanup()
            raise

        # testing.cassandra is meant to be used with the Thrift API, so we need to
        # extract the IPs and native port for use with the native driver.
        cls.contact_points = [s.split(":")[0]
                              for s in cls.cassandra.server_list()]
        cls.port = cls.cassandra.cassandra_yaml["native_transport_port"]
        create_unreplicated_keyspace(cls.contact_points, cls.port, cls.KEYSPACE)

    @classmethod
    def tearDownClass(cls):
        """Stop the test Cassandra Cluster."""
        super(TestCaseWithAccessor, cls).tearDownClass()
        cls.cassandra.stop()

    def setUp(self):
        """Create a new Accessor in self.acessor."""
        super(TestCaseWithAccessor, self).setUp()
        self.accessor = bg_cassandra.connect(
            self.KEYSPACE, self.contact_points, self.port, default_timeout=60,
        )
        self.accessor.connect()
        self.addCleanup(self.accessor.shutdown)
        self.addCleanup(self.__drop_all_metrics)
        self.metadata_cache = bg_metadata_cache.DiskCache(self.accessor, self.tempdir)
        self.metadata_cache.open()
        self.addCleanup(self.metadata_cache.close)

    def __drop_all_metrics(self):
        self.accessor.connect()
        self.accessor.drop_all_metrics()
