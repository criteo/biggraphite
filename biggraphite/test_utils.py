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
import inspect
import os
import re
import sys
import unittest

from cassandra import cluster as c_cluster
import sortedcontainers
from testing import cassandra as testing_cassandra

from biggraphite import accessor as bg_accessor


class _SlowerTestingCassandra(testing_cassandra.Cassandra):
    """Just like testing_cassandra.Cassandra but waits 5 minutes for start."""

    BOOT_TIMEOUT = 5 * 60


def create_unreplicated_keyspace(contact_points, port, keyspace):
    """Create a keyspace, mostly used for tests."""
    cluster = c_cluster.Cluster(contact_points, port)
    session = cluster.connect()
    session.execute(
        "CREATE KEYSPACE %s "
        " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};" %
        keyspace)
    session.shutdown()
    cluster.shutdown()


def prepare_graphite_imports():
    """Add to graphite libs to sys.path."""
    try:
        import carbon  # noqa
    except ImportError:
        to_add = "/opt/graphite/lib"
        if os.environ.get("VIRTUAL_ENV"):
            # Running in a virtual environment
            for package_path in sys.path:
                if package_path.endswith("site-packages"):
                    graphite_path = package_path + "/opt/graphite/lib"
                    if os.path.isdir(graphite_path):
                        to_add = graphite_path
        if to_add not in sys.path:
            sys.path.insert(0, to_add)


class FakeAccessor(object):
    """A fake acessor that never connects."""

    def __init__(self, *args, **kwargs):
        """Validate arguments like accessor.Accessor would."""
        self._real_accessor = bg_accessor.Accessor(*args, **kwargs)
        self._is_connected = False
        self._metric_to_points = collections.defaultdict(sortedcontainers.SortedDict)
        self._metric_to_metadata = {}
        self._directory_names = sortedcontainers.SortedSet()

    @property
    def _metric_names(self):
        return self._metric_to_metadata.keys()

    def __check_args(self, method_name, *args, **kwargs):
        """Validate arguments of a method on the real Accessor."""
        method = getattr(self._real_accessor, method_name)
        # Will raise a TypeError if arguments don't match.
        inspect.getcallargs(method, *args, **kwargs)

    @property
    def is_connected(self):
        """See the real Accessor for a description."""
        return self._is_connected

    def connect(self, *args, **kwargs):
        """See the real Accessor for a description."""
        try:
            self.__check_args("connect", *args, **kwargs)
        except bg_accessor.CassandraError:
            pass
        self._is_connected = True

    def shutdown(self, *args, **kwargs):
        """See the real Accessor for a description."""
        self.__check_args("shutdown", *args, **kwargs)
        self._is_connected = False

    def insert_points(self, metric_name, timestamps_and_values):
        """See the real Accessor for a description."""
        self.__check_args("insert_points", metric_name, timestamps_and_values)
        assert metric_name in self._metric_to_metadata
        points = self._metric_to_points[metric_name]
        for t, v in timestamps_and_values:
            points[t] = v

    def drop_all_metrics(self, *args, **kwargs):
        """See the real Accessor for a description."""
        self.__check_args("drop_all_metrics", *args, **kwargs)
        self._metric_to_points.clear()
        self._metric_to_metadata.clear()
        self._directory_names.clear()

    def create_metric(self, metric_metadata):
        """See the real Accessor for a description."""
        self.__check_args("create_metric", metric_metadata)
        self._metric_to_metadata[metric_metadata.name] = metric_metadata
        parts = metric_metadata.name.split(".")[:-1]
        path = []
        for part in parts:
            path.append(part)
            self._directory_names.add(".".join(path))

    @staticmethod
    def __glob_names(names, glob):
        res = []
        parts_count = glob.count(".")
        glob_re = re.compile(fnmatch.translate(glob))
        for name in names:
            if name.count(".") != parts_count:
                continue
            if glob_re.match(name):
                res.append(name)
        return res

    def glob_metric_names(self, glob):
        """See the real Accessor for a description."""
        self.__check_args("glob_metric_names", glob)
        return self.__glob_names(self._metric_names, glob)

    def glob_directory_names(self, glob):
        """See the real Accessor for a description."""
        self.__check_args("glob_directory_names", glob)
        return self.__glob_names(self._directory_names, glob)

    def get_metric(self, metric_name):
        """See the real Accessor for a description."""
        self.__check_args("get_metric", metric_name)
        return self._metric_to_metadata.get(metric_name)

    def fetch_points(self, metric_name, time_start, time_end, step,
                     aggregator_func=None, _fake_query_results=None):
        """See the real Accessor for a description."""
        if not _fake_query_results:
            points = self._metric_to_points[metric_name]
            rows = []
            for ts in points.irange(time_start, time_end, reverse=True):
                # A row is time_base_ms, time_offset_ms, value
                row = (ts * 1000.0, 0, points[ts])
                rows.append(row)
            _fake_query_results = [(True, rows)]
        return self._real_accessor.fetch_points(
            metric_name, time_start, time_end, step, aggregator_func, _fake_query_results,
        )


class TestCaseWithFakeAccessor(unittest.TestCase):
    """"A TestCase with a FakeAccessor."""

    def setUp(self):
        """Create a new Accessor in self.acessor."""
        super(TestCaseWithFakeAccessor, self).setUp()
        self.accessor = FakeAccessor("fake keyspace", contact_points=[])
        self.accessor.connect()
        self.addCleanup(self.accessor.shutdown)


class TestCaseWithAccessor(unittest.TestCase):
    """"A TestCase with an Accessor for an ephemeral Cassandra cluster."""

    KEYSPACE = "test_keyspace"

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
        self.accessor = bg_accessor.Accessor(
            self.KEYSPACE, self.contact_points, self.port)
        self.accessor.connect()
        self.addCleanup(self.accessor.shutdown)
        self.addCleanup(self.accessor.drop_all_metrics)
