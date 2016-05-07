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

import os
import sys
import unittest

from cassandra import cluster as c_cluster
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
