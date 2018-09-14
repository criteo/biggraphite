#!/usr/bin/env python
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
"""Cassandra utilities factoring code across tests.

The dependencies for this module are not included in requirements.txt or in the package
dependencies, instead one needs the elements of tests-requirements.txt .
"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import sys

from cassandra import cluster as c_cluster

HAS_CASSANDRA_HOME = bool(os.getenv("CASSANDRA_HOME"))
CASSANDRA_HOSTPORT = os.getenv("CASSANDRA_HOSTPORT")
HAS_CASSANDRA = HAS_CASSANDRA_HOME or CASSANDRA_HOSTPORT

# Only try to import cassandra if we are going to use it. This is better
# than using try/except because the failure case is easier to handle.
if HAS_CASSANDRA_HOME:
    from testing import cassandra3 as testing_cassandra


class CassandraHelper:
    """Helper for an ephemeral Cassandra cluster."""

    KEYSPACE = "testkeyspace"

    @classmethod
    def get_accessor_settings(cls):
        """Prepare accessor settings for Cassandra driver."""
        return {
            "cassandra_keyspace": cls.KEYSPACE,
            "cassandra_contact_points": cls.contact_points,
            "cassandra_port": cls.port,
            "cassandra_timeout": 60,
        }

    @classmethod
    def setUpClass(cls):
        """Create the test Cassandra Cluster as cls.cassandra."""
        cls.cassandra = None
        if CASSANDRA_HOSTPORT:
            host, cls.port = CASSANDRA_HOSTPORT.split(":")
            cls.contact_points = [host]
        else:
            cls.setUpCassandra()

        # Make it easy to do raw queries to Cassandra.
        cls.cluster = c_cluster.Cluster(cls.contact_points, cls.port)
        cls.session = cls.cluster.connect()
        cls._reset_keyspace(cls.session, cls.KEYSPACE)
        cls._reset_keyspace(cls.session, cls.KEYSPACE + "_metadata")

    @classmethod
    def setUpCassandra(cls):
        """Start Cassandra."""
        cls.cassandra = testing_cassandra.Cassandra(auto_start=False)
        try:
            cls.cassandra.setup()
            cls.cassandra.start()
        except Exception as e:
            logging.exception(e)
            print(
                "fail to starting cassandra, logging potentially useful debug info",
                file=sys.stderr,
            )
            for attr in (
                "cassandra_home",
                "cassandra_yaml",
                "cassandra_bin",
                "base_dir",
                "settings",
            ):
                print(
                    attr, ":", getattr(cls.cassandra, attr, "Unknown"), file=sys.stderr
                )
            cls.cassandra.cleanup()
            raise

        # testing.cassandra is meant to be used with the Thrift API, so we need to
        # extract the IPs and native port for use with the native driver.
        cls.contact_points = [s.split(":")[0] for s in cls.cassandra.server_list()]
        cls.port = cls.cassandra.cassandra_yaml["native_transport_port"]

    @classmethod
    def tearDownClass(cls):
        """Stop the test Cassandra Cluster."""
        cls.cluster.shutdown()
        cls.session = None
        cls.cluster = None
        if cls.cassandra:
            cls.cassandra.stop()
            cls.cassandra = None

    @classmethod
    def _reset_keyspace(cls, session, keyspace):
        cls.drop_keyspace(session, keyspace)
        cls.create_unreplicated_keyspace(session, keyspace)

    @staticmethod
    def create_unreplicated_keyspace(session, keyspace):
        """Create a keyspace, mostly used for tests."""
        template = (
            'CREATE KEYSPACE "%s" '
            " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1}"
            " AND durable_writes = false;"
        )
        session.execute(template % keyspace)

    @staticmethod
    def drop_keyspace(session, keyspace):
        """Drop a keyspace, mostly used for tests."""
        template = 'DROP KEYSPACE IF EXISTS "%s";'
        session.execute(template % keyspace)

    def flush(self, accessor):
        """Flush all kind of buffers related to Cassandra."""
        # When using Lucene, we need to force a refresh on the index as the default
        # refresh period is 60s.
        if accessor.TYPE == 'cassandra-lucene':
            statements = accessor.metadata_query_generator.sync_queries()
            for statement in statements:
                self.session.execute(statement)

        # Hope that after this all async calls will have been processed.
        self.cluster.refresh_schema_metadata()
