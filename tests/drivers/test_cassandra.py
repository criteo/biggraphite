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
from __future__ import print_function

import unittest
import time
from distutils import version

from biggraphite import accessor as bg_accessor
from biggraphite import metric as bg_metric
from biggraphite.drivers import cassandra as bg_cassandra
from tests import test_utils as bg_test_utils
from tests.drivers.base_test_metadata import BaseTestAccessorMetadata
from tests.test_utils_cassandra import HAS_CASSANDRA

_METRIC = bg_test_utils.make_metric("test.metric")

# Points test query.
_QUERY_RANGE = 3600
_QUERY_START = 1000 * _QUERY_RANGE
_QUERY_END = _QUERY_START + _QUERY_RANGE

# Points injected in the test DB, a superset of above.
_EXTRA_POINTS = 1000
_POINTS_START = _QUERY_START - _EXTRA_POINTS
_POINTS_END = _QUERY_END + _EXTRA_POINTS
_POINTS = [(t, v) for v, t in enumerate(range(_POINTS_START, _POINTS_END))]
_USEFUL_POINTS = _POINTS[_EXTRA_POINTS:-_EXTRA_POINTS]
assert _QUERY_RANGE == len(_USEFUL_POINTS)


@unittest.skipUnless(HAS_CASSANDRA, "CASSANDRA_HOME must be set to a >=3.5 install")
class TestAccessorWithCassandraSASI(
    BaseTestAccessorMetadata, bg_test_utils.TestCaseWithAccessor
):
    def test_glob_too_many_directories(self):
        for name in "a", "a.b", "x.y.z":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.flush()

        old_value = self.accessor.max_metrics_per_pattern
        self.accessor.max_metrics_per_pattern = 1
        with self.assertRaises(bg_cassandra.TooManyMetrics):
            list(self.accessor.glob_directory_names("**"))
        self.accessor.max_metrics_per_pattern = old_value

    # FIXME (t.chataigner) some duplication with ElasticsearchTestAccessorMetadata.
    def test_metric_is_updated_after_ttl(self):
        metric = bg_test_utils.make_metric("foo")
        self.accessor.create_metric(metric)
        self.flush()

        created_metric = self.accessor.get_metric(metric.name)

        old_ttl = self.accessor._CassandraAccessor__metadata_touch_ttl_sec
        self.accessor._CassandraAccessor__metadata_touch_ttl_sec = 1

        # TODO: use freezegun instead of a sleep.
        # We can't use freezegun here since creating/updating metric
        # now() is computed inside cassandra queries (outside python).
        time.sleep(2)
        self.accessor.touch_metric(metric)
        self.flush()

        updated_metric = self.accessor.get_metric(metric.name)

        self.assertNotEqual(created_metric.updated_on, updated_metric.updated_on)

        self.accessor._CassandraAccessor__metadata_touch_ttl_sec = old_ttl


@unittest.skipUnless(HAS_CASSANDRA, "CASSANDRA_HOME must be set to a >=3.5 install")
class TestAccessorWithCassandraLucene(
    TestAccessorWithCassandraSASI, bg_test_utils.TestCaseWithAccessor
):
    ACCESSOR_SETTINGS = {"cassandra_use_lucene": True}


@unittest.skipUnless(HAS_CASSANDRA, "CASSANDRA_HOME must be set to a >=3.5 install")
class TestAccessorWithCassandraData(bg_test_utils.TestCaseWithAccessor):
    def fetch(self, metric, *args, **kwargs):
        """Helper to fetch points as a list."""
        # default kwargs for stage.
        if "stage" not in kwargs:
            kwargs["stage"] = metric.retention[0]
        ret = self.accessor.fetch_points(metric, *args, **kwargs)
        self.assertTrue(hasattr(ret, "__iter__"))
        return list(ret)

    def test_fetch_empty(self):
        no_such_metric = bg_test_utils.make_metric("no.such.metric")
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()
        self.accessor.drop_all_metrics()
        self.assertEqual(len(self.fetch(no_such_metric, _POINTS_START, _POINTS_END)), 0)
        self.assertFalse(len(self.fetch(_METRIC, _POINTS_START, _POINTS_END)), 0)

    def test_insert_empty(self):
        # We've had a regression where inserting empty list would freeze
        # the process
        self.accessor.insert_points(_METRIC, [])
        self.flush()

    def test_insert_fetch(self):
        self.accessor.create_metric(_METRIC)
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()

        # TODO: Test fetch at different stages for a given metric.
        fetched = self.fetch(_METRIC, _QUERY_START, _QUERY_END)

        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        self.assertEqual(_QUERY_RANGE, len(fetched))
        self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        self.assertEqual(_USEFUL_POINTS, fetched)

    def test_insert_fetch_existing_metric(self):
        self.accessor.create_metric(_METRIC)
        actual_metric = self.accessor.get_metric(_METRIC.name)
        self.accessor.insert_points(actual_metric, _POINTS)

        # TODO: Test fetch at different stages for a given metric.
        fetched = self.fetch(_METRIC, _QUERY_START, _QUERY_END)

        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        self.assertEqual(_QUERY_RANGE, len(fetched))
        self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        self.assertEqual(_USEFUL_POINTS, fetched)

    def test_insert_fetch_replicas(self):
        self.accessor.shard = bg_accessor.pack_shard(replica=0, writer=0)
        self.accessor.insert_points(_METRIC, _POINTS)
        self.accessor.shard = bg_accessor.pack_shard(replica=3, writer=0xFFFF)
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()

        # TODO: Test fetch at different stages for a given metric.
        fetched = self.fetch(_METRIC, _QUERY_START, _QUERY_END)
        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        self.assertEqual(_QUERY_RANGE, len(fetched))
        self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        self.assertEqual(_USEFUL_POINTS, fetched)

    def test_fetch_doubledots(self):
        metric = bg_test_utils.make_metric("a.b..c")
        metric_1 = bg_test_utils.make_metric("a.b.c")
        points = [(1, 42)]
        self.accessor.create_metric(metric)
        self.accessor.create_metric(metric_1)
        self.flush()

        self.accessor.insert_points(metric, points)
        self.flush()
        actual_points = self.accessor.fetch_points(
            metric, 1, 2, stage=metric.retention[0]
        )
        self.assertEqual(points, list(actual_points))
        actual_points = self.accessor.fetch_points(
            metric_1, 1, 2, stage=metric.retention[0]
        )
        self.assertEqual(points, list(actual_points))

    def _get_version(self):
        for host in self.cassandra_helper.cluster.metadata.all_hosts():
            return version.LooseVersion(host.release_version)
        return None

    def test_create_datapoints_table_dtcs(self):
        """Validate that we can create a DTCS table."""
        orig_cs = bg_cassandra._COMPACTION_STRATEGY
        bg_cassandra._COMPACTION_STRATEGY = "DateTieredCompactionStrategy"

        max_version = version.LooseVersion("3.8")
        if self._get_version() > max_version:
            print("Skipping DTCS test, incompatible version")
            return

        self.cassandra_helper._reset_keyspace(
            self.cassandra_helper.session, self.cassandra_helper.KEYSPACE
        )

        # We create a fake metric to create the table. This also validate
        # that breaking changes aren't introduced to the schema.
        self.accessor.create_metric(_METRIC)
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()
        self.cassandra_helper.cluster.refresh_schema_metadata()

        self.assertTrue(
            self.cassandra_helper.KEYSPACE in self.cassandra_helper.cluster.metadata.keyspaces
        )
        keyspace = self.cassandra_helper.cluster.metadata.keyspaces[self.cassandra_helper.KEYSPACE]

        datapoints_86400p_1s = keyspace.tables["datapoints_86400p_1s_0"]
        options = datapoints_86400p_1s.options
        self.assertEqual(
            options["compaction"]["class"],
            "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy",
        )
        self.assertEqual(options["compaction"]["base_time_seconds"], "901")
        self.assertEqual(options["compaction"]["max_window_size_seconds"], "2000")
        self.assertEqual(options["default_time_to_live"], 87300)

        datapoints_10080_60s = keyspace.tables["datapoints_10080p_60s_aggr"]
        options = datapoints_10080_60s.options
        self.assertEqual(
            options["compaction"]["class"],
            "org.apache.cassandra.db.compaction.DateTieredCompactionStrategy",
        )
        self.assertEqual(options["compaction"]["base_time_seconds"], "960")
        self.assertEqual(options["compaction"]["max_window_size_seconds"], "120000")
        self.assertEqual(options["default_time_to_live"], 605700)

        bg_cassandra._COMPACTION_STRATEGY = orig_cs

    def test_create_datapoints_table_twcs(self):
        """Validate that we can create a TWCS table."""
        min_version = version.LooseVersion("3.8")
        if self._get_version() < min_version:
            print("Skipping TWCS test, incompatible version")
            return

        orig_cs = bg_cassandra._COMPACTION_STRATEGY
        bg_cassandra._COMPACTION_STRATEGY = "TimeWindowCompactionStrategy"

        self.cassandra_helper._reset_keyspace(
            self.cassandra_helper.session, self.cassandra_helper.KEYSPACE
        )

        # We create a fake metric to create the table. This also validate
        # that breaking changes aren't introduced to the schema.
        self.accessor.create_metric(_METRIC)
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()
        self.cassandra_helper.cluster.refresh_schema_metadata()

        self.assertTrue(
            self.cassandra_helper.KEYSPACE in self.cassandra_helper.cluster.metadata.keyspaces
        )
        keyspace = self.cassandra_helper.cluster.metadata.keyspaces[self.cassandra_helper.KEYSPACE]

        datapoints_86400p_1s = keyspace.tables["datapoints_86400p_1s_0"]
        options = datapoints_86400p_1s.options
        self.assertEqual(
            options["compaction"]["class"],
            "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy",
        )
        self.assertEqual(options["compaction"]["compaction_window_unit"], "HOURS")
        self.assertEqual(options["compaction"]["compaction_window_size"], "1")
        self.assertEqual(options["default_time_to_live"], 87300)

        datapoints_10080_60s = keyspace.tables["datapoints_10080p_60s_aggr"]
        options = datapoints_10080_60s.options
        self.assertEqual(
            options["compaction"]["class"],
            "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy",
        )
        self.assertEqual(options["compaction"]["compaction_window_unit"], "HOURS")
        self.assertEqual(options["compaction"]["compaction_window_size"], "3")
        self.assertEqual(options["default_time_to_live"], 605700)

        bg_cassandra._COMPACTION_STRATEGY = orig_cs

    def test_syncdb(self):
        retentions = [bg_metric.Retention.from_string("60*1s:60*60s")]
        self.accessor.syncdb(retentions=retentions, dry_run=True)
        self.accessor.syncdb(retentions=retentions, dry_run=False)


@unittest.skipUnless(HAS_CASSANDRA, "CASSANDRA_HOME must be set to a >=3.5 install")
class TestAccessorWithHybridCassandraData(TestAccessorWithCassandraData):
    ACCESSOR_SETTINGS = {
        "driver": "hybrid",
        "data_driver": "cassandra",
        "metadata_driver": "memory"
    }

    def test_metadata_disabled(self):
        self.assertFalse(self.accessor._data_accessor.metadata_enabled)


if __name__ == "__main__":
    unittest.main()
