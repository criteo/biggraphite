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
from __future__ import print_function

import unittest
import time
import re

from distutils import version

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils
from biggraphite import glob_utils as bg_glob_utils
from biggraphite.drivers import cassandra as bg_cassandra

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


class BaseTestAccessorWithCassandraMetadata(bg_test_utils.TestCaseWithAccessor):

    def test_glob_metrics(self):
        IS_LUCENE = self.accessor_settings.get('stratio_lucene', False)

        metrics = [
            "a", "a.a", "a.b", "a.a.a", "a.b.c", "a.x.y",
            "x.y.z", "x.y.y.z", "x.y.y.y.z",
            "super", "superb", "supercomputer", "superconductivity", "superman",
            "supper", "suppose",
            "ad.o.g", "af.o.g", "ap.o.g", "az.o.g",
            "b.o.g", "m.o.g",
            "zd.o.g", "zf.o.g", "zp.o.g", "zz.o.g",
            "-b-.a.t", "-c-.a.t", "-d-.a.t", "-e-.a.t",
        ]
        metrics.sort()

        for name in metrics:
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.flush()

        def assert_find(glob, expected_matches):
            # Check we can find the matches of a glob
            matches = sorted(list(self.accessor.glob_metric_names(glob)))

            # Lucene is supposed to give perfect results, so filter wrongly expected matches.
            if IS_LUCENE:
                glob_re = re.compile(bg_glob_utils.glob_to_regex(glob))
                expected_matches = filter(glob_re.match, expected_matches)

            self.assertEqual(expected_matches, matches)

        # Empty query
        assert_find("", [])

        # Exact matches
        assert_find("a.a", ["a.a"])
        assert_find("A", [])

        # Character wildcard
        assert_find("?",
                    [x for x in metrics if x.count('.') == 0])
        assert_find("sup?er",
                    [x for x in metrics if x.startswith("sup")])

        # Character selector
        for pattern in [
                "a[!dfp].o.g",
                u"a[!dfp].o.g",
                "a[!dfp]suffix.o.g",
                "a[nope].o.g",
                "a[nope]suffix.o.g",
        ]:
            assert_find(pattern,
                        ["a{0}.o.g".format(x) for x in "dfpz"])

        # Sequence wildcard
        assert_find("*",
                    [x for x in metrics if x.count('.') == 0])
        assert_find("*.*",
                    [x for x in metrics if x.count('.') == 1])
        assert_find("*.*.*",
                    [x for x in metrics if x.count('.') == 2])
        assert_find("super*",
                    [x for x in metrics if x.startswith("super")])

        # Sequence selector
        assert_find("a.{b,x}.{c,y}",
                    ["a.b.c", "a.x.y"])
        assert_find("a{d,f,p}.o.g",
                    ["a{0}.o.g".format(c) for c in "dfp"])
        assert_find("{a,z}{d,f,p}.o.g",
                    ["{0}{1}.o.g".format(a, b) for a in "az" for b in "dfp"])
        assert_find("{a{d,f,p},z{d,f,p}}.o.g",
                    ["{0}{1}.o.g".format(a, b) for a in "az" for b in "dfp"])
        for pattern in [
                "-{b,c,d}-.a.t",
                u"-{b,c,d}-.a.t",
                "-{b,c,d}?.a.t",
                "-{b,c,d}?suffix.a.t",
                "-{b,c,d}[ha].a.t",
                "-{b,c,d}[ha]suffix.a.t",
                "-{b,c,d}[!-].a.t",
                "-{b,c,d}[!-]suffix.a.t",
                "-{b,c,d}*.a.t",
                "-{b,c,d}*suffix.a.t",
                u"-{b,c,d}*suffix.a.t",
        ]:
            assert_find(pattern, ["-b-.a.t", "-c-.a.t", "-d-.a.t"])

        # Ensure the query optimizer works as expected by having a high
        # combinatorial pattern.
        assert_find(
            "-{b,c,d}*suffix.a.t{,u}{,v}{,w}{,x}{,y}{,z}",
            ["-{0}-.a.t".format(c) for c in "bcde"],
        )

        # Globstars
        assert_find("**",
                    metrics)
        assert_find("x.**",
                    [x for x in metrics if x.startswith("x.")])

        if not IS_LUCENE:
            # FIXME: Lucene doesn't support globstars here yet.
            assert_find("**.z",
                        [x for x in metrics if x.endswith(".z")])
            assert_find("x.**.z",
                        [x for x in metrics
                         if x.startswith("x.") and x.endswith(".z")])

        self.accessor.drop_all_metrics()
        assert_find("*", [])
        assert_find("**", [])

    def test_glob_directories(self):
        for name in "a", "a.b", "x.y.z":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.flush()

        def assert_find(glob, expected_matches):
            # Check we can find the matches of a glob
            self.assertEqual(expected_matches, list(
                self.accessor.glob_directory_names(glob)))

        assert_find("x.y", ["x.y"])  # Test exact match
        assert_find("A", [])  # Test case mismatch

        # Test various depths
        assert_find("*", ["a", "x"])
        assert_find("*.*", ["x.y"])
        assert_find("*.*.*", [])

        self.accessor.drop_all_metrics()
        assert_find("*", [])

    def test_glob_too_many_directories(self):
        for name in "a", "a.b", "x.y.z":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.flush()

        old_value = self.accessor.max_metrics_per_pattern
        self.accessor.max_metrics_per_pattern = 1
        with self.assertRaises(bg_cassandra.TooManyMetrics):
            list(self.accessor.glob_directory_names('**'))
        self.accessor.max_metrics_per_pattern = old_value

    def test_create_metrics(self):
        meta_dict = {
            "aggregator": bg_accessor.Aggregator.last,
            "retention": bg_accessor.Retention.from_string("60*1s:60*60s"),
            "carbon_xfilesfactor": 0.3,
        }
        metric = bg_test_utils.make_metric("a.b.c.d.e.f", **meta_dict)

        self.assertEqual(self.accessor.has_metric(metric.name), False)
        self.accessor.create_metric(metric)
        self.assertEqual(self.accessor.has_metric(metric.name), True)
        metric_again = self.accessor.get_metric(metric.name)
        self.assertEqual(metric.name, metric_again.name)
        for k, v in meta_dict.items():
            self.assertEqual(v, getattr(metric_again.metadata, k))

    def test_update_metrics(self):
        # prepare test
        meta_dict = {
            "aggregator": bg_accessor.Aggregator.last,
            "retention": bg_accessor.Retention.from_string("60*1s:60*60s"),
            "carbon_xfilesfactor": 0.3,
        }
        metadata = bg_accessor.MetricMetadata(**meta_dict)
        metric_name = "a.b.c.d.e.f"
        self.accessor.create_metric(
            self.accessor.make_metric(metric_name, metadata))
        metric = self.accessor.get_metric(metric_name)
        for k, v in meta_dict.items():
            self.assertEqual(v, getattr(metric.metadata, k))

        # test
        updated_meta_dict = {
            "aggregator": bg_accessor.Aggregator.maximum,
            "retention": bg_accessor.Retention.from_string("30*1s:120*30s"),
            "carbon_xfilesfactor": 0.5,
        }
        updated_metadata = bg_accessor.MetricMetadata(**updated_meta_dict)
        # Setting a known metric name should work
        self.accessor.update_metric(metric_name, updated_metadata)
        updated_metric = self.accessor.get_metric(metric_name)
        for k, v in updated_meta_dict.items():
            self.assertEqual(v, getattr(updated_metric.metadata, k))
        # Setting an unknown metric name should fail
        self.assertRaises(
            bg_cassandra.InvalidArgumentError,
            self.accessor.update_metric, "fake.metric.name", updated_metadata)

    def test_has_metric(self):
        metric = self.make_metric("a.b.c.d.e.f")

        self.assertEqual(self.accessor.has_metric(metric.name), False)
        self.accessor.create_metric(metric)
        self.assertEqual(self.accessor.has_metric(metric.name), True)

    def test_delete_metric(self):
        metric = self.make_metric("a.b.c.d.e.f")

        self.accessor.create_metric(metric)
        self.assertEqual(self.accessor.has_metric(metric.name), True)
        self.accessor.delete_metric(metric.name)
        self.assertEqual(self.accessor.has_metric(metric.name), False)

    def test_repair(self):
        # TODO(c.chary): Add better test for repair()
        self.accessor.repair()

    def test_doubledots(self):
        metric = self.make_metric("a.b..c")
        metric_1 = self.make_metric("a.b.c")
        points = [(1, 42)]
        self.accessor.create_metric(metric)
        self.accessor.create_metric(metric_1)
        self.flush()

        self.assertEqual(['a.b.c'],
                         list(self.accessor.glob_metric_names("a.b.*")))
        self.assertEqual(True, self.accessor.has_metric("a.b..c"))
        self.assertNotEqual(None, self.accessor.get_metric("a.b..c"))

        self.accessor.insert_points(metric, points)
        self.flush()
        actual_points = self.accessor.fetch_points(
            metric, 1, 2, stage=metric.retention[0])
        self.assertEqual(points, list(actual_points))
        actual_points = self.accessor.fetch_points(
            metric_1, 1, 2, stage=metric.retention[0])
        self.assertEqual(points, list(actual_points))

    def test_metrics_ttl_correctly_refreshed(self):
        metric1 = self.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        # Set ttl to a lower value
        self.accessor._CassandraAccessor__metadata_touch_ttl_sec = 1

        # Setting up the moc function
        isUpdated = [False]

        def touch_metric_moc(*args, **kwargs):
            isUpdated[0] = True

        old_touch_fn = self.accessor.touch_metric
        self.accessor.touch_metric = touch_metric_moc

        time.sleep(3)
        self.accessor.get_metric(metric1.name, touch=True)
        self.assertEqual(isUpdated[0], True)

        self.accessor.touch_metric = old_touch_fn
        self.addCleanup(self.accessor.drop_all_metrics)

    def test_metrics_ttl_not_refreshed(self):
        metric1 = self.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        # Setting up the moc function
        isUpdated = [False]

        def touch_metric_moc(*args, **kwargs):
            isUpdated[0] = True

        old_touch_fn = self.accessor.touch_metric
        self.accessor.touch_metric = touch_metric_moc

        time.sleep(3)
        self.accessor.get_metric(metric1.name)
        self.assertEqual(isUpdated[0], False)

        self.accessor.get_metric(metric1.name, touch=True)
        self.assertEqual(isUpdated[0], False)

        self.accessor.touch_metric = old_touch_fn
        self.addCleanup(self.accessor.drop_all_metrics)

    def test_clean_expired(self):
        metric1 = self.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        metric2 = self.make_metric("g.h.i.j.k.l")
        self.accessor.create_metric(metric2)
        self.flush()

        # Check that the metrics exist before the cleanup
        self.assertEqual(self.accessor.has_metric(metric1.name), True)
        self.assertEqual(self.accessor.has_metric(metric2.name), True)

        # set cutoff time in the future to delete all created metrics
        cutoff = -3600
        self.accessor.clean(cutoff)

        # Check that the metrics are correctly deleted
        self.assertEqual(self.accessor.has_metric(metric1.name), False)
        self.assertEqual(self.accessor.has_metric(metric2.name), False)
        self.addCleanup(self.accessor.drop_all_metrics)

    def test_clean_not_expired(self):
        metric1 = self.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        metric2 = self.make_metric("g.h.i.j.k.l")
        self.accessor.create_metric(metric2)
        self.flush()

        # Check that the metrics exist before the cleanup
        self.assertEqual(self.accessor.has_metric(metric1.name), True)
        self.assertEqual(self.accessor.has_metric(metric2.name), True)

        # set cutoff time in the past to delete nothing
        cutoff = 3600
        self.accessor.clean(cutoff)

        # Check that the metrics still exist after the cleanup
        self.assertEqual(self.accessor.has_metric(metric1.name), True)
        self.assertEqual(self.accessor.has_metric(metric2.name), True)
        self.addCleanup(self.accessor.drop_all_metrics)

    def test_map(self):
        metric1 = self.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        metric2 = self.make_metric("g.h.i.j.k.l")
        self.accessor.create_metric(metric2)
        self.flush()

        def _callback(metric, done, total):
            self.assertIsNotNone(metric)
            self.assertTrue(done <= total)

        self.accessor.map(_callback)


class TestAccessorWithCassandraSASI(BaseTestAccessorWithCassandraMetadata):
    pass


class TestAccessorWithCassandraLucene(BaseTestAccessorWithCassandraMetadata):
    def setUp(self):
        self.accessor_settings['stratio_lucene'] = True
        super(TestAccessorWithCassandraLucene, self).setUp()


class TestAccessorWithCassandraData(bg_test_utils.TestCaseWithAccessor):

    def fetch(self, metric, *args, **kwargs):
        """Helper to fetch points as a list."""
        # default kwargs for stage.
        if 'stage' not in kwargs:
            kwargs['stage'] = metric.retention[0]
        ret = self.accessor.fetch_points(metric, *args, **kwargs)
        self.assertTrue(hasattr(ret, "__iter__"))
        return list(ret)

    def test_fetch_empty(self):
        no_such_metric = bg_test_utils.make_metric("no.such.metric")
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()
        self.accessor.drop_all_metrics()
        self.assertEqual(
            len(self.fetch(no_such_metric, _POINTS_START, _POINTS_END)),
            0,
        )
        self.assertFalse(
            len(self.fetch(_METRIC, _POINTS_START, _POINTS_END)),
            0,
        )

    def test_insert_empty(self):
        # We've had a regression where inserting empty list would freeze
        # the process
        self.accessor.insert_points(_METRIC, [])
        self.flush()

    def test_insert_fetch(self):
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()
        self.addCleanup(self.accessor.drop_all_metrics)

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
        self.addCleanup(self.accessor.drop_all_metrics)

        # TODO: Test fetch at different stages for a given metric.
        fetched = self.fetch(_METRIC, _QUERY_START, _QUERY_END)
        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        self.assertEqual(_QUERY_RANGE, len(fetched))
        self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        self.assertEqual(_USEFUL_POINTS, fetched)

    def _get_version(self):
        for host in self.cluster.metadata.all_hosts():
            return version.LooseVersion(host.release_version)
        return None

    def test_create_datapoints_table_dtcs(self):
        """Validate that we can create table."""
        orig_cs = bg_cassandra._COMPACTION_STRATEGY
        bg_cassandra._COMPACTION_STRATEGY = "DateTieredCompactionStrategy"

        max_version = version.LooseVersion('3.8')
        if self._get_version() > max_version:
            print('Skipping DTCS test, incompatible version')
            return

        self._reset_keyspace(self.session, self.KEYSPACE)

        # We create a fake metric to create the table. This also validate
        # that breaking changes aren't introduced to the schema.
        self.accessor.create_metric(_METRIC)
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()
        self.cluster.refresh_schema_metadata()

        keyspace = None
        for name, keyspace in self.cluster.metadata.keyspaces.items():
            if name == self.accessor.keyspace:
                break

        datapoints_86400p_1s = keyspace.tables['datapoints_86400p_1s_0']
        options = datapoints_86400p_1s.options
        self.assertEqual(
            options['compaction']['class'],
            'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy')
        self.assertEqual(options['compaction']['base_time_seconds'], '901')
        self.assertEqual(options['compaction']
                         ['max_window_size_seconds'], '2000')
        self.assertEqual(options['default_time_to_live'], 87300)

        datapoints_10080_60s = keyspace.tables['datapoints_10080p_60s_aggr']
        options = datapoints_10080_60s.options
        self.assertEqual(
            options['compaction']['class'],
            'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy')
        self.assertEqual(options['compaction']['base_time_seconds'], '960')
        self.assertEqual(options['compaction']
                         ['max_window_size_seconds'], '120000')
        self.assertEqual(options['default_time_to_live'], 605700)

        bg_cassandra._COMPACTION_STRATEGY = orig_cs

    def test_create_datapoints_table_twcs(self):
        """Validate that we can create table."""
        min_version = version.LooseVersion('3.8')
        if self._get_version() < min_version:
            print('Skipping TWCS test, incompatible version')
            return

        orig_cs = bg_cassandra._COMPACTION_STRATEGY
        bg_cassandra._COMPACTION_STRATEGY = "TimeWindowCompactionStrategy"

        self._reset_keyspace(self.session, self.KEYSPACE)

        # We create a fake metric to create the table. This also validate
        # that breaking changes aren't introduced to the schema.
        self.accessor.create_metric(_METRIC)
        self.accessor.insert_points(_METRIC, _POINTS)
        self.flush()
        self.cluster.refresh_schema_metadata()

        keyspace = None
        for name, keyspace in self.cluster.metadata.keyspaces.items():
            if name == self.accessor.keyspace:
                break

        datapoints_86400p_1s = keyspace.tables['datapoints_86400p_1s_0']
        options = datapoints_86400p_1s.options
        self.assertEqual(
            options['compaction']['class'],
            'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy')
        self.assertEqual(options['compaction']
                         ['compaction_window_unit'], 'HOURS')
        self.assertEqual(options['compaction']['compaction_window_size'], '1')
        self.assertEqual(options['default_time_to_live'], 87300)

        datapoints_10080_60s = keyspace.tables['datapoints_10080p_60s_aggr']
        options = datapoints_10080_60s.options
        self.assertEqual(
            options['compaction']['class'],
            'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy')
        self.assertEqual(options['compaction']
                         ['compaction_window_unit'], 'HOURS')
        self.assertEqual(options['compaction']['compaction_window_size'], '3')
        self.assertEqual(options['default_time_to_live'], 605700)

        bg_cassandra._COMPACTION_STRATEGY = orig_cs

    def test_syncdb(self):
        retentions = [bg_accessor.Retention.from_string("60*1s:60*60s")]
        self.accessor.syncdb(retentions=retentions, dry_run=True)
        self.accessor.syncdb(retentions=retentions, dry_run=False)


if __name__ == "__main__":
    unittest.main()
