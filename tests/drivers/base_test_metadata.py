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

from biggraphite import accessor_cache as bg_accessor_cache
from biggraphite import metric as bg_metric
from biggraphite.drivers import cassandra as bg_cassandra
from biggraphite.drivers import elasticsearch as bg_elasticsearch
from tests import test_utils as bg_test_utils


class BaseTestAccessorMetadata(object):
    def test_glob_metric_names(self):
        IS_CASSANDRA_LUCENE = self.ACCESSOR_SETTINGS.get("cassandra_use_lucene", False)
        IS_ELASTICSEARCH = self.ACCESSOR_SETTINGS.get("driver", "") == "elasticsearch"

        metrics = [
            "a",
            "a.a",
            "a.b",
            "a.a.a",
            "a.b.c",
            "a.x.y",
            "x.y.z",
            "x.y.y.z",
            "x.y.y.y.z",
            "super",
            "superb",
            "supercomputer",
            "superconductivity",
            "superman",
            "supper",
            "suppose",
            "ad.o.g",
            "af.o.g",
            "ap.o.g",
            "az.o.g",
            "b.o.g",
            "m.o.g",
            "zd.o.g",
            "zf.o.g",
            "zp.o.g",
            "zz.o.g",
            "-b-.a.t",
            "-c-.a.t",
            "-d-.a.t",
            "-e-.a.t",
        ]
        metrics.sort()

        for name in metrics:
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.flush()

        def assert_find(glob, expected_matches):
            # Check we can find the matches of a glob
            matches = sorted(list(self.accessor.glob_metric_names(glob)))

            self.assertEqual(
                expected_matches,
                matches,
                "Expected '%s' for glob '%s', got '%s'"
                % (expected_matches, glob, matches),
            )

        # Empty query
        assert_find("", [])

        # Exact matches
        assert_find("a.a", ["a.a"])
        assert_find("A", [])

        # Character wildcard
        assert_find("?", [x for x in metrics if len(x) == 1])
        assert_find("sup?er", ["supper"])

        # Character selector
        for pattern, expected in [
            ("a[!dfp].o.g", ["az.o.g"]),
            (u"a[!dfp].o.g", ["az.o.g"]),
            ("a[!dfp]suffix.o.g", []),
            ("a[nope].o.g", ["ap.o.g"]),
            ("a[nope]suffix.o.g", []),
        ]:
            assert_find(pattern, expected)

        # Sequence wildcard
        assert_find("*", [x for x in metrics if x.count(".") == 0])
        assert_find("*.*", [x for x in metrics if x.count(".") == 1])
        assert_find("*.*.*", [x for x in metrics if x.count(".") == 2])
        assert_find("super*", [x for x in metrics if x.startswith("super")])

        # Sequence selector
        assert_find("a.{b,x}.{c,y}", ["a.b.c", "a.x.y"])
        assert_find("a{d,f,p}.o.g", ["a{0}.o.g".format(c) for c in "dfp"])
        assert_find(
            "{a,z}{d,f,p}.o.g", ["{0}{1}.o.g".format(a, b) for a in "az" for b in "dfp"]
        )
        assert_find(
            "{a{d,f,p},z{d,f,p}}.o.g",
            ["{0}{1}.o.g".format(a, b) for a in "az" for b in "dfp"],
        )
        for pattern in [
            "-{b,c,d}-.a.t",
            u"-{b,c,d}-.a.t",
            "-{b,c,d}?.a.t",
            "-{b,c,d}[!ha].a.t",
            "-{b,c,d}*.a.t",
            "-{b,c,d}*.[ha].t",
        ]:
            assert_find(pattern, ["-b-.a.t", "-c-.a.t", "-d-.a.t"])

        for pattern in [
            "-{b,c,d}?suffix.a.t",
            "-{b,c,d}[ha].a.t",
            "-{b,c,d}[ha]suffix.a.t",
            "-{b,c,d}[!ha]suffix.a.t",
            "-{b,c,d}*suffix.a.t",
            u"-{b,c,d}*suffix.a.t",
        ]:
            assert_find(pattern, [])

        # Ensure the query optimizer works as expected by having a high
        # combinatorial pattern.
        assert_find(
            "-{b,c,d}*.a.t{,u}{,v}{,w}{,x}{,y}{,z}",
            [
                u"-{0}-.a.t".format(c)
                for c in "bcd"
                if not IS_CASSANDRA_LUCENE and not IS_ELASTICSEARCH
            ],
        )

        # Globstars
        assert_find("**", metrics)
        assert_find("x.**", [x for x in metrics if x.startswith("x.")])

        if not IS_CASSANDRA_LUCENE and not IS_ELASTICSEARCH:
            # FIXME: Lucene doesn't support globstars here yet.
            assert_find("**.z", [x for x in metrics if x.endswith(".z")])
            assert_find(
                "x.**.z",
                [x for x in metrics if x.startswith("x.") and x.endswith(".z")],
            )

        self.accessor.drop_all_metrics()
        assert_find("*", [])
        assert_find("**", [])

    def test_glob_directories(self):
        IS_ELASTICSEARCH = self.ACCESSOR_SETTINGS.get("driver", "") == "elasticsearch"
        for name in "a", "a.b", "x.y.z":
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.flush()

        def assert_find(glob, expected_matches):
            # Check we can find the matches of a glob
            self.assertEqual(
                expected_matches, list(self.accessor.glob_directory_names(glob))
            )

        assert_find("x.y", ["x.y"])  # Test exact match
        assert_find("A", [])  # Test case mismatch

        # Test various depths
        assert_find("*", ["a", "x"])
        if IS_ELASTICSEARCH:
            assert_find("*.*", ["*.y"])
        else:
            assert_find("*.*", ["x.y"])
        assert_find("*.*.*", [])

        self.accessor.drop_all_metrics()
        assert_find("*", [])

    def test_glob_metric_names_cached(self):
        if isinstance(self.accessor, bg_elasticsearch._ElasticSearchAccessor):
            # TODO (t.chataigner) Remove once accessor.cache is implemented.
            self.skipTest(
                "accessor.cache is not implemented for _ElasticSearchAccessor."
            )

        metrics = ["a", "a.b", "x.y.z"]
        for name in metrics:
            metric = bg_test_utils.make_metric(name)
            self.accessor.create_metric(metric)
        self.flush()

        cache = bg_accessor_cache.MemoryCache(10, 60)
        original_cache = self.accessor.cache
        self.accessor.cache = cache

        def assert_find(glob, results):
            res = self.accessor.glob_metric_names(glob)
            self.assertEqual(set(results), set(res))

        # Nothing should be cached here.
        assert_find("**", metrics)
        assert_find("a", ["a"])
        assert_find("{x,y}.*y.[z]", ["x.y.z"])

        # Things should be cached here.
        assert_find("**", metrics)
        assert_find("a", ["a"])
        assert_find("{x,y}.*y.[z]", ["x.y.z"])

        # Make sure we use the cache.
        self.accessor.cache.get = lambda _, version: ["fake.foo.a"]
        assert_find("fake.foo.a", ["fake.foo.a"])
        assert_find("**", ["fake.foo.a"])
        assert_find("{fake,feke}.*oo.[a]", ["fake.foo.a"])

        self.accessor.cache = original_cache

    def test_create_metrics(self):
        meta_dict = {
            "aggregator": bg_metric.Aggregator.last,
            "retention": bg_metric.Retention.from_string("60*1s:60*60s"),
            "carbon_xfilesfactor": 0.3,
        }
        metric = bg_test_utils.make_metric("a.b.c.d.e.f", **meta_dict)

        self.assertEqual(self.accessor.has_metric(metric.name), False)
        self.accessor.create_metric(metric)
        self.flush()
        self.assertEqual(self.accessor.has_metric(metric.name), True)
        metric_again = self.accessor.get_metric(metric.name)
        self.assertEqual(metric.name, metric_again.name)
        for k, v in meta_dict.items():
            self.assertEqual(v, getattr(metric_again.metadata, k))

    def test_update_metrics(self):
        if isinstance(self.accessor, bg_elasticsearch._ElasticSearchAccessor):
            # TODO (t.chataigner) Remove once update_metric is implemented.
            self.skipTest(
                "update_metric is not implemented for _ElasticSearchAccessor."
            )

        # prepare test
        meta_dict = {
            "aggregator": bg_metric.Aggregator.last,
            "retention": bg_metric.Retention.from_string("60*1s:60*60s"),
            "carbon_xfilesfactor": 0.3,
        }
        metadata = bg_metric.MetricMetadata(**meta_dict)
        metric_name = "a.b.c.d.e.f"
        self.accessor.create_metric(bg_test_utils.make_metric(metric_name, metadata))
        self.flush()
        metric = self.accessor.get_metric(metric_name)
        for k, v in meta_dict.items():
            self.assertEqual(v, getattr(metric.metadata, k))

        # test
        updated_meta_dict = {
            "aggregator": bg_metric.Aggregator.maximum,
            "retention": bg_metric.Retention.from_string("30*1s:120*30s"),
            "carbon_xfilesfactor": 0.5,
        }
        updated_metadata = bg_metric.MetricMetadata(**updated_meta_dict)
        # Setting a known metric name should work
        self.accessor.update_metric(metric_name, updated_metadata)
        updated_metric = self.accessor.get_metric(metric_name)
        for k, v in updated_meta_dict.items():
            self.assertEqual(v, getattr(updated_metric.metadata, k))
        # Setting an unknown metric name should fail
        self.assertRaises(
            bg_cassandra.InvalidArgumentError,
            self.accessor.update_metric,
            "fake.metric.name",
            updated_metadata,
        )

    def test_has_metric(self):
        metric = bg_test_utils.make_metric("a.b.c.d.e.f")

        self.assertEqual(self.accessor.has_metric(metric.name), False)
        self.accessor.create_metric(metric)
        self.flush()
        self.assertEqual(self.accessor.has_metric(metric.name), True)

    def test_delete_metric(self):
        if isinstance(self.accessor, bg_elasticsearch._ElasticSearchAccessor):
            # TODO (t.chataigner) Remove once delete_metric is implemented.
            self.skipTest(
                "delete_metric is not implemented for _ElasticSearchAccessor."
            )

        metric = bg_test_utils.make_metric("a.b.c.d.e.f")

        self.accessor.create_metric(metric)
        self.flush()
        self.assertEqual(self.accessor.has_metric(metric.name), True)

        self.accessor.delete_metric(metric.name)
        self.flush()
        self.assertEqual(self.accessor.has_metric(metric.name), False)

    def test_repair(self):
        if isinstance(self.accessor, bg_elasticsearch._ElasticSearchAccessor):
            # TODO (t.chataigner) Remove once repair is implemented.
            self.skipTest("repair is not implemented for _ElasticSearchAccessor.")

        # TODO(c.chary): Add better test for repair()
        self.accessor.repair()

    def test_get_metric_doubledots(self):
        metric = bg_test_utils.make_metric("a.b..c")
        metric_1 = bg_test_utils.make_metric("a.b.c")
        self.accessor.create_metric(metric)
        self.accessor.create_metric(metric_1)
        self.flush()

        self.assertEqual(["a.b.c"], list(self.accessor.glob_metric_names("a.b.*")))
        self.assertEqual(True, self.accessor.has_metric("a.b..c"))
        self.assertNotEqual(None, self.accessor.get_metric("a.b..c"))

    def test_clean_expired(self):
        if isinstance(self.accessor, bg_elasticsearch._ElasticSearchAccessor):
            # TODO (t.chataigner) Remove once clean is implemented.
            self.skipTest("clean is not implemented for _ElasticSearchAccessor.")

        metric1 = bg_test_utils.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        metric2 = bg_test_utils.make_metric("g.h.i.j.k.l")
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
        if isinstance(self.accessor, bg_elasticsearch._ElasticSearchAccessor):
            # TODO (t.chataigner) Remove once clean is implemented.
            self.skipTest("clean is not implemented for _ElasticSearchAccessor.")

        metric1 = bg_test_utils.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        metric2 = bg_test_utils.make_metric("g.h.i.j.k.l")
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
        metric1 = bg_test_utils.make_metric("a.b.c.d.e.f")
        self.accessor.create_metric(metric1)

        metric2 = bg_test_utils.make_metric("g.h.i.j.k.l")
        self.accessor.create_metric(metric2)
        self.flush()

        def _callback(metric, done, total):
            self.assertIsNotNone(metric)
            self.assertTrue(done <= total)

        def _errback(name):
            self.assertIsNotNone(name)

        self.accessor.map(_callback, errback=_errback)

    def test_touch_without_create(self):
        metric = bg_test_utils.make_metric("foo")
        self.accessor.touch_metric(metric)
        self.accessor.create_metric(metric)
        self.accessor.touch_metric(metric)
