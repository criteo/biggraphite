#!/usr/bin/env python
from __future__ import print_function

import unittest

import statistics
from testing import cassandra as testing_cassandra

from biggraphite import accessor as bg_accessor
from biggraphite import test_utils as bg_test_utils

_KEYSPACE = "test_keyspace"
_METRIC = "test.metric"

# Points test query.
_QUERY_RANGE = 3600
_QUERY_START = 1000 * _QUERY_RANGE
_QUERY_END = _QUERY_START + _QUERY_RANGE

# Points injected in the test DB, a superset of above.
_EXTRA_POINTS = 1000
_POINTS_START = _QUERY_START - _EXTRA_POINTS
_POINTS_END = _QUERY_END + _EXTRA_POINTS
_POINTS = [(t, v) for v, t in enumerate(xrange(_POINTS_START, _POINTS_END))]
_USEFUL_POINTS = _POINTS[_EXTRA_POINTS:-_EXTRA_POINTS]
assert _QUERY_RANGE == len(_USEFUL_POINTS)


class TestWithCassandra(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cassandra = testing_cassandra.Cassandra()
        cls.cassandra.start()

        # testing.cassandra is meant to be used with the Thrift API, so we need to
        # extract the IPs and native port for use with the native driver.
        cls.contact_points = [s.split(":")[0]
                              for s in cls.cassandra.server_list()]
        cls.port = cls.cassandra.cassandra_yaml["native_transport_port"]
        bg_test_utils.create_unreplicated_keyspace(
            cls.contact_points, cls.port, _KEYSPACE)

    @classmethod
    def tearDownClass(cls):
        cls.cassandra.stop()

    def setUp(self):
        self.accessor = bg_accessor.Accessor(
            _KEYSPACE, self.contact_points, self.port)
        self.accessor.connect()
        self.addCleanup(self.accessor.shutdown)

    def test_fetch_empty(self):
        self.accessor.insert_points(_METRIC, _POINTS)
        self.accessor.drop_all_metrics()
        self.assertFalse(
            self.accessor.fetch_points("no.such.metric", _POINTS_START, _POINTS_END, step=1))
        self.assertFalse(
            self.accessor.fetch_points(_METRIC, _POINTS_START, _POINTS_END, step=1))

    def test_insert_fetch(self):
        self.accessor.insert_points(_METRIC, _POINTS)
        self.addCleanup(self.accessor.drop_all_metrics)

        fetched = self.accessor.fetch_points(_METRIC, _QUERY_START, _QUERY_END, step=1)
        # assertEqual is very slow when the diff is huge, so we give it a chance of
        # failing early to avoid imprecise test timeouts.
        self.assertEqual(_QUERY_RANGE, len(fetched))
        self.assertEqual(_USEFUL_POINTS[:10], fetched[:10])
        self.assertEqual(_USEFUL_POINTS[-10:], fetched[-10:])
        self.assertEqual(_USEFUL_POINTS, fetched)

    def test_fetch_lower_res(self):
        self.accessor.insert_points(_METRIC, _POINTS)
        self.addCleanup(self.accessor.drop_all_metrics)

        fetched_tenth = self.accessor.fetch_points(
            _METRIC, _QUERY_START, _QUERY_END, step=_QUERY_RANGE / 10)
        self.assertEqual(10, len(fetched_tenth))

        fetched_median = self.accessor.fetch_points(
            _METRIC, _QUERY_START, _QUERY_END, step=_QUERY_RANGE)
        self.assertEqual(1, len(fetched_median))
        median = statistics.median(v for t, v in _USEFUL_POINTS)
        self.assertEqual(median, fetched_median[0][1])

    def test_glob(self):
        self.assertFalse(self.accessor.glob_metric_names(_METRIC + ".*"))
        for name in "a", "a.a", "a.b", "a.a.a":
            meta = bg_accessor.MetricMetadata(name, {})
            self.accessor.update_metric(meta)
        self.assertEqual(["a"], self.accessor.glob_metric_names("*"))
        self.assertEqual(["a.a", "a.b"], self.accessor.glob_metric_names("*.*"))
        self.assertEqual(["a.a.a"], self.accessor.glob_metric_names("*.*.*"))
        self.accessor.drop_all_metrics()
        self.assertFalse(self.accessor.glob_metric_names("*"))

    def test_update_metrics(self):
        metric_data = {
            "name": "a.b.c.d.e.f",
            "carbon_aggregation": "last",
            "carbon_retentions": [[1, 60], [60, 3600]],
            "carbon_xfilesfactor": 0.3,
        }
        metric = bg_accessor.MetricMetadata(**metric_data)
        self.accessor.update_metric(metric)
        metric_again = self.accessor.get_metric(metric.name)
        for k, v in metric_data.iteritems():
            self.assertEqual(v, getattr(metric_again, k))


if __name__ == "__main__":
    unittest.main()
