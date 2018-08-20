#!/usr/bin/env python
# coding: utf-8
"""Test clusters_diff.py."""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import json
import tempfile
import unittest

import mock

from biggraphite.cli import clusters_diff


class TestRequest(unittest.TestCase):
    """Test the class Rquest."""

    def test_prepare(self):
        """Should correctly create an http request."""
        request = clusters_diff.Request("http://fakeurl.com", "auth_key", 5.0)
        self.assertEqual(request._request.get_method(), "GET")
        self.assertEqual(request._request.get_full_url(), "http://fakeurl.com")
        self.assertEqual(request._request.get_header("Authorization"), "Basic auth_key")

    def test_parse_request_result(self):
        """Should correctly parse a jsonObject into a list of DiffableTarget."""
        data = [
            {"target": "target_1", "datapoints": [[0.1, 10], [0.2, 20]]},
            {"target": "target_2", "datapoints": [[0.1, 60], [0.2, 70]]},
        ]
        data_json = json.dumps(data)

        diffable_target_1 = clusters_diff.DiffableTarget("target_1", {10: 0.1, 20: 0.2})
        diffable_target_2 = clusters_diff.DiffableTarget("target_2", {60: 0.1, 70: 0.2})

        request = clusters_diff.Request("http://fakeurl.com", "auth_key", 5.0)
        self.assertEqual(len(request._parse_request_result(data_json)), 2)
        self.assertEqual(
            request._parse_request_result(data_json)[0].name, diffable_target_1.name
        )
        self.assertEqual(
            request._parse_request_result(data_json)[0].ts_to_val,
            diffable_target_1.ts_to_val,
        )
        self.assertEqual(
            request._parse_request_result(data_json)[1].name, diffable_target_2.name
        )
        self.assertEqual(
            request._parse_request_result(data_json)[1].ts_to_val,
            diffable_target_2.ts_to_val,
        )


class TestHostResult(unittest.TestCase):
    """Test the class HostResult."""

    def test_host_result(self):
        """Should correctly update an HostResult."""
        host_result = clusters_diff.HostResult("host")
        self.assertEqual(host_result.name, "host")
        self.assertEqual(host_result.query_to_error, {})
        self.assertEqual(host_result.query_to_time_s, {})
        self.assertEqual(host_result.diffable_queries, [])

        host_result.add_error("query", "error")
        host_result.add_time_s("query1", 1)
        host_result.add_time_s("query2", 2)
        host_result.add_diffable_query("diffable_query")

        self.assertEqual(host_result.name, "host")
        self.assertEqual(host_result.query_to_error, {"query": "error"})
        self.assertEqual(host_result.query_to_time_s, {"query1": 1, "query2": 2})
        self.assertEqual(host_result.diffable_queries, ["diffable_query"])

    def test_get_error_to_query(self):
        """Should correctly reverse query_to_error to get error_to_queries."""
        host_result = clusters_diff.HostResult("host")
        host_result.add_error("query1", "error1")
        host_result.add_error("query2", "error1")
        host_result.add_error("query3", "error2")

        error_to_queries = host_result.get_error_to_query()
        self.assertEqual(
            {k: sorted(v) for k, v in error_to_queries.items()},
            {"error1": ["query1", "query2"], "error2": ["query3"]},
        )


class TestDiffableTarget(unittest.TestCase):
    """Test mesure_disymmetries in DiffableTarget."""

    def test_measure_disymmetry(self):
        """Should correctly mesure dissymmetries with an other instance."""
        diffable_target_1 = clusters_diff.DiffableTarget("target_1", {10: 0.1, 20: 0.2})
        diffable_target_2 = clusters_diff.DiffableTarget("target_2", {10: 0.3, 20: 0.6})

        dissymmetry = diffable_target_1.measure_dissymmetry(diffable_target_2)
        measures = dissymmetry.measures
        rounded_measures = [round(i, 1) for i in measures]

        self.assertEqual(rounded_measures, [0.5, 0.5])


class TestDiffableQuery(unittest.TestCase):
    """Test mesure_disymmetries in DiffableQuery."""

    def test_measure_disymmetry(self):
        """Should correctly mesure dissymmetries with an other instance."""
        diffable_target_1 = clusters_diff.DiffableTarget("target", {10: 0.1, 20: 0.2})
        diffable_query_1 = clusters_diff.DiffableQuery(
            "query_1", [diffable_target_1], 0.01
        )
        diffable_target_2 = clusters_diff.DiffableTarget("target", {10: 0.1, 20: 0.5})
        diffable_query_2 = clusters_diff.DiffableQuery(
            "query_2", [diffable_target_2], 0.01
        )

        dissymmetry = diffable_query_1.measure_dissymmetry(diffable_query_2)
        measures = dissymmetry.measures
        rounded_measures = [round(i, 1) for i in measures]

        self.assertEqual(rounded_measures, [0.5])


class TestClustersDiff(unittest.TestCase):
    """Test all methods without class of clusters_diff.py."""

    def test_read_queries(self):
        """Sould correctly read inputs."""
        with tempfile.NamedTemporaryFile() as tmp_file:
            inputs = "\n".join(
                [
                    "# comment 1",
                    "query1",
                    "  query 2  ",
                    "query3 # comment 2 ",
                    "  #### comment 3",
                    "    ",
                    "",
                ]
            )
            tmp_file.write(inputs.encode("ascii"))
            tmp_file.flush()

            predicted_queries = ["query1", "query 2", "query3"]
            queries = clusters_diff._read_queries(tmp_file.name)
            self.assertEqual(predicted_queries, queries)

    def test_get_url_from_query(self):
        """Should correctly create an url from a query."""
        host = "host"
        prefix = "foo."
        query = "query 1,*()"
        from_param = "-24hours"
        until_param = "-2minutes"

        predicted_url = (
            "http://host/render/?noCache&format=json&from=-24hours"
            + "&until=-2minutes&target=foo.query%201%2C%2A%28%29"
        )
        url = clusters_diff._get_url_from_query(
            host, prefix, query, from_param, until_param
        )

        self.assertEqual(predicted_url, url)

    def test_fetch_queries(self):
        """Should correctly fill host_results."""
        mocked_return_val = """[{"target": "target_1", "datapoints": [[0.1, 10], [0.2, 20]]},
            {"target": "target_2", "datapoints": [[0.1, 60], [0.2, 70]]}]"""
        with mock.patch("six.moves.urllib.request.urlopen") as urlopen:
            urlopen.return_value.read.return_value = mocked_return_val

            host_result = clusters_diff.fetch_queries(
                "host",
                "prefix",
                "auth_key",
                ["query"],
                "-24hours",
                "-2minutes",
                "5.0",
                "0.01",
                lambda x: x,
            )

            self.assertEqual(len(host_result.diffable_queries), 1)
            self.assertEqual(len(host_result.query_to_error), 0)
            self.assertEqual(len(host_result.query_to_time_s), 1)
            self.assertEqual(len(host_result.diffable_queries[0].diffable_targets), 2)
            self.assertEqual(host_result.name, "host")
            self.assertEqual(host_result.diffable_queries[0].name, "query")

            predicted_dt_1 = clusters_diff.DiffableTarget(
                "target_1", {10: 0.1, 20: 0.2}
            )
            predicted_dt_2 = clusters_diff.DiffableTarget(
                "target_2", {60: 0.1, 70: 0.2}
            )

            diffable_targets = host_result.diffable_queries[0].diffable_targets
            self.assertEqual(predicted_dt_1.name, diffable_targets[0].name)
            self.assertEqual(predicted_dt_1.ts_to_val, diffable_targets[0].ts_to_val)
            self.assertEqual(predicted_dt_2.name, diffable_targets[1].name)
            self.assertEqual(predicted_dt_2.ts_to_val, diffable_targets[1].ts_to_val)

    def test_fetch_queries_error(self):
        """Should correctly raise RequestError."""
        with mock.patch("six.moves.urllib.request.urlopen") as urlopen:
            urlopen.side_effect = clusters_diff.RequestError("not found")

            host_result = clusters_diff.fetch_queries(
                "host",
                "prefix",
                "auth_key",
                ["query"],
                "-24hours",
                "-2minutes",
                "5.0",
                "0.01",
                lambda x: None,
            )

            self.assertEqual(len(host_result.diffable_queries), 1)
            self.assertEqual(len(host_result.query_to_error), 1)
            self.assertEqual(len(host_result.query_to_time_s), 0)
            self.assertEqual(len(host_result.diffable_queries[0].diffable_targets), 0)
            self.assertEqual(host_result.name, "host")
            self.assertEqual(host_result.diffable_queries[0].name, "query")
            self.assertEqual(host_result.query_to_error, {"query": "not found"})

    def test_compute_pctls(self):
        """Should correctly compute pctls from mesures."""
        mesures = [
            1,
            2,
            4,
            8,
            9,
            6,
            2,
            4,
            6,
            1,
            4,
            5,
            8,
            4,
            6,
            7,
            1,
            3,
            4,
            8,
            6,
            3,
            4,
            5,
            8,
            7,
            2,
        ]
        pctls = clusters_diff._compute_pctls(mesures)

        self.assertEqual(pctls, {50: 4, 75: 7, 90: 8, 99: 9, 99.9: 9})

    def test_compute_dissymmetries_pctls(self):
        """Sould correctly compute dissymmetries pctls."""
        diffable_target_1 = clusters_diff.DiffableTarget("target_1", {10: 0.1, 20: 0.2})
        diffable_target_2 = clusters_diff.DiffableTarget("target_2", {60: 0.1, 70: 0.2})

        args1 = [(None, None), (None, []), ([], None), ([], [])]
        for diffable_target_a, diffable_target_b in args1:
            dissymmetries = clusters_diff.compute_dissymmetries(
                diffable_target_a, diffable_target_b
            )
            self.assertEqual(len(dissymmetries), 0)

        args2 = [([diffable_target_1], [diffable_target_1])]
        for diffable_target_a, diffable_target_b in args2:
            dissymmetries = clusters_diff.compute_dissymmetries(
                diffable_target_a, diffable_target_b
            )
            self.assertEqual(len(dissymmetries), 1)
            self.assertEqual(dissymmetries[0].name, "target_1")
            self.assertEqual(dissymmetries[0].measures, [0, 0])
            self.assertEqual(
                dissymmetries[0].pctls,
                collections.OrderedDict(
                    [(50, 0.0), (75, 0.0), (90, 0.0), (99, 0.0), (99.9, 0.0)]
                ),
            )

        args3 = [([diffable_target_1], [diffable_target_2])]
        for diffable_target_a, diffable_target_b in args3:
            dissymmetries = clusters_diff.compute_dissymmetries(
                diffable_target_a, diffable_target_b
            )
            self.assertEqual(len(dissymmetries), 2)
            assert "target_1" in [dissymmetries[0].name, dissymmetries[1].name]
            self.assertEqual(dissymmetries[0].measures, [1, 1])
            self.assertEqual(
                dissymmetries[0].pctls,
                collections.OrderedDict(
                    [(50, 1.0), (75, 1.0), (90, 1.0), (99, 1.0), (99.9, 1.0)]
                ),
            )
            assert "target_2" in [dissymmetries[0].name, dissymmetries[1].name]
            self.assertEqual(dissymmetries[1].measures, [1, 1])
            self.assertEqual(
                dissymmetries[1].pctls,
                collections.OrderedDict(
                    [(50, 1.0), (75, 1.0), (90, 1.0), (99, 1.0), (99.9, 1.0)]
                ),
            )

        args4 = [([diffable_target_1], []), ([], [diffable_target_1])]
        for diffable_target_a, diffable_target_b in args4:
            dissymmetries = clusters_diff.compute_dissymmetries(
                diffable_target_a, diffable_target_b
            )
            self.assertEqual(len(dissymmetries), 1)
            self.assertEqual(dissymmetries[0].name, "target_1")
            self.assertEqual(dissymmetries[0].measures, [1, 1])
            self.assertEqual(
                dissymmetries[0].pctls,
                collections.OrderedDict(
                    [(50, 1.0), (75, 1.0), (90, 1.0), (99, 1.0), (99.9, 1.0)]
                ),
            )

    def test_outer_join_diffables(self):
        """Should correctly compute outer join on diffable name."""
        diffables_a = [
            clusters_diff.DiffableTarget("1", {1: 101}),
            clusters_diff.DiffableTarget("2", {1: 102}),
            clusters_diff.DiffableTarget("6", {1: 106}),
            clusters_diff.DiffableTarget("7", {1: 107}),
            clusters_diff.DiffableTarget("9", {1: 109}),
            clusters_diff.DiffableTarget("11", {1: 111}),
            clusters_diff.DiffableTarget("14", {1: 114}),
        ]

        diffables_b = [
            clusters_diff.DiffableTarget("2", {1: 202}),
            clusters_diff.DiffableTarget("4", {1: 204}),
            clusters_diff.DiffableTarget("5", {1: 205}),
            clusters_diff.DiffableTarget("9", {1: 209}),
            clusters_diff.DiffableTarget("12", {1: 212}),
            clusters_diff.DiffableTarget("14", {1: 214}),
        ]

        result = clusters_diff._outer_join_diffables(diffables_a, diffables_b)

        formated_result = []
        for (y, z) in result:
            assert not y or not z or y.name == z.name
            val1 = y.ts_to_val if y else None
            val2 = z.ts_to_val if z else None
            formated_result.append((val1, val2))

        predicted_result = [
            ({1: 111}, None),
            (None, {1: 212}),
            ({1: 114}, {1: 214}),
            ({1: 101}, None),
            ({1: 102}, {1: 202}),
            (None, {1: 205}),
            (None, {1: 204}),
            ({1: 107}, None),
            ({1: 106}, None),
            ({1: 109}, {1: 209}),
        ]

        def _key(v):
            a, b = v
            a = 0 if a is None else a[1]
            b = 0 if b is None else b[1]
            return max(a, b)

        self.assertEqual(
            sorted(formated_result, key=_key), sorted(predicted_result, key=_key)
        )


if __name__ == "__main__":
    unittest.main()
