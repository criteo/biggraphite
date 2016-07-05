#!/usr/bin/env python
# coding: utf-8
"""Test clusters_diff.py."""
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
import unittest
from biggraphite.cli import clusters_diff
import tempfile
import mock
import socket


class TestRequest(unittest.TestCase):
    """Test the class Rquest."""

    def test_prepare(self):
        """Should correctly create an http request."""
        request = clusters_diff.Request('http://fakeurl.com', 'auth_key', 5.0)
        assert request._request.get_method() == 'GET'
        assert request._request.get_full_url() == 'http://fakeurl.com'
        assert request._request.get_header('Authorization') == 'Basic auth_key'

    def test_parse_request_result(self):
        """Should correctly parse a jsonObject into a list of DiffableTarget."""
        raw_data = ("""[{"target": "target_1", "datapoints": [[0.1, 10], [0.2, 20]]}, """ +
                    """{"target": "target_2", "datapoints": [[0.1, 60], [0.2, 70]]}]""")

        diffable_target_1 = clusters_diff.DiffableTarget("target_1", {10: 0.1, 20: 0.2})
        diffable_target_2 = clusters_diff.DiffableTarget("target_2", {60: 0.1, 70: 0.2})

        request = clusters_diff.Request('http://fakeurl.com', 'auth_key', 5.0)
        assert len(request._parse_request_result(raw_data)) == 2
        assert request._parse_request_result(raw_data)[0].__dict__ == diffable_target_1.__dict__
        assert request._parse_request_result(raw_data)[1].__dict__ == diffable_target_2.__dict__


class TestHostResult(unittest.TestCase):
    """Test the class HostResult."""

    def test_host_result(self):
        """Should correctly update an HostResult."""
        host_result = clusters_diff.HostResult('host')
        assert host_result.name == 'host'
        assert host_result.query_to_error == {}
        assert host_result.query_to_time_s == {}
        assert host_result.diffable_queries == []

        host_result.add_error('query', 'error')
        host_result.add_time_s('query1', 1)
        host_result.add_time_s('query2', 2)
        host_result.add_diffable_query('diffable_query')

        assert host_result.name == 'host'
        assert host_result.query_to_error == {'query': 'error'}
        assert host_result.query_to_time_s == {'query1': 1, 'query2': 2}
        assert host_result.diffable_queries == ['diffable_query']

    def test_get_error_to_query(self):
        """Should correctly reverse query_to_error to get error_to_queries."""
        host_result = clusters_diff.HostResult('host')
        host_result.add_error('query1', 'error1')
        host_result.add_error('query2', 'error1')
        host_result.add_error('query3', 'error2')

        error_to_queries = host_result.get_error_to_query()
        assert {k: sorted(v) for k, v in error_to_queries.iteritems()} == (
            {'error1': ['query1', 'query2'], 'error2': ['query3']})


class TestDiffableTarget(unittest.TestCase):
    """Test mesure_disymmetries in DiffableTarget."""

    def test_measure_disymmetry(self):
        """Should correctly mesure dissymmetries with an other instance."""
        diffable_target_1 = clusters_diff.DiffableTarget("target_1", {10: 0.1, 20: 0.2})
        diffable_target_2 = clusters_diff.DiffableTarget("target_2", {10: 0.3, 20: 0.6})

        dissymmetry = diffable_target_1.measure_dissymmetry(diffable_target_2)
        measures = dissymmetry.measures
        rounded_measures = [round(i, 1) for i in measures]

        assert rounded_measures == [0.5, 0.5]


class TestDiffableQuery(unittest.TestCase):
    """Test mesure_disymmetries in DiffableQuery."""

    def test_measure_disymmetry(self):
        """Should correctly mesure dissymmetries with an other instance."""
        diffable_target_1 = clusters_diff.DiffableTarget("target", {10: 0.1, 20: 0.2})
        diffable_query_1 = clusters_diff.DiffableQuery("query_1", [diffable_target_1], 0.01)
        diffable_target_2 = clusters_diff.DiffableTarget("target", {10: 0.1, 20: 0.5})
        diffable_query_2 = clusters_diff.DiffableQuery("query_2", [diffable_target_2], 0.01)

        dissymmetry = diffable_query_1.measure_dissymmetry(diffable_query_2)
        measures = dissymmetry.measures
        rounded_measures = [round(i, 1) for i in measures]

        assert rounded_measures == [0.5]


class TestClustersDiff(unittest.TestCase):
    """Test all methods without class of clusters_diff.py."""

    def test_read_queries(self):
        """Sould correctly read inputs."""
        tmpFile = tempfile.NamedTemporaryFile()
        filename = tmpFile.name
        inputs = "\n".join([
            "# comment 1",
            "query1",
            "  query 2  ",
            "query3 # comment 2 ",
            "  #### comment 3",
            "    ",
            ""])
        with open(filename, 'w') as f:
            f.write(inputs)

        predicted_queries = ["query1", "query 2", "query3"]
        queries = clusters_diff._read_queries(filename)

        assert predicted_queries == queries

    def test_get_url_from_query(self):
        """Should correctly create an url from a query."""
        host = "host"
        query = "query 1,*()"
        from_param = "-24hours"
        until_param = "-2minutes"

        predicted_url = ("http://host/render/?noCache&format=json&from=-24hours" +
                         "&until=-2minutes&target=query%201%2C%2A%28%29")
        url = clusters_diff._get_url_from_query(host, query, from_param, until_param)

        assert predicted_url == url

    def test_fetch_queries(self):
        """Should correctly fill host_results."""
        mocked_return_val = ("""[{"target": "target_1", "datapoints": [[0.1, 10], [0.2, 20]]},
            {"target": "target_2", "datapoints": [[0.1, 60], [0.2, 70]]}]""")
        # without error
        with mock.patch('urllib2.urlopen') as urlopen:
            urlopen.return_value.read.return_value = mocked_return_val

            host_result = clusters_diff.fetch_queries(
                "host", "auth_key", ["query"], "-24hours", "-2minutes",
                "5.0", "0.01", lambda x: x)

            assert len(host_result.diffable_queries) == 1
            assert len(host_result.query_to_error) == 0
            assert len(host_result.query_to_time_s) == 1
            assert len(host_result.diffable_queries[0].diffable_targets) == 2
            assert host_result.name == "host"
            assert host_result.diffable_queries[0].name == "query"

            predicted_dt_1 = clusters_diff.DiffableTarget("target_1", {10: 0.1, 20: 0.2})
            predicted_dt_2 = clusters_diff.DiffableTarget("target_2", {60: 0.1, 70: 0.2})

            diffable_targets = host_result.diffable_queries[0].diffable_targets
            formated_diffable_targets = [dt.__dict__ for dt in diffable_targets]
            assert predicted_dt_1.__dict__ in formated_diffable_targets
            assert predicted_dt_2.__dict__ in formated_diffable_targets

        # with error
        with mock.patch('urllib2.urlopen') as urlopen:
            urlopen.side_effect = clusters_diff.RequestError('not found')

            host_result = clusters_diff.fetch_queries(
                "host", "auth_key", ["query"], "-24hours", "-2minutes",
                "5.0", "0.01", lambda x: None)

            assert len(host_result.diffable_queries) == 1
            assert len(host_result.query_to_error) == 1
            assert len(host_result.query_to_time_s) == 0
            assert len(host_result.diffable_queries[0].diffable_targets) == 0
            assert host_result.name == "host"
            assert host_result.diffable_queries[0].name == "query"
            assert host_result.query_to_error == {'query': 'not found'}

    def test_compute_pctls(self):
        """Should correctly compute pctls from mesures."""
        mesures = [1, 2, 4, 8, 9, 6, 2, 4, 6, 1, 4, 5, 8, 4, 6, 7, 1, 3, 4, 8, 6, 3, 4, 5, 8, 7, 2]
        pctls = clusters_diff._compute_pctls(mesures)

        assert pctls == {50: 4, 99: 9, 90: 8, 99.9: 9}

    def test_compute_dissymmetries_pctls(self):
        """Sould correctly compute dissymmetries pctls."""
        diffable_target_1 = clusters_diff.DiffableTarget("target_1", {10: 0.1, 20: 0.2})
        diffable_target_2 = clusters_diff.DiffableTarget("target_2", {60: 0.1, 70: 0.2})

        args1 = [(None, None), (None, []), ([], None), ([], []), ]
        for diffable_target_a, diffable_target_b in args1:
            res = clusters_diff.compute_dissymmetries(diffable_target_a, diffable_target_b)
            assert len(res) == 0

        args2 = [([diffable_target_1], [diffable_target_1])]
        for diffable_target_a, diffable_target_b in args2:
            res = clusters_diff.compute_dissymmetries(diffable_target_a, diffable_target_b)
            formated_res = [p.__dict__ for p in res]
            assert len(res) == 1
            assert clusters_diff.Dissymmetry("target_1", [0.0, 0.0]).__dict__ in formated_res

        args3 = [([diffable_target_1], [diffable_target_2])]
        for diffable_target_a, diffable_target_b in args3:
            res = clusters_diff.compute_dissymmetries(diffable_target_a, diffable_target_b)
            formated_res = [p.__dict__ for p in res]
            assert len(res) == 2
            assert clusters_diff.Dissymmetry("target_1", [1.0, 1.0]).__dict__ in formated_res
            assert clusters_diff.Dissymmetry("target_2", [1.0, 1.0]).__dict__ in formated_res

        args4 = [([diffable_target_1], []), ([], [diffable_target_1])]
        for diffable_target_a, diffable_target_b in args4:
            res = clusters_diff.compute_dissymmetries(diffable_target_a, diffable_target_b)
            formated_res = [p.__dict__ for p in res]
            assert len(res) == 1
            assert clusters_diff.Dissymmetry("target_1", [1.0, 1.0]).__dict__ in formated_res

    def test_outer_join_diffables(self):
        """Should correctly compute outer join on diffable name."""
        diffables_a = [
            clusters_diff.DiffableTarget('1', {01: 101}),
            clusters_diff.DiffableTarget('2', {01: 102}),
            clusters_diff.DiffableTarget('6', {01: 106}),
            clusters_diff.DiffableTarget('7', {01: 107}),
            clusters_diff.DiffableTarget('9', {01: 109}),
            clusters_diff.DiffableTarget('11', {01: 111}),
            clusters_diff.DiffableTarget('14', {01: 114})
        ]

        diffables_b = [
            clusters_diff.DiffableTarget('2', {01: 202}),
            clusters_diff.DiffableTarget('4', {01: 204}),
            clusters_diff.DiffableTarget('5', {01: 205}),
            clusters_diff.DiffableTarget('9', {01: 209}),
            clusters_diff.DiffableTarget('12', {01: 212}),
            clusters_diff.DiffableTarget('14', {01: 214})
        ]

        result = clusters_diff._outer_join_diffables(diffables_a, diffables_b)

        formated_result = []
        for (y, z) in result:
            assert not y or not z or y.name == z.name
            val1 = y.ts_to_val if y else None
            val2 = z.ts_to_val if z else None
            formated_result.append((val1, val2))

        predicted_result = [
            ({1: 111}, None), (None, {1: 212}), ({1: 114}, {1: 214}), ({1: 101}, None),
            ({1: 102}, {1: 202}), (None, {1: 205}), (None, {1: 204}), ({1: 107}, None),
            ({1: 106}, None), ({1: 109}, {1: 209})
            ]

        assert formated_result == predicted_result


if __name__ == '__main__':
    unittest.main()
