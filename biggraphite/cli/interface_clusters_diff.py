"""Compare two Graphite clusters for a given list of queries."""
from __future__ import absolute_import
from __future__ import print_function
import argparse
import urllib2
import json
import base64
import time
import sys
import urllib
import collections
import progressbar
import datadiff


class Request(object):
    def __init__(self, url, auth_key):

    def _prepare(self):
        """Create an http request."""

    def _parse_request_result(self, raw_json):
        """Parse a jsonObject into a list of TargetPoints."""

    def execute(self):
        """Execute the request and returns a list of TargetPoints, time, and err."""


class HostResult(object):

    def __init__(self, name, auth_key):

    def add_error(self, err):

    def add_time(self, time):

    def add_queryPoints(self, queryPoints):


class Diffable(object):
    # ...

class TargetPoints(Diffable):

    def __init__(self, name, ts_to_val): 
        # ...
    def mesure_disymmetries(other):
        """Return a array of the dissymmetries for each val"""
        # For each ts, mesure the dissymmetry between the two vals
        # dissymmetry = (|val1 - val2|) / (|val1| + |val2|)

class QueryPoints(Diffable):

    def __init__(self, query, targetPoints_list):
        # ...
    def mesure_disymmetries(other):
        """Return a array of the dissymmetries for each targetPoints""" 
        # For each couple of targetPoints call mesure_disymmetries
        # and mesure the percentage of non-equal points using a threshold.

class NamedPctls(object):
    def __init__(self, name, percentiles): 


class Printer(object):


class TxtPrinter(Printer):
    def __init__(self, file):
        self._file = file or sys.stdout

    def _print(*args, **kwargs):
        kwargs[file=self._file]
        print(*args, **kwargs)

    def print_header(self, opts):
        self._print("==HEADER==")

    def _print_namedPctls(self, NamedPctls, chip="", delay=""):

    def print_dissymetry_results(self, query_namedPctls_list, query_to_target_namedPctls_list):

    def print_errors(self, errors_1, errors_2):

    def print_times(self, time_namedPctls_1, time_namedPctls_2):


class HtmlPrinter(Printer):
    # ...


def _read_queries(filename):
    """Read the list of queries from a given input text file."""


def get_url_from_query(host, query, from_param, until_param):


def fetch_queries(hosts, auth_keys, queries):
    hostResults = []
    for i, host in enumerate(hosts):
        auth_key = auth_keys[i]
        hostResult = HostResult(host, auth_key)
        for query in queries:
            url = get_url_from_query(host, query, from_param, until_param)
            request = Request(url, auth_key)
            (targetPoints_list, time, err) = request.execute()
            hostResult.add_time(time)
            hostResult.add_error(err)
            hostResult.add_queryPoints(QueryPoints(query, targetPoints_list))
        hostResults.append(hostResult)
    return hostResults


def _compute_pctls(self, mesures):
    """Return 50pctl, 99pctl, and 999pctl in a dict for the given dissymetries."""


def compute_disymmetries_pctls(diffables_a, diffables_b):
    namedPctls_list = []
    for a, b in _safe_zip(diffables_a, diffables_b):
        assert a.name == b.name
        disymmetries = a.mesure_disymmetries(b)
        pctls = _compute_pctls(dissymmetries)
        namedPctls_list.append(NamedPctls(a.name, pctls))
     return namedPctls_list


def _safe_zip(diffables_a, diffables_b):
    """Return a safe list of tuples"""


def _setup_process(opts):
    """Instantiate all global vars."""


def _parse_opts(args):


def main(args=None):
    """Entry point for the module."""
    if not args:
        args = sys.argv[1:]
    opts = _parse_opts(args)

    _setup_process(opts)

    queries = _read_queries(opts.input_filename)
    hostResults = fetch_queries(opts.hosts, opts.auth_keys, queries)

    query_namedPctls_list  = compute_disymmetries_pctls(
        hostResults[0].queryPoints_list,
        hostResults[1].queryPoints_list)

    query_to_target_namedPctls_list = {}
    queryPoints_tuples = _safe_zip(
        hostResults[0].queryPoints_list,
        hostResults[1].queryPoints_list)
    for (queryPoints_1, queryPoints_2) in queryPoints_tuples:
        assert queryPoints_1.name == queryPoints_2.name
        target_namedPctls_list  = compute_disymmetries_pctls(
            queryPoints_1.targetPoints_list,
            queryPoints_2.targetPoints_list)
        query_to_target_namedPctls_list[queryPoints_1.name] = target_namedPctls_list

    time_namedPctls_list = []
    for hostResult in hostResults:
        time_pctls = _compute_pctls(hostResult.times)
        time_namedPctls = NamedPctls('time for :%s' % hostResult.name, time_pctls)
        time_namedPctls_list.append(time_namedPctls)


    # prints

if __name__ == '__main__':
    main()
