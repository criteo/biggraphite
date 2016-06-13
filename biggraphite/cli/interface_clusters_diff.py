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

QUERY_RESULT = collections.namedtuple(
    'QUERY_RESULT',
    (
        'name',
        'host_to_avg_duration_s',
        'target_to_ts_to_val_1',
        'target_to_ts_to_val_2'
    ),
    verbose=False)

STAT = collections.namedtuple(
    'STAT',
    (
        'name',
        'type',
        'dissymmetries_pctl_dict',
    ),
    verbose=False)

QUERY_STAT = collections.namedtuple(
    'QUERY_STAT',
    STAT._fields + (
        'host_to_avg_duration_s',
        'target_stats',
        ),
    verbose=False)

class _HttpHelper():
    def __init__(self, opts):

    def _create_request(self, key, url, data=None):
        """Create an http request."""

    def _do_request(self, request):
        """Execute a request and returns the result on a json format if possible."""

    def _parse_url_result(self, raw_json):
        """Parse a jsonObject into a dict that links each target to its ts_to_val."""

    def fetch_urls(self, urls):
        """Return responses to urls and the durations in second to get them."""


class _StatsHelper():
    def __init__(self, opts):

    def _percentiles(self, dissymetries):
        """Return 50pctl, 99pctl, and 999pctl in a dict for the given dissymetries."""

    def _val_dissymmetry(self, val1, val2):
        """Percentage of relative difference between two values."""
        # Suggestion : dissimmetry = (val1 - val2) / (val1 + val2)

    def _target_dissymmetry(self, ts_to_val_dissymmetry):
        """Percentage of almost equal points at a given threshold."""
        # threshold is in self._opts.threshold

    def _join_on_target_ts(self, target_to_ts_to_val1, target_to_ts_to_val2):
        """Return target_to_ts_to_val_dissymmetry.

        Join two target_to_ts_to_val on target and ts,
        then replace (val1, val2) by dissymmetry."""

    def _get_target_dissymmetry_dict(self, target_to_ts_to_val_dissymmetry):
        """Return the dict of target_dissymmetry from a given target_to_ts_to_val_dissymmetry"""

    def _get_target_stats(self, target_to_ts_to_val_dissymmetry):
        """Return the list of target_stat from a given target_to_ts_to_val_dissymmetry"""
        # target_stats = []
        # for (target, ts_to_val_dissymmetry) in  target_to_ts_to_val_dissymmetry.iteritems():
        #   val_dissymmetries = ts_to_val_dissymmetry.values()
        #   val_percentiles = _percentiles(val_dissymmetries)
        #   target_stat = STAT(target, 'target', val_percentiles)
        #   target_stats.append(target_stat)

        # return target_stats

    def get_query_stats(self, query_results):
        """Return the list of query_stat from a given query_results"""
        # query_stats = []
        # for query_result in query_results:
        #   target_to_ts_to_val_1 = query_result.target_to_ts_to_val_1
        #   target_to_ts_to_val_2 = query_result.target_to_ts_to_val_2
        
        #   target_to_ts_to_val_dissymmetry = (
        #     _join_on_target_ts(target_to_ts_to_val_1, target_to_ts_to_val_2))
        
        #   target_stats = _get_target_stats(target_to_ts_to_val_dissymmetry)
        
        #   target_dissymmetry_dict = _get_target_dissymmetry_dict(target_to_ts_to_val_dissymmetry)
        #   target_dissymmetries = target_dissymmetry_dict.values()
        #   target_percentiles = _percentiles(target_dissymmetries)
        
        #   query_stat = QUERY_STAT(
        #       query_result.name, 'query', target_percentiles,
        #       query_result.host_to_avg_duration_s, target_stats)
        #   query_stats.append(query_stat)
        
        # return query_stats


class _Controler():
    def __init__(self, opts):

    def _query_to_urls(opts, query):
        """Encode an url list (one per host) from a given query."""

    def fetch_query_results(opts, queries):
        """Return query results"""

    def get_query_datadiff_results(self, query_results):


class _Printer():
    def __init__(self, opts):

    def _get_printable_stat(self, stat, chip="", delay=""):

    def _get_printable_opts(self):

    def get_printable_outputs(self, query_stats):

    def get_printable_datadiff_result(self, query_datadiff_results):


def _read_queries(filename):
    """Read the list of queries from a given input text file."""


def _setup_process(opts):
    """Instantiate all global vars."""


def _parse_opts(args):


def main(args=None):
    """Entry point for the module."""


if __name__ == '__main__':
    main()
