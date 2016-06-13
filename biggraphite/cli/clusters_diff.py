#!/usr/bin/env python
# coding: utf-8
"""Compare two Graphite clusters for a given list of queries.

Through this module, a "query" is "the name of a query", a string.
"""
from __future__ import unicode_literals
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
import numpy
import operator
import abc


class Error(Exception):
    """Error."""


class RequestError(Error):
    """RequestError."""


class Request(object):
    """Runs HTTP requests and parses JSON."""

    def __init__(self, url, auth_key, timeout_s):
        """Create a Request."""
        self._request = self._prepare(url, auth_key)
        self._timeout_s = timeout_s

    def _prepare(self, url, auth_key):
        """Create an http request."""
        headers = {
            'Authorization': 'Basic %s' % auth_key,
        }
        request = urllib2.Request(url, None, headers)
        return request

    def _parse_request_result(self, json_str):
        """Parse a jsonObject into a list of DiffableTarget."""
        if not json_str:
            return []
        json_data = json.loads(json_str)

        diffable_targets = []
        for json_object in json_data:
            target = json_object['target']
            # target are not always formated in the same way in every cluster, so we delete spaces
            target = target.replace(' ', '')

            ts_to_val = {}
            for val, ts in json_object['datapoints']:
                ts_to_val[ts] = val

            diffable_targets.append(DiffableTarget(target, ts_to_val))
        return diffable_targets

    def execute(self):
        """Execute the request and returns a list of DiffableTarget, time_s."""
        start = time.time()
        diffable_targets = []
        try:
            response = urllib2.urlopen(self._request, timeout=self._timeout_s)
            json_str = response.read()
            diffable_targets = self._parse_request_result(json_str)
        except IOError as e:
            raise RequestError(e)
        time_s = time.time() - start

        return (diffable_targets, time_s)


class HostResult(object):
    """Store all information on a given host."""

    def __init__(self, name):
        """Create a HostResult."""
        self.name = name
        self.query_to_error = {}
        self.query_to_time_s = {}
        self.diffable_queries = []

    def add_error(self, query, e):
        """Add an error message for a given query."""
        self.query_to_error[query] = "%s" % e

    def add_time_s(self, query, time_s):
        """Add a recorded time to fetch a query."""
        self.query_to_time_s[query] = time_s

    def add_diffable_query(self, diffable_query):
        """Add a diffable_query to the list."""
        self.diffable_queries.append(diffable_query)


class Diffable(object):
    """ABC for diffable objects."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def mesure_dissymmetries(self, other):
        """Measure the list of differences between 0 and 1.0."""
        pass


class DiffableTarget(Diffable):
    """DiffableTarget."""

    def __init__(self, name, ts_to_val):
        """DiffableTarget."""
        self.name = name
        self.ts_to_val = ts_to_val

    def mesure_dissymmetries(self, other):
        """Return a array of the dissymmetries for each val."""
        dissymmetries = []
        all_ts_set = self.ts_to_val.viewkeys() | other.ts_to_val.viewkeys()

        for ts in all_ts_set:
            val1 = self.ts_to_val.get(ts)
            val2 = other.ts_to_val.get(ts)

            if val1 == val2:
                dissymmetries.append(0.0)
                continue

            if val1 is None or val2 is None:
                dissymmetries.append(1.0)
                continue

            # dissymmetry = (|val1 - val2|) / (|val1| + |val2|)
            dissymmetry = abs(val1 - val2)/(abs(val1)+abs(val2))
            dissymmetries.append(dissymmetry)
        return dissymmetries


class DiffableQuery(Diffable):
    """DiffableQuery."""

    def __init__(self, name, diffable_targets, threshold):
        """Create a DiffableQuery."""
        self.name = name
        self.threshold = threshold
        self.diffable_targets = diffable_targets

    def _mesure_non_equal_pts_percentage(self, diffable_targets1, diffable_targets2, threshold):
        val_dissymmetries = diffable_targets1.mesure_dissymmetries(diffable_targets2)
        if not val_dissymmetries:
            return 0.0

        count = 0.0
        for val_dissymmetry in val_dissymmetries:
            if val_dissymmetry > threshold:
                count += 1

        return count / len(val_dissymmetries)

    def mesure_dissymmetries(self, other):
        """Return a array of the dissymmetries for each diffable_target.

        For each couple of diffable_target it calls mesure_dissymmetries
        and mesure the percentage of non-equal points using a threshold.
        """
        if not self.diffable_targets and not other.diffable_targets:
            return [0.0]

        dissymmetries = []
        diffable_target_tuples = _outer_join_diffables(
            self.diffable_targets, other.diffable_targets)
        for diffable_targets1, diffable_targets2 in diffable_target_tuples:
            if diffable_targets1 is None or diffable_targets2 is None:
                dissymmetries.append(1.0)
                continue

            dissymmetry = self._mesure_non_equal_pts_percentage(
                diffable_targets1, diffable_targets2, self.threshold)
            dissymmetries.append(dissymmetry)
        return dissymmetries


class Percentiles(object):
    """Percentiles."""

    def __init__(self, name, mesures):
        """Create a Percentiles."""
        self.name = name
        self.pctls = self._compute_pctls(mesures)

    def _compute_pctls(self, mesures):
        """Return 50pctl, 90pctl, 99pctl, and 999pctl in a dict for the given dissymetries."""
        # If mesures is empty, then one of the diffables was empty and not the other,
        # so the dissymmetry pctls should all be set at 100%
        if not mesures:
            mesures = [1.0]

        numpy_array = numpy.array(mesures)
        pctls = collections.OrderedDict()
        pctls[50] = numpy.percentile(numpy_array, 50, interpolation='higher')
        pctls[90] = numpy.percentile(numpy_array, 90, interpolation='higher')
        pctls[99] = numpy.percentile(numpy_array, 99, interpolation='higher')
        pctls[99.9] = numpy.percentile(numpy_array, 99.9, interpolation='higher')
        return pctls

    def formatted(self, nth, unit):
        """Return the nth pctl formatted as str with unit."""
        val = self.pctls.get(nth)
        if unit == "%":
            val *= 100
        return "%s pctl : %0.3f %s" % (nth, val, unit)

    def get_99th(self):
        """Return only the 99pctl.

        Usefull to sort Percentiles.
        """
        return self.pctls.get(99, None)


class Printer(object):
    """ABC for printers."""

    QUERY_PCTLS_DESCRIPTION = "Percentage of almost non-equal points using threshold"
    TARGET_PCTLS_DESCRIPTION = "Percentage of dyssymmetries between values"
    TIMING_PCTLS_DESCRIPTION = "Durations in second to fetch queries"


class TxtPrinter(Printer):
    """TxtPrinter."""

    def __init__(self, file=None):
        """Create a txt Printer."""
        self._file = file or sys.stdout

    def _print(self, *args, **kwargs):
        """Print in a given file."""
        kwargs['file'] = self._file
        print(*args, **kwargs)

    def print_header(self, opts):
        """Print all used arguments of the script."""
        seq = [
            "",
            "==========================",
            "==========HEADER==========",
            "|",
            "| hosts :",
            "| \t- %s" % opts.hosts[0],
            "| \t- %s" % opts.hosts[1],
            "|",
            "| input filename : %s" % opts.input_filename,
            "| \t- from : %s" % opts.From,
            "| \t- until : %s" % opts.Until,
            "| \t- timeout : %s seconds" % opts.timeout_s,
            "|",
            "| threshold : %s" % opts.threshold,
            "|",
            "| max number of shown results : %s" % opts.show_max,
            "| timing information : %s" % opts.timing,
            "| datadiff result : %s" % opts.printdiff,
            ]
        self._print("\n".join(seq))

    def _print_percentiles(self, percentiles, prefix="", chip="", delay=""):
        self._print("\n %s %s %s : %s" % (delay, chip, prefix, percentiles.name))
        for k in percentiles.pctls.iterkeys():
            if prefix == "host":
                self._print("\t%s %s" % (delay, percentiles.formatted(k, unit="s")))
            else:
                self._print("\t%s %s" % (delay, percentiles.formatted(k, unit="%")))

    def print_dissymetry_results(self, query_percentiles_list, query_to_target_percentiles_list,
                                 verbosity, show_max):
        """Print all percentiles per query and per target.

        The list is limited with the show_max parameter
        and target percentiles are shown only if the verbose parameter is True.
        """
        self._print("\n==========================")
        self._print("====COMPARISON RESULTS====")
        self._print("The %s most dissymmetrical queries : " % show_max)

        query_percentiles_list = sorted(
            query_percentiles_list, key=Percentiles.get_99th, reverse=True)
        del query_percentiles_list[show_max:]

        self._print("\n%s for : " % (Printer.QUERY_PCTLS_DESCRIPTION))
        for query_percentiles in query_percentiles_list:
            self._print_percentiles(query_percentiles, "query", chip=">")

            if verbosity >= 1:
                self._print("\n\t%s for : " % (Printer.TARGET_PCTLS_DESCRIPTION))
                for target_percentiles in query_to_target_percentiles_list[query_percentiles.name]:
                    self._print_percentiles(target_percentiles, "target", chip=">>", delay="\t")

    def print_errors(self, hosts, error_counts_list, error_to_queries_list, verbosity):
        """Print per host the number of errors and the number of occurrences of errors.

        If the verbose parameter is True,
        the list of queries affected are printed after each error.
        """
        self._print("\n==========================")
        self._print("=======ERROR REPORT=======")
        for i, host in enumerate(hosts):
            error_counts = error_counts_list[i]
            total_errors = 0
            for error, count in error_counts:
                total_errors += count

            self._print("\nThere was %s error(s) for %s" % (total_errors, host))

            if error_counts:
                error_to_queries = error_to_queries_list[i]
                self._print("\tError counts : ")
                for (error, count) in error_counts:
                    self._print("\t > %s : %s" % (error, count))

                    if verbosity >= 1:
                        for query in error_to_queries[error]:
                            self._print("\t\t >> %s " % query)

    def print_times(self, time_s_percentiles_list, query_to_time_s_list, verbosity, show_max):
        """Print per host the durations percentiles for fetching queries.

        If the verbose parameter is True, the list of the slowest queries is printed,
        limited with the show_max parameter.
        """
        self._print("\n==========================")
        self._print("=====TIME INFORMATION=====")
        assert len(time_s_percentiles_list) == len(query_to_time_s_list)

        self._print("\n%s for : " % (Printer.TIMING_PCTLS_DESCRIPTION))
        for i, time_s_percentiles in enumerate(time_s_percentiles_list):
            self._print_percentiles(time_s_percentiles, prefix="host")

            if verbosity >= 1:
                query_to_time_s = query_to_time_s_list[i]
                query_time_s = sorted(
                    query_to_time_s.items(), key=operator.itemgetter(1), reverse=True)
                del query_time_s[show_max:]
                self._print("\n\tThe %s slowest queries : " % show_max)
                for (query, time_s) in query_time_s:
                    self._print("\t > %s : %s" % (query, time_s))


def _read_queries(filename):
    """Read the list of queries from a given input text file."""
    with open(filename, 'r') as f:
        lines = f.readlines()

    queries = []
    for line in lines:
        query = line.partition('#')[0]
        query = query.strip()
        if not query:
            continue
        queries.append(query)
    return queries


def _get_url_from_query(host, query, from_param, until_param):
    """Encode an url from a given query for a given host."""
    # If the query is not already url-friendly, we make it be
    if '%' not in query:
        query = urllib.quote(query)

    url = 'http://%s/render/?noCache&format=json&from=%s&until=%s&target=%s' % (
        host, from_param, until_param, query)
    return url


def fetch_queries(host, auth_key, queries,
                  from_param, until_param, timeout_s, threshold, pbar_update):
    """Return a list of HostResult."""
    host_result = HostResult(host)
    for n, query in enumerate(queries):
        url = _get_url_from_query(host, query, from_param, until_param)
        request = Request(url, auth_key, timeout_s)
        try:
            diffable_targets, time_s = request.execute()
            host_result.add_time_s(query, time_s)
            host_result.add_diffable_query(DiffableQuery(query, diffable_targets, threshold))
        except RequestError as e:
            host_result.add_error(query, e)
            host_result.add_diffable_query(DiffableQuery(query, [], threshold))

        pbar_update(n)
    return host_result


def compute_dissymmetries_pctls(diffables_a, diffables_b):
    """Return a list of dissymmetry percentiles from two given diffable lists."""
    if not diffables_a and not diffables_b:
        return [Percentiles("Empty", [0.0])]

    percentiles_list = []
    for a, b in _outer_join_diffables(diffables_a, diffables_b):
        if not a or not b:
            name = a.name if a else b.name
            percentiles_list.append(Percentiles(name, []))
            continue

        assert a.name == b.name
        dissymmetries = a.mesure_dissymmetries(b)
        if not dissymmetries:
            percentiles_list.append(Percentiles(a.name, []))
        else:
            percentiles_list.append(Percentiles(a.name, dissymmetries))
    return percentiles_list


def _outer_join_diffables(diffables_a, diffables_b):
    """Return a safe list of tuples of diffables whose name is equal."""
    index_a = {a.name: a for a in diffables_a}
    index_b = {b.name: b for b in diffables_b}

    names = {a.name for a in diffables_a}
    names.union({b.name for b in diffables_b})
    return [(index_a.get(n), index_b.get(n)) for n in names]


def _parse_opts(args):
    parser = argparse.ArgumentParser(
        description='Compare two Graphite clusters for a given list of queries.')
    parser.add_argument('--hosts', nargs=2, help='Cluster hostnames', required=True)
    parser.add_argument('-u', '--username', help='Username', required=True)

    auth = parser.add_mutually_exclusive_group(required=True)
    auth.add_argument('-p', '--passwords', nargs=2, help='Passwords')
    auth.add_argument('-k', '--auth_keys', nargs=2, help='Hosts keys')

    parser.add_argument('--input_filename', help='Text file containing queries', required=True)
    parser.add_argument('--From', help='From', default='-10minutes')
    parser.add_argument('--Until', help='Until', default='-2minutes')
    parser.add_argument(
        '--timeout_s', help='Timeout in seconds to fetch queries', type=float, default=5.0)
    parser.add_argument(
        '--threshold', help='Relative threshold for equality between two value',
        type=float, default=0.01)

    parser.add_argument(
        '--printdiff', dest='printdiff',
        help='Print the diff returned by datadiff module', action='store_true')

    diff_parser = parser.add_mutually_exclusive_group(required=False)
    diff_parser.add_argument(
        '--timing', dest='timing',
        help='Print timing information', action='store_true')
    diff_parser.add_argument(
        '--no-timing', dest='timing',
        help='Do not print timing information', action='store_false')
    parser.set_defaults(timing=True)

    parser.add_argument("-v", "--verbosity", action="count", default=0)
    parser.add_argument(
        "--show_max", help="Truncate the number of shown percentiles", type=int, default=5)

    # TODO (t.chataigner) enable several kind of outputs : txt, csv, html...

    opts = parser.parse_args(args)

    # compute auth_keys if not given
    if not opts.auth_keys:
        opts.auth_keys = []
        for password in opts.passwords:
            opts.auth_keys.append(
                base64.encodestring(opts.username + ':' + password).replace('\n', ''))

    return opts


def main(args=None):
    """Entry point for the module."""
    if not args:
        args = sys.argv[1:]
    opts = _parse_opts(args)

    # fetch inputs
    queries = _read_queries(opts.input_filename)

    host_results = []
    for i, host in enumerate(opts.hosts):
        pbar = progressbar.ProgressBar(maxval=len(queries)).start()
        host_result = fetch_queries(opts.hosts[i], opts.auth_keys[i], queries, opts.From,
                                    opts.Until, opts.timeout_s, opts.threshold, pbar.update)
        host_results.append(host_result)
        pbar.finish()

    # compute outputs
    query_percentiles_list = compute_dissymmetries_pctls(
        host_results[0].diffable_queries, host_results[1].diffable_queries)

    diffable_query_tuples = _outer_join_diffables(
        host_results[0].diffable_queries, host_results[1].diffable_queries)
    query_to_target_percentiles_list = {
        diffable_query_1.name: compute_dissymmetries_pctls(
            diffable_query_1.diffable_targets, diffable_query_2.diffable_targets)
        for (diffable_query_1, diffable_query_2) in diffable_query_tuples}

    time_s_percentiles_list = [
        Percentiles(h_r.name, h_r.query_to_time_s.values()) for h_r in host_results]
    query_to_time_s_list = [h_r.query_to_time_s for h_r in host_results]

    error_counts_list = [
        collections.Counter(h_r.query_to_error.values()).most_common() for h_r in host_results]

    error_to_queries_list = []
    for host_result in host_results:
        error_to_queries = collections.defaultdict(list)
        for query, err in host_result.query_to_error.iteritems():
            error_to_queries[err].append(query)
        error_to_queries_list.append(error_to_queries)

    # print outputs
    printer = TxtPrinter()
    printer.print_header(opts)

    if opts.timing:
        printer.print_times(
            time_s_percentiles_list, query_to_time_s_list, opts.verbosity, opts.show_max)

    printer.print_errors(
        opts.hosts, error_counts_list, error_to_queries_list, opts.verbosity)

    printer.print_dissymetry_results(
        query_percentiles_list, query_to_target_percentiles_list, opts.verbosity, opts.show_max)


if __name__ == '__main__':
    main()
