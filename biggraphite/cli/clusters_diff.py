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
"""Compare two Graphite clusters for a given list of queries.

Through this module, a "query" is "the name of a query", a string.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import abc
import argparse
import base64
import collections
import json
import logging
import netrc
import operator
import sys
import time

import progressbar
import six
from six.moves.urllib import parse
from six.moves.urllib import request as urllib


class Error(Exception):
    """Error."""


class RequestError(Error):
    """RequestError."""


class Request(object):
    """Runs HTTP requests and parses JSON."""

    def __init__(self, url, auth_key, timeout_s, data=None):
        """Create a Request."""
        self._request = self._prepare(url, auth_key, data=data)
        self._timeout_s = timeout_s

    def _prepare(self, url, auth_key, data=None):
        """Create an http request."""
        headers = {}
        if auth_key is not None:
            headers["Authorization"] = "Basic %s" % auth_key
        request = urllib.Request(url, data, headers)
        return request

    def _parse_request_result(self, json_str):
        """Parse a jsonObject into a list of DiffableTarget."""
        if not json_str:
            return []

        try:
            json_data = json.loads(json_str)
        except ValueError:
            return []

        diffable_targets = []
        for json_object in json_data:
            if "target" not in json_object:
                continue

            target = json_object["target"]
            # target are not always formated in the same way in every cluster, so we delete spaces
            target = target.replace(" ", "")

            ts_to_val = {ts: val for val, ts in json_object["datapoints"]}

            diffable_targets.append(DiffableTarget(target, ts_to_val))
        return diffable_targets

    def execute(self):
        """Execute the request and returns a list of DiffableTarget, time_s."""
        start = time.time()
        diffable_targets = []
        try:
            response = urllib.urlopen(self._request, timeout=self._timeout_s)
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

    def compute_timing_pctls(self):
        """Compute a dict of pctl from query_to_time_s values."""
        return _compute_pctls(self.query_to_time_s.values())

    def get_error_to_query(self):
        """Reverse query_to_error to get error_to_queries."""
        error_to_queries = collections.defaultdict(list)
        for query, err in self.query_to_error.items():
            error_to_queries[err].append(query)
        return error_to_queries


class Diffable(object):
    """ABC for diffable objects."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def measure_dissymmetry(self, other):
        """Return measure of difference as a Dissymmetry."""
        pass


class DiffableTarget(Diffable):
    """DiffableTarget."""

    def __init__(self, name, ts_to_val):
        """DiffableTarget."""
        self.name = name
        self.ts_to_val = ts_to_val

    def _measure_relative_gap(self, val1, val2):
        if val1 == val2:
            return 0.0

        if val1 is None or val2 is None:
            return 1.0

        return abs(val1 - val2) / (abs(val1) + abs(val2))

    def measure_dissymmetry(self, other):
        """Return measure of difference as a Dissymmetry."""
        other_ts_to_val = other.ts_to_val if other else {}
        all_ts_set = six.viewkeys(self.ts_to_val) | six.viewkeys(other_ts_to_val)

        if not all_ts_set:
            return None

        val_tuples = [
            (self.ts_to_val.get(ts), other_ts_to_val.get(ts)) for ts in all_ts_set
        ]
        diff_measures = [
            self._measure_relative_gap(val1, val2) for val1, val2 in val_tuples
        ]

        return Dissymmetry(self.name, diff_measures)


class DiffableQuery(Diffable):
    """DiffableQuery."""

    def __init__(self, name, diffable_targets, threshold):
        """Create a DiffableQuery."""
        self.name = name
        self.threshold = threshold
        self.diffable_targets = diffable_targets

    def _measure_non_equal_pts_percentage(
        self, diffable_targets1, diffable_targets2, threshold
    ):
        if not diffable_targets1 or not diffable_targets2:
            return 1.0

        # if target_dissymmetry is None, both host responses were empty for this target name
        target_dissymmetry = diffable_targets1.measure_dissymmetry(diffable_targets2)
        if not target_dissymmetry:
            return 0.0

        val_diff_measures = target_dissymmetry.measures
        count = 0
        for val_diff_measure in val_diff_measures:
            if val_diff_measure > threshold:
                count += 1

        return float(count) / len(val_diff_measures)

    def measure_dissymmetry(self, other):
        """Return measure of difference as a Dissymmetry."""
        # For each couple of diffable_target it calls measure_dissymmetries
        # and measure the percentage of non-equal points using a threshold.
        diffable_target_tuples = _outer_join_diffables(
            self.diffable_targets, other.diffable_targets
        )

        if not diffable_target_tuples:
            return None

        diff_measures = [
            self._measure_non_equal_pts_percentage(
                diffable_target1, diffable_target2, self.threshold
            )
            for diffable_target1, diffable_target2 in diffable_target_tuples
        ]
        return Dissymmetry(self.name, diff_measures)


class Dissymmetry(object):
    """Dissymmetry."""

    def __init__(self, name, measures):
        """Create a Dissymmetry."""
        self.name = name
        self.measures = measures
        self.pctls = _compute_pctls(measures)

    def get_99th(self):
        """Return only the 99pctl.

        Usefull to sort Percentiles.
        """
        return self.pctls.get(99, None)


class Printer(object):
    """ABC for printers."""

    __metaclass__ = abc.ABCMeta

    QUERY_PCTLS_DESCRIPTION = "Percentage of almost non-equal points using threshold"
    TARGET_PCTLS_DESCRIPTION = "Percentage of dyssymmetries between values"
    TIMING_PCTLS_DESCRIPTION = "Durations in second to fetch queries"

    @abc.abstractmethod
    def print_parameters(self, opts):
        """Print all parameters of the script."""
        pass

    @abc.abstractmethod
    def print_dissymetry_results(
        self, query_dissymmetries, query_to_target_dissymmetries, verbosity, show_max
    ):
        """Print all percentiles per query and per target.

        The list is limited with the show_max parameter
        and target percentiles are shown only if the verbose parameter is True.
        """
        pass

    @abc.abstractmethod
    def print_errors(
        self, hosts, error_counts_tuple, error_to_queries_tuple, verbosity
    ):
        """Print per host the number of errors and the number of occurrences of errors.

        If the verbose parameter is True,
        the list of queries affected are printed after each error.
        """
        pass

    @abc.abstractmethod
    def print_times(
        self, hosts, timing_pctls_tuple, query_to_time_s_tuple, verbosity, show_max
    ):
        """Print per host the durations percentiles for fetching queries.

        If the verbose parameter is True, the list of the slowest queries is printed,
        limited with the show_max parameter.
        """
        pass


class TxtPrinter(Printer):
    """TxtPrinter."""

    def __init__(self, fp=None):
        """Create a txt Printer."""
        self._file = fp or sys.stdout

    def _print(self, *args, **kwargs):
        """Print in a given file."""
        kwargs["file"] = self._file
        print(*args, **kwargs)

    def _format_header(self, header_title):
        seq = ["", "".center(30, "="), header_title.upper().center(30, "=")]
        return "\n".join(seq)

    def _format_pctl(self, pctls, nth, unit):
        val = pctls.get(nth)
        if unit == "%":
            val *= 100
        return "%s pctl : %0.3f %s" % (nth, val, unit)

    def print_parameters(self, opts):
        """See Printer. Print all parameters of the script."""
        seq = [
            self._format_header("Parameters"),
            "|",
            "| netrc_filename : %s" % (opts.netrc_filename or "$HOME/$USER/.netrc"),
            "|",
            "| hosts :",
            "| \t- %s" % opts.hosts[0],
            "| \t- %s" % opts.hosts[1],
            "|",
            "| inputs filename : %s" % opts.input_filename,
            "| \t- from : %s" % opts.from_param,
            "| \t- until : %s" % opts.until_param,
            "| \t- timeout : %s seconds" % opts.timeout_s,
            "| \t- threshold : %s %%" % opts.threshold,
            "|",
            "| outputs filename : %s" % (opts.output_filename or "stdout"),
            "| \t- max number of shown results : %s" % opts.show_max,
            "| \t- verbosity : %s" % opts.verbosity,
            "|",
        ]
        self._print("\n".join(seq))

    def _print_pctls(self, name, pctls, prefix="", chip="", delay=""):
        self._print("\n %s %s %s : %s" % (delay, chip, prefix, name))
        if pctls is None:
            return
        for k in pctls.iterkeys():
            if prefix == "host":
                self._print("\t%s %s" % (delay, self._format_pctl(pctls, k, unit="s")))
            else:
                self._print("\t%s %s" % (delay, self._format_pctl(pctls, k, unit="%")))

    def print_dissymetry_results(
        self, query_dissymmetries, query_to_target_dissymmetries, verbosity, show_max
    ):
        """See Printer. Print all percentiles per query and per target.

        The list is limited with the show_max parameter
        and target percentiles are shown only if the verbose parameter is True.
        """
        self._print(self._format_header("Comparison results"))

        all_query_dissymmetries_count = len(query_dissymmetries)
        query_dissymmetries = [
            query_dissymmetry
            for query_dissymmetry in query_dissymmetries
            if query_dissymmetry
        ]
        self._print(
            "\nThere was %s queries with empty response on both clusters."
            % (all_query_dissymmetries_count - len(query_dissymmetries))
        )

        query_dissymmetries = sorted(
            query_dissymmetries, key=Dissymmetry.get_99th, reverse=True
        )
        del query_dissymmetries[show_max:]

        self._print("\nThe %s most dissymmetrical queries : " % show_max)
        self._print("%s for : " % Printer.QUERY_PCTLS_DESCRIPTION)
        for query_dissymmetry in query_dissymmetries:
            self._print_pctls(
                query_dissymmetry.name, query_dissymmetry.pctls, "query", chip=">"
            )

            if verbosity >= 1:
                self._print("\n\tThe %s most dissymmetrical targets : " % show_max)
                self._print("\t%s for : " % Printer.TARGET_PCTLS_DESCRIPTION)
                for target_dissymmetry in query_to_target_dissymmetries[
                    query_dissymmetry.name
                ]:
                    self._print_pctls(
                        target_dissymmetry.name,
                        target_dissymmetry.pctls,
                        "target",
                        chip=">>",
                        delay="\t",
                    )

    def print_errors(
        self, hosts, error_counts_tuple, error_to_queries_tuple, verbosity
    ):
        """See Printer. Print per host the number of errors and the number of occurrences of errors.

        If the verbose parameter is True,
        the list of queries affected are printed after each error.
        """
        self._print(self._format_header("Error report"))
        for i, host in enumerate(hosts):
            error_counts = error_counts_tuple[i]
            total_errors = 0
            for error, count in error_counts:
                total_errors += count

            self._print("\nThere was %s error(s) for %s" % (total_errors, host))

            if error_counts:
                error_to_queries = error_to_queries_tuple[i]
                self._print("\tError counts : ")
                for (error, count) in error_counts:
                    self._print("\t > %s : %s" % (error, count))

                    if verbosity >= 1:
                        for query in error_to_queries[error]:
                            self._print("\t\t >> %s " % query)

    def print_times(
        self, hosts, timing_pctls_tuple, query_to_time_s_tuple, verbosity, show_max
    ):
        """See Printer. Print per host the durations percentiles for fetching queries.

        If the verbose parameter is True, the list of the slowest queries is printed,
        limited with the show_max parameter.
        """
        self._print(self._format_header("Timing info"))

        self._print("\n%s for : " % Printer.TIMING_PCTLS_DESCRIPTION)
        for i in range(2):
            host = hosts[i]
            timing_pctls = timing_pctls_tuple[i]
            self._print_pctls(host, timing_pctls, prefix="host")

            if verbosity >= 1:
                query_to_time_s = query_to_time_s_tuple[i]
                query_time_s = sorted(
                    query_to_time_s.items(), key=operator.itemgetter(1), reverse=True
                )
                del query_time_s[show_max:]
                self._print("\n\tThe %s slowest queries : " % show_max)
                for (query, time_s) in query_time_s:
                    self._print("\t > %s : %s" % (query, time_s))


def _read_queries(filename):
    """Read the list of queries from a given input text file."""
    with open(filename, "r") as f:
        lines = f.readlines()

    queries = []
    for line in lines:
        query = line.partition("#")[0]
        query = query.strip()
        if not query:
            continue
        queries.append(query)
    return queries


def _get_url_from_query(host, prefix, query, from_param, until_param):
    """Encode an url from a given query for a given host."""
    # If the query is not already url-friendly, we make it be
    if "%" not in query:
        query = parse.quote(query)

    url = "http://%s/render/?noCache&format=json&from=%s&until=%s&target=%s" % (
        host,
        from_param,
        until_param,
        prefix + query,
    )
    return url


def fetch_queries(
    host,
    prefix,
    auth_key,
    queries,
    from_param,
    until_param,
    timeout_s,
    threshold,
    progress_cb,
):
    """Return a list of HostResult."""
    host_result = HostResult(host)
    for n, query in enumerate(queries):
        url = _get_url_from_query(host, prefix, query, from_param, until_param)
        request = Request(url, auth_key, timeout_s)
        try:
            diffable_targets, time_s = request.execute()
            host_result.add_time_s(query, time_s)
            host_result.add_diffable_query(
                DiffableQuery(query, diffable_targets, threshold)
            )
        except RequestError as e:
            host_result.add_error(query, e)
            host_result.add_diffable_query(DiffableQuery(query, [], threshold))

        progress_cb(n)
    return host_result


def _compute_pctls(measures):
    """Return 50pctl, 90pctl, 99pctl, and 999pctl in a dict for the given dissymetries."""
    # If measures is empty, then one of the diffables was empty and not the other,
    # so the dissymmetry pctls should all be set at 100%
    if not measures:
        return None

    pctls = collections.OrderedDict()

    sorted_measures = sorted(measures)
    for i in [50, 75, 90, 99, 99.9]:
        # Don't even try to interpolate.
        rank = int(i / 100. * (len(measures)))
        pctls[i] = sorted_measures[rank]

    return pctls


def compute_dissymmetries(diffables_a, diffables_b):
    """Return a list of dissymmetry from two given diffable lists."""
    if not diffables_a and not diffables_b:
        return []

    dissymmetries = []
    for a, b in _outer_join_diffables(diffables_a, diffables_b):
        if not a:
            dissymmetry = b.measure_dissymmetry(a)
        else:
            dissymmetry = a.measure_dissymmetry(b)
        dissymmetries.append(dissymmetry)
    return dissymmetries


def _outer_join_diffables(diffables_a, diffables_b):
    """Return a safe list of tuples of diffables whose name is equal."""
    index_a = {a.name: a for a in diffables_a}
    index_b = {b.name: b for b in diffables_b}

    names = {a.name for a in diffables_a}
    names.update([b.name for b in diffables_b])
    return [(index_a.get(n), index_b.get(n)) for n in names]


def _parse_opts(args):
    parser = argparse.ArgumentParser(
        description="Compare two Graphite clusters for a given list of queries.",
        epilog='Through this module, a "query" is "the name of a query", a string.',
    )

    # authentication
    authentication = parser.add_argument_group("authentication")
    authentication.add_argument(
        "--netrc-file",
        metavar="FILENAME",
        dest="netrc_filename",
        action="store",
        help="a netrc file (default: $HOME/$USER/.netrc)",
        default="",
    )

    # clusters parameters
    comparison_params = parser.add_argument_group("comparison parameters")
    comparison_params.add_argument(
        "--hosts",
        metavar="HOST",
        dest="hosts",
        action="store",
        nargs=2,
        help="hosts to compare",
        required=True,
    )
    comparison_params.add_argument(
        "--prefixes",
        metavar="PREFIX",
        dest="prefixes",
        action="store",
        nargs=2,
        help="prefix for each host.",
        required=False,
        default=["", ""],
    )
    comparison_params.add_argument(
        "--input-file",
        metavar="FILENAME",
        dest="input_filename",
        action="store",
        help="text file containing one query per line",
        required=True,
    )
    comparison_params.add_argument(
        "--from",
        metavar="FROM_PARAM",
        dest="from_param",
        action="store",
        default="-24hours",
        help="from param for Graphite API (default: %(default)s)",
    )
    comparison_params.add_argument(
        "--until",
        metavar="UNTIL_PARAM",
        dest="until_param",
        action="store",
        default="-2minutes",
        help="until param for Graphite API (default: %(default)s)",
    )
    comparison_params.add_argument(
        "--timeout",
        metavar="SECONDS",
        dest="timeout_s",
        action="store",
        type=float,
        help="timeout in seconds used to fetch queries (default: %(default)ss)",
        default=5,
    )
    comparison_params.add_argument(
        "--threshold",
        metavar="PERCENT",
        action="store",
        type=float,
        default=1,
        help="percent threshold to evaluate equality between two values (default: %(default)s%%)",
    )

    # outputs parameters
    outputs_params = parser.add_argument_group("outputs parameters")
    outputs_params.add_argument(
        "--output-file",
        metavar="FILENAME",
        dest="output_filename",
        action="store",
        help="file containing outputs (default: stdout)",
        default="",
    )
    outputs_params.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="increases verbosity, can be passed multiple times",
    )
    outputs_params.add_argument(
        "--show-max",
        metavar="N_LINES",
        dest="show_max",
        type=int,
        default=5,
        help="truncate the number of shown dissymmetry in outputs (default: %(default)s)",
    )

    # TODO (t.chataigner) enable several kind of outputs : txt, csv, html...

    opts = parser.parse_args(args)

    # compute authentication keys from netrc file
    opts.auth_keys = [None, None]
    for host in opts.hosts:
        auth = netrc.netrc().authenticators(host)
        if auth is not None:
            username = auth[0]
            password = auth[2]
            opts.auth_keys.append(
                base64.encodestring(username + ":" + password).replace("\n", "")
            )
        else:
            logging.info(netrc.NetrcParseError("No authenticators for %s" % host))

    opts.threshold /= 100

    return opts


def main(args=None):
    """Entry point for the module."""
    if not args:
        args = sys.argv[1:]
    opts = _parse_opts(args)

    # fetch inputs
    queries = _read_queries(opts.input_filename)

    # host_result_1
    pbar = progressbar.ProgressBar(maxval=len(queries)).start()
    host_result_1 = fetch_queries(
        opts.hosts[0],
        opts.prefixes[0],
        opts.auth_keys[0],
        queries,
        opts.from_param,
        opts.until_param,
        opts.timeout_s,
        opts.threshold,
        pbar.update,
    )
    pbar.finish()
    # host_result_2
    pbar = progressbar.ProgressBar(maxval=len(queries)).start()
    host_result_2 = fetch_queries(
        opts.hosts[1],
        opts.prefixes[1],
        opts.auth_keys[1],
        queries,
        opts.from_param,
        opts.until_param,
        opts.timeout_s,
        opts.threshold,
        pbar.update,
    )
    pbar.finish()

    # compute outputs
    query_dissymmetries = compute_dissymmetries(
        host_result_1.diffable_queries, host_result_2.diffable_queries
    )

    diffable_query_tuples = _outer_join_diffables(
        host_result_1.diffable_queries, host_result_2.diffable_queries
    )
    query_to_target_dissymmetries = {
        diffable_query_1.name: compute_dissymmetries(
            diffable_query_1.diffable_targets, diffable_query_2.diffable_targets
        )
        for diffable_query_1, diffable_query_2 in diffable_query_tuples
    }

    timing_pctls_tuple = (
        host_result_1.compute_timing_pctls(),
        host_result_2.compute_timing_pctls(),
    )
    query_to_time_s_tuple = (
        host_result_1.query_to_time_s,
        host_result_2.query_to_time_s,
    )

    error_counts_tuple = (
        collections.Counter(host_result_1.query_to_error.values()).most_common(),
        collections.Counter(host_result_2.query_to_error.values()).most_common(),
    )
    error_to_queries_tuple = (
        host_result_1.get_error_to_query(),
        host_result_2.get_error_to_query(),
    )

    # print outputs
    if not opts.output_filename:
        fp = sys.stdout
    else:
        fp = open(opts.output_filename, "w")

    printer = TxtPrinter(fp=fp)
    printer.print_parameters(opts)

    printer.print_times(
        opts.hosts,
        timing_pctls_tuple,
        query_to_time_s_tuple,
        opts.verbosity,
        opts.show_max,
    )

    printer.print_errors(
        opts.hosts, error_counts_tuple, error_to_queries_tuple, opts.verbosity
    )

    printer.print_dissymetry_results(
        query_dissymmetries,
        query_to_target_dissymmetries,
        opts.verbosity,
        opts.show_max,
    )


if __name__ == "__main__":
    main()
