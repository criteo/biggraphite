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

_READER = None
_WORKER = None
_PRINTER = None

Fetched_Query = collections.namedtuple(
    'Fetched_Query',
    [
        'name',
        'durations',
        'fetched_urls_list',
    ],
    verbose=False)
Report = collections.namedtuple(
    'Report',
    [
        'name',
        'percentage_of_non_equal_points',
        'avg_relative_distance_per_point',
        'avg_duration_per_host_dict',
    ],
    verbose=False)


class _Reader():
    def __init__(self, opts):
        self._opts = opts

    def _create_request(self, key, url, data=None):
        """Create a Grafana API request."""
        headers = {
            'Authorization': 'Basic %s' % key,
        }
        request = urllib2.Request(url, data, headers)
        return request

    def _do_request(self, request):
        """Execute a request and returns the result."""
        response = urllib2.urlopen(request, self._opts.timeout)
        data = response.read()
        if data:
            data = json.loads(data)
        return data

    def _parse_fetched_url(self, raw_json):
        """Parse a fetched url.

        Parse a jsonObject into a dict that links each target to its datapoints.
        Datapoints is a dict that link each timestamp to a value.
        """
        if not raw_json:
            return {}

        target_to_datapoints_dict = {}
        for json_metric in raw_json:
            target = json_metric['target']
            # target are not always formated in the same way in every cluster, so we delete spaces
            target = target.replace(' ', '')

            timestamp_to_value_dict = {}
            for datapoint in json_metric['datapoints']:
                timestamp = datapoint[1]
                value = datapoint[0]
                timestamp_to_value_dict[timestamp] = value

            target_to_datapoints_dict[target] = timestamp_to_value_dict

        return target_to_datapoints_dict

    def _fetch_urls_list(self, urls_list):
        """Get responses to the couple of urls for each query and the durations to get them."""
        fetched_urls_list = []
        durations_dict = {}
        for i, url in enumerate(urls_list):
            request = self._create_request(self._opts.hosts_keys[i], url)

            try:
                before_ms = time.time() * 1000
                raw_json = self._do_request(request)
                after_ms = time.time() * 1000
                durations_dict[self._opts.hosts[i]] = (after_ms - before_ms)
            except Exception as err:
                sys.stderr.write(err)

            fetched_url = self._parse_fetched_url(raw_json)
            fetched_urls_list.append(fetched_url)

        return (fetched_urls_list, durations_dict)

    def _query_to_urls_list(self, query):
        """Encode an urls list from a given queries list."""
        # If the query is not already url-friendly, we make it be
        if '%' not in query:
            query = urllib.quote(query)

        urls_list = []
        for host in self._opts.hosts:
            urls_list.append('http://%s/render/?format=json&from=%s&until=%s&target=%s' % (
                host, self._opts.From, self._opts.Until, query))

        return urls_list

    def fetch_queries_list(self, queries_list):
        fetched_queries_list = []
        pbar = progressbar.ProgressBar().start()
        for i, query in enumerate(queries_list):
            urls_list = self._query_to_urls_list(query)
            (fetched_urls_list, durations_dict) = self._fetch_urls_list(urls_list)
            fetched_queries_list.append(Fetched_Query(query, durations_dict, fetched_urls_list))
            pbar.update((i+1)*100/len(queries_list))
        pbar.finish()
        return fetched_queries_list

    def read_queries_from_inputs(self):
        """Read the list of queries from a given input text file."""
        with open(self._opts.queries, 'r') as f:
            lines = f.readlines()

        queries = []
        for line in lines:
            query = line.partition('#')[0]
            query = query.strip()
            if query == "":
                continue
            query = query.replace(' ', '')
            queries.append(query)
        return queries


class _Worker():
    def __init__(self, opts):
        self._opts = opts

    def _get_avg_relative_distance_per_point(self, first_datapoints_dict, second_datapoints_dict):
        common_timestamps = set(first_datapoints_dict).intersection(set(second_datapoints_dict))

        total_number_timestamps = (
            len(set(first_datapoints_dict).union(set(second_datapoints_dict))))
        sum_relative_distance = (total_number_timestamps-len(common_timestamps)) * 100.0

        # If total_number_timestamps == 0, then neither of the hosts store data for the target,
        # so all distances are 0
        if (total_number_timestamps == 0):
            return 0.0

        for timestamp in common_timestamps:
            # If both datapoints_dict store a null, values are equal
            if not first_datapoints_dict[timestamp] and not second_datapoints_dict[timestamp]:
                continue
            # If only one datapoints_dict store a null, values are differents
            if not first_datapoints_dict[timestamp] or not second_datapoints_dict[timestamp]:
                sum_relative_distance = sum_relative_distance + 100.0
                continue
            relative_distance = (
                100 * abs(
                    (2 * (first_datapoints_dict[timestamp] - second_datapoints_dict[timestamp])) /
                    (first_datapoints_dict[timestamp] + second_datapoints_dict[timestamp]))
                )
            sum_relative_distance = sum_relative_distance + relative_distance

        avg_relative_distance_per_point = (
            (sum_relative_distance) / float(total_number_timestamps))
        return avg_relative_distance_per_point

    def _get_percentage_of_non_equal_points(self, first_datapoints_dict, second_datapoints_dict):
        threshold = self._opts.threshold
        common_timestamps = set(first_datapoints_dict).intersection(set(second_datapoints_dict))

        total_number_timestamps = (
            len(set(first_datapoints_dict).union(set(second_datapoints_dict))))
        count_equal_timestamps = len(common_timestamps)

        # If total_number_timestamps == 0, then neither of the hosts store data for the target,
        # so all points are equal
        if (total_number_timestamps == 0):
            return 0.0

        for timestamp in common_timestamps:
            # If both datapoints_dict store a null, values are equal
            if not first_datapoints_dict[timestamp] and not second_datapoints_dict[timestamp]:
                continue
            # If only one datapoints_dict store a null, values are differents
            if not first_datapoints_dict[timestamp] or not second_datapoints_dict[timestamp]:
                count_equal_timestamps = count_equal_timestamps - 1
                continue
            # otherwise values are differents if the relative distance is greater than threshold
            relative_distance = (
                100 * abs(
                    (2 * (first_datapoints_dict[timestamp] - second_datapoints_dict[timestamp])) /
                    (first_datapoints_dict[timestamp] + second_datapoints_dict[timestamp]))
                )
            if relative_distance > threshold:
                count_equal_timestamps = count_equal_timestamps - 1

        percentage_of_non_equal_points = (
            100 - (100 * count_equal_timestamps) / float(total_number_timestamps))
        return percentage_of_non_equal_points

    def _get_detailed_reports_per_target_dict(self, fetched_queries_list):
        detailed_reports_per_target_dict = {}
        for fetched_query in fetched_queries_list:
            fetched_urls_list = fetched_query.fetched_urls_list
            all_targets_set = set(fetched_urls_list[0]).union(set(fetched_urls_list[1]))

            detailed_reports_per_target_list = []
            for target in all_targets_set:
                first_datapoints_dict = fetched_urls_list[0].get(target, '{}')
                second_datapoints_dict = fetched_urls_list[1].get(target, '{}')

                detailed_report_per_target = Report(
                    name=target,
                    percentage_of_non_equal_points=(
                        self._get_percentage_of_non_equal_points(
                            first_datapoints_dict,
                            second_datapoints_dict)),
                    avg_relative_distance_per_point=(
                        self._get_avg_relative_distance_per_point(
                            first_datapoints_dict,
                            second_datapoints_dict)),
                    avg_duration_per_host_dict=None,
                    )

                detailed_reports_per_target_list.append(detailed_report_per_target)

            detailed_reports_per_target_dict[fetched_query.name] = detailed_reports_per_target_list

        return detailed_reports_per_target_dict

    def _get_detailed_reports_per_query_list(self, fetched_queries_list,
                                             detailed_reports_per_target_dict):
        detailed_reports_per_query_list = []
        for fetched_query in fetched_queries_list:
            percentage_of_non_equal_points = 0.0
            avg_relative_distance_per_point = 0.0

            target_reports = detailed_reports_per_target_dict[fetched_query.name]
            for target_report in target_reports:
                len_targets = len(target_reports)
                percentage_of_non_equal_points += (
                    getattr(target_report, 'percentage_of_non_equal_points') / len_targets)
                avg_relative_distance_per_point += (
                    getattr(target_report, 'avg_relative_distance_per_point') / len_targets)

            detailed_report_per_query = Report(
                name=fetched_query.name,
                percentage_of_non_equal_points=percentage_of_non_equal_points,
                avg_relative_distance_per_point=avg_relative_distance_per_point,
                avg_duration_per_host_dict=fetched_query.durations,
                )

            detailed_reports_per_query_list.append(detailed_report_per_query)

        return detailed_reports_per_query_list

    def _get_global_report(self, detailed_reports_per_query_list):
        percentage_of_non_equal_points = 0.0
        avg_relative_distance_per_point = 0.0
        avg_duration_per_host_dict = {self._opts.hosts[0]: 0.0, self._opts.hosts[1]: 0.0}

        for query_report in detailed_reports_per_query_list:
            len_queries = len(detailed_reports_per_query_list)
            percentage_of_non_equal_points += (
                getattr(query_report, 'percentage_of_non_equal_points') / len_queries)
            avg_relative_distance_per_point += (
                getattr(query_report, 'avg_relative_distance_per_point') / len_queries)

            for host in self._opts.hosts:
                avg_duration_per_host_dict[host] += (
                    getattr(query_report, 'avg_duration_per_host_dict').get(host, 0.0) /
                    len_queries)

        global_report = Report(
            name='Global report',
            percentage_of_non_equal_points=percentage_of_non_equal_points,
            avg_relative_distance_per_point=avg_relative_distance_per_point,
            avg_duration_per_host_dict=avg_duration_per_host_dict,
            )

        return global_report

    def compute_difference(self, fetched_queries_list):
        detailed_reports_per_target_dict = self._get_detailed_reports_per_target_dict(
            fetched_queries_list)
        detailed_reports_per_query_list = self._get_detailed_reports_per_query_list(
            fetched_queries_list, detailed_reports_per_target_dict)
        global_report = self._get_global_report(detailed_reports_per_query_list)

        return (
            global_report,
            detailed_reports_per_query_list,
            detailed_reports_per_target_dict,
            )

    def compute_printable_diff(self, fetched_queries_list):
        printable_queries_diff_list = []
        for fetched_query in fetched_queries_list:
            printable_query_diff = datadiff.diff(fetched_query.fetched_urls_list[0], fetched_query.fetched_urls_list[1])
            printable_queries_diff_list.append(printable_query_diff)

        return printable_queries_diff_list


class _Printer():
    def __init__(self, opts):
        self._opts = opts

    def get_printable_report(self, report, chip="", delay=""):
        seq = [
            "%s %s %s" % (delay, chip, report.name),
            "%s\t- percentage_of_non_equal_points  : %f%%" % (delay, report.percentage_of_non_equal_points),
            "%s\t- avg_relative_distance_per_point : %f%%" % (delay, report.avg_relative_distance_per_point),
            ]

        if report.avg_duration_per_host_dict:
            seq.append("%s\t- avg_duration_per_host_dict : " % delay)
            for host in self._opts.hosts:
                seq.append("%s\t\t%s -> %s ms" % (delay, host, report.avg_duration_per_host_dict[host]))

        seq.append("")
        printable_report = "\n".join(seq)

        return printable_report

    def get_printable_outputs(self, global_report, detailed_reports_per_query_list, detailed_reports_per_target_dict):
        # TODO (t.chataigner) finish dealing with datadiff
        if self._opts.printdiff:
            printable_queries_diff_list = _WORKER.compute_printable_diff(fetched_queries_list)

        seq = [
            "",
            self.get_printable_report(global_report),
            "",
            ]

        for query_report in detailed_reports_per_query_list:
            seq.append(self.get_printable_report(query_report, chip=">"))
            for target_report in detailed_reports_per_target_dict[query_report.name]:
                seq.append(self.get_printable_report(target_report, chip=">>", delay="\t"))

        return "\n".join(seq)


def _setup_process(opts):
    global _READER
    global _WORKER
    global _PRINTER
    _READER = _Reader(opts)
    _WORKER = _Worker(opts)
    _PRINTER = _Printer(opts)


def _parse_opts(args):
    parser = argparse.ArgumentParser(
        description='Compare two Graphite clusters for a given list of queries.')
    parser.add_argument('--hosts', nargs=2, help='Cluster hostnames', required=True)
    parser.add_argument('-u', '--username', help='Username', required=True)

    auth = parser.add_mutually_exclusive_group(required=True)
    auth.add_argument('-p', '--passwords', nargs=2, help='Passwords')
    auth.add_argument('-k', '--hosts_keys', nargs=2, help='Hosts keys')

    parser.add_argument('--queries', help='Text file containing queries', required=True)
    parser.add_argument('--From', help='From', default='-10minutes')
    parser.add_argument('--Until', help='Until', default='-2minutes')
    parser.add_argument('--timeout', help='Timeout in seconds to fetch queries', default='5')
    parser.add_argument(
        '--threshold', help='Relative threshold for equality between two value',
        type=float, default=1.0)
    parser.add_argument(
        '--printdiff', help='Print the diff returned by datadiff module', action="store_true")
    # TODO (t.chataigner) add verbose param
    # TODO (t.chataigner) add flag to print timing informations
    # TODO (t.chataigner) enable several kind of outputs : txt, csv, html...

    opts = parser.parse_args(args)

    # compute hosts_keys if not given
    if not opts.hosts_keys:
        opts.hosts_keys = []
        for password in opts.passwords:
            opts.hosts_keys.append(
                base64.encodestring(opts.username + ':' + password).replace('\n', ''))

    # hide passwords in futures sys.argv and opts calls
    for i, password in enumerate(opts.passwords):
        sys.argv = [w.replace(password, '******') for w in sys.argv]
        opts.passwords[i] = '******'

    return opts


def main(args=None):
    """Entry point for the module."""
    if not args:
        args = sys.argv[1:]
    opts = _parse_opts(args)

    _setup_process(opts)

    queries_list = _READER.read_queries_from_inputs()

    fetched_queries_list = _READER.fetch_queries_list(queries_list)

    (global_report, detailed_reports_per_query_list, detailed_reports_per_target_dict) = (
        _WORKER.compute_difference(fetched_queries_list))

    print(_PRINTER.get_printable_outputs(global_report, detailed_reports_per_query_list, detailed_reports_per_target_dict))

if __name__ == '__main__':
    main()
