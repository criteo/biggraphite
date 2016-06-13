"""Compare two Graphite clusters for a given list of queries."""
from __future__ import absolute_import
from __future__ import print_function
import argparse
import urllib2
import json
import base64
import datadiff
import time
import sys
import urllib

_READER = None
# def _create_request(key, url, data=None):
#     """Create a Grafana API request."""
#     headers = {
#         'Authorization': 'Basic %s' % key,
#     }
#     request = urllib2.Request(url, data, headers)
#     return request


# def _do_request(request):
#     """Execute a request and returns the result."""
#     response = urllib2.urlopen(request, timeout=5)
#     data = response.read()
#     if data:
#         data = json.loads(data)
#     return data


# def _readQueriesList(textFileName):
#     """Read the list of queries from a given text file."""
#     f = open(textFileName, 'r')
#     lignes = f.readlines()
#     f.close()

#     queries = []
#     for ligne in lignes:
#         query = ligne.partition('#')[0]
#         query = query.strip()
#         if (query == ""):
#             continue
#         query = query.replace(' ', '')
#         queries.append(query)
#     return queries


# def _fetchQuery(hosts, keys, query, from_, until_):
#     """Get the response to a given query and the duration to get it."""
#     fetched_query = []
#     duration_ms = []

#     for i in range(len(hosts)):
#         host = hosts[i]
#         key = keys[i]
#         if '%' not in query:
#             query = urllib.quote(query)
#         url = 'http://%s/render/?format=json&from=%s&until=%s&target=%s' % (
#             host, from_, until_, query)
#         request = _create_request(key, url)

#         before_ms = time.time() * 1000
#         fetched_query.append(_do_request(request))
#         after_ms = time.time() * 1000
#         duration_ms.append(after_ms - before_ms)

#     return (fetched_query, duration_ms)


# def _parseQueryResponse(jsonMetrics):
#     """Parse a query response to get a dict."""
#     mappedMetrics = {}
#     for jsonMetric in jsonMetrics:
#         target = jsonMetric['target']
#         target = target.replace(
#             ' ', '')  # target not always formated in the same way in every cluster

#         mappedDatapoints = {}
#         for datapoint in jsonMetric['datapoints']:
#             timestamp = datapoint[1]
#             value = datapoint[0]
#             mappedDatapoints[timestamp] = value

#         mappedMetrics[target] = mappedDatapoints

#     return mappedMetrics


# def _jaccard_similarity(datapoints1, datapoints2, threshold=1.0):
#     """Compute the jaccard similarity between two set of datapoints."""
#     total = len(set(datapoints1).union(set(datapoints2)))
#     common = set(datapoints1).intersection(set(datapoints2))
#     score = len(common)
#     total_diff = total - score

#     for k in common:

#         if not datapoints1[k] and not datapoints2[k]:
#             continue

#         if not datapoints1[k] or not datapoints2[k]:
#             total_diff = total_diff + \
#                 2  # max diff is 200% in standard use cases
#             score = score - 1
#             continue

#         diff = abs((2 * (datapoints1[k] - datapoints2[k])) / (
#             datapoints1[k] + datapoints2[k]))
#         total_diff = total_diff + diff
#         if diff > threshold:
#             score = score - 1

#     similarity_percent = (100 * score) / float(total) if (total > 0) else 100.0
#     avg_diff_percent = (100 * total_diff) / float(
#         total) if (total > 0) else 100.0
#     result = {
#         'avg_diff_percent': avg_diff_percent,
#         'similarity_percent': similarity_percent
#     }

#     return result


# def _compute_similarity_target(target, fetched_queries, threshold):
#     """Compute similarity between a given target, resulting from the fetched query."""
#     datapoints1 = fetched_queries[0].get(target, '{}')
#     datapoints2 = fetched_queries[1].get(target, '{}')
#     result_target = _jaccard_similarity(datapoints1, datapoints2, threshold)
#     return result_target


# def _compute_similarity_query(
#         hosts, keys, query, from_, until_, threshold, printdiff=False):
#     """Compute similarity between the two hosts for the given query."""
#     result_query = {
#         'avg_diff_percent': 0.0,
#         'similarity_percent': 100.0,
#         'targets': {},
#         'duration_ms': {},
#     }

#     (fetched_queries, durations_ms) = _fetchQuery(
#         hosts, keys, query, from_, until_)
#     printed_diff = "%s" % datadiff.diff(fetched_queries[0], fetched_queries[1])

#     for i in range(2):
#         result_query['duration_ms'][hosts[i]] = durations_ms[i]
#         fetched_queries[i] = _parseQueryResponse(fetched_queries[i])

#     targets = set(fetched_queries[0]).union(set(fetched_queries[1]))
#     for target in targets:
#         result_target = _compute_similarity_target(
#             target, fetched_queries, threshold)

#         result_query['targets'][target] = result_target

#         result_query['avg_diff_percent'] = result_query[
#             'avg_diff_percent'] + result_target['avg_diff_percent'] / len(targets)
#         result_query['similarity_percent'] = result_query['similarity_percent'] - (
#             100.0 - result_target['similarity_percent']) / len(targets)

#     return (result_query, printed_diff)


# def _compute_similarity(
#         hosts, keys, queries, from_, until_, threshold, printdiff=False):
#     """Compute similarity between the two hosts for the given list of queries."""
#     printed_diff = ""
#     result = {
#         'avg_diff_percent': 0.0,
#         'similarity_percent': 100.0,
#         'queries': {},
#         'duration_ms': {},
#     }
#     for i in range(len(queries)):
#         sys.stdout.flush()
#         sys.stdout.write("\r%d%%" % (100 * (i + 1) / len(queries)))

#         query = queries[i]
#         try:
#             (result_query, printed_diff_query) = _compute_similarity_query(
#                 hosts, keys, query, from_, until_, threshold)
#             printed_diff = printed_diff + printed_diff_query

#             result['queries'][query] = result_query

#             result['avg_diff_percent'] = result['avg_diff_percent'] + \
#                 result_query['avg_diff_percent'] / len(queries)
#             result['similarity_percent'] = result['similarity_percent'] - \
#                 (100.0 - result_query['similarity_percent']) / len(queries)

#         except Exception as err:
#             print err

#     return (result, printed_diff)


# def _print_header(args, queries):
#     """Print the outputs header."""
#     to_print = (
#         "============================================",
#         "Comparing the two following clusters : ",
#         "   - " + args.hosts[0],
#         "   - " + args.hosts[1],
#         "",
#         "Arguments : ",
#         "   -from : " + args.From,
#         "   -until : " + args.Until,
#         "   -threshold : %s%%" % args.threshold,
#         "",
#         "List of queries : ",
#         )
#     for query in queries:
#         to_print.append(" - " + query)

#     print "\n".join(to_print)


# def _print_general(result):
#     """Print a general overview of the comparison result."""
#     table_result = prettytable.PrettyTable()
#     table_result.header = False
#     table_result.hrules = prettytable.ALL
#     table_result.add_row([
#         "Percentage of queries that are more different than threshold",
#         "%f%%" % (100 - result['similarity_percent'])
#         ])
#     table_result.add_row(
#         ["Avg diff percent per target", "%f%%" % (result['avg_diff_percent'])])
#     print "\nGeneral result :"
#     print table_result


# def _print_html_per_query(args, queries, result):
#     """Generate an html with result per query and print link."""
#     images = {}
#     table_result_query = prettytable.PrettyTable([
#         "",
#         "percentage of different queries",
#         "Avg relative diff percent per target",
#         "images host1", "images host2"
#         ])
#     for (query, result_query) in result['queries'].iteritems():
#         diff_queries_percent = (100.0 - result_query['similarity_percent'])
#         avg_diff_percent = (result_query['avg_diff_percent'])

#         # hack because PrettyTable does not allow us to use html in the table
#         for host in args.hosts:
#             if '%' not in query:  # if the query is not already url-friendly, we make it be
#                 query = urllib.quote(query)
#             url = 'http://%s/render/?format=%s&from=%s&until=%s&target=%s' % (
#                 host, "", args.From, args.Until, query)
#             image_html = "<img src=\"%s\" alt=\"\" />" % url
#             image_title = 'img_%s_%s' % (host, query)
#             images[image_title] = image_html

#         table_result_query.add_row([query, diff_queries_percent, avg_diff_percent, 'img_%s_%s' %
#                                    (args.hosts[0], query), 'img_%s_%s' % (args.hosts[1], query)])

#     table_result_query.sortby = "percentage of different queries"
#     table_result_query.format = True
#     html = table_result_query.get_html_string(
#         attributes={"name": "my_table", "class": "red_table"})
#     for image_title, image_html in images.iteritems():
#         html = html.replace(image_title, image_html)
#     filename = '/tmp/result_query.html'
#     open(filename, 'w').write(html)

#     print "\nResult per query : \nfile:///tmp/result_query.html"


# def _print_results(args, queries, result, diff=None):
#     """Print outputs of the script."""
#     _print_header(args, queries)
#     if diff:
#         print "\nOutput of datadiff : \n %s" % diff

#     _print_general(result)
#     _print_html_per_query(args, queries, result)


# class Reader
# class Worker
# class Printer
class _Reader():
    def __init__(self, opts):
        self._opts = opts

    def read_queries_from_inputs(self, queries):
        return 'toto'


def _setup_process(opts):
    global _READER
    _READER = _Reader(opts)

def _parse_opts(args):
    parser = argparse.ArgumentParser(
        description='Compare two Graphite clusters for a given list of queries.')
    parser.add_argument(
        '--hosts', nargs=2, help='Cluster hostnames', required=True)
    # TODO (t.chataigner) enable the use of keys from opts
    parser.add_argument('-u', '--username', help='Username', required=True)

    auth = parser.add_mutually_exclusive_group(required=True)
    auth.add_argument(
        '-p', '--passwords', nargs=2, help='Passwords')
    auth.add_argument(
        '-k', '--hosts_keys', nargs=2, help='Hosts keys')

    parser.add_argument(
        '--queries', help='Text file containing queries', required=True)
    parser.add_argument('--From', help='From', default='-10minutes')
    parser.add_argument('--Until', help='Until', default='-2minutes')
    parser.add_argument(
        '--threshold', help='Relative threshold for equality between two value',
        type=float, default=1.0)
    parser.add_argument(
        '--printdiff', help='Print the diff returned by datadiff module', action="store_true")
    parser.add_argument(
        '--timing', help='Print the timing informations', action="store_true")
    # TODO (t.chataigner) add verbose param
    # TODO (t.chataigner) add flag to print timing informations
    # TODO (t.chataigner) enable several kind of outputs : txt, csv, html...

    opts = parser.parse_args(args)

    if not opts.hosts_keys:
        opts.hosts_keys = []
        for i in range(2):
            opts.hosts_keys.append(base64.encodestring(opts.username + ':' + opts.passwords[i]).replace('\n', ''))

    return opts


def main(args=None):
    """Entry point for the module."""
    if not args:
        args = sys.argv[1:]
    opts = _parse_opts(args)

    _setup_process(opts)

    queries_list = _READER.read_queries_from_inputs(opts.queries)
    print(queries_list)



    # (result, diff) = _compute_similarity(
    #     opts.hosts, keys, queries, opts.From, opts.Until, opts.threshold)
    # if not opts.printdiff:
    #     diff = None

    # _print_results(opts, queries, result, diff)


if __name__ == '__main__':
    main()
