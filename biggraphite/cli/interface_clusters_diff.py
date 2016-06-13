"""Compare two Graphite clusters for a given list of queries."""
from __future__ import absolute_import
from __future__ import print_function


class Error(Exception):
    """Error."""

    pass


class RequestError(Error):
    """RequestError."""

    pass


class Request(object):
    """Request."""

    def __init__(self, url, auth_key, timeout_s):
        """Create  a Request."""
        pass

    def _prepare(self, url, auth_key):
        """Create an http request."""
        pass

    def _parse_request_result(self, raw_data):
        """Parse a jsonObject into a list of DiffableTarget."""
        pass

    def execute(self):
        """Execute the request and returns a list of DiffableTarget, time_s."""
        pass


class HostResult(object):
    """HostResult."""

    def __init__(self, name, auth_key):
        """Create a HostResult."""
        pass

    def add_error(self, query, e_msg):
        """Add an error message for a given query."""
        pass

    def add_time_s(self, query, time_s):
        """Add a recorded time to fetch a query."""
        pass

    def add_diffable_query(self, diffable_query):
        """Add a diffable_query to the list."""
        pass


class Diffable(object):
    """Diffable."""


class DiffableTarget(Diffable):
    """DiffableTarget."""

    def __init__(self, name, ts_to_val):
        """Create a DiffableTarget."""
        pass

    def mesure_dissymmetries(self, other):
        """Return a array of the dissymmetries for each val."""
        pass


class DiffableQuery(Diffable):
    """DiffableQuery."""

    def __init__(self, name, diffable_targets, threshold):
        """Create a DiffableQuery."""
        pass

    def _mesure_non_equal_pts_percentage(self, val_dissymmetries, threshold):
        pass

    def mesure_dissymmetries(self, other):
        """Return a array of the dissymmetries for each diffable_target.

        For each couple of diffable_target it calls mesure_dissymmetries
        and mesure the percentage of non-equal points using a threshold.
        """
        pass


class Percentiles(object):
    """Percentiles."""

    def __init__(self, name, mesures):
        """Create a Percentiles."""
        pass

    def _compute_pctls(self, mesures):
        """Return 50pctl, 90pctl, 99pctl, and 999pctl in a dict for the given dissymetries."""
        pass

    def get_99th(self):
        """Return only the 99pctl.

        Usefull to sort Percentiles.
        """
        pass


class Printer(object):
    """Printer."""

    pctls_kind_to_description = {
        "query": "Percentage of almost non-equal points using threshold",
        "target": "Percentage of dyssymmetries between values",
        "timing": "Durations in second to fetch queries",
        }


class TxtPrinter(Printer):
    """TxtPrinter."""

    def __init__(self, file=None):
        """Create a txt Printer."""
        pass

    def _print(self, *args, **kwargs):
        """Print in a given file."""
        pass

    def print_header(self, opts):
        """Print all used arguments of the script."""
        pass

    def _print_percentiles(self, percentiles, prefix="", chip="", delay=""):
        pass

    def print_dissymetry_results(self, query_percentiles_list, query_to_target_percentiles_list,
                                 verbosity, show_max):
        """Print all percentiles per query and per target.

        The list is limited with the show_max parameter
        and target percentiles are shown only if the verbose parameter is True.
        """
        pass

    def print_errors(self, hosts, error_counts_list, error_to_queries_list, verbosity):
        """Print per host the number of errors and the number of occurrences of errors.

        If the verbose parameter is True,
        the list of queries affected are printed after each error.
        """
        pass

    def print_times(self, time_s_percentiles_list, query_to_time_s_list, verbosity, show_max):
        """Print per host the durations percentiles for fetching queries.

        If the verbose parameter is True, the list of the slowest queries is printed,
        limited with the show_max parameter.
        """
        pass


def _read_queries(filename):
    """Read the list of queries from a given input text file."""
    pass


def _get_url_from_query(host, query, from_param, until_param):
    """Encode an url from a given query for a given host."""
    pass


def fetch_queries(hosts, auth_keys, queries, from_param, until_param, timeout_s, threshold):
    """Return a list of HostResult."""
    pass


def compute_dissymmetries_pctls(diffables_a, diffables_b):
    """Return a list of dissymmetry percentiles from two given diffable lists."""
    pass


def _outer_join_diffables(diffables_a, diffables_b):
    """Return a safe list of tuples of diffables whose name is equal."""
    pass


def _parse_opts(args):
    pass


def main(args=None):
    """Entry point for the module."""
    pass


if __name__ == '__main__':
    main()
