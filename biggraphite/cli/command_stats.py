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
"""Stats Command."""

from __future__ import print_function

import collections
import datetime
import re
import random
import time
import socket
import logging

from os import environ

import tabulate
from six.moves.configparser import ConfigParser

from biggraphite.cli import command

from prometheus_client import write_to_textfile, CollectorRegistry, Gauge


# Hack to add some more formats.
# TODO: Add Graphite support.
# TODO: Remove padding.
tabulate._table_formats["csv"] = tabulate.TableFormat(
    lineabove=None,
    linebelowheader=None,
    linebetweenrows=None,
    linebelow=None,
    headerrow=tabulate.DataRow("", ";", ""),
    datarow=tabulate.DataRow("", ";", ""),
    padding=0,
    with_header_hide=None,
)

tabulate.tabulate_formats = list(sorted(tabulate._table_formats.keys()))


class Namespaces(object):
    r"""Helper for namespaces.

    The config file would look like:
    ```
    [carbon-relay]
    pattern = carbon\.relay\.*

    [carbon-cache]
    pattern = carbon\.agents\.*

    [carbon-aggregator]
    pattern = carbon\.aggregator\.*

    [prometheus]
    pattern = prometheus\.*
    ```
    """

    def __init__(self, filename=None):
        """Initializer."""
        self.config = ConfigParser({}, collections.OrderedDict)
        self.patterns = collections.OrderedDict()

        if not filename:
            self.patterns[re.compile(".*")] = "total"
            self.config.add_section("total")
            return

        self.config.read(filename)
        for section in self.config.sections():
            pattern = re.compile(self.config.get(section, "pattern"))
            self.patterns[pattern] = section

    def lookup(self, metric_name):
        """Return the namespace corresponding to the metric."""
        for pattern, section in self.patterns.items():
            if pattern.match(metric_name):
                return section, self.config.items(section)
        return "none", None


class CommandStats(command.BaseCommand):
    """Stats for metrics."""

    NAME = "stats"
    HELP = "disk usage if one or several specific metrics."

    def __init__(self, *args, **kwargs):
        """Initialize."""
        super(CommandStats, self).__init__(*args, **kwargs)
        self.ns = None
        self._n_metrics = collections.defaultdict(int)
        self._n_points = collections.defaultdict(int)

        expiration_days = 15

        env_expiration_days = environ.get('STATS_ANALYSIS_EXPIRATION')
        if env_expiration_days is not None and env_expiration_days.isdigit():
            expiration_days = int(env_expiration_days)

        self.delta = datetime.timedelta(days=expiration_days)
        self.cutoff = datetime.datetime.utcnow() - self.delta
        self.oldest = datetime.datetime.utcnow()

        self._n_count = 0
        self._n_range_size = 0
        self._n_count_expired = 0

        self._n_dir_count = 0
        self._n_dir_empty = 0

        self.metrics_file_path = ""

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        command.add_sharding_arguments(parser)
        parser.add_argument(
            "-c", "--conf", help="Configuration file for namespaces", dest="conf"
        )

        formats = tabulate.tabulate_formats
        formats.append("graphite")
        parser.add_argument(
            "-f", "--format", help="Format: %s" % ", ".join(formats), dest="fmt"
        )
        parser.add_argument(
            "--carbon",
            help="Carbon host:port to send points to when using graphite output."
        )
        parser.add_argument(
            "--prefix",
            help="Prefix to add to every section name.",
            default='',
        )
        parser.add_argument(
            "--metadata-analysis",
            help="Run a statistical analysis on metadata contents.",
            default=False,
        )
        parser.add_argument(
            "--metrics-file-path",
            help="Dump metrics in file",
            type=str,
            default="",
            action="store"
        )

        self._n_metrics = collections.defaultdict(int)
        self._n_points = collections.defaultdict(int)

    def statistical_analysis_stats(self, metric, done, total):
        """Aggregate stats for given metrics."""
        if metric.updated_on < self.cutoff:
            self._n_count_expired += 1

            if self.oldest > metric.updated_on:
                self.oldest = metric.updated_on

        self._n_count += 1

    def statistical_analysis_stats_directories(self, dir, result):
        """Aggregate stats for given directories."""
        if result == []:
            self._n_dir_empty += 1

        self._n_dir_count += 1

    def write_metrics(self):
        """Write metrics in textfile."""
        registry = CollectorRegistry()

        metric_scanned_range = Gauge(
            'scanned_range',
            'Total of offset ranges scanned',
            registry=registry)
        metric_scanned_range.set(self._n_range_size)

        metric_total = Gauge(
            'metric_total',
            'Total of metric found in offset ranges scanned',
            registry=registry)
        metric_total.set(self._n_count)

        metric_total_expired = Gauge(
            'metric_expired',
            'Total of expired metric found in offset ranges scanned',
            registry=registry)
        metric_total_expired.set(self._n_count_expired)

        multiplier = 2**64 / self._n_range_size

        metric_estimated_total = Gauge(
            'metric_estimated_total',
            'Estimated total of metric in database',
            registry=registry)
        metric_estimated_total.set(int(self._n_count * multiplier))

        metric_estimated_total_expired = Gauge(
            'metric_estimated_expired',
            'Estimated total of expired metric in database',
            registry=registry)
        metric_estimated_total_expired.set(int(self._n_count_expired * multiplier))

        directories_total = Gauge(
            'directories_total',
            'Total of directories found in offset ranges scanned',
            registry=registry)
        directories_total.set(self._n_dir_count)

        directories_total_empty = Gauge(
            'directories_empty',
            'Total of empty directories found in offset ranges scanned',
            registry=registry)
        directories_total_empty.set(self._n_dir_empty)

        directories_estimated_total = Gauge(
            'directories_estimated_total',
            'Estimated total of directories in database',
            registry=registry)
        directories_estimated_total.set(int(self._n_dir_count * multiplier))

        directories_estimated_total_empty = Gauge(
            'directories_estimated_empty',
            'Estimated total of empty directories in database',
            registry=registry)
        directories_estimated_total_empty.set(int(self._n_dir_empty * multiplier))

        # Final metric dump
        write_to_textfile(self.metrics_file_path, registry)

    def statistical_analysis(self, accessor):
        """Access cassandra database and get compute metrics."""
        range_size = 2**48

        env_exponent = environ.get('STATS_ANALYSIS_EXPONENT')
        if env_exponent is not None and env_exponent.isdigit():
            range_size = 2**int(env_exponent)

        scanned_ranges = 20

        env_ranges = environ.get('STATS_ANALYSIS_RANGES')
        if env_ranges is not None and env_ranges.isdigit():
            scanned_ranges = int(env_ranges)

        for i in range(0, scanned_ranges):
            start_offset = random.getrandbits(64) - 2**63
            self._n_range_size += range_size

            # Scan metrics
            accessor.sync_map(
                self.statistical_analysis_stats,
                start_key=start_offset,
                end_key=start_offset + range_size,
            )

            # Scan directories
            accessor.map_empty_directories_sync(
                self.statistical_analysis_stats_directories,
                start_key=start_offset,
                end_key=start_offset + range_size,
            )

        print("Range: %d (%f%%)" % (self._n_range_size, self._n_range_size / 2**64))
        print("Metrics extrated: %d; Outdated: %d (%.2f%%)" % (
            self._n_count,
            self._n_count_expired,
            100 * self._n_count_expired / self._n_count))
        multiplier = 2**64 / self._n_range_size
        print("Extrapolation: %d; Estimated outdated: %d" % (
            self._n_count * multiplier,
            self._n_count_expired * multiplier))
        print("Oldest metric found: %s" % self.oldest)

        print("Directories found: %d / Empty: %d" % (self._n_dir_count, self._n_dir_empty))

        if self.metrics_file_path != "":
            self.write_metrics()

    def run(self, accessor, opts):
        """Disk usage of metrics.

        See command.CommandBase.
        """
        self.ns = Namespaces(opts.conf)

        accessor.connect()

        self.metrics_file_path = opts.metrics_file_path

        if accessor.TYPE.startswith("elasticsearch+"):
            accessor = accessor._metadata_accessor

        if accessor.TYPE == "cassandra" and opts.metadata_analysis:
            self.statistical_analysis(accessor)
            return

        if accessor.TYPE == "elasticsearch":
            # Elasticsearch has a better implementation.
            self._n_metrics, self._n_points = accessor.metric_stats(self.ns)
        else:
            accessor.map(
                self.stats,
                start_key=opts.start_key,
                end_key=opts.end_key,
                shard=opts.shard,
                nshards=opts.nshards,
            )

        columns = ("Namespace", "Metrics", "Points")
        rows = [columns]

        if opts.fmt == "graphite":
            now = int(time.time())
            output = ""
            for k, v in self._n_metrics.items():
                output += "%smetrics.%s %s %s\n" % (opts.prefix, k, v, now)
            for k, v in self._n_points.items():
                output += "%spoints.%s %s %s\n" % (opts.prefix, k, v, now)
            if not opts.carbon:
                print(output)
            else:
                # This is a very-very cheap implementation of a carbon client.
                host, port = opts.carbon.split(':')
                logging.info("Sending data to %s:%s" % (host, port))
                sock = socket.socket()
                sock.connect((host, int(port)))
                sock.sendall(output)
                sock.close()
            return

        for k in self._n_metrics.keys():
            data = (
                '%s%s' % (opts.prefix, k),
                self._n_metrics.get(k),
                self._n_points.get(k)
            )
            rows.append(data)

        print(tabulate.tabulate(rows, headers="firstrow", tablefmt=opts.fmt))

    def stats(self, metric, done, total):
        """Compute stats."""
        ns, _ = self.ns.lookup(metric.name)
        self._n_metrics[ns] += 1
        self._n_points[ns] += metric.metadata.retention.points
