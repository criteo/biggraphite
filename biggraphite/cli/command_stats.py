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
import re
import time
import socket
import logging

import tabulate
from six.moves.configparser import ConfigParser

from biggraphite.cli import command

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
        self._n_metrics = collections.defaultdict(int)
        self._n_points = collections.defaultdict(int)

    def run(self, accessor, opts):
        """Disk usage of metrics.

        See command.CommandBase.
        """
        self.ns = Namespaces(opts.conf)

        accessor.connect()

        if accessor.TYPE.startswith("elasticsearch+"):
            accessor = accessor._metadata_accessor

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
