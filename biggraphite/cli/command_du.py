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
"""Du Command."""

from __future__ import print_function

import humanize

from biggraphite.cli import command
from biggraphite.cli.command_list import list_metrics

_BYTES_PER_POINT = 24


class CommandDu(command.BaseCommand):
    """Du for metrics."""

    NAME = "du"
    HELP = "disk usage if one or several specific metrics."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument(
            "-h", help="Human readable units", action="store_true", dest="unit"
        )
        parser.add_argument(
            "-s", help="Summary, i.e. total size", action="store_true", dest="total"
        )
        parser.add_argument(
            "metrics", help="One metric name or globbing on metrics names"
        )

    def run(self, accessor, opts):
        """Disk usage of metrics.

        See command.CommandBase.
        """
        accessor.connect()
        total = 0
        for metric in list_metrics(accessor, opts.metrics):
            points = metric.metadata.retention.points
            total += points
            self._display_metric_size(
                metric.name, metric.metadata.retention.points, opts.unit
            )

        if opts.total:
            self._display_metric_size("TOTAL", total, opts.unit)

    @staticmethod
    def _human_size_of_points(nb_points, use_unit=True):
        size = nb_points * _BYTES_PER_POINT
        return humanize.naturalsize(size, gnu=True) if use_unit else size

    @staticmethod
    def _display_metric_size(metric_name, nb_points, use_unit):
        """Print metric's disk usage."""
        size = CommandDu._human_size_of_points(nb_points, use_unit)
        print("%s %s" % (size, metric_name))
