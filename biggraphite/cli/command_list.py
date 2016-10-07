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
"""List Command."""

from __future__ import print_function

from biggraphite.cli import command


def list_metrics(accessor, pattern):
    """Return the list of metrics corresponding to pattern. Exit with error message if None.

    Args:
        - accessor, Accessor, a connected accessor
        - pattern, string, e.g. my.metric.a or my.metric.**.a
    """
    metric_names = accessor.glob_metric_names(pattern)

    if not metric_names:
        print("Pattern '%s' doesn't match any metric" % pattern)
        exit(1)

    return [
        accessor.get_metric(metric)
        for metric in metric_names
    ]


class CommandList(command.BaseCommand):
    """List for metrics."""

    NAME = "list"
    HELP = "List metrics."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument(
            "metrics",
            help="One metric name or globbing on metrics names"
        )

    def run(self, accessor, opts):
        """Disk usage of metrics.

        See command.CommandBase.
        """
        accessor.connect()
        for metric in list_metrics(accessor, opts.metrics):
            print(metric.name)
