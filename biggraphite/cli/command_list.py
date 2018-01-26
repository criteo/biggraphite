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
from biggraphite.glob_utils import graphite_glob


def list_metrics(accessor, pattern, graphite=True):
    """Return the list of metrics corresponding to pattern.

    Exit with error message if None.

    Args:
        accessor: Accessor, a connected accessor
        pattern: string, e.g. my.metric.a or my.metric.**.a

    Optional Args:
        graphite: bool, use graphite globbing if True.

    Returns:
        iterable(Metric)
    """
    if not graphite:
        metrics_names = accessor.glob_metric_names(pattern)
    else:
        metrics_names, _ = graphite_glob(
            accessor,
            pattern,
            metrics=True,
            directories=False
        )

    for metric in metrics_names:
        if metric is None:
            continue
        yield accessor.get_metric(metric)


class CommandList(command.BaseCommand):
    """List for metrics."""

    NAME = "list"
    HELP = "List metrics."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument(
            "glob",
            help="One metric name or globbing on metrics names"
        )
        parser.add_argument(
            "--graphite",
            default=False,
            action='store_true',
            help="Enable Graphite globbing"
        )

    def run(self, accessor, opts):
        """List metrics and directories.

        See command.CommandBase.
        """
        accessor.connect()

        if not opts.graphite:
            directories_names = accessor.glob_directory_names(opts.glob)
        else:
            _, directories_names = graphite_glob(
                accessor,
                opts.glob,
                metrics=False,
                directories=True
            )
        for directory in directories_names:
            print("d %s" % directory)
        for metric in list_metrics(accessor, opts.glob, opts.graphite):
            if metric:
                print("m %s %s" % (
                    metric.name,
                    metric.metadata.as_string_dict()
                ))
