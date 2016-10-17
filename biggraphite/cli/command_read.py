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
"""Read Command."""

from __future__ import print_function

import time
import datetime

from biggraphite.cli import command
from biggraphite.cli.command_list import list_metrics
from biggraphite import accessor as bg_accessor


class CommandRead(command.BaseCommand):
    """Read points."""

    NAME = "read"
    HELP = "read points for one or several specific metrics."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument(
            "metrics",
            help="One metric name or globbing on metrics names"
        )
        parser.add_argument(
            "--time-start",
            action=command.ParseDateTimeArg,
            help="Read points written later than this time.",
            default=datetime.datetime.now() - datetime.timedelta(minutes=10),
            required=False,
        )
        parser.add_argument(
            "--time-end",
            action=command.ParseDateTimeArg,
            help="Read points written earlier than this time.",
            default=datetime.datetime.now(),
            required=False,
        )
        parser.add_argument(
            "--stage",
            help="Read points from this specific stage.",
            default="",
            required=False,
        )
        parser.add_argument(
            "--async",
            help="Do reads asynchronously.",
            action="store_true"
        )

    def run(self, accessor, opts):
        """Read points.

        See command.CommandBase.
        """
        accessor.connect()

        metrics = list_metrics(accessor, opts.metrics)

        forced_stage = bg_accessor.Stage.from_string(opts.stage) if opts.stage else None
        time_start = opts.time_start
        time_end = opts.time_end

        async_results = []
        if opts.async:
            # Fetch all points asynchronously.
            for metric in metrics:
                results = self._fetch_points(
                    accessor, metric, time_start, time_end, forced_stage)
                async_results.append(results)
        else:
            async_results = [None] * len(metrics)

        for metric, results in zip(metrics, async_results):
            if not results:
                results = self._fetch_points(
                    accessor, metric, time_start, time_end, forced_stage)

            self._display_metric(metric, results)

    @staticmethod
    def _fetch_points(accessor, metric, time_start, time_end, stage):
        time_start = time.mktime(time_start.timetuple())
        time_end = time.mktime(time_end.timetuple())
        if stage:
            time_start = stage.round_up(time_start)
            time_end = stage.round_up(time_end)
        else:
            time_start, time_end, stage = metric.retention.align_time_window(
                time_start, time_end, time.time()
            )
            points = accessor.fetch_points(metric, time_start, time_end, stage)

        return (points, time_start, time_end, stage)

    @staticmethod
    def _display_metric(metric, results):
        """Print metric's information."""
        (points, time_start, time_end, stage) = results

        print("Name: ", metric.name)
        print("Time window: %s to %s" % (time_start, time_end))
        print("Stage: ", str(stage))
        print("Points:")

        for point in points:
            print('%s: %s' % (point[0], point[1]))
        print()
