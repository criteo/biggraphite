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
"""Copy Command."""

from __future__ import print_function

import time
import datetime
import logging

from biggraphite.cli import command
from biggraphite.cli.command_list import list_metrics
from biggraphite.drivers.cassandra import TooManyMetrics


log = logging.getLogger(__name__)


class CommandCopy(command.BaseCommand):
    """Copy for metrics."""

    NAME = "copy"
    HELP = "Copy metrics."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument(
            "src",
            help="One source metric or subdirectory name"
        )
        parser.add_argument(
            "dst",
            help="One destination metric or subdirectory name"
        )
        parser.add_argument(
            "-r",
            "--recursive",
            action="store_const", default=False, const=True,
            help="Copy points for all metrics as a subtree",
        )
        parser.add_argument(
            "--time-start",
            action=command.ParseDateTimeArg,
            help="Copy points written later than this time.",
            default=datetime.datetime.now() - datetime.timedelta(minutes=10),
            required=False,
        )
        parser.add_argument(
            "--time-end",
            action=command.ParseDateTimeArg,
            help="Copy points written earlier than this time.",
            default=datetime.datetime.now(),
            required=False,
        )
        parser.add_argument(
            "--dry-run",
            action="store_const", default=False, const=True,
            help="Only show commands to create/upgrade the schema.",
            required=False,
        )

    def run(self, accessor, opts):
        """Copy metrics and directories.

        See command.CommandBase.
        """
        accessor.connect()

        metric_tuples = self._get_metric_tuples(
            accessor, opts.src, opts.dst, opts.recursive, opts.dry_run)
        if opts.dry_run:
            for m, _ in metric_tuples:
                log.info("Metric to copy : %s" % m.name)
            return

        time_start = time.mktime(opts.time_start.timetuple())
        time_end = time.mktime(opts.time_end.timetuple())

        for src_metric, dst_metric in metric_tuples:
            self._copy_metric(accessor, src_metric, dst_metric, time_start, time_end)

    @staticmethod
    def _get_metric_tuples(accessor, src, dst, recursive, dry_run=True):
        pattern = "%s.**" % src if recursive else src
        try:
            src_metrics = list_metrics(accessor, pattern)
        except TooManyMetrics as e:
            log.error("%s; copy aborted" % e)
            return

        for src_metric in src_metrics:
            dst_metric_name = src_metric.name.replace(src, dst, 1)
            dst_metric = accessor.get_metric(dst_metric_name)
            if dst_metric is None:
                log.debug("Metric '%s' was not found and will be created" % dst_metric_name)
                if not dry_run:
                    dst_metric = accessor.make_metric(dst_metric_name, src_metric.metadata)
                    accessor.create_metric(dst_metric)

            yield (src_metric, dst_metric)

    @staticmethod
    def _copy_metric(accessor, src_metric, dst_metric, time_start, time_end):
        """Copy a metric."""
        log.info("Copying points from '%s' to '%s'" % (src_metric.name, dst_metric.name))
        for stage in src_metric.retention.stages:
            rounded_time_start = stage.round_up(time_start)
            rounded_time_end = stage.round_up(time_end)

            points = []
            res = accessor.fetch_points(src_metric, rounded_time_start, rounded_time_end, stage)
            # TODO (t.chataigner) : store aggregated points correctly
            for timestamp, value in res:
                if timestamp >= rounded_time_start and timestamp <= rounded_time_end:
                    points.append((timestamp, value, 1, stage))

            accessor.insert_downsampled_points(dst_metric, points)
