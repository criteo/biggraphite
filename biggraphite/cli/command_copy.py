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
import copy

from biggraphite.cli import command
from biggraphite.cli.command_list import list_metrics
from biggraphite.drivers.cassandra import TooManyMetrics
from biggraphite import accessor as bg_accessor


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
        parser.add_argument(
            "--src_retention",
            help="Retention used to read points from the source metrics.",
            required=False,
        )
        parser.add_argument(
            "--dst_retention",
            help="Retention used to write points to the destination metrics.\
                  It only works if retentions are similar, i.e. with same precisions.",
            required=False,
        )

    def run(self, accessor, opts):
        """Copy metrics and directories.

        See command.CommandBase.
        """
        accessor.connect()

        metric_tuples = self._get_metric_tuples(
            accessor, opts.src, opts.dst, opts.recursive,
            opts.src_retention, opts.dst_retention, opts.dry_run)

        if opts.dry_run:
            for m, _ in metric_tuples:
                log.info("Metric to copy : %s" % m.name)
            return

        time_start = time.mktime(opts.time_start.timetuple())
        time_end = time.mktime(opts.time_end.timetuple())

        for src_metric, dst_metric in metric_tuples:
            self._copy_metric(accessor, src_metric,
                              dst_metric, time_start, time_end)

    @staticmethod
    def _get_metric_tuples(accessor, src, dst, recursive,
                           src_retention, dst_retention, dry_run=True):
        pattern = "%s.**" % src if recursive else src
        try:
            src_metrics = list_metrics(accessor, pattern)
        except TooManyMetrics as e:
            log.error("%s; copy aborted" % e)
            return

        # Prepare retention override
        if src_retention:
            src_retention = bg_accessor.Retention.from_string(src_retention)
        if dst_retention:
            dst_retention = bg_accessor.Retention.from_string(dst_retention)

        for src_metric in src_metrics:
            if src_retention and src_metric.metadata.retention != src_retention:
                src_metric.metadata.retention = src_retention

            dst_metric_name = src_metric.name.replace(src, dst, 1)
            dst_metric = accessor.get_metric(dst_metric_name)

            if dst_metric is None:
                log.debug("Metric '%s' was not found and will be created" %
                          dst_metric_name)
                dst_metadata = copy.deepcopy(src_metric.metadata)
                if dst_retention:
                    dst_metadata.retention = dst_retention
                dst_metric = accessor.make_metric(
                    dst_metric_name, dst_metadata)
                if not dry_run:
                    accessor.create_metric(dst_metric)
            elif dst_retention and dst_metric.metadata.retention != dst_retention:
                log.debug("Metric '%s' was found without '%s' retention and will be updated" % (
                    dst_metric_name, dst_retention.as_string))
                dst_metric.metadata.retention = dst_retention
                if not dry_run:
                    accessor.update_metric(
                        dst_metric_name, dst_metric.metadata)

            yield (src_metric, dst_metric)

    @staticmethod
    def _copy_metric(accessor, src_metric, dst_metric, time_start, time_end):
        """Copy a metric.

        Points are copied only from a given stage to a stage with the same precision
        """
        log.info("Copying points from '%s' to '%s'" %
                 (src_metric.name, dst_metric.name))
        src_precision_to_stage = {
            s.precision: s for s in src_metric.retention.stages}
        for dst_stage in dst_metric.retention.stages:
            if dst_stage.precision not in src_precision_to_stage:
                continue
            src_stage = src_precision_to_stage[dst_stage.precision]

            rounded_time_start = src_stage.round_up(time_start)
            rounded_time_end = src_stage.round_up(time_end)

            res = accessor.fetch_points(
                src_metric, rounded_time_start, rounded_time_end, src_stage, aggregated=False)

            points = []
            for timestamp, value, count in res:
                if timestamp >= rounded_time_start and timestamp <= rounded_time_end:
                    points.append((timestamp, value, count, dst_stage))

            accessor.insert_downsampled_points(dst_metric, points)
