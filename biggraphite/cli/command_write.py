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
"""Write command."""

from __future__ import print_function

import time
import datetime

from biggraphite.cli import command
from biggraphite import accessor as bg_accessor


class CommandWrite(command.BaseCommand):
    """Write points."""

    NAME = "write"
    HELP = "write a point to a given metric, creating it if it does not exist."

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "metric",
            help="Name of the metric to update.",
        )
        parser.add_argument(
            "value",
            help="Value to write at the select time.",
        )
        parser.add_argument(
            "-t",
            "--timestamp",
            help="Timestamp at which to write the new point.",
            action=command.ParseDateTimeArg,
            default=datetime.datetime.now(),
            required=False,
        )
        parser.add_argument(
            "-c",
            "--count",
            help="Count associated with the value to be written.",
            default=1,
            required=False,
        )
        # TODO(d.forest): support for writing directly a chosen stage
        aggregators = ', '.join([str(v.value) for v in bg_accessor.Aggregator])
        parser.add_argument(
            "--aggregator",
            help="Aggregator function for the metric (%s)." % aggregators,
            default="average",
            required=False,
        )
        parser.add_argument(
            "--retention",
            help="Retention configuration for the metric.",
            default="86400*1s:10080*60s",
            required=False,
        )
        parser.add_argument(
            "--x-files-factor",
            help="Science fiction coefficient.",
            default=0.5,
            required=False,
        )

    def run(self, accessor, opts):
        """Run the command."""
        accessor.connect()

        metric = accessor.get_metric(opts.metric)
        if not metric:
            print("Metric '%s' was not found and will be created" % opts.metric)
            metadata = bg_accessor.MetricMetadata(
                aggregator=bg_accessor.Aggregator.from_config_name(
                    opts.aggregator),
                retention=bg_accessor.Retention.from_string(opts.retention),
                carbon_xfilesfactor=opts.x_files_factor,
            )
            metric = accessor.make_metric(opts.metric, metadata)
            accessor.create_metric(metric)

        timestamp = int(time.mktime(opts.timestamp.timetuple()))
        points = [(timestamp, float(opts.value))] * opts.count
        accessor.insert_points_async(metric, points)
