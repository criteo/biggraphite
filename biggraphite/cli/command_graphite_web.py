#!/usr/bin/env python
# Copyright 2018 Criteo
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
"""Simulate Graphite-Web in a Command."""

from __future__ import print_function

import datetime
import os
try:
    import flamegraph
except ImportError:
    flamegraph = None

from biggraphite import utils as bg_utils
from biggraphite.cli import command
from biggraphite.cli import command_read


class _FakeMetric(object):
    def __init__(self, name):
        self.name = name


class CommandGraphiteWeb(command_read.CommandRead):
    """GraphiteWeb points."""

    NAME = "graphite_web"
    HELP = "read points for one or several specific metrics."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument(
            "patterns",
            nargs="+",
            help="One metric name or globbing on metrics names"
        )
        parser.add_argument(
            "--time-start",
            action=command.ParseDateTimeArg,
            help="GraphiteWeb points written later than this time.",
            default=datetime.datetime.now() - datetime.timedelta(minutes=10),
            required=False,
        )
        parser.add_argument(
            "--time-end",
            action=command.ParseDateTimeArg,
            help="GraphiteWeb points written earlier than this time.",
            default=datetime.datetime.now(),
            required=False,
        )
        parser.add_argument(
            "--output-csv",
            help="Output points in CSV format: metric;timestamp;value.",
            action="store_true"
        )
        parser.add_argument(
            "--profile",
            help="Start a profiler.",
        )
        parser.add_argument(
            "--no-output",
            help="Don't print results.",
            action="store_true"
        )

    def run(self, accessor, opts):
        """Ask fake Graphite Web for points.

        See command.CommandBase.
        """
        # import here to make sure everything is setup.
        os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'

        accessor.connect()

        from django.conf import settings as django_settings

        # Disable carbon link (enabled by default)
        django_settings.CARBONLINK_HOSTS = []
        # Make sure logging goes to stderr
        django_settings.LOG_FILE_INFO = '-'
        django_settings.LOG_FILE_EXCEPTION = '-'
        django_settings.LOG_FILE_CACHE = '-'
        django_settings.LOG_FILE_RENDERING = '-'

        from graphite import util as graphite_util
        from biggraphite.plugins import graphite

        settings = bg_utils.settings_from_confattr(opts, prefix='')
        metadata_cache = bg_utils.cache_from_settings(accessor, settings)
        metadata_cache.open()

        if opts.profile == "flamegraph":
            flamegraph.start_profile_thread(fd=open("./perf.log", "w"))

        finder = graphite.Finder(
            directories=[],
            accessor=accessor,
            metadata_cache=metadata_cache
        )

        time_start = graphite_util.timestamp(opts.time_start)
        time_end = graphite_util.timestamp(opts.time_end)
        output_csv = opts.output_csv

        results = finder.fetch(opts.patterns, time_start, time_end)
        for i, result in enumerate(results):
            # Change the output to match something that display_metrics
            # can work with.
            metric = _FakeMetric(result['path'])
            time_start, time_end, step = result['time_info']

            points = []
            for i, v in enumerate(result['values']):
                v = 0 if v is None else v
                points.append((time_start + step * i, v))

            result = (
                points,
                time_start,
                time_end,
                step
            )
            if not opts.no_output:
                self._display_metric(metric, result, output_csv)
