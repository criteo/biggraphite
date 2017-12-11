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
"""Info Command."""

from __future__ import print_function

from biggraphite.cli import command


class CommandInfo(command.BaseCommand):
    """Display metric metadata."""

    NAME = "info"
    HELP = "Display metadata for a given metric."

    def add_arguments(self, parser):
        """Add custom arguments.

        See command.CommandBase.
        """
        parser.add_argument("metric")

    def run(self, accessor, opts):
        """Display metric.

        See command.CommandBase.
        """
        accessor.connect()
        metric = accessor.get_metric(opts.metric)
        if metric is None:
            print("Metric '%s' doesn't exist" % opts.metric)
            return

        print("Name: ", metric.name)
        print("Metadata: ", metric.metadata.as_string_dict())
