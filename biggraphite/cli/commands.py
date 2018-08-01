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
"""List of available commands."""

import argparse

from biggraphite import settings as bg_settings
from biggraphite.cli import (
    command_clean,
    command_copy,
    command_web,
    command_delete,
    command_du,
    command_graphite_web,
    command_info,
    command_list,
    command_read,
    command_repair,
    command_shell,
    command_stats,
    command_syncdb,
    command_test,
    command_write,
)

COMMANDS = [
    command_clean.CommandClean(),
    command_copy.CommandCopy(),
    command_web.CommandWeb(),
    command_delete.CommandDelete(),
    command_du.CommandDu(),
    command_graphite_web.CommandGraphiteWeb(),
    command_info.CommandInfo(),
    command_list.CommandList(),
    command_read.CommandRead(),
    command_repair.CommandRepair(),
    command_shell.CommandShell(),
    command_stats.CommandStats(),
    command_syncdb.CommandSyncdb(),
    command_test.CommandTest(),
    command_write.CommandWrite(),
]


def parse_opts(args):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="BigGraphite command line utility.")
    bg_settings.add_argparse_arguments(parser)
    subparsers = parser.add_subparsers(help="commands")
    for command in COMMANDS:
        subparser = subparsers.add_parser(
            command.NAME, add_help=False
        )  # accept -h for du
        # but we still want --help
        subparser.add_argument(
            "--help", action="help", help="show this help message and exit"
        )
        command.add_arguments(subparser)
        subparser.set_defaults(func=command.run)
    return parser.parse_args(args)
