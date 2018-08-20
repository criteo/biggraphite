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
"""Command Skeleton."""

import argparse
import datetime

import parsedatetime


class BaseCommand(object):
    """Base command."""

    NAME = ""
    HELP = ""

    def add_arguments(self, parser):
        """Add custom arguments.

        Args:
          parser: argparse.Parser
        """
        pass

    def run(self, accessor, opts):
        """Run the command.

        Args:
          accessor: biggraphite.accessor.Accessor
          opts: argparse.Namespace
        """
        pass


# Following code comes from https://github.com/Clemson-DPA/dpa-pipe.
class ParseDateTimeArg(argparse.Action):
    """argparse.Action subclass. parses natural language cl datetime strings.

    Use this class as an argument to the 'action' argument when calling
    add_argument on an argparse parser. When the command line arguments are
    parsed, the resulting namespace will have a datetime.datetime object
    assigned to the argument's destination.

    If a datetime could not be parsed from the string, a ValueError will be
    raised.

    Examples of parsable human readable datetime strings:
        "now", "yesterday", "2 weeks from now", "3 days ago", etc.
    Note: When the datetime string is more than one word, you should include
    the argument in quotes on the command line.
    """

    def __call__(self, parser, namespace, datetime_str, option_string=None):
        """Do the thing."""
        parsed_datetime = date_time_from_str(datetime_str)
        setattr(namespace, self.dest, parsed_datetime)


def date_time_from_str(datetime_str):
    """Parse a humanly readable datetime.

    Args:
      datetime_str: str, humanly readable date time.

    Returns:
      datetime, correctponding datetime object.
    """
    cal = parsedatetime.Calendar()
    parsed_result, date_type = cal.parse(datetime_str)
    parsed_datetime = None

    if date_type == 3:
        # parsed_result is a datetime
        parsed_datetime = parsed_result
    elif date_type in (1, 2):
        # parsed_result is struct_time
        parsed_datetime = datetime.datetime(*parsed_result[:6])
    else:
        # Failed to parse
        raise ValueError("Could not parse date/time string: " + datetime_str)
    return parsed_datetime


def add_sharding_arguments(parser):
    """Add sharding arguments to a parser.

    repair()/clean()/map() all use the same arguments, this
    utility function will add the related command line options.
    """
    parser.add_argument("--start-key", help="Start key.")
    parser.add_argument("--end-key", help="End key.")
    parser.add_argument("--shard", help="Shard number.", type=int, default=0)
    parser.add_argument("--nshards", help="Number of shards.", type=int, default=1)
