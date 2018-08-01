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
"""A CLI to interact with BigGraphite."""

from __future__ import print_function

import sys

from biggraphite import accessor_factory as bg_accessor_factory
from biggraphite import settings as bg_settings
from biggraphite import utils as bg_utils
from biggraphite.cli import command_shell
from biggraphite.cli import commands


def main(args=None, accessor=None):
    """Entry point for the module."""
    if not args:
        args = sys.argv[1:]

    opts = commands.parse_opts(args)
    if not getattr(opts, "func", None):
        opts.func = command_shell.CommandShell().run

    settings = bg_settings.settings_from_args(opts)
    bg_utils.set_log_level(settings)
    accessor = accessor or bg_accessor_factory.accessor_from_settings(settings)
    opts.func(accessor, opts)
    accessor.flush()
    accessor.shutdown()


if __name__ == "__main__":
    main()
