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
"""bgutil API."""

from __future__ import absolute_import

import argparse
import logging

import flask_restplus as rp

from biggraphite.cli.web import context
from biggraphite.cli.web.capture import Capture
from biggraphite import settings as bg_settings


api = rp.Namespace("bgutil", description="bgutil as a service")

command = api.model(
    "Command",
    {"arguments": rp.fields.List(rp.fields.String(), description="command arguments")},
)


class UnknownCommandException(Exception):
    """Unknown command exception."""

    def __init__(self, command_name):
        """Init UnknownCommandException."""
        super(UnknownCommandException, self).__init__(
            "Unknown command: %s" % command_name
        )


def parse_command(command_name, payload):
    """Parse and build a BgUtil command."""
    # Import that here only because we are inside a command and `commands`
    # need to be able to import files from all commands.
    from biggraphite.cli import commands

    cmd = None
    for cmd in commands.COMMANDS:
        if cmd.NAME == command_name:
            break
    if not cmd or cmd.NAME != command_name:
        raise UnknownCommandException(command_name)

    parser = NonExitingArgumentParser(add_help=False)
    parser.add_argument(
        "--help",
        action=_HelpAction,
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )
    bg_settings.add_argparse_arguments(parser)
    cmd.add_arguments(parser)

    if not payload:
        arguments = []
    else:
        arguments = payload.get("arguments", [])

    args = [a for a in arguments]
    opts = parser.parse_args(args)

    return cmd, opts


class _HelpAction(argparse.Action):
    """Help Action that sends an exception."""

    def __init__(
        self,
        option_strings,
        dest=argparse.SUPPRESS,
        default=argparse.SUPPRESS,
        help=None,
    ):
        """Constructor."""
        super(_HelpAction, self).__init__(
            option_strings=option_strings,
            dest=dest,
            default=default,
            nargs=0,
            help=help,
        )

    def __call__(self, parser, namespace, values, option_string=None):
        """Help action."""
        raise Exception(parser.format_help())


class NonExitingArgumentParser(argparse.ArgumentParser):
    """An ArgumentParser that doesn't exit."""

    def exit(self, status=0, message=None):
        """Override the normal exit behavior."""
        if message:
            raise Exception(message)


@api.route("/<string:command_name>")
@api.param("command_name", "bgutil sub-command to run.")
class BgUtilResource(rp.Resource):
    """BgUtil Resource.

    This could be implemented with one resource per command if we
    dynamically looked at commands, but it's simpler this way.
    """

    @api.doc("Run a bgutil command.")
    @api.expect(command)
    def post(self, command_name):
        """Starts a bgutil command in this thread."""
        result = None
        try:
            cmd, opts = parse_command(command_name, api.payload)
            with Capture() as capture:
                cmd.run(context.accessor, opts)
                result = capture.get_content()
        except UnknownCommandException as e:
            rp.abort(message=str(e))
        except Exception as e:
            logging.exception("bgutil failed")
            rp.abort(message=str(e))

        context.accessor.flush()

        # TODO:
        # - Allow asynchronous execution of commands.
        # To do that we might want to run new bgutil process and to add
        # a --bgutil_binary option to bgutil web (by default argv[0]). It would be
        # much easier to capture output and input this way.

        return result


@api.route("/async/<string:command_name>")
@api.param("command_name", "bgutil sub-command to run.")
class BgUtilAsyncResource(rp.Resource):
    """BgUtil asynchronous resource."""

    @api.doc("Run a bgutil command.")
    @api.expect(command)
    @api.response(201, "Created")
    def post(self, command_name):
        """Run asynchronously a BgUtil command."""
        # TODO: monitor background tasks and feed /workers with it
        try:
            cmd, opts = parse_command(command_name, api.payload)
            label = self._make_label(command_name)
            context.task_runner.submit(label, cmd, opts)
        except UnknownCommandException as e:
            rp.abort(message=str(e))
        except Exception as e:
            logging.exception("bgutil failed")
            rp.abort(message=str(e))

        context.accessor.flush()

        return "Running in background.", 201

    @staticmethod
    def _make_label(command_name):
        return "%s %s" % (command_name, " ".join(api.payload["arguments"]))


@api.route("/tasks/")
class BgUtilTasksResource(rp.Resource):
    """BgUtil list asynchronous resource."""

    @api.doc("List asynchronous bgutil tasks.")
    def get(self):
        """List asynchronous bgutil tasks."""
        return [self._format(task) for task in context.task_runner.tasks]

    @staticmethod
    def _format(task):
        return {
            "label": task.label,
            "submitted_on": BgUtilTasksResource._format_date(task.submitted_on),
            "started_on": BgUtilTasksResource._format_date(task.started_on),
            "completed_on": BgUtilTasksResource._format_date(task.completed_on),
            "status": task.status.value,
            "result": task.result,
        }

    @staticmethod
    def _format_date(date):
        return date.isoformat() if date else None
