#!/usr/bin/env python
# Copyright 2016 Criteo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use self file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Daemon Command."""


import logging
import threading
import time
import SimpleHTTPServer
import SocketServer
import collections

from biggraphite.cli import command
from biggraphite.cli import command_clean, command_repair


def _init_logger(workers):
    """Init logger to be able to intercept message from each command."""
    class HandlerWrapper(logging.Handler):
        def emit(self, record):
            w = workers.get(record.threadName, None)
            if not w:
                return

            w["state"].append(record.getMessage())

    class LoggerWrapper(logging.Logger):
        def __init__(self, name):
            super(LoggerWrapper, self).__init__(name)
            self.addHandler(HandlerWrapper())
            self.propagate = True

    logging.setLoggerClass(LoggerWrapper)
    logging.getLogger().propagate = True
    logging.getLogger().addHandler(HandlerWrapper())


def _run_for_ever(fn):
    """Forever call in loop a function."""
    def wrapper(*args, **kwargs):
        while True:
            try:
                fn(*args, **kwargs)
            except Exception as e:
                logging.error(e)

    return wrapper


class CommandDaemon(command.BaseCommand):
    """Check that you can use BigGraphite."""

    NAME = "daemon"
    HELP = "Run clean and repair at a given frequency"

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "--clean-frequency",
            help="Start key.",
            default=24 * 60 * 60,
            type=int
        )
        parser.add_argument(
            "--repair-frequency",
            help="End key.",
            default=24 * 60 * 60,
            type=int
        )
        parser.add_argument(
            "--listen-on",
            help="Port on which the http server will listen",
            default=8080
        )
        command_repair.CommandRepair.add_arguments(command_repair.CommandRepair(), parser)
        command_clean.CommandClean.add_arguments(command_clean.CommandClean(), parser)

    @_run_for_ever
    def _run_repair(self, state, accessor, opts):
        logging.info("Repair started at %s" % time.strftime("%c"))

        repair = command_repair.CommandRepair()
        repair.run(accessor, opts)

        logging.info("Repair finished at %s" % time.strftime("%c"))
        logging.info("Going to sleep for %d seconds " % opts.repair_frequency)
        time.sleep(opts.repair_frequency)

    @_run_for_ever
    def _run_clean(self, state, accessor, opts):
        logging.info("Clean started at %s" % time.strftime("%c"))

        clean = command_clean.CommandClean()
        clean.run(accessor, opts)

        logging.info("Clean finished at %s" % time.strftime("%c"))
        logging.info("Going to sleep for %d seconds" % opts.clean_frequency)
        time.sleep(opts.clean_frequency)

    def _run_webserver(self, workers, accessor, opts):
        def get_handler(request):
            worker = workers.get(request.path.strip("/"), None)
            workers_to_display = [worker] if worker else workers.values()

            request.send_response(200)
            request.end_headers()
            for worker in workers_to_display:
                request.wfile.write(worker["name"] + " ::::::::::::::::::::::\n")
                request.wfile.write('\n'.join(worker["state"]))
                request.wfile.write('\n\n')

        http_handler = SimpleHTTPServer.SimpleHTTPRequestHandler
        http_handler.do_GET = get_handler

        logging.info("Starting http server on %s" % opts.listen_on)
        SocketServer.TCPServer.allow_reuse_address = True
        http_server = SocketServer.TCPServer(("", opts.listen_on), http_handler)

        http_server.serve_forever()

    def run(self, accessor, opts):
        """Run clean and repair at a given frequency.

        See command.BaseCommand
        """
        workers = {
            "repair": {"name": "repair",
                       "fn": lambda x: self._run_repair(x, accessor, opts),
                       "state": collections.deque(maxlen=4096),
                       "thread": None},

            "clean": {"name": "clean",
                      "fn": lambda x: self._run_clean(x, accessor, opts),
                      "state": collections.deque(maxlen=4096),
                      "thread": None},
        }

        _init_logger(workers)
        accessor.connect()

        # Spawn workers
        for worker in workers.values():
            logging.info("starting %s worker" % worker["name"])
            th = threading.Thread(name=worker["name"], target=worker["fn"], args=(worker["state"],))
            th.start()
            worker["thread"] = th

        self._run_webserver(workers, accessor, opts)
