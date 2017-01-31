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
import progressbar
import StringIO

from biggraphite.cli import command
from biggraphite.cli import command_clean, command_repair
from biggraphite import utils


def _init_logger(workers):
    """Init logger to be able to intercept message from each command."""
    class HandlerWrapper(logging.Handler):
        def emit(self, record):
            w = workers.get(record.threadName, None)
            if not w:
                return

            w["output"].append("{:<7} {:<25} :: {}".format(
                record.levelname, record.filename + ':' + str(record.lineno),
                record.getMessage()))

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
                logging.exception(e)

    return wrapper


def _run_webserver(workers, accessor, opts):
    def handle_health_request(request):
        path = request.path.strip("/")
        if path != "health":
            return False

        statusOk = all(w["thread"].is_alive() for w in workers.values())
        if statusOk:
            request.send_response(200)
            request.end_headers()
            request.wfile.write("OK")
        else:
            request.send_response(503)
            request.end_headers()
            request.wfile.write("FAILURE: some thread(s) are down\n")
            for worker in workers.values():
                request.wfile.write("%s is %s\n" % (worker["name"],
                                                    "OK" if worker["thread"].is_alive() else "KO"))

        return True

    def handle_worker_request(request):
        worker = workers.get(request.path.strip("/"), None)
        workers_to_display = [worker] if worker else workers.values()

        request.send_response(200)
        request.end_headers()
        for worker in workers_to_display:
            request.wfile.write(worker["name"] + " ::::::::::::::::::::::\n")
            if worker["status"]:
                request.wfile.write('\nStatus: ' + worker["status"] + '\n')
            request.wfile.write('\n'.join(worker["output"]))
            request.wfile.write('\n\n')

    http_handler = SimpleHTTPServer.SimpleHTTPRequestHandler
    http_handler.do_GET = lambda req: handle_health_request(req) or handle_worker_request(req)

    logging.info("Starting http server on %s" % opts.listen_on)
    SocketServer.TCPServer.allow_reuse_address = True
    http_server = SocketServer.TCPServer(("", opts.listen_on), http_handler)

    http_server.serve_forever()


class CommandDaemon(command.BaseCommand):
    """Check that you can use BigGraphite."""

    NAME = "daemon"
    HELP = "Run clean and repair at a given frequency"

    def add_arguments(self, parser):
        """Add custom arguments."""
        parser.add_argument(
            "--clean-frequency",
            help="How much time to wait between two clean.",
            default=24 * 60 * 60,
            type=int
        )
        parser.add_argument(
            "--repair-frequency",
            help="How much time to wait between two repair.",
            default=24 * 60 * 60,
            type=int
        )
        parser.add_argument(
            "--listen-on",
            help="Port on which the http server will listen",
            default=8080,
            type=int
        )

        # To avoid to fail to add the same option twice
        old_fn = parser.add_argument

        def add_arg(*args, **kwargs):
            try:
                old_fn(*args, **kwargs)
            except Exception:
                pass

        parser.add_argument = add_arg
        command_repair.CommandRepair.add_arguments(command_repair.CommandRepair(), parser)
        command_clean.CommandClean.add_arguments(command_clean.CommandClean(), parser)

    @_run_for_ever
    def _run_repair(self, worker, accessor, opts):
        logging.info("Repair started at %s" % time.strftime("%c"))

        fake_stderr = StringIO.StringIO()
        pbar = progressbar.ProgressBar(fd=fake_stderr)

        def on_progress(done, total):
            fake_stderr.seek(0)
            pbar.max_value = total
            pbar.update(done)
            worker["status"] = fake_stderr.getvalue()

        repair = command_repair.CommandRepair()
        repair.pbar = pbar
        repair.run(accessor, opts, on_progress=on_progress)

        logging.info("Repair finished at %s" % time.strftime("%c"))
        logging.info("Going to sleep for %d seconds " % opts.repair_frequency)
        time.sleep(opts.repair_frequency)

    @_run_for_ever
    def _run_clean(self, worker, accessor, opts):
        logging.info("Clean started at %s" % time.strftime("%c"))

        clean = command_clean.CommandClean()
        clean.run(accessor, opts)

        logging.info("Clean finished at %s" % time.strftime("%c"))
        logging.info("Going to sleep for %d seconds" % opts.clean_frequency)
        time.sleep(opts.clean_frequency)

    def run(self, accessor, opts):
        """Run clean and repair at a given frequency.

        See command.BaseCommand
        """
        workers = {
            "repair": {"name": "repair",
                       "fn": lambda x: self._run_repair(x, accessor, opts),
                       "output": collections.deque(maxlen=4096),
                       "status": "",
                       "thread": None},

            "clean": {"name": "clean",
                      "fn": lambda x: self._run_clean(x, accessor, opts),
                      "output": collections.deque(maxlen=4096),
                      "status": "",
                      "thread": None},
        }

        _init_logger(workers)
        accessor.connect()

        # start prometheus server
        utils.start_admin(utils.settings_from_args(opts))

        # Spawn workers
        for worker in workers.values():
            logging.info("starting %s worker" % worker["name"])
            th = threading.Thread(name=worker["name"], target=worker["fn"], args=(worker,))
            th.daemon = True
            th.start()
            worker["thread"] = th

        _run_webserver(workers, accessor, opts)
