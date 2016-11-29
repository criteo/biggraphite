import logging
import threading
import time
import SimpleHTTPServer
import SocketServer
import collections


from biggraphite.cli import command
from biggraphite.cli import command_clean, command_repair
from biggraphite import metadata_cache



class CommandDaemon(command.BaseCommand):
    """Check that you can use BigGraphite."""

    NAME = "daemon"
    HELP = "Run clean and repair at a given frequency"
    _logger_path = "graphite.log"


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

    def run_for_ever(fn):
        def wrapper(*args, **kwargs):
            while True:
                try:
                    fn(*args, **kwargs)
                except Exception as e:
                    logging.error(e)

        return wrapper

    @run_for_ever
    def __run_repair(self, state, accessor, opts):
        logging.info("Repair started at %s" % time.strftime("%c"))

        repair = command_repair.CommandRepair()
        repair.run(accessor, opts)

        logging.info("Repair finished at %s" % time.strftime("%c"))
        logging.info("Going to sleep for %d seconds " % opts.repair_frequency)
        time.sleep(opts.repair_frequency)

    @run_for_ever
    def __run_clean(self, state, accessor, opts):
        logging.info("Clean started at %s" % time.strftime("%c"))

        clean = command_clean.CommandClean()
        clean.run(accessor, opts)

        logging.info("Clean finished at %s" % time.strftime("%c"))
        logging.info("Going to sleep for %d seconds" % opts.clean_frequency)
        time.sleep(opts.clean_frequency)

    def __run_webserver(self, workers, accessor, opts):
        def get_handler(request):
            worker = workers.get(request.path.strip("/"), None)
            workers_to_display = workers.values() if worker == None else [worker]

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

    def __logger_init(self, workers):

        class HandlerWrapper(logging.Handler):
            def emit(self, record):
                w = workers.get(record.threadName, None)
                if w == None:
                    return

                w["state"].append(record.getMessage())


        class LoggerWrapper(logging.Logger):
            def __init__(this, name):
                super(LoggerWrapper, this).__init__(name)
                this.addHandler(HandlerWrapper())
                this.propagate = True
                this.setLevel(logging.INFO)


        logging.setLoggerClass(LoggerWrapper)
        logging.getLogger().propagate = True
        logging.getLogger().addHandler(HandlerWrapper())

    def run(self, accessor, opts):
        """Run clean and repair at a given frequency.

        See command.BaseCommand
        """


        workers = {
            "repair": { "name": "repair",
                        "fn": lambda x: self.__run_repair(x, accessor, opts),
                        "state": collections.deque(maxlen=4096),
                        "thread": None
            },
            "clean" : { "name": "clean",
                        "fn": lambda x: self.__run_clean(x, accessor, opts),
                        "state": collections.deque(maxlen=4096),
                        "thread": None
            },
        }

        self.__logger_init(workers)
        accessor.connect()

        # Spawn workers
        for worker in workers.values():
            logging.info("starting %s worker" % worker["name"])
            th = threading.Thread(name=worker["name"], target=worker["fn"], args=(worker["state"],))
            th.start()
            worker["thread"] = th

        self.__run_webserver(workers, accessor, opts)
