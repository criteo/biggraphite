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
"""Web Application for BigGraphite."""

import logging

import flask
import flask_restplus
import gourde
import prometheus_client

from biggraphite.cli.web import context
from biggraphite.cli.web.namespaces import bgutil as ns_bgutil
from biggraphite.cli.web.namespaces import biggraphite as ns_biggraphite


class Error(Exception):
    """All local errors."""

    pass


class WebApp(object):
    """A Web UI and API for BigGraphite."""

    def __init__(self, registry=None):
        """Constructor."""
        registry = registry or prometheus_client.REGISTRY
        # Here registry is explicit to allow us to mess with it in the tests.
        self.gourde = gourde.Gourde(__name__, registry=registry)
        self.app = self.gourde.app
        self.accessor = None
        self.args = None
        self.bgutil_workers = {}

    def index(self):
        """Main page."""
        return flask.render_template(
            "index.html", accessor=self.accessor, args=vars(self.args)
        )

    def workers(self):
        """Display background operations."""
        return flask.render_template("workers.html", workers=self.bgutil_workers)

    def is_healthy(self):
        """Custom "health" check."""
        return all(w["thread"].is_alive() for w in self.bgutil_workers.values())

    def initialize_api(self):
        """Initialize an API."""
        blueprint = flask.Blueprint("api", __name__, url_prefix="/api")

        api = flask_restplus.Api(version="1.0", title="BigGraphite API")
        api.namespaces = []
        api.add_namespace(ns_bgutil.api)
        api.add_namespace(ns_biggraphite.api)
        api.init_app(blueprint)

        self.app.register_blueprint(blueprint)

    def initialize_app(self, accessor, args):
        """Initialize the App."""
        # Hack to access accessor and app in resources.
        context.accessor = accessor
        context.app = self

        self.accessor = accessor
        self.args = args
        self.gourde.add_url_rule("/", "index", self.index)
        self.gourde.add_url_rule("/workers", "workers", self.workers)
        self.gourde.setup(args)
        self.gourde.is_healthy = self.is_healthy

        self.initialize_api()

    def _init_logger(self):
        """Init logger to be able to intercept message from each command."""
        class HandlerWrapper(logging.Handler):
            def emit(self, record):
                w = self.bgutil_workers.get(record.threadName, None)
                if not w:
                    return

                w["output"].append(
                    "{:<7} {:<25} :: {}".format(
                        record.levelname,
                        record.filename + ":" + str(record.lineno),
                        record.getMessage(),
                    )
                )

        class LoggerWrapper(logging.Logger):
            def __init__(self, name):
                super(LoggerWrapper, self).__init__(name)
                self.addHandler(HandlerWrapper())
                self.propagate = True

        logging.setLoggerClass(LoggerWrapper)
        logging.getLogger().propagate = True
        logging.getLogger().addHandler(HandlerWrapper())

    def run(self):
        """Run the application."""
        self.gourde.run()
