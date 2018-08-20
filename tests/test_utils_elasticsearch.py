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
"""Elasticsearch utilities factoring code across tests.

The dependencies for this module are not included in requirements.txt or in the package
dependencies, instead one needs the elements of tests-requirements.txt .
"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import os
import sys

HAS_ES_HOME = bool(os.getenv("ES_HOME"))
ES_HOSTPORT = os.getenv("ES_HOSTPORT")
HAS_ELASTICSEARCH = HAS_ES_HOME or ES_HOSTPORT

# Only try to import elasticsearch if we are going to use it. This is better
# than using try/except because the failure case is easier to handle.
if HAS_ELASTICSEARCH:
    from testing import elasticsearch as testing_elasticsearch


class ElasticsearchHelper:
    """Helper for an ephemeral Elasticsearch cluster."""

    INDEX = "testindex"

    @classmethod
    def get_accessor_settings(cls):
        """Prepare accessor settings for Elasticsearch driver."""
        return {
            "elasticsearch_index": cls.INDEX,
            "elasticsearch_hosts": cls.hosts,
            "elasticsearch_port": cls.port,
            "elasticsearch_timeout": 60,
        }

    @classmethod
    def setUpClass(cls):
        """Create the test Elasticsearch Cluster as cls.elasticsearch."""
        cls.elasticsearch = None
        if ES_HOSTPORT:
            # Use existing and running instance.
            host, cls.port = ES_HOSTPORT.split(":")
            cls.hosts = [host]
        else:
            # Setup a new instance, and dynamically get its host and port.
            cls.setUpElasticsearch()

    @classmethod
    def setUpElasticsearch(cls):
        """Start Elasticsearch."""
        cls.elasticsearch = testing_elasticsearch.Elasticsearch(auto_start=False)
        try:
            cls.elasticsearch.setup()
            cls.elasticsearch.start()
        except Exception as e:
            logging.exception(e)
            print(
                "fail to starting elasticsearch, logging potentially useful debug info",
                file=sys.stderr,
            )
            for attr in [
                "elasticsearch_home",
                "elasticsearch_yaml",
                "elasticsearch_major_version",
                "base_dir",
                "settings",
            ]:
                print(
                    attr,
                    ":",
                    getattr(cls.elasticsearch, attr, "Unknown"),
                    file=sys.stderr,
                )
            cls.elasticsearch.cleanup()
            raise

        # testing.elasticsearch is meant to be used with the Thrift API, so we need to
        # extract the IPs and native port for use with the native driver.
        cls.hosts = [s.split(":")[0] for s in cls.elasticsearch.dsn()["hosts"]]
        cls.port = cls.elasticsearch.elasticsearch_yaml["http.port"]

    @classmethod
    def tearDownClass(cls):
        """Stop the test Elasticsearch Cluster."""
        if cls.elasticsearch:
            cls.elasticsearch.stop()
            cls.elasticsearch = None
