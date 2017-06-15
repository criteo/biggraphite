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

"""setuptools integration for BigGraphite."""
import os
import setuptools


def _read(relpath):
    fullpath = os.path.join(os.path.dirname(__file__), relpath)
    with open(fullpath) as f:
        return f.read()


def _read_reqs(relpath):
    fullpath = os.path.join(os.path.dirname(__file__), relpath)
    with open(fullpath) as f:
        return [s.strip() for s in f.readlines()
                if (s.strip() and not s.startswith("#"))]


_REQUIREMENTS_TXT = _read_reqs("requirements.txt")
_DEPENDENCY_LINKS = [l for l in _REQUIREMENTS_TXT if "://" in l]
_INSTALL_REQUIRES = [l for l in _REQUIREMENTS_TXT if "://" not in l]


setuptools.setup(
    name="biggraphite",
    version="0.8.6",
    maintainer="Criteo Graphite Team",
    maintainer_email="github@criteo.com",
    description="Simple Scalable Time Series Database.",
    license="Apache Software License",
    keywords="graphite carbon cassandra biggraphite tsdb timeseries",
    url="https://github.com/criteo/biggraphite",
    include_package_data=True,
    packages=["biggraphite", "biggraphite.plugins", "biggraphite.drivers", "biggraphite.cli"],
    long_description=_read("README.md"),
    install_requires=_INSTALL_REQUIRES,
    dependency_links=_DEPENDENCY_LINKS,
    test_requires=_read_reqs("tests-requirements.txt"),
    test_suite="tests",
    entry_points={
        "console_scripts": [
            'bg-carbon-cache = biggraphite.cli.bg_carbon_cache:main',
            'bg-carbon-aggregator-cache = biggraphite.cli.bg_carbon_aggregator_cache:main',
            'bg-import-whisper = biggraphite.cli.import_whisper:main',
            'bg-clusters-diff = biggraphite.cli.clusters_diff:main',
            'bg-replay-traffic = biggraphite.cli.replay_traffic:main',
            'bgutil = biggraphite.cli.bgutil:main',
        ]
    },
)
