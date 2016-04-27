#!/usr/bin/env python
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
        return [s for s in f.readlines() if s]


setuptools.setup(
    name="biggraphite",
    version="0.1",
    maintainer="Corentin Chary",
    maintainer_email="c.chary@criteo.com",
    description=("Tools for tools for storing carbon data in Cassandra."),
    license="MIT",
    keywords="graphite carbon cassandra biggraphite",
    url="https://github.com/criteo/biggraphite",
    include_package_data=True,
    py_modules=["biggraphite"],
    long_description=_read("README.md"),
    install_requires=_read_reqs("requirements.txt"),
    test_requires=_read_reqs("tests-requirements.txt"),
    test_suite="tests",
    entry_points={
        "console_scripts": [
        ]
    }
)
