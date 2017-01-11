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
"""A CLI to import whisper data into Cassandra."""

from __future__ import print_function

import argparse
import multiprocessing
from multiprocessing import dummy as multiprocessing_dummy
import os
import struct
import sys
import logging

import progressbar
import whisper

from biggraphite import accessor as bg_accessor
from biggraphite import utils as bg_utils

_DEV_NULL = open(os.devnull, "w")


_POINT_STRUCT = struct.Struct(whisper.pointFormat)
_WORKER = None


log = logging.getLogger(__name__)


def metric_name_from_wsp(root_dir, wsp_path):
    """Return the name of a metric given a wsp file path and a root directory.

    The path do not have to exist.

    Args:
      root_dir: A directory that is parent to all metrics.
      wsp_path: The name of a file ending with wsp file in root_dir.

    Returns:
      The metric name.
    """
    relpath = os.path.relpath(wsp_path, root_dir)
    assert ".." not in relpath, "%s not a child of %" % (root_dir, wsp_path)
    relpath_noext = os.path.splitext(relpath)[0]
    return relpath_noext.replace(os.path.sep, ".")


class _Worker(object):

    def __init__(self, opts):
        settings = bg_utils.settings_from_args(opts)
        bg_utils.set_log_level(settings)
        self._accessor = bg_utils.accessor_from_settings(settings)
        self._opts = opts

    @staticmethod
    def _read_metadata(metric_name, path):
        info = whisper.info(path)
        if not info:
            return None

        retentions = bg_accessor.Retention([
            bg_accessor.Stage(precision=a["secondsPerPoint"], points=a["points"])
            for a in info["archives"]
        ])
        aggregator = bg_accessor.Aggregator.from_carbon_name(
            info["aggregationMethod"])
        return bg_accessor.MetricMetadata(
            aggregator=aggregator,
            retention=retentions,
            carbon_xfilesfactor=info["xFilesFactor"],
        )

    def _read_points(self, path):
        """Return a list of (timestamp, value)."""
        info = whisper.info(path)
        res = []
        if not info:
            return []

        archives = info["archives"]
        with open(path) as f:
            buf = f.read()

        # Two or more archives can contain a given timestamp.
        # As archives are from most precise to least precise, we track the oldest
        # point we've found in more precise archives and ignore the newer ones.
        prev_archive_starts_at = float("inf")
        for archive in archives:
            offset = archive["offset"]
            step = archive["secondsPerPoint"]
            archive_starts_at = float("inf")
            expected_next_timestamp = 0
            stage = bg_accessor.Stage(
                precision=archive["secondsPerPoint"],
                points=archive["points"],
                stage0=prev_archive_starts_at == float("inf"))
            if stage in self._opts.ignored_stages:
                continue
            for _ in range(archive["points"]):
                timestamp, value = _POINT_STRUCT.unpack_from(buf, offset)
                # Detect holes in data. The heuristic is the following:
                # - If a value is non-zero, it is assumed to be meaningful.
                # - If it is a zero with a fresh timestamp relative to the last
                #   time we saw meaningful data, it is assumed to be meaningful.
                # So it unfortunately skips leading zeroes after a gap.
                if timestamp != expected_next_timestamp and value == 0:
                    expected_next_timestamp += step
                    continue
                else:
                    expected_next_timestamp = timestamp + step
                archive_starts_at = min(timestamp, archive_starts_at)

                if timestamp < prev_archive_starts_at:
                    res.append((timestamp, value, 1, stage))
                offset += whisper.pointSize

            prev_archive_starts_at = archive_starts_at

        return res

    def import_whisper(self, path):
        if not self._accessor.is_connected:
            self._accessor.connect()

        name = metric_name_from_wsp(self._opts.root_directory, path)
        metadata = self._read_metadata(name, path)
        if not metadata:
            return 0

        metric = self._accessor.make_metric(name, metadata)
        self._accessor.create_metric(metric)

        if self._opts.no_data:
            return 0

        points = self._read_points(path)
        self._accessor.insert_downsampled_points(metric, points)
        return len(points)


def _setup_process(opts):
    global _WORKER
    _WORKER = _Worker(opts)


def _import_whisper(*args, **kwargs):
    assert _WORKER is not None, "_setup_process was never called"
    try:
        return _WORKER.import_whisper(*args, **kwargs)
    except Exception as e:
        log.exception(e)
        return 0


def _parse_opts(args):
    parser = argparse.ArgumentParser(
        description="Import whisper files into BigGraphite.")
    parser.add_argument("root_directory", metavar="WHISPER_DIR",
                        help="directory in which to find whisper files")
    parser.add_argument("--quiet", action="store_const", default=False, const=True,
                        help="Show no output unless there are problems.")
    parser.add_argument("--process", metavar="N", type=int,
                        help="number of concurrent process",
                        default=multiprocessing.cpu_count())
    parser.add_argument("--no-data", action="store_true",
                        help="Do not import data, only metadata.")
    parser.add_argument("--ignored_stages", nargs="*",
                        help="Do not import data for these stages.",
                        default=[])
    bg_utils.add_argparse_arguments(parser)
    opts = parser.parse_args(args)
    opts.ignored_stages = [
        bg_accessor.Stage.from_string(s)
        for s in opts.ignored_stages
    ]
    return opts


def main(args=None):
    """Entry point for the module."""
    if not args:
        args = sys.argv[1:]

    opts = _parse_opts(args)
    paths = [os.path.join(root, f)
             for root, _, files in os.walk(opts.root_directory)
             for f in files if f.endswith("wsp")]

    pool_factory = multiprocessing.Pool
    if opts.process == 1:
        pool_factory = multiprocessing_dummy.Pool
    pool = pool_factory(opts.process, initializer=_setup_process, initargs=(opts,))

    out_fd = sys.stderr
    if opts.quiet:
        out_fd = _DEV_NULL
    if "__pypy__" not in sys.builtin_module_names:
        print("Running without PyPy, this is about 20 times slower", file=out_fd)
        out_fd.flush()

    total_points = 0
    with progressbar.ProgressBar(max_value=len(paths), fd=out_fd, redirect_stderr=True) as pbar:
        try:
            res = pool.imap_unordered(_import_whisper, paths)
            for n_path, n_points in enumerate(res):
                total_points += n_points
                pbar.update(n_path)
        except KeyboardInterrupt:
            pool.terminate()

    pool.close()
    pool.join()

    print("Uploaded", len(paths), "metrics containing", total_points, "points", file=out_fd)


if __name__ == "__main__":
    main()
