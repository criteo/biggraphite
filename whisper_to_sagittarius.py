#!/usr/bin/env pypy
from __future__ import print_function

import argparse
import multiprocessing
import os
import struct
import sys
import time

import progressbar
import whisper
import cassandra
from cassandra import cluster as c_cluster
from cassandra import concurrent as c_concurrent


_POINT_STRUCT = struct.Struct(whisper.pointFormat)
_WORKER = None


def read_points(metric_name, path):
    """Returns a list of (metric_name, time_start, time_offset, value)"""
    info = whisper.info(path)
    res = []
    if not info:
        return []

    archives = info['archives']
    with open(path) as f:
        buf = f.read()
    for archive in archives:
        offset = archive['offset']
        step =  archive['secondsPerPoint']
        expected_next_timestamp = 0

        for point in range(archive['points']):
            timestamp, val = _POINT_STRUCT.unpack_from(buf, offset)
            # Detect holes in data. The heuristic is the following:
            # - If a value is non-zero, it is assumed to be meaningful.
            # - If it is a zero with a fresh timestamp relative to the last
            #   time we saw meaningful data, it is assumed to be meaningful.
            # So it unfortunatelly skips leading zeroes after a gap.
            if timestamp != expected_next_timestamp and val == 0:
                expected_next_timestamp += step
                continue
            else:
                expected_next_timestamp = timestamp + step
            time_offset = timestamp % 3600
            time_start = timestamp - time_offset
            res.append((metric_name, time_start, time_offset, val))
            offset += whisper.pointSize
    return res


class _Worker(object):

    def __init__(self, opts):
        self.opts = opts
        self.cluster = None

    def connect(self):
        if self.cluster:
            return

        self.cluster = c_cluster.Cluster(
            self.opts.contact_points, executor_threads=self.opts.connections_per_process)
        self.session = self.cluster.connect()
        self.session.set_keyspace('v0')
        self.statement = self.session.prepare('INSERT INTO raw (metric, time_start, time_offset, value) VALUES (?, ?, ?, ?);')
        self.statement.consistency_level = cassandra.ConsistencyLevel.ONE

    def import_whisper(self, path):
        self.connect()
        metric_name = path[len(self.opts.root_directory)+1:-4].replace('/', '.')
        points = read_points(metric_name, path)
        c_concurrent.execute_concurrent_with_args(
            self.session, self.statement, points, concurrency=self.opts.statements_per_connection)
        return len(points)


def _setup_process(opts):
    global _WORKER
    _WORKER = _Worker(opts)


def _import_whisper(*args, **kwargs):
    assert _WORKER is not None, '_setup_process was never called'
    return _WORKER.import_whisper(*args, **kwargs)


def _parse_opts():
    parser = argparse.ArgumentParser(description='Import whisper files into Sagittarius.')
    parser.add_argument('root_directory', metavar='WHISPER_DIR', help='directory in which to find whisper files')
    parser.add_argument('contact_points', metavar='HOST', nargs='+', help='hosts used for discovery')
    parser.add_argument('--statements_per_connection', metavar='N', type=int,
                        help='number of concurrent statements per connection',
                        default=50)
    parser.add_argument('--connections_per_process', metavar='N', type=int,
                        help='number of connections per Cassandra host per process',
                        default=4)
    parser.add_argument('--process', metavar='N', type=int, help='number of concurrent process',
                        default=multiprocessing.cpu_count())
    return parser.parse_args()


def main():

    if '__pypy__' not in sys.builtin_module_names:
        print('Running without PyPy, this is about 20 times slower', file=sys.stderr)

    opts = _parse_opts()
    paths = [os.path.join(root, f)
             for root, _, files in os.walk(opts.root_directory)
             for f in files if f.endswith('wsp')]

    pool = multiprocessing.Pool(opts.process, initializer=_setup_process, initargs=(opts,))

    total_points = 0
    with progressbar.ProgressBar(max_value=len(paths), redirect_stderr=True) as pbar:
        for n_path, n_points in enumerate(pool.imap_unordered(_import_whisper, paths)):
            total_points += n_points
            pbar.update(n_path)

    pool.close()
    pool.join()

    print('Uploaded', len(paths), 'metrics containing', total_points, 'points')

if __name__ == "__main__":
    main()
