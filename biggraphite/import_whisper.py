#!/usr/bin/env python
from __future__ import print_function

import argparse
import multiprocessing
import os
import struct
import sys

import progressbar
import whisper

from biggraphite import accessor as bg_accessor


_POINT_STRUCT = struct.Struct(whisper.pointFormat)
_WORKER = None


class _Worker(object):

    def __init__(self, opts):
        self._accessor = bg_accessor.Accessor(
            keyspace=opts.keyspace,
            contact_points=opts.contact_points,
            port=opts.port,
            connections_per_process=opts.connections_per_process,
        )
        self._opts = opts

    @staticmethod
    def _read_points(path):
        """Return a list of (timestamp, value)."""
        info = whisper.info(path)
        res = []
        if not info:
            return []

        archives = info['archives']
        with open(path) as f:
            buf = f.read()
        for archive in archives:
            offset = archive['offset']
            step = archive['secondsPerPoint']
            expected_next_timestamp = 0

            for _ in range(archive['points']):
                timestamp, val = _POINT_STRUCT.unpack_from(buf, offset)
                # Detect holes in data. The heuristic is the following:
                # - If a value is non-zero, it is assumed to be meaningful.
                # - If it is a zero with a fresh timestamp relative to the last
                #   time we saw meaningful data, it is assumed to be meaningful.
                # So it unfortunately skips leading zeroes after a gap.
                if timestamp != expected_next_timestamp and val == 0:
                    expected_next_timestamp += step
                    continue
                else:
                    expected_next_timestamp = timestamp + step
                res.append((timestamp, val))
                offset += whisper.pointSize
        return res

    def import_whisper(self, path):
        if not self._accessor.is_connected:
            self._accessor.connect()
        metric_name = path[len(self._opts.root_directory) + 1:-4].replace('/', '.')
        points = self._read_points(path)
        self._accessor.insert_points(metric_name, points)
        return len(points)


def _setup_process(opts):
    global _WORKER
    _WORKER = _Worker(opts)


def _import_whisper(*args, **kwargs):
    assert _WORKER is not None, '_setup_process was never called'
    return _WORKER.import_whisper(*args, **kwargs)


def _parse_opts():
    parser = argparse.ArgumentParser(description='Import whisper files into BigGraphite.')
    parser.add_argument('root_directory', metavar='WHISPER_DIR',
                        help='directory in which to find whisper files')
    parser.add_argument('contact_points', metavar='HOST', nargs='+',
                        help='hosts used for discovery')
    parser.add_argument('--keyspace', metavar='NAME',
                        help="Cassandra keyspace", default="biggraphite")
    parser.add_argument('--statements_per_connection', metavar='N', type=int,
                        help='number of concurrent statements per connection',
                        default=50)
    parser.add_argument('--port', metavar='PORT', type=int,
                        help='the native port to connect to', default=9042)
    parser.add_argument('--connections_per_process', metavar='N', type=int,
                        help='number of connections per Cassandra host per process', default=4)
    parser.add_argument('--process', metavar='N', type=int,
                        help='number of concurrent process', default=multiprocessing.cpu_count())
    return parser.parse_args()


def main():

    if '__pypy__' not in sys.builtin_module_names:
        print('Running without PyPy, this is about 20 times slower', file=sys.stderr)
        sys.stderr.flush()

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
