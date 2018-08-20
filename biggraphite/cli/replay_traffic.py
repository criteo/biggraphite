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
"""Replays traffic from tcpdump to a Graphite cluster."""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import datetime
import logging
import multiprocessing
import re
import sys
import time
from multiprocessing import dummy as multiprocessing_dummy

import dpkt
import progressbar
from six.moves.urllib import parse

from biggraphite.cli import clusters_diff


class Error(Exception):
    """Error."""


def handle_packet(timestamp, packet):
    """Parse one packet and return the associate HTTP request if any."""
    # Unpack the Ethernet frame (mac src/dst, ethertype)
    eth = dpkt.ethernet.Ethernet(packet)
    try:
        eth = dpkt.ethernet.Ethernet(packet)
    except (dpkt.dpkt.NeedData, dpkt.dpkt.UnpackError):
        return None

    # Make sure the Ethernet data contains an IP packet
    if not isinstance(eth.data, dpkt.ip.IP):
        logging.info(
            "Non IP Packet type not supported %s\n" % eth.data.__class__.__name__
        )
        return None

    # Now grab the data within the Ethernet frame (the IP packet)
    ip = eth.data

    # Check for TCP in the transport layer
    if not isinstance(ip.data, dpkt.tcp.TCP):
        return

    # Set the TCP data
    tcp = ip.data

    # Now see if we can parse the contents as a HTTP request
    try:
        request = dpkt.http.Request(tcp.data)
    except (dpkt.dpkt.NeedData, dpkt.dpkt.UnpackError):
        return None

    return (datetime.datetime.utcfromtimestamp(timestamp), request)


def get_requests(filename):
    """Get all Http requests from a pcap file."""
    f = open(filename, "rb")
    pcap = dpkt.pcap.Reader(f)

    for timestamp, packet in pcap:
        res = handle_packet(timestamp, packet)
        if res is not None:
            yield res


class _Worker(object):
    """A multiprocesed worker doing queries."""

    def __init__(self, opts):
        self._opts = opts
        self._exclude = self._opts.exclude
        self._last_request = 0
        self._last_played_timestamp = 0

    def _sleep(self, timestamp):
        if self._last_played_timestamp and self._opts.time_factor:
            delta = (timestamp - self._last_played_timestamp).total_seconds()
            delta -= time.time() - self._last_request
            delta /= self._opts.time_factor
            if delta > 0:
                time.sleep(delta)
        self._last_played_timestamp = timestamp
        self._last_request = time.time()

    def do_request(self, orig_request):
        timestamp, orig_request = orig_request
        self._sleep(timestamp)
        url = "http://%s%s" % (self._opts.host, orig_request.uri)
        auth_key = ""
        timeout_s = self._opts.timeout
        if orig_request.body:
            data = orig_request.body
        else:
            data = None
        if self._exclude:
            if self._exclude.match(url):
                return
            if self._exclude.match(str(data)):
                return

        if data:
            query = data.decode()
        else:
            query = parse.urlparse(url).query
        query = parse.parse_qs(query)

        logging.debug("%s: %s (%s)", url, data, query["target"])

        request = clusters_diff.Request(url, auth_key, timeout_s, data)
        request.execute()


def _setup_process(opts):
    global _WORKER
    _WORKER = _Worker(opts)


def _do_request(*args, **kwargs):
    assert _WORKER is not None, "_setup_process was never called"
    try:
        return _WORKER.do_request(*args, **kwargs)
    except Exception:
        logging.exception("%s" % (args))
        return 0


def _parse_opts(args):
    parser = argparse.ArgumentParser(
        description="Replay HTTP Traffic",
        epilog=(
            "Capture traffic with:"
            "sudo tcpdump -i any dst port 80 -s 0 -w graphite.pcap"
        ),
    )
    parser.add_argument(
        "pcap_files",
        metavar="PCAP_FILE",
        nargs="*",
        help="pcap files to read packets from.",
    )
    parser.add_argument(
        "--process",
        metavar="N",
        type=int,
        help="number of concurrent process",
        default=multiprocessing.cpu_count(),
    )
    parser.add_argument(
        "--time_factor",
        type=int,
        help="Speed up time by this factor. 0 to disable.",
        default=1,
    )
    parser.add_argument(
        "--host",
        metavar="host",
        type=str,
        required=True,
        help="Host to execute queries on.",
    )
    parser.add_argument(
        "--exclude",
        metavar="exclude",
        type=str,
        required=False,
        default="",
        help="Exclude queries matching.",
    )
    parser.add_argument("--timeout", type=int, help="Request timeout", default=60)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--progress", action="store_true", default=False)

    opts = parser.parse_args(args)
    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    return opts


def read_requests(opts):
    """Read all requests."""
    if opts.progress:
        # If we want progress, we read everything.
        requests = []
        for filename in opts.pcap_files:
            requests.extend(get_requests(filename))
        requests = sorted(requests, key=lambda k: k[0])
    else:
        # Else we can be more efficient.
        for filename in opts.pcap_files:
            for request in get_requests(filename):
                yield request


def main():
    """Main function."""
    opts = _parse_opts(sys.argv[1:])
    if opts.exclude:
        opts.exclude = re.compile(opts.exclude)

    requests = read_requests(opts)
    pool_factory = multiprocessing.Pool
    if opts.process == 1:
        pool_factory = multiprocessing_dummy.Pool
    pool = pool_factory(opts.process, initializer=_setup_process, initargs=(opts,))

    if opts.progress:
        pbar = progressbar.ProgressBar(max_value=len(requests))
    else:
        pbar = None

    for i, _ in enumerate(pool.imap_unordered(_do_request, requests)):
        if pbar:
            pbar.update(i)

    pool.close()
    pool.join()

    print("Did %s queries" % i)


if __name__ == "__main__":
    main()
