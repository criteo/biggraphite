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
"""Capture I/O."""

import logging
import sys

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class Capture:
    """Capture outputs in usage scope."""

    def __init__(self):
        """Init capture scope."""
        self.closed = True
        self.content = None

    def __enter__(self):
        """Enter capture scope."""
        self.capture_io = StringIO()
        self.sys_stdout = sys.stdout
        self.sys_stderr = sys.stderr

        sys.stdout = self.capture_io
        sys.stderr = self.capture_io

        capture_logger = logging.getLogger()
        capture_logger.setLevel(logging.DEBUG)
        self.log_handler = logging.StreamHandler(self.capture_io)
        capture_logger.addHandler(self.log_handler)

        self.closed = False

        return self

    def __exit__(self, t, value, traceback):
        """End capture scope."""
        sys.stdout = self.sys_stdout
        sys.stderr = self.sys_stderr

        capture_logger = logging.getLogger()
        capture_logger.removeHandler(self.log_handler)

        self.log_handler.flush()
        self.capture_io.flush()

        self.content = self.get_content()

        self.capture_io.close()
        self.closed = True

    def get_content(self):
        """Get capture content as string."""
        if self.closed:
            return self.content
        else:
            return self.capture_io.getvalue()
