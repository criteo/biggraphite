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

import sys

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


class Capture:

    def __init__(self):
        self.closed = True
        self.content = None

    def __enter__(self):
        self.capture_io = StringIO()
        self.sys_stdout = sys.stdout
        self.sys_stderr = sys.stderr

        sys.stdout = self.capture_io
        sys.stderr = self.capture_io

        self.closed = False

        return self

    def __exit__(self, t, value, traceback):
        sys.stdout = self.sys_stdout
        sys.stderr = self.sys_stderr
        self.content = self.get_content()
        self.capture_io.close()
        self.closed = True

    def get_content(self):
        if self.closed:
            return self.content
        else:
            return self.capture_io.getvalue()
