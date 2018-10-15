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


import unittest
import mock

from opencensus.trace import execution_context

from biggraphite.drivers import tracing


class TestTracingTrace(unittest.TestCase):
    def testDecoratorTrace(self):
        """Test that we wrap correctly the function and that a span
        is created."""
        mock_span = mock.Mock()
        mock_span.span_id = '1234'
        mock_tracer = MockTracer(mock_span)

        mock_accessor_func = mock.Mock()
        mock_accessor_func.__name__ = 'func_name'
        mock_accessor_func.__module__ = 'module_path.module_name'

        execution_context.set_opencensus_tracer(mock_tracer)

        wrapped = tracing.trace(mock_accessor_func)

        mock_self = mock.Mock()
        wrapped(mock_self)

        expected_name = 'module_name.func_name'

        self.assertEqual(expected_name, mock_tracer.span.name)


class MockTracer(object):
    def __init__(self, span=None):
        self.span = span

    def current_span(self):
        return self.span

    def start_span(self):
        span = mock.Mock()
        span.attributes = {}
        span.context_tracer = mock.Mock()
        span.context_tracer.span_context = mock.Mock()
        span.context_tracer.span_context.trace_id = '123'
        span.context_tracer.span_context.span_id = '456'
        self.span = span
        return span
