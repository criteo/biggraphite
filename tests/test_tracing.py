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

from opencensus.trace import tracer
from opencensus.trace import execution_context

from biggraphite import tracing


class TestTracingTrace(unittest.TestCase):
    """Test tracing integration of Biggraphite."""

    def test_decorator_correctly_wrap_and_trace_func(self):
        """Test that we wrap correctly the function and that a span is created."""
        test_tracer = tracer.Tracer()

        def test_traced_func(self):
            _tracer = execution_context.get_opencensus_tracer()
            _span = _tracer.current_span()
            return _span.name

        execution_context.set_opencensus_tracer(test_tracer)

        wrapped = tracing.trace(test_traced_func)

        mock_self = mock.Mock()
        mock_self.module_name = "module_name"

        span_name = wrapped(mock_self)

        expected_name = "module_name.test_traced_func"

        self.assertEqual(expected_name, span_name)


if __name__ == "__main__":
    unittest.main()
