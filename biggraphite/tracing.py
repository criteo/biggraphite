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
"""Function used for the tracing of Biggraphite."""

try:
    from opencensus.trace import execution_context
except ImportError:
    execution_context = None


def trace(func):
    """Decorator for tracing of functions."""
    if not execution_context:
        return func

    def tracer(self, *args, **kwargs):
        if not hasattr(self, 'module_name'):
            self.module_name = func.__module__.split('.')[-1]
        _tracer = execution_context.get_opencensus_tracer()
        with _tracer.span(name="%s.%s" % (self.module_name, func.__name__)):
            return func(self, *args, **kwargs)
    return tracer
