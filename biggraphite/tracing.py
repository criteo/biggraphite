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
    from opencensus.trace.samplers import always_off
    from opencensus.trace.tracers import noop_tracer
    from opencensus.trace import trace_options
    from opencensus.trace.span_context import SpanContext
except ImportError:
    execution_context = None


def get_bg_trace(self, func):
    """Prepare the module_name and return a tracer."""
    if not hasattr(self, 'module_name'):
        self.module_name = func.__module__.split('.')[-1]
    return execution_context.get_opencensus_tracer()


def trace(func):
    """Decorator for tracing of functions."""
    if not execution_context:
        return func

    def tracer(self, *args, **kwargs):
        tracer = get_bg_trace(self, func)
        with tracer.span(name="%s.%s" % (self.module_name, func.__name__)):
            return func(self, *args, **kwargs)
    return tracer


def trace_simple(func):
    """Decorator for tracing of functions."""
    if not execution_context:
        return func

    def tracer(*args, **kwargs):
        tracer = execution_context.get_opencensus_tracer()
        with tracer.span(name="%s" % (func.__name__)):
            return func(*args, **kwargs)
    return tracer


def stop_trace():
    """Stop the current trace."""
    if not execution_context:
        return
    execution_context.set_current_span(None)
    tracer = execution_context.get_opencensus_tracer()
    tracer.tracer = noop_tracer.NoopTracer()
    tracer.span_context = SpanContext(trace_options=trace_options.TraceOptions(0))
    tracer.sampler = always_off.AlwaysOffSampler()


def add_attr_to_trace(key, value):
    """Add an attribute to the current span if tracing is enabled."""
    if not execution_context:
        return
    tracer = execution_context.get_opencensus_tracer()
    tracer.add_attribute_to_current_span(
                attribute_key=key,
                attribute_value=value)
