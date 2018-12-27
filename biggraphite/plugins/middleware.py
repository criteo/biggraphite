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
# limitations under the License.from biggraphite import tracing
"""Middleware for Django (Graphite web). Provides wrapper on queries before they reach Graphite."""

from django.conf import settings
from biggraphite import tracing

# Default method type that biggraphite will trace
BG_TRACING_METHODS_DEFAULT = ["GET", "POST"]
# Default target that biggraphite will trace
BG_TRACING_TARGET_WHITELIST_DEFAULT = None


class BiggraphiteMiddleware(object):
    """Middleware for Django (Graphite web)."""

    def __init__(self, get_response):
        """One-time configuration and initialization."""
        self.get_response = get_response

    def __call__(self, request):
        """Code to be executed for each request before request execution."""
        tracing_methods = getattr(settings, 'BG_TRACING_METHODS', BG_TRACING_METHODS_DEFAULT)
        tracing_whitelist = getattr(settings, 'BG_TRACING_TARGET_WHITELIST',
                                    BG_TRACING_TARGET_WHITELIST_DEFAULT)

        if request.method not in tracing_methods:
            tracing.stop_trace()

        # Improve trace data
        if request.method == "GET":
            target = str(request.GET.get('target', 'unknown'))
            if (tracing_whitelist) and (target not in tracing_whitelist):
                print("%s/%s" % (tracing_whitelist, target))
                tracing.stop_trace()
            tracing.add_attr_to_trace('graphite.target', target)
            tracing.add_attr_to_trace('graphite.format',
                                      str(request.GET.get('format', 'unknown')))

        response = self.get_response(request)

        return response
