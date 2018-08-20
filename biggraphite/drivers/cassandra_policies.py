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

"""Custom cassandra policies."""
from __future__ import absolute_import
from __future__ import print_function

from cassandra import policies


class AlwaysRetryPolicy(policies.RetryPolicy):
    """A Custom Retry Policy that always retries.

    This avoid duplicating the retry policy in the application itself.
    It happens that Cassandra is transiatly slow (GC), and retrying
    a few seconds later will allow us to get the results.
    """

    MAX_RETRIES = 3

    def __init__(self, max_retries=None):
        """Creates the policy."""
        self.max_retries = max_retries or self.MAX_RETRIES

    def on_read_timeout(
        self,
        query,
        consistency,
        required_responses,
        received_responses,
        data_retrieved,
        retry_num,
    ):
        """Called when a read operation times out."""
        if retry_num > self.max_retries:
            return self.RETHROW, None
        else:
            # It's usually a good idea to retry on another host.
            return self.RETRY_NEXT_HOST, consistency

    def on_unavailable(
        self, query, consistency, required_replicas, alive_replicas, retry_num
    ):
        """Called when the operation cannot be successful."""
        if retry_num > self.max_retries:
            return self.RETHROW, None
        else:
            return (self.RETRY_NEXT_HOST, consistency)
