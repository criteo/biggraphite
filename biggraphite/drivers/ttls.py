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
"""Time constants and functions used by accessors."""

import ciso8601
import datetime
import time

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR

DEFAULT_READ_ON_TTL_SEC = 3 * DAY
DEFAULT_UPDATED_ON_TTL_SEC = 3 * DAY


def str_to_datetime(str_repr):
    """Convert a string into a datetime."""
    # Allow the caller to be stupid.
    if type(str_repr) == datetime.datetime:
        return str_repr
    if not str_repr:
        return None
    return ciso8601.parse_datetime(str_repr)


def str_to_timestamp(str_repr):
    """Convert a string into a timestamp."""
    if not str_repr:
        return None
    datetime_tuple = str_to_datetime(str_repr)
    ts = time.mktime(datetime_tuple.timetuple())
    return ts


def datetime_to_str(dt):
    """Converts a datetime to ISO representation without microseconds."""
    return dt.replace(microsecond=0).isoformat()
