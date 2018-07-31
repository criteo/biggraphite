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
"""Metric class definition."""
import codecs
import datetime
import uuid

import six

_UTF8_CODEC = codecs.getencoder('utf8')
_UUID_NAMESPACE = uuid.UUID('{00000000-1111-2222-3333-444444444444}')


class Metric(object):
    """Represents all information about a metric.

    This is not an instance of MetricMetadata: It cannot be serialized
    in JSON to minimise confusion in cache that expects few possible
    Metadata at any time.

    Not meant to be mutated.
    """

    __slots__ = (
        "name",
        "id",
        "metadata",
        "created_on",
        "updated_on",
        "read_on",
    )

    def __init__(self, name, id, metadata,
                 created_on=None, updated_on=None, read_on=None):
        """Record its arguments."""
        super(Metric, self).__init__()
        assert name, "Metric: name is None"
        assert id, "Metric: id is None"
        assert metadata, "Metric: metadata is None"
        self.name = encode_metric_name(name)
        self.id = id
        self.metadata = metadata
        self.created_on = created_on
        self.updated_on = updated_on
        self.read_on = read_on

    def as_string_dict(self):
        """Turn an instance into a dict of string to string."""
        return {
            "id": str(self.id),
            "name": self.name,
            "created_on": self.created_on,
            "updated_on": self.updated_on,
            "read_on": self.read_on,
            "metadata": self.metadata.as_string_dict(),
        }

    def __getattr__(self, name):
        return getattr(self.metadata, name)

    def __dir__(self):
        res = dir(self.metadata)
        res.extend(self.__slots__)
        res.sort()
        return res

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, Metric):
            return False
        return (self.name == other.name and
                self.metadata == other.metadata)

    def __ne__(self, other):
        return not (self == other)


def encode_metric_name(name):
    """Encode name as utf-8, raise UnicodeError if it can't.

    Args:
      name: The metric to encode.

    This function make sure that we only have simple strings.

    For Python 2: must be an instance of basestring.
        If it is an instance of string, it will be assumed to already have been
        encoded for performance reasons.

    For Python 3: breaks bytes are given. We could probably decode them instead.

    Raises:
      UnicodeError: Couldn't encode.
    """
    if six.PY3:
        assert(name) is not bytes, "%s should not be of type 'bytes'" % name
        return name

    if isinstance(name, str):
        return name
    # Next line may raise UnicodeError
    return _UTF8_CODEC(name)[0]


def make_metric(name, metadata, created_on=None, updated_on=None, read_on=None):
    """Create a Metric object from its definition as name and metadata.

    Args:
      name: metric name.
      metadata: metric metadata.
      created_on: metric creation date.
      updated_on: metric last update date.
      read_on: metric last read date.

    Returns: a Metric object with a valid id.
    """
    name = sanitize_metric_name(name)
    uid = uuid.uuid5(_UUID_NAMESPACE, name)
    now = datetime.datetime.now()
    return Metric(
        name,
        uid,
        metadata,
        created_on=created_on or now,
        updated_on=updated_on or now,
        read_on=read_on
    )


def sanitize_metric_name(name):
    """Sanitize a metric name by removing double dots.

    :param name: Metric name
    :return: Sanitized metric name
    """
    if name is None:
        return None
    return ".".join(_components_from_name(name))


def _components_from_name(metric_name):
    res = metric_name.split(".")
    return list(filter(None, res))
