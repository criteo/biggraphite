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

"""Create Lucene queries from Graphite-like globs."""
from __future__ import absolute_import
from __future__ import print_function

import json

import six

from biggraphite import glob_utils
from biggraphite.drivers import cassandra_common


class Error(Exception):
    """Base class for all exceptions from this module."""


class GlobError(Exception):
    """Base class for all translation exceptions from this module."""


# Graphite Abstract Syntax Tree supported types
GLOBSTAR = glob_utils.Globstar()
ANYSEQUENCE = glob_utils.AnySequence()

# Value stored in Cassandra columns component_? to mark the end of the metric name or pattern
END_MARK = cassandra_common.LAST_COMPONENT


def translate_to_lucene_filter(components):
    """Translate a list of constraints on components to a lucene query.

    Take a glob components iterable. Build an equivalent Apache Lucene filter using
    hardcoded assumptions about the index schema.

    Return an Lucene filter json string.
    """
    lucene_filter = {"filter": {"type": "boolean", "must": []}}
    must_list = []
    globstars = components.count(GLOBSTAR)

    if globstars > 1:
        raise GlobError("Contains more than one globstar (**) operator")

    if globstars:
        gs_index = components.index(GLOBSTAR)
        if gs_index == len(components) - 1:
            # No define length: match on components before the GLOBSTAR
            components.pop()
            must_list = _build_filters(components)
        else:
            # GLOBSTAR is only supported at the end of a glob syntax
            raise GlobError(
                "Metric pattern syntax only supports '%s' at the end" % GLOBSTAR
            )
    elif (  # Parent query
        len(components) > 1
        and all(
            len(c) == 1 and glob_utils.is_fixed_sequence(c[0]) for c in components[:-1]
        )
        and isinstance(components[-1][0], glob_utils.AnySequence)
    ):
        parent = ""
        for component in components[:-1]:
            parent += component[0] + "."
        must_list.append({"field": "parent", "type": "match", "value": parent})
    else:
        must_list = _build_filters(components)
        # Restrict length by matching the END_MARK
        must_list.append(
            {
                "field": _component_name(len(components)),
                "type": "match",
                "value": END_MARK,
            }
        )

    if not must_list:
        return None

    lucene_filter["filter"]["must"] = must_list

    # Join the constraints (with a nice indentation)
    return json.dumps(lucene_filter)


def _component_name(idx):
    """Helper to get the name of a component."""
    return "component_%i" % idx


def _build_filters(components):
    """Build filters from components."""
    must_list = []
    for idx, component in enumerate(components):
        if len(component) == 0:
            # FIXME: When can this happen ?
            raise GlobError(
                "Illegal glob component %s in glob %s" % (component, components)
            )
        elif len(component) == 1:
            must_list.append(_build_simple_field_constrain(idx, component[0]))
        else:  # len(component) > 1
            # Lucene prefix, wildcard & regexp are 3 different syntax.
            # Behavior and performances of underlying implementations are almost the same.
            # We therefore only use the most expressive syntax: 'regexp'
            must_list.append(_build_regex_field_constrain(idx, component))
    return list(filter(None, must_list))


def _build_simple_field_constrain(index, value):
    """Given a component with a simple type, this builds a constraint."""
    if isinstance(value, glob_utils.AnySequence):
        return None  # No constrain
    elif isinstance(value, six.string_types):
        field = {"field": _component_name(index), "type": "match", "value": value}
    elif isinstance(value, glob_utils.CharNotIn):
        field = {
            "field": _component_name(index),
            "type": "regexp",
            "value": "[^" + "".join(value.values) + "]",
        }
    elif isinstance(value, glob_utils.CharIn):
        field = {
            "field": _component_name(index),
            "type": "regexp",
            "value": "[" + "".join(value.values) + "]",
        }
    elif isinstance(value, glob_utils.SequenceIn):
        field = {
            "field": _component_name(index),
            "type": "contains",
            "values": value.values,
        }
    elif isinstance(value, glob_utils.AnyChar):
        field = {"field": _component_name(index), "type": "regexp", "value": "."}
    else:
        raise GlobError("Unhandled type '%s'" % value)
    return field


def _build_regex_field_constrain(index, component):
    """Given a complex component, this builds a constraint."""
    regex = ""
    for subcomponent in component:
        if isinstance(subcomponent, glob_utils.AnySequence):
            regex += ".*"
        elif isinstance(subcomponent, six.string_types):
            regex += subcomponent
        elif isinstance(subcomponent, glob_utils.CharNotIn):
            regex += "[^" + "".join(subcomponent.values) + "]"
        elif isinstance(subcomponent, glob_utils.CharIn):
            regex += "[" + "".join(subcomponent.values) + "]"
        elif isinstance(subcomponent, glob_utils.SequenceIn):
            if subcomponent.negated:
                regex += ".*"
            else:
                regex += "(" + "|".join(subcomponent.values) + ")"
        elif isinstance(subcomponent, glob_utils.AnyChar):
            regex += "."
        else:
            raise GlobError("Unhandled type '%s'" % subcomponent)
    return {"field": _component_name(index), "type": "regexp", "value": regex}
