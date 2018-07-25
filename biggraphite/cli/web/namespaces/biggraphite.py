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
"""BigGraphite API."""

from __future__ import absolute_import

import flask_restplus as rp

from biggraphite.cli.web import context


# TODO:
# - Add the equivalent of what the accessor provides
# - Add the ability to get/set points.

api = rp.Namespace("biggraphite", description="BigGraphite API")

metric = api.model(
    "Metric",
    {
        "id": rp.fields.String(readOnly=True, description="The metric identifier"),
        "name": rp.fields.String(description="The metric name"),
        "metadata": rp.fields.Raw(description="The metric metadata"),
        "created_on": rp.fields.DateTime(),
        "updated_on": rp.fields.DateTime(),
        "read_on": rp.fields.DateTime(),
    },
)


@api.route("/metric/<string:name>")
@api.doc("Operations on metrics.")
@api.param("name", "The metric name")
class MetricResource(rp.Resource):
    """A Metric."""

    @api.doc("Get a metric by name.")
    @api.marshal_with(metric)
    def get(self, name):
        """Get a metric."""
        m = context.accessor.get_metric(name)
        if not m:
            rp.abort(404)
        return m.as_string_dict()
