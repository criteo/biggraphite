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
import gc
import prometheus_client
import weakref

from biggraphite import metric as bg_metric
from tests import test_utils as bg_test_utils


class TestMetric(unittest.TestCase):
    def test_dir(self):
        metric = bg_test_utils.make_metric_with_defaults("a.b.c")
        self.assertIn("name", dir(metric))
        self.assertIn("carbon_xfilesfactor", dir(metric))


class TestAccessorFunctions(unittest.TestCase):
    def test_sanitize_metric_name_should_remove_multiple_dots(self):
        self.assertEqual("foo.bar.baz", bg_metric.sanitize_metric_name("foo.bar..baz"))
        self.assertEqual("foo.bar.baz", bg_metric.sanitize_metric_name("foo.bar...baz"))
        self.assertEqual("foo.bar.baz", bg_metric.sanitize_metric_name("foo..bar..baz"))

    def test_sanitize_metric_name_should_trim_trailing_dots(self):
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name("foo.bar."))
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name("foo.bar.."))

    def test_sanitize_metric_name_should_trim_heading_dots(self):
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name(".foo.bar"))
        self.assertEqual("foo.bar", bg_metric.sanitize_metric_name("..foo.bar"))

    def test_sanitize_metric_name_should_handle_None_value(self):
        self.assertEqual(None, bg_metric.sanitize_metric_name(None))

    def test_sanitize_metric_name_should_handle_empty_value(self):
        self.assertEqual("", bg_metric.sanitize_metric_name(""))
        self.assertEqual("", bg_metric.sanitize_metric_name("."))
        self.assertEqual("", bg_metric.sanitize_metric_name(".."))


class TestMetricMetadata(unittest.TestCase):
    DEFAULT_AGGREGATOR = bg_metric.Aggregator.average

    DEFAULT_RETENTION = bg_metric.Retention.from_string("86400*1s:10080*60s")
    TEST_RETENTION = bg_metric.Retention.from_string("42*10s")

    DEFAULT_XFACTOR = 0.5

    def test_create_should_define_default_values_when_no_parameters_given(self):
        """Ensure default parameters."""
        metadata = bg_metric.MetricMetadata.create()
        self.assertEqual(metadata.aggregator, self.DEFAULT_AGGREGATOR)
        self.assertEqual(metadata.retention, self.DEFAULT_RETENTION)
        self.assertEqual(metadata.carbon_xfilesfactor, self.DEFAULT_XFACTOR)

    def test_metadata_object_should_be_the_same_when_created_with_same_parameters(self):
        """MetricMetadata.create() should always return the same MetricMetadata instance."""
        metadata1 = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.DEFAULT_RETENTION, self.DEFAULT_XFACTOR
        )
        metadata2 = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.DEFAULT_RETENTION, self.DEFAULT_XFACTOR
        )
        self.assertIs(metadata1, metadata2)

        # the returned instance should't depend on the order of the parameters
        metadata3 = bg_metric.MetricMetadata.create(
            carbon_xfilesfactor=self.DEFAULT_XFACTOR,
            aggregator=self.DEFAULT_AGGREGATOR,
            retention=self.DEFAULT_RETENTION,
        )
        self.assertIs(metadata1, metadata3)

    def test_xfilesfactor_should_have_no_impact_on_uniqueness(self):
        """Check that carbon_xfilesfactor has no influence on uniqueness."""
        metadata1 = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.DEFAULT_RETENTION, self.DEFAULT_XFACTOR
        )
        metadata2 = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.DEFAULT_RETENTION, self.DEFAULT_XFACTOR - 0.1
        )

        self.assertIsNot(metadata1, metadata2)

    def test_aggregator_should_have_no_impact_on_uniqueness(self):
        """Check that aggregator has no influence on uniqueness."""
        metadata1 = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.DEFAULT_RETENTION, self.DEFAULT_XFACTOR
        )
        metadata2 = bg_metric.MetricMetadata.create(
            bg_metric.Aggregator.total, self.DEFAULT_RETENTION, self.DEFAULT_XFACTOR
        )

        self.assertIsNot(metadata1, metadata2)

    def test_retention_should_have_no_impact_on_uniqueness(self):
        """Check that retention has no influence on uniqueness."""
        metadata1 = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.DEFAULT_RETENTION, self.DEFAULT_XFACTOR
        )
        metadata2 = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.TEST_RETENTION, self.DEFAULT_XFACTOR
        )

        self.assertIsNot(metadata1, metadata2)

    def test_metadata_object_should_be_deleted_when_there_is_no_more_references_on_it(
        self,
    ):
        """Check that a metadata are properly cleaned-up when no one holds a reference on it."""
        metadata = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.TEST_RETENTION, self.DEFAULT_XFACTOR
        )

        metadata_weak = weakref.ref(metadata)
        self.assertIsNotNone(metadata_weak())

        # remove reference and force garbage collection
        del metadata
        gc.collect()
        self.assertIsNone(metadata_weak())

    def test_total_metadata_object_count_should_be_reported_by_prometheus_client(self):
        """Test the gauge reporting the number of metadata hold in the internal dictionnary."""
        def get_metadata_instances_count():
            return prometheus_client.REGISTRY.get_sample_value(
                "bg_metadata_instances_count"
            )

        # make sure previously allocated metadata are cleaned up before starting the test
        gc.collect()

        # allocate a new metadata object
        metadata_count_before = get_metadata_instances_count()
        metadata = bg_metric.MetricMetadata.create(
            self.DEFAULT_AGGREGATOR, self.TEST_RETENTION, self.DEFAULT_XFACTOR
        )
        self.assertEqual(get_metadata_instances_count() - metadata_count_before, 1)

        # delete the reference on the metadata object
        del metadata
        gc.collect()
        self.assertEqual(get_metadata_instances_count(), metadata_count_before)

    def test_from_dict_string(self):
        parameters = (
            (
                {
                    "aggregator": "sum",
                    "retention": "86400*1s:10080*60s",
                    "carbon_xfilesfactor": 0.5,
                },
                bg_metric.Aggregator.total,
            ),
            (
                {
                    "aggregator": u"sum",
                    "retention": "86400*1s:10080*60s",
                    "carbon_xfilesfactor": 0.5,
                },
                bg_metric.Aggregator.total,
            ),
            (
                {
                    "aggregator": "total",
                    "retention": "86400*1s:10080*60s",
                    "carbon_xfilesfactor": 0.5,
                },
                bg_metric.Aggregator.total,
            ),
        )
        for parameter in parameters:
            metadata = bg_metric.MetricMetadata.from_string_dict(parameter[0])
            self.assertEqual(metadata.aggregator, parameter[1])
