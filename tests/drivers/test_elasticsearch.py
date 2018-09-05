#!/usr/bin/env python
# coding=utf-8
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

from __future__ import print_function

import datetime
import unittest
import uuid

import elasticsearch_dsl
import freezegun
from freezegun import freeze_time

from biggraphite import glob_utils as bg_glob
from biggraphite import metric as bg_metric
from biggraphite.drivers import elasticsearch as bg_elasticsearch
from biggraphite.metric import Aggregator, Retention, MetricMetadata
from tests import test_utils as bg_test_utils
from tests.drivers.base_test_metadata import BaseTestAccessorMetadata
from tests.test_utils_elasticsearch import HAS_ELASTICSEARCH


class ComponentFromNameTest(unittest.TestCase):
    def test_components_from_name_should_throw_AttributeError_on_None_path(self):
        with self.assertRaises(AttributeError):
            bg_elasticsearch._components_from_name(None)

    def test_components_from_name_should_create_array_by_splitting_on_dots(self):
        expected = ["a", "b", "c", "d"]
        result = bg_elasticsearch._components_from_name("a.b.c.d")
        self.assertEqual(result, expected)

    def test_components_from_name_should_ignore_double_dots(self):
        expected = ["a", "b", "c"]
        result = bg_elasticsearch._components_from_name("a.b..c")
        self.assertEqual(result, expected)

    def test_components_from_name_should_handle_empty_strings(self):
        expected = []
        result = bg_elasticsearch._components_from_name("")
        self.assertEqual(result, expected)


class DocumentFromMetricTest(unittest.TestCase):
    def test_document_from_metric_should_throw_AttributeError_on_None_metric(self):
        with self.assertRaises(AttributeError):
            bg_elasticsearch.document_from_metric(None)

    def test_document_from_metric_should_build_a_document_from_a_metric(self):
        p0 = "foo"
        p1 = "bar"
        p2 = "baz"
        metric_name = "%s.%s.%s" % (p0, p1, p2)
        metric_id = uuid.uuid5(
            uuid.UUID("{00000000-1111-2222-3333-444444444444}"), metric_name
        )

        aggregator = Aggregator.maximum
        retention_str = "42*1s:43*60s"
        retention = Retention.from_string(retention_str)
        carbon_xfilesfactor = 0.5
        metadata = MetricMetadata(aggregator, retention, carbon_xfilesfactor)
        metric = bg_metric.Metric(
            metric_name,
            metric_id,
            metadata,
            created_on=datetime.datetime(2017, 1, 1),
            updated_on=datetime.datetime(2018, 2, 2),
        )

        document = bg_elasticsearch.document_from_metric(metric)

        self.__check_document_value(document, "depth", 2)
        self.__check_document_value(document, "uuid", metric_id)
        self.__check_document_value(document, "p0", p0)
        self.__check_document_value(document, "p1", p1)
        self.__check_document_value(document, "p2", p2)

        self.assertTrue("config" in document)
        document_config = document["config"]
        self.__check_document_value(document_config, "aggregator", aggregator.name)
        self.__check_document_value(document_config, "retention", retention_str)
        self.__check_document_value(
            document_config, "carbon_xfilesfactor", "%f" % carbon_xfilesfactor
        )

        self.assertTrue("created_on" in document)
        self.assertTrue(isinstance(document["created_on"], datetime.datetime))
        self.assertEqual(metric.created_on, document["created_on"])
        self.assertTrue("updated_on" in document)
        self.assertTrue(isinstance(document["updated_on"], datetime.datetime))
        self.assertEqual(metric.updated_on, document["updated_on"])
        self.assertTrue("read_on" in document)
        self.assertEqual(document["read_on"], None)

    def __check_document_value(self, document, key, value):
        self.assertTrue(key in document)
        self.assertEqual(document[key], value)


class ParseSimpleComponentTest(unittest.TestCase):
    def test_any_sequence_should_have_no_constraint(self):
        self.assertEqual(
            bg_elasticsearch.parse_simple_component([bg_glob.AnySequence()]),
            (None, None),
        )

    def test_string_sequence_should_have_term_constraint(self):
        value = "foo"
        self.assertEqual(
            bg_elasticsearch.parse_simple_component([value]), ("term", value)
        )

    def test_CharNotIn_should_have_regex_constraint(self):
        value1 = "a"
        value2 = "b"
        self.assertEqual(
            bg_elasticsearch.parse_simple_component(
                [bg_glob.CharNotIn([value1, value2])]
            ),
            ("regexp", "[^ab]"),
        )

    def test_CharIn_should_have_regex_constraint(self):
        value1 = "a"
        value2 = "b"
        self.assertEqual(
            bg_elasticsearch.parse_simple_component([bg_glob.CharIn([value1, value2])]),
            ("regexp", "[ab]"),
        )

    def test_SequenceIn_should_have_terms_constraint_when_there_are_no_regexp_in_terms(
        self
    ):
        values = ["a", "b"]
        self.assertEqual(
            bg_elasticsearch.parse_simple_component([bg_glob.SequenceIn(values)]),
            ("terms", values),
        )

    def test_SequenceIn_should_have_regexp_constraint_when_there_are_regexp_in_terms(
        self
    ):
        values = ["a", "b*"]
        self.assertEqual(
            bg_elasticsearch.parse_simple_component([bg_glob.SequenceIn(values)]),
            ("regexp", "(a|b*)"),
        )

    def test_AnyChar_should_have_wildcard_constraint(self):
        self.assertEqual(
            bg_elasticsearch.parse_simple_component([bg_glob.AnyChar()]),
            ("wildcard", "?"),
        )

    def test_Globstar_component_should_trigger_Error(self):
        with self.assertRaisesRegexp(
            bg_elasticsearch.Error, "Unhandled type 'Globstar'"
        ):
            bg_elasticsearch.parse_simple_component([bg_glob.Globstar()])

    def test_None_component_should_trigger_Error(self):
        with self.assertRaisesRegexp(bg_elasticsearch.Error, "Unhandled type 'None'"):
            bg_elasticsearch.parse_simple_component([None])


class ParseComplexComponentTest(unittest.TestCase):
    def setUp(self):
        self._wildcard_samples = [bg_glob.AnyChar(), bg_glob.AnySequence(), "a"]
        self._regexp_samples = [
            bg_glob.CharIn(["α", "β"]),
            bg_glob.SequenceIn(["γ", "δ"]),
        ]

    def test_wildcard_component_should_be_parsed_as_wildcard(self):
        for wildcard_sample in self._wildcard_samples:
            parsed_type, _ = bg_elasticsearch.parse_complex_component([wildcard_sample])
            self.assertEqual(
                parsed_type,
                "wildcard",
                "%s should be parsed as a wildcard" % type(wildcard_sample).__name__,
            )

    def test_combination_of_wildcard_components_should_be_parsed_as_wildcard(self):
        for wildcard_sample_1 in self._wildcard_samples:
            for wildcard_sample_2 in self._wildcard_samples:
                parsed_type, _ = bg_elasticsearch.parse_complex_component(
                    [wildcard_sample_1, wildcard_sample_2]
                )
                self.assertEqual(
                    parsed_type,
                    "wildcard",
                    "[%s, %s] should be parsed as a wildcard"
                    % (
                        type(wildcard_sample_1).__name__,
                        type(wildcard_sample_2).__name__,
                    ),
                )

    def test_regexp_component_should_be_parsed_as_regexp(self):
        for regexp_sample in self._regexp_samples:
            parsed_type, _ = bg_elasticsearch.parse_complex_component([regexp_sample])
            self.assertEqual(
                parsed_type,
                "regexp",
                "%s should be parsed as a regexp" % type(regexp_sample).__name__,
            )

    def test_combination_of_regexp_components_should_be_parsed_as_regexp(self):
        for regexp_sample_1 in self._regexp_samples:
            for regexp_sample_2 in self._regexp_samples:
                parsed_type, _ = bg_elasticsearch.parse_complex_component(
                    [regexp_sample_1, regexp_sample_2]
                )
                self.assertEqual(
                    parsed_type,
                    "regexp",
                    "[%s, %s] should be parsed as a regexp"
                    % (type(regexp_sample_1).__name__, type(regexp_sample_2).__name__),
                )

    def test_combination_of_regexp_and_wildcard_components_should_be_parsed_as_regexp(
        self
    ):
        for regexp_sample in self._regexp_samples:
            for wildcard_sample in self._wildcard_samples:
                parsed_type, _ = bg_elasticsearch.parse_complex_component(
                    [regexp_sample, wildcard_sample]
                )
                self.assertEqual(
                    parsed_type,
                    "regexp",
                    "[%s, %s] should be parsed as a regexp"
                    % (type(regexp_sample).__name__, type(wildcard_sample).__name__),
                )

    def test_AnyChar_should_be_translated_into_a_question_mark(self):
        _, result = bg_elasticsearch.parse_complex_component([bg_glob.AnyChar()])
        self.assertEqual(result, "?")

    def test_AnySequence_should_be_translated_into_an_asterisk(self):
        _, result = bg_elasticsearch.parse_complex_component([bg_glob.AnySequence()])
        self.assertEqual(result, "*")

    def test_string_should_be_translated_into_itself(self):
        _, result = bg_elasticsearch.parse_complex_component(["a"])
        self.assertEqual(result, "a")

    def test_CharIn_should_be_translated_into_a_range(self):
        _, result = bg_elasticsearch.parse_complex_component(
            [bg_glob.CharIn(["a", "b"])]
        )
        self.assertEqual(result, "[ab]")

    def test_CharNotIn_should_be_translated_into_a_range(self):
        _, result = bg_elasticsearch.parse_complex_component(
            [bg_glob.CharNotIn(["a", "b"])]
        )
        self.assertEqual(result, "[^ab]")

    def test_SequenceIn_should_be_translated_into_an_enum_regexp(self):
        _, result = bg_elasticsearch.parse_complex_component(
            [bg_glob.SequenceIn(["a", "b"])]
        )
        self.assertEqual(result, "(a|b)")

    def test_Globstar_should_be_translated_into_a_match_all_regexp(self):
        _, result = bg_elasticsearch.parse_complex_component([bg_glob.Globstar()])
        self.assertEqual(result, ".*")

    def test_combination_of_glob_expressions_should_be_concatenated(self):
        _, result = bg_elasticsearch.parse_complex_component(
            [
                "foo",
                bg_glob.SequenceIn(["bar", "baz"]),
                bg_glob.CharNotIn(["a", "b"]),
                bg_glob.Globstar(),
            ]
        )
        self.assertEqual(result, "foo(bar|baz)[^ab].*")


def _create_default_metadata():
    aggregator = Aggregator.maximum
    retention_str = "42*1s:43*60s"
    retention = Retention.from_string(retention_str)
    carbon_xfilesfactor = 0.5
    metadata = MetricMetadata(aggregator, retention, carbon_xfilesfactor)
    return metadata


@unittest.skipUnless(HAS_ELASTICSEARCH, "ES_HOME must be set.")
class TestAccessorWithElasticsearch(
    BaseTestAccessorMetadata, bg_test_utils.TestCaseWithAccessor
):
    ACCESSOR_SETTINGS = {"driver": "elasticsearch"}

    def test_created_on_and_updated_on_are_set_upon_creation_date(self):
        metric_name = "test_created_on_is_set_upon_current_date.a.b.c"
        expected_created_on = datetime.datetime(2017, 1, 2)
        with freezegun.freeze_time(expected_created_on):
            metric = bg_test_utils.make_metric(metric_name, _create_default_metadata())
            self.accessor.create_metric(metric)
            self.accessor.flush()

        metric = self.accessor.get_metric(metric_name)
        self.accessor.flush()

        self.assertEqual(metric.created_on, expected_created_on)
        self.assertEqual(metric.updated_on, expected_created_on)

    def test_updated_on_is_set_upon_current_date_with_created_on_unchanged(self):
        metric_name = "test_created_on_is_set_upon_current_date.a.b.c"
        expected_created_on = datetime.datetime(2000, 1, 1)
        expected_updated_on = datetime.datetime(2011, 1, 1)
        with freezegun.freeze_time(expected_created_on):
            metric = bg_test_utils.make_metric(metric_name, _create_default_metadata())
            self.accessor.create_metric(metric)
            self.accessor.flush()
        with freezegun.freeze_time(expected_updated_on):
            self.accessor.touch_metric(metric)
            self.accessor.flush()
        metric = self.accessor.get_metric(metric_name)

        self.assertEqual(expected_created_on, metric.created_on)
        self.assertEqual(expected_updated_on, metric.updated_on)

    def test_metric_is_updated_after_ttl(self):
        with freezegun.freeze_time("2014-01-01 00:00:00"):
            metric = bg_test_utils.make_metric("foo")
            self.accessor.create_metric(metric)
        self.flush()

        created_metric = self.accessor.get_metric(metric.name)

        old_ttl = self.accessor._ElasticSearchAccessor__updated_on_ttl_sec
        self.accessor._ElasticSearchAccessor__updated_on_ttl_sec = 1

        with freezegun.freeze_time("2014-01-01 00:00:02"):
            self.accessor.touch_metric(metric)
        self.flush()

        updated_metric = self.accessor.get_metric(metric.name)

        self.assertNotEqual(created_metric.updated_on, updated_metric.updated_on)

        self.accessor._ElasticSearchAccessor__updated_on_ttl_sec = old_ttl

    def test_metric_is_recreated_if_index_has_changed(self):
        old_get_index_fn = self.accessor.get_index

        def get_index_mock(metric):
            return "testindex_2013-07-21"

        self.accessor.get_index = get_index_mock

        with freezegun.freeze_time("2014-01-01 00:00:00"):
            metric = bg_test_utils.make_metric(
                "elasticsearch.test_metric_is_recreated_if_index_has_changed"
            )
            self.accessor.create_metric(metric)
            self.flush()

        with freezegun.freeze_time("2014-01-01 00:00:00"):
            self.accessor.get_metric(metric.name)
            self.flush()

        self.accessor.get_index = old_get_index_fn

        old_ttl = self.accessor._ElasticSearchAccessor__updated_on_ttl_sec
        self.accessor._ElasticSearchAccessor__updated_on_ttl_sec = 1

        with freezegun.freeze_time("2014-01-01 00:00:02"):
            self.accessor.touch_metric(metric)
            self.flush()

        search = elasticsearch_dsl.Search()
        search = (
            search.using(self.accessor.client)
            .index("testindex*")
            .source(["uuid", "config", "created_on", "updated_on", "read_on"])
            .filter("term", name=metric.name)
            .sort({"updated_on": {"order": "desc"}})
        )

        responses = search.execute()
        self.assertEqual(2, responses.hits.total)
        with freezegun.freeze_time("2014-01-01 00:00:02"):
            self.assertEqual(
                self.accessor.get_index(metric), responses.hits[0].meta.index
            )
        self.assertEqual(get_index_mock(None), responses.hits[1].meta.index)

        self.accessor._ElasticSearchAccessor__updated_on_ttl_sec = old_ttl

    def test_fetch_points_updates_read_on(self):
        metric = bg_test_utils.make_metric("foo")
        self.accessor.create_metric(metric)
        self.flush()

        metric = self.accessor.get_metric(metric.name)
        self.assertIsNone(metric.read_on)

        points = self.accessor.fetch_points(metric, 0, 1, metric.retention[0])
        self.assertListEqual(list(points), [])
        self.flush()

        metric = self.accessor.get_metric(metric.name)
        self.assertIsNotNone(metric.read_on)

        # Try again, we read_on != None now
        self.accessor.fetch_points(metric, 0, 1, metric.retention[0])

    def test_update_metric_should_raise_InvalidArgumentError_for_unknown_metric(self):
        with self.assertRaises(bg_elasticsearch.InvalidArgumentError):
            self.accessor.update_metric("whatever", None)

    def test_update_metric_should_update_metric_metadata(self):
        metric_name = "test_update_metric_should_update_metric_metadata.a.b.c"
        initial_metadata = MetricMetadata(
            aggregator=Aggregator.total,
            retention=Retention.from_string("42*1s"),
            carbon_xfilesfactor=0.5,
        )
        create_date = datetime.datetime(2014, 1, 1)
        update_date = datetime.datetime(2018, 1, 1)
        new_metadata = MetricMetadata(
            aggregator=Aggregator.average,
            retention=Retention.from_string("43*100s"),
            carbon_xfilesfactor=0.25,
        )
        with freezegun.freeze_time(create_date):
            metric = bg_test_utils.make_metric(metric_name, initial_metadata)
            self.accessor.create_metric(metric)
            self.accessor.flush()
        with freezegun.freeze_time(update_date):
            self.accessor.update_metric(metric_name, new_metadata)
            self.accessor.flush()

        metric = self.accessor.get_metric(metric_name)
        self.assertEqual(update_date, metric.updated_on)
        self.assertEqual(new_metadata, metric.metadata)

    def test_delete_metric_should_remove_metric_from_name(self):
        metric_name = "test_delete_metric_should_remove_metric_from_name.a.b.c"
        metric = bg_test_utils.make_metric(metric_name)
        self.accessor.create_metric(metric)
        self.accessor.flush()

        self.accessor.delete_metric(metric_name)
        self.accessor.flush()

        self.assertIsNone(self.accessor.get_metric(metric_name))

    def test_delete_directory_should_delete_all_metrics_in_the_given_directory(self):
        prefix = (
            "test_delete_directory_should_delete_all_metrics_in_the_given_directory"
        )
        to_delete_prefix = "%s.should_not_be_found" % prefix
        not_to_delete_prefix = "%s.should_be_found" % prefix
        short_names = ["a", "b", "c"]

        unexpected_metrics = [
            "%s.%s" % (to_delete_prefix, short_name) for short_name in short_names
        ]
        expected_metrics = [
            "%s.%s" % (not_to_delete_prefix, short_name) for short_name in short_names
        ]

        for metric_name in expected_metrics + unexpected_metrics:
            self.accessor.create_metric(bg_test_utils.make_metric(metric_name))
            self.accessor.flush()

        self.accessor.delete_directory(to_delete_prefix)
        self.accessor.flush()

        for expected_metric in expected_metrics:
            self.assertIsNotNone(self.accessor.get_metric(expected_metric))
        for unexpected_metric in unexpected_metrics:
            self.assertIsNone(self.accessor.get_metric(unexpected_metric))

    def test_delete_metric_should_remove_metric_from_index(self):
        prefix = "test_delete_metric_should_remove_metric_from_index"
        to_delete_metric_name = "%s.to_delete" % prefix
        not_to_delete_metric_name = "%s.not_to_delete" % prefix

        self.accessor.create_metric(bg_test_utils.make_metric(to_delete_metric_name))
        self.accessor.create_metric(
            bg_test_utils.make_metric(not_to_delete_metric_name)
        )
        self.accessor.flush()

        self.accessor.delete_metric(to_delete_metric_name)
        self.accessor.flush()

        self.assertIsNone(self.accessor.get_metric(to_delete_metric_name))
        self.assertIsNotNone(self.accessor.get_metric(not_to_delete_metric_name))

    def test_insert_points_async_is_not_supported(self):
        metric = bg_test_utils.make_metric("foo")
        with self.assertRaises(Exception):
            self.accessor.insert_points_async(metric, [])

    def test_insert_downsampled_points_async_is_not_supported(self):
        metric = bg_test_utils.make_metric("foo")
        with self.assertRaises(Exception):
            self.accessor.insert_downsampled_points_async(metric, [])

    def test_glob_metrics_should_return_metrics_matching_glob(self):
        metric_name = "test_glob_metrics_should_return_metrics_matching_glob.a.b.c"
        metric = self._create_updated_metric(metric_name)

        glob = "test_glob_metrics_should_return_metrics_matching_glob.*.*.*"
        results = self.accessor.glob_metrics(glob)

        self.assertTrue(len(results) > 0, "Expected metric was not found")
        found_metric = results[0]
        self.assertEqual(metric_name, found_metric.name)
        self.assertEqual(metric.metadata, found_metric.metadata)
        self.assertEqual(metric.created_on, found_metric.created_on)

    def test_glob_metric_names_should_return_empty_results_when_out_of_bounds(self):
        self._metric_out_of_search_bounds_should_not_be_found(self.accessor.glob_metric_names)

    def test_glob_metrics_should_return_empty_results_when_out_of_bounds(self):
        self._metric_out_of_search_bounds_should_not_be_found(self.accessor.glob_metrics)

    def _metric_out_of_search_bounds_should_not_be_found(self, glob_search_function):
        metric_name = "test_metric_out_of_search_bounds_should_not_be_found.a.b.c"
        metric = self._create_updated_metric(metric_name)
        metric_created_on = metric.created_on
        metric_updated_on = metric.updated_on

        one_week = datetime.timedelta(weeks=1)
        bounds = [
            (None, metric_created_on - one_week),
            (metric_created_on - 2 * one_week, metric_created_on - one_week),
            (metric_updated_on + one_week, None),
            (metric_updated_on + one_week, metric_updated_on + 2 * one_week),
            # Dummy input cases (inverted bounds)
            (metric_updated_on + one_week, metric_created_on - one_week),
        ]

        for start_time, end_time in bounds:
            should_not_be_found_metric = glob_search_function(metric_name, start_time, end_time)
            self.assert_iter_empty(should_not_be_found_metric, start_time, end_time)

    def test_glob_metric_names_should_return_results_when_in_bounds(self):
        self._metric_in_search_bounds_should_be_found(self.accessor.glob_metric_names)

    def test_glob_metrics_should_return_results_when_in_bounds(self):
        self._metric_in_search_bounds_should_be_found(self.accessor.glob_metrics)

    def _metric_in_search_bounds_should_be_found(self, glob_search_function):
        metric_name = "test_metric_in_search_bounds_should_be_found.a.b.c"
        metric = self._create_updated_metric(metric_name)
        metric_created_on = metric.created_on
        metric_updated_on = metric.updated_on

        one_week = datetime.timedelta(weeks=1)
        bounds = [
            (None, None),
            (None, metric_created_on),
            (None, metric_created_on + one_week),
            (None, metric_updated_on - one_week),
            (None, metric_updated_on),
            (None, metric_updated_on + one_week),
            (metric_created_on - one_week, None),
            (metric_created_on - one_week, metric_created_on),
            (metric_created_on - one_week, metric_created_on + one_week),
            (metric_created_on - one_week, metric_updated_on - one_week),
            (metric_created_on - one_week, metric_updated_on),
            (metric_created_on - one_week, metric_updated_on + one_week),
            (metric_created_on, None),
            (metric_created_on, metric_created_on),
            (metric_created_on, metric_created_on + one_week),
            (metric_created_on, metric_updated_on - one_week),
            (metric_created_on, metric_updated_on),
            (metric_created_on, metric_updated_on + one_week),
            (metric_created_on + one_week, None),
            (metric_created_on + one_week, metric_created_on + one_week),
            (metric_created_on + one_week, metric_created_on + 2 * one_week),
            (metric_created_on + one_week, metric_updated_on - one_week),
            (metric_created_on + one_week, metric_updated_on),
            (metric_created_on + one_week, metric_updated_on + one_week),
            (metric_updated_on - one_week, None),
            (metric_updated_on - one_week, metric_updated_on - one_week),
            (metric_updated_on - one_week, metric_updated_on),
            (metric_updated_on - one_week, metric_updated_on + one_week),
            (metric_updated_on, None),
            (metric_updated_on, metric_updated_on),
            (metric_updated_on, metric_updated_on + one_week),
        ]

        for start_time, end_time in bounds:
            should_be_found_metric = glob_search_function(metric_name, start_time, end_time)
            self.assert_iter_not_empty(should_be_found_metric, start_time, end_time)

    def test_glob_metrics_should_return_deduplicated_results(self):
        metric_name = "test_glob_metrics_should_return_deduplicated_results.a.b.c"
        # _create_updated_metric is responsible for creating metrics over two indices
        self._create_updated_metric(metric_name)

        results = self.accessor.glob_metrics(metric_name)

        self.assertEqual(len(results), 1, "Only one metric is expected")

    def test_accessor_should_support_utf8_in_metric_paths(self):
        metric_name = "foo.👍.Ｔｈｅ.𝐪𝐮𝐢𝐜𝐤.𝖇𝖗𝖔𝖜𝖓.𝒇𝒐𝒙.𝚕𝚊𝚣𝚢.⒟⒪⒢"
        metric = self._create_updated_metric(metric_name)
        self.assertEqual(metric_name, metric.name)

    def _create_updated_metric(self, metric_name):
        creation_date = datetime.datetime(2018, 1, 1)
        update_date = datetime.datetime(2018, 3, 1)
        with freeze_time(creation_date):
            metric = bg_test_utils.make_metric(metric_name)
            self.accessor.create_metric(metric)
            self.accessor.flush()
            metric = self.accessor.get_metric(metric_name)
            assert metric.created_on == creation_date
            assert metric.updated_on == creation_date
        with freeze_time(update_date):
            metric = self.accessor.get_metric(metric_name)
            self.accessor.touch_metric(metric)
            self.accessor.flush()
            metric = self.accessor.get_metric(metric_name)
            assert metric.created_on == creation_date
            assert metric.updated_on == update_date
        return self.accessor.get_metric(metric_name)

    def assert_iter_empty(self, it, *params):
        items = list(it)
        self.assertEqual(
            len(items),
            0,
            "Expected no result, got '%s' (range: '%s')" % (items, str(params)),
        )

    def assert_iter_not_empty(self, it, *params):
        items = list(it)
        self.assertTrue(
            len(items) > 0, "Expected results, got empty (range: '%s')" % str(params)
        )


if __name__ == "__main__":
    unittest.main()
